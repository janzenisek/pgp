using PGP.Data;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {
  public static class Evaluation {
    // Cross-instance structural delegate cache (Cause 3 fix).
    // Key = GetStructuralKey(p, rowCount); value is reused by any program with the same topology.
    private static readonly ConcurrentDictionary<string, Action<double[], double[], double[]>>
      _delegateCache = new ConcurrentDictionary<string, Action<double[], double[], double[]>>();

    // MethodInfo cache used by CompileToDelegate to build Math.* call nodes
    private static readonly System.Reflection.MethodInfo _miSin = typeof(Math).GetMethod(nameof(Math.Sin), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miCos = typeof(Math).GetMethod(nameof(Math.Cos), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miTan = typeof(Math).GetMethod(nameof(Math.Tan), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miTanh = typeof(Math).GetMethod(nameof(Math.Tanh), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miLog = typeof(Math).GetMethod(nameof(Math.Log), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miExp = typeof(Math).GetMethod(nameof(Math.Exp), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miSqrt = typeof(Math).GetMethod(nameof(Math.Sqrt), new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miMin = typeof(Math).GetMethod(nameof(Math.Min), new[] { typeof(double), typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miMax = typeof(Math).GetMethod(nameof(Math.Max), new[] { typeof(double), typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miIsNaN = typeof(double).GetMethod(nameof(double.IsNaN), new[] { typeof(double) })!;

    public static double EvaluateStack(PgpAlgorithm pgp, RPN<Symbol> program, Task task, DataRecord data) {
      var localEvaluationBuffer = new Stack<double>();
      int targetIdx = task.VariableIndices[task.TargetVariable];

      for (int i = 0; i < data.RowCount; i++) {
        foreach (var symbol in program) {
          if (symbol.Type == SymbolType.Constant) {
            localEvaluationBuffer.Push(symbol.Con.Value);
          } else if (symbol.Type == SymbolType.Variable) {
            localEvaluationBuffer.Push(data.Data[symbol.Var.Index * data.RowCount + i] * symbol.Var.Coefficient);
          } else {
            var tmpResult = symbol.Opr.Function(localEvaluationBuffer);
            if (double.IsNaN(tmpResult) || double.IsInfinity(tmpResult) || double.IsNegativeInfinity(tmpResult)) {
              localEvaluationBuffer.Clear();
              return double.NaN;
            } else {
              localEvaluationBuffer.Push(tmpResult);
            }
            //localEvaluationBuffer.Push(symbol.Opr.Function(localEvaluationBuffer));

            //if (operation.Arity == 1) evaluationBuffer.Push(operation.Function(new[] {evaluationBuffer.Pop()}));
            //else evaluationBuffer.Push(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
          }
        }
        var result = localEvaluationBuffer.Pop();
        if (localEvaluationBuffer.Count > 0) {
          Console.WriteLine("\n!!! ERROR !!!\n");
          localEvaluationBuffer.Clear();
        }
        if (double.IsNaN(result) || double.IsInfinity(result) || double.IsNegativeInfinity(result)) {
          return double.NaN;
        }


        program.TrueResults[i] = data.Data[targetIdx * data.RowCount + i]; // not necessary to do this in every evaluation, but it is more convenient to have the true values stored in the program for later use (e.g. for statistics)
        program.EstimatedResults[i] = result;
      }

      program.PearsonR = PearsonR.ComputeScore(program);
      program.NMSE = NMSE.ComputeScore(program);
      program.LD = LD.ComputeScore(program);

      return task.Score.Compute(program);
    }

    public static double EvaluateProgram(PgpAlgorithm pgp, RPN<Symbol> p, Task t, DataRecord data) {
      // --- Cause 3 fix: look up / build a structurally-keyed delegate shared across instances ---
      // --- Cause 2 fix: constants & coefficients are NOT baked in; they are passed at runtime  ---
      //     via the paramValues array, so the same delegate is valid for any numeric values.
      // --- Cause 1 fix: the row-loop runs *inside* the compiled delegate; only one call needed. ---

      string key = GetStructuralKey(p, data.RowCount);

      if (p.CompiledDelegate == null || !_delegateCache.ContainsKey(key)) {
        var compiled = CompileToDelegate(p, data.RowCount);
        if (compiled == null) return t.Score.GetPessimal();
        _delegateCache[key] = compiled;
        p.CompiledDelegate = compiled;
      } else {
        // Reuse cached cross-instance delegate on this program instance
        p.CompiledDelegate = _delegateCache[key];
      }

      int targetIdx = t.VariableIndices[t.TargetVariable];
      int rowCount = data.RowCount;

      // Extract current numeric parameters (constants then coefficients) into a small array.
      double[] paramValues = ExtractParameterValues(p);

      // Rent a temporary output buffer — zero heap allocation on the hot path.
      double[] estBuffer = ArrayPool<double>.Shared.Rent(rowCount);
      try {
        p.CompiledDelegate(data.Data, paramValues, estBuffer);

        // Check for non-finite results and copy into program lists via span (no _version bump).
        Span<double> estSpan = CollectionsMarshal.AsSpan(p.EstimatedResults);
        for (int i = 0; i < rowCount; i++) {
          if (!double.IsFinite(estBuffer[i])) return t.Score.GetPessimal();
          estSpan[i] = estBuffer[i];
          p.TrueResults[i] = data.Data[targetIdx * rowCount + i];
        }
      } finally {
        ArrayPool<double>.Shared.Return(estBuffer);
      }

      p.PearsonR = PearsonR.ComputeScore(p);
      p.NMSE = NMSE.ComputeScore(p);
      p.LD = LD.ComputeScore(p);

      return t.Score.Compute(p);
    }



    // Translates an RPN<Symbol> expression into a compiled Action<double[], double[], double[]>.
    // Parameters of the delegate:
    //   data        — column-major flat array (stride = rowCount)
    //   paramValues — [constants... , coefficients...] in RPN traversal order
    //   estimates   — pre-allocated output array; estimates[i] is written for each row i
    // The row loop runs inside the compiled body (Cause 1 fix).
    // Numeric values are read from paramValues at runtime (Cause 2 / 3 fix: cache by structure).
    // Returns null if the program is structurally invalid.
    private static Action<double[], double[], double[]>? CompileToDelegate(RPN<Symbol> p, int rowCount) {
      try {
        var dataParam = Expression.Parameter(typeof(double[]), "data");
        var paramValuesParam = Expression.Parameter(typeof(double[]), "paramValues");
        var estimatesParam = Expression.Parameter(typeof(double[]), "estimates");

        // Loop variable
        var rowVar = Expression.Variable(typeof(int), "row");

        // Walk the RPN once to collect param slot assignments and build per-row expression.
        // paramSlot: constants get slots 0..nConst-1, coefficients get slots nConst..nConst+nCoef-1
        int paramSlot = 0;
        int coefSlotBase = p.Count(s => ((Symbol)(object)s!).Type == SymbolType.Constant);

        int constSlot = 0;
        int coefSlot = 0;
        var slotByRpnIndex = new int[p.Count]; // index into paramValues for each terminal
        for (int i = 0; i < p.Count; i++) {
          var sym = (Symbol)(object)p[i]!;
          if (sym.Type == SymbolType.Constant) {
            slotByRpnIndex[i] = constSlot++;
          } else if (sym.Type == SymbolType.Variable) {
            slotByRpnIndex[i] = coefSlotBase + coefSlot++;
          }
        }

        // Build the scalar expression for one row, reading params from paramValues
        var stack = new Stack<Expression>();
        constSlot = 0;
        coefSlot = 0;

        for (int i = 0; i < p.Count; i++) {
          var sym = (Symbol)(object)p[i]!;
          if (sym.Type == SymbolType.Constant) {
            // Read constant from paramValues[slot] at runtime
            int slot = slotByRpnIndex[i];
            stack.Push(Expression.ArrayIndex(paramValuesParam, Expression.Constant(slot)));
          } else if (sym.Type == SymbolType.Variable) {
            // data[varIndex * rowCount + row]
            int stride = sym.Var.Index * rowCount;
            Expression idx = stride == 0
              ? (Expression)rowVar
              : Expression.Add(Expression.Constant(stride), rowVar);
            Expression load = Expression.ArrayIndex(dataParam, idx);

            // Multiply by coefficient read from paramValues[slot] at runtime
            int slot = slotByRpnIndex[i];
            Expression coef = Expression.ArrayIndex(paramValuesParam, Expression.Constant(slot));
            stack.Push(Expression.Multiply(load, coef));
          } else {
            Expression? node = BuildOperatorExpression(sym.Opr, stack);
            if (node == null) return null;
            stack.Push(node);
          }
        }

        if (stack.Count != 1) return null;
        Expression scalarBody = stack.Pop();

        // for (int row = 0; row < rowCount; row++) { estimates[row] = ...; }
        var breakLabel = Expression.Label("loopEnd");

        var loop = Expression.Block(
          new[] { rowVar },
          Expression.Assign(rowVar, Expression.Constant(0)),
          Expression.Loop(
            Expression.Block(
              Expression.IfThen(
                Expression.GreaterThanOrEqual(rowVar, Expression.Constant(rowCount)),
                Expression.Break(breakLabel)),
              Expression.Assign(
                Expression.ArrayAccess(estimatesParam, rowVar),
                scalarBody),
              Expression.PostIncrementAssign(rowVar)),
            breakLabel));

        var lambda = Expression.Lambda<Action<double[], double[], double[]>>(
          loop, dataParam, paramValuesParam, estimatesParam);
        return lambda.Compile();
      } catch {
        return null;
      }
    }

    // Builds a structural key for the delegate cache.
    // Encodes operator symbols, variable indices, and constant *positions* — but never numeric
    // values — so any program with the same tree topology maps to the same compiled delegate.
    // Format: "{rowCount}|{token},{token},...", where token is:
    //   "v{varIndex}" for a variable, "c" for a constant, the operator symbol for operators.
    private static string GetStructuralKey(RPN<Symbol> p, int rowCount) {
      var sb = new StringBuilder();
      sb.Append(rowCount);
      sb.Append('|');
      for (int i = 0; i < p.Count; i++) {
        if (i > 0) sb.Append(',');
        var sym = p[i];
        if (sym.Type == SymbolType.Variable)
          sb.Append('v').Append(sym.Var.Index);
        else if (sym.Type == SymbolType.Constant)
          sb.Append('c');
        else
          sb.Append(sym.Opr.Symbol);
      }
      return sb.ToString();
    }

    // Extracts current runtime parameter values from a program in the same order that
    // CompileToDelegate assigns paramValues slots: constants first (RPN order), then
    // variable coefficients (RPN order).
    internal static double[] ExtractParameterValues(RPN<Symbol> p) {
      int nConst = 0, nCoef = 0;
      for (int i = 0; i < p.Count; i++) {
        if (p[i].Type == SymbolType.Constant) nConst++;
        else if (p[i].Type == SymbolType.Variable) nCoef++;
      }
      var values = new double[nConst + nCoef];
      int ci = 0, ki = nConst;
      for (int i = 0; i < p.Count; i++) {
        var sym = p[i];
        if (sym.Type == SymbolType.Constant) values[ci++] = sym.Con.Value;
        else if (sym.Type == SymbolType.Variable) values[ki++] = sym.Var.Coefficient;
      }
      return values;
    }

    // Pops operands from the expression stack and returns the combined Expression node.
    // Pop order mirrors EvaluateStack: first Pop() = top of stack = right operand.
    private static Expression? BuildOperatorExpression(Operator opr, Stack<Expression> stack) {
      if (opr.Arity == 1) {
        if (stack.Count < 1) return null;
        Expression arg = stack.Pop();

        return opr.Symbol switch {
          "sin" => Expression.Call(_miSin, arg),
          "cos" => Expression.Call(_miCos, arg),
          "tan" => Expression.Call(_miTan, arg),
          "tanh" => Expression.Call(_miTanh, arg),
          "log" => Expression.Call(_miLog, arg),
          // protected log: value > 0 ? log(value) : 0.0
          "plog" => Expression.Condition(
                      Expression.GreaterThan(arg, Expression.Constant(0.0)),
                      Expression.Call(_miLog, arg),
                      Expression.Constant(0.0)),
          "exp" => Expression.Call(_miExp, arg),
          // protected exp: exp(clamp(value, -100, 100))
          "pexp" => Expression.Call(_miExp,
                      Expression.Call(_miMin,
                        Expression.Call(_miMax, arg, Expression.Constant(-100.0)),
                        Expression.Constant(100.0))),
          "pi" => Expression.Multiply(arg, Expression.Constant(Math.PI)),
          _ => null
        };
      } else if (opr.Arity == 2) {
        if (stack.Count < 2) return null;
        Expression right = stack.Pop(); // top = right operand (matches stack pop order)
        Expression left = stack.Pop();

        return opr.Symbol switch {
          "+" => Expression.Add(left, right),
          "-" => Expression.Subtract(left, right),
          "*" => Expression.Multiply(left, right),
          "/" => Expression.Divide(left, right),
          // protected division: denominator != 0 ? numerator / denominator : 1.0
          "pd" => Expression.Condition(
                    Expression.NotEqual(right, Expression.Constant(0.0)),
                    Expression.Divide(left, right),
                    Expression.Constant(1.0)),
          // analytic quotient: left / sqrt(1 + right²)
          "aq" => Expression.Divide(left,
                    Expression.Call(_miSqrt,
                      Expression.Add(
                        Expression.Constant(1.0),
                        Expression.Multiply(right, right)))),
          _ => null
        };
      }

      return null;
    }

    // deprecated
    //public double EvaluateDict(RPN<Symbol> p, Dictionary<string, double> variableDict, int idx, string targetVariable) {

    //  foreach (var symbol in p) {
    //    if (symbol.Type == SymbolType.Constant) {
    //      evaluationBuffer.Push(symbol.Con.Value);
    //    }
    //    else if (symbol.Type == SymbolType.Variable) {
    //      evaluationBuffer.Push(variableDict[symbol.Var.Name] * symbol.Var.Coefficient);
    //    }
    //    else {
    //      evaluationBuffer.Push(symbol.Opr.Function(evaluationBuffer));
    //      //if (operation.Arity == 1) evaluationBuffer.Push(operation.Function(new[] {evaluationBuffer.Pop()}));
    //      //else evaluationBuffer.Push(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
    //    }
    //  }
    //  var result = evaluationBuffer.Pop();
    //  evaluationBuffer.Clear();
    //  p.TrueResults[idx] = variableDict[targetVariable];
    //  p.EstimatedResults[idx] = result;
    //  return p.TrueResults[idx] - result;
    //}

  }
}
