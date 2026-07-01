using PGP.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {
  public static class Evaluation {    
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
      // Compile once and cache on the program instance; reuse on every subsequent call
      // for the same individual (e.g. during constant optimisation inner loops).
      // CloneDeep (called after crossover/mutation) nulls the cache automatically.
      p.CompiledDelegate ??= CompileToDelegate(p, data.RowCount);
      if (p.CompiledDelegate == null) return t.Score.GetPessimal();

      int targetIdx = t.VariableIndices[t.TargetVariable];

      for (int i = 0; i < data.RowCount; i++) {
        double estimated = p.CompiledDelegate(data.Data, i);
        if (!double.IsFinite(estimated))
          return t.Score.GetPessimal();

        p.TrueResults[i] = data.Data[targetIdx * data.RowCount + i];
        p.EstimatedResults[i] = estimated;
      }

      p.PearsonR = PearsonR.ComputeScore(p);
      p.NMSE = NMSE.ComputeScore(p);
      p.LD = LD.ComputeScore(p);

      return t.Score.Compute(p);
    }



    // Translates an RPN<Symbol> expression into a compiled Func<double[], int, double>.
    // Parameters of the delegate: data array (column-major, stride = rowCount), row index.
    // Returns null if the program is structurally invalid.
    private static Func<double[], int, double>? CompileToDelegate(RPN<Symbol> p, int rowCount) {
      try {
        // Parameters: data array and row index
        var dataParam = Expression.Parameter(typeof(double[]), "data");
        var rowParam = Expression.Parameter(typeof(int), "rowIdx");

        var stack = new Stack<Expression>();

        foreach (var symbol in p) {
          if (symbol.Type == SymbolType.Constant) {
            // Bake constant value directly as a literal
            stack.Push(Expression.Constant(symbol.Con.Value, typeof(double)));
          } else if (symbol.Type == SymbolType.Variable) {
            // data[varIndex * rowCount + rowIdx]  —  matches EvaluateStack layout
            int stride = symbol.Var.Index * rowCount;
            Expression index = stride == 0
              ? (Expression)rowParam
              : Expression.Add(Expression.Constant(stride), rowParam);
            Expression load = Expression.ArrayIndex(dataParam, index);

            // Apply coefficient if not 1.0
            Expression varExpr = symbol.Var.Coefficient != 1.0
              ? Expression.Multiply(load, Expression.Constant(symbol.Var.Coefficient))
              : load;

            stack.Push(varExpr);
          } else // Operator
            {
            Expression? node = BuildOperatorExpression(symbol.Opr, stack);
            if (node == null) return null;
            stack.Push(node);
          }
        }

        if (stack.Count != 1) return null;

        var body = stack.Pop();
        var lambda = Expression.Lambda<Func<double[], int, double>>(body, dataParam, rowParam);
        return lambda.Compile();
      } catch {
        return null;
      }
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
