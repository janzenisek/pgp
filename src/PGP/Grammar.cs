using PGP.Utils;

namespace PGP.Core {
  public static class Operators {
    //public static Operator Addition = new Operator(new(x => x[0] + x[1]), 2);
    //public static Operator Subtraction = new Operator(x => x[0] - x[1], 2);
    //public static Operator Multiplication = new Operator(x => x[0] * x[1], 2);
    //public static Operator Division = new Operator(x => x[0] / x[1], 2);
    //public static Operator Sine = new Operator(x => Math.Sin(x[0]), 1);
    //public static Operator Cosine = new Operator(x => Math.Cos(x[0]), 1);
    //public static Operator Tangent = new Operator(x => Math.Tan(x[0]), 1);
    //public static Operator HyperbolicTangent = new Operator(x => Math.Tanh(x[0]), 1);
    //public static Operator Logarithm = new Operator(x => Math.Log(x[0]), 1);
    //public static Operator Exponential = new Operator(x => Math.Exp(x[0]), 1);

    public static Operator Addition = new Operator(
      x => x.Pop() + x.Pop()
      , 2, "+", "Addition");
    public static Operator Subtraction = new Operator(
      x => x.Pop() - x.Pop()
      , 2, "-", "Subtraction");
    public static Operator Multiplication = new Operator(
      x => x.Pop() * x.Pop()
      , 2, "*", "Multiplication");
    public static Operator Division = new Operator(
      x => x.Pop() / x.Pop() // insecure, possibly delivers NaNs
      , 2, "/", "Division");
    public static Operator ProtectedDivision = new Operator(
      x => {
        double denominator = x.Pop();
        double numerator = x.Pop();
        return denominator != 0 ? numerator / denominator : 1.0; // protected division
      }
      , 2, "pd", "ProtectedDivision");
    public static Operator AnalyticQuotient = new Operator(
      x => {
        double denominator = x.Pop();
        double numerator = x.Pop();
        return numerator / Math.Sqrt(1.0 + denominator * denominator); // analytic quotient
      }
      , 2, "aq", "AnalyticQuotient");
    public static Operator Sine = new Operator(
      x => Math.Sin(x.Pop())
      , 1, "sin", "Sine");
    public static Operator Cosine = new Operator(
      x => Math.Cos(x.Pop())
      , 1, "cos", "Cosine");
    public static Operator Tangent = new Operator(
      x => Math.Tan(x.Pop())
      , 1, "tan", "Tangent");
    public static Operator HyperbolicTangent = new Operator(
      x => Math.Tanh(x.Pop())
      , 1, "tanh", "HyperbolicTangent");
    public static Operator Logarithm = new Operator(
      x => Math.Log(x.Pop()) // insecure, possibly delivers NaNs      
      , 1, "log", "Logarithm");
    public static Operator ProtectedLogarithm = new Operator(
      x => {
        double value = x.Pop();
        return value > 0 ? Math.Log(value) : 0.0; // protected logarithm
      }
      , 1, "plog", "ProtectedLogarithm");
    public static Operator Exponential = new Operator(
      x => Math.Exp(x.Pop())
      , 1, "exp", "Exponential");
    public static Operator ProtectedExponential = new Operator(
      x => {
        double value = x.Pop();
        return Math.Exp(Math.Min(Math.Max(value, -100), 100)); // protected exponential
      }
      , 1, "pexp", "ProtectedExponential");
    public static Operator Pi = new Operator(
      x => x.Pop() * Math.PI, 1, "pi", "Pi");

    public static List<Operator> All = new() {
      Addition
      ,Subtraction
      ,Multiplication
      //,Division // depr: insecure
      //,ProtectedDivision // depr: impricise
      ,AnalyticQuotient
      ,Sine
      ,Cosine
      ,Tangent
      ,HyperbolicTangent
      //,Logarithm // depr: insecure
      ,ProtectedLogarithm
      //,Exponential // depr: insecure
      ,ProtectedExponential
      //,Pi // currently not in use
    };

    public static Operator SelectRandom(FastRandom rng) {
      return All.ElementAt(rng.Next(All.Count()));
    }

    public static Operator SelectRandom(FastRandom rng, int arity) {
      var ops = All.Where(x => x.Arity == arity);
      return ops.ElementAt(rng.Next(ops.Count()));
    }

    public static Operator SelectRandomDifferent(FastRandom rng, Operator op) {
      var ops = All.Where(x => x.Arity == op.Arity && x != op);
      return ops.ElementAt(rng.Next(ops.Count()));
    }
  }

  public class Operator {
    public Func<Stack<double>, double> Function { get; set; }
    public int Arity { get; set; }
    public string Symbol { get; set; }
    public string Name { get; set; }

    public Operator(Func<Stack<double>, double> function, int arity, string symbol, string name) {
      Function = function;
      Arity = arity;
      Symbol = symbol;
      Name = name;
    }
  }

  public class Variable {
    public string Name { get; set; }
    public int Index { get; set; }
    public double Coefficient { get; set; }

    public Variable(string name, int index, double coefficient) {
      Name = name;
      Index = index;
      Coefficient = coefficient;
    }

    public Variable Clone() => new Variable(Name, Index, Coefficient);
  }

  public class Constant {
    public string Name { get; set; }
    public double Value { get; set; }

    public Constant(string name, double value) {
      Name = name;
      Value = value;
    }

    public Constant Clone() => new Constant(Name, Value);
  }

  // base type node
  // clean, but slow (?)
  //public class Node
  //{
  //  //public string Name { get; set; }
  //  public Node() { }
  //  //public Node(string name) { Name = name; }
  //}

  // composition node
  // not clean, but fast (?)
  public class Symbol {
    public SymbolType Type { get; set; }
    public Variable Var { get; set; }
    public Constant Con { get; set; }
    public Operator Opr { get; set; }

    public Symbol(Variable var) { Var = var; Type = SymbolType.Variable; }
    public Symbol(Constant con) { Con = con; Type = SymbolType.Constant; }
    public Symbol(Operator opr) { Opr = opr; Type = SymbolType.Operator; }

    // Operators are stateless (immutable lambdas) and shared by design — only
    // Variable and Constant carry mutable state and must be deep-copied.
    public Symbol Clone() => Type switch {
      SymbolType.Variable => new Symbol(Var.Clone()),
      SymbolType.Constant => new Symbol(Con.Clone()),
      _                   => new Symbol(Opr)   // Operator is immutable, safe to share
    };

    public override string ToString() {
      return Type == SymbolType.Variable ? Var.Coefficient != 1.0 ? $"{Var.Name}*{Var.Coefficient:f2}" : $"{Var.Name}" : Type == SymbolType.Constant ? $"{Con.Value:f2}" : Opr.Symbol;
      //return Type == SymbolType.Variable ? "v" : Type == SymbolType.Constant ? "c" : "op" + Opr.Arity;
    }

    public string ToReadableString() {
      return Type == SymbolType.Variable ? Var.Name : Type == SymbolType.Constant ? Con.Name : Opr.Symbol;
    }
  }

  public enum SymbolType { Variable, Constant, Operator }
}
