using PGP.Utils;

namespace PGP.Core {

  public interface ITerminal {
    string Name { get; set; }
  }

  public interface INonterminal {
    string Name { get; set; }
  }

  public static class Functions {

    public static Function Addition = new Function(
      x => x.Pop() + x.Pop()
      , 2, "+", "Addition");
    public static Function Subtraction = new Function(
      x => x.Pop() - x.Pop()
      , 2, "-", "Subtraction");
    public static Function Multiplication = new Function(
      x => x.Pop() * x.Pop()
      , 2, "*", "Multiplication");
    public static Function Division = new Function(
      x => x.Pop() / x.Pop() // insecure, possibly delivers NaNs
      , 2, "/", "Division");
    public static Function ProtectedDivision = new Function(
      x => {
        double denominator = x.Pop();
        double numerator = x.Pop();
        return denominator != 0 ? numerator / denominator : 1.0; // protected division
      }
      , 2, "pd", "ProtectedDivision");
    public static Function AnalyticQuotient = new Function(
      x => {
        double denominator = x.Pop();
        double numerator = x.Pop();
        return numerator / Math.Sqrt(1.0 + denominator * denominator); // analytic quotient
      }
      , 2, "aq", "AnalyticQuotient");
    public static Function Sine = new Function(
      x => Math.Sin(x.Pop())
      , 1, "sin", "Sine");
    public static Function Cosine = new Function(
      x => Math.Cos(x.Pop())
      , 1, "cos", "Cosine");
    public static Function Tangent = new Function(
      x => Math.Tan(x.Pop())
      , 1, "tan", "Tangent");
    public static Function HyperbolicTangent = new Function(
      x => Math.Tanh(x.Pop())
      , 1, "tanh", "HyperbolicTangent");
    public static Function Logarithm = new Function(
      x => Math.Log(x.Pop()) // insecure, possibly delivers NaNs      
      , 1, "log", "Logarithm");
    public static Function ProtectedLogarithm = new Function(
      x => {
        double value = x.Pop();
        return value > 0 ? Math.Log(value) : 0.0; // protected logarithm
      }
      , 1, "plog", "ProtectedLogarithm");
    public static Function Exponential = new Function(
      x => Math.Exp(x.Pop())
      , 1, "exp", "Exponential");
    public static Function ProtectedExponential = new Function(
      x => {
        double value = x.Pop();
        return Math.Exp(Math.Min(Math.Max(value, -100), 100)); // protected exponential
      }
      , 1, "pexp", "ProtectedExponential");
    public static Function Pi = new Function(
      x => x.Pop() * Math.PI, 1, "pi", "Pi");

    public static List<Function> All = new() {
      Addition
      ,Subtraction
      ,Multiplication
      ,Division // depr: insecure
      ,ProtectedDivision // depr: impricise
      ,AnalyticQuotient
      ,Sine
      ,Cosine
      ,Tangent
      ,HyperbolicTangent
      ,Logarithm // depr: insecure
      ,ProtectedLogarithm
      ,Exponential // depr: insecure
      ,ProtectedExponential
      ,Pi // currently not in use
    };

    public static Function SelectRandom(PgpAlgorithm pgp) {
      return pgp.SelectedNonterminals.ElementAt(pgp.Rng.Next(pgp.SelectedNonterminals.Count()));
    }

    public static Function SelectRandom(PgpAlgorithm pgp, int arity) {
      var ops = pgp.SelectedNonterminals.Where(x => x.Arity == arity);
      return ops.ElementAt(pgp.Rng.Next(ops.Count()));
    }

    public static Function SelectRandomDifferent(PgpAlgorithm pgp, Function op) {
      var ops = pgp.SelectedNonterminals.Where(x => x.Arity == op.Arity && x != op);
      return ops.ElementAt(pgp.Rng.Next(ops.Count()));
    }
  }

  public class Function : INonterminal {
    public Func<Stack<double>, double> Term { get; set; }
    public int Arity { get; set; }
    public string Symbol { get; set; }
    public string Name { get; set; }

    public Function(Func<Stack<double>, double> term, int arity, string symbol, string name) {
      Term = term;
      Arity = arity;
      Symbol = symbol;
      Name = name;
    }
  }

  public class Variable : ITerminal {
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

  public class Constant : ITerminal {
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
    public Function Opr { get; set; }

    public Symbol(Variable var) { Var = var; Type = SymbolType.Variable; }
    public Symbol(Constant con) { Con = con; Type = SymbolType.Constant; }
    public Symbol(Function opr) { Opr = opr; Type = SymbolType.Operator; }

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
  public enum Terminal { Variable, Constant }
}
