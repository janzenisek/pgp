using System.Collections;

namespace PGP.Core {
  public class RPN<T> : List<T>, ICloneable {
    public double PearsonR { get; set; }
    public double NMSE { get; set; }
    public double MAE { get; set; }
    public double MRE { get; set; }

    public double LD { get; set; } // description length = L(H) + L(D|H) = length of the expression + length of the data given the expression, where the latter is estimated by NMSE

    public double[] EstimatedResults { get; set; }
    public double[] TrueResults { get; set; }


    //public RPN() : base() {
    //  EstimatedResult = new List<double>();
    //  TrueResult = new List<double>();
    //}

    public RPN(int capacity, int initialEvaluationCapacity) : base(capacity) {
      this.Capacity = capacity;
      EstimatedResults = new double[initialEvaluationCapacity];
      TrueResults = new double[initialEvaluationCapacity];
    }

    public RPN(RPN<T> rpn) : base(rpn) {
      this.Capacity = rpn.Capacity;
      EstimatedResults = new double[rpn.EstimatedResults.Length];
      TrueResults = new double[rpn.TrueResults.Length];
      PearsonR = rpn.PearsonR;
      NMSE = rpn.NMSE;
      MAE = rpn.MAE;
      MRE = rpn.MRE;
      LD = rpn.LD;
      Array.Copy(rpn.EstimatedResults, 0, EstimatedResults, 0, EstimatedResults.Length);
      Array.Copy(rpn.TrueResults, 0, TrueResults, 0, TrueResults.Length);
      //int bCount = sizeof(double) * EstimatedResults.Length;
      //System.Buffer.BlockCopy(rpn.EstimatedResults, 0, EstimatedResults, 0, bCount);
      //System.Buffer.BlockCopy(rpn.TrueResults, 0, TrueResults, 0, bCount);
    }

    public RPN(IEnumerable<T> items, int capacity, int initialEvaluationCapacity) : base(items) {
      this.Capacity = capacity;
      EstimatedResults = new double[initialEvaluationCapacity];
      TrueResults = new double[initialEvaluationCapacity];
    }

    public RPN(IEnumerable<T> items, int capacity, double[] estimatedResults, double[] trueResults, double pearsonR, double nmse, double mae, double mre, double ld) : base(items) {
      this.Capacity = capacity;
      EstimatedResults = new double[estimatedResults.Length];
      TrueResults = new double[trueResults.Length];
      PearsonR = pearsonR;
      NMSE = nmse;
      MAE = mae;
      MRE = mre;
      LD = ld;
      Array.Copy(estimatedResults, 0, EstimatedResults, 0, EstimatedResults.Length);
      Array.Copy(trueResults, 0, TrueResults, 0, TrueResults.Length);
    }

    // copying all symbol references, without results
    public object Clone() {
      return new RPN<T>(this, this.Capacity, this.EstimatedResults.Length);
    }

    // copying all symbol references, with results
    public object CloneWithResults() {
      return new RPN<T>(this);
    }

    // copying all symbols, without results
    public RPN<T> CloneDeep() {
      var arr = new T[this.Count];
      this.CopyTo(arr, 0);
      var clone = new RPN<T>(arr, this.Capacity, this.EstimatedResults.Length);
      clone.Capacity = this.Capacity;
      return clone;
    }

    // copying all symbols with results
    public RPN<T> CloneDeepWithResults() {
      var arr = new T[this.Count];
      this.CopyTo(arr, 0);
      var clone = new RPN<T>(arr, this.Capacity, this.EstimatedResults, this.TrueResults, PearsonR, NMSE, MAE, MRE, LD);
      clone.Capacity = this.Capacity;
      return clone;
    }

    public T Peek() {
      return this[0];
    }

    public T Pop() {
      T item = this[0];
      this.RemoveAt(0);
      return item;
    }

    public void Push(T item) {
      this.Insert(0, item);
    }

    public override string ToString() {
      return String.Join(' ', this.Select(x => x.ToString()));
    }

    public string ToInfixString() {
      // (expression, precedence) where higher precedence = binds tighter
      // unary functions get int.MaxValue so they never need extra parens
      static int Precedence(Core.Operator opr) => opr.Symbol switch {
        "+" or "-" => 1,
        "*" or "/" => 2,
        _          => int.MaxValue  // unary: sin, cos, etc.
      };

      // subtraction and division are left-associative, so the right operand
      // needs parentheses when it has equal precedence, e.g. a - (b - c)
      static bool IsLeftAssociative(Core.Operator opr) =>
        opr.Symbol == "-" || opr.Symbol == "/";

      var stack = new Stack<(string Expr, int Prec)>();

      foreach (var item in this) {
        if (item is Core.Symbol symbol) {
          if (symbol.Type == Core.SymbolType.Operator) {
            int arity = symbol.Opr.Arity;
            if (arity == 1) {
              var (expr, _) = stack.Pop();
              stack.Push(($"{symbol.Opr.Symbol}({expr})", int.MaxValue));
            } else {
              int prec = Precedence(symbol.Opr);
              var (rightExpr, rightPrec) = stack.Pop();
              var (leftExpr,  leftPrec)  = stack.Pop();

              // wrap left if its precedence is strictly lower
              string l = leftPrec < prec ? $"({leftExpr})" : leftExpr;

              // wrap right if its precedence is lower, or equal for left-assoc ops
              bool wrapRight = rightPrec < prec ||
                               (rightPrec == prec && IsLeftAssociative(symbol.Opr));
              string r = wrapRight ? $"({rightExpr})" : rightExpr;

              stack.Push(($"{l} {symbol.Opr.Symbol} {r}", prec));
            }
          } else {
            stack.Push((symbol.ToString(), int.MaxValue));
          }
        } else {
          stack.Push((item?.ToString() ?? "", int.MaxValue));
        }
      }

      return stack.Count == 1 ? stack.Pop().Expr : String.Join(' ', stack.Select(x => x.Expr));
    }
  }

  public class RPNCustom<T> : IEnumerable<T>, ICloneable {
    public double PearsonR { get; set; }
    public double NMSE { get; set; }
    public double MAE { get; set; }
    public double MRE { get; set; }

    public double[] EstimatedResults { get; set; }
    public double[] TrueResults { get; set; }

    public T this[int idx] {
      get => arr[idx];
      set => arr[idx] = value;
    }

    private T[] arr;
    private int count;
    private int capacityFactor = 2;

    public RPNCustom(int n, int initialEvaluationCapacity) {
      count = 0;
      arr = new T[n * capacityFactor];
      EstimatedResults = new double[initialEvaluationCapacity];
      TrueResults = new double[initialEvaluationCapacity];
    }

    public RPNCustom(RPNCustom<T> rpn) {
      count = 0;
      arr = new T[rpn.Capacity];
      EstimatedResults = new double[rpn.EstimatedResults.Length];
      TrueResults = new double[rpn.TrueResults.Length];
      Array.Copy(rpn.Values, 0, arr, 0, rpn.Count);
      Array.Copy(rpn.EstimatedResults, 0, EstimatedResults, 0, EstimatedResults.Length);
      Array.Copy(rpn.TrueResults, 0, TrueResults, 0, TrueResults.Length);
    }

    public RPNCustom(T[] items, int capacity, int initialEvaluationCapacity) {
      count = 0;
      arr = new T[capacity];
      Array.Copy(items, 0, arr, 0, items.Length);
      EstimatedResults = new double[initialEvaluationCapacity];
      TrueResults = new double[initialEvaluationCapacity];
    }

    public RPNCustom(T[] items, int initialEvaluationCapacity) {
      count = 0;
      arr = items;
      EstimatedResults = new double[initialEvaluationCapacity];
      TrueResults = new double[initialEvaluationCapacity];
    }

    public RPNCustom(T[] items, int capacity, double[] estimatedResults, double[] trueResults) {
      count = 0;
      arr = new T[capacity];
      EstimatedResults = new double[estimatedResults.Length];
      TrueResults = new double[trueResults.Length];
      Array.Copy(items, 0, arr, 0, items.Length);
      Array.Copy(estimatedResults, 0, EstimatedResults, 0, EstimatedResults.Length);
      Array.Copy(trueResults, 0, TrueResults, 0, TrueResults.Length);
    }

    public RPNCustom(T[] items, double[] estimatedResults, double[] trueResults) {
      count = 0;
      arr = items;
      EstimatedResults = new double[estimatedResults.Length];
      TrueResults = new double[trueResults.Length];
      Array.Copy(estimatedResults, 0, EstimatedResults, 0, EstimatedResults.Length);
      Array.Copy(trueResults, 0, TrueResults, 0, TrueResults.Length);
    }


    // copying all symbol references, without results
    public object Clone() {
      return new RPNCustom<T>(this.Values, EstimatedResults.Length);
    }

    // copying all symbol references, with results
    public object CloneWithResults() {
      return new RPNCustom<T>(this.Values, this.EstimatedResults, this.TrueResults);
    }

    // copying all symbols, without results
    public RPNCustom<T> CloneDeep() {
      return new RPNCustom<T>(this.Values, this.Capacity, this.EstimatedResults.Length);
    }

    // copying all symbolc with results
    public RPNCustom<T> CloneDeepWithResults() {
      return new RPNCustom<T>(this);
    }

    public T Peek() {
      return arr[count - 1];
    }

    public T Pop() {
      return arr[--count];
    }

    public void Push(T item) {

      if (count == arr.Length) {
        Array.Resize(ref arr, arr.Length + 1);
      }
      arr[count] = item;
      count++;
    }

    public void Insert(int index, T item) {
      var newArr = new T[Capacity + 1];
      int originalCount = 0;
      for (int i = 0; i < newArr.Length; i++) {
        if (i == index) newArr[i] = item;
        else { newArr[i] = arr[originalCount]; originalCount++; }
      }
      arr = newArr;
      this.count = newArr.Length;
    }

    public void InsertRange(int index, T[] items) {
      var newArr = new T[Capacity + items.Length];
      int originalCount = 0;
      int newCount = 0;

      for (int i = 0; i < index; i++) {
        newArr[i] = arr[originalCount];
        newCount++;
        originalCount++;
      }
      for (int i = 0; i < items.Length; i++) {
        newArr[newCount] = items[i];
        newCount++;
      }
      for (int i = originalCount; i < arr.Length; i++) {
        newArr[newCount] = arr[originalCount];
        newCount++;
        originalCount++;
      }
      arr = newArr;
      this.count = newArr.Length;
    }

    public T[] GetRange(int start, int count) {
      var newArr = new T[count];
      Array.Copy(arr, start, newArr, 0, count);
      return newArr;
    }

    public void RemoveRange(int start, int count) {
      var newArr = new T[Capacity];
      for (int i = 0; i < arr.Length; i++) {
        if (i < start || i >= start + count) newArr[i] = arr[i];
      }
      arr = newArr;
      this.count = newArr.Length;
    }

    public int FindIndex(Predicate<T> match) {
      for (int i = 0; i < arr.Length; i++) {
        if (match(arr[i])) return i;
      }
      return -1;
    }

    public int FindIndex(int start, Predicate<T> match) {
      for (int i = start; i < arr.Length; i++) {
        if (match(arr[i])) return i;
      }
      return -1;
    }

    public void Clear() {
      count = 0;
    }

    public T[] Values {
      get { return arr; }
    }

    public int Count {
      get { return count; }
    }

    public int Capacity {
      get { return arr.Length; }
    }

    public bool IsEmpty {
      get { return count == 0; }
    }

    public IEnumerator<T> GetEnumerator() {
      return new RPN2Enumerator(this);
    }

    IEnumerator IEnumerable.GetEnumerator() {
      return new RPN2Enumerator(this);
    }

    public class RPN2Enumerator : IEnumerator<T> {
      private int position;
      private RPNCustom<T> stack;

      public RPN2Enumerator(RPNCustom<T> stack) {
        this.stack = stack;
        position = -1;
      }

      Object IEnumerator.Current {
        get {
          return stack.arr[position];
        }
      }
      public T Current {
        get {
          return stack.arr[position];

        }
      }

      public void Dispose() {

      }

      public bool MoveNext() {
        position++;
        return position < stack.Count;
      }

      public void Reset() {
        position = -1;
      }
    }
  }
}
