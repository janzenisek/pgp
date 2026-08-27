namespace PGP.Core {
  public readonly struct Interval : IEquatable<Interval> {
    public double LowerBound { get; }
    public double UpperBound { get; }

    public double Width => UpperBound - LowerBound;

    public Interval(double lowerBound, double upperBound) {
      if (IsAlmost(lowerBound, upperBound)) {
        if (lowerBound <= 0 && upperBound >= 0) {
          lowerBound = 0;
          upperBound = 0;
        } else if (upperBound < 0) {
          lowerBound = upperBound;
        } else {
          upperBound = lowerBound;
        }
      }

      if (lowerBound > upperBound)
        throw new ArgumentException("Lower bound cannot be greater than upper bound.");

      this.LowerBound = lowerBound;
      this.UpperBound = upperBound;
    }

    public bool IsPositive {
      get => LowerBound > 0;
    }

    public bool IsNegative {
      get => UpperBound < 0;
    }

    public bool IsEmpty => double.IsNaN(LowerBound) || double.IsNaN(UpperBound);

    public bool Contains(double value) {
      return LowerBound <= value && value <= UpperBound;
    }

    public bool Contains(Interval other) {
      return LowerBound <= other.LowerBound && UpperBound >= other.UpperBound;
    }

    public override string ToString() {
      return "Interval: [" + LowerBound + ", " + UpperBound + "]";
    }

    private static bool IsAlmost(double a, double b) {
      return Math.Abs(a - b) < 1E-12;
    }

    public static Interval Intersect(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);

      //get thightest inner bounds from intervals
      double maxLower = Math.Max(a.LowerBound, b.LowerBound);
      double minUpper = Math.Min(a.UpperBound, b.UpperBound);

      //if the tightest lower is greater than tightest upper, there is no overlap
      if (maxLower > minUpper)
        return new Interval(double.NaN, double.NaN);

      return new Interval(maxLower, minUpper);
    }

    #region Operations
    public static Interval Add(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);
      return new Interval(Math.BitDecrement(a.LowerBound + b.LowerBound), Math.BitIncrement(a.UpperBound + b.UpperBound));
    }

    public static Interval Subtract(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);
      return new Interval(Math.BitDecrement(a.LowerBound - b.UpperBound), Math.BitIncrement(a.UpperBound - b.LowerBound));
    }

    public static Interval Multiply(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);

      double v1 = a.LowerBound * b.LowerBound;
      double v2 = a.LowerBound * b.UpperBound;
      double v3 = a.UpperBound * b.LowerBound;
      double v4 = a.UpperBound * b.UpperBound;

      double min = Math.BitDecrement(Math.Min(Math.Min(v1, v2), Math.Min(v3, v4)));
      double max = Math.BitIncrement(Math.Max(Math.Max(v1, v2), Math.Max(v3, v4)));

      return new Interval(min, max);
    }

    public static Interval Divide(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);

      // division by [0, 0] results in an empty interval
      if (b.LowerBound == 0.0 && b.UpperBound == 0.0) {
        return new Interval(double.NaN, double.NaN);
      }

      // divisor crosses or touches 0
      if (b.Contains(0.0)) {
        if (b.LowerBound == 0.0) {
          // divisor is [0, x] where x > 0
          double invMin = Math.BitDecrement(1.0 / b.UpperBound);
          return Multiply(a, new Interval(invMin, double.PositiveInfinity));
        } else if (b.UpperBound == 0.0) {
          // divisor is [y, 0] where y < 0
          double invMax = Math.BitIncrement(1.0 / b.LowerBound);
          return Multiply(a, new Interval(double.NegativeInfinity, invMax));
        } else {
          // divisor crosses zero: [-y, x]
          return new Interval(double.NegativeInfinity, double.PositiveInfinity);
        }
      }

      // Normal division if 0 not in divisor interval
      double safeInvMin = Math.BitDecrement(1.0 / b.UpperBound);
      double safeInvMax = Math.BitIncrement(1.0 / b.LowerBound);
      return Multiply(a, new Interval(safeInvMin, safeInvMax));
    }

    public static Interval SquareRoot(Interval a) {
      if (a.IsEmpty || a.UpperBound < 0) return new Interval(double.NaN, double.NaN);

      // Cut negative domain (Julia's _cut_negative_domain strategy)
      double lo = a.LowerBound < 0 ? 0.0 : a.LowerBound;
      return new Interval(Math.BitDecrement(Math.Sqrt(lo)), Math.BitIncrement(Math.Sqrt(a.UpperBound)));
    }

    public static Interval Sine(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);

      double width = a.Width;
      // if full cycle => [-1, 1]
      if (width >= 2 * Math.PI) return new Interval(-1.0, 1.0);

      double lo = a.LowerBound;
      double hi = a.UpperBound;

      int loQuad = GetQuadrant(lo);
      int hiQuad = GetQuadrant(hi);

      if (loQuad == hiQuad) {
        if (width >= Math.PI) return new Interval(-1.0, 1.0);

        // q1 and q2 => sine is decreasing
        if (loQuad == 1 || loQuad == 2) {
            return new Interval(Math.BitDecrement(Math.Sin(hi)), Math.BitIncrement(Math.Sin(lo)));
        }
        // q0 and q3 => sine is increasing
        return new Interval(Math.BitDecrement(Math.Sin(lo)), Math.BitIncrement(Math.Sin(hi)));
      } 
      else if (loQuad == 3 && hiQuad == 0) {
        if (width >= Math.PI) return new Interval(-1.0, 1.0);
        return new Interval(Math.BitDecrement(Math.Sin(lo)), Math.BitIncrement(Math.Sin(hi)));
      } 
      else if (loQuad == 1 && hiQuad == 2) {
        if (width >= Math.PI) return new Interval(-1.0, 1.0);
        return new Interval(Math.BitDecrement(Math.Sin(hi)), Math.BitIncrement(Math.Sin(lo)));
      } 
      else if ((loQuad == 0 || loQuad == 3) && (hiQuad == 1 || hiQuad == 2)) {
        // hits peak => sine = 1
        double minSin = Math.Min(Math.Sin(lo), Math.Sin(hi));
        return new Interval(Math.BitDecrement(minSin), 1.0);
      } 
      else if ((loQuad == 1 || loQuad == 2) && (hiQuad == 3 || hiQuad == 0)) {
        // hit through => sine = -1
        double maxSin = Math.Max(Math.Sin(lo), Math.Sin(hi));
        return new Interval(-1.0, Math.BitIncrement(maxSin));
      } 
      else {
        return new Interval(-1.0, 1.0);
      }
    }

    private static int GetQuadrant(double val) {
      double q = (val / (Math.PI / 2.0)) % 4.0;
      if (q < 0) q += 4.0;
      return (int)Math.Floor(q);
    }

    public static Interval Cosine(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      return Interval.Sine(Interval.Add(a, new Interval(Math.PI / 2, Math.PI / 2)));
    }

    public static Interval Tangens(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      return Interval.Divide(Interval.Sine(a), Interval.Cosine(a));
    }

    public static Interval Logarithm(Interval a) {
      if (a.IsEmpty || a.UpperBound <= 0.0) return new Interval(double.NaN, double.NaN);

      // If lowerbound is negative, cut domain to 0
      double lo = a.LowerBound < 0 ? 0.0 : a.LowerBound;
      double lowerResult = lo == 0.0 ? double.NegativeInfinity : Math.BitDecrement(Math.Log(lo));

      return new Interval(lowerResult, Math.BitIncrement(Math.Log(a.UpperBound)));
    }

    public static Interval Exponential(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      return new Interval(Math.BitDecrement(Math.Exp(a.LowerBound)), Math.BitIncrement(Math.Exp(a.UpperBound)));
    }

    public static Interval Square(Interval a) {
      return Power(a, 2);
    }

    public static Interval Cube(Interval a) {
      return Power(a, 3);
    }

    public static Interval Power(Interval a, int b) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      if (b < 0) return Power(Interval.Divide(new Interval(1.0, 1.0), a), -b); 
      if (b == 0) return new Interval(1.0, 1.0); 
      if (b == 1) return a;

      if (b % 2 == 0) {
        if (a.UpperBound <= 0) return new Interval(Math.BitDecrement(Math.Pow(a.UpperBound, b)), Math.BitIncrement(Math.Pow(a.LowerBound, b)));
        if (a.LowerBound >= 0) return new Interval(Math.BitDecrement(Math.Pow(a.LowerBound, b)), Math.BitIncrement(Math.Pow(a.UpperBound, b)));
        return new Interval(0, Math.BitIncrement(Math.Max(Math.Pow(a.LowerBound, b), Math.Pow(a.UpperBound, b))));
      } else {
        return new Interval(Math.BitDecrement(Math.Pow(a.LowerBound, b)), Math.BitIncrement(Math.Pow(a.UpperBound, b)));
      }
    }

    public static Interval CubicRoot(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      var lower = (a.LowerBound < 0) ? -Math.Pow(-a.LowerBound, 1d / 3d) : Math.Pow(a.LowerBound, 1d / 3d);
      var upper = (a.UpperBound < 0) ? -Math.Pow(-a.UpperBound, 1d / 3d) : Math.Pow(a.UpperBound, 1d / 3d);
      return new Interval(Math.BitDecrement(lower), Math.BitIncrement(upper));
    }

    public static Interval Absolute(Interval a) {
      if (a.IsEmpty) return new Interval(double.NaN, double.NaN);
      var absLower = Math.Abs(a.LowerBound);
      var absUpper = Math.Abs(a.UpperBound);
      var min = Math.Min(absLower, absUpper);
      var max = Math.Max(absLower, absUpper);

      if (a.Contains(0.0)) {
        min = 0.0;
      }
      return new Interval(Math.BitDecrement(min), Math.BitIncrement(max));
    }

    public static Interval AnalyticQuotient(Interval a, Interval b) {
      if (a.IsEmpty || b.IsEmpty) return new Interval(double.NaN, double.NaN);

      var dividend = a;
      var divisorSquared = Square(b);
      var divisor = Add(divisorSquared, new Interval(1.0, 1.0));
      divisor = SquareRoot(divisor);
      var result = Divide(dividend, divisor);

      return result;
    }
    #endregion

    #region Arithmetic Overloads
    public static Interval operator +(Interval a, Interval b) => Add(a, b);
    public static Interval operator -(Interval a, Interval b) => Subtract(a, b);
    public static Interval operator *(Interval a, Interval b) => Multiply(a, b);
    public static Interval operator /(Interval a, Interval b) => Divide(a, b);
    #endregion

    #region Equals and GetHashCode
    public bool Equals(Interval other) {
      return (UpperBound == other.UpperBound || (double.IsNaN(UpperBound) && double.IsNaN(other.UpperBound)))
        && (LowerBound == other.LowerBound || (double.IsNaN(LowerBound) && double.IsNaN(other.LowerBound)));
    }

    public override bool Equals(object obj) {
      return obj is Interval other && Equals(other);
    }

    public static bool operator ==(Interval left, Interval right) {
      return left.Equals(right);
    }

    public static bool operator !=(Interval left, Interval right) {
      return !(left == right);
    }

    public override int GetHashCode() {
      return HashCode.Combine(LowerBound, UpperBound);
    }
    #endregion
  }
}
