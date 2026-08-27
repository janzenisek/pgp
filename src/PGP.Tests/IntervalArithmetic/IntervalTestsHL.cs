using System;
using System.Collections.Generic;
using Xunit;
using PGP.Core;

namespace PGP.Tests.IntervalArithmetic {
  public class IntervalTestsHL {
    private readonly Interval a = new Interval(-1, 1);
    private readonly Interval b = new Interval(-2, 2);
    private readonly Interval c = new Interval(0, 3);
    private readonly Interval d = new Interval(1, 3);
    private readonly Interval e = new Interval(4, 6);

    private void CheckLowerAndUpperBoundOfInterval(Interval expected, Interval calculated) {
      var lowerBoundExpected = expected.LowerBound;
      var upperBoundExpected = expected.UpperBound;
      var lowerBoundCalculated = calculated.LowerBound;
      var upperBoundCalculated = calculated.UpperBound;

      if(double.IsNaN(lowerBoundExpected) && double.IsNaN(lowerBoundCalculated)) {
        Assert.True(double.IsNaN(lowerBoundExpected) && double.IsNaN(lowerBoundCalculated));
      } else if (double.IsNaN(upperBoundExpected) && double.IsNaN(upperBoundCalculated)) {
        Assert.True(double.IsNaN(upperBoundExpected) && double.IsNaN(upperBoundCalculated));
      } else {
        Assert.Equal(lowerBoundExpected, lowerBoundCalculated, 9);
        Assert.Equal(upperBoundExpected, upperBoundCalculated, 9);
      } 
    }

    private bool IsInfiniteOrUndefined(Interval i) {
      return double.IsInfinity(i.LowerBound) || double.IsInfinity(i.UpperBound) ||
             double.IsNaN(i.LowerBound) || double.IsNaN(i.UpperBound);
    }

    [Fact]
    public void AddIntervalTest() {
      //add        [x1,x2] + [y1,y2] = [x1 + y1,x2 + y2]

      // [-1,1] + [-2,2] = [-3,3]
      CheckLowerAndUpperBoundOfInterval(new Interval(-3, 3), Interval.Add(a, b));
      //([-1, 1] + [-2, 2]) + [0, 3] = [-3, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(-3, 6), Interval.Add(Interval.Add(a, b), c));
      //([-1, 1] + [0, 3]) + [-2, 2] = [-3, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(-3, 6), Interval.Add(Interval.Add(a, c), b));
    }

    [Fact]
    public void SubtractIntervalTest() {
      //subtract   [x1,x2] − [y1,y2] = [x1 − y2,x2 − y1]

      //[-1, 1] - [-2, 2] = [-3, 3]
      CheckLowerAndUpperBoundOfInterval(new Interval(-3, 3), Interval.Subtract(a, b));
      //([-1, 1] - [-2, 2]) - [0, 3] = [-6, 3]
      CheckLowerAndUpperBoundOfInterval(new Interval(-6, 3), Interval.Subtract(Interval.Subtract(a, b), c));
      //([-1, 1] - [0, 3]) - [-2, 2] = [-6, 3]
      CheckLowerAndUpperBoundOfInterval(new Interval(-6, 3), Interval.Subtract(Interval.Subtract(a, c), b));
    }

    [Fact]
    public void MultiplyIntervalTest() {
      //multiply   [x1,x2] * [y1,y2] = [min(x1*y1,x1*y2,x2*y1,x2*y2),max(x1*y1,x1*y2,x2*y1,x2*y2)]

      //[-1, 1] * [-2, 2] = [-2, 2]
      CheckLowerAndUpperBoundOfInterval(new Interval(-2, 2), Interval.Multiply(a, b));
      //([-1, 1] * [-2, 2]) * [0, 3] = [-6, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(-6, 6), Interval.Multiply(Interval.Multiply(a, b), c));
      //([-1, 1] * [0, 3]) * [-2, 2] = [-6, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(-6, 6), Interval.Multiply(Interval.Multiply(a, c), b));

      // [-2, 0] * [-2, 0]  = [0, 4]
      CheckLowerAndUpperBoundOfInterval(new Interval(0, 4), Interval.Multiply(new Interval(-2, 0), new Interval(-2, 0)));
    }

    [Fact]
    public void DivideIntervalTest() {
      //divide  [x1, x2] / [y1, y2] = [x1, x2] * (1/[y1, y2]), where 1 / [y1,y2] = [1 / y2,1 / y1] if 0 not in [y_1, y_2].

      //[4, 6] / [1, 3] = [4/3, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(4.0 / 3.0, 6), Interval.Divide(e, d));
      //([4, 6] / [1, 3]) / [1, 3] = [4/9, 6]
      CheckLowerAndUpperBoundOfInterval(new Interval(4.0 / 9.0, 6), Interval.Divide(Interval.Divide(e, d), d));
      //[4, 6] / [0, 3] = [4/3, +Inf]
      CheckLowerAndUpperBoundOfInterval(new Interval(4.0 / 3.0, double.PositiveInfinity), Interval.Divide(e, c));
      //[-1, 1] / [0, 3] = [+Inf, -Inf]
      CheckLowerAndUpperBoundOfInterval(new Interval(double.NegativeInfinity, double.PositiveInfinity), Interval.Divide(a, c));

      //Devision by 0 ==> IsInfiniteOrUndefined == true
      Assert.True(IsInfiniteOrUndefined(Interval.Divide(e, c)));
      //Devision by 0 ==> IsInfiniteOrUndefined == true
      Assert.True(IsInfiniteOrUndefined(Interval.Divide(a, c)));
      CheckLowerAndUpperBoundOfInterval(new Interval(double.NegativeInfinity, double.PositiveInfinity), Interval.Divide(d, b));
    }

    [Fact]
    public void SineIntervalTest() {
      //sine depends on interval
      CheckLowerAndUpperBoundOfInterval(new Interval(-1, 1), Interval.Sine(new Interval(0, 2 * Math.PI)));
      CheckLowerAndUpperBoundOfInterval(new Interval(-1, 1), Interval.Sine(new Interval(-1 * Math.PI / 2, Math.PI / 2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(0, 1), Interval.Sine(new Interval(0, Math.PI / 2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(-1, 0), Interval.Sine(new Interval(Math.PI, 3 * Math.PI / 2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(Math.Min(Math.Sin(1), Math.Sin(2)), 1), Interval.Sine(new Interval(1, 2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(Math.Min(Math.Sin(1), Math.Sin(3)), 1), Interval.Sine(new Interval(1, 3)));
      CheckLowerAndUpperBoundOfInterval(new Interval(-1, 1), Interval.Sine(new Interval(Math.PI, 5 * Math.PI / 2)));
    }

    [Fact]
    public void CosineIntervalTest() {
      //Cosine uses sine Interval.Sine(Interval.Subtract(a, new Interval(Math.PI / 2, Math.PI / 2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(-1, 1), Interval.Cosine(new Interval(0, 2 * Math.PI)));
      CheckLowerAndUpperBoundOfInterval(Interval.Cosine(new Interval(Math.PI, 4 * Math.PI / 2)), new Interval(-1, 1));
    }

    [Fact]
    public void LogIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(new Interval(Math.Log(3), Math.Log(5)), Interval.Logarithm(new Interval(3, 5)));
      CheckLowerAndUpperBoundOfInterval(new Interval(Math.Log(0.5), 0), Interval.Logarithm(new Interval(0.5, 1)));

      var result = Interval.Logarithm(new Interval(-1, 5));
      CheckLowerAndUpperBoundOfInterval(new Interval(double.NegativeInfinity, Math.Log(5)), result);
      Assert.True(IsInfiniteOrUndefined(result));
    }

    [Fact]
    public void ExponentialIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(Interval.Exponential(new Interval(0, 1)), new Interval(1, Math.Exp(1)));
    }

    [Fact]
    public void SquareIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(Interval.Square(new Interval(1, 2)), new Interval(1, 4));
      CheckLowerAndUpperBoundOfInterval(Interval.Square(new Interval(-2, -1)), new Interval(1, 4));
      CheckLowerAndUpperBoundOfInterval(Interval.Square(new Interval(-2, 2)), new Interval(0, 4));
    }

    [Fact]
    public void SquarerootIntervalTest() {
      // HL sqrt allowed for negative intervals returning [-2, 2] for sqrt([1, 4]). 
      // The strict continuous hull definition (IEEE 1788 / Julia) onlypositive root: [1, 2].
      CheckLowerAndUpperBoundOfInterval(new Interval(1, 2), Interval.SquareRoot(new Interval(1, 4)));
      CheckLowerAndUpperBoundOfInterval(new Interval(double.NaN, double.NaN), Interval.SquareRoot(new Interval(-4, -1)));
    }

    [Fact]
    public void AnalyticalQuotientIntervalTest() {
      var aPos = new Interval(3, 5);
      var aZero = new Interval(-3, 5);
      var aNeg = new Interval(-5, -3);

      var bPos = new Interval(2, 4);
      var bZero = new Interval(-2, 4);
      var bNeg = new Interval(-4, -2);

      // Second interval goes over zero
      CheckLowerAndUpperBoundOfInterval(new Interval(3.0 / Math.Sqrt(17), 5.0), Interval.AnalyticQuotient(aPos, bZero));
      CheckLowerAndUpperBoundOfInterval(new Interval(-3.0, 5.0), Interval.AnalyticQuotient(aZero, bZero));
      CheckLowerAndUpperBoundOfInterval(new Interval(-5.0, -3.0 / Math.Sqrt(17)), Interval.AnalyticQuotient(aNeg, bZero));

      // Second interval is positive
      CheckLowerAndUpperBoundOfInterval(new Interval(3.0 / Math.Sqrt(17), 5.0 / Math.Sqrt(5)), Interval.AnalyticQuotient(aPos, bPos));
      CheckLowerAndUpperBoundOfInterval(new Interval(-3.0 / Math.Sqrt(5), 5.0 / Math.Sqrt(5)), Interval.AnalyticQuotient(aZero, bPos));
      CheckLowerAndUpperBoundOfInterval(new Interval(-5.0 / Math.Sqrt(5), -3.0 / Math.Sqrt(17)), Interval.AnalyticQuotient(aNeg, bPos));

      // Second interval is negative
      CheckLowerAndUpperBoundOfInterval(new Interval(3.0 / Math.Sqrt(17), 5.0 / Math.Sqrt(5)), Interval.AnalyticQuotient(aPos, bNeg));
      CheckLowerAndUpperBoundOfInterval(new Interval(-3.0 / Math.Sqrt(5), 5.0 / Math.Sqrt(5)), Interval.AnalyticQuotient(aZero, bNeg));
      CheckLowerAndUpperBoundOfInterval(new Interval(-5.0 / Math.Sqrt(5), -3.0 / Math.Sqrt(17)), Interval.AnalyticQuotient(aNeg, bNeg));
    }

    [Fact]
    public void CubeIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(Interval.Cube(new Interval(1, 2)), new Interval(1, 8));
      CheckLowerAndUpperBoundOfInterval(Interval.Cube(new Interval(-2, -1)), new Interval(-8, -1));
      CheckLowerAndUpperBoundOfInterval(Interval.Cube(new Interval(-2, 2)), new Interval(-8, 8));
    }

    [Fact]
    public void CubeRootIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(1, 8)), new Interval(1, 2));
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(-8, -8)), new Interval(-2, -2));
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(-8, 8)), new Interval(-2, 2));

      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(8, 8)), new Interval(2, 2));
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(-6, 8)), new Interval(-Math.Pow(6, 1.0 / 3), 2));
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(8, 8)), new Interval(2, 2));
      CheckLowerAndUpperBoundOfInterval(Interval.CubicRoot(new Interval(-8, 0)), new Interval(-2, 0));
    }

    [Fact]
    public void AbsoluteIntervalTest() {
      CheckLowerAndUpperBoundOfInterval(new Interval(2, 5), Interval.Absolute(new Interval(-5, -2)));
      CheckLowerAndUpperBoundOfInterval(new Interval(2, 5), Interval.Absolute(new Interval(2, 5)));
      CheckLowerAndUpperBoundOfInterval(new Interval(0, 3), Interval.Absolute(new Interval(-3, 0)));
      CheckLowerAndUpperBoundOfInterval(new Interval(0, 5), Interval.Absolute(new Interval(0, 5)));
      CheckLowerAndUpperBoundOfInterval(new Interval(0, 5), Interval.Absolute(new Interval(-2, 5)));
    }

    [Fact]
    public void IsNegativeIntervalTest() {
      Assert.True(new Interval(-2, -1).IsNegative);
      Assert.False(new Interval(-2, 0).IsNegative);
      Assert.False(new Interval(-2, 2).IsNegative);
      Assert.False(new Interval(2, 4).IsNegative);
    }

    [Fact]
    public void IsPositiveIntervalTest() {
      Assert.True(new Interval(3, 5).IsPositive);
      Assert.False(new Interval(0, 5).IsPositive);
      Assert.False(new Interval(-1, 5).IsPositive);
      Assert.False(new Interval(-5, -2).IsPositive);
    }

    [Fact]
    public void IsAlmostIntervalTest() {
      var negativeLowerBound = -2E-13;
      var negativeUpperBound = -1E-13;
      var positiveLowerBound = 3E-13;
      var positiveUpperBound = 5E-13;

      var negativeInterval = new Interval(negativeLowerBound, negativeUpperBound);
      var positiveInterval = new Interval(positiveLowerBound, positiveUpperBound);
      var zeroInterval = new Interval(negativeUpperBound, positiveLowerBound);

      Assert.Equal(negativeUpperBound, negativeInterval.LowerBound);
      Assert.Equal(negativeUpperBound, negativeInterval.UpperBound);
      Assert.Equal(positiveLowerBound, positiveInterval.LowerBound);
      Assert.Equal(positiveLowerBound, positiveInterval.UpperBound);
      Assert.Equal(0, zeroInterval.LowerBound);
      Assert.Equal(0, zeroInterval.UpperBound);
    }

    [Fact]
    public void ContaintsTest() {
      var negativeInterval = new Interval(-10, -5);
      var positiveInterval = new Interval(5, 10);
      var overZeroInterval = new Interval(-5, 5);

      Assert.True(negativeInterval.Contains(new Interval(-9, -7)));
      Assert.False(negativeInterval.Contains(new Interval(-11, -3)));
      Assert.False(negativeInterval.Contains(positiveInterval));
      Assert.False(negativeInterval.Contains(overZeroInterval));
      Assert.True(negativeInterval.Contains(-8));
      Assert.False(negativeInterval.Contains(-12));
      Assert.False(negativeInterval.Contains(0));

      Assert.True(positiveInterval.Contains(new Interval(6, 10)));
      Assert.False(positiveInterval.Contains(new Interval(6, 12)));
      Assert.False(positiveInterval.Contains(negativeInterval));
      Assert.False(positiveInterval.Contains(overZeroInterval));
      Assert.True(positiveInterval.Contains(7));
      Assert.False(positiveInterval.Contains(11));
      Assert.False(positiveInterval.Contains(0));

      Assert.True(overZeroInterval.Contains(new Interval(-3, 3)));
      Assert.True(overZeroInterval.Contains(new Interval(-4, -1)));
      Assert.True(overZeroInterval.Contains(new Interval(1, 5)));
      Assert.False(overZeroInterval.Contains(new Interval(-6, 0)));
      Assert.False(overZeroInterval.Contains(new Interval(0, 6)));
      Assert.False(overZeroInterval.Contains(new Interval(-7, 7)));
      Assert.True(overZeroInterval.Contains(-3));
      Assert.True(overZeroInterval.Contains(0));
      Assert.True(overZeroInterval.Contains(3));
      Assert.False(overZeroInterval.Contains(12));
      Assert.False(overZeroInterval.Contains(-7));
    }
  }
}