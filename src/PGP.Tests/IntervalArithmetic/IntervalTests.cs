using PGP.Core;

namespace PGP.Tests.IntervalArithmetic {
  public class IntervalTests {
    [Fact]
    public void Creation_ValidBounds_Works() {
      var i = new Interval(-1.0, 5.0);
      Assert.Equal(-1.0, i.LowerBound);
      Assert.Equal(5.0, i.UpperBound);
      Assert.Equal(6.0, i.Width);
      Assert.False(i.IsEmpty);
    }

    [Fact]
    public void Creation_InvalidBounds_Throws() {
      Assert.Throws<ArgumentException>(() => new Interval(5.0, -1.0));
    }

    [Fact]
    public void Contains_ValueInside_ReturnsTrue() {
      var i = new Interval(0.0, 10.0);
      Assert.True(i.Contains(5.0));
      Assert.True(i.Contains(0.0));
      Assert.True(i.Contains(10.0));
    }

    [Fact]
    public void Contains_ValueOutside_ReturnsFalse() {
      var i = new Interval(0.0, 10.0);
      Assert.False(i.Contains(-0.1));
      Assert.False(i.Contains(10.1));
    }

    [Fact]
    public void Contains_Interval_ReturnsCorrectly() {
      var i1 = new Interval(0.0, 10.0);
      var i2 = new Interval(2.0, 8.0);
      var i3 = new Interval(-2.0, 8.0);

      Assert.True(i1.Contains(i2));
      Assert.False(i1.Contains(i3));
    }

    #region Addition Tests
    [Fact]
    public void Core_Addition_BoundsPushedOutward() {
      var a = new Interval(2.0, 3.0);
      var b = new Interval(4.0, 5.0);
      var c = a + b;

      // Mathematical answer is [6, 8]
      // Outward rounding pushes lower bound slightly down and upper bound slightly up
      Assert.True(c.LowerBound < 6.0);
      Assert.Equal(6.0, c.LowerBound, 10); // Checks they are close (precision)

      Assert.True(c.UpperBound > 8.0);
      Assert.Equal(8.0, c.UpperBound, 10);
    }

    [Fact]
    public void Addition_WithEmpty_ReturnsEmpty() {
      var a = new Interval(2.0, 3.0);
      var b = new Interval(double.NaN, double.NaN);
      var c = a + b;

      Assert.True(c.IsEmpty);
    }
    #endregion

    #region Subtraction Tests
    [Fact]
    public void Core_Subtraction_ProperlySubtractsAndBounds() {
      var a = new Interval(2.0, 5.0);
      var b = new Interval(3.0, 7.0);
      var c = a - b;

      // Mathematical answer is [2-7, 5-3] = [-5, 2]
      Assert.True(c.LowerBound < -5.0);
      Assert.Equal(-5.0, c.LowerBound, 10);

      Assert.True(c.UpperBound > 2.0);
      Assert.Equal(2.0, c.UpperBound, 10);
    }
    #endregion

    #region Multiplication Tests
    [Fact]
    public void Core_Multiplication_CrossesZero_CorrectBounds() {
      var a = new Interval(-2.0, 3.0);
      var b = new Interval(-4.0, 5.0);
      var c = a * b;

      // Min crosses: v1=8, v2=-10, v3=-12, v4=15
      // Min: -12, Max: 15
      Assert.True(c.LowerBound < -12.0);
      Assert.Equal(-12.0, c.LowerBound, 10);

      Assert.True(c.UpperBound > 15.0);
      Assert.Equal(15.0, c.UpperBound, 10);
    }
    #endregion

    #region Division Tests
    [Fact]
    public void Division_Normal_CorrectBounds() {
      var a = new Interval(4.0, 8.0);
      var b = new Interval(2.0, 4.0);
      var c = a / b;

      // Math: [4*(1/4), 8*(1/2)] = [1, 4]
      Assert.True(c.LowerBound < 1.0);
      Assert.Equal(1.0, c.LowerBound, 10);

      Assert.True(c.UpperBound > 4.0);
      Assert.Equal(4.0, c.UpperBound, 10);
    }

    [Fact]
    public void Division_ByExactZero_ReturnsEmpty() {
      var a = new Interval(4.0, 8.0);
      var b = new Interval(0.0, 0.0);
      var c = a / b;

      Assert.True(c.IsEmpty);
    }

    [Fact]
    public void Division_CrossesZero_ReturnsInfiniteHull() {
      var a = new Interval(4.0, 8.0);
      var b = new Interval(-2.0, 3.0);
      var c = a / b;

      Assert.Equal(double.NegativeInfinity, c.LowerBound);
      Assert.Equal(double.PositiveInfinity, c.UpperBound);
    }

    [Fact]
    public void Division_TouchesZeroBottom_GoesTowardsPositiveInfinity() {
      var a = new Interval(4.0, 8.0);
      var b = new Interval(0.0, 2.0);
      var c = a / b;

      Assert.Equal(double.PositiveInfinity, c.UpperBound);
      // LowerBound = math approx of 4 / 2 = 2 (pushed outward)
      Assert.True(c.LowerBound < 2.0);
      Assert.Equal(2.0, c.LowerBound, 10);
    }
    #endregion

    #region SquareRoot Tests
    [Fact]
    public void SquareRoot_PositiveDomain_Works() {
      var a = new Interval(4.0, 9.0);
      var c = Interval.SquareRoot(a);

      Assert.True(c.LowerBound < 2.0);
      Assert.Equal(2.0, c.LowerBound, 10);

      Assert.True(c.UpperBound > 3.0);
      Assert.Equal(3.0, c.UpperBound, 10);
    }

    [Fact]
    public void SquareRoot_StrictlyNegativeDomain_ReturnsEmpty() {
      var a = new Interval(-9.0, -4.0);
      var c = Interval.SquareRoot(a);

      Assert.True(c.IsEmpty);
    }

    [Fact]
    public void SquareRoot_CrossingDomain_CutsNegativePart() {
      var a = new Interval(-4.0, 9.0);
      var c = Interval.SquareRoot(a);

      // Math: [0, 3]  (since -4 is cut to 0)
      Assert.True(c.LowerBound < 0.0);
      Assert.Equal(0.0, c.LowerBound, 10);

      Assert.True(c.UpperBound > 3.0);
      Assert.Equal(3.0, c.UpperBound, 10);
    }
    #endregion
  }
}