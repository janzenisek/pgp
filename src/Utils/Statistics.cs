using System.Text;

namespace PGP.Utils
{
  public static class Statistics
  {
    public static int Max(int a, int b) {
      return a > b ? a : b;
    }

    public static int Min(int a, int b) {
      return a < b ? a : b;
    }

    public static double ComputeMean(IEnumerable<double> values) {
      var cleanValues = values.Where(x => !double.IsNaN(x)).ToList();
      if (cleanValues.Count > 0)
      {
        return cleanValues.Average();
      }
      else
      {
        return double.NaN;
      }
    }

    public static double ComputeMedian(IEnumerable<double> values) {
      var cleanValues = values.Where(x => !double.IsNaN(x)).ToList();
      double median = 0.0;
      if (cleanValues.Count > 0)
      {
        cleanValues.Sort();
        if (cleanValues.Count % 2 == 0)
          median = (cleanValues[cleanValues.Count / 2 - 1] + cleanValues[cleanValues.Count / 2]) / 2.0;
        else median = cleanValues[(cleanValues.Count / 2)];
        return median;
      }
      else
      {
        return double.NaN;
      }
    }

    public static double ComputeMean(IEnumerable<int> values) {
      var cleanValues = values.Where(x => !double.IsNaN(x)).ToList();
      if (cleanValues.Count > 0)
      {
        return cleanValues.Average();
      }
      else
      {
        return double.NaN;
      }
    }

    public static double ComputeStandardDeviation(IEnumerable<double> values) {
      var cleanValues = values.Where(x => !double.IsNaN(x)).ToList();
      if (cleanValues.Count > 0)
      {
        double mean = cleanValues.Average();
        return Math.Sqrt(cleanValues.Average(x => Math.Pow(x - mean, 2)));
      }
      else
      {
        return double.NaN;
      }
    }

    public static List<double> ComputeNumericalDifferentiation(IEnumerable<double> values) {
      var valueList = values.Where(x => !double.IsNaN(x)).ToList();
      var diffs = new List<double>();
      for (int i = 1; i < valueList.Count; i++)
      {
        diffs.Add(valueList[i] - valueList[i - 1]);
      }
      return diffs;
    }

    public static IEnumerable<double> NumericalDifferentiation(this IEnumerable<double> source) {
      return ComputeNumericalDifferentiation(source);
    }

    public static double StandardDeviation(this IEnumerable<double> source) {
      return ComputeStandardDeviation(source);
    }

    public static double Mean(this IEnumerable<double> source) {
      return ComputeMean(source);
    }

    public static double Median(this IEnumerable<double> source) {
      return ComputeMedian(source);
    }

    public static double Mean(this IEnumerable<int> source) {
      return ComputeMean(source);
    }

    public static double Median(this IEnumerable<int> source) {
      var cleanValues = source.ToList();
      double median = 0.0;
      if (cleanValues.Count > 0)
      {
        cleanValues.Sort();
        if (cleanValues.Count % 2 == 0)
          median = (cleanValues[cleanValues.Count / 2 - 1] + cleanValues[cleanValues.Count / 2]) / 2.0;
        else median = cleanValues[(cleanValues.Count / 2)];
        return median;
      }
      else
      {
        return double.NaN;
      }
    }

    public static double Q1(this IEnumerable<double> source) {
      double median = source.Median();
      var q1 = source.Where(x => x < median);
      if (q1.Any()) return q1.Median();
      else return median;
    }

    public static double Q3(this IEnumerable<double> source) {
      double median = source.Median();
      var q3 = source.Where(x => x > median);
      if (q3.Any()) return q3.Median();
      else return median;
    }

    public static double IQR(this IEnumerable<double> source) {
      return source.Q3() - source.Q1();
    }

    public static double Covariance(IEnumerable<double> x, IEnumerable<double> y) {      
      var xl = x.Where(_x => !double.IsNaN(_x)).ToList();
      var yl = y.Where(_y => !double.IsNaN(_y)).ToList();

      var xm = xl.Average();
      var ym = yl.Average();

      double sum = 0.0;
      for (int i = 0; i < xl.Count; i++)
      {
        sum += (xl[i] - xm) * (yl[i] - ym);
      }
      return sum / (xl.Count - 1);
    }

    public static IEnumerable<double> GetRanks(IEnumerable<double> x) {
      var sorted = x.ToList();
      sorted.Sort();

      var ranks = new List<double>();
      foreach (var i in x)
      {
        var idx = sorted.FindIndex(ii => ii == i);
        ranks.Add(idx);
      }
      return ranks;
    }

    public static IEnumerable<double> GetRanksDesc(IEnumerable<double> x) {
      var sorted = x.ToList();
      sorted = sorted.OrderByDescending(y => y).ToList();

      var ranks = new List<double>();
      foreach (var i in x)
      {
        var idx = sorted.FindIndex(ii => ii == i);
        ranks.Add(idx);
      }
      return ranks;
    }

    public static double PearsonRFast(IEnumerable<double> x, IEnumerable<double> y) {
      var xl = x.ToList();
      var yl = y.ToList();

      var mx = xl.Average();
      var my = yl.Average();

      double num = 0.0, den = 0.0, den1 = 0.0, den2 = 0.0;
      for (int i = 0; i < xl.Count; i++)
      {
        num += (xl[i] - mx) * (yl[i] - my);
        den1 += (xl[i] - mx) * (xl[i] - mx);
        den2 += (yl[i] - my) * (yl[i] - my);
      }
      den = Math.Sqrt(den1 * den2);

      if(den == 0 || double.IsNaN(den) || double.IsInfinity(den) || double.IsNaN(num) || double.IsInfinity(num))
      {
        //Console.WriteLine($"{num}  /  {den}");
        return -1.0;
      }

      return num / den;
    }

    public static double PearsonR(IEnumerable<double> x, IEnumerable<double> y) {
      return Covariance(x, y) / (x.StandardDeviation() * y.StandardDeviation());
    }

    public static double Spearman(IEnumerable<double> x, IEnumerable<double> y) {
      var xr = GetRanks(x);
      var yr = GetRanks(y);

      return Covariance(xr, yr) / (xr.StandardDeviation() * yr.StandardDeviation());
    }

    public static long GetBinomealCoefficient(long N, long K) {
      // This function gets the total number of unique combinations based upon N and K.
      // N is the total number of items.
      // K is the size of the group.
      // Total number of unique combinations = N! / ( K! (N - K)! ).
      // This function is less efficient, but is more likely to not overflow when N and K are large.
      // Taken from:  http://blog.plover.com/math/choose.html
      //
      long r = 1;
      long d;
      if (K > N) return 0;
      for (d = 1; d <= K; d++)
      {
        r *= N--;
        r /= d;
      }
      return r;
    }



    // Feature scaling (https://en.wikipedia.org/wiki/Feature_scaling)

    // min-max scaling or normalization
    public static IEnumerable<double> Normalize(this IEnumerable<double> source) {
      var normalized = new List<double>();

      double min = source.Min();
      double max = source.Max();
      double diff = max - min;

      foreach (var item in source) normalized.Add((item - min) / diff);

      return normalized;
    }

    public static IEnumerable<double> NormalizeMean(this IEnumerable<double> source) {
      var normalized = new List<double>();

      double min = source.Min();
      double max = source.Max();
      double diff = max - min;
      double mean = source.Mean();

      foreach (var item in source) normalized.Add((item - mean) / diff);

      return normalized;
    }

    public static IEnumerable<double> Standardize(this IEnumerable<double> source) {
      var standardized = new List<double>();

      double mean = source.Mean();
      double sd = source.StandardDeviation();

      foreach (var item in source) standardized.Add((item - mean) / sd);

      return standardized;
    }

    public static IEnumerable<double> ScaleToUnitLength(this IEnumerable<double> source) {
      var scaled = new List<double>();

      double sum = source.Sum();
      double factor = 1 / sum;
      foreach (var item in source) scaled.Add(item * factor);

      return scaled;
    }
  }
}
