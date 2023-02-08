using System.Collections;
using System.Runtime.InteropServices;

namespace PGP.Utils {
  public static class Extensions {
    public static IEnumerable<T> TakeLast<T>(this IEnumerable<T> source, double R) {
      if (R < 0 || R > 1.0) return null;
      var rn = (int)Math.Floor(source.Count() * R);
      return source.Skip(Math.Max(0, source.Count() - rn));
    }

    public static List<T> Clone<T>(this IEnumerable<T> listToClone) where T : ICloneable {
      return listToClone.Select(item => (T)item.Clone()).ToList();
    }

    public static Dictionary<TKey, TValue> Clone<TKey, TValue>(this Dictionary<TKey, TValue> dictToClone) where TValue : ICloneable {
      //return dictToClone.Select(x => x).ToDictionary(v => (TKey)v.Key.Clone(), v => (TValue)v.Value.Clone());
      var dict = new Dictionary<TKey, TValue>(dictToClone.Count, dictToClone.Comparer);
      foreach (var entry in dictToClone) {
        dict.Add(entry.Key, (TValue)entry.Value.Clone());
      }
      return dict;
    }

    public static Dictionary<TKey, TValue> ShallowClone<TKey, TValue>(this Dictionary<TKey, TValue> dictToClone) {
      //return dictToClone.Select(x => x).ToDictionary(v => (TKey)v.Key.Clone(), v => (TValue)v.Value.Clone());
      var dict = new Dictionary<TKey, TValue>(dictToClone.Count, dictToClone.Comparer);
      foreach (var entry in dictToClone) {
        dict.Add(entry.Key, (TValue)entry.Value);
      }
      return dict;
    }

    public static Dictionary<TKey, TValue> CloneDictionary<TKey, TValue>(this Dictionary<TKey, TValue> dictToClone) where TValue : ICloneable {
      var dict = new Dictionary<TKey, TValue>(dictToClone.Count, dictToClone.Comparer);
      foreach (var entry in dictToClone) {
        dict.Add(entry.Key, (TValue)entry.Value.Clone());
      }
      return dict;
    }

    public static Dictionary<TKey, TValue> ShallowCloneDictionary<TKey, TValue>(this Dictionary<TKey, TValue> dictToClone) {
      var dict = new Dictionary<TKey, TValue>(dictToClone.Count, dictToClone.Comparer);
      foreach (var entry in dictToClone) {
        dict.Add(entry.Key, (TValue)entry.Value);
      }
      return dict;
    }

    public static void SetValues<TKey, TValue>(this Dictionary<TKey, TValue> dict, IEnumerable<TValue> values) {
      var keys = dict.Keys.ToList();
      for (int i = 0; i < dict.Keys.Count; i++) {
        dict[keys[i]] = values.ElementAt(i);
      }
    }

    public static void Shuffle<T>(this IList<T> list, FastRandom fr) {
      int n = list.Count;
      while (n > 1) {
        byte[] box = new byte[1];
        do fr.NextBytes(box);
        while (!(box[0] < n * (Byte.MaxValue / n)));
        int k = (box[0] % n);
        n--;
        T value = list[k];
        list[k] = list[n];
        list[n] = value;
      }
    }

    public static IEnumerable<T> ShuffleFisherYates<T>(this IEnumerable<T> source) {
      return source.ShuffleFisherYates(new FastRandom());
    }

    public static IEnumerable<T> ShuffleFisherYates<T>(this IEnumerable<T> source, FastRandom rng) {
      if (source == null) throw new ArgumentNullException("source");
      if (rng == null) throw new ArgumentNullException("rng");

      return source.ShuffleIterator(rng);
    }

    private static IEnumerable<T> ShuffleIterator<T>(this IEnumerable<T> source, FastRandom rng) {
      var buffer = source.ToList();
      for (int i = 0; i < buffer.Count; i++) {
        int j = rng.Next(i, buffer.Count);
        yield return buffer[j];

        buffer[j] = buffer[i];
      }
    }

    public static IEnumerable<T> SortByIndex<T>(this IEnumerable<T> source, int[] order) {
      var sorted = new List<T>();

      foreach (var i in order) {
        sorted.Add(source.ElementAt(i));
      }
      return sorted;
    }

    public static IList SortByIndex(this IList source, int[] order) {
      //var listType = typeof(List<>);
      //var constructedListType = listType.MakeGenericType(source.GetType());
      IList sorted = (IList)Activator.CreateInstance(source.GetType());

      foreach (var i in order) {
        sorted.Add(source[i]);
      }

      return sorted;
    }

    public static T[] GetRow<T>(this T[,] array, int row) {
      if (!typeof(T).IsPrimitive)
        throw new InvalidOperationException("Not supported for managed types.");

      if (array == null)
        throw new ArgumentNullException("array");

      int cols = array.GetUpperBound(1) + 1;
      T[] result = new T[cols];

      int size;

      if (typeof(T) == typeof(bool))
        size = 1;
      else if (typeof(T) == typeof(char))
        size = 2;
      else
        size = Marshal.SizeOf<T>();

      Buffer.BlockCopy(array, row * cols * size, result, 0, cols * size);

      return result;
    }


    // Iterative, using 'i' as bitmask to choose each combo members
    public static IEnumerable<IEnumerable<T>> AllCombinations<T>(this IEnumerable<T> source, bool empty) {
      var list = source.ToList();
      int combinationCount = (int)Math.Pow(2, list.Count) - 1;
      List<List<T>> result = new List<List<T>>();
      if (empty) result.Add(new List<T>());

      for (int i = 1; i <= combinationCount; i++) {
        // make each combo here
        result.Add(new List<T>());
        for (int j = 0; j < list.Count; j++) {
          if ((i >> j) % 2 != 0)
            result.Last().Add(list[j]);
        }
      }

      return result;
    }
  }

  public static class FastRandomExtesions {
    public static double NextDouble(this FastRandom rnd, double lowerBound, double upperBound) {
      //var half_min = lowerBound / 2.0;
      //var half_max = upperBound / 2.0;
      //var average = half_min + half_max;
      //var factor = upperBound - average;
      //return (2.0 * rnd.NextDouble() - 1.0) * factor + average;

      double average = lowerBound / 2.0 + upperBound / 2.0;
      return (2.0 * rnd.NextDouble() - 1.0) * (upperBound - average) + average;
    }

    public static double NextGaussian_BoxMuller(this FastRandom rnd, double mean = 0.0, double stdDev = 1.0) {
      double u1 = rnd.NextDouble(); // uniform(0,1) random doubles
      double u2 = rnd.NextDouble();
      double rndStdNormal = Math.Cos(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2)); // random normal(0,1)
      return mean + stdDev * rndStdNormal;
    }

    public static double[] NextGaussians_BoxMuller(this FastRandom rnd, double mean = 0.0, double stdDev = 1.0) {
      double u1 = rnd.NextDouble(); // uniform(0,1) random doubles
      double u2 = rnd.NextDouble();
      double rndStdNormal1 = Math.Sin(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2));
      double rndStdNormal2 = Math.Cos(2.0 * Math.PI * u1) * Math.Sqrt(-2.0 * Math.Log(u2));
      return new[] { mean + stdDev * rndStdNormal1, mean + stdDev * rndStdNormal2 };
    }

    public static double NextGaussian_Polar(this FastRandom rnd) {
      double u1 = 0.0, u2 = 0.0, q = 0.0, p = 0.0;

      do {
        u1 = rnd.NextDouble(-1.0, 1.0);
        u2 = rnd.NextDouble(-1.0, 1.0);
        q = u1 * u1 + u2 * u2;
      } while (q == 0.0 || q > 1.0);

      p = Math.Sqrt(-2 * Math.Log(q) / q);
      return u1 * p;
    }

    public static double[] NextGaussians_Polar(this FastRandom rnd) {
      double u1 = 0.0, u2 = 0.0, q = 0.0, p = 0.0;

      do {
        u1 = rnd.NextDouble(-1.0, 1.0);
        u2 = rnd.NextDouble(-1.0, 1.0);
        q = u1 * u1 + u2 * u2;
      } while (q == 0.0 || q > 1.0);

      p = Math.Sqrt(-2 * Math.Log(q) / q);
      return new[] { u1 * p, u2 * p };
    }
  }

  public static class StackExtensions {
    public static Stack<T> Clone1<T>(this Stack<T> original) {
      return new Stack<T>(new Stack<T>(original));
    }

    public static Stack<T> Clone2<T>(this Stack<T> original) {
      return new Stack<T>(original.Reverse());
    }

    public static Stack<T> Clone3<T>(this Stack<T> original) {
      var arr = original.ToArray();
      Array.Reverse(arr);
      return new Stack<T>(arr);
    }

    public static Stack<T> Clone<T>(this Stack<T> original) {
      var arr = new T[original.Count];
      original.CopyTo(arr, 0);
      Array.Reverse(arr);
      return new Stack<T>(arr);
    }

    public static Stack CloneDeep(this Stack original) {
      var arr = new object[original.Count];
      original.CopyTo(arr, 0);
      Array.Reverse(arr);
      return new Stack(arr);
    }
  }
}
