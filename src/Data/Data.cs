using PGP.Utils;
using System.Collections;

namespace PGP.Data {

  public interface ISource : ICloneable {
    string Id { get; set; }
    string ParentId { get; set; }
  }

  public abstract class Source : ISource {
    public string Id { get; set; }

    public string ParentId { get; set; }

    public Source() { }

    public Source(Source source) {
      Id = source.Id;
      ParentId = source.ParentId;
    }

    public Source(string id, string parentId) {
      Id = id;
      ParentId = parentId;
    }

    public abstract object Clone();
  }

  public class DataSet : Source {

    public string Name { get; set; }
    public string Description { get; set; }
    public string Version { get; set; }

    public int RowCount { get { return Series.Max(x => x.Value.Values.Count); } }    

    public Dictionary<string, ISeries> Series { get; set; }

    public DataSet() {
      Series = new Dictionary<string, ISeries>();
    }

    public DataSet(string id, string parentId) : base(id, parentId) {
      Series = new Dictionary<string, ISeries>();
    }

    public DataSet(DataSet ds) : base(ds) {
      Series = new Dictionary<string, ISeries>();
    }
    public override object Clone() {
      var clone = new DataSet(this);
      clone.Name = Name;
      clone.Description = Description;
      clone.Version = Version;

      clone.Series = new Dictionary<string, ISeries>();
      foreach (var s in Series) {
        clone.Series.Add(s.Key, (ISeries)s.Value.Clone());
      }

      return clone;
    }

    public Dictionary<string, Series<double>> GetDoubleSet() {
      var doubleSet = new Dictionary<string, Series<double>>();

      foreach (var series in Series.Values.Where(x => x.DataType == typeof(double))) {
        doubleSet.Add(series.Name, (Series<double>)Series[series.Name]);
      }

      return doubleSet;
    }

    public Series<double> GetDoubleSeries(string name) {
      Series<double> s = null;
      if (Series.ContainsKey(name)
        && Series[name].DataType == typeof(double)) {
        s = (Series<double>)Series[name];
      }
      return s;
    }

    public Series<string> GetStringSeries(string name) {
      Series<string> s = null;
      if (Series.ContainsKey(name)
        && Series[name].DataType == typeof(string)) {
        s = (Series<string>)Series[name];
      }
      return s;
    }

    public Series<DateTime> GetDateTimeSeries(string name) {
      Series<DateTime> s = null;
      if (Series.ContainsKey(name)
        && Series[name].DataType == typeof(DateTime)) {
        s = (Series<DateTime>)Series[name];
      }
      return s;
    }

    public DataSet Subset(int start, int count) {
      var s = (DataSet)this.Clone();

      foreach (var ser in Series.Keys) {
        if (s.Series[ser].DataType == typeof(double)) s.Series[ser].Values = ((List<double>)s.Series[ser].Values).GetRange(start, count);
        if (s.Series[ser].DataType == typeof(string)) s.Series[ser].Values = ((List<string>)s.Series[ser].Values).GetRange(start, count);
        if (s.Series[ser].DataType == typeof(DateTime)) s.Series[ser].Values = ((List<DateTime>)s.Series[ser].Values).GetRange(start, count);
      }

      return s;
    }

    public DataSet Shuffle(FastRandom fr) {
      var s = (DataSet)this.Clone();

      foreach (var ser in s.Series) ser.Value.Values.Clear();
      var order = Enumerable.Range(0, RowCount).ToList();
      order.ShuffleFisherYates(fr);
      foreach (var ser in Series) {
        for (int i = 0; i < order.Count; i++) {
          s.Series[ser.Key].Values.Add(Series[ser.Key].Values[order[i]]);
        }
      }
      return s;
    }

    public double[] GetArray(List<string> order) {
      var arr = new double[Series.Count * RowCount];
      for (int i = 0; i < order.Count; i++) {
        Series[order[i]].Values.CopyTo(arr, i * RowCount);
      }

      return arr;
    }

    public Dictionary<string, Tuple<double, double>> GetDoubleSetLimits() {
      var dict = new Dictionary<string, Tuple<double, double>>();
      foreach (var s in Series.Values.Where(x => x.DataType == typeof(double))) {
        var doubleSeries = (Series<double>)s;
        var min = doubleSeries.Values.Min();
        var max = doubleSeries.Values.Max();
        dict.Add(doubleSeries.Name, Tuple.Create(min, max));
      }
      return dict;
    }
  }

  public enum SupportedSeriesTypes {
    Double,
    String,
    DateTime
  }

  public interface ISeries : ISource {
    string Name { get; set; }
    IList Values { get; set; }

    Type DataType { get; }

    Type ListType { get; }
  }

  public class Series<T> : Source, ISeries {

    public string Name { get; set; }

    public List<T> Values { get; set; }

    public Type DataType {
      get { return typeof(T); }
    }

    public Type ListType {
      get {
        var listType = typeof(List<>);
        var constructedListType = listType.MakeGenericType(DataType);
        return constructedListType;
      }
    }


    public Series() {
      Values = new List<T>();
    }

    public Series(string id, string parentId) : base(id, parentId) {
      Values = new List<T>();
    }

    public Series(Series<T> series) : base(series) {
      Values = new List<T>();
    }

    IList ISeries.Values {
      get { return Values; }
      set { Values = (List<T>)value; }

    }

    public T this[int idx] {
      get => Values[idx];
      set => Values[idx] = value;
    }

    public override object Clone() {
      var clone = new Series<T>(this);
      clone.Name = Name;

      foreach (var v in Values) {
        clone.Values.Add(v);
      }

      return clone;
    }
  }
}
