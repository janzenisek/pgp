using System.Globalization;

namespace PGP.Data {

  public class Transaction {
    public DateTime Date { get; set; }
    public double Store_Nbr { get; set; }
    public double Transactions { get; set; }

    public Transaction() { }

    public Transaction(DateTime date, double store_nbr, double transaction) {
      Date = date;
      Store_Nbr = store_nbr;
      Transactions = transaction;
    }
  }


  public static class ProtoDataReader {
    public static Set ReadDataset(string filepath, Dictionary<string, Tuple<SupportedSeriesTypes, string>> vardict) {
      var s = new Set(Utils.Misc.GenerateId(10), null);
      s.Name = Path.GetFileNameWithoutExtension(filepath);

      s.Series = new Dictionary<string, ISeries>();

      var lines = File.ReadLines(filepath).ToArray();
      if (lines.Any()) {
        var dict = GenerateVariableDict(lines[0].Split(';'));

        foreach (var name in vardict.Keys) {
          ISeries series = null;

          switch (vardict[name].Item1) {
            case SupportedSeriesTypes.DateTime:
              series = new Series<DateTime>(Utils.Misc.GenerateId(10), s.Id);
              break;
            case SupportedSeriesTypes.String:
              series = new Series<string>(Utils.Misc.GenerateId(10), s.Id);
              break;
            case SupportedSeriesTypes.Double:
              series = new Series<double>(Utils.Misc.GenerateId(10), s.Id);
              break;
          }

          series.Name = name;
          s.Series.Add(name, series);
        }
        for (int i = 1; i < lines.Length; i++) {
          var line = lines[i].Split(';');

          foreach (var name in vardict.Keys) {
            switch (vardict[name].Item1) {
              case SupportedSeriesTypes.DateTime:
                s.Series[name].Values.Add(DateTime.ParseExact(line[dict[name]], vardict[name].Item2, CultureInfo.InvariantCulture));
                break;
              case SupportedSeriesTypes.String:
                s.Series[name].Values.Add(line[dict[name]]);
                break;
              case SupportedSeriesTypes.Double:
                s.Series[name].Values.Add(double.Parse(line[dict[name]]));
                break;
            }
          }

        }
      }
      return s;
    }

    public static Set ReadDataset_Numeric(string filepath, List<string> variables, int? n = null) {
      var s = new Set(Utils.Misc.GenerateId(10), null);
      s.Name = Path.GetFileNameWithoutExtension(filepath);
      s.Series = new Dictionary<string, ISeries>();

      var lines = File.ReadLines(filepath).ToArray();
      if (lines.Any()) {
        var dict = GenerateVariableDict(lines[0].Split(';'));
        if (variables == null || variables.Count == 0) variables = dict.Keys.ToList();

        foreach (var name in variables) {
          var series = new Series<double>(Utils.Misc.GenerateId(10), s.Id);
          series.Name = name;
          s.Series.Add(name, series);
        }
        for (int i = 1; i < lines.Length; i++) {
          var line = lines[i].Split(';');

          foreach (var name in variables) {
            s.Series[name].Values.Add(double.Parse(line[dict[name]]));
          }

        }
      }
      return s;
    }

    private static Dictionary<string, int> GenerateVariableDict(string[] headline) {
      var dict = new Dictionary<string, int>();
      for (int i = 0; i < headline.Length; i++) {
        dict.Add(headline[i].Trim(), i);
      }
      return dict;
    }

    public static void WriteDataset(string filepath, Set ds) {
      using (var sw = new StreamWriter(filepath)) {
        sw.WriteLine(string.Join(";", ds.Series.Values.Select(x => x.Name)));

        int rows = ds.Series.First().Value.Values.Count;
        for (int i = 0; i < rows; i++) {
          sw.WriteLine(string.Join(";", ds.Series.Values.Select(x => x.Values[i])));
        }
      }
    }
  }
}

