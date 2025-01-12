using PGP.Data;
using PGP.Utils;
using PGP.Core;
using System.Diagnostics;

namespace PGP.Runner {
  public class Program {
    public static void Main(string[] args) {
      var fr = new FastRandom();

      // setup sample data set
      var targetVariable = Resinet_TargetVariable_PvProduction;
      var inputVariables = InputVariables["Resinet_BasicVariableSet_PvProduction"];
      var allVariables = inputVariables.Append(targetVariable).ToList();
      var variableIndices = allVariables
        .Select((x, i) => new { Item = x, Index = i })
        .ToDictionary(x => x.Item, x => x.Index);
      Set ds = ProtoDataReader.ReadDataset_Numeric(Datasets["Resinet"], allVariables);
      var dds = ds.GetDoubleSet();
      var variableLimitDict = new Dictionary<string, Tuple<double, double>>();

      foreach (var s in dds) {
        var min = s.Value.Values.Min();
        var max = s.Value.Values.Max();
        variableLimitDict.Add(s.Value.Name, Tuple.Create(min, max));
      }

      Set trainingSetOriginalOrder = ds.Subset(0, 1000);
      Set trainingSet = trainingSetOriginalOrder.Shuffle(fr);

      // configure gp
      var pgp = new PgpAlgorithm(randomNumberGenerator:fr,
        inputVariables:inputVariables,
        targetVariable:targetVariable,
        variableIndices:variableIndices,
        variableLimitsDict:variableLimitDict,
        generations:1000,
        populationSize:100,
        treeLength:100,
        crossoverRate:1,
        mutationRate:0.25,
        maximumSelectionPressure:100,
        elites:1);

      // configure algorithm
      pgp.LogGenerations = true;
      pgp.UseParallelization = true;
      pgp.UseConstantOptimization= true;

      Stopwatch sw = new Stopwatch();
      sw.Start();
      pgp.Fit(trainingSet, true);
      
      sw.Stop();

      Console.WriteLine();
      Console.WriteLine($"Runtime:           {(sw.ElapsedMilliseconds / 1000.0):f8} seconds");
      Console.WriteLine($"Evaluations:       {pgp.EvaluationCount}");
      Console.WriteLine($"Time / Evaluation: {(sw.ElapsedMilliseconds / 1000.0 / pgp.EvaluationCount):f8} seconds");
      Console.WriteLine();
    }

    // =============================================================================================

    // Sample data set "Resinet"

    public static Dictionary<string, string> Datasets = new Dictionary<string, string>()
    {
      { "Resinet", @"..\..\..\sample-data\resinet.csv" }
    };

    public static Dictionary<string, List<string>> InputVariables = new Dictionary<string, List<string>>()
    {
       { "Resinet_BasicVariableSet_PvProduction", new List<string>() { "globalRadiation", "globalRadiationSum1h", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_BasicVariableSet_PowerConsumption", new List<string>() { "globalRadiation", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_BasicVariableSet_BatterySOC", new List<string>() { "globalRadiation", "globalRadiationSum1h", "globalRadiationSum2h", "globalRadiationSum3h", "globalRadiationSumFrame07to12h", "globalRadiationSumFrame13to24h", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_PvProduction", new List<string>() { "globalRadiation", "age", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_PowerConsumption", new List<string>() { "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_BatterySOC", new List<string>() { "globalRadiation", "globalRadiationSum2h", "globalRadiationSumFrame07to12h" } }
    };

    public static string Resinet_TargetVariable_PvProduction = "pvProduction";
    public static string Resinet_TargetVariable_PowerConsumption = "powerConsumption";
    public static string Resinet_TargetVariable_BatterySOC = "batterySOC";
  }
}


