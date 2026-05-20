using PGP.Data;
using PGP.Utils;
using PGP.Core;
using System.Diagnostics;

namespace PGP.Runner {
  public class Program {
    public static void Main(string[] args) {
      var fr = new FastRandom();

      // setup sample data set
      //var targetVariable = Resinet_TargetVariable_PvProduction;
      //var inputVariables = InputVariables["Resinet_BasicVariableSet_PvProduction"];
      var targetVariable = GeoTorus_TargetVariable_Volume;
      var inputVariables = InputVariables["GeoTorus_Volume"];
      var allVariables = inputVariables.Append(targetVariable).ToList();
      var variableIndices = allVariables
        .Select((x, i) => new { Item = x, Index = i })
        .ToDictionary(x => x.Item, x => x.Index);
      //Set ds = ProtoDataReader.ReadDataset_Numeric(Datasets["Resinet"], allVariables);
      DataSet ds = ProtoDataReader.ReadDataset_Numeric(Datasets["GeoTorus"], allVariables);
      var dds = ds.GetDoubleSet();
      var variableLimitDict = new Dictionary<string, Tuple<double, double>>();

      foreach (var s in dds) {
        var min = s.Value.Values.Min();
        var max = s.Value.Values.Max();
        variableLimitDict.Add(s.Value.Name, Tuple.Create(min, max));
      }

      // configure data set and modeling task
      DataSet trainingSetOriginalOrder = ds.Subset(0, 100);           
      DataSet trainingSet = trainingSetOriginalOrder.Shuffle(fr);
      ModelingTask modelingTask = new ModelingTask(
        name: "GeoTorus_Volume",
        targetVariable: targetVariable,
        inputVariables: inputVariables,
        metric: Metric.NMSE,
        optimizationDirection: OptimizationDirection.Minimize
      );
      modelingTask.VariableLimitsDict = trainingSetOriginalOrder.GetDoubleSetLimits();

      // configure gp
      var pgp = new PgpAlgorithm(randomNumberGenerator:fr,
        generations:100,
        populationSize:100,
        symbolCount:25,
        nestingDepth:10,
        crossoverRate:0.9,
        mutationRate:0.25,
        maximumSelectionPressure:1000,
        elites:1);

      // configure algorithm
      pgp.LogStatistics = true;
      pgp.UseParallelization = true;
      pgp.PerformCoefficentOptimization = true;
      pgp.PerformConstantOptimization = true;
      pgp.PerformSimplification = true;
      pgp.OptimizationIterations = 10;

      Console.WriteLine("Starting GPSR...");
      Console.WriteLine("(press any key to stop computation)\n");
      
      Stopwatch sw = new Stopwatch();
      sw.Start();
      bool k = false;
      Task t = pgp.Fit(modelingTask, trainingSet);     
      
      while (!k && !t.IsCompleted) {
        k = Console.KeyAvailable;
        t.Wait(100);
      }      

      sw.Stop();

      Console.WriteLine();
      Console.WriteLine($"Evaluations:        {pgp.EvaluationCount}");
      Console.WriteLine($"Runtime:            {(sw.ElapsedMilliseconds / 1000.0):f8} seconds");
      Console.WriteLine($"Time / Evaluation:  {(sw.ElapsedMilliseconds / 1000.0 / pgp.EvaluationCount):f8} seconds");
      Console.WriteLine();
      //Console.WriteLine($"Best Program RPN:   {pgp.BestProgramRPN}");
      Console.WriteLine($"Best Program INF:   {pgp.BestProgram}");
      Console.WriteLine($"Best NMSE:          {pgp.BestProgramNMSE}");
      Console.WriteLine($"Best Pearson R:     {pgp.BestProgramPearsonR}");
      Console.WriteLine($"Best LD:            {pgp.BestProgramLD}");
      Console.WriteLine();
      Console.WriteLine($"Max Pearson R:      {pgp.MaxPearsonR}");
      Console.WriteLine($"Min Length:         {pgp.MinLength}");
      Console.WriteLine($"Min LD:             {pgp.MinLD}");
      Console.WriteLine();
      Console.WriteLine($"Mean Pearson R:     {pgp.MeanPearsonR}");
      Console.WriteLine($"Median Pearson R:   {pgp.MedianPearsonR}");
      Console.WriteLine($"Mean Length:        {pgp.MeanLength}");
      Console.WriteLine($"Median Length:      {pgp.MedianLength}");
      Console.WriteLine($"Mean LD:            {pgp.MeanLD}");
      Console.WriteLine($"Median LD:          {pgp.MedianLD}");

    }

    // =============================================================================================

    // Sample data set "Resinet"

    public static Dictionary<string, string> Datasets = new Dictionary<string, string>()
    {
       { "Resinet", @"..\..\..\sample-data\resinet.csv" }
      ,{ "GeoTorus", @"..\..\..\sample-data\geo-torus.csv" }
      ,{ "SinglePoint", @"..\..\..\sample-data\single-point.csv" }
      ,{ "SomePoints", @"..\..\..\sample-data\some-points.csv" }
    };

    public static Dictionary<string, List<string>> InputVariables = new Dictionary<string, List<string>>()
    {
       { "Resinet_BasicVariableSet_PvProduction", new List<string>() { "globalRadiation", "globalRadiationSum1h", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_BasicVariableSet_PowerConsumption", new List<string>() { "globalRadiation", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_BasicVariableSet_BatterySOC", new List<string>() { "globalRadiation", "globalRadiationSum1h", "globalRadiationSum2h", "globalRadiationSum3h", "globalRadiationSumFrame07to12h", "globalRadiationSumFrame13to24h", "relativeHumidity", "airTemperature", "oneHourPrecipitationSum", "age", "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_PvProduction", new List<string>() { "globalRadiation", "age", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_PowerConsumption", new List<string>() { "dayLength", "hoursAfterSunrise" } }
      ,{ "Resinet_ReducedVariableSet_BatterySOC", new List<string>() { "globalRadiation", "globalRadiationSum2h", "globalRadiationSumFrame07to12h" } }
      ,{ "GeoTorus_Surface", new List<string>() { "r", "R" } }
      ,{ "GeoTorus_Volume", new List<string>() { "r", "R" } }
    };

    public static string Resinet_TargetVariable_PvProduction = "pvProduction";
    public static string Resinet_TargetVariable_PowerConsumption = "powerConsumption";
    public static string Resinet_TargetVariable_BatterySOC = "batterySOC";
    public static string GeoTorus_TargetVariable_Surface = "A";
    public static string GeoTorus_TargetVariable_Volume = "V";
  }
}


