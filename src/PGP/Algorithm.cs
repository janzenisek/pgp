using PGP.Data;
using PGP.Utils;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace PGP.Core {

  // TODO:

  // Performance optimizations to consider:
  // - consider using struct for Symbol and RPN to reduce memory pressure and GC overhead
  // - consider more efficient evaluation buffer management to avoid boxing/unboxing and array resizing (e.g. use a fixed-size array and manage a stack pointer manually)
  // - consider more efficient crossover and mutation implementations that operate directly on the RPN data structure without needing to create intermediate lists or clones (e.g. by implementing subtree swapping and in-place mutation methods on RPN)
  // - consider more efficient selection implementations that avoid creating intermediate lists or using LINQ (e.g. by implementing a custom weighted random selection method that operates directly on arrays)
  // - consider more efficient constant optimization implementations that avoid cloning the entire program for each candidate solution (e.g. by implementing an in-place optimization method that modifies the constants directly on the RPN and only clones when a better solution is found)
  // - consider more efficient simplification implementations that avoid cloning the entire program (e.g. by implementing an in-place simplification method that modifies the RPN directly and only clones when a simpler solution is found)
  // - consider more efficient evaluation implementations that avoid boxing/unboxing and array resizing (e.g. by implementing a custom evaluation method that operates directly on the RPN data structure and uses a fixed-size evaluation buffer)
  // - consider using bitshift-based encoding for operators and operands to reduce memory usage and improve cache performance

  // Refactoring ideas:
  // - consider splitting the PgpAlgorithm class into multiple classes or modules based on functionality (e.g. separate classes for selection, crossover, mutation, evaluation, optimization, etc.) to improve code organization and maintainability
  // - consider defining interfaces or abstract base classes for different components (e.g. ISelector, ICrossoverOperator, IMutationOperator, IEvaluator, IOptimizer, etc.) to allow for more flexible and extensible implementations that can be easily swapped or extended without modifying the core algorithm logic
  // - consider defining a configuration class or struct to encapsulate all the algorithm settings and parameters, which can be passed around as a single object instead of having many individual properties on the PgpAlgorithm class
  // - consider defining a statistics class or struct to encapsulate all the statistics and metrics related to the population and best solution, which can be updated and accessed in a more organized way instead of having many individual properties on the PgpAlgorithm class
  // - consider defining a logging or reporting mechanism to handle the logging of statistics and progress during the algorithm run, which can be easily enabled or disabled and can provide more detailed information if needed without cluttering the core algorithm logic with console output statements
  // - Split Mutation operations into different mutation operators (e.g. SubtreeMutation, PointMutation, HoistMutation, etc.) that can be applied with different probabilities and can be easily extended with new mutation types without modifying the core algorithm logic
  // - Synchronize Run() and RunParallel(), currently RunParallel() is more up to date
  // - Synchronize ModelingTask and Score, so that OptimizationDirection is available only once
  // - Rename Score to Result
  // - Rename ModelingTask to Task

  // GP improvements and extensions to consider:
  // - implement LD-based evaluation (unify with PR and NMSE)
  // - implement a NSGA variant
  // - implement a Bezier-based encoding, implement a reversed Casteljau algorithm to search control points from data.
  // - implement a Bezier crossover operator that creates offspring by interpolating between two parents in the program space, which can help to explore the search space more smoothly and potentially find better solutions by combining features of both parents in a more nuanced way than simple subtree swapping


  public class PgpAlgorithm {
    private FastRandom seedRng;
    private ThreadLocal<FastRandom> rng;
    public FastRandom Rng => rng.Value;
    private Stack<double> evaluationBuffer;
    private RPN<Symbol>[] population;

    // MethodInfo cache used by CompileToDelegate to build Math.* call nodes
    private static readonly System.Reflection.MethodInfo _miSin   = typeof(Math).GetMethod(nameof(Math.Sin),   new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miCos   = typeof(Math).GetMethod(nameof(Math.Cos),   new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miTan   = typeof(Math).GetMethod(nameof(Math.Tan),   new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miTanh  = typeof(Math).GetMethod(nameof(Math.Tanh),  new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miLog   = typeof(Math).GetMethod(nameof(Math.Log),   new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miExp   = typeof(Math).GetMethod(nameof(Math.Exp),   new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miSqrt  = typeof(Math).GetMethod(nameof(Math.Sqrt),  new[] { typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miMin   = typeof(Math).GetMethod(nameof(Math.Min),   new[] { typeof(double), typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miMax   = typeof(Math).GetMethod(nameof(Math.Max),   new[] { typeof(double), typeof(double) })!;
    private static readonly System.Reflection.MethodInfo _miIsNaN = typeof(double).GetMethod(nameof(double.IsNaN), new[] { typeof(double) })!;

    private object locker;
    private object bestSolutionLocker;

    private int targetVariableIdx;
    private Dictionary<string, Tuple<double, double>> variableLimitsDict;

    // General
    public Task Task { get; set; }
    public DataSet DataSet { get; set; }
    public Store Store { get; set; }

    
    // GP Settings
    public int Generations { get; set; }
    public int PopulationSize { get; set; }
    public double CrossoverRate { get; set; }
    public double MutationRate { get; set; }
    public double MaximumSelectionPressure { get; set; }
    public int Elites { get; set; }
    public int SymbolCount { get; set; }
    public int NestingDepth { get; set; }
    public double OperatorToOperandRatio { get; set; }


    // GP Operators
    public Func<PgpAlgorithm, RPN<Symbol>> Breed { get; set; } = Creation.BreedConstrained;
    public Func<RPN<Symbol>[], Task, Tuple<RPN<Symbol>, int>> Select { get; set; } = Selection.TournamentSelection;
    public Func<RPN<Symbol>, Task, RPN<Symbol>> Mutate { get; set; }
    public Func<RPN<Symbol>, Task, double> Evaluate { get; set; }


    // Algorithm control settings
    public int EvaluationCount { get; private set; }
    public bool LogStatistics { get; set; } = false;
    public bool UseParallelization { get; set; } = true;
    public bool PerformConstantOptimization { get; set; } = false;
    public bool PerformCoefficentOptimization { get; set; } = false;
    public int OptimizationIterations { get; set; } = 10;
    public bool PerformSimplification { get; set; } = false;
    public bool PerformPenalization { get; set; } = false;
    public bool ComputeScores { get; set; } = false;


    // Statistics
    public double MaxPearsonR => population.Max(x => x.PearsonR);
    public double MinMAE => population.Min(x => x.MAE);
    public double MinNMSE => population.Min(x => x.NMSE);
    public double MinLength => population.Min(x => x.Count);
    public double MinLD => population.Min(x => x.LD);

    public string BestProgram => population.OrderBy(x => OrderByScore(x, Task.Metric)).First().ToInfixString();
    public string BestProgramRPN => population.OrderBy(x => OrderByScore(x, Task.Metric)).First().ToString();
    public double BestProgramPearsonR => population.OrderBy(x => OrderByScore(x, Task.Metric)).First().PearsonR;
    public double BestProgramNMSE => population.OrderBy(x => OrderByScore(x, Task.Metric)).First().NMSE;
    public double BestProgramLD => population.OrderBy(x => OrderByScore(x, Task.Metric)).First().LD;
    public double MeanPearsonR => population.Average(x => x.PearsonR);
    public double MedianPearsonR => population.Select(x => x.PearsonR).Median();
    public double MeanLength => population.Average(x => x.Count);
    public double MedianLength => population.Select(x => x.Count).Median();
    public double MeanLD => population.Average(x => x.LD);
    public double MedianLD => population.Select(x => x.LD).Median();

    private double OrderByScore(RPN<Symbol> p, Metric m = Metric.NMSE) { 
      return m switch {
        Metric.PearsonR => -p.PearsonR, // negate because higher is better
        Metric.NMSE => p.NMSE,
        Metric.MRE => p.MRE,
        Metric.LD => p.LD,
        _ => throw new ArgumentException("Unsupported metric")
      };
    }

    public PgpAlgorithm(FastRandom randomNumberGenerator,
      int generations = 1000, int populationSize = 1000, int symbolCount = 50, int nestingDepth = 10, double crossoverRate = 1.0, double mutationRate = 0.25, double maximumSelectionPressure = 200, int elites = 1) {
      locker = new object();
      bestSolutionLocker = new object();
      evaluationBuffer = new Stack<double>(symbolCount * 2);


      seedRng = randomNumberGenerator;
      rng = new ThreadLocal<FastRandom>(() => { lock (seedRng) { return new FastRandom(seedRng.Next()); } });

      Generations = generations;
      PopulationSize = populationSize;
      CrossoverRate = crossoverRate;
      MutationRate = mutationRate;
      MaximumSelectionPressure = maximumSelectionPressure;
      Elites = elites;
      SymbolCount = symbolCount;
      NestingDepth = nestingDepth;

      Selection.Rng = Rng;      

      UseParallelization = false;
      LogStatistics = false;

      population = new RPN<Symbol>[PopulationSize];
      OperatorToOperandRatio = 1.0 * Operators.All.Count / Operators.All.Sum(x => x.Arity);
    }


    
    #region Fit and Run

    public async System.Threading.Tasks.Task Fit(Task task, DataSet trainingData, CancellationToken ct) {
      // setup alg-internal data representation
      Task = task;
      DataSet = trainingData;
      var data = trainingData.GetArray(task.VariableIndices.Keys.ToList());
      targetVariableIdx = task.VariableIndices[task.TargetVariable];

      // create and evaluate initial population
      await System.Threading.Tasks.Task.Run(() => Initialize(task, trainingData, data));

      if(ct.IsCancellationRequested) return;

      // run main gp loop
      await System.Threading.Tasks.Task.Run(() => {
        if (UseParallelization) RunParallel(task, trainingData, data, ct);
        else Run(task, trainingData, data);
      });
    }

    // create and evaluate initial population
    public void Initialize(Task modelingTask, DataSet trainingData, double[] data) {

      double bestFitScore = modelingTask.Score.GetPessimal();
      RPN<Symbol> bestSolution = null;

      for (int i = 0; i < population.Length;) {
        population[i] = Breed(this);
        double f = EvaluateArr(population[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);

        if (!double.IsNaN(f)) {
          if (modelingTask.Score.IsBetter(f, bestFitScore)) {
            bestSolution = (RPN<Symbol>)population[i].CloneDeepWithResults();
            bestFitScore = f;
          }
          i++;
        }
      }
      population[0] = (RPN<Symbol>)bestSolution.CloneDeepWithResults();
      EvaluationCount = 0;
    }

    public void RunParallel(Task modelingTask, DataSet trainingData, double[] data, CancellationToken ct) {      
      double[] fitScores = GetScores(modelingTask.Metric);
      double[] fitScoresNew = fitScores.ToArray(); // pre-fill so no slot is ever zero     
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      var bestSolution = (RPN<Symbol>)population.First().CloneDeepWithResults(); // take the first (elite or random)
      double bestFitScore = fitScores.First(); // take the first (elite or random)

      double currentSelectionPressure = 0.0;
      int crossoverFailed = 0;

      // tmp stats
      var sizeDiffAfterCrossover = new List<double>();
      var sizeDiffAfterMutation = new List<double>();

      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        if (ct.IsCancellationRequested) return;

        int generationalEvaluationCount = 0;        
        double sumFitScores = modelingTask.Score.GetScoreSum(fitScores);
        var fitScoresList = fitScores.ToList();

        Selection.fitScoreSum = sumFitScores;
        Selection.fitScores = fitScoresList;

        var rangePartitioner = Partitioner.Create(Elites, PopulationSize);
        Parallel.ForEach(rangePartitioner,
          () => 0,
          (range, state, localEvaluationCount) =>
        {
          var localEvaluationBuffer = new Stack<double>(SymbolCount * 2);
          for (int i = range.Item1; i < range.Item2 && currentSelectionPressure < MaximumSelectionPressure;) {

            // select
            var c1Idx = Select(population, modelingTask).Item2;
            var c2Idx = Select(population, modelingTask).Item2;

            var c1 = population[c1Idx];
            var c2 = population[c2Idx];
            var f1 = fitScores[c1Idx];
            var f2 = fitScores[c2Idx];

            // cross
            if (Rng.NextDouble() < CrossoverRate) {
              RPN<Symbol> offspring = Cross(c1, c2);
              //RPN<Symbol> offspring = Cross_Bezier(c1, c2);
              var diff = offspring.Count - (c1.Count + c2.Count) / 2.0;
              sizeDiffAfterCrossover.Add(diff);
              if (offspring == null) {
                offspring = (RPN<Symbol>)c1.CloneDeep(); // safe fallback: use parent
                Interlocked.Increment(ref crossoverFailed);
              }
              populationNew[i] = offspring;
            }

            // mutate
            if (Rng.NextDouble() < MutationRate) {
              populationNew[i] = MutateMultiCase(populationNew[i], modelingTask);
              var diff = populationNew[i].Count - population[i].Count;
              sizeDiffAfterMutation.Add(diff);
            }

            // simplify
            if(PerformSimplification)
              populationNew[i] = Simplify(populationNew[i]);

            // evaluate
            //double f = EvaluateSet(populationNew[i], doubleSet, trainingData.RowCount, TargetVariable);            
            //double f = EvaluateArr(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);
            double f = EvaluateProgram(populationNew[i], data, trainingData.RowCount, modelingTask);
            localEvaluationCount++;

            // constant / coefficient optimization
            if(!double.IsNaN(f) && PerformConstantOptimization && PerformCoefficentOptimization) {              
              var optimizationResult = OptimizeCoefficientsAndConstants(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);
              populationNew[i] = optimizationResult.Item1;
              f = optimizationResult.Item2;
            } else if (!double.IsNaN(f) && PerformConstantOptimization) {
              var optimizationResult = OptimizeConstants(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);
              populationNew[i] = optimizationResult.Item1;
              f = optimizationResult.Item2;
            }

            // penalize
            //f = PenalizePearsonR(populationNew[i]);
            //f = PenalizeNMSE(populationNew[i]);

            // minimum description length
            //f = EvaluateDescriptionLength(populationNew[i]);


            if (!double.IsNaN(f)) {
              //if (f > Math.Min(f1, f2)) { // OS              
              if (modelingTask.Score.IsBetter(f, bestFitScore)) { // f > bestFitScore if Pearson R is used
                lock (bestSolutionLocker) {
                  if (modelingTask.Score.IsBetter(f, bestFitScore)) { // f > bestFitScore if Pearson R is used
                    bestFitScore = f;
                    bestSolution = (RPN<Symbol>)populationNew[i].CloneDeepWithResults();
                  }
                }
              }
              fitScoresNew[i] = f;
              i++;
              //} // OS
            } else {
              Console.WriteLine("Evaluation resulted in NaN.");
              Console.WriteLine(populationNew[i].ToInfixString());
            }

            //lock (locker) currentSelectionPressure = generationalEvaluations / (double)PopulationSize;
          }
          return localEvaluationCount;
        }, (localEvaluationCount) =>
        {
          lock (locker) generationalEvaluationCount += localEvaluationCount;
        });
        // swap 
        var tmpPopulation = population;
        var tmpFitScores = fitScores;
        population = populationNew;
        fitScores = fitScoresNew;
        populationNew = tmpPopulation;
        fitScoresNew = tmpFitScores;

        // keep elite
        EvaluateDescriptionLength(bestSolution);
        populationNew[0] = (RPN<Symbol>)bestSolution.CloneDeepWithResults();
        fitScoresNew[0] = bestFitScore;

        EvaluationCount += generationalEvaluationCount;

        if (LogStatistics) {
          bestSolution.PearsonR = Statistics.PearsonRFast(bestSolution.TrueResults, bestSolution.EstimatedResults);
          bestSolution.NMSE = Statistics.NMSE(bestSolution.TrueResults, bestSolution.EstimatedResults);
          bestSolution.LD = LD.ComputeScore(bestSolution);

          Console.WriteLine($"Gen: {g + 1:d4}, PR: {bestSolution.PearsonR:f4}, NMSE: {bestSolution.NMSE:f4}, LD: {bestSolution.LD:f2}, MeanSize: {population.Select(x => x.Count).Average():f2}, MedSize: {population.Select(x => x.Count).Median():f2}, AfterCross: {sizeDiffAfterCrossover.Mean():f2}, AfterMut: {sizeDiffAfterMutation.Mean():f2}");
          sizeDiffAfterCrossover.Clear();
          sizeDiffAfterMutation.Clear();
        }
      }

      if (LogStatistics) {
        Console.WriteLine();
        Console.WriteLine($"Pearson R: {bestSolution.PearsonR}, NMSE: {bestSolution.NMSE}, LD: {bestSolution.LD}");
        Console.WriteLine($"Crossover Failed: {crossoverFailed}");
      }
    }

    public void Run(Task modelingTask, DataSet trainingData, double[] data) {
      var score = modelingTask.Score;

      double[] fitScores = GetScores(modelingTask.Metric);
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      double[] fitScoresNew = fitScores.ToArray(); // pre-fill so no slot is ever zero
      double bestFitScore = fitScores.Aggregate((a, b) => score.IsBetter(a, b) ? a : b);
      var bestSolution = (RPN<Symbol>)population[Array.IndexOf(fitScores, bestFitScore)].CloneDeepWithResults();

      double currentSelectionPressure = 0.0;

      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        // recompute selection weights each generation from the current population scores
        double sumFitScores = score.GetScoreSum(fitScores);
        var fitScoresList = fitScores.ToList();

        Selection.fitScoreSum = sumFitScores;
        Selection.fitScores = fitScoresList;

        int i = Elites;
        int generationalEvaluationCount = 0;
        do {
          //var c1Idx = SelectProportionalIdx(fitScoresList, sumFitScores, modelingTask);
          //var c2Idx = SelectProportionalIdx(fitScoresList, sumFitScores, modelingTask);
          var c1Idx = Select(population, modelingTask).Item2;
          var c2Idx = Select(population, modelingTask).Item2;
          var c1 = population[c1Idx];
          var c2 = population[c2Idx];

          // crossover — fall back to a parent clone when no operator crosspoint exists
          var result = Cross_Constrained(c1, c2);
          if (result != null) {
            populationNew[i] = score.IsBetter(GetScore(result.Item1, modelingTask.Metric), GetScore(result.Item2, modelingTask.Metric))
                               ? result.Item1 : result.Item2;
          } else {
            populationNew[i] = (RPN<Symbol>)c1.CloneDeep(); // safe fallback: use parent
          }

          if (Rng.NextDouble() < MutationRate) {
            var mutated = MutateMultiCase(populationNew[i], modelingTask);
            // only accept a size-shrinking result if it is not trivially degenerate
            // (size 1 while both parents were larger means the mutation over-pruned)
            if (mutated.Count > 1 || (c1.Count == 1 && c2.Count == 1))
              populationNew[i] = mutated;
          }

          // Evaluate
          double f = EvaluateArr(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);
          generationalEvaluationCount++;

          // perform constant optimization
          if (!double.IsNaN(f) && PerformConstantOptimization) {
            var optimizationResult = OptimizeConstantsViaEvolutionStrategy(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx, modelingTask);
            populationNew[i] = optimizationResult.Item1;
            f = optimizationResult.Item2;
          }

          if (!double.IsNaN(f)) {
            if (score.IsBetter(f, bestFitScore)) {
              bestFitScore = f;
              bestSolution = (RPN<Symbol>)populationNew[i].CloneDeepWithResults();
            }
            fitScoresNew[i] = f;
            i++;
          }

          //currentSelectionPressure = generationalEvaluations / (double)PopulationSize;
        } while (i < populationNew.Length && currentSelectionPressure < MaximumSelectionPressure);

        // swap 
        var tmpPopulation = population;
        var tmpFitScores = fitScores;
        population = populationNew;
        fitScores = fitScoresNew;
        populationNew = tmpPopulation;
        fitScoresNew = tmpFitScores;

        // keep elite (1) — also refresh fitScoresNew so next gen has no stale zeros
        Array.Copy(fitScores, fitScoresNew, PopulationSize);
        populationNew[0] = (RPN<Symbol>)bestSolution.CloneDeepWithResults();
        fitScoresNew[0] = bestFitScore;

        EvaluationCount += generationalEvaluationCount;

        if (LogStatistics) Console.WriteLine($"Generation: {g:d4}, Evaluations: {generationalEvaluationCount:d4}, Selection Pressure: {currentSelectionPressure:f2}, Score: {bestFitScore:f12}");
      }
    }

    #endregion Fit and Run




    #region Crossovers

    // Simple crossover: picks a random operator crosspoint in each parent and swaps
    // the subtrees. No size or depth checks — fast but can produce bloated or
    // degenerate offspring.
    public Tuple<RPN<Symbol>, RPN<Symbol>> Cross_Simple(RPN<Symbol> a, RPN<Symbol> b) {
      int aIdx = Rng.Next(0, a.Count);
      int aUpperBound = a.FindIndex(aIdx, x => x.Type == SymbolType.Operator);
      if (aUpperBound == -1) return null;
      int aLowerBound = FindSubtreeLimit(a, aUpperBound);
      if (aLowerBound == -1) return null;

      int bIdx = Rng.Next(0, b.Count);
      int bUpperBound = b.FindIndex(bIdx, x => x.Type == SymbolType.Operator);
      if (bUpperBound == -1) return null;
      int bLowerBound = FindSubtreeLimit(b, bUpperBound);
      if (bLowerBound == -1) return null;

      var aSubtree = a.GetRange(aLowerBound, aUpperBound - aLowerBound + 1)
                       .Select(s => s.Clone()).ToList();
      var bSubtree = b.GetRange(bLowerBound, bUpperBound - bLowerBound + 1)
                       .Select(s => s.Clone()).ToList();

      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();
      aOffspring.RemoveRange(aLowerBound, aUpperBound - aLowerBound + 1);
      aOffspring.InsertRange(aLowerBound, bSubtree);
      bOffspring.RemoveRange(bLowerBound, bUpperBound - bLowerBound + 1);
      bOffspring.InsertRange(bLowerBound, aSubtree);

      return Tuple.Create(aOffspring, bOffspring);
    }

    // Attempt-based crossover: tries up to maxAttempts times to find a random
    // crosspoint pair whose offspring both satisfy the SymbolCount size bounds.
    // Simpler than the exact enumeration but may fail if compatible pairs are rare.
    public Tuple<RPN<Symbol>, RPN<Symbol>> Cross_AttemptBased(RPN<Symbol> a, RPN<Symbol> b, int maxAttempts = 10) {
      double sizeTolerance = 0.5; // allow offspring up to SymbolCount * (1 ± 0.5)
      int minSize = (int)(SymbolCount * (1.0 - sizeTolerance));
      int maxSize = (int)(SymbolCount * (1.0 + sizeTolerance));

      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        int aIdx = Rng.Next(0, a.Count);
        int aUpperBound = aIdx;
        int aLowerBound = (a[aIdx].Type == SymbolType.Operator)
            ? FindSubtreeLimit(a, aUpperBound)
            : aUpperBound;
        if (aLowerBound == -1) continue;

        int bIdx = Rng.Next(0, b.Count);
        int bUpperBound = bIdx;
        int bLowerBound = (b[bIdx].Type == SymbolType.Operator)
            ? FindSubtreeLimit(b, bUpperBound)
            : bUpperBound;
        if (bLowerBound == -1) continue;

        int aSubtreeSize = aUpperBound - aLowerBound + 1;
        int bSubtreeSize = bUpperBound - bLowerBound + 1;
        int aOffspringSize = a.Count - aSubtreeSize + bSubtreeSize;
        int bOffspringSize = b.Count - bSubtreeSize + aSubtreeSize;

        if (aOffspringSize < minSize || aOffspringSize > maxSize ||
            bOffspringSize < minSize || bOffspringSize > maxSize) continue;

        var aSubtree = a.GetRange(aLowerBound, aSubtreeSize).Select(s => s.Clone()).ToList();
        var bSubtree = b.GetRange(bLowerBound, bSubtreeSize).Select(s => s.Clone()).ToList();

        var aOffspring = a.CloneDeep();
        var bOffspring = b.CloneDeep();
        aOffspring.RemoveRange(aLowerBound, aSubtreeSize);
        aOffspring.InsertRange(aLowerBound, bSubtree);
        bOffspring.RemoveRange(bLowerBound, bSubtreeSize);
        bOffspring.InsertRange(bLowerBound, aSubtree);

        return Tuple.Create(aOffspring, bOffspring);
      }

      return null;
    }

    // Constrained crossover: enumerates all valid subtree pairs across both parents
    // and selects uniformly from those satisfying the SymbolCount (size) and
    // NestingDepth constraints. Returns null only when no valid pair exists.
    public Tuple<RPN<Symbol>, RPN<Symbol>> Cross_Constrained(RPN<Symbol> a, RPN<Symbol> b) {
      // Enumerate every valid subtree in both parents.
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      // Precompute per-span metrics so the inner loop is O(1).
      int[] aSize      = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize      = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] aDepth     = aSpans.Select(s => SubtreeDepth(a, s.lower, s.upper)).ToArray();
      int[] bDepth     = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();
      int[] bAncestors = bSpans.Select(s => AncestorCount(b, s.upper)).ToArray();

      bool checkDepth = NestingDepth > 0;

      // Collect all (ai, bi) pairs that satisfy BOTH constraints for BOTH offspring.
      var compatible = new List<(int ai, int bi)>();
      for (int ai = 0; ai < aSpans.Count; ai++) {
        int aOff_base = a.Count - aSize[ai]; // aOffspringCount = aOff_base + bSize[bi]
        int bOff_base = b.Count + aSize[ai]; // bOffspringCount = bOff_base - bSize[bi]

        for (int bi = 0; bi < bSpans.Count; bi++) {
          // ── size constraint ─────────────────────────────────────────────────
          int aOffCount = aOff_base + bSize[bi];
          int bOffCount = bOff_base - bSize[bi];
          if (aOffCount < 1 || aOffCount > SymbolCount) continue;
          if (bOffCount < 1 || bOffCount > SymbolCount) continue;

          // ── depth constraint ─────────────────────────────────────────────────
          // Inserting b's subtree (depth bDepth[bi]) at a's crosspoint (aAncestors[ai]
          // operators above it) yields a path of length aAncestors[ai] + bDepth[bi].
          if (checkDepth) {
            if (aAncestors[ai] + bDepth[bi] > NestingDepth) continue;
            if (bAncestors[bi] + aDepth[ai] > NestingDepth) continue;
          }

          compatible.Add((ai, bi));
        }
      }

      if (compatible.Count == 0) return null;

      // Pick one compatible pair uniformly at random — no retries needed.
      var (selAi, selBi) = compatible[Rng.Next(compatible.Count)];
      var (aL, aU) = aSpans[selAi];
      var (bL, bU) = bSpans[selBi];

      var aSubtree = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
      var bSubtree = a.GetRange(aL, aSize[selAi]).Select(s => s.Clone()).ToList();

      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();

      aOffspring.RemoveRange(aL, aSize[selAi]);
      aOffspring.InsertRange(aL, aSubtree);
      bOffspring.RemoveRange(bL, bSize[selBi]);
      bOffspring.InsertRange(bL, bSubtree);

      return Tuple.Create(aOffspring, bOffspring);
    }

    // Single-offspring constrained crossover: picks a random valid subtree in `a`
    // (the recipient) and finds all subtrees in `b` whose insertion into `a` at that
    // point satisfies the SymbolCount and NestingDepth constraints for the ONE
    // produced offspring. Because the reciprocal offspring is never required to be
    // valid, the compatible set is always at least as large as Cross_Constrained's,
    // so this version fails far less often.
    public RPN<Symbol> Cross(RPN<Symbol> a, RPN<Symbol> b) {
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      int[] aSize      = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize      = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bDepth     = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();

      bool checkDepth = NestingDepth > 0;

      // Shuffle a's crosspoints so the final pick is uniformly distributed.
      int[] aOrder = Enumerable.Range(0, aSpans.Count).ToArray().ShuffleFisherYates(Rng).ToArray();

      foreach (int ai in aOrder) {
        int aOffBase = a.Count - aSize[ai]; // aOffspring.Count = aOffBase + bSize[bi]

        // Collect all b-subtrees compatible with this a-crosspoint.
        var compatibleB = new List<int>();
        for (int bi = 0; bi < bSpans.Count; bi++) {
          int aOffCount = aOffBase + bSize[bi];
          if (aOffCount < 1 || aOffCount > SymbolCount) continue;
          if (checkDepth && aAncestors[ai] + bDepth[bi] > NestingDepth) continue;
          compatibleB.Add(bi);
        }

        if (compatibleB.Count == 0) continue;

        int selBi = compatibleB[Rng.Next(compatibleB.Count)];
        var (aL, aU) = aSpans[ai];
        var (bL, bU) = bSpans[selBi];

        var bSubtree = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
        var offspring = a.CloneDeep();
        offspring.RemoveRange(aL, aSize[ai]);
        offspring.InsertRange(aL, bSubtree);

        return offspring;
      }

      return null;
    }

    // Returns (lowerBound, upperBound) for every valid subtree in p.
    // Operators yield a multi-node subtree; terminals yield a single-node subtree.
    private List<(int lower, int upper)> GetSubtreeSpans(RPN<Symbol> p) {
      var spans = new List<(int, int)>(p.Count);
      for (int i = 0; i < p.Count; i++) {
        if (p[i].Type == SymbolType.Operator) {
          int lower = FindSubtreeLimit(p, i);
          if (lower != -1) spans.Add((lower, i));
        } else {
          spans.Add((i, i));
        }
      }
      return spans;
    }

    // Depth of the expression tree for the standalone subtree p[start..end].
    // Terminals contribute depth 1; an operator contributes 1 + max(child depths).
    private int SubtreeDepth(RPN<Symbol> p, int start, int end) {
      var stack = new Stack<int>(end - start + 1);
      for (int i = start; i <= end; i++) {
        if (p[i].Type != SymbolType.Operator) {
          stack.Push(1);
        } else {
          int arity = p[i].Opr.Arity;
          int maxChild = 0;
          for (int j = 0; j < arity; j++) maxChild = Math.Max(maxChild, stack.Pop());
          stack.Push(1 + maxChild);
        }
      }
      return stack.Count == 1 ? stack.Peek() : 0;
    }

    // Number of operators in p that are strict ancestors of the node at upperBound.
    // An operator at position j > upperBound is an ancestor iff its subtree
    // start (FindSubtreeLimit) is ≤ upperBound, meaning upperBound lies inside it.
    private int AncestorCount(RPN<Symbol> p, int upperBound) {
      int count = 0;
      for (int j = upperBound + 1; j < p.Count; j++) {
        if (p[j].Type == SymbolType.Operator) {
          int lower = FindSubtreeLimit(p, j);
          if (lower != -1 && lower <= upperBound) count++;
        }
      }
      return count;
    }

    private int FindSubtreeLimit(RPN<Symbol> p, int idx) {
      int limit = -1;
      var op = p[idx].Opr;

      var arityTarget = op.Arity;
      var arityCount = 0;

      for (int i = idx - 1; i >= 0 && limit == -1; i--) {
        if (p[i].Type == SymbolType.Operator) {
          arityTarget += p[i].Opr.Arity - 1;
        }
        else // any terminal
        {
          arityCount++;
        }
        if (arityTarget == arityCount) limit = i;
      }

      return limit;
    }

    // Bézier crossover: interpolates between two parents in program space using a
    // Bézier parameter t ∈ (0,1) that continuously slides the offspring from A (t→0)
    // toward B (t→1). The crossover operates in two coupled phases:
    //
    // Phase 1 — Structural selection
    //   A subtree in A is replaced by a subtree from B (single-offspring, like Cross()).
    //   The selection is NOT uniform: candidate subtree pairs are weighted by how well
    //   their fractional size contribution matches t.  Specifically, a pair (aSubtree,
    //   bSubtree) gets weight  exp(-(ratio - t)² / σ²)  where
    //   ratio = bSize / (aSize + bSize).  At t=0.5 the selection is nearly uniform;
    //   at t→0 it prefers small B-insertions (preserving A's structure); at t→1 it
    //   prefers large B-insertions (replacing most of A with B material).
    //
    // Phase 2 — Numeric Bézier interpolation
    //   After the structural swap every numeric parameter in the offspring (constants
    //   and variable coefficients) is blended with the order-corresponding parameter
    //   from each parent using a quadratic Bézier formula:
    //     v(t) = (1-t)² · vA  +  2t(1-t) · ½(vA+vB)  +  t² · vB
    //   Parameters are matched by occurrence order (i-th numeric in offspring ↔
    //   i-th numeric in A and B), clamped so neither parent runs out of entries.
    //
    // Returns null only when no structurally compatible subtree pair exists.
    public RPN<Symbol> Cross_Bezier(RPN<Symbol> a, RPN<Symbol> b) {
      double t = Rng.NextDouble(); // Bézier parameter ∈ (0,1)

      // ── Phase 1: Bézier-weighted structural selection ───────────────────────
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      int[] aSize      = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize      = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bDepth     = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();

      bool checkDepth = NestingDepth > 0;

      // σ controls how sharply the weight peaks around the target ratio.
      // σ = 0.20 gives a broad window so even at extreme t values there are
      // usually some candidates with non-negligible weight.
      const double sigma2 = 0.20 * 0.20;

      // Shuffle a's crosspoints so the final pick is unbiased across positions.
      int[] aOrder = Enumerable.Range(0, aSpans.Count).ToArray().ShuffleFisherYates(Rng).ToArray();

      foreach (int ai in aOrder) {
        int aOffBase = a.Count - aSize[ai];

        // Collect every valid b-subtree with its Bézier weight.
        var compatibleB = new List<(int bi, double weight)>();
        for (int bi = 0; bi < bSpans.Count; bi++) {
          int aOffCount = aOffBase + bSize[bi];
          if (aOffCount < 1 || aOffCount > SymbolCount) continue;
          if (checkDepth && aAncestors[ai] + bDepth[bi] > NestingDepth) continue;

          // ratio: what fraction of the offspring's total symbol budget B contributes
          double ratio  = (double)bSize[bi] / (aSize[ai] + bSize[bi]);
          double diff   = ratio - t;
          double weight = Math.Exp(-diff * diff / sigma2);
          compatibleB.Add((bi, weight));
        }

        if (compatibleB.Count == 0) continue;

        // Weighted roulette selection among compatible b-subtrees.
        double totalWeight = compatibleB.Sum(x => x.weight);
        double pick = Rng.NextDouble() * totalWeight;
        double cumulative = 0.0;
        int selBi = compatibleB[^1].bi; // default to last as fallback
        foreach (var (bi, w) in compatibleB) {
          cumulative += w;
          if (pick <= cumulative) { selBi = bi; break; }
        }

        var (aL, aU) = aSpans[ai];
        var (bL, bU) = bSpans[selBi];

        var bSubtree  = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
        var offspring = a.CloneDeep();
        offspring.RemoveRange(aL, aSize[ai]);
        offspring.InsertRange(aL, bSubtree);

        // ── Phase 2: quadratic Bézier numeric interpolation ─────────────────
        // Collect all numeric parameters from the offspring, A, and B in
        // program order. Pair by occurrence index; apply the Bézier formula.
        var offNumerics = CollectNumerics(offspring);
        var aNumerics   = CollectNumerics(a);
        var bNumerics   = CollectNumerics(b);

        if (aNumerics.Count > 0 && bNumerics.Count > 0) {
          for (int ni = 0; ni < offNumerics.Count; ni++) {
            // Clamp so we never go out-of-range when parent lengths differ.
            double vA   = aNumerics[Math.Min(ni, aNumerics.Count - 1)].value;
            double vB   = bNumerics[Math.Min(ni, bNumerics.Count - 1)].value;
            double vMid = (vA + vB) * 0.5;

            // Quadratic Bézier:  B(t) = (1-t)²·P0 + 2t(1-t)·P1 + t²·P2
            // P0=vA, P1=vMid (control point at the arithmetic mean), P2=vB
            double blended = (1 - t) * (1 - t) * vA
                           + 2.0 * t * (1 - t) * vMid
                           + t * t * vB;

            var (offIdx, _, isConst) = offNumerics[ni];
            if (isConst)
              offspring[offIdx].Con.Value = blended;
            else
              offspring[offIdx].Var.Coefficient = blended;
          }
        }

        return offspring;
      }

      return null;
    }

    // Returns the ordered list of numeric parameters (constants and variable
    // coefficients) in p together with their RPN index and type.
    private List<(int index, double value, bool isConst)> CollectNumerics(RPN<Symbol> p) {
      var result = new List<(int, double, bool)>(p.Count);
      for (int i = 0; i < p.Count; i++) {
        if (p[i].Type == SymbolType.Constant)
          result.Add((i, p[i].Con.Value, true));
        else if (p[i].Type == SymbolType.Variable)
          result.Add((i, p[i].Var.Coefficient, false));
      }
      return result;
    }

    #endregion Crossovers

    #region Mutators

    public RPN<Symbol> MutateMultiCase(RPN<Symbol> o, Task mt) {
      var p = o.CloneDeep();
      int idx = Rng.Next(0, o.Count); // uniformly distributed

      if (p[idx].Type == SymbolType.Constant) {
        var val = p[idx].Con.Value;
        var ratio = Rng.NextDouble() * 0.1;
        if (Rng.NextDouble() < 0.5) p[idx].Con.Value = val + val * ratio;
        else p[idx].Con.Value = val - val * ratio;
      }
      else if (p[idx].Type == SymbolType.Variable) {
        // grow: wrap this terminal in a new binary operator + a new random terminal
        // (bounded by SymbolCount so the tree can't exceed the size limit)
        if (p.Count + 2 <= SymbolCount && Rng.NextDouble() < 0.25) {
          var binaryOps = Operators.All.Where(op => op.Arity == 2).ToList();
          if (binaryOps.Count > 0) {
            var op = binaryOps[Rng.Next(binaryOps.Count)];
            int ctr = 0;
            var newTerminal = Creation.CreateTerminal(this, ref ctr);
            // insert new terminal before idx, then append operator after idx
            p.Insert(idx, newTerminal);         // new left operand before original terminal
            p.Insert(idx + 2, new Symbol(op));  // operator after the pair
          } else {
            p[idx] = Creation.CreateVariable(this);
          }
        } else {
          p[idx] = Creation.CreateVariable(this);
        }
      }
      else // operator
      {
        var ratio = Rng.NextDouble();
        if (ratio < 0.5) // prune: remove operator and one of its subtrees, keep the other
        {
          if (p[idx].Opr.Arity == 1) {
            p.RemoveAt(idx); // drop unary operator; its single child argument remains
          }
          else if (p[idx].Opr.Arity == 2) {
            int limit = FindSubtreeLimit(p, idx); // start of left child
            // find boundary between left and right child subtrees
            int rightStart = FindSubtreeLimit_Right(p, idx); // start of right child
            if (Rng.NextDouble() < 0.5) {
              // keep right child: remove [limit .. rightStart-1] (left child) and the operator
              int leftLen = rightStart - limit;
              p.RemoveAt(idx);           // remove operator first (end of span)
              p.RemoveRange(limit, leftLen); // remove left subtree
            } else {
              // keep left child: remove [rightStart .. idx-1] (right child) and the operator
              int rightLen = idx - rightStart;
              p.RemoveAt(idx);           // remove operator
              p.RemoveRange(rightStart, rightLen); // remove right subtree
            }
          }
        }
        else // replace operator with a different one of the same arity
        {
          p[idx].Opr = Operators.SelectRandomDifferent(Rng, p[idx].Opr);
        }
      }

      return p;
    }

    // Returns the start index of the RIGHT child subtree of the binary operator at idx.
    // The right child is the subtree immediately before the operator; the left child
    // is everything from FindSubtreeLimit(p, idx) up to rightStart-1.
    private int FindSubtreeLimit_Right(RPN<Symbol> p, int idx) {
      // Walk backward from idx-1 to find the root of the right subtree.
      // The right child's subtree ends at idx-1; its start is FindSubtreeLimit(p, idx-1)
      // if that position is an operator, or idx-1 itself if it is a terminal.
      int rightEnd = idx - 1;
      if (p[rightEnd].Type == SymbolType.Operator) {
        int s = FindSubtreeLimit(p, rightEnd);
        return s >= 0 ? s : rightEnd;
      }
      return rightEnd; // terminal: single-node subtree
    }

    #endregion Mutators
    
    #region Simplifiers

    public RPN<Symbol> Simplify(RPN<Symbol> o) {
      var p = o.CloneDeep();
      bool changed = true;

      while (changed) {
        changed = false;
        for (int i = 0; i < p.Count; i++) {
          if (p[i].Type != SymbolType.Operator) continue;

          var opr = p[i].Opr;

          // ── arity-1 rules ────────────────────────────────────────────────────
          if (opr.Arity == 1) {
            int argEnd   = i - 1;
            int argStart = p[argEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, argEnd) : argEnd;

            // constant folding: f(c) => c'
            if (IsPureConstantSubtree(p, argStart, argEnd)) {
              double val = EvaluateConstantSubtree(p, argStart, i);
              if (!double.IsNaN(val) && !double.IsInfinity(val)) {
                ReplaceWithConstant(p, argStart, i, val);
                changed = true; break;
              }
            }

            // log(exp(x)) => x  |  exp(log(x)) => x
            if ((opr.Symbol == "plog" || opr.Symbol == "log") &&
                p[argEnd].Type == SymbolType.Operator &&
                (p[argEnd].Opr.Symbol == "pexp" || p[argEnd].Opr.Symbol == "exp")) {
              // strip both outer ops: remove [argStart..i] then re-insert inner arg
              int innerArgEnd   = argEnd - 1;
              int innerArgStart = p[innerArgEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, innerArgEnd) : innerArgEnd;
              var innerArg = p.GetRange(innerArgStart, innerArgEnd - innerArgStart + 1).Select(s => s.Clone()).ToList();
              p.RemoveRange(argStart, i - argStart + 1);
              p.InsertRange(argStart, innerArg);
              changed = true; break;
            }
            if ((opr.Symbol == "pexp" || opr.Symbol == "exp") &&
                p[argEnd].Type == SymbolType.Operator &&
                (p[argEnd].Opr.Symbol == "plog" || p[argEnd].Opr.Symbol == "log")) {
              int innerArgEnd   = argEnd - 1;
              int innerArgStart = p[innerArgEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, innerArgEnd) : innerArgEnd;
              var innerArg = p.GetRange(innerArgStart, innerArgEnd - innerArgStart + 1).Select(s => s.Clone()).ToList();
              p.RemoveRange(argStart, i - argStart + 1);
              p.InsertRange(argStart, innerArg);
              changed = true; break;
            }

          } else { // ── arity-2 rules ───────────────────────────────────────────
            int rightEnd   = i - 1;
            int rightStart = p[rightEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, rightEnd) : rightEnd;
            int leftEnd    = rightStart - 1;
            int leftStart  = p[leftEnd].Type  == SymbolType.Operator ? FindSubtreeLimit(p, leftEnd)  : leftEnd;

            bool leftIsConst  = IsPureConstantSubtree(p, leftStart,  leftEnd);
            bool rightIsConst = IsPureConstantSubtree(p, rightStart, rightEnd);

            // constant folding: both subtrees are constants
            if (leftIsConst && rightIsConst) {
              double val = EvaluateConstantSubtree(p, leftStart, i);
              if (!double.IsNaN(val) && !double.IsInfinity(val)) {
                ReplaceWithConstant(p, leftStart, i, val);
                changed = true; break;
              }
            }

            bool   leftIsSingleConst  = leftStart  == leftEnd  && p[leftStart].Type  == SymbolType.Constant;
            bool   rightIsSingleConst = rightStart  == rightEnd && p[rightStart].Type == SymbolType.Constant;
            double leftVal            = leftIsSingleConst  ? p[leftStart].Con.Value  : double.NaN;
            double rightVal           = rightIsSingleConst ? p[rightStart].Con.Value : double.NaN;

            // commutativity normalisation: move lone constants to the right operand so
            // that all downstream const-chain rules can fire uniformly.
            // c + x  =>  x + c  |  c * x  =>  x * c
            if ((opr.Symbol == "+" || opr.Symbol == "*") && leftIsSingleConst && !rightIsSingleConst) {
              // swap left and right subtrees in-place
              var leftCopy  = p.GetRange(leftStart,  leftEnd  - leftStart  + 1).Select(s => s.Clone()).ToList();
              var rightCopy = p.GetRange(rightStart, rightEnd - rightStart + 1).Select(s => s.Clone()).ToList();
              p.RemoveRange(leftStart, i - leftStart + 1); // removes left + right + op atomically
              p.InsertRange(leftStart, rightCopy);
              p.InsertRange(leftStart + rightCopy.Count, leftCopy);
              p.Insert(leftStart + rightCopy.Count + leftCopy.Count, new Symbol(opr));
              changed = true; break;
            }

            if (opr.Symbol == "+") {
              if (rightIsSingleConst && rightVal == 0.0) {          // x + 0 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 0.0) {            // 0 + x => x (after normalisation above this is rare)
                p.RemoveAt(i); p.RemoveAt(leftStart); changed = true; break;
              }
              if (rightIsSingleConst && rightVal < 0.0) {           // x + (-c) => x - c
                p[rightStart].Con.Value = -rightVal;
                p[i] = new Symbol(Operators.Subtraction); changed = true; break;
              }
              // (x + c1) + c2 => x + (c1+c2)
              if (rightIsSingleConst && p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "+") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value += rightVal;
                  changed = true; break;
                }
              }
              // x + x => 2 * x  (x is any subtree)
              if (SubtreesAreEqual(p, leftStart, leftEnd, rightStart, rightEnd)) {
                var xCopy = p.GetRange(leftStart, leftEnd - leftStart + 1).Select(s => s.Clone()).ToList();
                p.RemoveRange(leftStart, i - leftStart + 1);
                p.Insert(leftStart, new Symbol(new Constant("c", 2.0)));
                p.InsertRange(leftStart + 1, xCopy);
                p.Insert(leftStart + 1 + xCopy.Count, new Symbol(Operators.Multiplication));
                changed = true; break;
              }
              // sin²(x) + cos²(x) => 1  (Pythagorean identity)
              if (p[leftEnd].Type  == SymbolType.Operator && p[leftEnd].Opr.Symbol  == "*" &&
                  p[rightEnd].Type == SymbolType.Operator && p[rightEnd].Opr.Symbol == "*") {
                // left  subtree must be  sin(x) * sin(x)
                // right subtree must be  cos(x) * cos(x)  (or vice versa)
                if (IsSinSquared(p, leftStart, leftEnd) && IsCosSquared(p, rightStart, rightEnd) ||
                    IsCosSquared(p, leftStart, leftEnd) && IsSinSquared(p, rightStart, rightEnd)) {
                  ReplaceWithConstant(p, leftStart, i, 1.0); changed = true; break;
                }
              }
            } else if (opr.Symbol == "-") {
              if (rightIsSingleConst && rightVal == 0.0) {          // x - 0 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (SubtreesAreEqual(p, leftStart, leftEnd, rightStart, rightEnd)) { // x - x => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (rightIsSingleConst && rightVal < 0.0) {           // x - (-c) => x + c
                p[rightStart].Con.Value = -rightVal;
                p[i] = new Symbol(Operators.Addition); changed = true; break;
              }
              // (x - c1) - c2 => x - (c1+c2)
              if (rightIsSingleConst && p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "-") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value += rightVal;
                  changed = true; break;
                }
              }
              // (x + y) - y => x  |  (x - y) + y already handled in + branch
              if (p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "+") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && SubtreesAreEqual(p, lrStart, lrEnd, rightStart, rightEnd)) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  changed = true; break;
                }
              }
              // (x - y) + y => x  (inner left op is -, right operand matches outer right)
              if (p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "-") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && SubtreesAreEqual(p, lrStart, lrEnd, rightStart, rightEnd)) {
                  // result is the left-left child: remove right subtree + outer op, then remove inner right + inner op
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  changed = true; break;
                }
              }
            } else if (opr.Symbol == "*") {
              if (rightIsSingleConst && rightVal == 1.0) {          // x * 1 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 1.0) {            // 1 * x => x  (after normalisation above, rare)
                p.RemoveAt(i); p.RemoveAt(leftStart); changed = true; break;
              }
              if ((rightIsSingleConst && rightVal == 0.0) ||
                  (leftIsSingleConst  && leftVal  == 0.0)) {        // x * 0 => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (rightIsSingleConst && rightVal == -1.0) {         // x * -1 => 0 - x
                p[rightStart].Con.Value = 0.0;
                p[i] = new Symbol(Operators.Subtraction); changed = true; break;
              }
              // (x * c1) * c2 => x * (c1*c2)
              if (rightIsSingleConst && p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "*") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value *= rightVal;
                  changed = true; break;
                }
              }
              // x * x => x² : replace with  x  x  *  (stays same), but tag for future:
              // represented as  2.0  x  plog  *  pexp  — only when x is a single variable
              // (too invasive for general subtrees; skip complex cases)
              // x*a + x*b => x*(a+b) is handled by the distributive law below — nothing extra needed here.

            } else if (opr.Symbol == "/" || opr.Symbol == "pd" || opr.Symbol == "aq") {
              if (rightIsSingleConst && rightVal == 1.0) {          // x / 1 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 0.0) {            // 0 / x => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (opr.Symbol == "/" && rightIsSingleConst && rightVal != 0.0) { // x / c => x * (1/c)
                p[rightStart].Con.Value = 1.0 / rightVal;
                p[i] = new Symbol(Operators.Multiplication); changed = true; break;
              }
              if (SubtreesAreEqual(p, leftStart, leftEnd, rightStart, rightEnd)) { // x / x => 1
                ReplaceWithConstant(p, leftStart, i, 1.0); changed = true; break;
              }
            }

            // ── distributive law: x*a + x*b => x*(a+b)  and  x*a - x*b => x*(a-b) ──
            // Applies when both children are * nodes and share a common left or right factor.
            if ((opr.Symbol == "+" || opr.Symbol == "-") &&
                p[leftEnd].Type  == SymbolType.Operator && p[leftEnd].Opr.Symbol  == "*" &&
                p[rightEnd].Type == SymbolType.Operator && p[rightEnd].Opr.Symbol == "*") {

              // decompose left  * into (llStart..llEnd) * (lrStart..lrEnd)
              int llEnd   = leftEnd - 1;
              int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
              int lrEnd   = llStart - 1;
              int lrStart = lrEnd >= leftStart && p[lrEnd].Type == SymbolType.Operator
                            ? FindSubtreeLimit(p, lrEnd) : lrEnd;

              // decompose right * into (rlStart..rlEnd) * (rrStart..rrEnd)
              int rlEnd   = rightEnd - 1;
              int rlStart = p[rlEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, rlEnd) : rlEnd;
              int rrEnd   = rlStart - 1;
              int rrStart = rrEnd >= rightStart && p[rrEnd].Type == SymbolType.Operator
                            ? FindSubtreeLimit(p, rrEnd) : rrEnd;

              bool lrValid = lrEnd >= leftStart;
              bool rrValid = rrEnd >= rightStart;

              // case: shared left factor   (x*a) ± (x*b) => x*(a±b)
              if (lrValid && rrValid && SubtreesAreEqual(p, llStart, llEnd, rlStart, rlEnd)) {
                var xCopy = p.GetRange(llStart, llEnd - llStart + 1).Select(s => s.Clone()).ToList();
                var aCopy = p.GetRange(lrStart, lrEnd - lrStart + 1).Select(s => s.Clone()).ToList(); // left  non-shared factor (right of inner *)
                // wait — in RPN (post-order) left * has: llStart..llEnd  lrStart..lrEnd  *
                // so llStart..llEnd is the LEFT operand of the inner * and lrStart..lrEnd is the RIGHT operand
                // same for right *: rlStart..rlEnd is LEFT, rrStart..rrEnd is RIGHT
                // shared = left operand => factor out left operand
                var bCopy = p.GetRange(rrStart, rrEnd - rrStart + 1).Select(s => s.Clone()).ToList();
                // build: x  a  b  ±  *
                p.RemoveRange(leftStart, i - leftStart + 1);
                p.InsertRange(leftStart, xCopy);
                p.InsertRange(leftStart + xCopy.Count, aCopy);
                p.InsertRange(leftStart + xCopy.Count + aCopy.Count, bCopy);
                p.Insert(leftStart + xCopy.Count + aCopy.Count + bCopy.Count, new Symbol(opr == Operators.Addition ? Operators.Addition : Operators.Subtraction));
                p.Insert(leftStart + xCopy.Count + aCopy.Count + bCopy.Count + 1, new Symbol(Operators.Multiplication));
                changed = true; break;
              }
            }
          }
        }
      }

      return p;
    }

    // helpers for Pythagorean identity check
    private bool IsSinSquared(RPN<Symbol> p, int start, int end) =>
      IsSquaredUnary(p, start, end, "sin");
    private bool IsCosSquared(RPN<Symbol> p, int start, int end) =>
      IsSquaredUnary(p, start, end, "cos");
    private bool IsSquaredUnary(RPN<Symbol> p, int start, int end, string opSym) {
      // pattern: [arg] opSym [arg] opSym  *   (i.e. f(x) * f(x))
      if (end - start < 2) return false;
      if (p[end].Type != SymbolType.Operator || p[end].Opr.Symbol != "*") return false;
      int rightEnd   = end - 1;
      int rightStart = p[rightEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, rightEnd) : rightEnd;
      int leftEnd    = rightStart - 1;
      int leftStart  = p[leftEnd].Type  == SymbolType.Operator ? FindSubtreeLimit(p, leftEnd)  : leftEnd;
      if (leftStart < start) return false;
      if (p[leftEnd].Type  != SymbolType.Operator || p[leftEnd].Opr.Symbol  != opSym) return false;
      if (p[rightEnd].Type != SymbolType.Operator || p[rightEnd].Opr.Symbol != opSym) return false;
      // both unary ops must share the same argument
      int largStart = leftStart;  int largEnd = leftEnd - 1;
      int rargStart = rightStart; int rargEnd = rightEnd - 1;
      return SubtreesAreEqual(p, largStart, largEnd, rargStart, rargEnd);
    }

    private bool IsPureConstantSubtree(RPN<Symbol> p, int start, int end) {
      for (int i = start; i <= end; i++)
        if (p[i].Type == SymbolType.Variable) return false;
      return true;
    }

    private double EvaluateConstantSubtree(RPN<Symbol> p, int start, int end) {
      var stack = new Stack<double>();
      for (int i = start; i <= end; i++) {
        var s = p[i];
        if (s.Type == SymbolType.Constant)
          stack.Push(s.Con.Value);
        else if (s.Type == SymbolType.Operator)
          stack.Push(s.Opr.Function(stack));
      }
      return stack.Count == 1 ? stack.Pop() : double.NaN;
    }

    private void ReplaceWithConstant(RPN<Symbol> p, int start, int end, double value) {
      p.RemoveRange(start, end - start + 1);
      p.Insert(start, new Symbol(new Constant("c", value)));
    }

    private bool SubtreesAreEqual(RPN<Symbol> p, int ls, int le, int rs, int re) {
      if (le - ls != re - rs) return false;
      int len = le - ls;
      for (int i = 0; i <= len; i++) {
        var l = p[ls + i]; var r = p[rs + i];
        if (l.Type != r.Type) return false;
        if (l.Type == SymbolType.Variable && (l.Var.Name != r.Var.Name || l.Var.Coefficient != r.Var.Coefficient)) return false;
        if (l.Type == SymbolType.Constant && l.Con.Value != r.Con.Value) return false;
        if (l.Type == SymbolType.Operator && l.Opr.Symbol != r.Opr.Symbol) return false;
      }
      return true;
    }

    #endregion Simplifiers

    #region Evaluators

    //public double EvaluateSet(RPN<Symbol> p, Dictionary<string, Series<double>> variableDict, int rowCount, string targetVariable) {
    //  for (int i = 0; i < rowCount; i++) {
    //    var variableDictRow = variableDict.ToDictionary(x => x.Key, y => y.Value.Values[i]);
    //    Evaluate(p, variableDictRow, i, targetVariable);
    //  }
    //  p.PearsonR = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
    //  //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
    //  //p.MAE = absoluteErrors.Mean();
    //  return p.PearsonR;
    //}

    public double EvaluateDict(RPN<Symbol> p, Dictionary<string, double> variableDict, int idx, string targetVariable) {

      foreach (var symbol in p) {
        if (symbol.Type == SymbolType.Constant) {
          evaluationBuffer.Push(symbol.Con.Value);
        }
        else if (symbol.Type == SymbolType.Variable) {
          evaluationBuffer.Push(variableDict[symbol.Var.Name] * symbol.Var.Coefficient);
        }
        else {
          evaluationBuffer.Push(symbol.Opr.Function(evaluationBuffer));
          //if (operation.Arity == 1) evaluationBuffer.Push(operation.Function(new[] {evaluationBuffer.Pop()}));
          //else evaluationBuffer.Push(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
        }
      }
      var result = evaluationBuffer.Pop();
      evaluationBuffer.Clear();
      p.TrueResults[idx] = variableDict[targetVariable];
      p.EstimatedResults[idx] = result;
      return p.TrueResults[idx] - result;
    }

    public double EvaluateArr(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int targetVariableIdx, Task modelingTask) {

      //// V2 parallel
      //var rangePartitioner = Partitioner.Create(0, rowCount);
      //Parallel.ForEach(rangePartitioner, (range, state, partialSum) =>
      //{
      //  for(int i = range.Item1; i < range.Item2; i++)
      //  {
      //    EvaluateParallel(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
      //  }
      //});

      // V1 parallel
      //Parallel.For(0, rowCount, (i, state) =>
      //{
      //  EvaluateParallel(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
      //});


      // V0 sequential
      for (int i = 0; i < rowCount; i++) {
        //Evaluate(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
        if (double.IsNaN(EvaluateStack(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx))) return double.NaN;
      }

      p.PearsonR = PearsonR.ComputeScore(p);
      p.NMSE = NMSE.ComputeScore(p);
      p.LD = LD.ComputeScore(p);

      return modelingTask.Score.Compute(p);

      //p.LD = EvaluateDescriptionLength(p);
      //p.MRE = Statistics.MRE(p.TrueResults, p.EstimatedResults);      
      //p.PearsonR = Statistics.PearsonR(p.TrueResults, p.EstimatedResults);
      //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);      
      //return p.NMSE;
    }

    //public double EvaluateSetParallel(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int targetVariableIdx) {

    //  //// V2 parallel
    //  var rangePartitioner = Partitioner.Create(0, rowCount);
    //  Parallel.ForEach(rangePartitioner, (range, state, partialSum) =>
    //  {
    //    for (int i = range.Item1; i < range.Item2; i++) {
    //      Evaluate(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
    //    }
    //  });

    //  // V1 parallel
    //  //Parallel.For(0, rowCount, (i, state) =>
    //  //{
    //  //  EvaluateParallel(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
    //  //});


    //  p.PearsonR = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
    //  //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
    //  //p.MAE = absoluteErrors.Mean();
    //  return p.PearsonR;
    //}

    public double EvaluateStack(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int idx, int targetVariableIdx) {
      
      foreach (var symbol in p) {
        if (symbol.Type == SymbolType.Constant) {
          localEvaluationBuffer.Push(symbol.Con.Value);
        }
        else if (symbol.Type == SymbolType.Variable) {
          localEvaluationBuffer.Push(data[symbol.Var.Index * rowCount + idx] * symbol.Var.Coefficient);
        }
        else {
          var tmpResult = symbol.Opr.Function(localEvaluationBuffer);
          if (double.IsNaN(tmpResult) || double.IsInfinity(tmpResult) || double.IsNegativeInfinity(tmpResult)) {
            localEvaluationBuffer.Clear();
            return double.NaN;
          }
          else {
            localEvaluationBuffer.Push(tmpResult);
          }
          //localEvaluationBuffer.Push(symbol.Opr.Function(localEvaluationBuffer));

          //if (operation.Arity == 1) evaluationBuffer.Push(operation.Function(new[] {evaluationBuffer.Pop()}));
          //else evaluationBuffer.Push(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
        }
      }
      var result = localEvaluationBuffer.Pop();
      if (localEvaluationBuffer.Count > 0) {
        Console.WriteLine("\n!!! ERROR !!!\n");
        localEvaluationBuffer.Clear();
      }
      if (double.IsNaN(result) || double.IsInfinity(result) || double.IsNegativeInfinity(result)) {
        return double.NaN;
      }

      //if (double.IsInfinity(result)) result = double.MaxValue;
      //else if (double.IsNegativeInfinity(result)) result = double.MinValue;


      p.TrueResults[idx] = data[targetVariableIdx * rowCount + idx]; // not necessary to do this in every evaluation, but it is more convenient to have the true values stored in the program for later use (e.g. for statistics)
      p.EstimatedResults[idx] = result;
      return p.TrueResults[idx] - result;
    }

    public double EvaluateProgram(RPN<Symbol> p, double[] data, int rowCount, Task t) {
      // Compile once and cache on the program instance; reuse on every subsequent call
      // for the same individual (e.g. during constant optimisation inner loops).
      // CloneDeep (called after crossover/mutation) nulls the cache automatically.
      p.CompiledDelegate ??= CompileToDelegate(p, rowCount);
      if (p.CompiledDelegate == null) return t.Score.GetPessimal();

      int targetIdx = t.VariableIndices[t.TargetVariable];

      for (int i = 0; i < rowCount; i++) {
        double estimated = p.CompiledDelegate(data, i);
        if (!double.IsFinite(estimated))
          return t.Score.GetPessimal();

        p.TrueResults[i]      = data[targetIdx * rowCount + i];
        p.EstimatedResults[i] = estimated;
      }

      p.PearsonR = PearsonR.ComputeScore(p);
      p.NMSE = NMSE.ComputeScore(p);
      p.LD = LD.ComputeScore(p);

      return t.Score.Compute(p);
    }

    // Translates an RPN<Symbol> expression into a compiled Func<double[], int, double>.
    // Parameters of the delegate: data array (column-major, stride = rowCount), row index.
    // Returns null if the program is structurally invalid.
    private static Func<double[], int, double>? CompileToDelegate(RPN<Symbol> p, int rowCount) {
      try {
        // Parameters: data array and row index
        var dataParam = Expression.Parameter(typeof(double[]), "data");
        var rowParam  = Expression.Parameter(typeof(int),      "rowIdx");

        var stack = new Stack<Expression>();

        foreach (var symbol in p) {
          if (symbol.Type == SymbolType.Constant) {
            // Bake constant value directly as a literal
            stack.Push(Expression.Constant(symbol.Con.Value, typeof(double)));
          }
          else if (symbol.Type == SymbolType.Variable) {
            // data[varIndex * rowCount + rowIdx]  —  matches EvaluateStack layout
            int stride = symbol.Var.Index * rowCount;
            Expression index = stride == 0
              ? (Expression)rowParam
              : Expression.Add(Expression.Constant(stride), rowParam);
            Expression load = Expression.ArrayIndex(dataParam, index);

            // Apply coefficient if not 1.0
            Expression varExpr = symbol.Var.Coefficient != 1.0
              ? Expression.Multiply(load, Expression.Constant(symbol.Var.Coefficient))
              : load;

            stack.Push(varExpr);
          }
          else // Operator
          {
            Expression? node = BuildOperatorExpression(symbol.Opr, stack);
            if (node == null) return null;
            stack.Push(node);
          }
        }

        if (stack.Count != 1) return null;

        var body   = stack.Pop();
        var lambda = Expression.Lambda<Func<double[], int, double>>(body, dataParam, rowParam);
        return lambda.Compile();
      }
      catch {
        return null;
      }
    }

    // Pops operands from the expression stack and returns the combined Expression node.
    // Pop order mirrors EvaluateStack: first Pop() = top of stack = right operand.
    private static Expression? BuildOperatorExpression(Operator opr, Stack<Expression> stack) {
      if (opr.Arity == 1) {
        if (stack.Count < 1) return null;
        Expression arg = stack.Pop();

        return opr.Symbol switch {
          "sin"  => Expression.Call(_miSin,  arg),
          "cos"  => Expression.Call(_miCos,  arg),
          "tan"  => Expression.Call(_miTan,  arg),
          "tanh" => Expression.Call(_miTanh, arg),
          "log"  => Expression.Call(_miLog,  arg),
          // protected log: value > 0 ? log(value) : 0.0
          "plog" => Expression.Condition(
                      Expression.GreaterThan(arg, Expression.Constant(0.0)),
                      Expression.Call(_miLog, arg),
                      Expression.Constant(0.0)),
          "exp"  => Expression.Call(_miExp,  arg),
          // protected exp: exp(clamp(value, -100, 100))
          "pexp" => Expression.Call(_miExp,
                      Expression.Call(_miMin,
                        Expression.Call(_miMax, arg, Expression.Constant(-100.0)),
                        Expression.Constant(100.0))),
          "pi"   => Expression.Multiply(arg, Expression.Constant(Math.PI)),
          _      => null
        };
      }
      else if (opr.Arity == 2) {
        if (stack.Count < 2) return null;
        Expression right = stack.Pop(); // top = right operand (matches stack pop order)
        Expression left  = stack.Pop();

        return opr.Symbol switch {
          "+"  => Expression.Add(left, right),
          "-"  => Expression.Subtract(left, right),
          "*"  => Expression.Multiply(left, right),
          "/"  => Expression.Divide(left, right),
          // protected division: denominator != 0 ? numerator / denominator : 1.0
          "pd" => Expression.Condition(
                    Expression.NotEqual(right, Expression.Constant(0.0)),
                    Expression.Divide(left, right),
                    Expression.Constant(1.0)),
          // analytic quotient: left / sqrt(1 + right²)
          "aq" => Expression.Divide(left,
                    Expression.Call(_miSqrt,
                      Expression.Add(
                        Expression.Constant(1.0),
                        Expression.Multiply(right, right)))),
          _    => null
        };
      }

      return null;
    }

    public double EvaluateDescriptionLength(RPN<Symbol> p) {
      // Implements the description length L(D) = aifeyn + codelen + negloglike
      // as defined by Bartlett et al. (2022), https://arxiv.org/abs/2211.11461
      // Reference implementation: https://github.com/DeaglanBartlett/ESR

      // ── 1. aifeyn: structural description length of the expression tree ────
      // L_func = n_nodes * ln(nop) + Σ ln(|integer constants|)
      // where nop = number of distinct operator/function tokens
      //           + 1 if any free parameter (variable or fitted constant) exists.
      var distinctOperatorSymbols = new HashSet<string>();
      bool hasFreeParam = false;
      double integerPenalty = 0.0;

      foreach (var sym in p) {
        if (sym.Type == SymbolType.Operator) {
          distinctOperatorSymbols.Add(sym.Opr.Symbol);
        } else {
          hasFreeParam = true;
          if (sym.Type == SymbolType.Constant) {
            // integer-valued constants contribute ln(|n|); treat 0 as 1 (ln(1)=0)
            double absVal = Math.Abs(sym.Con.Value);
            if (absVal == Math.Floor(absVal) && absVal > 0)
              integerPenalty += Math.Log(absVal);
          }
        }
      }

      int nop = distinctOperatorSymbols.Count + (hasFreeParam ? 1 : 0);
      if (nop < 1) nop = 1;
      double aifeyn = p.Count * Math.Log(nop) + integerPenalty;

      // ── 2. codelen: parametric description length (MDL, without Fisher) ────
      // L_param = -k/2 * ln(3) + Σ ln(max(1, |θ_i|))
      // where k = number of free numerical parameters (constants + coefficients ≠ 1)
      double codelength = 0.0;
      int k = 0;

      foreach (var sym in p) {
        double theta = double.NaN;
        if (sym.Type == SymbolType.Constant)
          theta = sym.Con.Value;
        else if (sym.Type == SymbolType.Variable && sym.Var.Coefficient != 1.0)
          theta = sym.Var.Coefficient;

        if (!double.IsNaN(theta)) {
          k++;
          codelength += Math.Log(Math.Max(1.0, Math.Abs(theta)));
        }
      }

      codelength -= k / 2.0 * Math.Log(3.0);

      // ── 3. negloglike: Gaussian negative log-likelihood ─────────────────────
      // Mirrors GaussLikelihood.negloglike in ESR (likelihood.py):
      //   nll = Σ [ 0.5*(ŷᵢ - yᵢ)²/σᵢ² + 0.5*ln(2π) + ln(σᵢ) ]
      //
      // We don't have per-point measurement uncertainties, so we use a fixed σ
      // estimated from the *target variable's own standard deviation* (independent
      // of this model). This keeps σ constant across all models so that
      // Σ (rᵢ/σ)² genuinely discriminates between good and bad fits — a model
      // that fits poorly accumulates a large quadratic penalty, not just a small
      // logarithmic one.
      int n = p.TrueResults.Count;

      double yMean = 0.0;
      for (int i = 0; i < n; i++) yMean += p.TrueResults[i];
      yMean /= n;

      double yVar = 0.0;
      for (int i = 0; i < n; i++) {
        double d = p.TrueResults[i] - yMean;
        yVar += d * d;
      }
      yVar /= n;

      // Guard: if the target is perfectly constant use a unit scale
      double sigma = yVar > 0.0 ? Math.Sqrt(yVar) : 1.0;
      double sigma2 = sigma * sigma;

      double negloglike = 0.0;
      for (int i = 0; i < n; i++) {
        double r = p.EstimatedResults[i] - p.TrueResults[i];
        negloglike += 0.5 * r * r / sigma2 + 0.5 * Math.Log(2.0 * Math.PI) + Math.Log(sigma);
      }

      // ── Total description length ────────────────────────────────────────────
      p.LD = aifeyn + codelength + negloglike;
      return p.LD;
    }


    #endregion Evaluators

    #region Penalizers

    public double PenalizePearsonR(RPN<Symbol> o) {
      o.PearsonR = o.Count > SymbolCount ? o.PearsonR * (SymbolCount / (double)o.Count) : o.PearsonR;
      return o.PearsonR;
    }

    public double PenalizeNMSE(RPN<Symbol> o) {
      o.NMSE = o.Count > SymbolCount ? o.NMSE * ((double)o.Count / SymbolCount) : o.NMSE;
      return o.NMSE;
    }

    #endregion Penalizers

    #region Optimizers

    public Tuple<RPN<Symbol>, double> OptimizeConstantsViaEvolutionStrategy(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx, Task modelingTask) {      
      var p = o.CloneDeep();
      double pFit = EvaluateArr(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
      var pNew = p.CloneDeep();
      var pNewFit = pFit;

      var constantsAndIndices = ParseConstants(p);
      var constants = constantsAndIndices.Item1;
      var indices = constantsAndIndices.Item2;
      var constantsNew = (double[])constants.Clone();

      // Step sizes are initialised relative to each constant's magnitude so that
      // a constant of 1000 gets a meaningful perturbation, not just ±0.1.
      double[] stepSizes = constants.Select(c => Math.Max(Math.Abs(c) * 0.1, 0.1)).ToArray();
      const double stepMax  = 1e4;   // hard cap — prevents runaway growth
      const double paramMax = 1e6;   // clamp constants to [-paramMax, paramMax]
      int[] executionOrder = Enumerable.Range(0, constants.Length).ToArray();

      for(int g = 0; g < OptimizationIterations; g++) {
        executionOrder = executionOrder.ShuffleFisherYates(Rng).ToArray();
        for(int i = 0; i < indices.Length; i++) {
          var idx = executionOrder[i];

          // Additive perturbation: c' = c ± step  (1/5-success rule adaptation)
          double step = stepSizes[idx];
          double delta = Rng.NextDouble() < 0.5 ? step : -step;
          double constantMutated = Math.Clamp(constantsNew[idx] + delta, -paramMax, paramMax);

          var pMutated = pNew.CloneDeep();
          UpdateConstant(pMutated, constantMutated, indices[idx]);
          var pMutatedFit = EvaluateArr(pMutated, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);

          if(!double.IsNaN(pMutatedFit) && modelingTask.Score.IsBetter(pMutatedFit, pNewFit)) {
            constantsNew[idx] = constantMutated;
            UpdateConstant(pNew, constantMutated, indices[idx]);
            stepSizes[idx] = Math.Min(stepSizes[idx] * 1.5, stepMax); // grow step on success
            pNewFit = pMutatedFit;
          } else {
            stepSizes[idx] *= Math.Pow(1.5, -0.25); // shrink step on failure
          }
        }
      }

      // sanity check and final swap
      pNewFit = EvaluateArr(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
      if(modelingTask.Score.IsBetter(pNewFit, pFit)) {
        p = pNew;
        pFit = pNewFit;
      }
      return Tuple.Create(p, pFit);
    }

    public Tuple<RPN<Symbol>, double> OptimizeConstants(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx, Task modelingTask) {
      var p = o.CloneDeep();
      double pFit = EvaluateArr(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);

      var constAndIdx  = ParseConstants(p);
      double[] constants    = constAndIdx.Item1;
      int[]    constIndices = constAndIdx.Item2;

      int n = constants.Length;
      if (n == 0) return Tuple.Create(p, pFit);

      const double h             = 1e-4;
      const double learningRate  = 0.01;
      int maxIterations = OptimizationIterations;
      const double gradClip      = 1e3;  // clip individual gradient components
      const double paramMax      = 1e6;  // clamp constants to [-paramMax, paramMax]
      double gradientSign = modelingTask.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        for (int i = 0; i < n; i++) {
          var pPlus  = p.CloneDeep();
          var pMinus = p.CloneDeep();

          UpdateConstant(pPlus,  constants[i] + h, constIndices[i]);
          UpdateConstant(pMinus, constants[i] - h, constIndices[i]);

          double fPlus  = EvaluateArr(pPlus,  localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
          double fMinus = EvaluateArr(pMinus, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
        }

        for (int i = 0; i < n; i++)
          constants[i] = Math.Clamp(constants[i] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);

        var pNew   = p.CloneDeep();
        UpdateConstants(pNew, constants, constIndices);

        double newFit = EvaluateArr(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
        if (!double.IsNaN(newFit) && modelingTask.Score.IsBetter(newFit, pFit)) {
          p    = pNew;
          pFit = newFit;
        } else {
          break;
        }
      }

      return Tuple.Create(p, pFit);
    }

    public Tuple<RPN<Symbol>, double> OptimizeCoefficientsAndConstants(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx, Task modelingTask) {
      var p = o.CloneDeep();
      double pFit = EvaluateArr(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);

      var constAndIdx  = ParseConstants(p);
      var coefAndIdx   = ParseCoefficients(p);

      double[] constants    = constAndIdx.Item1;
      int[]    constIndices = constAndIdx.Item2;
      double[] coefficients = coefAndIdx.Item2;
      int[]    coefIndices  = coefAndIdx.Item3;

      int n = constants.Length + coefficients.Length;
      if (n == 0) return Tuple.Create(p, pFit);

      const double h          = 1e-4;   // finite-difference step
      const double learningRate = 0.01;
      int maxIterations = OptimizationIterations;
      const double gradClip   = 1e3;  // clip individual gradient components
      const double paramMax   = 1e6;  // clamp parameters to [-paramMax, paramMax]
      double gradientSign = modelingTask.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        // central finite differences for each parameter
        for (int i = 0; i < n; i++) {
          var pPlus  = p.CloneDeep();
          var pMinus = p.CloneDeep();

          if (i < constants.Length) {
            UpdateConstant(pPlus,  constants[i] + h, constIndices[i]);
            UpdateConstant(pMinus, constants[i] - h, constIndices[i]);
          } else {
            int ci = i - constants.Length;
            UpdateCoefficient(pPlus,  coefficients[ci] + h, coefIndices[ci]);
            UpdateCoefficient(pMinus, coefficients[ci] - h, coefIndices[ci]);
          }

          double fPlus  = EvaluateArr(pPlus,  localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
          double fMinus = EvaluateArr(pMinus, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
        }

        // gradient step (direction determined by Score.Direction)
        bool improved = false;
        for (int i = 0; i < n; i++) {
          if (i < constants.Length)
            constants[i] = Math.Clamp(constants[i] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);
          else
            coefficients[i - constants.Length] = Math.Clamp(coefficients[i - constants.Length] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);
        }

        // apply updated parameters
        var pNew = p.CloneDeep();
        UpdateConstants(pNew, constants, constIndices);
        UpdateCoefficients(pNew, coefficients, coefIndices);

        double newFit = EvaluateArr(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx, modelingTask);
        if (!double.IsNaN(newFit) && modelingTask.Score.IsBetter(newFit, pFit)) {
          p = pNew;
          pFit = newFit;
          improved = true;
        }

        if (!improved) break;
      }

      return Tuple.Create(p, pFit);
    }
    
    private Tuple<double[], int[]> ParseConstants(RPN<Symbol> p) {
      var constants = new List<double>();
      var indices = new List<int>();

      for(int i = 0; i < p.Count; i++) {
        var s = p[i];
        if(s.Type == SymbolType.Constant) {
          constants.Add(s.Con.Value);
          indices.Add(i);
        }
      }

      return Tuple.Create(constants.ToArray(), indices.ToArray());      
    }

    private Tuple<string[], double[], int[]> ParseCoefficients(RPN<Symbol> p) {
      var variables = new List<string>();
      var coefficients = new List<double>();
      var indices = new List<int>();

      for(int i = 0; i < p.Count; i++) {
        var s = p[i];
        if(s.Type == SymbolType.Variable) {
          variables.Add(s.Var.Name);
          coefficients.Add(s.Var.Coefficient);
          indices.Add(i);
        }
      }

      return Tuple.Create(variables.ToArray(), coefficients.ToArray(), indices.ToArray()); 
    }

    private void UpdateConstants(RPN<Symbol> p, double[] constants, int[] indices) {
      for(int i = 0; i < indices.Length; i++) {
        p[indices[i]].Con.Value = constants[i];
      }
    }

    private void UpdateConstant(RPN<Symbol> p, double constant, int index) {
      p[index].Con.Value = constant;
    }

    private void UpdateCoefficient(RPN<Symbol> p, double coefficient, int index) {
      p[index].Var.Coefficient = coefficient;
    }

    private void UpdateCoefficients(RPN<Symbol> p, double[] coefficients, int[] indices) {
      for (int i = 0; i < indices.Length; i++) {
        p[indices[i]].Var.Coefficient = coefficients[i];
      }
    }

    #endregion Optimizers

    #region Helpers

    private double[] GetScores(Metric metric) {
      return metric switch {
        Metric.PearsonR => population.Select(x => x.PearsonR).ToArray(),
        Metric.PearsonR2 => population.Select(x => x.PearsonR2).ToArray(),
        Metric.NMSE => population.Select(x => x.NMSE).ToArray(),
        Metric.MRE => population.Select(x => x.MRE).ToArray(),
        Metric.LD => population.Select(x => x.LD).ToArray(),
        _ => throw new ArgumentException("Unsupported metric: " + metric)
      };
    }

    private double GetScore(RPN<Symbol> p, Metric metric) => metric switch {
      Metric.PearsonR => p.PearsonR,
      Metric.NMSE => p.NMSE,
      Metric.MRE => p.MRE,
      Metric.LD => p.LD,
      _ => throw new ArgumentException("Unsupported metric: " + metric)
    };

    #endregion Helpers

  }

}
