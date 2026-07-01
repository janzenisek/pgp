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

    private object locker;
    private object bestSolutionLocker;

    private int targetVariableIdx;
    private Dictionary<string, Tuple<double, double>> variableLimitsDict;



    // General
    public Task Task { get; set; }
    public DataSet DataSet { get; set; }
    public DataRecord DataRecord { get; set; }
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
    public Func<PgpAlgorithm, RPN<Symbol>, RPN<Symbol>, RPN<Symbol>> Crossover { get; set; } = Crossing.Cross;
    public Func<PgpAlgorithm, RPN<Symbol>, RPN<Symbol>> Mutate { get; set; } = Mutation.MutateMultiCase;
    public List<Func<PgpAlgorithm, RPN<Symbol>, RPN<Symbol>>> Mutators { get; set; } = new List<Func<PgpAlgorithm, RPN<Symbol>, RPN<Symbol>>>();
    public Func<PgpAlgorithm, RPN<Symbol>, Task, DataRecord, double> Evaluate { get; set; } = Evaluation.EvaluateStack;
    public Func<PgpAlgorithm, RPN<Symbol>, Task, DataRecord, Tuple<RPN<Symbol>, double>> Optimizer { get; set; } = Optimization.OptimizeCoefficientsAndConstants;


    // Algorithm control settings
    public int EvaluationCount { get; private set; }
    public bool LogStatistics { get; set; } = false;
    public bool UseParallelization { get; set; } = true;
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
        Metric.PearsonR => 1.0-(p.PearsonR*p.PearsonR),
        Metric.PearsonR2 => 1.0-p.PearsonR2,
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
      DataRecord = new DataRecord { Data = data, RowCount = trainingData.RowCount, TargetIndex = targetVariableIdx};

      // create and evaluate initial population
      await System.Threading.Tasks.Task.Run(() => Initialize(task, trainingData, data));

      if(ct.IsCancellationRequested) return;

      // run main gp loop
      await System.Threading.Tasks.Task.Run(() => {
        if (UseParallelization) RunParallel(ct);
        else Run();
      });
    }

    // create and evaluate initial population
    public void Initialize(Task modelingTask, DataSet trainingData, double[] data) {

      double bestFitScore = modelingTask.Score.GetPessimal();
      RPN<Symbol> bestSolution = null;

      for (int i = 0; i < population.Length;) {
        population[i] = Breed(this);
        double f = Evaluate(this, population[i], modelingTask, DataRecord);

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

    public void RunParallel(CancellationToken ct) {      
      double[] fitScores = GetScores(Task.Metric);
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
        double sumFitScores = Task.Score.GetScoreSum(fitScores);
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
            var c1Idx = Select(population, Task).Item2;
            var c2Idx = Select(population, Task).Item2;

            var c1 = population[c1Idx];
            var c2 = population[c2Idx];
            var f1 = fitScores[c1Idx];
            var f2 = fitScores[c2Idx];

            // cross
            if (Rng.NextDouble() < CrossoverRate) {
              RPN<Symbol> offspring = Crossover(this, c1, c2);              
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
              // select mutator from list or use default
              if (Mutators.Count > 0) {
                var mutator = Mutators[Rng.Next(0, Mutators.Count)];
                populationNew[i] = mutator(this, populationNew[i]);
              } else {
                populationNew[i] = Mutate(this, populationNew[i]);
              }
                          
              var diff = populationNew[i].Count - population[i].Count;
              sizeDiffAfterMutation.Add(diff);
            }

            // simplify
            if(PerformSimplification)
              populationNew[i] = Simplify(populationNew[i]);

            // evaluate
            double f = Evaluate(this, populationNew[i], Task, DataRecord);
            localEvaluationCount++;

            if(Optimizer != null) {
              var optimizedResult = Optimizer(this, populationNew[i], Task, DataRecord);
              populationNew[i] = optimizedResult.Item1;
              f = optimizedResult.Item2;
            }

            if (!double.IsNaN(f)) {
              //if (f > Math.Min(f1, f2)) { // OS              
              if (Task.Score.IsBetter(f, bestFitScore)) { // f > bestFitScore if Pearson R is used
                lock (bestSolutionLocker) {
                  if (Task.Score.IsBetter(f, bestFitScore)) { // f > bestFitScore if Pearson R is used
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

    public void Run() {
      var score = Task.Score;

      double[] fitScores = GetScores(Task.Metric);
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
          var c1Idx = Select(population, Task).Item2;
          var c2Idx = Select(population, Task).Item2;
          var c1 = population[c1Idx];
          var c2 = population[c2Idx];

          // crossover — fall back to a parent clone when no operator crosspoint exists
          RPN<Symbol> offspring = Crossover(this, c1, c2);
          if (offspring != null) {
            populationNew[i] = offspring;// score.IsBetter(GetScore(result.Item1, Task.Metric), GetScore(result.Item2, Task.Metric))
                               //? result.Item1 : result.Item2;
          } else {
            populationNew[i] = (RPN<Symbol>)c1.CloneDeep(); // safe fallback: use parent
          }

          // mutate
          if (Rng.NextDouble() < MutationRate) {
            // select mutator from list or use default
            if (Mutators.Count > 0) {
              var mutator = Mutators[Rng.Next(0, Mutators.Count)];
              populationNew[i] = mutator(this, populationNew[i]);
            } else {
              populationNew[i] = Mutate(this, populationNew[i]);
            }
          }

          // Evaluate
          double f = Evaluate(this, populationNew[i], Task, DataRecord);
          generationalEvaluationCount++;

          // perform constant optimization
          if (Optimizer != null) {
            var optimizedResult = Optimizer(this, populationNew[i], Task, DataRecord);
            populationNew[i] = optimizedResult.Item1;
            f = optimizedResult.Item2;
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
            int argStart = p[argEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, argEnd) : argEnd;

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
              int innerArgStart = p[innerArgEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, innerArgEnd) : innerArgEnd;
              var innerArg = p.GetRange(innerArgStart, innerArgEnd - innerArgStart + 1).Select(s => s.Clone()).ToList();
              p.RemoveRange(argStart, i - argStart + 1);
              p.InsertRange(argStart, innerArg);
              changed = true; break;
            }
            if ((opr.Symbol == "pexp" || opr.Symbol == "exp") &&
                p[argEnd].Type == SymbolType.Operator &&
                (p[argEnd].Opr.Symbol == "plog" || p[argEnd].Opr.Symbol == "log")) {
              int innerArgEnd   = argEnd - 1;
              int innerArgStart = p[innerArgEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, innerArgEnd) : innerArgEnd;
              var innerArg = p.GetRange(innerArgStart, innerArgEnd - innerArgStart + 1).Select(s => s.Clone()).ToList();
              p.RemoveRange(argStart, i - argStart + 1);
              p.InsertRange(argStart, innerArg);
              changed = true; break;
            }

          } else { // ── arity-2 rules ───────────────────────────────────────────
            int rightEnd   = i - 1;
            int rightStart = p[rightEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, rightEnd) : rightEnd;
            int leftEnd    = rightStart - 1;
            int leftStart  = p[leftEnd].Type  == SymbolType.Operator ? Utils.FindSubtreeLimit(p, leftEnd)  : leftEnd;

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
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
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
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value += rightVal;
                  changed = true; break;
                }
              }
              // (x + y) - y => x  |  (x - y) + y already handled in + branch
              if (p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "+") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && SubtreesAreEqual(p, lrStart, lrEnd, rightStart, rightEnd)) {
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  changed = true; break;
                }
              }
              // (x - y) + y => x  (inner left op is -, right operand matches outer right)
              if (p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "-") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
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
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
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
              int llStart = p[llEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, llEnd) : llEnd;
              int lrEnd   = llStart - 1;
              int lrStart = lrEnd >= leftStart && p[lrEnd].Type == SymbolType.Operator
                            ? Utils.FindSubtreeLimit(p, lrEnd) : lrEnd;

              // decompose right * into (rlStart..rlEnd) * (rrStart..rrEnd)
              int rlEnd   = rightEnd - 1;
              int rlStart = p[rlEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, rlEnd) : rlEnd;
              int rrEnd   = rlStart - 1;
              int rrStart = rrEnd >= rightStart && p[rrEnd].Type == SymbolType.Operator
                            ? Utils.FindSubtreeLimit(p, rrEnd) : rrEnd;

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
      int rightStart = p[rightEnd].Type == SymbolType.Operator ? Utils.FindSubtreeLimit(p, rightEnd) : rightEnd;
      int leftEnd    = rightStart - 1;
      int leftStart  = p[leftEnd].Type  == SymbolType.Operator ? Utils.FindSubtreeLimit(p, leftEnd)  : leftEnd;
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
