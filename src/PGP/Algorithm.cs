using PGP.Data;
using PGP.Utils;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace PGP.Core {
  public class PgpAlgorithm {
    private FastRandom rng;
    private Stack<double> evaluationBuffer;
    private RPN<Symbol>[] population;

    private int targetVariableIdx;
    private double operatorToOperandRatio;
    private object locker;
    private object bestSolutionLocker;

    // GP Settings
    public List<string> InputVariables { get; set; }
    public string TargetVariable { get; set; }
    public Dictionary<string, int> VariableIndices { get; set; }
    public Dictionary<string, Tuple<double, double>> VariableLimitsDict { get; set; }
    public int Generations { get; set; }
    public int PopulationSize { get; set; }
    public double CrossoverRate { get; set; }
    public double MutationRate { get; set; }
    public double MaximumSelectionPressure { get; set; }
    public int Elites { get; set; }
    public int TreeLength { get; set; }


    // Algorithm control settings and statistics
    public bool UseParallelization { get; set; }
    public bool LogGenerations { get; set; }
    public int EvaluationCount { get; private set; }
    public bool UseConstantOptimization { get; set; }



    public PgpAlgorithm(FastRandom randomNumberGenerator, IEnumerable<string> inputVariables, string targetVariable, Dictionary<string, int> variableIndices, Dictionary<string, Tuple<double, double>> variableLimitsDict,
      int generations = 1000, int populationSize = 1000, int treeLength = 50, double crossoverRate = 1.0, double mutationRate = 0.25, double maximumSelectionPressure = 200, int elites = 1) {
      locker = new object();
      bestSolutionLocker = new object();
      evaluationBuffer = new Stack<double>(treeLength * 2);


      rng = randomNumberGenerator;
      InputVariables = inputVariables.ToList();
      TargetVariable = targetVariable;
      VariableIndices = variableIndices;
      targetVariableIdx = variableIndices[targetVariable];
      VariableLimitsDict = variableLimitsDict;
      Generations = generations;
      PopulationSize = populationSize;
      CrossoverRate = crossoverRate;
      MutationRate = mutationRate;
      MaximumSelectionPressure = maximumSelectionPressure;
      Elites = elites;
      TreeLength = treeLength;

      UseParallelization = false;
      LogGenerations = false;

      population = new RPN<Symbol>[PopulationSize];
      operatorToOperandRatio = 1.0 * Operators.All.Count / Operators.All.Sum(x => x.Arity);
    }

    public void Fit(Set trainingData, bool initialize = true) {
      var data = trainingData.GetArray(VariableIndices.Keys.ToList());

      if(initialize) Initialize(trainingData, data);

      if (UseParallelization) RunParallel(trainingData, data);
      else Run(trainingData, data);
    }

    public void Initialize(Set trainingData, double[] data) {
      //var doubleSet = trainingData.GetDoubleSet();
      double bestFitScore = 0.0;
      RPN<Symbol> bestSolution = null;

      for (int i = 0; i < population.Length;) {
        population[i] = Breed(trainingData.RowCount);
        //double f = EvaluateSet(population[i], doubleSet, trainingData.RowCount, TargetVariable);
        double f = EvaluateSet(population[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx);

        if (!double.IsNaN(f)) {
          if (f > bestFitScore) {
            bestSolution = (RPN<Symbol>)population[i].Clone();
            bestFitScore = f;
          }
          i++;
        }

      }
      population[0] = (RPN<Symbol>)bestSolution.Clone();
      EvaluationCount = 0;
    }

    public void RunParallel(Set trainingData, double[] data) {
      double[] fitScores = population.Select(x => x.PearsonR).ToArray();
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      double[] fitScoresNew = new double[PopulationSize];
      double bestFitScore = population.First().PearsonR;
      var bestSolution = (RPN<Symbol>)population.First().CloneDeepWithResults(); // take a random solution (don't care)

      double currentSelectionPressure = 0.0;

      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        int generationalEvaluationCount = 0;
        var rangePartitioner = Partitioner.Create(Elites, PopulationSize);
        Parallel.ForEach(rangePartitioner,
          () => 0,
          (range, state, localEvaluationCount) =>
        {
          var localEvaluationBuffer = new Stack<double>(TreeLength * 2);
          for (int i = range.Item1; i < range.Item2 && currentSelectionPressure < MaximumSelectionPressure;) {
            var c1Idx = SelectProportionalIdx(fitScoresNew);
            var c2Idx = SelectProportionalIdx(fitScoresNew);
            var c1 = population[c1Idx];
            var c2 = population[c2Idx];
            var f1 = fitScores[c1Idx];
            var f2 = fitScores[c2Idx];

            var result = Cross(c1, c2);
            if (result != null) {
              populationNew[i] = result.Item1.PearsonR > result.Item2.PearsonR ? result.Item1 : result.Item2;

            }

            if (rng.NextDouble() < MutationRate) {
              populationNew[i] = Mutate(populationNew[i]);
            }



            // Evaluate
            //double f = EvaluateSet(populationNew[i], doubleSet, trainingData.RowCount, TargetVariable);            
            double f = EvaluateSet(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
            localEvaluationCount++;

            // perform constant optimization
            if (!double.IsNaN(f) && UseConstantOptimization && rng.NextDouble() < 0.1) {
              var optimizationResult = OptimizeConstants(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
              populationNew[i] = optimizationResult.Item1;
              f = optimizationResult.Item2;
            }


            if (!double.IsNaN(f)) {
              //if (f > Math.Min(f1, f2)) { // OS              
              if (f > bestFitScore) {
                lock (bestSolutionLocker) {
                  if (f > bestFitScore) {
                    bestFitScore = f;
                    bestSolution = (RPN<Symbol>)populationNew[i].CloneDeepWithResults();
                  }
                }
              }
              fitScoresNew[i] = f;
              i++;
              //} // OS
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
        populationNew[0] = (RPN<Symbol>)bestSolution.Clone(); // clone references only, without results
        fitScoresNew[0] = bestFitScore;

        EvaluationCount += generationalEvaluationCount;

        if (LogGenerations) Console.WriteLine($"Generation: {g:d4}, Evaluations: {generationalEvaluationCount:d4}, Selection Pressure: {currentSelectionPressure:f2}, Score: {bestFitScore:f12}");
      }
    }

    public void Run(Set trainingData, double[] data) {
      //var doubleSet = trainingData.GetDoubleSet();
      double[] fitScores = population.Select(x => x.PearsonR).ToArray();
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      double[] fitScoresNew = new double[PopulationSize];
      double bestFitScore = population.First().PearsonR;
      var bestSolution = (RPN<Symbol>)population.First().CloneDeepWithResults(); // take a random solution (don't care)

      double currentSelectionPressure = 0.0;
      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        int i = Elites;
        int generationalEvaluationCount = 0;
        do {
          var c1Idx = SelectProportionalIdx(fitScoresNew);
          var c2Idx = SelectProportionalIdx(fitScoresNew);
          var c1 = population[c1Idx];
          var c2 = population[c2Idx];
          var f1 = fitScores[c1Idx];
          var f2 = fitScores[c2Idx];

          var result = Cross(c1, c2);
          if (result != null) {
            populationNew[i] = result.Item1.PearsonR > result.Item2.PearsonR ? result.Item1 : result.Item2;

          }

          if (rng.NextDouble() < MutationRate) {
            populationNew[i] = Mutate(populationNew[i]);
          }

          // Evaluate
          //double f = EvaluateSet(populationNew[i], doubleSet, trainingData.RowCount, TargetVariable);
          double f = EvaluateSet(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
          generationalEvaluationCount++;

          // perform constant optimization
          if (!double.IsNaN(f) && UseConstantOptimization && rng.NextDouble() < 0.1) {
            var optimizationResult = OptimizeConstants(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
            populationNew[i] = optimizationResult.Item1;
            f = optimizationResult.Item2;
          }

          if (!double.IsNaN(f)) {
            //if (f > Math.Min(f1, f2)) { // OS            
            if (f > bestFitScore) {
              bestFitScore = f;
              bestSolution = (RPN<Symbol>)populationNew[i].CloneDeepWithResults();
            }
            fitScoresNew[i] = f;
            i++;
            //} // OS
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

        // keep elite
        populationNew[0] = (RPN<Symbol>)bestSolution.Clone();
        fitScoresNew[0] = bestFitScore;

        EvaluationCount += generationalEvaluationCount;

        if (LogGenerations) Console.WriteLine($"Generation: {g:d4}, Evaluations: {generationalEvaluationCount:d4}, Selection Pressure: {currentSelectionPressure:f2}, Score: {bestFitScore:f12}");
      }
    }

    private Symbol GetTerminal(ref int c) {
      double rndD = rng.NextDouble();
      int rndI;

      if (rndD < 0.75) {
        rndI = rng.Next(0, InputVariables.Count);
        string varName = InputVariables[rndI];
        return new Symbol(new Variable(varName, VariableIndices[varName], 1.0));
      }
      else {
        rndI = rng.Next(0, VariableLimitsDict.Count);
        c++;
        return new Symbol(new Constant(
              $"c{c}",
              rng.NextDouble(VariableLimitsDict.ElementAt(rndI).Value.Item1, VariableLimitsDict.ElementAt(rndI).Value.Item2)
            ));
      }
    }

    private Symbol GetVariable() {
      int rndI = rng.Next(0, InputVariables.Count);
      string varName = InputVariables[rndI];
      return new Symbol(new Variable(InputVariables[rndI], VariableIndices[varName], 1.0));
    }

    public RPN<Symbol> Breed(int initialEvaluationCapacity = 0) {
      var p = new RPN<Symbol>(TreeLength, initialEvaluationCapacity);
      int aritySum = 0, arityCount = 0, tCount = 0;
      int constantCounter = 0;
      double rndD;

      Symbol curSmybol = GetTerminal(ref constantCounter);
      p.Add(curSmybol);
      tCount++;


      while (p.Count < TreeLength)
      //while (p.Count < TreeLength || (tCount > 1 || tCount == 1 && curSmybol.Type != SymbolType.Operator))
      {
        rndD = rng.NextDouble();
        if (rndD > operatorToOperandRatio) {
          // add terminal (variable or constant) to the program leafs
          curSmybol = GetTerminal(ref constantCounter);
          p.Add(curSmybol);
          tCount++;
        }
        else {
          curSmybol = new Symbol(Operators.SelectRandom(rng));
          if (tCount >= curSmybol.Opr.Arity) {
            tCount -= curSmybol.Opr.Arity - 1;
            p.Add(curSmybol);
          }
        }
      }

      while (tCount > 1) {
        curSmybol = new Symbol(Operators.SelectRandom(rng));
        if (tCount >= curSmybol.Opr.Arity) {
          tCount -= curSmybol.Opr.Arity - 1;
          p.Add(curSmybol);
        }
      }

      //// (1) setup start
      //var startSymbol = Operators.SelectRandom(rng);
      //p.Push(new Symbol(startSymbol));
      ////aritySum = startSymbol.Arity;
      //aritySum = startSymbol.Arity;

      //for (int i = 1; p.Count < TreeLength; i++)
      //{
      //  rndD = rng.NextDouble();
      //  if(rndD > operatorToOperandRatio)
      //  {
      //    // push terminal (variable or constant) to the program leafs
      //    p.Push(GetTerminal(ref constantCounter));
      //    aritySum--;          
      //  }
      //  else
      //  {
      //    // push operator to the program leafs
      //    var op = new Symbol(Operators.SelectRandom(rng));
      //    aritySum += op.Opr.Arity-1;
      //    arityCount = op.Opr.Arity;
      //    p.Push(op);
      //  }
      //}

      //// (3) validate and correct
      //while(aritySum != 0 && arityCount != 0)
      //{
      //  // case 1: we need terminals at leafs, since the overall arity of all preceding operations is not fullfilled 
      //  // e.g. 2 4 + + 11 4 + /  corrected: 3 2 4 + + 11 4 + /
      //  // case 1: we need terminals at leafs, since arity of the preceding operation is not fullfilled
      //  // (if the overall arity is already fullfilled, this causes adding a operation to the root within the next loop iteration)
      //  // e.g.               + 1 1 4 + 11 4 + /      ==> aritySum is  0, arityCount is 2
      //  // 1. correction:   2 + 1 1 4 + 11 4 + /      ==> aritySum is -2, arityCount is 1
      //  // 2. correction: 2 2 + 1 1 4 + 11 4 + /      ==> aritySum is -2, arityCount is 0
      //  // 3. correction: 2 2 + 1 1 4 + 11 4 + / +    ==> aritySum is -1, arityCount is 0
      //  // 4. correction: 2 2 + 1 1 4 + 11 4 + / + +  ==> aritySum is  0, arityCount is 0
      //  if (arityCount > 0 || aritySum > 0)
      //  {
      //    // push terminal to begin of program (leaf)
      //    p.Push(GetTerminal(++constantCounter));
      //    aritySum--;
      //    arityCount--;

      //  } else if(aritySum < 0)
      //  {          
      //    // add operator to end of program (root)
      //    var op = new Symbol(Operators.SelectRandom(rng));          
      //    aritySum += op.Opr.Arity-1;
      //    p.Add(op);
      //  }
      //}

      return p;
    }

    public RPN<Symbol> SelectRandom(RPN<Symbol>[] pop) {
      return pop[rng.Next(pop.Length)];
    }

    public int SelectProportionalIdx(double[] score) {
      return rng.Next(score.Length);
    }
    public int SelectProportionalIdx(List<double> score) {
      return rng.Next(score.Count);
    }

    public Tuple<RPN<Symbol>, RPN<Symbol>> Cross(RPN<Symbol> a, RPN<Symbol> b) {
      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();

      int aIdx = rng.Next(0, a.Count);
      int aUpperBound = a.FindIndex(aIdx, x => x.Type == SymbolType.Operator);

      if (aUpperBound != -1) {
        //var aOp = (Operator)a[idxAOp];
        int aLowerBound = FindSubtreeLimit(a, aUpperBound);
        var aSubtree = a.GetRange(aLowerBound, aUpperBound - aLowerBound + 1);

        int bIdx = rng.Next(0, b.Count);
        int bUpperBound = b.FindIndex(bIdx, x => x.Type == SymbolType.Operator);

        if (bUpperBound != -1) {
          int bLowerBound = FindSubtreeLimit(b, bUpperBound);
          var bSubtree = b.GetRange(bLowerBound, bUpperBound - bLowerBound + 1);
          aOffspring.RemoveRange(aLowerBound, aUpperBound - aLowerBound + 1);
          aOffspring.InsertRange(aLowerBound, bSubtree);
          bOffspring.RemoveRange(bLowerBound, bUpperBound - bLowerBound + 1);
          bOffspring.InsertRange(bLowerBound, aSubtree);

          return Tuple.Create(aOffspring, bOffspring);
        }
      }

      return null;
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

    public RPN<Symbol> Mutate(RPN<Symbol> o) {
      var p = o.CloneDeep();
      int idx = rng.Next(0, o.Count); // uniformely distributed
      var obj = p[idx];

      if (p[idx].Type == SymbolType.Constant) {
        var val = p[idx].Con.Value;
        var ratio = rng.NextDouble() * 0.1;
        if (rng.NextDouble() < 0.5) p[idx].Con.Value = val + val * ratio;
        else p[idx].Con.Value = val - val * ratio;
      }
      else if (p[idx].Type == SymbolType.Variable) {
        p[idx] = GetVariable();
      }
      else // operator
        {
        var ratio = rng.NextDouble();
        if (ratio < 0.5) // remove
        {
          if (p[idx].Opr.Arity == 1) p.RemoveAt(idx);
          else if (p[idx].Opr.Arity == 2) {
            int limit = FindSubtreeLimit(p, idx);
            //limit = limit < 0 ? 0 : limit;
            p.RemoveRange(limit, idx - limit + 1);
            p.Insert(limit, GetVariable());
          }
        }
        else // replace
        {
          p[idx].Opr = Operators.SelectRandomDifferent(rng, p[idx].Opr);
        }
      }

      return p;
    }

    public double EvaluateSet(RPN<Symbol> p, Dictionary<string, Series<double>> variableDict, int rowCount, string targetVariable) {
      for (int i = 0; i < rowCount; i++) {
        var variableDictRow = variableDict.ToDictionary(x => x.Key, y => y.Value.Values[i]);
        Evaluate(p, variableDictRow, i, targetVariable);
      }
      p.PearsonR = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
      //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
      //p.MAE = absoluteErrors.Mean();
      return p.PearsonR;
    }

    public double Evaluate(RPN<Symbol> p, Dictionary<string, double> variableDict, int idx, string targetVariable) {

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

    public double EvaluateSet(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int targetVariableIdx) {

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
        if (double.IsNaN(Evaluate(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx))) return double.NaN;
      }
      p.PearsonR = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
      //p.PearsonR = Statistics.PearsonR(p.TrueResults, p.EstimatedResults);
      //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
      //p.MAE = absoluteErrors.Mean();
      return p.PearsonR;
    }

    public double EvaluateSetParallel(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int targetVariableIdx) {

      //// V2 parallel
      var rangePartitioner = Partitioner.Create(0, rowCount);
      Parallel.ForEach(rangePartitioner, (range, state, partialSum) =>
      {
        for (int i = range.Item1; i < range.Item2; i++) {
          Evaluate(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
        }
      });

      // V1 parallel
      //Parallel.For(0, rowCount, (i, state) =>
      //{
      //  EvaluateParallel(p, localEvaluationBuffer, data, rowCount, i, targetVariableIdx);
      //});


      p.PearsonR = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
      //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
      //p.MAE = absoluteErrors.Mean();
      return p.PearsonR;
    }


    public double Evaluate(RPN<Symbol> p, Stack<double> localEvaluationBuffer, double[] data, int rowCount, int idx, int targetVariableIdx) {
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


      p.TrueResults[idx] = data[targetVariableIdx * rowCount + idx];
      p.EstimatedResults[idx] = result;
      return p.TrueResults[idx] - result;
    }

    public double Evaluate(RPN<Symbol> p, double[] data, int rowCount, int idx, int targetVariableIdx) {

      foreach (var symbol in p) {
        if (symbol.Type == SymbolType.Constant) {
          evaluationBuffer.Push(symbol.Con.Value);
        }
        else if (symbol.Type == SymbolType.Variable) {
          evaluationBuffer.Push(data[symbol.Var.Index * rowCount + idx] * symbol.Var.Coefficient);
        }
        else {
          evaluationBuffer.Push(symbol.Opr.Function(evaluationBuffer));
          //if (operation.Arity == 1) evaluationBuffer.Push(operation.Function(new[] {evaluationBuffer.Pop()}));
          //else evaluationBuffer.Push(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
        }
      }
      var result = evaluationBuffer.Pop();
      evaluationBuffer.Clear();
      p.TrueResults[idx] = data[targetVariableIdx * rowCount + idx];
      p.EstimatedResults[idx] = result;
      return p.TrueResults[idx] - result;
    }

    public Tuple<RPN<Symbol>, double> OptimizeConstants(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx) {      
      var p = o.CloneDeep();
      double pFit = EvaluateSet(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
      var pNew = p.CloneDeep();
      var pNewFit = pFit;

      var constantsAndIndices = ParseConstants(p);
      var constants = constantsAndIndices.Item1;
      var indices = constantsAndIndices.Item2;
      var constantsNew = (double[])constants.Clone();

      double[] mutationRates = Enumerable.Range(0, constants.Length).Select(x => 0.1).ToArray();
      int[] executionOrder = Enumerable.Range(0, constants.Length).ToArray();

      for(int g = 0; g < 10; g++) {
        executionOrder = executionOrder.ShuffleFisherYates(rng).ToArray();
        for(int i = 0; i < indices.Length; i++) {
          var idx = executionOrder[i];

          var pMutated = pNew.CloneDeep();
          var constantMutated = constantsNew[idx] * mutationRates[idx];                    
          UpdateConstant(pMutated, constantMutated, indices[idx]);
          var pMutatedFit = EvaluateSet(pMutated, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);

          if(pMutatedFit > pNewFit) { // Pearson R ... i.e. larger number = better
            constantsNew[idx] = constantMutated;
            UpdateConstant(pNew, constantMutated, indices[idx]);
            mutationRates[idx] *= 1.5;
            pNewFit = pMutatedFit;
          } else {
            mutationRates[idx] *= Math.Pow(1.5, -0.25); // 1.5^-(1/4)
          }
        }
      }

      // sanity check and final swap
      pNewFit = EvaluateSet(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
      if(pNewFit > pFit) { // Pearson R ... i.e. larger number = better
        p = pNew;
        pFit = pNewFit;
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

    private void UpdateConstants(RPN<Symbol> p, double[] constants, int[] indices) {
      for(int i = 0; i < indices.Length; i++) {
        p[indices[i]].Con.Value = constants[i];
      }
    }

    private void UpdateConstant(RPN<Symbol> p, double constant, int index) {
      p[index].Con.Value = constant;
    }

  }

}
