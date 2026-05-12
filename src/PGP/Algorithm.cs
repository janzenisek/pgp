using PGP.Data;
using PGP.Utils;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace PGP.Core {
  public class PgpAlgorithm {
    private FastRandom seedRng;
    private ThreadLocal<FastRandom> rng;
    private FastRandom Rng => rng.Value;
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
    public int SymbolCount { get; set; }
    public int NestinDepth { get; set; }


    // Algorithm control settings
    public bool UseParallelization { get; set; }
    public bool LogGenerations { get; set; }
    public int EvaluationCount { get; private set; }
    public bool UseConstantOptimization { get; set; }


    // statistics
    public double MaxPearsonR => population.Max(x => x.PearsonR);
    public double MinMAE => population.Min(x => x.MAE);
    public double MinNMSE => population.Min(x => x.NMSE);
    public double MinLength => population.Min(x => x.Count);
    public double MinLD => population.Min(x => x.LD);

    public string BestProgram => population.OrderBy(x => x.NMSE).First().ToInfixString();
    public string BestProgramRPN => population.OrderBy(x => x.NMSE).First().ToString();
    public double BestProgramPearsonR => population.OrderBy(x => x.NMSE).First().PearsonR;
    public double BestProgramNMSE => population.OrderBy(x => x.NMSE).First().NMSE;
    public double BestProgramLD => population.OrderBy(x => x.NMSE).First().LD;

    public double MeanPearsonR => population.Average(x => x.PearsonR);
    public double MedianPearsonR => population.Select(x => x.PearsonR).Median();
    public double MeanLength => population.Average(x => x.Count);
    public double MedianLength => population.Select(x => x.Count).Median();
    public double MeanLD => population.Average(x => x.LD);
    public double MedianLD => population.Select(x => x.LD).Median();

    public PgpAlgorithm(FastRandom randomNumberGenerator, IEnumerable<string> inputVariables, string targetVariable, Dictionary<string, int> variableIndices, Dictionary<string, Tuple<double, double>> variableLimitsDict,
      int generations = 1000, int populationSize = 1000, int treeLength = 50, double crossoverRate = 1.0, double mutationRate = 0.25, double maximumSelectionPressure = 200, int elites = 1) {
      locker = new object();
      bestSolutionLocker = new object();
      evaluationBuffer = new Stack<double>(treeLength * 2);


      seedRng = randomNumberGenerator;
      rng = new ThreadLocal<FastRandom>(() => { lock (seedRng) { return new FastRandom(seedRng.Next()); } });
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
      SymbolCount = treeLength;

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
      double bestFitScore = double.MinValue;
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
      double[] fitScores = population.Select(x => x.NMSE).ToArray();
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      double[] fitScoresNew = new double[PopulationSize];
      double bestFitScore = double.MaxValue;
      var bestSolution = (RPN<Symbol>)population.First().CloneDeepWithResults(); // take a random solution (don't care)

      double currentSelectionPressure = 0.0;

      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        int generationalEvaluationCount = 0;
        var rangePartitioner = Partitioner.Create(Elites, PopulationSize);
        //double sumFitScores = fitScores.Sum(); // Pearson R
        double sumFitScores = fitScores.Select(f => 1.0 / (1.0 + f)).Sum(); // NMSE
        var fitScoresList = fitScores.ToList();
        Parallel.ForEach(rangePartitioner,
          () => 0,
          (range, state, localEvaluationCount) =>
        {
          var localEvaluationBuffer = new Stack<double>(SymbolCount * 2);
          for (int i = range.Item1; i < range.Item2 && currentSelectionPressure < MaximumSelectionPressure;) {
            var c1Idx = SelectProportionalIdxNMSE(fitScoresList, sumFitScores);
            var c2Idx = SelectProportionalIdxNMSE(fitScoresList, sumFitScores);
            //var c1Idx = SelectRandomIdx(fitScoresList);
            //var c2Idx = SelectRandomIdx(fitScoresList);
            var c1 = population[c1Idx];
            var c2 = population[c2Idx];
            var f1 = fitScores[c1Idx];
            var f2 = fitScores[c2Idx];

            // cross
            var result = Cross(c1, c2);
            RPN<Symbol> offspring;
            if (result != null) {
              offspring = result.Item1.NMSE < result.Item2.NMSE ? result.Item1 : result.Item2;
            } else {
              offspring = (RPN<Symbol>)c1.CloneDeep(); // safe fallback: use parent
            }
            populationNew[i] = offspring;

            // mutate
            if (Rng.NextDouble() < MutationRate) {
              populationNew[i] = Mutate(populationNew[i]);
            }

            // simplify 
            populationNew[i] = Simplify(populationNew[i]);

            // evaluate
            //double f = EvaluateSet(populationNew[i], doubleSet, trainingData.RowCount, TargetVariable);            
            double f = EvaluateSet(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
            localEvaluationCount++;

            // constant / coefficient optimization
            if (!double.IsNaN(f) && UseConstantOptimization) {
              //var optimizationResult = OptimizeConstants(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
              var optimizationResult = OptimizeConstants(populationNew[i], localEvaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
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
              if (f < bestFitScore) { // f > bestFitScore if Pearson R is used
                lock (bestSolutionLocker) {
                  if (f < bestFitScore) { // f > bestFitScore if Pearson R is used
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

        if (LogGenerations) Console.WriteLine($"Generation: {g+1:d4}, Evaluations: {generationalEvaluationCount:d4}, Pearson R: {bestSolution.PearsonR}, NMSE: {bestSolution.NMSE}, LD: {bestSolution.LD}");
      }

      if (LogGenerations) Console.WriteLine($"Pearson R: {bestSolution.PearsonR}, NMSE: {bestSolution.NMSE}, LD: {bestSolution.LD}");
    }

    public void Run(Set trainingData, double[] data) {
      //var doubleSet = trainingData.GetDoubleSet();
      double[] fitScores = population.Select(x => x.PearsonR).ToArray();
      RPN<Symbol>[] populationNew = population.Select(pi => (RPN<Symbol>)pi.Clone()).ToArray();
      double[] fitScoresNew = new double[PopulationSize];
      double bestFitScore = population.First().PearsonR;
      var bestSolution = (RPN<Symbol>)population.First().CloneDeepWithResults(); // take a random solution (don't care)

      double currentSelectionPressure = 0.0;
      double sumFitScores = fitScores.Sum();
      var fitScoresList = fitScores.ToList();

      for (int g = 0; g < Generations && currentSelectionPressure < MaximumSelectionPressure; g++) // g = generation
      {
        int i = Elites;
        int generationalEvaluationCount = 0;
        do {
          var c1Idx = SelectProportionalIdx(fitScoresList, sumFitScores);
          var c2Idx = SelectProportionalIdx(fitScoresList, sumFitScores);
          var c1 = population[c1Idx];
          var c2 = population[c2Idx];
          var f1 = fitScores[c1Idx];
          var f2 = fitScores[c2Idx];

          var result = Cross(c1, c2);
          if (result != null) {
            populationNew[i] = result.Item1.PearsonR > result.Item2.PearsonR ? result.Item1 : result.Item2;

          }

          if (Rng.NextDouble() < MutationRate) {
            populationNew[i] = Mutate(populationNew[i]);
          }

          // Evaluate
          //double f = EvaluateSet(populationNew[i], doubleSet, trainingData.RowCount, TargetVariable);
          double f = EvaluateSet(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
          generationalEvaluationCount++;

          // perform constant optimization
          if (!double.IsNaN(f) && UseConstantOptimization) {
            var optimizationResult = OptimizeConstantsViaEvolutionStrategy(populationNew[i], evaluationBuffer, data, trainingData.RowCount, targetVariableIdx);
            populationNew[i] = optimizationResult.Item1;
            f = optimizationResult.Item2;
          }

          if (!double.IsNaN(f)) {
            //if (f > Math.Min(f1, f2)) { // OS            
            if (f < bestFitScore) {
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

        // keep elite (1)
        populationNew[0] = (RPN<Symbol>)bestSolution.CloneDeepWithResults();
        fitScoresNew[0] = bestFitScore;

        EvaluationCount += generationalEvaluationCount;

        if (LogGenerations) Console.WriteLine($"Generation: {g:d4}, Evaluations: {generationalEvaluationCount:d4}, Selection Pressure: {currentSelectionPressure:f2}, Score: {bestFitScore:f12}");
      }
    }

    private Symbol GetTerminal(ref int c) {
      double rndD = Rng.NextDouble();
      int rndI;

      if (rndD < 0.75) {
        rndI = Rng.Next(0, InputVariables.Count);
        string varName = InputVariables[rndI];
        return new Symbol(new Variable(varName, VariableIndices[varName], 1.0));
      }
      else {
        rndI = Rng.Next(0, VariableLimitsDict.Count);
        c++;
        return new Symbol(new Constant(
              $"c{c}",
              Rng.NextDouble(VariableLimitsDict.ElementAt(rndI).Value.Item1, VariableLimitsDict.ElementAt(rndI).Value.Item2)
            ));
      }
    }

    private Symbol GetVariable() {
      int rndI = Rng.Next(0, InputVariables.Count);
      string varName = InputVariables[rndI];
      return new Symbol(new Variable(InputVariables[rndI], VariableIndices[varName], 1.0));
    }

    public RPN<Symbol> Breed(int initialEvaluationCapacity = 0) {
      var p = new RPN<Symbol>(SymbolCount, initialEvaluationCapacity);
      int aritySum = 0, arityCount = 0, tCount = 0;
      int constantCounter = 0;
      double rndD;

      Symbol curSmybol = GetTerminal(ref constantCounter);
      p.Add(curSmybol);
      tCount++;


      while (p.Count < SymbolCount)
      //while (p.Count < TreeLength || (tCount > 1 || tCount == 1 && curSmybol.Type != SymbolType.Operator))
      {
        rndD = Rng.NextDouble();
        if (rndD > operatorToOperandRatio) {
          // add terminal (variable or constant) to the program leafs
          curSmybol = GetTerminal(ref constantCounter);
          p.Add(curSmybol);
          tCount++;
        }
        else {
          curSmybol = new Symbol(Operators.SelectRandom(Rng));
          if (tCount >= curSmybol.Opr.Arity) {
            tCount -= curSmybol.Opr.Arity - 1;
            p.Add(curSmybol);
          }
        }
      }

      while (tCount > 1) {
        curSmybol = new Symbol(Operators.SelectRandom(Rng));
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
      return pop[Rng.Next(pop.Length)];
    }

    public int SelectRandomIdx(int populationSize) {
      return Rng.Next(populationSize);
    }

    public int SelectRandomIdx(List<double> score) {
      return Rng.Next(score.Count);
    }

    public int SelectProportionalIdx(List<double> score, double sum) {

      double rnd = Rng.NextDouble() * sum;
      double cumulative = 0.0;
      for (int i = 0; i < score.Count; i++) {
        cumulative += score[i];
        if (rnd < cumulative) return i;
      }
      return score.Count - 1; // should not happen, but just in case of rounding errors
    }

    // NMSE variant: lower scores are better. Weights are w = 1/(1+NMSE) so that
    // solutions closer to 0 receive a higher selection probability.
    public int SelectProportionalIdxNMSE(List<double> score, double sum) {
      double rnd = Rng.NextDouble() * sum;
      double cumulative = 0.0;
      for (int i = 0; i < score.Count; i++) {
        cumulative += 1.0 / (1.0 + score[i]);
        if (rnd < cumulative) return i;
      }
      return score.Count - 1; // should not happen, but just in case of rounding errors
    }

    public Tuple<RPN<Symbol>, RPN<Symbol>> Cross(RPN<Symbol> a, RPN<Symbol> b) {
      // TODO 1: this can be optimized by doing the subtree extraction and insertion in-place without creating new lists (just move the symbols around in the existing lists)
      // TODO 2: change to arbitrary symbol types instead of just operators (e.g. also allow crossing at terminals)

      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();

      int aIdx = Rng.Next(0, a.Count);
      int aUpperBound = a.FindIndex(aIdx, x => x.Type == SymbolType.Operator);

      if (aUpperBound != -1) {
        //var aOp = (Operator)a[idxAOp];
        int aLowerBound = FindSubtreeLimit(a, aUpperBound);
        var aSubtree = a.GetRange(aLowerBound, aUpperBound - aLowerBound + 1);

        int bIdx = Rng.Next(0, b.Count);
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
      // TODO: this can be optimized by doing the mutation in-place without creating new lists (just move the symbols around in the existing list)
      // TODO: allow insertions (e.g. insert a new random subtree at a random position, as in Breed(..))

      var p = o.CloneDeep();
      int idx = Rng.Next(0, o.Count); // uniformely distributed
      var obj = p[idx];

      if (p[idx].Type == SymbolType.Constant) {
        var val = p[idx].Con.Value;
        var ratio = Rng.NextDouble() * 0.1;
        if (Rng.NextDouble() < 0.5) p[idx].Con.Value = val + val * ratio;
        else p[idx].Con.Value = val - val * ratio;
      }
      else if (p[idx].Type == SymbolType.Variable) {
        p[idx] = GetVariable();
      }
      else // operator
        {
        var ratio = Rng.NextDouble();
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
          p[idx].Opr = Operators.SelectRandomDifferent(Rng, p[idx].Opr);
        }
      }

      return p;
    }

    public RPN<Symbol> Simplify(RPN<Symbol> o) {
      var p = o.CloneDeep();
      bool changed = true;

      while (changed) {
        changed = false;
        for (int i = 0; i < p.Count; i++) {
          if (p[i].Type != SymbolType.Operator) continue;

          var opr = p[i].Opr;

          if (opr.Arity == 1) {
            int argEnd   = i - 1;
            int argStart = p[argEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, argEnd) : argEnd;

            if (IsPureConstantSubtree(p, argStart, argEnd)) {
              double val = EvaluateConstantSubtree(p, argStart, i);
              if (!double.IsNaN(val) && !double.IsInfinity(val)) {
                ReplaceWithConstant(p, argStart, i, val);
                changed = true; break;
              }
            }
          } else { // arity == 2
            int rightEnd   = i - 1;
            int rightStart = p[rightEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, rightEnd) : rightEnd;
            int leftEnd    = rightStart - 1;
            int leftStart  = p[leftEnd].Type  == SymbolType.Operator ? FindSubtreeLimit(p, leftEnd)  : leftEnd;

            bool leftIsConst  = IsPureConstantSubtree(p, leftStart,  leftEnd);
            bool rightIsConst = IsPureConstantSubtree(p, rightStart, rightEnd);

            // constant folding: both sides are constant-only subtrees
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

            if (opr.Symbol == "+") {
              if (rightIsSingleConst && rightVal == 0.0) {          // x + 0 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 0.0) {            // 0 + x => x
                p.RemoveAt(i); p.RemoveAt(leftStart); changed = true; break;
              }
              if (rightIsSingleConst && rightVal < 0.0) {           // x + (-c) => x - c
                p[rightStart].Con.Value = -rightVal;
                p[i] = new Symbol(Operators.Subtraction); changed = true; break;
              }
              // (x + c1) + c2 => x + (c1+c2)  [right-const chain]
              if (rightIsSingleConst && p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "+" ) {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  double merged = p[lrStart].Con.Value + rightVal;
                  p.RemoveRange(rightStart, i - rightStart + 1); // remove outer op + right const
                  p[lrStart].Con.Value = merged;                 // update inner const
                  changed = true; break;
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
                  double merged = p[lrStart].Con.Value + rightVal;
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value = merged;
                  changed = true; break;
                }
              }
              // (x + b) - b => x  and  (x - b) + b => x  (cancel addend)
              if (p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "+") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && SubtreesAreEqual(p, lrStart, lrEnd, rightStart, rightEnd)) {
                  // remove right subtree, inner right subtree, and both operators
                  p.RemoveRange(rightStart, i - rightStart + 1); // remove outer right + op
                  // now left subtree is at leftStart..leftEnd (shifted), redo from scratch
                  changed = true; break;
                }
              }
            } else if (opr.Symbol == "*") {
              if (rightIsSingleConst && rightVal == 1.0) {          // x * 1 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 1.0) {            // 1 * x => x
                p.RemoveAt(i); p.RemoveAt(leftStart); changed = true; break;
              }
              if (rightIsSingleConst && rightVal == 0.0) {          // x * 0 => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 0.0) {            // 0 * x => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (rightIsSingleConst && rightVal == -1.0) {         // x * -1 => 0 - x (use 0-x pattern)
                p[rightStart].Con.Value = 0.0;
                p[i] = new Symbol(Operators.Subtraction); changed = true; break;
              }
              // (x * c1) * c2 => x * (c1*c2)
              if (rightIsSingleConst && p[leftEnd].Type == SymbolType.Operator && p[leftEnd].Opr.Symbol == "*") {
                int llEnd = leftEnd - 1; int llStart = p[llEnd].Type == SymbolType.Operator ? FindSubtreeLimit(p, llEnd) : llEnd;
                int lrEnd = llStart - 1; int lrStart = lrEnd;
                if (lrEnd >= leftStart && lrStart == lrEnd && p[lrStart].Type == SymbolType.Constant) {
                  double merged = p[lrStart].Con.Value * rightVal;
                  p.RemoveRange(rightStart, i - rightStart + 1);
                  p[lrStart].Con.Value = merged;
                  changed = true; break;
                }
              }
            } else if (opr.Symbol == "/") {
              if (rightIsSingleConst && rightVal == 1.0) {          // x / 1 => x
                p.RemoveRange(rightStart, i - rightStart + 1); changed = true; break;
              }
              if (leftIsSingleConst && leftVal == 0.0) {            // 0 / x => 0
                ReplaceWithConstant(p, leftStart, i, 0.0); changed = true; break;
              }
              if (rightIsSingleConst && rightVal != 0.0) {          // x / c => x * (1/c)
                p[rightStart].Con.Value = 1.0 / rightVal;
                p[i] = new Symbol(Operators.Multiplication); changed = true; break;
              }
              if (SubtreesAreEqual(p, leftStart, leftEnd, rightStart, rightEnd)) { // x / x => 1
                ReplaceWithConstant(p, leftStart, i, 1.0); changed = true; break;
              }
            }
          }
        }
      }

      return p;
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
      p.NMSE = Statistics.NMSE(p.TrueResults, p.EstimatedResults);
      p.MRE = Statistics.MRE(p.TrueResults, p.EstimatedResults);
      //p.PearsonR = Statistics.PearsonR(p.TrueResults, p.EstimatedResults);
      //var absoluteErrors = p.TrueResult.Zip(p.EstimatedValues, (t, e) => t - e);
      //p.MAE = absoluteErrors.Mean();
      return p.NMSE;
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
          //else evaluationBufferPush(operation.Function(new[] { evaluationBuffer.Pop(), evaluationBuffer.Pop() }));
        }
      }
      var result = evaluationBuffer.Pop();
      evaluationBuffer.Clear();
      p.TrueResults[idx] = data[targetVariableIdx * rowCount + idx];
      p.EstimatedResults[idx] = result;
      return p.TrueResults[idx] - result;
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
      int n = p.TrueResults.Length;

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

    public double PenalizePearsonR(RPN<Symbol> o) {
      o.PearsonR = o.Count > SymbolCount ? o.PearsonR * (SymbolCount / (double)o.Count) : o.PearsonR;
      return o.PearsonR;
    }

    public double PenalizeNMSE(RPN<Symbol> o) {
      o.NMSE = o.Count > SymbolCount ? o.NMSE * ((double)o.Count / SymbolCount) : o.NMSE;
      return o.NMSE;
    }

    public Tuple<RPN<Symbol>, double> OptimizeConstantsViaEvolutionStrategy(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx) {      
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
        executionOrder = executionOrder.ShuffleFisherYates(Rng).ToArray();
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

    public Tuple<RPN<Symbol>, double> OptimizeConstants(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx) {
      var p = o.CloneDeep();
      double pFit = EvaluateSet(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);

      var constAndIdx  = ParseConstants(p);
      double[] constants    = constAndIdx.Item1;
      int[]    constIndices = constAndIdx.Item2;

      int n = constants.Length;
      if (n == 0) return Tuple.Create(p, pFit);

      const double h             = 1e-4;
      const double learningRate  = 0.01;
      const int    maxIterations = 10;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        for (int i = 0; i < n; i++) {
          var pPlus  = p.CloneDeep();
          var pMinus = p.CloneDeep();

          UpdateConstant(pPlus,  constants[i] + h, constIndices[i]);
          UpdateConstant(pMinus, constants[i] - h, constIndices[i]);

          double fPlus  = EvaluateSet(pPlus,  localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
          double fMinus = EvaluateSet(pMinus, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = (fPlus - fMinus) / (2.0 * h);
        }

        for (int i = 0; i < n; i++)
          constants[i] += learningRate * gradient[i];

        var pNew   = p.CloneDeep();
        UpdateConstants(pNew, constants, constIndices);

        double newFit = EvaluateSet(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
        if (!double.IsNaN(newFit) && newFit > pFit) {
          p    = pNew;
          pFit = newFit;
        } else {
          break;
        }
      }

      return Tuple.Create(p, pFit);
    }

    public Tuple<RPN<Symbol>, double> OptimizeCoefficientsAndConstants(RPN<Symbol> o, Stack<double> localEvaluationBuffer, double[] data, int trainingDataRowCount, int targetVariableIdx) {
      var p = o.CloneDeep();
      double pFit = EvaluateSet(p, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);

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
      const int    maxIterations = 10; // 200

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

          double fPlus  = EvaluateSet(pPlus,  localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
          double fMinus = EvaluateSet(pMinus, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = (fPlus - fMinus) / (2.0 * h);
        }

        // gradient ascent step (maximising PearsonR)
        bool improved = false;
        for (int i = 0; i < n; i++) {
          if (i < constants.Length)
            constants[i] += learningRate * gradient[i];
          else
            coefficients[i - constants.Length] += learningRate * gradient[i];
        }

        // apply updated parameters
        var pNew = p.CloneDeep();
        UpdateConstants(pNew, constants, constIndices);
        UpdateCoefficients(pNew, coefficients, coefIndices);

        double newFit = EvaluateSet(pNew, localEvaluationBuffer, data, trainingDataRowCount, targetVariableIdx);
        if (!double.IsNaN(newFit) && newFit > pFit) {
          p = pNew;
          pFit = newFit;
          improved = true;
        }

        if (!improved) break;
      }

      return Tuple.Create(p, pFit);
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

  }

}
