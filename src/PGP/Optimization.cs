using PGP.Data;
using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core {
  public class Optimization {

    public static Tuple<RPN<Symbol>, double> OptimizeConstantsViaEvolutionStrategy(PgpAlgorithm pgp, RPN<Symbol> program, Task task, DataRecord data) {
      var p = program.CloneDeep();
      double pFit = pgp.Evaluate(pgp, p, task, data);
      var pNew = p.CloneDeep();
      var pNewFit = pFit;

      var constantsAndIndices = ParseConstants(p);
      var constants = constantsAndIndices.Item1;
      var indices = constantsAndIndices.Item2;
      var constantsNew = (double[])constants.Clone();

      // Step sizes are initialised relative to each constant's magnitude so that
      // a constant of 1000 gets a meaningful perturbation, not just ±0.1.
      double[] stepSizes = constants.Select(c => Math.Max(Math.Abs(c) * 0.1, 0.1)).ToArray();
      const double stepMax = 1e4;   // hard cap — prevents runaway growth
      const double paramMax = 1e6;   // clamp constants to [-paramMax, paramMax]
      int[] executionOrder = Enumerable.Range(0, constants.Length).ToArray();

      for (int g = 0; g < pgp.OptimizationIterations; g++) {
        executionOrder = executionOrder.ShuffleFisherYates(pgp.Rng).ToArray();
        for (int i = 0; i < indices.Length; i++) {
          var idx = executionOrder[i];

          // Additive perturbation: c' = c ± step  (1/5-success rule adaptation)
          double step = stepSizes[idx];
          double delta = pgp.Rng.NextDouble() < 0.5 ? step : -step;
          double constantMutated = Math.Clamp(constantsNew[idx] + delta, -paramMax, paramMax);

          var pMutated = pNew.CloneDeep();
          UpdateConstant(pMutated, constantMutated, indices[idx]);
          var pMutatedFit = pgp.Evaluate(pgp,pMutated, task, data);

          if (!double.IsNaN(pMutatedFit) && task.Score.IsBetter(pMutatedFit, pNewFit)) {
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
      pNewFit = pgp.Evaluate(pgp,pNew, task, data);
      if (task.Score.IsBetter(pNewFit, pFit)) {
        p = pNew;
        pFit = pNewFit;
      }
      return Tuple.Create(p, pFit);
    }

    public static Tuple<RPN<Symbol>, double> OptimizeConstants(PgpAlgorithm pgp, RPN<Symbol> program, Task task, DataRecord data) {
      var p = program.CloneDeep();
      double pFit = pgp.Evaluate(pgp, p, task, data);

      var constAndIdx = ParseConstants(p);
      double[] constants = constAndIdx.Item1;
      int[] constIndices = constAndIdx.Item2;

      int n = constants.Length;
      if (n == 0) return Tuple.Create(p, pFit);

      const double h = 1e-4;
      const double learningRate = 0.01;
      int maxIterations = pgp.OptimizationIterations;
      const double gradClip = 1e3;  // clip individual gradient components
      const double paramMax = 1e6;  // clamp constants to [-paramMax, paramMax]
      double gradientSign = task.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        for (int i = 0; i < n; i++) {
          var pPlus = p.CloneDeep();
          var pMinus = p.CloneDeep();

          UpdateConstant(pPlus, constants[i] + h, constIndices[i]);
          UpdateConstant(pMinus, constants[i] - h, constIndices[i]);

          double fPlus = pgp.Evaluate(pgp, pPlus, task, data);
          double fMinus = pgp.Evaluate(pgp, pMinus, task, data);
          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
        }

        for (int i = 0; i < n; i++)
          constants[i] = Math.Clamp(constants[i] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);

        var pNew = p.CloneDeep();
        UpdateConstants(pNew, constants, constIndices);

        double newFit = pgp.Evaluate(pgp, pNew, task, data);
        if (!double.IsNaN(newFit) && task.Score.IsBetter(newFit, pFit)) {
          p = pNew;
          pFit = newFit;
        } else {
          break;
        }
      }

      return Tuple.Create(p, pFit);
    }

    public static Tuple<RPN<Symbol>, double> OptimizeCoefficientsAndConstants(PgpAlgorithm pgp, RPN<Symbol> program, Task task, DataRecord data) {
      var p = program.CloneDeep();
      double pFit = pgp.Evaluate(pgp, p, task, data);

      var constAndIdx = ParseConstants(p);
      var coefAndIdx = ParseCoefficients(p);

      double[] constants = constAndIdx.Item1;
      int[] constIndices = constAndIdx.Item2;
      double[] coefficients = coefAndIdx.Item2;
      int[] coefIndices = coefAndIdx.Item3;

      int n = constants.Length + coefficients.Length;
      if (n == 0) return Tuple.Create(p, pFit);

      const double h = 1e-4;   // finite-difference step
      const double learningRate = 0.01;
      int maxIterations = pgp.OptimizationIterations;
      const double gradClip = 1e3;  // clip individual gradient components
      const double paramMax = 1e6;  // clamp parameters to [-paramMax, paramMax]
      double gradientSign = task.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        // central finite differences for each parameter
        for (int i = 0; i < n; i++) {
          var pPlus = p.CloneDeep();
          var pMinus = p.CloneDeep();

          if (i < constants.Length) {
            UpdateConstant(pPlus, constants[i] + h, constIndices[i]);
            UpdateConstant(pMinus, constants[i] - h, constIndices[i]);
          } else {
            int ci = i - constants.Length;
            UpdateCoefficient(pPlus, coefficients[ci] + h, coefIndices[ci]);
            UpdateCoefficient(pMinus, coefficients[ci] - h, coefIndices[ci]);
          }

          double fPlus = pgp.Evaluate(pgp, pPlus, task, data);
          double fMinus = pgp.Evaluate(pgp, pMinus, task, data);
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

        double newFit = pgp.Evaluate(pgp, pNew, task, data);
        if (!double.IsNaN(newFit) && task.Score.IsBetter(newFit, pFit)) {
          p = pNew;
          pFit = newFit;
          improved = true;
        }

        if (!improved) break;
      }

      return Tuple.Create(p, pFit);
    }






    private static Tuple<double[], int[]> ParseConstants(RPN<Symbol> p) {
      var constants = new List<double>();
      var indices = new List<int>();

      for (int i = 0; i < p.Count; i++) {
        var s = p[i];
        if (s.Type == SymbolType.Constant) {
          constants.Add(s.Con.Value);
          indices.Add(i);
        }
      }

      return Tuple.Create(constants.ToArray(), indices.ToArray());
    }

    private static Tuple<string[], double[], int[]> ParseCoefficients(RPN<Symbol> p) {
      var variables = new List<string>();
      var coefficients = new List<double>();
      var indices = new List<int>();

      for (int i = 0; i < p.Count; i++) {
        var s = p[i];
        if (s.Type == SymbolType.Variable) {
          variables.Add(s.Var.Name);
          coefficients.Add(s.Var.Coefficient);
          indices.Add(i);
        }
      }

      return Tuple.Create(variables.ToArray(), coefficients.ToArray(), indices.ToArray());
    }

    private static void UpdateConstants(RPN<Symbol> p, double[] constants, int[] indices) {
      for (int i = 0; i < indices.Length; i++) {
        p[indices[i]].Con.Value = constants[i];
      }
    }

    private static void UpdateConstant(RPN<Symbol> p, double constant, int index) {
      p[index].Con.Value = constant;
    }

    private static void UpdateCoefficient(RPN<Symbol> p, double coefficient, int index) {
      p[index].Var.Coefficient = coefficient;
    }

    private static void UpdateCoefficients(RPN<Symbol> p, double[] coefficients, int[] indices) {
      for (int i = 0; i < indices.Length; i++) {
        p[indices[i]].Var.Coefficient = coefficients[i];
      }
    }

  }
}
