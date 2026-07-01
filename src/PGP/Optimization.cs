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

      // Keep a separate "best" value array; pNew tracks the running best program.
      var pNew = p.CloneDeepWithResults();
      pNew.CompiledDelegate = p.CompiledDelegate; // carry compiled delegate to pNew
      var pNewFit = pFit;

      var constantsAndIndices = ParseConstants(p);
      var constants = constantsAndIndices.Item1;
      var indices = constantsAndIndices.Item2;
      var constantsNew = (double[])constants.Clone();

      double[] stepSizes = constants.Select(c => Math.Max(Math.Abs(c) * 0.1, 0.1)).ToArray();
      const double stepMax = 1e4;
      const double paramMax = 1e6;
      int[] executionOrder = Enumerable.Range(0, constants.Length).ToArray();

      for (int g = 0; g < pgp.OptimizationIterations; g++) {
        executionOrder = executionOrder.ShuffleFisherYates(pgp.Rng).ToArray();
        for (int i = 0; i < indices.Length; i++) {
          var idx = executionOrder[i];

          double step = stepSizes[idx];
          double delta = pgp.Rng.NextDouble() < 0.5 ? step : -step;
          double originalValue = constantsNew[idx];
          double constantMutated = Math.Clamp(originalValue + delta, -paramMax, paramMax);

          // --- Cause 2 fix: mutate in-place, evaluate, then restore ---
          UpdateConstant(pNew, constantMutated, indices[idx]);
          var pMutatedFit = pgp.Evaluate(pgp, pNew, task, data);

          if (!double.IsNaN(pMutatedFit) && task.Score.IsBetter(pMutatedFit, pNewFit)) {
            constantsNew[idx] = constantMutated;
            stepSizes[idx] = Math.Min(stepSizes[idx] * 1.5, stepMax);
            pNewFit = pMutatedFit;
          } else {
            // Restore to the previous best value
            UpdateConstant(pNew, originalValue, indices[idx]);
            stepSizes[idx] *= Math.Pow(1.5, -0.25);
          }
        }
      }

      // Final sanity check and swap
      pNewFit = pgp.Evaluate(pgp, pNew, task, data);
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
      const double gradClip = 1e3;
      const double paramMax = 1e6;
      double gradientSign = task.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        // --- Cause 2 fix: in-place +h / -h probes; no CloneDeep inside the loop ---
        for (int i = 0; i < n; i++) {
          double orig = constants[i];

          UpdateConstant(p, orig + h, constIndices[i]);
          double fPlus = pgp.Evaluate(pgp, p, task, data);

          UpdateConstant(p, orig - h, constIndices[i]);
          double fMinus = pgp.Evaluate(pgp, p, task, data);

          // Restore original value
          UpdateConstant(p, orig, constIndices[i]);

          if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
          gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
        }

        // Apply gradient step in-place and check improvement
        double[] prevConstants = (double[])constants.Clone();
        for (int i = 0; i < n; i++)
          constants[i] = Math.Clamp(constants[i] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);

        UpdateConstants(p, constants, constIndices);
        double newFit = pgp.Evaluate(pgp, p, task, data);

        if (!double.IsNaN(newFit) && task.Score.IsBetter(newFit, pFit)) {
          pFit = newFit;
        } else {
          // Revert constants to the previous best
          constants = prevConstants;
          UpdateConstants(p, constants, constIndices);
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

      const double h = 1e-4;
      const double learningRate = 0.01;
      int maxIterations = pgp.OptimizationIterations;
      const double gradClip = 1e3;
      const double paramMax = 1e6;
      double gradientSign = task.Score.Direction == OptimizationDirection.Maximize ? 1.0 : -1.0;

      for (int iter = 0; iter < maxIterations; iter++) {
        var gradient = new double[n];

        // --- Cause 2 fix: in-place +h / -h probes for both constants and coefficients ---
        for (int i = 0; i < n; i++) {
          if (i < constants.Length) {
            double orig = constants[i];
            UpdateConstant(p, orig + h, constIndices[i]);
            double fPlus = pgp.Evaluate(pgp, p, task, data);
            UpdateConstant(p, orig - h, constIndices[i]);
            double fMinus = pgp.Evaluate(pgp, p, task, data);
            UpdateConstant(p, orig, constIndices[i]); // restore
            if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
            gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
          } else {
            int ci = i - constants.Length;
            double orig = coefficients[ci];
            UpdateCoefficient(p, orig + h, coefIndices[ci]);
            double fPlus = pgp.Evaluate(pgp, p, task, data);
            UpdateCoefficient(p, orig - h, coefIndices[ci]);
            double fMinus = pgp.Evaluate(pgp, p, task, data);
            UpdateCoefficient(p, orig, coefIndices[ci]); // restore
            if (double.IsNaN(fPlus) || double.IsNaN(fMinus)) continue;
            gradient[i] = Math.Clamp((fPlus - fMinus) / (2.0 * h), -gradClip, gradClip);
          }
        }

        // Snapshot before applying step so we can revert on no-improvement
        double[] prevConstants = (double[])constants.Clone();
        double[] prevCoefficients = (double[])coefficients.Clone();

        for (int i = 0; i < n; i++) {
          if (i < constants.Length)
            constants[i] = Math.Clamp(constants[i] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);
          else
            coefficients[i - constants.Length] = Math.Clamp(coefficients[i - constants.Length] + gradientSign * learningRate * gradient[i], -paramMax, paramMax);
        }

        // Apply updated parameters in-place and evaluate
        UpdateConstants(p, constants, constIndices);
        UpdateCoefficients(p, coefficients, coefIndices);
        double newFit = pgp.Evaluate(pgp, p, task, data);

        if (!double.IsNaN(newFit) && task.Score.IsBetter(newFit, pFit)) {
          pFit = newFit;
        } else {
          // Revert to previous best values in-place
          constants = prevConstants;
          coefficients = prevCoefficients;
          UpdateConstants(p, constants, constIndices);
          UpdateCoefficients(p, coefficients, coefIndices);
          break;
        }
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
