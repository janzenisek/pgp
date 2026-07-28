using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public class LD : IMetric {
    public string Name { get => "LD"; }
    public EvaluationMetric Metric { get => EvaluationMetric.LD; }
    public OptimizationDirection Direction { get => OptimizationDirection.Minimize; }

    public double GetMaxValue() {
      return double.MaxValue;
    }

    public double GetMinValue() {
      return 0.0;
    }

    public double GetOptimum() {
      return 0.0;
    }

    public double GetPessimal() {
      return double.MaxValue;
    }

    public bool IsBetter(double score1, double score2) {
      return score1 < score2;
    }

    public double GetScoreSum(double[] scores) {
      return scores.Select(s => 1.0 / (1.0 + s)).Sum();
    }

    public double GetScoreCummulative(double score) {
      return 1.0 / (1.0 + score);
    }

    public static double ComputeScore(RPN<Symbol> p) {
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
      // Both log terms are invariant across rows — hoist them out of the loop instead of
      // recomputing (and re-calling Math.Log) n times per evaluation.
      double halfLog2Pi = 0.5 * Math.Log(2.0 * Math.PI);
      double logSigma = Math.Log(sigma);
      for (int i = 0; i < n; i++) {
        double r = p.EstimatedResults[i] - p.TrueResults[i];
        negloglike += 0.5 * r * r / sigma2 + halfLog2Pi + logSigma;
      }

      // ── Total description length ────────────────────────────────────────────
      p.LD = aifeyn + codelength + negloglike;
      return p.LD;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

}
