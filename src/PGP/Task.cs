using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {

  public enum Metric {
    PearsonR = 0,
    PearsonR2 = 1,
    NMSE = 2,
    MRE = 3,
    LD = 4
  }

  public enum OptimizationDirection {
    Maximize,
    Minimize
  }

  public interface IScore {
    string Name { get; }
    Metric Metric { get; }
    OptimizationDirection Direction { get; }

    double GetMaxValue();
    double GetMinValue();
    double GetOptimum();
    double GetPessimal();
    bool IsBetter(double score1, double score2);
    double GetScoreSum(double[] scores);
    double GetScoreCummulative(double score);    
    public static double ComputeScore(RPN<Symbol> p) => throw new NotImplementedException("Use specific metric implementations to compute scores.");
    public double Compute(RPN<Symbol> p);
  }

  public class PearsonR : IScore {
    public string Name { get => "PearsonR"; }
    public Metric Metric { get => Metric.PearsonR; }
    public OptimizationDirection Direction { get => OptimizationDirection.Maximize; }

    public double GetMaxValue() {
      return 1.0;
    }

    public double GetMinValue() {
      return -1.0;
    }

    public double GetOptimum() {
      return 1.0;
    }

    public double GetPessimal() {
      return -1.0;
    }

    public bool IsBetter(double score1, double score2) {
      return score1 > score2;
    }

    public double GetScoreSum(double[] scores) {
      return scores.Sum();
    }

    public double GetScoreCummulative(double score) {
      return score;
    }    

    public static double ComputeScore(RPN<Symbol> p) {
      double r = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
      p.PearsonR = r;
      return r;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

  public class PearsonR2 : IScore {
    public string Name { get => "PearsonR2"; }
    public Metric Metric { get => Metric.PearsonR2; }
    public OptimizationDirection Direction { get => OptimizationDirection.Maximize; }

    public double GetMaxValue() {
      return 1.0;
    }

    public double GetMinValue() {
      return 0.0;
    }

    public double GetOptimum() {
      return 1.0;
    }

    public double GetPessimal() {
      return 0.0;
    }

    public bool IsBetter(double score1, double score2) {
      return score1 > score2;
    }

    public double GetScoreSum(double[] scores) {
      return scores.Sum();
    }

    public double GetScoreCummulative(double score) {
      return score;
    }

    public static double ComputeScore(RPN<Symbol> p) {
      double r = Statistics.PearsonRFast(p.TrueResults, p.EstimatedResults);
      p.PearsonR2 = r * r;
      return p.PearsonR2;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }


  public class NMSE : IScore {
    public string Name { get => "NMSE"; }
    public Metric Metric { get => Metric.NMSE; }
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
      double nmse = Statistics.NMSE(p.TrueResults, p.EstimatedResults);
      p.NMSE = nmse;
      return nmse;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

  public class LD : IScore {
    public string Name { get => "LD"; }
    public Metric Metric { get => Metric.LD; }
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

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

  public class Task {
    public string Name { get; set; }
    public string TargetVariable { get; set; }
    public IList<string> InputVariables { get; set; }
    public Metric Metric { get; set; }
    public OptimizationDirection OptimizationDirection { get; set; }
    public Dictionary<string, int> VariableIndices { get; private set; }
    public Dictionary<string, Tuple<double, double>> VariableLimitsDict { get; set; }
    public IScore Score { get; private set; }

    private Task() { }

    public Task(string name, string targetVariable, IList<string> inputVariables, Metric metric = Metric.NMSE, OptimizationDirection optimizationDirection = OptimizationDirection.Minimize) {
      Name = name;
      TargetVariable = targetVariable;
      InputVariables = inputVariables;
      Metric = metric;
      OptimizationDirection = optimizationDirection;

      var variables = inputVariables.Append(targetVariable).ToList();
      VariableIndices = variables
        .Select((x, i) => new { Item = x, Index = i })
        .ToDictionary(x => x.Item, x => x.Index);
      VariableLimitsDict = new Dictionary<string, Tuple<double, double>>();

      switch (metric) {
        case Metric.PearsonR:
          Score = new PearsonR();
          break;
        case Metric.NMSE:
          Score = new NMSE();
          break;
        case Metric.LD:
          Score = new LD();
          break;
        default:
          throw new NotImplementedException($"Metric {metric} not implemented yet.");
      }
    }
  }
}
