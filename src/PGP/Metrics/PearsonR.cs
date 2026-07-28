using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public class PearsonR : IMetric {
    public string Name { get => "PearsonR"; }
    public EvaluationMetric Metric { get => EvaluationMetric.PearsonR; }
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
      return 0.0; // 0.0 or -1.0
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
      double r = Statistics.PearsonR(p.TrueResults, p.EstimatedResults);
      p.PearsonR = r;
      return r;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

}
