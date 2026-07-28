using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public class PearsonR2 : IMetric {
    public string Name { get => "PearsonR2"; }
    public EvaluationMetric Metric { get => EvaluationMetric.PearsonR2; }
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
      double r = Statistics.PearsonR(p.TrueResults, p.EstimatedResults);
      p.PearsonR2 = r * r;
      return p.PearsonR2;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

}
