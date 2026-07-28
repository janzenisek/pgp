using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public class MRE : IMetric {
    public string Name { get => "MRE"; }
    public EvaluationMetric Metric { get => EvaluationMetric.MRE; }
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
      p.MRE = Statistics.MRE(p.TrueResults, p.EstimatedResults);
      return p.MRE;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

}
