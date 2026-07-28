using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public class NMSE : IMetric {
    public string Name { get => "NMSE"; }
    public EvaluationMetric Metric { get => EvaluationMetric.NMSE; }
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
      p.NMSE = Statistics.NMSE(p.TrueResults, p.EstimatedResults);
      return p.NMSE;
    }

    public double Compute(RPN<Symbol> p) {
      return ComputeScore(p);
    }
  }

}
