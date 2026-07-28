using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core.Metrics {
  public interface IMetric {
    string Name { get; }
    EvaluationMetric Metric { get; }
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
}
