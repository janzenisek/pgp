using PGP.Core.Metrics;
using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {

  public enum EvaluationMetric {
    PearsonR = 0,
    PearsonR2 = 1,
    NMSE = 2,
    RMSE = 3,
    MRE = 4,
    MAE = 5,
    LD = 6
  }

  public enum OptimizationDirection {
    Maximize,
    Minimize
  }

  public struct OptimizationTarget {
    public const string Accuracy = "Accuracy"; // e.g., NMSE, RMSE, MRE, MAE, LD
    public const string Complexity = "Complexity"; // e.g., number of nodes in the expression tree (goal: usually minimize)
    public const string Diversity = "Diversity"; // e.g., distance from other solutions in the population (goal: usually maximize)
  }

  public class Task {
    public string Name { get; set; }
    public string TargetVariable { get; private set; }
    public HashSet<string> OptimizationTargets { get; set; }
    public Dictionary<string, double> OptimizationTargetWeights { get; set; } // [0-1] weights for each optimization target
    public IList<string> InputVariables { get; set; }
    public EvaluationMetric Metric { get; set; }
    public OptimizationDirection OptimizationDirection { get; set; }
    public Dictionary<string, int> VariableIndices { get; private set; }
    public Dictionary<string, Tuple<double, double>> VariableLimitsDict { get; set; }
    public IMetric Score { get; private set; }

    private Task() { }

    public Task(string name, IList<string> inputVariables, HashSet<string> optimizationTargets, EvaluationMetric metric = EvaluationMetric.NMSE, OptimizationDirection optimizationDirection = OptimizationDirection.Minimize, Dictionary<string, double> optimizationTargetWeights = null) {
      Name = name;
      InputVariables = inputVariables;
      TargetVariable = optimizationTargets.First();
      Metric = metric;
      OptimizationDirection = optimizationDirection;
      OptimizationTargets = optimizationTargets;
      if(optimizationTargetWeights != null) {
        OptimizationTargetWeights = optimizationTargetWeights;
      } else {
        OptimizationTargetWeights = new Dictionary<string, double>();
        foreach(var t in OptimizationTargets) {
          OptimizationTargetWeights.Add(t, 1.0/OptimizationTargets.Count);
        }
      }

      var variables = inputVariables.Append(TargetVariable).ToList();
      VariableIndices = variables
        .Select((x, i) => new { Item = x, Index = i })
        .ToDictionary(x => x.Item, x => x.Index);
      VariableLimitsDict = new Dictionary<string, Tuple<double, double>>();



      switch (metric) {
        case EvaluationMetric.PearsonR:
          Score = new PearsonR();
          break;
        case EvaluationMetric.PearsonR2:
          Score = new PearsonR2();
          break;
        case EvaluationMetric.NMSE:
          Score = new NMSE();
          break;
        case EvaluationMetric.RMSE:
          Score = new RMSE();
          break;
        case EvaluationMetric.MRE:
          Score = new MRE();
          break;
        case EvaluationMetric.MAE:
          Score = new MAE();
          break;
        case EvaluationMetric.LD:
          Score = new LD();
          break;
        default:
          throw new NotImplementedException($"Metric {metric} not implemented yet.");
      }
    }
  }
}
