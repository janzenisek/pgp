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

  public class Task {
    public string Name { get; set; }
    public string TargetVariable { get; set; }
    public IList<string> InputVariables { get; set; }
    public EvaluationMetric Metric { get; set; }
    public OptimizationDirection OptimizationDirection { get; set; }
    public Dictionary<string, int> VariableIndices { get; private set; }
    public Dictionary<string, Tuple<double, double>> VariableLimitsDict { get; set; }
    public IMetric Score { get; private set; }

    private Task() { }

    public Task(string name, string targetVariable, IList<string> inputVariables, EvaluationMetric metric = EvaluationMetric.NMSE, OptimizationDirection optimizationDirection = OptimizationDirection.Minimize) {
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
