using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Formats.Asn1.AsnWriter;

namespace PGP.Core.Operators {

  public class Selection {  

    // stored values
    public static double fitScoreSum = 0;
    public static List<double> fitScores = new List<double>();

    public static Tuple<RPN<Symbol>, int> RandomSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {
      var index = pgp.Rng.Next(population.Length);
      return Tuple.Create(population[index], index);
    }

    public static Tuple<RPN<Symbol>, int> ProportionalSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {
      double rnd = pgp.Rng.NextDouble() * fitScoreSum;
      double cumulative = 0.0;
      for (int i = 0; i < fitScores.Count; i++) {
        cumulative += fitScores[i];
        if (rnd < cumulative) return Tuple.Create(population[i], i);
      }
      return Tuple.Create(population[fitScores.Count - 1], fitScores.Count - 1); // should not happen, but just in case of rounding errors
    }
    
    public static Tuple<RPN<Symbol>, int> TournamentSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {      
      var tournament = new List<Tuple<RPN<Symbol>, int, double>>();
      for (int j = 0; j < pgp.TournamentSize; j++) {
        int idx = pgp.Rng.Next(population.Length);
        tournament.Add(Tuple.Create(population[idx], idx, fitScores[idx]));
      }      
      var best = tournament.OrderBy(p => p.Item3).First();

      return Tuple.Create(best.Item1, best.Item2);
    } 

  }
}
