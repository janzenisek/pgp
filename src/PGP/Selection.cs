using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Formats.Asn1.AsnWriter;

namespace PGP.Core {

  public class Selection {

    public static FastRandom Rng = new FastRandom();

    // stored values
    public static double fitScoreSum = 0;
    public static List<double> fitScores = new List<double>();

    public static Tuple<RPN<Symbol>, int> RandomSelection(RPN<Symbol>[] population, Task task) {
      var index = Rng.Next(population.Length);
      return Tuple.Create(population[index], index);
    }

    public static Tuple<RPN<Symbol>, int> ProportionalSelection(RPN<Symbol>[] population, Task task) {
      double rnd = Rng.NextDouble() * fitScoreSum;
      double cumulative = 0.0;
      for (int i = 0; i < fitScores.Count; i++) {
        cumulative += fitScores[i];
        if (rnd < cumulative) return Tuple.Create(population[i], i);
      }
      return Tuple.Create(population[fitScores.Count - 1], fitScores.Count - 1); // should not happen, but just in case of rounding errors
    }


    public static int tournamentSize = 3;
    public static Tuple<RPN<Symbol>, int> TournamentSelection(RPN<Symbol>[] population, Task task) {      
      var tournament = new List<Tuple<RPN<Symbol>, int, double>>();
      for (int j = 0; j < tournamentSize; j++) {
        int idx = Rng.Next(population.Length);
        tournament.Add(Tuple.Create(population[idx], idx, fitScores[idx]));
      }      
      var best = tournament.OrderBy(p => p.Item3).First();

      return Tuple.Create(best.Item1, best.Item2);
    } 

  }
}
