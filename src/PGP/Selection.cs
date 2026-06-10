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



    public static List<RPN<Symbol>> RouletteWheelSelection(List<RPN<Symbol>> population, IScore score) {
      var selected = new List<RPN<Symbol>>();
      var random = new Random();
      var scores = population.Select(p => score.Compute(p)).ToArray();
      var totalScore = scores.Sum();
      for (int i = 0; i < population.Count; i++) {
        var pick = random.NextDouble() * totalScore;
        var cumulative = 0.0;
        for (int j = 0; j < population.Count; j++) {
          cumulative += scores[j];
          if (cumulative > pick) {
            selected.Add(population[j]);
            break;
          }
        }
      }
      return selected;
    }

    public static List<RPN<Symbol>> TournamentSelection(List<RPN<Symbol>> population, IScore score, int tournamentSize) {
      var selected = new List<RPN<Symbol>>();
      var random = new Random();
      for (int i = 0; i < population.Count; i++) {
        var tournament = new List<RPN<Symbol>>();
        for (int j = 0; j < tournamentSize; j++) {
          tournament.Add(population[random.Next(population.Count)]);
        }
        var best = tournament.OrderByDescending(p => score.Compute(p)).First();
        selected.Add(best);
      }
      return selected;
    }

  }
}
