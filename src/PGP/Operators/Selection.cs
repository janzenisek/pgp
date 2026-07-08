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

    public static int RandomSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {
      return pgp.Rng.Next(population.Length);      
    }

    public static int ProportionalSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {
      double rnd = pgp.Rng.NextDouble() * fitScoreSum;
      double cumulative = 0.0;
      for (int i = 0; i < fitScores.Count; i++) {
        cumulative += fitScores[i];
        if (rnd < cumulative) return i;
      }
      return fitScores.Count - 1; // should not happen, but just in case of rounding errors
    }
    
    public static int TournamentSelection(PgpAlgorithm pgp, RPN<Symbol>[] population, Task task) {     
      
      // setup tournament group
      var tournament = new int[pgp.TournamentSize];
      for (int j = 0; j < pgp.TournamentSize; j++) {
        tournament[j] = pgp.Rng.Next(population.Length);         
      }

      // find tournament winner
      int best = tournament[0];
      for(int i = 1; i < tournament.Length; i++) { 
        // v1
        //if(task.Score.IsBetter(population[tournament[i]].Score, population[best].Score)) {
        //  best = tournament[i];
        //}

        //v2
        if (task.OptimizationDirection == OptimizationDirection.Maximize) {
          if (population[tournament[i]].Score > population[best].Score) {
            best = tournament[i];
          }
        } else {
          if (population[tournament[i]].Score < population[best].Score) {
            best = tournament[i];
          }
        }
      }      
      return best;
    }

  }
}
