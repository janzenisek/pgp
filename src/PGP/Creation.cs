using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {
  public class Creation {
    public static Symbol CreateTerminal(PgpAlgorithm pgp, ref int c) {
      double rndD = pgp.Rng.NextDouble();
      int rndI;

      if (rndD < 0.75) {
        rndI = pgp.Rng.Next(0, pgp.Task.InputVariables.Count);
        string varName = pgp.Task.InputVariables[rndI];
        return new Symbol(new Variable(varName, pgp.Task.VariableIndices[varName], 1.0));
      } else {
        rndI = pgp.Rng.Next(0, pgp.Task.VariableLimitsDict.Count);
        c++;
        return new Symbol(new Constant(
              $"c{c}",
              pgp.Rng.NextDouble(pgp.Task.VariableLimitsDict.ElementAt(rndI).Value.Item1, pgp.Task.VariableLimitsDict.ElementAt(rndI).Value.Item2)
            ));
      }
    }

    public static Symbol CreateVariable(PgpAlgorithm pgp) {
      int rndI = pgp.Rng.Next(0, pgp.Task.InputVariables.Count);
      string varName = pgp.Task.InputVariables[rndI];
      return new Symbol(new Variable(pgp.Task.InputVariables[rndI], pgp.Task.VariableIndices[varName], 1.0));
    }

    public static RPN<Symbol> Breed(PgpAlgorithm pgp) {
      var p = new RPN<Symbol>(pgp.SymbolCount, pgp.DataSet.RowCount);
      int aritySum = 0, arityCount = 0, tCount = 0;
      int constantCounter = 0;
      double rndD;

      Symbol curSmybol = CreateTerminal(pgp, ref constantCounter);
      p.Add(curSmybol);
      tCount++;


      while (p.Count < pgp.SymbolCount)
      //while (p.Count < TreeLength || (tCount > 1 || tCount == 1 && curSmybol.Type != SymbolType.Operator))
      {
        rndD = pgp.Rng.NextDouble();
        if (rndD > pgp.OperatorToOperandRatio) {
          // add terminal (variable or constant) to the program leafs
          curSmybol = CreateTerminal(pgp, ref constantCounter);
          p.Add(curSmybol);
          tCount++;
        } else {
          curSmybol = new Symbol(Operators.SelectRandom(pgp.Rng));
          if (tCount >= curSmybol.Opr.Arity) {
            tCount -= curSmybol.Opr.Arity - 1;
            p.Add(curSmybol);
          }
        }
      }

      while (tCount > 1) {
        curSmybol = new Symbol(Operators.SelectRandom(pgp.Rng));
        if (tCount >= curSmybol.Opr.Arity) {
          tCount -= curSmybol.Opr.Arity - 1;
          p.Add(curSmybol);
        }
      }

      return p;
    }

    // Constrained breed: builds a tree top-down using a recursive post-order
    // generator. SymbolCount is a hard budget cap; NestingDepth is a hard depth
    // cap (ignored when NestingDepth == 0). Both constraints are satisfied by
    // construction — no rejection sampling or correction loops needed.
    public static RPN<Symbol> BreedConstrained(PgpAlgorithm pgp) {
      var p = new RPN<Symbol>(pgp.SymbolCount, pgp.DataSet.RowCount);
      int constantCounter = 0;
      int depthLimit = pgp.NestingDepth > 0 ? pgp.NestingDepth : int.MaxValue;
      int budget = pgp.SymbolCount;
      BreedSubtree(pgp, p, ref constantCounter, ref budget, depthLimit);
      return p;
    }

    // Recursively generates one valid subtree in post-order directly into p.
    // budget: symbols remaining across the entire program (shared, passed by ref).
    // depthRemaining: how many more levels this subtree may grow downward.
    private static void BreedSubtree(PgpAlgorithm pgp, RPN<Symbol> p, ref int constantCounter, ref int budget, int depthRemaining) {
      // Force a terminal when the budget is exhausted or the depth cap is reached.
      bool forceTerminal = budget <= 1 || depthRemaining <= 1;

      if (!forceTerminal) {
        // Budget-aware operator probability: large budget → strongly prefer operators
        // so the available symbol slots are actually used.
        //   budget=2  → pOperator = 0.50
        //   budget=5  → pOperator = 0.80
        //   budget=25 → pOperator = 0.96
        double pOperator = 1.0 - 1.0 / budget;

        if (pgp.Rng.NextDouble() < pOperator) {
          int budgetSnapshot = budget;
          var feasible = Operators.All.Where(op => budgetSnapshot >= op.Arity + 1).ToList();
          if (feasible.Count > 0) {
            var op = feasible[pgp.Rng.Next(feasible.Count)];
            budget--; // consume this operator's own symbol slot

            // Distribute the remaining budget among children, each receiving
            // at least 2 symbols (so every child can itself be an operator).
            int[] childBudgets = SplitBudget(pgp.Rng, budget, op.Arity);

            for (int ci = 0; ci < op.Arity; ci++) {
              int childAlloc = childBudgets[ci];
              budget -= childAlloc;
              int localBudget = childAlloc;
              BreedSubtree(pgp, p, ref constantCounter, ref localBudget, depthRemaining - 1);
              budget += localBudget; // return unused symbols to the shared pool
            }

            p.Add(new Symbol(op));
            return;
          }
        }
      }

      // Terminal (variable or constant) — always consumes exactly 1 symbol.
      p.Add(CreateTerminal(pgp, ref constantCounter));
      budget--;
    }

    // Splits 'total' symbols into 'parts' positive integers chosen at random.
    // Each part receives at least min(2, floor(total/parts)) symbols so that
    // every child has a realistic chance of itself being an operator node.
    private static int[] SplitBudget(FastRandom rng, int total, int parts) {
      int minPerPart = total >= parts * 2 ? 2 : 1; // guarantee min 2 when budget allows
      int[] result = new int[parts];
      int remaining = total - minPerPart * parts;   // symbols available above the minimum
      for (int i = 0; i < parts - 1; i++) {
        int maxExtra = remaining - (parts - 1 - i); // keep at least 1 spare per remaining part
        int extra = maxExtra > 0 ? rng.Next(0, maxExtra + 1) : 0;
        result[i] = minPerPart + extra;
        remaining -= extra;
      }
      result[parts - 1] = minPerPart + remaining;
      return result;
    }

  }
}
