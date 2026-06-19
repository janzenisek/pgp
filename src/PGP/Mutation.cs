using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core {
  public class Mutation {

    public static RPN<Symbol> MutateReplaceSubtree(PgpAlgorithm pgp, RPN<Symbol> o) {
      var p = o.CloneDeep();
      int idx = pgp.Rng.Next(0, o.Count);

      int cc = 0; // const counter for naming

      // (1) Find the start of the subtree rooted at idx by scanning backwards.
      //     Each operator contributes (arity - 1) pending children; each terminal
      //     consumes one pending slot. We stop when all pending slots are satisfied.
      int start = idx;
      int pending = 1;
      while (pending > 0) {
        pending += p[start].Type == SymbolType.Operator ? p[start].Opr.Arity - 1 : -1;
        start--;
      }
      start++;

      // Remove the subtree [start..idx].
      p.RemoveRange(start, idx - start + 1);

      // (2) Budget: how many symbols can the replacement subtree use?
      //     It must not push the total count above SymbolCount.
      int budget = Math.Max(1, pgp.SymbolCount - p.Count);
      int depthLimit = pgp.NestingDepth > 0 ? pgp.NestingDepth : int.MaxValue;

      // (3) Generate the replacement subtree into a scratch program, then splice
      //     its symbols into p at the removal point.
      var scratch = new RPN<Symbol>(budget, 0);
      Creation.BreedSubtree(pgp, scratch, ref cc, ref budget, depthLimit);
      p.InsertRange(start, scratch);

      return p;
    }

    public static RPN<Symbol> MutateTerminateSubtree(PgpAlgorithm pgp, RPN<Symbol> o) {
      var p = o.CloneDeep();
      int idx = pgp.Rng.Next(0, o.Count);

      int ctr = 0;

      // Scan backwards from idx to find the start of the subtree.
      // ctr tracks how many pending child subtrees are still needed:
      // each node consumes one slot but requires arity(node) more children.
      int start = idx;
      ctr = 1;
      while (ctr > 0) {
        ctr += p[start].Type == SymbolType.Operator ? p[start].Opr.Arity - 1 : -1;
        start--;
      }
      start++;

      // Replace the entire subtree [start..idx] with a single random terminal,
      // keeping the RPN program structurally valid.
      var terminal = Creation.CreateTerminal(pgp, ref ctr);
      p.RemoveRange(start, idx - start + 1);
      p.Insert(start, terminal);

      return p;
    }

    public static RPN<Symbol> MutateMultiCase(PgpAlgorithm pgp, RPN<Symbol> o) {
      var p = o.CloneDeep();
      int idx = pgp.Rng.Next(0, o.Count); // uniformly distributed
      
      if (p[idx].Type == SymbolType.Constant) {
        var val = p[idx].Con.Value;
        var ratio = pgp.Rng.NextDouble() * 0.1;
        if (pgp.Rng.NextDouble() < 0.5) p[idx].Con.Value = val + val * ratio;
        else p[idx].Con.Value = val - val * ratio;
      } else if (p[idx].Type == SymbolType.Variable) {
        // grow: wrap this terminal in a new binary operator + a new random terminal
        // (bounded by SymbolCount so the tree can't exceed the size limit)
        if (p.Count + 2 <= pgp.SymbolCount && pgp.Rng.NextDouble() < 0.25) {
          var binaryOps = Operators.All.Where(op => op.Arity == 2).ToList();
          if (binaryOps.Count > 0) {
            var op = binaryOps[pgp.Rng.Next(binaryOps.Count)];
            int ctr = 0;
            var newTerminal = Creation.CreateTerminal(pgp, ref ctr);
            // insert new terminal before idx, then append operator after idx
            p.Insert(idx, newTerminal);         // new left operand before original terminal
            p.Insert(idx + 2, new Symbol(op));  // operator after the pair
          } else {
            p[idx] = Creation.CreateVariable(pgp);
          }
        } else {
          p[idx] = Creation.CreateVariable(pgp);
        }
      } else // operator
        {
        var ratio = pgp.Rng.NextDouble();
        if (ratio < 0.5) // prune: remove operator and one of its subtrees, keep the other
        {
          if (p[idx].Opr.Arity == 1) {
            p.RemoveAt(idx); // drop unary operator; its single child argument remains
          } else if (p[idx].Opr.Arity == 2) {
            int limit = Utils.FindSubtreeLimit(p, idx); // start of left child
            // find boundary between left and right child subtrees
            int rightStart = FindSubtreeLimit_Right(p, idx); // start of right child
            if (pgp.Rng.NextDouble() < 0.5) {
              // keep right child: remove [limit .. rightStart-1] (left child) and the operator
              int leftLen = rightStart - limit;
              p.RemoveAt(idx);           // remove operator first (end of span)
              p.RemoveRange(limit, leftLen); // remove left subtree
            } else {
              // keep left child: remove [rightStart .. idx-1] (right child) and the operator
              int rightLen = idx - rightStart;
              p.RemoveAt(idx);           // remove operator
              p.RemoveRange(rightStart, rightLen); // remove right subtree
            }
          }
        } else // replace operator with a different one of the same arity
          {
          p[idx].Opr = Operators.SelectRandomDifferent(pgp.Rng, p[idx].Opr);
        }
      }

      return p;
    }

    // Returns the start index of the RIGHT child subtree of the binary operator at idx.
    // The right child is the subtree immediately before the operator; the left child
    // is everything from FindSubtreeLimit(p, idx) up to rightStart-1.
    private static int FindSubtreeLimit_Right(RPN<Symbol> p, int idx) {
      // Walk backward from idx-1 to find the root of the right subtree.
      // The right child's subtree ends at idx-1; its start is FindSubtreeLimit(p, idx-1)
      // if that position is an operator, or idx-1 itself if it is a terminal.
      int rightEnd = idx - 1;
      if (p[rightEnd].Type == SymbolType.Operator) {
        int s = Utils.FindSubtreeLimit(p, rightEnd);
        return s >= 0 ? s : rightEnd;
      }
      return rightEnd; // terminal: single-node subtree
    }
  }
}
