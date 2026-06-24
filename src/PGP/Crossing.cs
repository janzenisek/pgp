using PGP.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core {
  public class Crossing {

    // Simple crossover: picks a random operator crosspoint in each parent and swaps
    // the subtrees. No size or depth checks — fast but can produce bloated or
    // degenerate offspring.
    public static Tuple<RPN<Symbol>, RPN<Symbol>> Cross_Simple(PgpAlgorithm pgp, RPN<Symbol> a, RPN<Symbol> b) {
      int aIdx = pgp.Rng.Next(0, a.Count);
      int aUpperBound = a.FindIndex(aIdx, x => x.Type == SymbolType.Operator);
      if (aUpperBound == -1) return null;
      int aLowerBound = Utils.FindSubtreeLimit(a, aUpperBound);
      if (aLowerBound == -1) return null;

      int bIdx = pgp.Rng.Next(0, b.Count);
      int bUpperBound = b.FindIndex(bIdx, x => x.Type == SymbolType.Operator);
      if (bUpperBound == -1) return null;
      int bLowerBound = Utils.FindSubtreeLimit(b, bUpperBound);
      if (bLowerBound == -1) return null;

      var aSubtree = a.GetRange(aLowerBound, aUpperBound - aLowerBound + 1)
                       .Select(s => s.Clone()).ToList();
      var bSubtree = b.GetRange(bLowerBound, bUpperBound - bLowerBound + 1)
                       .Select(s => s.Clone()).ToList();

      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();
      aOffspring.RemoveRange(aLowerBound, aUpperBound - aLowerBound + 1);
      aOffspring.InsertRange(aLowerBound, bSubtree);
      bOffspring.RemoveRange(bLowerBound, bUpperBound - bLowerBound + 1);
      bOffspring.InsertRange(bLowerBound, aSubtree);

      return Tuple.Create(aOffspring, bOffspring);
    }

    // Attempt-based crossover: tries up to maxAttempts times to find a random
    // crosspoint pair whose offspring both satisfy the SymbolCount size bounds.
    // Simpler than the exact enumeration but may fail if compatible pairs are rare.
    public static Tuple<RPN<Symbol>, RPN<Symbol>> Cross_AttemptBased(PgpAlgorithm pgp, RPN<Symbol> a, RPN<Symbol> b, int maxAttempts = 10) {
      double sizeTolerance = 0.5; // allow offspring up to SymbolCount * (1 ± 0.5)
      int minSize = (int)(pgp.SymbolCount * (1.0 - sizeTolerance));
      int maxSize = (int)(pgp.SymbolCount * (1.0 + sizeTolerance));

      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        int aIdx = pgp.Rng.Next(0, a.Count);
        int aUpperBound = aIdx;
        int aLowerBound = (a[aIdx].Type == SymbolType.Operator)
            ? Utils.FindSubtreeLimit(a, aUpperBound)
            : aUpperBound;
        if (aLowerBound == -1) continue;

        int bIdx = pgp.Rng.Next(0, b.Count);
        int bUpperBound = bIdx;
        int bLowerBound = (b[bIdx].Type == SymbolType.Operator)
            ? Utils.FindSubtreeLimit(b, bUpperBound)
            : bUpperBound;
        if (bLowerBound == -1) continue;

        int aSubtreeSize = aUpperBound - aLowerBound + 1;
        int bSubtreeSize = bUpperBound - bLowerBound + 1;
        int aOffspringSize = a.Count - aSubtreeSize + bSubtreeSize;
        int bOffspringSize = b.Count - bSubtreeSize + aSubtreeSize;

        if (aOffspringSize < minSize || aOffspringSize > maxSize ||
            bOffspringSize < minSize || bOffspringSize > maxSize) continue;

        var aSubtree = a.GetRange(aLowerBound, aSubtreeSize).Select(s => s.Clone()).ToList();
        var bSubtree = b.GetRange(bLowerBound, bSubtreeSize).Select(s => s.Clone()).ToList();

        var aOffspring = a.CloneDeep();
        var bOffspring = b.CloneDeep();
        aOffspring.RemoveRange(aLowerBound, aSubtreeSize);
        aOffspring.InsertRange(aLowerBound, bSubtree);
        bOffspring.RemoveRange(bLowerBound, bSubtreeSize);
        bOffspring.InsertRange(bLowerBound, aSubtree);

        return Tuple.Create(aOffspring, bOffspring);
      }

      return null;
    }

    // Constrained crossover: enumerates all valid subtree pairs across both parents
    // and selects uniformly from those satisfying the SymbolCount (size) and
    // NestingDepth constraints. Returns null only when no valid pair exists.
    public static Tuple<RPN<Symbol>, RPN<Symbol>> Cross_Constrained(PgpAlgorithm pgp, RPN<Symbol> a, RPN<Symbol> b) {
      // Enumerate every valid subtree in both parents.
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      // Precompute per-span metrics so the inner loop is O(1).
      int[] aSize = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] aDepth = aSpans.Select(s => SubtreeDepth(a, s.lower, s.upper)).ToArray();
      int[] bDepth = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();
      int[] bAncestors = bSpans.Select(s => AncestorCount(b, s.upper)).ToArray();

      bool checkDepth = pgp.NestingDepth > 0;

      // Collect all (ai, bi) pairs that satisfy BOTH constraints for BOTH offspring.
      var compatible = new List<(int ai, int bi)>();
      for (int ai = 0; ai < aSpans.Count; ai++) {
        int aOff_base = a.Count - aSize[ai]; // aOffspringCount = aOff_base + bSize[bi]
        int bOff_base = b.Count + aSize[ai]; // bOffspringCount = bOff_base - bSize[bi]

        for (int bi = 0; bi < bSpans.Count; bi++) {
          // ── size constraint ─────────────────────────────────────────────────
          int aOffCount = aOff_base + bSize[bi];
          int bOffCount = bOff_base - bSize[bi];
          if (aOffCount < 1 || aOffCount > pgp.SymbolCount) continue;
          if (bOffCount < 1 || bOffCount > pgp.SymbolCount) continue;

          // ── depth constraint ─────────────────────────────────────────────────
          // Inserting b's subtree (depth bDepth[bi]) at a's crosspoint (aAncestors[ai]
          // operators above it) yields a path of length aAncestors[ai] + bDepth[bi].
          if (checkDepth) {
            if (aAncestors[ai] + bDepth[bi] > pgp.NestingDepth) continue;
            if (bAncestors[bi] + aDepth[ai] > pgp.NestingDepth) continue;
          }

          compatible.Add((ai, bi));
        }
      }

      if (compatible.Count == 0) return null;

      // Pick one compatible pair uniformly at random — no retries needed.
      var (selAi, selBi) = compatible[pgp.Rng.Next(compatible.Count)];
      var (aL, aU) = aSpans[selAi];
      var (bL, bU) = bSpans[selBi];

      var aSubtree = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
      var bSubtree = a.GetRange(aL, aSize[selAi]).Select(s => s.Clone()).ToList();

      var aOffspring = a.CloneDeep();
      var bOffspring = b.CloneDeep();

      aOffspring.RemoveRange(aL, aSize[selAi]);
      aOffspring.InsertRange(aL, aSubtree);
      bOffspring.RemoveRange(bL, bSize[selBi]);
      bOffspring.InsertRange(bL, bSubtree);

      return Tuple.Create(aOffspring, bOffspring);
    }

    // Bézier crossover: interpolates between two parents in program space using a
    // Bézier parameter t ∈ (0,1) that continuously slides the offspring from A (t→0)
    // toward B (t→1). The crossover operates in two coupled phases:
    //
    // Phase 1 — Structural selection
    //   A subtree in A is replaced by a subtree from B (single-offspring, like Cross()).
    //   The selection is NOT uniform: candidate subtree pairs are weighted by how well
    //   their fractional size contribution matches t.  Specifically, a pair (aSubtree,
    //   bSubtree) gets weight  exp(-(ratio - t)² / σ²)  where
    //   ratio = bSize / (aSize + bSize).  At t=0.5 the selection is nearly uniform;
    //   at t→0 it prefers small B-insertions (preserving A's structure); at t→1 it
    //   prefers large B-insertions (replacing most of A with B material).
    //
    // Phase 2 — Numeric Bézier interpolation
    //   After the structural swap every numeric parameter in the offspring (constants
    //   and variable coefficients) is blended with the order-corresponding parameter
    //   from each parent using a quadratic Bézier formula:
    //     v(t) = (1-t)² · vA  +  2t(1-t) · ½(vA+vB)  +  t² · vB
    //   Parameters are matched by occurrence order (i-th numeric in offspring ↔
    //   i-th numeric in A and B), clamped so neither parent runs out of entries.
    //
    // Returns null only when no structurally compatible subtree pair exists.
    public static RPN<Symbol> Cross_Bezier(PgpAlgorithm pgp, RPN<Symbol> a, RPN<Symbol> b) {
      double t = pgp.Rng.NextDouble(); // Bézier parameter ∈ (0,1)

      // ── Phase 1: Bézier-weighted structural selection ───────────────────────
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      int[] aSize = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bDepth = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();

      bool checkDepth = pgp.NestingDepth > 0;

      // σ controls how sharply the weight peaks around the target ratio.
      // σ = 0.20 gives a broad window so even at extreme t values there are
      // usually some candidates with non-negligible weight.
      const double sigma2 = 0.20 * 0.20;

      // Shuffle a's crosspoints so the final pick is unbiased across positions.
      int[] aOrder = Enumerable.Range(0, aSpans.Count).ToArray().ShuffleFisherYates(pgp.Rng).ToArray();

      foreach (int ai in aOrder) {
        int aOffBase = a.Count - aSize[ai];

        // Collect every valid b-subtree with its Bézier weight.
        var compatibleB = new List<(int bi, double weight)>();
        for (int bi = 0; bi < bSpans.Count; bi++) {
          int aOffCount = aOffBase + bSize[bi];
          if (aOffCount < 1 || aOffCount > pgp.SymbolCount) continue;
          if (checkDepth && aAncestors[ai] + bDepth[bi] > pgp.NestingDepth) continue;

          // ratio: what fraction of the offspring's total symbol budget B contributes
          double ratio = (double)bSize[bi] / (aSize[ai] + bSize[bi]);
          double diff = ratio - t;
          double weight = Math.Exp(-diff * diff / sigma2);
          compatibleB.Add((bi, weight));
        }

        if (compatibleB.Count == 0) continue;

        // Weighted roulette selection among compatible b-subtrees.
        double totalWeight = compatibleB.Sum(x => x.weight);
        double pick = pgp.Rng.NextDouble() * totalWeight;
        double cumulative = 0.0;
        int selBi = compatibleB[^1].bi; // default to last as fallback
        foreach (var (bi, w) in compatibleB) {
          cumulative += w;
          if (pick <= cumulative) { selBi = bi; break; }
        }

        var (aL, aU) = aSpans[ai];
        var (bL, bU) = bSpans[selBi];

        var bSubtree = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
        var offspring = a.CloneDeep();
        offspring.RemoveRange(aL, aSize[ai]);
        offspring.InsertRange(aL, bSubtree);

        // ── Phase 2: quadratic Bézier numeric interpolation ─────────────────
        // Collect all numeric parameters from the offspring, A, and B in
        // program order. Pair by occurrence index; apply the Bézier formula.
        var offNumerics = CollectNumerics(offspring);
        var aNumerics = CollectNumerics(a);
        var bNumerics = CollectNumerics(b);

        if (aNumerics.Count > 0 && bNumerics.Count > 0) {
          for (int ni = 0; ni < offNumerics.Count; ni++) {
            // Clamp so we never go out-of-range when parent lengths differ.
            double vA = aNumerics[Math.Min(ni, aNumerics.Count - 1)].value;
            double vB = bNumerics[Math.Min(ni, bNumerics.Count - 1)].value;
            double vMid = (vA + vB) * 0.5;

            // Quadratic Bézier:  B(t) = (1-t)²·P0 + 2t(1-t)·P1 + t²·P2
            // P0=vA, P1=vMid (control point at the arithmetic mean), P2=vB
            double blended = (1 - t) * (1 - t) * vA
                           + 2.0 * t * (1 - t) * vMid
                           + t * t * vB;

            var (offIdx, _, isConst) = offNumerics[ni];
            if (isConst)
              offspring[offIdx].Con.Value = blended;
            else
              offspring[offIdx].Var.Coefficient = blended;
          }
        }

        return offspring;
      }

      return null;
    }


    // Single-offspring constrained crossover: picks a random valid subtree in `a`
    // (the recipient) and finds all subtrees in `b` whose insertion into `a` at that
    // point satisfies the SymbolCount and NestingDepth constraints for the ONE
    // produced offspring. Because the reciprocal offspring is never required to be
    // valid, the compatible set is always at least as large as Cross_Constrained's,
    // so this version fails far less often.
    public static RPN<Symbol> Cross(PgpAlgorithm pgp, RPN<Symbol> a, RPN<Symbol> b) {
      var aSpans = GetSubtreeSpans(a);
      var bSpans = GetSubtreeSpans(b);
      if (aSpans.Count == 0 || bSpans.Count == 0) return null;

      int[] aSize = aSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bSize = bSpans.Select(s => s.upper - s.lower + 1).ToArray();
      int[] bDepth = bSpans.Select(s => SubtreeDepth(b, s.lower, s.upper)).ToArray();
      int[] aAncestors = aSpans.Select(s => AncestorCount(a, s.upper)).ToArray();

      bool checkDepth = pgp.NestingDepth > 0;

      // Shuffle a's crosspoints so the final pick is uniformly distributed.
      int[] aOrder = Enumerable.Range(0, aSpans.Count).ToArray().ShuffleFisherYates(pgp.Rng).ToArray();

      foreach (int ai in aOrder) {
        int aOffBase = a.Count - aSize[ai]; // aOffspring.Count = aOffBase + bSize[bi]

        // Collect all b-subtrees compatible with this a-crosspoint.
        var compatibleB = new List<int>();
        for (int bi = 0; bi < bSpans.Count; bi++) {
          int aOffCount = aOffBase + bSize[bi];
          if (aOffCount < 1 || aOffCount > pgp.SymbolCount) continue;
          if (checkDepth && aAncestors[ai] + bDepth[bi] > pgp.NestingDepth) continue;
          compatibleB.Add(bi);
        }

        if (compatibleB.Count == 0) continue;

        int selBi = compatibleB[pgp.Rng.Next(compatibleB.Count)];
        var (aL, aU) = aSpans[ai];
        var (bL, bU) = bSpans[selBi];

        var bSubtree = b.GetRange(bL, bSize[selBi]).Select(s => s.Clone()).ToList();
        var offspring = a.CloneDeep();
        offspring.RemoveRange(aL, aSize[ai]);
        offspring.InsertRange(aL, bSubtree);

        return offspring;
      }

      return null;
    }





    // Returns (lowerBound, upperBound) for every valid subtree in p.
    // Operators yield a multi-node subtree; terminals yield a single-node subtree.
    private static List<(int lower, int upper)> GetSubtreeSpans(RPN<Symbol> p) {
      var spans = new List<(int, int)>(p.Count);
      for (int i = 0; i < p.Count; i++) {
        if (p[i].Type == SymbolType.Operator) {
          int lower = Utils.FindSubtreeLimit(p, i);
          if (lower != -1) spans.Add((lower, i));
        } else {
          spans.Add((i, i));
        }
      }
      return spans;
    }

    // Depth of the expression tree for the standalone subtree p[start..end].
    // Terminals contribute depth 1; an operator contributes 1 + max(child depths).
    private static int SubtreeDepth(RPN<Symbol> p, int start, int end) {
      var stack = new Stack<int>(end - start + 1);
      for (int i = start; i <= end; i++) {
        if (p[i].Type != SymbolType.Operator) {
          stack.Push(1);
        } else {
          int arity = p[i].Opr.Arity;
          int maxChild = 0;
          for (int j = 0; j < arity; j++) maxChild = Math.Max(maxChild, stack.Pop());
          stack.Push(1 + maxChild);
        }
      }
      return stack.Count == 1 ? stack.Peek() : 0;
    }

    // Number of operators in p that are strict ancestors of the node at upperBound.
    // An operator at position j > upperBound is an ancestor iff its subtree
    // start (FindSubtreeLimit) is ≤ upperBound, meaning upperBound lies inside it.
    private static int AncestorCount(RPN<Symbol> p, int upperBound) {
      int count = 0;
      for (int j = upperBound + 1; j < p.Count; j++) {
        if (p[j].Type == SymbolType.Operator) {
          int lower = Utils.FindSubtreeLimit(p, j);
          if (lower != -1 && lower <= upperBound) count++;
        }
      }
      return count;
    }

    // Returns the ordered list of numeric parameters (constants and variable
    // coefficients) in p together with their RPN index and type.
    private static List<(int index, double value, bool isConst)> CollectNumerics(RPN<Symbol> p) {
      var result = new List<(int, double, bool)>(p.Count);
      for (int i = 0; i < p.Count; i++) {
        if (p[i].Type == SymbolType.Constant)
          result.Add((i, p[i].Con.Value, true));
        else if (p[i].Type == SymbolType.Variable)
          result.Add((i, p[i].Var.Coefficient, false));
      }
      return result;
    }

  }
}
