using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core {
  public class Utils {

    public static int FindSubtreeLimit(RPN<Symbol> p, int idx) {
      int limit = -1;
      var op = p[idx].Opr;

      var arityTarget = op.Arity;
      var arityCount = 0;

      for (int i = idx - 1; i >= 0 && limit == -1; i--) {
        if (p[i].Type == SymbolType.Operator) {
          arityTarget += p[i].Opr.Arity - 1;
        } else // any terminal
          {
          arityCount++;
        }
        if (arityTarget == arityCount) limit = i;
      }

      return limit;
    }

  }
}
