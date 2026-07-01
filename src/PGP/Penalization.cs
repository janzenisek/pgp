using System;
using System.Collections.Generic;
using System.Text;

namespace PGP.Core {
  public class Penalization {

    public double PenalizeOversize(PgpAlgorithm pgp, RPN<Symbol> o) {
      o.PearsonR = o.Count > pgp.SymbolCount ? o.PearsonR * (pgp.SymbolCount / (double)o.Count) : o.PearsonR;
      return o.PearsonR;

      o.NMSE = o.Count > pgp.SymbolCount ? o.NMSE * ((double)o.Count / pgp.SymbolCount) : o.NMSE;
      return o.NMSE;
    }
  }
}
