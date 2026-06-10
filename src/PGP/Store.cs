using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PGP.Core {
  public class Store {

    private Dictionary<string, double> d;

    public Store() { 
      d = new Dictionary<string, double>();      
    }

    public void Set(string variable, double value) {      
      d[variable] = value;
    }

    public double Get(string variable) {
      return d[variable];
    }

  }
}
