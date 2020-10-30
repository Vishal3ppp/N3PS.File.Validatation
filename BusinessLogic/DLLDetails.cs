using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Validatation.BusinessLogic
{
    class DLLDetails
    {
        public string DLLName { get; set;  }

        public string FullyQualififedClassName { get; set; }


        public string RoutineName { get; set; }

        public bool IsStaticMethod { get; set; }


        public string ReturnType { get; set; }


        public string InputType { get; set; }
    }
}
