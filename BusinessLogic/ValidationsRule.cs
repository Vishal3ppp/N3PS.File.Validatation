using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Compare.BusinessLogic
{
    class ValidationsRule
    {
        public string ValidationType { get; set; }

        public int ValidationId { get; set; }

        public string ValidationName { get; set; }

        public int ColumnNumber { get; set; }

        public int ValidationSize { get; set; }

        public string ErrorMessage { get; set; }

        public List<string> Values = new List<string>();

        public string DateFormat { get; set; }

        public DLLDetails DLLInfo { get; set; }
    }
}
