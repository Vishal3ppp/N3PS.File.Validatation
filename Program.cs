using N3PS.File.Validatation.FileValidation;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Validatation
{
    class Program
    {


      
        static void Main(string[] args)
        {

            Logger logger = LogManager.GetCurrentClassLogger();
            logger.Error("This is an error message");
            logger.Info("This is an info message");
            logger.Fatal("This is an Fatal message");
            logger.Trace("This is an Trace message");
            logger.Warn("This is an Warn message");
            

            //FileHelper f = new FileHelper();
            //f.ValidateFile(@"C:\Users\vishal.chilka\Desktop\ZSB120OM.OUT");
        }
    }
}
