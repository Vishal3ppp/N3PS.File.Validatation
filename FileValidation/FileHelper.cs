using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Validatation.FileValidation
{
    class FileHelper
    {
        public void ValidateFile(string flatFilePath)
        {
            string[] lines = System.IO.File.ReadAllLines(flatFilePath);

            foreach (string line in lines)
            {


                Console.WriteLine(line);
            }
        }
    }
}
