using N3PS.File.Validatation.DiskManipulation;
using N3PS.File.Validatation.FileValidation;
using N3PS.File.Validatation.SQLLiteManiputation;
using N3PS.File.Validatation.XMLConfigClasses;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Validatation
{
    class Program
    {
        static int Main(string[] args)
        {

            Logger logger = LogManager.GetCurrentClassLogger();

            string FlatFileXmlName = @"..\..\XMLFiles\FileFormat.xml";

            string SettingsXmlName = @"..\..\XMLFiles\Settings.xml";

            string ValidationRuleXmlName = @"..\..\XMLFiles\ValidationRule.xml";

            
            FlatFile flatFile = new FlatFile();
            string tableName = "ICA_" + DateTime.Now.ToString("yyyyMMdd");
            string CreateTableSQLQuery = flatFile.CreateFlatFileTableScript(tableName);
            FlatFile fetchedFlatFileObj = flatFile.GetInstance(FlatFileXmlName, logger);


            SettingsFile settingsFile = new SettingsFile();
            SettingsFile fetchedSettingsObj = settingsFile.GetInstance(SettingsXmlName, logger);



            ValidationRuleFile validationRuleFile = new ValidationRuleFile();
            ValidationRuleFile fetchedValidationRuleObj = validationRuleFile.GetInstance(ValidationRuleXmlName, logger);


            HDDCheck check = new HDDCheck();
            bool isFreeSpaceAvailable = check.IsEnoughSpaceAvailable(fetchedFlatFileObj.FlatFilePath, logger);

            //Check for free space
            if(!isFreeSpaceAvailable)
            {
                return 0;
            }

            SQLiteHelper sqlLite = new SQLiteHelper();
            string DBName = "ICA";
            bool isDBExist = sqlLite.IsDBExist(DBName);
            if(!isDBExist)
            {
                sqlLite.CreateDB(DBName, logger);
            }
            
            sqlLite.CreateTable(DBName, CreateTableSQLQuery, logger);
            sqlLite.InsertRecord(DBName, tableName, 1, "Nothing", logger);
            DataSet dt = sqlLite.RetrieveRecord(DBName, tableName, 1,  logger);
            //FileHelper f = new FileHelper();
            //f.ValidateFile(@"C:\Users\vishal.chilka\Desktop\ZSB120OM.OUT");

            return 0;
        }
    }
}
