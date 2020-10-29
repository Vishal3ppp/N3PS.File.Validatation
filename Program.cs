using N3PS.File.Validatation.BusinessLogic;
using N3PS.File.Validatation.DiskManipulation;
using N3PS.File.Validatation.FileValidation;
using N3PS.File.Validatation.SQLLiteManiputation;
using N3PS.File.Validatation.XMLConfigClasses;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Validatation
{
    class Program
    {
        static int Main(string[] args)
        {
            //1. Logger initialization
            Logger logger = LogManager.GetCurrentClassLogger();


            //2. XML File Initialization
            string FlatFileXmlName = @"..\..\XMLFiles\FileFormat.xml";

            string SettingsXmlName = @"..\..\XMLFiles\Settings.xml";

            string ValidationRuleXmlName = @"..\..\XMLFiles\ValidationRule.xml";

            //3. Convert FlatFile to C# objects
            FlatFile flatFile = new FlatFile();
            string tableName = "ICA_" + DateTime.Now.ToString("yyyyMMdd");
            string CreateTableSQLQuery = flatFile.CreateFlatFileTableScript(tableName);
            FlatFile fetchedFlatFileObj = flatFile.GetInstance(FlatFileXmlName, logger);

            //4. Convert Settings File to C# objects
            SettingsFile settingsFile = new SettingsFile();
            SettingsFile fetchedSettingsObj = settingsFile.GetInstance(SettingsXmlName, logger);


            //5. Convert ValidationRule to C# objects
            ValidationRuleFile validationRuleFile = new ValidationRuleFile();
            ValidationRuleFile fetchedValidationRuleObj = validationRuleFile.GetInstance(ValidationRuleXmlName, logger);

            //6. HDD Size Check
            HDDCheck check = new HDDCheck();
            bool isFreeSpaceAvailable = check.IsEnoughSpaceAvailable(fetchedFlatFileObj.FlatFilePath, logger);



            
            
            //Check for free space
            if (!isFreeSpaceAvailable)
            {
                return 0;
            }
            string DBName = "ICA";
            SQLiteHelper sqlManipulation = new SQLiteHelper();
            bool isDBExist = sqlManipulation.IsDBExist(DBName);
            if (!isDBExist)
            {
                sqlManipulation.CreateDB(DBName, logger);
            }

            //SQLiteHelper sqlLite = new SQLiteHelper();
            SQLiteConnection m_dbConnection = sqlManipulation.OpenDBConnection(DBName);
            if (fetchedSettingsObj.NewRun)
            {
                 
                sqlManipulation.DeleteTable(DBName, tableName, logger);
                sqlManipulation.CreateTable(DBName, CreateTableSQLQuery, logger);
                //sqlManipulation.CloseDBConnection(m_dbConnection);

            }

            FileHelper helper = new FileHelper();
            helper.ValidateFile(fetchedFlatFileObj, fetchedSettingsObj, fetchedValidationRuleObj, sqlManipulation, m_dbConnection, DBName, tableName, logger);

            sqlManipulation.CloseDBConnection(m_dbConnection);

            //sqlLite.CreateTable(DBName, CreateTableSQLQuery, logger);
            //sqlLite.InsertRecord(DBName, tableName, 1, "Nothing", logger);
            //DataSet dt = sqlLite.RetrieveRecord(DBName, tableName, 1,  logger);
            //FileHelper f = new FileHelper();
            //f.ValidateFile(@"C:\Users\vishal.chilka\Desktop\ZSB120OM.OUT");

            return 0;
        }
    }
}
