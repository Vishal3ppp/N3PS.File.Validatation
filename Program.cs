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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Collections;

namespace N3PS.File.Validatation
{
    class Program
    {
        static int Main(string[] args)
        {
            //1. Logger initialization
            Logger logger = LogManager.GetCurrentClassLogger();


            //2. XML File Initialization
            string FlatFileXmlName = @".\XMLFiles\FileFormat.xml";

            string SettingsXmlName = @".\XMLFiles\Settings.xml";

            string ValidationRuleXmlName = @".\XMLFiles\ValidationRule.xml";

            //3. Convert FlatFile to C# objects
            FlatFile flatFile = new FlatFile();
            string tableName = "ICADetails";
            string CreateTableSQLQuery = flatFile.CreateFlatFileTableScript(tableName);
            FlatFile fetchedFlatFileObj = flatFile.GetInstance(FlatFileXmlName, logger);

            if (fetchedFlatFileObj == null)
            {
                logger.Error($"Error while loading the Flat File XML : {FlatFileXmlName}");
                return 0;
            }
                //4. Convert Settings File to C# objects
                SettingsFile settingsFile = new SettingsFile();
            SettingsFile fetchedSettingsObj = settingsFile.GetInstance(SettingsXmlName, logger);

            if (fetchedSettingsObj == null)
            {
                logger.Error($"Error while loading the Settings File XML : {SettingsXmlName}");
                return 0;
            }
            //5. Convert ValidationRule to C# objects
            ValidationRuleFile validationRuleFile = new ValidationRuleFile();
            ValidationRuleFile fetchedValidationRuleObj = validationRuleFile.GetInstance(ValidationRuleXmlName, logger);

            if (fetchedValidationRuleObj == null)
            {
                logger.Error($"Error while loading the Validation Rule File XML : {ValidationRuleXmlName}");
                return 0;
            }


            var dllsDetails = fetchedValidationRuleObj.ValidationRules.Where(x => x.DLLInfo != null && !string.IsNullOrEmpty(x.DLLInfo.DLLName) ).ToList();

            Hashtable assemblyDetails = new Hashtable();

            if (dllsDetails.Count() > 0)
            {
                foreach (ValidationsRule rule in dllsDetails)
                {
                    FileInfo f = new FileInfo(@".\ExternalDLLs\" + rule.DLLInfo.DLLName);
                    logger.Info($"Full File Name : {f.FullName}");
                    if (!System.IO.File.Exists(f.FullName))
                    {
                        logger.Error($"External DLL is not exist {rule.DLLInfo.DLLName} in ExternalDLLs folder.");
                        return 0;
                    }
                    else
                    {
                        Assembly assembly = Assembly.LoadFile(f.FullName);
                        assemblyDetails.Add(rule.ColumnNumber, assembly);
                    }
                }
            }
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
            DataSet dsTotalRecords = sqlManipulation.GetTotalRecords(m_dbConnection, tableName, logger);
            FileHelper helper = new FileHelper();
            ProcessedDetails processedDetails = helper.ValidateFile(fetchedFlatFileObj, fetchedSettingsObj, fetchedValidationRuleObj, sqlManipulation, m_dbConnection, DBName, tableName, assemblyDetails, dsTotalRecords, logger);


            logger.Info("------------------------------------------------------------");
            logger.Info($"Total Records: " + processedDetails.TotalRecords);
            logger.Info($"Total Error Records: " + processedDetails.TotalErrorRecords);
            logger.Info($"Total Seccessfully Processed Records: " + processedDetails.TotalSeccessfullyProcessedRecords);
            logger.Info("------------------------------------------------------------");
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
