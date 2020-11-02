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
            int TimePart = 0;
            decimal PercentagePart = 0M;
            bool? RunSettingsPart = null;
            bool? HDDCheckSettingsPart = null;
            if (args.Length > 0)
            {
                if(args.Where(x => x.ToLower() == "-help").ToList().Count > 0)
                {
                    logger.Info("-----------------------------------------------------------------------------");
                    logger.Info("-t for Time setting in mins");
                    logger.Info("-p for Percentage setting");
                    logger.Info("-r for Run setting (True/False)");
                    logger.Info("-h for HDD Check (True/False)");
                    logger.Info("Example : >N3PS.File.Validatation.exe -t30 -p100 -rTrue -hFalse");
                    logger.Info("Meaning program will be running 30 mins, 100% random record picked up and New table to be created for processing or not.");
                    logger.Info("-----------------------------------------------------------------------------");
                    return 0;
                }
                try
                {
                    var timePart = args.Where(x => x.ToLower().Contains("-t")).ToList();
                    if (timePart.Count > 0)
                    {
                        TimePart = Convert.ToInt32(timePart[0].ToLower().Replace("-t", ""));
                        logger.Info($"TimePart Argument Value : {TimePart}");
                    }


                    var percentagePart = args.Where(x => x.ToLower().Contains("-p")).ToList();
                    if (percentagePart.Count > 0)
                    {
                        PercentagePart = Convert.ToDecimal(percentagePart[0].ToLower().Replace("-p", ""));
                        logger.Info($"PercentagePart Argument Value : {PercentagePart}");
                    }

                    var runSettingsPart = args.Where(x => x.ToLower().Contains("-r")).ToList();
                    if (runSettingsPart.Count > 0)
                    {
                        RunSettingsPart = Convert.ToBoolean(runSettingsPart[0].ToLower().Replace("-r", ""));
                        logger.Info($"RunSettingsPart Argument Value : {RunSettingsPart}");
                    }


                    var runHddCheckPart = args.Where(x => x.ToLower().Contains("-h")).ToList();
                    if (runHddCheckPart.Count > 0)
                    {
                        HDDCheckSettingsPart = Convert.ToBoolean(runHddCheckPart[0].ToLower().Replace("-h", ""));
                        logger.Info($"HDDCheckSettingsPart Argument Value : {HDDCheckSettingsPart}");
                    }

                }
                catch(Exception excp)
                {
                    logger.Error("Error in processing the command level arguments. " + excp.ToString() + " --- " + excp.StackTrace);
                }
            }
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

            if (TimePart != 0)
            {
                logger.Info($"Overidden Time Part from Settings.xml: {TimePart}");
                fetchedSettingsObj.Time = TimePart;
            }

            if (PercentagePart != 0M)
            {
                logger.Info($"Overidden Percentage from Settings.xml: {PercentagePart}");
                fetchedSettingsObj.Percentage = PercentagePart;
            }


            if (RunSettingsPart != null)
            {
                logger.Info($"Overidden Run Settings from Settings.xml: {RunSettingsPart}");
                fetchedSettingsObj.NewRun = RunSettingsPart.Value;
            }


            logger.Info("Settings : Start ----------------------");
            logger.Info($"Time : {fetchedSettingsObj.Time}");
            logger.Info($"Percentage : {fetchedSettingsObj.Percentage}");
            logger.Info($"NewRun : {fetchedSettingsObj.NewRun}");

            logger.Info("Settings : END ----------------------");

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
           




            if (HDDCheckSettingsPart == null)
                HDDCheckSettingsPart = true;
            //Check for free space
            if (HDDCheckSettingsPart.Value)
            {
                HDDCheck check = new HDDCheck();
                bool isFreeSpaceAvailable = check.IsEnoughSpaceAvailable(fetchedFlatFileObj.FlatFilePath, logger);

                if (!isFreeSpaceAvailable)
                {
                    return 0;
                }
            }
            else
            {
                logger.Info("HDD Check is Skipped.");
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

            DataSet dsTotalRecords1 = sqlManipulation.GetTotalRecords(m_dbConnection, tableName, logger);
            int totalRecords = 0;
            int totalError = 0;
            int totalSuccessProcessed = 0;
            if (dsTotalRecords1 != null)
            {
                DataTable dt = dsTotalRecords1.Tables[0];
                foreach(DataRow dr in dt.Rows)
                {
                    int tr = 0;
                    if(dr["TotalRecords"] != DBNull.Value)
                    {
                        tr = Convert.ToInt32(dr["TotalRecords"].ToString());
                        if (dr["IsError"] != DBNull.Value && Convert.ToBoolean(dr["IsError"].ToString()))
                        {
                            totalError = totalError + tr;
                        }
                        else
                        {

                            totalSuccessProcessed = totalSuccessProcessed + tr;

                        }
                        
                        totalRecords = totalRecords + tr;
                    }
                }
            }
            logger.Info("------------------------------------------------------------");
            logger.Info($"Total Records: " + processedDetails.TotalRecords);
            logger.Info($"Total Records Processed: " + totalRecords);//(processedDetails.TotalErrorRecords + processedDetails.TotalSeccessfullyProcessedRecords));
            logger.Info($"Total Error Records: " + totalError);// processedDetails.TotalErrorRecords);
            logger.Info($"Total Seccessfully Processed Records: " + totalSuccessProcessed);// processedDetails.TotalSeccessfullyProcessedRecords);
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
