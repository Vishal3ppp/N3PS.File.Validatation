using N3PS.File.Validatation.BusinessLogic;
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

namespace N3PS.File.Validatation.FileValidation
{
    class FileHelper
    {
        public void ValidateFile(FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj, ValidationRuleFile fetchedValidationRuleObj, SQLiteHelper sqlManipulation, SQLiteConnection connection, string DBName, string tableName, Logger logger)
        {
            string[] allLines = System.IO.File.ReadAllLines(fetchedFlatFileObj.FlatFilePath);
            int totalCount = Convert.ToInt32((fetchedSettingsObj.Percentage / 100) * allLines.Length);

            logger.Info($"Total count = {totalCount}, ({fetchedSettingsObj.Percentage} / 100 )* {allLines.Length}");

            
           // SQLiteConnection connection = sqlManipulation.OpenDBConnection(DBName);

            DateTime startTime = DateTime.Now;

            DateTime endTime = startTime.AddMinutes(fetchedSettingsObj.Time);
            logger.Info($"Start Time : {startTime}");
            logger.Info($"End Time : {endTime}");

            Random rmd = new Random();

            for (int i = 0; i < totalCount; i++)
            {
                
                bool isError = false;


                int randomLineNumber = -1;
                DataSet ds = new DataSet();
                
                do
                {
                    randomLineNumber = rmd.Next(allLines.Length-1);
                    logger.Info($"Rnadom Line Number : {randomLineNumber+1}");
                    ds = sqlManipulation.RetrieveRecord(connection, tableName, randomLineNumber+1, logger);
                    logger.Info($"Total Records with Flat File Line Number : {randomLineNumber+1} and Total Returned Count : {ds.Tables[0].Rows.Count}");

                } while (ds.Tables[0].Rows.Count > 0);


                
                string randomLineContent = allLines[randomLineNumber];

                //Check Validations
                if (randomLineContent.Length > fetchedFlatFileObj.RecordSize)
                {
                    logger.Error($"Flat file line number {randomLineNumber+1} exceeded specified Record Size.");
                }


                if (randomLineContent.Length < fetchedFlatFileObj.RecordSize)
                {
                    logger.Error($"Flat file line number {randomLineNumber+1} less than specified Record Size.");
                }

                foreach (Fields fields in fetchedFlatFileObj.fields)
                {
                    string content = randomLineContent.Substring(fields.StartPosition - 1, fields.Length).Trim();
                    var relatedValidationRules = fetchedValidationRuleObj.ValidationRules.Where(x => x.ColumnNumber == fields.ColumnNumber).ToList();

                    foreach (ValidationsRule validationRule in relatedValidationRules)
                    {
                        switch (validationRule.ValidationType.ToLower())
                        {
                            case "lengthvalidation":
                                {
                                    if (content.Length != validationRule.ValidationSize)
                                    {
                                        logger.Error($"Flat File Line Number : {randomLineNumber+1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                        isError = true;
                                    }
                                }
                                break;

                            case "mandatorycheck":
                                {
                                    if (content == string.Empty)
                                    {
                                        logger.Error($"Flat File Line Number : {randomLineNumber+1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                        isError = true;
                                    }
                                }
                                break;

                            case "valuevalidation":
                                {
                                    if (!validationRule.Values.Contains(content))
                                    {
                                        logger.Error($"Flat File Line Number : {randomLineNumber+1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                        isError = true;
                                    }
                                }
                                break;


                            case "numbervalidation":
                                {
                                    int number = 0;

                                    if (!int.TryParse(content, out number))
                                    {
                                        logger.Error($"Flat File Line Number : {randomLineNumber+1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                        isError = true;
                                    }
                                }
                                break;

                            case "datevalidation":
                                {

                                    try
                                    {
                                        DateTime.ParseExact(content, validationRule.DateFormat, CultureInfo.InvariantCulture);
                                    }
                                    catch (Exception excp)
                                    {

                                        logger.Error($"Flat File Line Number : {randomLineNumber+1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");


                                        logger.Error(excp.ToString() + " --- " + excp.StackTrace);
                                        isError = true;
                                    }


                                }
                                break;
                        }
                    }
                }

                if(!isError)
                {
                    sqlManipulation.InsertRecord(connection, tableName, randomLineNumber+1, logger);
                }

                if (startTime > endTime)
                {
                    break;
                }
            }
        }
    }
}
