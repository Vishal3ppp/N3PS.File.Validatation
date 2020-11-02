using N3PS.File.Validatation.BusinessLogic;
using N3PS.File.Validatation.SQLLiteManiputation;
using N3PS.File.Validatation.XMLConfigClasses;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace N3PS.File.Validatation.FileValidation
{
    class FileHelper
    {
        public ProcessedDetails ValidateFile(FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj, ValidationRuleFile fetchedValidationRuleObj, SQLiteHelper sqlManipulation, SQLiteConnection connection, string DBName, string tableName, Hashtable assemblyDetails, DataSet dsTotalRecords, Logger logger)
        {
            string[] allLines = System.IO.File.ReadAllLines(fetchedFlatFileObj.FlatFilePath);

            if (fetchedSettingsObj.Percentage == 0)
            {
                fetchedSettingsObj.Percentage = 100;
            }
            decimal totalCountDec = Convert.ToDecimal((fetchedSettingsObj.Percentage / 100) * allLines.Length);
            logger.Info($"Total Count as per Percentage : {totalCountDec.ToString("#.00")}");
            int totalCount = Convert.ToInt32(totalCountDec);
            if((totalCount * 1.0M) < totalCountDec)
            {
                
                totalCount++;
                logger.Info($"Total Count as per Percentage after converting to Integer : {totalCount}");
            }



            ProcessedDetails processedDetails = new ProcessedDetails();
            int TotalRecords = 0;
            int TotalErrorRecords = 0;
            int TotalSeccessfullyProcessedRecords = 0;
            if (dsTotalRecords.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in dsTotalRecords.Tables[0].Rows)
                {
                    int records = Convert.ToInt32(dr[1].ToString());

                    if(Convert.ToBoolean(dr[0].ToString()))
                    {
                        TotalErrorRecords = TotalErrorRecords + records;
                    }
                    else
                    {
                        TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords + records;
                    }
                    TotalRecords = records + TotalRecords;
                    
                }

                if (TotalRecords >= allLines.Length)
                {
                    logger.Info("All records are processed, in case of any error please check.");
                    
                    processedDetails.TotalRecords = TotalRecords;
                    processedDetails.TotalErrorRecords = TotalErrorRecords;
                    processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
                    return processedDetails;
                }
            }

            TotalRecords = allLines.Length;
            logger.Info($"Total count = {totalCount}, ({fetchedSettingsObj.Percentage} / 100 )* {allLines.Length}");
            

            // SQLiteConnection connection = sqlManipulation.OpenDBConnection(DBName);

            DateTime startTime = DateTime.Now;
            if(fetchedSettingsObj.Time == 0)
            {
                fetchedSettingsObj.Time = 24 * 60 * 365;
            }

            
            DateTime endTime = startTime.AddMinutes(fetchedSettingsObj.Time);
            logger.Info($"Start Time : {startTime}");
            logger.Info($"End Time : {endTime}");

            Random rmd = new Random();
            ParallelOptions ops = new ParallelOptions();
            ops.MaxDegreeOfParallelism = 200;
            //for (int i = 0; i < totalCount; i++)
            Parallel.For(0, totalCount, ops, (i, loopState) =>

               {
                   try
                   {
                       bool isError = false;


                       int randomLineNumber = -1;
                       DataSet ds = new DataSet();

                       randomLineNumber = rmd.Next(allLines.Length - 1);
                        //logger.Info($"Generated Line Number : {randomLineNumber + 1}");
                        ds = sqlManipulation.RetrieveRecord(connection, tableName, randomLineNumber + 1, logger);
                       int loopIteration = 1;
                       bool allCompleted = false;
                       while (ds.Tables[0].Rows.Count > 0)
                       {
                           randomLineNumber = randomLineNumber + 1;
                           if (randomLineNumber > allLines.Length - 1)
                           {
                               randomLineNumber = 0;
                           }
                           ds = sqlManipulation.RetrieveRecord(connection, tableName, randomLineNumber + 1, logger);
                            //logger.Info($"Total Records with Flat File Line Number : {randomLineNumber + 1} and Total Returned Count : {ds.Tables[0].Rows.Count}");
                            if (loopIteration > TotalRecords)
                           {
                               allCompleted = true;
                               break;
                           }
                           loopIteration++;
                       }

                       if (allCompleted)
                           loopState.Stop();
                        //logger.Info($"Random Line Number : {randomLineNumber + 1}");
                        string randomLineContent = allLines[randomLineNumber];

                        //Check Validations
                        if (randomLineContent.Length > fetchedFlatFileObj.RecordSize)
                       {
                           logger.Error($"Flat file line number {randomLineNumber + 1} exceeded specified Record Size.");
                       }


                       if (randomLineContent.Length < fetchedFlatFileObj.RecordSize)
                       {
                           logger.Error($"Flat file line number {randomLineNumber + 1} less than specified Record Size.");
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
                                               
                                               logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                               isError = true;
                                           }
                                       }
                                       break;

                                   case "mandatorycheck":
                                       {
                                           if (content == string.Empty)
                                           {
                                               logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                               isError = true;
                                           }
                                       }
                                       break;

                                   case "valuevalidation":
                                       {
                                           if (!validationRule.Values.Contains(content))
                                           {
                                               logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                               isError = true;
                                           }
                                       }
                                       break;


                                   case "numbervalidation":
                                       {
                                           int number = 0;

                                           if (!int.TryParse(content, out number))
                                           {
                                               logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
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

                                               logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");


                                                //logger.Error(excp.ToString() + " --- " + excp.StackTrace);
                                                isError = true;
                                           }


                                       }
                                       break;


                                   case "externalroutinecall":
                                       {
                                           Assembly assembly = assemblyDetails[validationRule.ColumnNumber] as Assembly;
                                           Type type = assembly.GetType(validationRule.DLLInfo.FullyQualififedClassName);
                                           if (type != null)
                                           {
                                               MethodInfo methodInfo = type.GetMethod(validationRule.DLLInfo.RoutineName);

                                               if (methodInfo != null)
                                               {
                                                   object result = null;
                                                   ParameterInfo[] parameters = methodInfo.GetParameters();
                                                   object classInstance = Activator.CreateInstance(type, null);


                                                    // This works fine
                                                    if (validationRule.DLLInfo.IsStaticMethod)
                                                       result = methodInfo.Invoke(null, new object[] { content });
                                                   else
                                                       result = methodInfo.Invoke(classInstance, new object[] { content });


                                                   if (!(bool)result)
                                                   {
                                                       logger.Error($"Thread Number : {Thread.CurrentThread.ManagedThreadId}, Flat File Line Number : {randomLineNumber + 1}, Field Name : {fields.FieldName}, Error Message : {validationRule.ErrorMessage} ");
                                                       isError = true;
                                                   }
                                               }
                                               else
                                               {
                                                   logger.Error($"Error while getting method infor for {validationRule.DLLInfo.DLLName}");
                                                   isError = true;
                                               }
                                           }
                                       }
                                       break;
                               }
                           }
                       }

                       if (!isError)
                       {

                           TotalSeccessfullyProcessedRecords++;
                       }
                       else
                       {
                           TotalErrorRecords++;
                       }

                       sqlManipulation.InsertRecord(connection, tableName, randomLineNumber + 1, isError, logger);

                       if (endTime < DateTime.Now)
                       {

                           logger.Info($"As per settings ended the program at End Time {endTime}");
                           loopState.Stop();
                       }
                   }
                   catch
                   {

                   }
               });
            processedDetails.TotalRecords = TotalRecords;
            processedDetails.TotalErrorRecords = TotalErrorRecords;
            processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
            return processedDetails;
        }
    }
}
