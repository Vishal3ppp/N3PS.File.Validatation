using N3PS.File.Compare.BusinessLogic;
using N3PS.File.Compare.SQLLiteManiputation;
using N3PS.File.Compare.XMLConfigClasses;
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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace N3PS.File.Compare
{
    class FileHelper
    {
        public ProcessedDetails Compare(FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj,  SQLiteHelper sqlManipulation, SQLiteConnection connection, string DBName, string tableName1,string tableName2, string processedTable1, string processedTable2, string deletedTable, string insertedTable, int totalRecords, DataSet dsTotalRecords, Logger logger)
        {
            //string[] allLines = System.IO.File.ReadAllLines(fetchedFlatFileObj.FlatFilePath1);

            if (fetchedSettingsObj.Percentage == 0)
            {
                fetchedSettingsObj.Percentage = 100;
            }
            decimal totalCountDec = Convert.ToDecimal((fetchedSettingsObj.Percentage / 100) * totalRecords);
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

                if (TotalRecords >= totalRecords)
                {
                    logger.Info("All records are processed, in case of any error please check.");
                    
                    processedDetails.TotalRecords = TotalRecords;
                    processedDetails.TotalErrorRecords = TotalErrorRecords;
                    processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
                    return processedDetails;
                }
            }

            TotalRecords = totalRecords;
            logger.Info($"Total count = {totalCount}, ({fetchedSettingsObj.Percentage} / 100 )* {totalRecords}");
            

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
            ops.MaxDegreeOfParallelism = 2;
            for (int i = 0; i < totalCount; i++)
            //Parallel.For(0, totalCount, ops, (i, loopState) =>

            {
                try
                {
                    bool isError = false;


                    int randomLineNumber = -1;
                    DataSet ds = new DataSet();

                    randomLineNumber = rmd.Next(totalRecords);
                    //logger.Info($"Generated Line Number : {randomLineNumber + 1}");
                    ds = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    int loopIteration = 1;
                    bool allCompleted = false;
                    while (ds.Tables[0].Rows.Count > 0)
                    {
                        randomLineNumber = randomLineNumber + 1;
                        if (randomLineNumber > totalRecords - 1)
                        {
                            randomLineNumber = 0;
                        }
                        ds = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                        //logger.Info($"Total Records with Flat File Line Number : {randomLineNumber + 1} and Total Returned Count : {ds.Tables[0].Rows.Count}");
                        if (loopIteration > TotalRecords)
                        {
                            allCompleted = true;
                            break;
                        }
                        loopIteration++;
                    }

                    if (allCompleted)
                        break;
                    //loopState.Stop();
                    //logger.Info($"Random Line Number : {randomLineNumber + 1}");
                    string query1 = $"SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber+1};";
                    DataSet ds1 = sqlManipulation.RetrieveRecord(connection, query1, logger);
                    DataRow randomLineContentFromTable1 = ds1.Tables[0].Rows[0];
                    string fetchedNRIC = randomLineContentFromTable1[(fetchedFlatFileObj.PrimaryKeyColumnNumber)].ToString();

                    string query2 = $"SELECT * FROM {tableName2} WHERE NRICNumber='{fetchedNRIC}';";
                    DataSet ds2 = sqlManipulation.RetrieveRecord(connection, query2, logger);
                    //logger.Info("Check ");
                    if(ds2.Tables[0].Rows.Count < 0)
                    {
                        string query3 = $"INSERT INTO {deletedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                        logger.Info($"Query 3 : {query3}");
                        sqlManipulation.InsertRecord(connection, query3, logger);
                    }
                    else
                    {
                        //string query3 = $"INSERT INTO {insertedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                        string hashKey1 = string.Empty;
                        // DataRow randomLineContentFromTable1 = ds1.Tables[0].Rows[0];
                        if (randomLineContentFromTable1["HashingKey"] != DBNull.Value)
                        {
                             hashKey1 = randomLineContentFromTable1["HashingKey"].ToString();
                        }
                        else
                        {

                        }

                        DataRow randomLineContentFromTable2 = ds2.Tables[0].Rows[0];

                        string hashKey2 = randomLineContentFromTable2["HashingKey"].ToString();
                        logger.Info($"HashKey : {hashKey1} : {hashKey2}");

                        if (hashKey1 != hashKey2)
                        {
                            
                            bool isDifference = false;
                            foreach(DataColumn cl in ds1.Tables[0].Columns)
                            {
                                if(randomLineContentFromTable1[cl.Caption] != randomLineContentFromTable2[cl.Caption])
                                {
                                    isDifference = true;
                                }
                            }

                            if(isDifference)
                            {
                                string query4 = $"INSERT INTO {insertedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                                logger.Info($"Query 4 : {query4}");
                                sqlManipulation.InsertRecord(connection, query4, logger);
                            }
                        }
                        else
                        {
                            logger.Info($"Record is matching : {randomLineNumber + 1}");
                        }
                        

                    }
                    
                    //Check Validations
                    //if (randomLineContent.Length > fetchedFlatFileObj.RecordSize)
                    //{
                    //    logger.Error($"Flat file line number {randomLineNumber + 1} exceeded specified Record Size.");
                    //    isError = true;
                    //}


                    //if (randomLineContent.Length < fetchedFlatFileObj.RecordSize)
                    //{
                    //    logger.Error($"Flat file line number {randomLineNumber + 1} less than specified Record Size.");
                    //    isError = true;
                    //}

                    
                    

                    //if (!isError)
                    //{

                    //    TotalSeccessfullyProcessedRecords++;
                    //}
                    //else
                    //{
                    //    TotalErrorRecords++;
                    //}


                    DataSet dsMain = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    if (dsMain.Tables[0].Rows.Count <= 0)
                    {

                        sqlManipulation.InsertRecord(connection, processedTable1, randomLineNumber + 1, isError, logger);
                    }
                    if (endTime < DateTime.Now)
                    {

                        logger.Info($"As per settings ended the program at End Time {endTime}");
                        break;
                        //loopState.Stop();
                    }
                }
                catch(Exception excp)
                {
                    logger.Error("Error in Comapring record : " + excp.ToString() + " ---- " + excp.StackTrace);
                }
            }//);
            processedDetails.TotalRecords = TotalRecords;
            processedDetails.TotalErrorRecords = TotalErrorRecords;
            processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
            return processedDetails;
        }

        public void InsertInto(SQLiteHelper sqlManipulation,  FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj, SQLiteConnection connection, string dBName, string tableName, string processedTableName, string filePath, Logger logger)
        {
            string[] allLines = System.IO.File.ReadAllLines(filePath);

            /*if (fetchedSettingsObj.Percentage == 0)
            {
                fetchedSettingsObj.Percentage = 100;
            }
            decimal totalCountDec = Convert.ToDecimal((fetchedSettingsObj.Percentage / 100) * allLines.Length);
            logger.Info($"Total Count as per Percentage : {totalCountDec.ToString("#.00")}");
            int totalCount = Convert.ToInt32(totalCountDec);
            if ((totalCount * 1.0M) < totalCountDec)
            {

                totalCount++;
                logger.Info($"Total Count as per Percentage after converting to Integer : {totalCount}");
            }



            ProcessedDetails processedDetails = new ProcessedDetails();
            int TotalRecords = 0;
            int TotalErrorRecords = 0;
            int TotalSeccessfullyProcessedRecords = 0;
            DataSet dsTotalRecords = sqlManipulation.GetTotalRecords(connection, processedTableName, logger);
            if (dsTotalRecords.Tables[0].Rows.Count > 0)
            {
                foreach (DataRow dr in dsTotalRecords.Tables[0].Rows)
                {
                    int records = Convert.ToInt32(dr[1].ToString());

                    if (Convert.ToBoolean(dr[0].ToString()))
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
            if (fetchedSettingsObj.Time == 0)
            {
                fetchedSettingsObj.Time = 24 * 60 * 365;
            }


            DateTime endTime = startTime.AddMinutes(fetchedSettingsObj.Time);
            logger.Info($"Start Time : {startTime}");
            logger.Info($"End Time : {endTime}");

            Random rmd = new Random();*/
            ParallelOptions ops = new ParallelOptions();
            ops.MaxDegreeOfParallelism = 1;
            //for (int i = 0; i < totalCount; i++)

            var sha1 = SHA1.Create();
            logger.Info($"allLines.Length : {allLines.Length}");
            Parallel.For(0, (allLines.Length), ops, (i, loopState) =>

            {
                try
                {
                    //bool isError = false;


                    int randomLineNumber = -1;
                    DataSet ds = new DataSet();

                    randomLineNumber = i;// rmd.Next(allLines.Length - 1);
                    //logger.Info($"Generated Line Number : {randomLineNumber + 1}");
                    //ds = sqlManipulation.RetrieveRecord(connection, processedTableName, randomLineNumber + 1, logger);
                    //int loopIteration = 1;
                    //bool allCompleted = false;
                    //while (ds.Tables[0].Rows.Count > 0)
                    //{
                    //    randomLineNumber = randomLineNumber + 1;
                    //    if (randomLineNumber > allLines.Length - 1)
                    //    {
                    //        randomLineNumber = 0;
                    //    }
                    //    ds = sqlManipulation.RetrieveRecord(connection, processedTableName, randomLineNumber + 1, logger);
                    //    //logger.Info($"Total Records with Flat File Line Number : {randomLineNumber + 1} and Total Returned Count : {ds.Tables[0].Rows.Count}");
                    //    if (loopIteration > TotalRecords)
                    //    {
                    //        allCompleted = true;
                    //        break;
                    //    }
                    //    loopIteration++;
                    //}

                    //if (allCompleted)
                    //    break;
                    //loopState.Stop();
                    //logger.Info($"Random Line Number : {randomLineNumber + 1}");
                    string randomLineContent = allLines[randomLineNumber];

                    //Check Validations
                    if (randomLineContent.Length > fetchedFlatFileObj.RecordSize)
                    {
                        logger.Error($"Flat file line number {randomLineNumber + 1} exceeded specified Record Size.");
                        //isError = true;
                    }


                    if (randomLineContent.Length < fetchedFlatFileObj.RecordSize)
                    {
                        logger.Error($"Flat file line number {randomLineNumber + 1} less than specified Record Size.");
                        //isError = true;
                    }
                    StringBuilder cols = new StringBuilder();
                    StringBuilder values = new StringBuilder();
                    StringBuilder combinedValues = new StringBuilder();
                    //cols.AppendLine("(");
                    cols.Append("FlatFileRowNumber,");

                    //values.AppendLine("(");

                    values.Append($"{randomLineNumber+1},");
                    foreach (Fields fields in fetchedFlatFileObj.fields)
                    {
                        string content = randomLineContent.Substring(fields.StartPosition - 1, fields.Length).Trim();
                        cols.Append(fields.FieldName + ",");
                        values.Append($"'{content}',");
                        combinedValues.Append(content);


                    }
                    cols[cols.ToString().Length - 1] = ' ';
                    values[values.ToString().Length - 1] = ' ';
                    var buffer = sha1.ComputeHash(Encoding.Unicode.GetBytes(combinedValues.ToString()));

                    

                    string insertQuery = $"INSERT INTO {tableName} ({cols.ToString()},HashingKey) VALUES ({values.ToString()},'{Encoding.Unicode.GetString(buffer, 0, buffer.Length).Replace("'","+")}')";

                    //logger.Error($"SQL Query : {insertQuery}");
                    if(!sqlManipulation.InsertRecord(connection, insertQuery, logger))
                    {
                        logger.Error($"Error in iserting record : {randomLineNumber}");
                    }
                    //if (!isError)
                    //{

                    //    TotalSeccessfullyProcessedRecords++;
                    //}
                    //else
                    //{
                    //    TotalErrorRecords++;
                    //}


                    //DataSet ds1 = sqlManipulation.RetrieveRecord(connection, processedTableName, randomLineNumber + 1, logger);
                    //if (ds1.Tables[0].Rows.Count <= 0)
                    //{

                    //    sqlManipulation.InsertRecord(connection, processedTableName, randomLineNumber + 1, isError, logger);
                    //}
                    //if (endTime < DateTime.Now)
                    //{

                    //    logger.Info($"As per settings ended the program at End Time {endTime}");
                    //    break;
                    //    //loopState.Stop();
                    //}
                }
                catch(Exception excp)
                {
                    logger.Error("Error in inserting record : " + excp.ToString() + " ---- " + excp.StackTrace);
                }
            });
            //processedDetails.TotalRecords = TotalRecords;
            //processedDetails.TotalErrorRecords = TotalErrorRecords;
            //processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
            //return processedDetails;

        }
    }
}
