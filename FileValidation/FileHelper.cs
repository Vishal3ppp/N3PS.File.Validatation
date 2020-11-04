using N3PS.File.Compare.BusinessLogic;
using N3PS.File.Compare.SQLLiteManiputation;
using N3PS.File.Compare.XMLConfigClasses;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
//using System.Data.SQLite;
//using SQLite;
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
    class Factors
    {
        public string start { get; set; }
        public string end { get; set; }
    }
    class FileHelper
    {

        public List<int> GetBags(int totalRecords,int forIterate, SQLiteHelper sqlManipulation, System.Data.SQLite.SQLiteConnection connection,string  processedTable1, Logger logger)
        {
            List<int> ints = new List<int>();
            for (int i = 0; i < forIterate; i++)
            {
                Random rmd = new Random();
                int randomLineNumber = rmd.Next(totalRecords);
                //logger.Info($"Generated Line Number : {randomLineNumber + 1}");
               DataSet records = sqlManipulation.GetTotalRecordsInTable(connection,$"SELECT FlatFileRowNumber FROM ProcessedICATable1 WHERE FlatFileRowNumber = {randomLineNumber}", logger);//..RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);


                int count = records.Tables[0].Rows.Count;
                int loopIteration = 1;
                bool allCompleted = false;
                while (count > 0)
                {
                    randomLineNumber = randomLineNumber + 1;
                    if (randomLineNumber > totalRecords - 1)
                    {
                        randomLineNumber = 0;
                    }
                    //ds = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    records = sqlManipulation.GetTotalRecordsInTable(connection, $"SELECT FlatFileRowNumber FROM ProcessedICATable1 WHERE FlatFileRowNumber = {randomLineNumber}",logger);
                    //logger.Info($"Total Records with Flat File Line Number : {randomLineNumber + 1} and Total Returned Count : {ds.Tables[0].Rows.Count}");
                    count = records.Tables[0].Rows.Count;
                    if (loopIteration > totalRecords)
                    {
                        allCompleted = true;
                        break;
                    }
                    loopIteration++;
                    //forIterate++;
                }

                if (allCompleted)
                    break;
                //break;
                //DataSet dsMain = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                //if (dsMain.Tables[0].Rows.Count <= 0)
                //{

                    sqlManipulation.InsertRecord(connection, processedTable1, randomLineNumber + 1, false, logger);
                //}
                //logger.Info($"randomLineNumber --- {randomLineNumber+1}");
                ints.Add(randomLineNumber);
            }
            //loopState.Stop();
            
            //logger.Info($"Random Line Number : {randomLineNumber + 1}");
            return ints;
        }
        public ProcessedDetails Compare(FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj,  SQLiteHelper sqlManipulation, System.Data.SQLite.SQLiteConnection connection, string DBName, string tableName1,string tableName2, string processedTable1, string processedTable2, string deletedTable, string insertedTable, int totalRecords, DataSet dsTotalRecords, Logger logger)
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
                foreach (DataRow dr in  dsTotalRecords.Tables[0].Rows)
                {
                    int records = Convert.ToInt32(dr["TotalRecords"].ToString());

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
            ops.MaxDegreeOfParallelism = 10;
            object obj = new object();


            
            for (int i = 0; i < GetFactors(totalCount); i++)
            {
                int startCount = (i * 100);
                int lastCount = ((i + 1) * 100) > totalCount ? totalCount : ((i + 1) * 100);
                //for (int i = 0; i < totalCount; i++)
                List<int> bags = GetBags(totalRecords, lastCount-startCount, sqlManipulation, connection, processedTable1, logger);
                logger.Info($"startCount : {startCount}, lastCount : {lastCount}");
                Parallel.ForEach(bags, ops, (randomLineNumber, loopState) =>

            {
                try
                {
                    bool isError = false;


                    // int randomLineNumber = -1;
                    //DataSet ds = new DataSet();

                    //lock (obj)
                    //{
                    //    randomLineNumber = rmd.Next(totalRecords);
                    //    //logger.Info($"Generated Line Number : {randomLineNumber + 1}");
                    //    ds = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    //    int loopIteration = 1;
                    //    bool allCompleted = false;
                    //    while (ds.Tables[0].Rows.Count > 0)
                    //    {
                    //        randomLineNumber = randomLineNumber + 1;
                    //        if (randomLineNumber > totalRecords - 1)
                    //        {
                    //            randomLineNumber = 0;
                    //        }
                    //        ds = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    //        //logger.Info($"Total Records with Flat File Line Number : {randomLineNumber + 1} and Total Returned Count : {ds.Tables[0].Rows.Count}");
                    //        if (loopIteration > TotalRecords)
                    //        {
                    //            allCompleted = true;
                    //            break;
                    //        }
                    //        loopIteration++;
                    //    }

                    //    if (allCompleted)
                    //        loopState.Stop();
                    //    //break;

                    //    //loopState.Stop();
                    //    logger.Info($"Random Line Number : {randomLineNumber + 1}");
                    //}

                    //logger.Info($"Random Line Number : {randomLineNumber + 1}");

                    string query1 = $"SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                    //sqlManipulation.GetTotalRecordsInTable(connection, query1, logger);
                    //Sqlite.TableMapping map = new Sqlite.TableMapping(Type.GetType(tableName1));
                    ////string query = "select * from " + TableName;
                    //object[] objLists = new object[] { };

                    //List <object> list = connection.Query(map, query1, objLists).ToList();

                    DataSet ds1 = sqlManipulation.GetTotalRecordsInTable(connection, query1, logger);
                    //List<DynamicClass1> list = null;//connection.Query<DynamicClass1>(query1, fieldLists.Cast<object>().ToArray());
                    DataRow randomLineContentFromTable1 = null;
                    string fetchedNRIC = null;
                    if (ds1.Tables[0].Rows.Count > 0)
                    {
                        randomLineContentFromTable1 = ds1.Tables[0].Rows[0];

                        fetchedNRIC = randomLineContentFromTable1[(fetchedFlatFileObj.PrimaryKeyColumnNumber)].ToString();
                    }
                    //logger.Info($"fetchedNRIC : {fetchedNRIC}");
                    string query2 = $"SELECT * FROM {tableName2} WHERE NRICNumber='{fetchedNRIC}';";
                    DataSet ds2 = sqlManipulation.GetTotalRecordsInTable(connection, query2, logger);
                    //DataSet ds2 = sqlManipulation.RetrieveRecord(connection, query2, logger);
                    //logger.Info(query2);
                    if (ds2.Tables[0].Rows.Count <= 0)
                    {
                        string query3 = $"INSERT INTO {deletedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                        //logger.Info($"Query 3 : {query3}");
                        sqlManipulation.InsertRecord(connection, query3, logger);
                    }
                    else
                    {
                        //string query3 = $"INSERT INTO {insertedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                        //string hashKey1 = string.Empty;
                        DataRow randomLineContentFromTable2 = ds2.Tables[0].Rows[0];
                        //namicClass1 class1 = (list.FirstOrDefault() as DynamicClass1);



                        //DataRow randomLineContentFromTable2 = ds2.Tables[0].Rows[0];
                        string hashKey1 = randomLineContentFromTable1["HashingKey"].ToString();
                        string hashKey2 = randomLineContentFromTable2["HashingKey"].ToString();
                        //logger.Info($"HashKey : {hashKey1} : {hashKey2}");

                        if (hashKey1 != hashKey2)
                        {

                            bool isDifference = false;
                            foreach (Fields f1 in fetchedFlatFileObj.fields)
                            {
                                //DynamicClass2 class2 = (list2.FirstOrDefault() as DynamicClass2);

                                var propSource = randomLineContentFromTable1[f1.FieldName].ToString();

                                //DynamicClass2 class2 = (list2.FirstOrDefault() as DynamicClass2);
                                var propDest = randomLineContentFromTable2[f1.FieldName].ToString();

                                if (propSource != propDest)
                                {
                                    isDifference = true;
                                }
                            }

                            if (isDifference)
                            {
                                string query4 = $"INSERT INTO {insertedTable} SELECT * FROM {tableName1} WHERE FlatFileRowNumber={randomLineNumber + 1};";
                                //connection.CreateTable<ICADeletedTable1>();
                                //connection.Insert(class1);
                                //logger.Info($"Query 4 : {query4}");
                                sqlManipulation.InsertRecord(connection, query4, logger);
                            }
                        }
                        else
                        {
                            //logger.Info($"Record is matching : {randomLineNumber + 1}");
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


                    //DataSet dsMain = sqlManipulation.RetrieveRecord(connection, processedTable1, randomLineNumber + 1, logger);
                    //if (dsMain.Tables[0].Rows.Count <= 0)
                    //{

                    //    sqlManipulation.InsertRecord(connection, processedTable1, randomLineNumber + 1, isError, logger);
                    //}
                    if (endTime < DateTime.Now)
                    {

                        logger.Info($"As per settings ended the program at End Time {endTime}");
                        //break;
                        loopState.Stop();
                    }
                    // }
                }
                catch (Exception excp)
                {
                    logger.Error($"Error in Comapring record : {randomLineNumber}    " + excp.ToString() + " ---- " + excp.StackTrace);
                }
            });
            }
            processedDetails.TotalRecords = TotalRecords;
            processedDetails.TotalErrorRecords = TotalErrorRecords;
            processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
            return processedDetails;
        }


        public int GetFactors(int num)
        {
            int factors = num / 100;
            if(num % 100 > 0)
            {
                factors++;
            }
            return factors;
        }

        public void InsertInto(SQLiteHelper sqlManipulation, FlatFile fetchedFlatFileObj, SettingsFile fetchedSettingsObj, System.Data.SQLite.SQLiteConnection connection, string dBName, string tableName, string processedTableName, string filePath, Logger logger, dynamic objList, bool isFirst)
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

            ThreadLocal<KeyedHashAlgorithm> sha1 = new ThreadLocal<KeyedHashAlgorithm>(() =>
            {
                var kha = KeyedHashAlgorithm.Create("HMACSHA256");
                kha.Key = Encoding.UTF8.GetBytes("N3PS");
                return kha;
            });
            logger.Info($"allLines.Length : {allLines.Length}");


            var fieldLists = new List<Field>();
            foreach (Fields f in fetchedFlatFileObj.fields)
            {
                Field f1 = new Field(f.FieldName, typeof(string));
                fieldLists.Add(f1);
            }
            fieldLists.Add(new Field("HashingKey", typeof(string)));
            //dynamic obj = new DynamicClass1(fieldLists);
            //List<DynamicClass1> objList = new List<DynamicClass1>();


            for (int i = 0; i < GetFactors(allLines.Length); i++)
            {
                int lastCount = ((i + 1) * 100) > allLines.Length ? allLines.Length: ((i + 1) * 100);
                StringBuilder query = new StringBuilder();
                query.Append($"INSERT INTO  {tableName} Values");
                Parallel.For((i*100), lastCount, ops, (rand, loopState) =>

                { 
                    try
                    {
                    //bool isError = false;


                    int randomLineNumber = -1;
                        DataSet ds = new DataSet();

                        randomLineNumber = rand;// rmd.Next(allLines.Length - 1);
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
                        //StringBuilder cols = new StringBuilder();
                        //StringBuilder values = new StringBuilder();
                        //StringBuilder combinedValues = new StringBuilder();
                        ////cols.AppendLine("(");
                        //cols.Append("FlatFileRowNumber,");

                        ////values.AppendLine("(");

                        //values.Append($"{randomLineNumber+1},");
                        dynamic obj = null;
                        if (isFirst)
                        {
                            obj = new DynamicClass1(fieldLists);
                        }
                        else
                        {
                            obj = new DynamicClass2(fieldLists);
                        }
                        
                        query.Append($"({(randomLineNumber+1)},");
                        StringBuilder vals = new StringBuilder();
                        vals.Append((randomLineNumber + 1));
                        for (int ik =0;ik<fetchedFlatFileObj.fields.Count;ik++)
                        //foreach (KeyValuePair<Type, object> kvp in ((IDictionary<Type, object>)obj))
                        {
                            string content = randomLineContent.Substring(fetchedFlatFileObj.fields[ik].StartPosition - 1, fetchedFlatFileObj.fields[ik].Length).Trim();
                            //cols.Append(fields.FieldName + ",");
                            //values.Append($"'{content.Replace("'","''")}',");
                            //combinedValues.Append(content);
                            //PropertyInfo info = obj.GetType().GetProperty(fields.FieldName);
                            //KeyValuePair<Type, object> p = obj._fields[fetchedFlatFileObj.fields[ik].FieldName];
                            query.Append($"'{content.Replace("'","''")}',");
                            vals.Append($"{content}");
                            //obj._fields[fetchedFlatFileObj.fields[ik].FieldName] =  content;

                            // p[ = content;
                            //obj.TrySetMember(fields.FieldName, content);
                            //info.SetValue(obj, content);

                            //obj[fields.ColumnNumber] = content;
                        }
                        var buffer = sha1.Value.ComputeHash(Encoding.Unicode.GetBytes(vals.ToString()));
                        //query[query.ToString().Length - 1] = ' ';
                        if(Encoding.Unicode.GetString(buffer, 0, buffer.Length).Contains("'"))
                        {

                        }
                        query.Append($"'{Encoding.Unicode.GetString(buffer, 0, buffer.Length).Replace("'", "''").Replace("\0","+")}'");
                        query.Append($"),");
                        query.AppendLine(Environment.NewLine);
                        //objList.Add(obj);
                        //connection.Insert(obj);
                        //cols[cols.ToString().Length - 1] = ' ';
                        //values[values.ToString().Length - 1] = ' ';
                        //tl.Value.ComputeHash(Encoding.UTF8.GetBytes("message"));
                        



                        //string insertQuery = $"INSERT INTO {tableName} ({cols.ToString()},HashingKey) VALUES ({values.ToString()},'{Encoding.Unicode.GetString(buffer, 0, buffer.Length).Replace("'","+")}')";

                        //logger.Error($"SQL Query : {insertQuery}");
                        //if(!sqlManipulation.InsertRecord(connection, insertQuery, logger))
                        //{
                        //    logger.Error($"Error in iserting record : {randomLineNumber} -- {insertQuery}");
                        //}
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
                    catch (Exception excp)
                    {
                        logger.Error($"Error in inserting record : {i} :  " + excp.ToString() + " ---- " + excp.StackTrace);
                    }
                });
                query[query.ToString().Length - 5] = ' ';
                //SQLiteConnection connection = new SQLiteConnection(new SQLite.Net.Platform.Win32.SQLitePlatformWin32(), $"{DBName}.sqlite");
                // connection = sqlManipulation.OpenDBConnection(dBName);
                //connection.CreateTable<DynamicClass1>(CreateFlags.None);
                sqlManipulation.InsertRecord(connection, query.ToString(), logger);
                Thread.Sleep(100);
                //connection.InsertAll(objList);
                //processedDetails.TotalRecords = TotalRecords;
                //processedDetails.TotalErrorRecords = TotalErrorRecords;
                //processedDetails.TotalSeccessfullyProcessedRecords = TotalSeccessfullyProcessedRecords;
                //return processedDetails;

            }
        }

        public void WriteToFile(SQLiteHelper sqlManipulation, FlatFile fetchedFlatFileObj, System.Data.SQLite.SQLiteConnection connection, string dBName, string tableName, Logger logger, int totalRecords)
        {
            StringBuilder query = new StringBuilder();
            query.Append($"SELECT rowid,");
            
            for (int ik = 0; ik < fetchedFlatFileObj.fields.Count; ik++)
            //foreach (KeyValuePair<Type, object> kvp in ((IDictionary<Type, object>)obj))
            {
                //string content = randomLineContent.Substring(fetchedFlatFileObj.fields[ik].StartPosition - 1, fetchedFlatFileObj.fields[ik].Length).Trim();
                //cols.Append(fields.FieldName + ",");
                //values.Append($"'{content.Replace("'","''")}',");
                //combinedValues.Append(content);
                //PropertyInfo info = obj.GetType().GetProperty(fields.FieldName);
                //KeyValuePair<Type, object> p = obj._fields[fetchedFlatFileObj.fields[ik].FieldName];
                query.Append($"{fetchedFlatFileObj.fields[ik].FieldName},");
                
                //obj._fields[fetchedFlatFileObj.fields[ik].FieldName] =  content;

                // p[ = content;
                //obj.TrySetMember(fields.FieldName, content);
                //info.SetValue(obj, content);

                //obj[fields.ColumnNumber] = content;
            }
            //var buffer = sha1.Value.ComputeHash(Encoding.Unicode.GetBytes(vals.ToString()));
            query[query.ToString().Length - 1] = ' ';
           
            query.Append($" FROM {tableName}");

            //int records = sqlManipulation.GetTotalRecordsInTable1(connection, tableName, logger);
            StreamWriter sw =  System.IO.File.CreateText($"{tableName}.txt");
            sw.Close();
            for (int i = 0; i < GetFactors(totalRecords); i++)
            {
                int lastCount = ((i + 1) * 100) > totalRecords ? totalRecords : ((i + 1) * 100);

                string completeQuery = query.ToString() + $" WHERE rowid >= {(i * 100) + 1} AND rowid <={lastCount}";

                DataSet ds = sqlManipulation.GetTotalRecordsInTable(connection, completeQuery, logger);
                //StreamWriter sw = System.IO.File.AppendText($"{tableName}.txt");
                //DataColumnCollection dc = ds.Tables[0].Columns;
                StringBuilder data = new StringBuilder();
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    foreach (Fields f in fetchedFlatFileObj.fields)
                    {
                        object ob = dr[f.FieldName];
                        if (ob != DBNull.Value)
                        {
                            data.Append(ob.ToString().PadRight(f.Length, ' '));
                        }

                       
                        
                    }
                    data.Append(Environment.NewLine);
                }

                System.IO.File.AppendAllText($"{tableName}.txt", data.ToString(), Encoding.ASCII);
            }
        }
        
    }
}
