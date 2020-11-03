using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using NLog;
using System.Data;

namespace N3PS.File.Compare.SQLLiteManiputation
{
    class SQLiteHelper
    {

        /// <summary>
        /// Check for is DB Exist.
        /// </summary>
        /// <param name="DBName"></param>
        /// <returns></returns>
        public bool IsDBExist(string DBName)
        {
            return System.IO.File.Exists($"{DBName}.sqlite");
            
        }


        /// <summary>
        /// Create SQL Lite DB
        /// </summary>
        /// <param name="DBName"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public bool CreateDB(string DBName, Logger logger)
        {
            bool isDBCreated = false;
            try
            {
                SQLiteConnection.CreateFile($"{DBName}.sqlite");
                isDBCreated = true;
            }
            catch(Exception excp)
            {
                logger.Error("Error while creating sql lite DB : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isDBCreated;
        }

        /// <summary>
        /// Create SQL Table i SQL Lite
        /// </summary>
        /// <param name="DBName"></param>
        /// <param name="sqlQuery"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public bool CreateTable(string DBName, string sqlQuery, Logger logger)
        {
            bool isTableCreated = false;
            try
            {
                SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                m_dbConnection.Open();



                SQLiteCommand command = new SQLiteCommand(sqlQuery, m_dbConnection);
                command.ExecuteNonQuery();



                m_dbConnection.Close();
                isTableCreated = true;
            }
            catch(Exception excp)
            {
                logger.Error("Error while creating sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isTableCreated;
        }




        public bool DeleteTable(string DBName, string tableName, Logger logger)
        {
            bool isTableCreated = false;
            try
            {
                SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                m_dbConnection.Open();



                SQLiteCommand command = new SQLiteCommand($"DROP TABLE {tableName}", m_dbConnection);
                command.ExecuteNonQuery();



                m_dbConnection.Close();
                isTableCreated = true;
            }
            catch (Exception excp)
            {
                logger.Error("Error while deleting sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isTableCreated;
        }


        public bool InsertRecord(SQLiteConnection m_dbConnection, string tableName, int flatFileRecordNumber, bool IsError , Logger logger)
        {
            bool isRecordInserted = false;
            try
            {
                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();
                string isErrorString = IsError? "1":"0";
                string query = $"INSERT INTO {tableName}(FlatFileRowNumber, IsError) VALUES({flatFileRecordNumber},{isErrorString})";

                SQLiteCommand command = new SQLiteCommand(query, m_dbConnection);
                command.ExecuteNonQuery();



                //m_dbConnection.Close();
                isRecordInserted = true;
            }
            catch (Exception excp)
            {
                logger.Error("Error while creating sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isRecordInserted;
        }

        public int GetTotalRecordsInTable(SQLiteConnection m_dbConnection, string tableName, Logger logger)
        {
            int records = 0;// new DataSet();

            try
            {
                string query = $"SELECT count(1) TotalRecords FROM {tableName};";

                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                DataSet myDataSet = new DataSet();
                myAdapter.Fill(myDataSet, "Records");

                if (myDataSet.Tables[0].Rows.Count > 0)
                {
                    records = Convert.ToInt32(myDataSet.Tables[0].Rows[0][0].ToString());
                }
                //m_dbConnection.Close();




            }
            catch (Exception excp)
            {
                logger.Error("Error while retrieving from sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return records;
        }


        

        public DataSet CheckTableExists(SQLiteConnection m_dbConnection, string tableName, Logger logger)
        {
            DataSet myDataSet = new DataSet();

            try
            {
                string query = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';";

                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;

                myAdapter.Fill(myDataSet, "Records");


                //m_dbConnection.Close();




            }
            catch (Exception excp)
            {
                logger.Error("Error while retrieving from sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return myDataSet;
        }

        public bool InsertRecord(SQLiteConnection m_dbConnection, string query, Logger logger)
        {
            bool isRecordInserted = false;
            try
            {
                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();
                //string isErrorString = IsError ? "1" : "0";
                //string query = $"INSERT INTO {tableName}(FlatFileRowNumber, IsError) VALUES({flatFileRecordNumber},{isErrorString})";

                SQLiteCommand command = new SQLiteCommand(query, m_dbConnection);
                command.ExecuteNonQuery();



                //m_dbConnection.Close();
                isRecordInserted = true;
            }
            catch (Exception excp)
            {
                logger.Error("Error while inserting sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isRecordInserted;
        }

        public DataSet GetTotalRecords(SQLiteConnection m_dbConnection, string tableName, Logger logger)
        {
            DataSet myDataSet = new DataSet();
            //bool isRecordInserted = false;
            try
            {
                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                string query = $"SELECT IsError, Count(1) AS TotalRecords FROM {tableName} GROUP BY IsError";
                //string query = $"SELECT FlatFileRowNumber FROM {tableName} WHERE FlatFileRowNumber =  {flatFileRecordNumber}";

                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;

                myAdapter.Fill(myDataSet, "Records");
                //SQLiteCommand command = new SQLiteCommand(query, m_dbConnection);
                //command.ExecuteNonQuery();



                //m_dbConnection.Close();
                
            }
            catch (Exception excp)
            {
                logger.Error("Error while creating sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return myDataSet;
        }


        public DataSet RetrieveRecord(SQLiteConnection m_dbConnection, string query, Logger logger)
        {
            DataSet myDataSet = new DataSet();

            try
            {
                //string query = $"SELECT FlatFileRowNumber FROM {tableName} WHERE FlatFileRowNumber =  {flatFileRecordNumber}";

                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;

                myAdapter.Fill(myDataSet, "Records");


                //m_dbConnection.Close();




            }
            catch (Exception excp)
            {
                logger.Error("Error while retrieving from sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return myDataSet;
        }


        public DataSet RetrieveRecord(SQLiteConnection m_dbConnection, string tableName, int flatFileRecordNumber, Logger logger)
        {
            DataSet myDataSet = new DataSet();

            try
            {
                string query = $"SELECT FlatFileRowNumber FROM {tableName} WHERE FlatFileRowNumber =  {flatFileRecordNumber}";

                //SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                //m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                
                myAdapter.Fill(myDataSet, "Records");


                //m_dbConnection.Close();




            }
            catch (Exception excp)
            {
                logger.Error("Error while retrieving from sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return myDataSet;
        }


        public SQLiteConnection OpenDBConnection(string DBName)
        {
            SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
            m_dbConnection.Open();

            return m_dbConnection;
        }

        public SQLiteConnection CloseDBConnection(SQLiteConnection m_dbConnection)
        {
            m_dbConnection.Close();

            return m_dbConnection;
        }
    }
}
