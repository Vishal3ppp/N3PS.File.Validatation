using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using NLog;
using System.Data;

namespace N3PS.File.Validatation.SQLLiteManiputation
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



        public bool InsertRecord(string DBName, string tableName, int flatFileRecordNumber, string errorMessage, Logger logger)
        {
            bool isRecordInserted = false;
            try
            {
                SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                m_dbConnection.Open();

                string query = $"INSERT INTO {tableName}(FlatFileRowNumber,ErrorMessage) VALUES({flatFileRecordNumber},'{errorMessage}')";

                SQLiteCommand command = new SQLiteCommand(query, m_dbConnection);
                command.ExecuteNonQuery();



                m_dbConnection.Close();
                isRecordInserted = true;
            }
            catch (Exception excp)
            {
                logger.Error("Error while creating sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return isRecordInserted;
        }



        public DataSet RetrieveRecord(string DBName, string tableName, int flatFileRecordNumber, Logger logger)
        {
            DataSet myDataSet = new DataSet();

            try
            {
                string query = $"SELECT FlatFileRowNumber,ErrorMessage FROM {tableName} WHERE FlatFileRowNumber =  {flatFileRecordNumber}";

                SQLiteConnection m_dbConnection = new SQLiteConnection($"Data Source={DBName}.sqlite;Version=3;");
                m_dbConnection.Open();

                SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(query, m_dbConnection);
                //myAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                
                myAdapter.Fill(myDataSet, "Records");


                m_dbConnection.Close();




            }
            catch (Exception excp)
            {
                logger.Error("Error while retrieving from sql lite Table : " + excp.ToString() + " --- " + excp.StackTrace);
            }

            return myDataSet;
        }
    }
}
