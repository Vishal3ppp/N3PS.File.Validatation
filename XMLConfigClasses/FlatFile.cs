using N3PS.File.Validatation.BusinessLogic;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace N3PS.File.Validatation.XMLConfigClasses
{
    class FlatFile
    {
        public List<Fields> fields { get; set; }

        public bool HeaderExist { get; set; }

        public int RecordSize { get; set; }

        public string FlatFilePath { get; set; }

        /// <summary>
        /// Get instance of FlatFile
        /// </summary>
        /// <param name="xmlPath"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public FlatFile GetInstance(string xmlPath, Logger logger)
        {
            logger.Info("Inside the FlatFile.GetInstance() method.");
            FlatFile flatFile = new FlatFile();
            flatFile.fields = new List<Fields>();

            try
            {
                logger.Info("Loading the Flat File Xml.");
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(xmlPath);



                logger.Info("Retrieving the HeaderExist element.");
                XmlNode headerExistNode = xmlDoc.SelectSingleNode("//FlatFileFormat/HeaderExist");
                if (headerExistNode != null)
                {
                    logger.Info("HeaderExist element is exist.");
                    flatFile.HeaderExist = headerExistNode.InnerText.Trim().ToLower() == "yes" ? true : false;
                }
                else
                {
                    logger.Info("HeaderExist element is not exist.");
                    flatFile.HeaderExist = false;
                }

                logger.Info("Retrieving the RecordSize element.");
                XmlNode recordSizeNode = xmlDoc.SelectSingleNode("//FlatFileFormat/RecordSize");
                if (recordSizeNode != null)
                {
                    logger.Info("RecordSize element is exist.");
                    flatFile.RecordSize = recordSizeNode.InnerText.Trim() == string.Empty ? 0 : Convert.ToInt32(recordSizeNode.InnerText);
                }
                else
                {
                    logger.Info("RecordSize element is not exist.");
                    flatFile.RecordSize = 0;
                }





                logger.Info("Retrieving the FlatFile element.");
                XmlNode flatFilePathNode = xmlDoc.SelectSingleNode("//FlatFileFormat/FlatFilePath");
                if (flatFilePathNode != null)
                {
                    logger.Info("FlatFile element is exist.");
                    flatFile.FlatFilePath = flatFilePathNode.InnerText.Trim();
                }
                else
                {
                    logger.Info("FlatFile element is not exist.");
                    flatFile.FlatFilePath = string.Empty;
                }
                //flatFile.FlatFilePath = xmlPath;


                logger.Info("Retrieve all fields.");
                XmlNodeList fieldNodes = xmlDoc.SelectNodes("//FlatFileFormat/Fields/Field");

                if (fieldNodes != null)
                {
                    foreach (XmlNode fieldNode in fieldNodes)
                    {
                        Fields field = new Fields();
                        if (fieldNode.Attributes["Name"] != null)
                        {

                            field.FieldName = fieldNode.Attributes["Name"].Value;
                            logger.Info("Fetched name of Field." + field.FieldName);
                        }
                        else
                        {
                            logger.Info("Field Name is not specified so end the loop.");
                            break;
                        }


                        if (fieldNode.Attributes["ColumnNumber"] != null)
                        {

                            field.ColumnNumber = Convert.ToInt32(fieldNode.Attributes["ColumnNumber"].Value);
                            logger.Info("Fetched Column Number of Field." + field.ColumnNumber);
                        }
                        else
                        {
                            logger.Info("Field Column Numbeer is not specified so end the loop.");
                            break;
                        }


                        logger.Info("Fetching start position element.");
                        XmlNode startPosNode = fieldNode.SelectSingleNode("StartPos");
                        if (startPosNode != null)
                        {

                            field.StartPosition = Convert.ToInt32(startPosNode.InnerText.Trim());
                            logger.Info("Fetched start position element : " + field.StartPosition);
                        }
                        else
                        {
                            logger.Info("Field Start Position is not specified so end the loop.");
                            break;
                        }




                        logger.Info("Fetching Length element.");
                        XmlNode lengthNode = fieldNode.SelectSingleNode("Length");
                        if (lengthNode != null)
                        {

                            field.Length = Convert.ToInt32(lengthNode.InnerText.Trim());
                            logger.Info("Fetched Length element : " + field.Length);
                        }
                        else
                        {
                            logger.Info("Field Start Position is not specified so end the loop.");
                            break;
                        }

                        logger.Info("Adding field into field list.");
                        flatFile.fields.Add(field);
                    }



                }
            }catch(Exception excp)
            {
                logger.Error("Error in parsing the Flat File XML : " + excp.ToString() + " --- " + excp.StackTrace);
                flatFile = null;
            }
            return flatFile;
        }


        public string CreateFlatFileTableScript(string tableName)
        {
            StringBuilder tableCreateScript = new StringBuilder();
            tableCreateScript.AppendLine($"Create Table {tableName}");
            tableCreateScript.AppendLine("(");

           // tableCreateScript.AppendLine("RowNumber INT IDENTITY,");
            tableCreateScript.AppendLine("FlatFileRowNumber INT,");
            tableCreateScript.AppendLine("IsError bool");
            tableCreateScript.AppendLine(")");

            return tableCreateScript.ToString();

        }
    }
}
