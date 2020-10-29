using N3PS.File.Validatation.SQLLiteManiputation;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace N3PS.File.Validatation.XMLConfigClasses
{
    class SettingsFile
    {
        //public string LogLevel { get; set; }


        public decimal Percentage { get; set; }


        public int Time { get; set; }


        public SettingsFile GetInstance(string settingsXMLFile, Logger logger)
        {
            logger.Info("Inside the SettingsFile.GetInstance() method.");
            SettingsFile settings = new SettingsFile();

            logger.Info("Loading the Flat File Xml.");
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(settingsXMLFile);

            

            try
            {
                //logger.Info("Retrieving the LogLevel element.");
                //XmlNode logLevelNode = xmlDoc.SelectSingleNode("//Settings/LoggerSettings/LogLevel");
                //if (logLevelNode != null)
                //{
                //    logger.Info("LogLevel element is exist.");
                //    settings.LogLevel = logLevelNode.InnerText.Trim().ToLower();
                //}
                //else
                //{
                //    logger.Info("LogLevel element is not exist.");
                //    settings.LogLevel = string.Empty;
                //}


                logger.Info("Retrieving the Percentage element.");
                XmlNode percentageNode = xmlDoc.SelectSingleNode("//Settings/ValidationSetting/Percentage");
                if (percentageNode != null)
                {
                    logger.Info("Percentage element is exist.");
                    settings.Percentage = Convert.ToDecimal(percentageNode.InnerText.Trim().ToLower());
                }
                else
                {
                    logger.Info("Percentage element is not exist.");
                    settings.Percentage = 0.0M;
                }


                logger.Info("Retrieving the Time element.");
                XmlNode timeNode = xmlDoc.SelectSingleNode("//Settings/ValidationSetting/Time");
                if (timeNode != null)
                {
                    logger.Info("Time element is exist.");
                    settings.Time = Convert.ToInt32(timeNode.InnerText.Trim().ToLower());
                }
                else
                {
                    logger.Info("Time element is not exist.");
                    settings.Time = 0;
                }
            }catch(Exception excp)
            {
                logger.Error("Error in parsing the Settings XML : " + excp.ToString() + " --- " + excp.StackTrace);
                settings = null;
            }
            return settings;
        }
    }
}
