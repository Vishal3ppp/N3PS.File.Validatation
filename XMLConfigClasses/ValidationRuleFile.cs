using N3PS.File.Compare.BusinessLogic;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace N3PS.File.Compare.XMLConfigClasses
{
    class ValidationRuleFile
    {
        public List<ValidationsRule> ValidationRules { get; set; }

        public ValidationRuleFile GetInstance(string validationRuleXMLFile, Logger logger)
        {
            logger.Info("Inside the ValidationRuleFile.GetInstance() method.");
            ValidationRuleFile validations = new ValidationRuleFile();
            validations.ValidationRules = new List<ValidationsRule>();
            try
            {
                logger.Info("Loading the Validation Xml File.");
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(validationRuleXMLFile);

                XmlNodeList validationNodes = xmlDoc.SelectNodes("//ValidationRules/ValidationRule");

                foreach(XmlNode validationNode in validationNodes)
                {
                    ValidationsRule rule = new ValidationsRule();
                    if (validationNode.Attributes["ValidationId"] != null)
                    {

                        rule.ValidationId = Convert.ToInt32(validationNode.Attributes["ValidationId"].Value);
                        logger.Info("Fetched Validation Id." + rule.ValidationId);
                    }
                    else
                    {
                        logger.Info("Validation Id is not specified so end the loop.");
                        break;
                    }



                    if (validationNode.Attributes["ValidationName"] != null)
                    {

                        rule.ValidationName = validationNode.Attributes["ValidationName"].Value;
                        logger.Info("Fetched Validation Name." + rule.ValidationId);
                    }
                    else
                    {
                        logger.Info("Validation Name is not specified so end the loop.");
                        break;
                    }


                    if (validationNode.Attributes["ColumnNumber"] != null)
                    {

                        rule.ColumnNumber = Convert.ToInt32(validationNode.Attributes["ColumnNumber"].Value);
                        logger.Info("Fetched Column Number." + rule.ValidationId);
                    }
                    else
                    {
                        logger.Info("Column Number is not specified so end the loop.");
                        break;
                    }



                    logger.Info("Fetching Validation Type element.");
                    XmlNode validationTypeNode = validationNode.SelectSingleNode("ValidationType");
                    if (validationTypeNode != null)
                    {

                        rule.ValidationType = validationTypeNode.InnerText.Trim();
                        logger.Info("Fetched Validation Type element : " + rule.ValidationType);
                    }
                    else
                    {
                        logger.Info("Validation Type is not specified so end the loop.");
                        break;
                    }




                    logger.Info("Fetching Validation Size element.");
                    XmlNode validationSizeNode = validationNode.SelectSingleNode("ValidationSize");
                    if (validationSizeNode != null)
                    {

                        rule.ValidationSize = Convert.ToInt32(validationSizeNode.InnerText.Trim());
                        logger.Info("Fetched Validation Size element : " + rule.ValidationSize);
                    }
                    


                    logger.Info("Fetching Error Message element.");
                    XmlNode errorMessageNode = validationNode.SelectSingleNode("ErrorMessage");
                    if (errorMessageNode != null)
                    {

                        rule.ErrorMessage = errorMessageNode.InnerText.Trim();
                        logger.Info("Fetched Error Message element : " + rule.ErrorMessage);
                    }
                    else
                    {
                        logger.Info("Error Message is not specified so end the loop.");
                        break;
                    }


                    logger.Info("Fetching Values element.");
                    XmlNode valuesNode = validationNode.SelectSingleNode("Values");
                    if (valuesNode != null)
                    {
                        XmlNodeList valuesList = valuesNode.SelectNodes("value");
                        if(valuesList != null)
                        {
                            rule.Values = new List<string>();
                            foreach (XmlNode valNode in valuesList)
                            {
                                rule.Values.Add(valNode.InnerText.Trim());
                            }
                        }
                        
                        logger.Info("Fetched Values element : " + rule.Values.Count);
                    }
                   

                    logger.Info("Fetching Date Format element.");
                    XmlNode dateFormatNode = validationNode.SelectSingleNode("DateFormat");
                    if (dateFormatNode != null)
                    {

                        rule.DateFormat = dateFormatNode.InnerText.Trim();
                        logger.Info("Fetched Date Format element : " + rule.ErrorMessage);
                    }



                    rule.DLLInfo = new DLLDetails();
                    XmlNode DLLNameNode = validationNode.SelectSingleNode("DLL/DLLName");
                    if (DLLNameNode != null)
                    {

                        rule.DLLInfo.DLLName = DLLNameNode.InnerText.Trim();
                        logger.Info("Fetched DLLName element : " + rule.DLLInfo.DLLName);
                    }


                    XmlNode fullyQualififedClassNameNode = validationNode.SelectSingleNode("DLL/FullyQualififedClassName");
                    if (fullyQualififedClassNameNode != null)
                    {

                        rule.DLLInfo.FullyQualififedClassName = fullyQualififedClassNameNode.InnerText.Trim();
                        logger.Info("Fetched FullyQualififedClassName element : " + rule.DLLInfo.FullyQualififedClassName);
                    }


                    XmlNode routineNameNode = validationNode.SelectSingleNode("DLL/RoutineName");
                    if (routineNameNode != null)
                    {

                        rule.DLLInfo.RoutineName = routineNameNode.InnerText.Trim();
                        logger.Info("Fetched Routine Name element : " + rule.DLLInfo.RoutineName);

                        if (routineNameNode.Attributes["IsStaticMethod"] != null)
                        {

                            rule.DLLInfo.IsStaticMethod = Convert.ToBoolean(routineNameNode.Attributes["IsStaticMethod"].Value);
                            logger.Info("Fetched IsStaticMethod " + rule.DLLInfo.IsStaticMethod);
                        }


                        if (routineNameNode.Attributes["ReturnType"] != null)
                        {

                            rule.DLLInfo.ReturnType = routineNameNode.Attributes["ReturnType"].Value;
                            logger.Info("Fetched ReturnType " + rule.DLLInfo.ReturnType);
                        }



                        if (routineNameNode.Attributes["InputType"] != null)
                        {

                            rule.DLLInfo.InputType = routineNameNode.Attributes["InputType"].Value;
                            logger.Info("Fetched InputType " + rule.DLLInfo.InputType);
                        }
                    }





                    validations.ValidationRules.Add(rule);
                }
            }
            catch (Exception excp)
            {
                logger.Error("Error in parsing the Validation XML file : " + excp.ToString() + " --- " + excp.StackTrace);
                validations = null;
            }

            return validations;
        }


        
    }
}
