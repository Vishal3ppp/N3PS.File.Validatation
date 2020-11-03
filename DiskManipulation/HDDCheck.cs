using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N3PS.File.Compare.DiskManipulation
{
    class HDDCheck
    {

        /// <summary>
        /// Check free space exist
        /// </summary>
        /// <param name="flatFilePath"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public bool IsEnoughSpaceAvailable(string flatFilePath, Logger logger)
        {
            bool isFreeSpaceAvailable = false;
            try
            {

                logger.Info("Get current drive name");
                string root = Path.GetPathRoot(System.Reflection.Assembly.GetEntryAssembly().Location);
                logger.Info("Current drive name : " + root);

                DriveInfo currentDrive = DriveInfo.GetDrives().Where(x => x.Name == root).SingleOrDefault();

                FileInfo flatFileInfo = new FileInfo(flatFilePath);


                if (currentDrive.TotalFreeSpace > (flatFileInfo.Length * 4))
                {
                    logger.Info("There is free available space.");
                    isFreeSpaceAvailable = true;
                }
                else
                {
                    logger.Info("There is not free available space on drive.");
                }
            }catch(Exception excp)
            {
                logger.Error("Error while fetching the drive information : " + excp.ToString() + " --- " + excp.StackTrace);
            }
            return isFreeSpaceAvailable;
        }
    }
}
