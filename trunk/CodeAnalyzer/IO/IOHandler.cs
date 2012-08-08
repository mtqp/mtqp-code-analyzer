using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CodeAnalyzer.Enums;
using System.IO;
using CodeAnalyzer.Analyzer;

namespace CodeAnalyzer.IO
{
    public class IOHandler
    {
        public IOHandler()
        {}

        List<string> _allFiles = new List<string>();

        public List<string> GetAllFiles(string directory)
        {
            ProcessDir(directory, 0);
            return _allFiles;
        }
        
        // How much deep to scan. (of course you can also pass it to the method)
        private const int HowDeepToScan = 25;

        private void ProcessDir(string sourceDir, int recursionLvl)
        {
            if (recursionLvl <= HowDeepToScan)
            {
                // Process the list of files found in the directory. 
                string[] fileEntries = Directory.GetFiles(sourceDir);
                foreach (string fileName in fileEntries)
                {
                    if(fileName.Contains(".cs"))
                        _allFiles.Add(fileName);
                }

                // Recurse into subdirectories of this directory.
                string[] subdirEntries = Directory.GetDirectories(sourceDir);
                foreach (string subdir in subdirEntries)
                    // Do not iterate through reparse points
                    if ((File.GetAttributes(subdir) & FileAttributes.ReparsePoint) != FileAttributes.ReparsePoint)
                        ProcessDir(subdir, recursionLvl + 1);
            }
        }
        
        public static CodeFile TryOpen(string path, FileMode mode, out IOOperationResultEnum result)
        {
            StreamReader fileStream = null;
            CodeFile codeFile = null;

            if (File.Exists(path))
            {
                fileStream = new StreamReader(path); //FileStream(path, mode);
                string fileName = GetFileName(path);
                codeFile = new CodeFile(fileStream, fileName);
                result = IOOperationResultEnum.Success;
            }
            else
                result = IOOperationResultEnum.Error;

            return codeFile;
        }

        private static string GetFileName(string path)
        {
            string[] splittedPath = path.Split('\\');
            if (splittedPath.Length > 1)
                return splittedPath[splittedPath.Length - 1];

            throw new Exception("Incorrect path, cannot get file name");
        }
    }
}
