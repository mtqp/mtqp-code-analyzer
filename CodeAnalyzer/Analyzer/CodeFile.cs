using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CodeAnalyzer.Analyzer
{
    public class CodeFile
    {
        private StreamReader _fileStream;
        private List<ICodeAnalyserPolicy> _policies;
        private int _countLines;
        private bool _wasProcessed;

        public CodeFile(StreamReader fileStream) 
        {
            _fileStream = fileStream;
            _policies = new List<ICodeAnalyserPolicy>();
            _countLines = 0;
            _wasProcessed = false;
        }

        public int CountLines
        {
            get { return _countLines; }
        }

        public List<ICodeAnalyserPolicy> Policies
        {
            get { return _policies; }
        }

        public bool Processed
        {
            get { return _wasProcessed; }
        }

        public void LoadPolicies(ICodeAnalyserPolicy policy)
        {
            LoadPolicies(new List<ICodeAnalyserPolicy>() { policy });
        }

        public void LoadPolicies(List<ICodeAnalyserPolicy> policies)
        {
            if(_fileStream!=null)
                _policies = policies;
            else
                throw new Exception("Cannot load policies because no file has been loaded");
        }

        public void Process()
        {
            if(_fileStream == null)
                throw new Exception("Cannot start analysing process because no file has been loaded");

            string line = string.Empty;
            while (!_fileStream.EndOfStream)
            {
                _countLines++;
                line = _fileStream.ReadLine();

                if(!string.IsNullOrEmpty(line.Trim()))
                    foreach (ICodeAnalyserPolicy policy in _policies)
                        policy.AnalyseLine(line, _countLines);
            }
            _wasProcessed = true;
        }

        public void Close()
        {
            _fileStream.Close();
        }
    }
}
