using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CodeAnalyzer.Line;

namespace CodeAnalyzer.Analyzer
{
    public class Method
    {
        private string _methodName;
        private List<Parameter> _parameters; //NO SE ESTA UTILIZANDO, VER DSP!!!!
        private List<RecursiveCall> _recursiveCalls;
        private int _initLine;
        private int _numberOfParams;
        private int? _closeLine;
        private int _identationIndex;

        public Method(string line, int identationIndex, int lineIndex)
        {
            _methodName = GetMethodName(line);
            _numberOfParams = GetNumberOfParams(line);
            _recursiveCalls = new List<RecursiveCall>();
            _initLine = lineIndex;
            _identationIndex = identationIndex;
            _closeLine = null;
        }

        public static int GetNumberOfParams(string line)
        {
            int leftParenthesisIndex = line.IndexOf('(');

            if (leftParenthesisIndex != -1)
            {
                int rightParenthesisIndex = line.IndexOf(')');
                int offset = 0;
                if (rightParenthesisIndex == -1)
                    offset = line.Length - leftParenthesisIndex;
                else
                    offset = rightParenthesisIndex - leftParenthesisIndex;
                string[] parameters = line.Substring(leftParenthesisIndex, offset).Split(',');
                if (parameters.Length == 1 && parameters[0].Trim() == string.Empty)
                    return 0;
                return parameters.Length;
            }

            return -1;
        }

        public static string GetMethodName(string line)
        {
            string methodName = string.Empty;

            line = line.Trim();

            int leftParenthesisIndex = line.IndexOf('(');
            if (leftParenthesisIndex != -1)
            {
                string lineWithoutParenthesis = line.Substring(0, leftParenthesisIndex);

                bool containsEquals = lineWithoutParenthesis.Contains('=');
                if (!containsEquals)
                {
                    string[] splittedLine = lineWithoutParenthesis.Split(' ');
                    if (splittedLine.Length > 1)
                        methodName = splittedLine[splittedLine.Length - 1];
                }
            }

            return methodName;
        }

        public void AddRecursiveCall(string line, int lineIndex)
        {
            if (!_closeLine.HasValue)
            {
                RecursiveCall recursiveCall = new RecursiveCall(line, lineIndex);
                _recursiveCalls.Add(recursiveCall);
            }
            else 
            {
                throw new Exception("Cannot add recursive call to closed method");
            }
        }

        public List<RecursiveCall> RecursiveCalls
        {
            get { return _recursiveCalls; }
        }

        public void Close(int line)
        {
            _closeLine = line;
        }

        public int NumberOfParams
        {
            get { return _numberOfParams; }
        }

        public string Name
        {
            get { return _methodName; }
        }

        public int InitLineIndex
        {
            get { return _initLine; }
        }

        public int? CloseLineIndex
        {
            get { return _closeLine; }
        }

        public int LineCount
        {
            get
            {
                if (_closeLine.HasValue)
                    return _closeLine.Value - _initLine;
                else
                    throw new Exception("Method hasn't been closed");
            }
        }
        
        internal bool TryClosing(string line, int lineIndex)
        {
            int lineIdentationIndex = LineUtils.Identation(line);

            bool mustClose = lineIdentationIndex == _identationIndex && line.Trim() == "}";
            if (mustClose)
                this.Close(lineIndex);
            return mustClose;
        }

        internal static bool IsNewMethod(string line)
        {
            bool isNewMethod = false;

            line = line.Trim();

            int leftParenthesisIndex = line.IndexOf('(');
            if (leftParenthesisIndex != -1)
            {
                string lineWithoutParenthesis = line.Substring(0, leftParenthesisIndex);

                bool containsEquals = lineWithoutParenthesis.Contains('=');
                if (!containsEquals)
                {
                    string[] splittedLine = lineWithoutParenthesis.Split(' ');
                    isNewMethod = splittedLine.Length > 1;
                }
            }

            return isNewMethod;
        }
    }
}
