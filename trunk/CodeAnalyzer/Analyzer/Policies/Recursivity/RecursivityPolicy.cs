using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CodeAnalyzer.Line;

namespace CodeAnalyzer.Analyzer
{
    public class RecursivityPolicy : ICodeAnalyserPolicy
    {
        List<Method> _methods;
        bool _parsingMethod;

        public RecursivityPolicy()
        {
            _methods = new List<Method>();
            _parsingMethod = false;
        }

        public void AnalyseLine(string line, int lineIndex)
        {
            int identationIndex = LineUtils.Identation(line);

            if (Method.IsNewMethod(line) && !_parsingMethod)
            {
                Method method = new Method(line, identationIndex, lineIndex);
                _methods.Add(method);
                _parsingMethod = true;
            }
            else
            {
                if (_parsingMethod)
                {
                    if (ActualMethodContainsRecursion(line))
                        ActualMethod.AddRecursiveCall(line, lineIndex);

                    bool closed = ActualMethod.TryClosing(line, lineIndex);
                    _parsingMethod = !closed;
                }
            }
        }

        private bool ActualMethodContainsRecursion(string line)
        {
            string methodName = Method.GetMethodName(line);
            bool sameNameMethod = methodName.Trim() == ActualMethod.Name.Trim();
            if (sameNameMethod)
            {
                int countParameters = Method.GetNumberOfParams(line);
                bool sameCountParameters = countParameters == ActualMethod.NumberOfParams;
                return sameCountParameters;
            }
            return false;
        }

        private Method ActualMethod
        {
            get 
            {
                if (_methods.Count != 0)
                    return _methods[_methods.Count - 1];
                else
                    return null;
            }
        }

        public List<AnalysisColumn> GetColumns()
        {
            List<AnalysisColumn> columns = new List<AnalysisColumn>();
            AnalysisColumn colMethodName = new AnalysisColumn("MethodName", 0);
            AnalysisColumn colCantParameters = new AnalysisColumn("Number of params", 1);
            AnalysisColumn colLine = new AnalysisColumn("Line", 2);
            AnalysisColumn colMatchingLine = new AnalysisColumn("Description", 3);
            columns.Add(colMethodName);
            columns.Add(colCantParameters);
            columns.Add(colLine);
            columns.Add(colMatchingLine);
            return columns;
        }

        public List<AnalysisData> GetData()
        {
            List<AnalysisData> analysisData = new List<AnalysisData>();

            foreach (Method method in _methods)
            {
                if (method.RecursiveCalls.Count > 0)
                {
                    List<string> startData = new List<string>() { method.Name.ToString(), method.NumberOfParams.ToString(), method.InitLineIndex.ToString(), "method starts" };
                    analysisData.Add(new AnalysisData(startData));

                    foreach (RecursiveCall recursiveCall in method.RecursiveCalls)
                    {
                        List<string> matchData = new List<string>() { string.Empty, string.Empty, recursiveCall.Index.ToString() , recursiveCall.Line };
                        analysisData.Add(new AnalysisData(matchData));
                    }

                    string endLine = method.CloseLineIndex.HasValue ? method.CloseLineIndex.Value.ToString() : "cycle hasn't ended yet";
                    List<string> endData = new List<string>() { string.Empty, string.Empty, endLine, "method ends" };
                    analysisData.Add(new AnalysisData(endData));
                }
            }

            return analysisData;
        }

        public string Name
        {
            get { return "Recursivity"; }
        }
    }
}
