using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Analyzer.Policies
{
    public class TabIndexPolicy : ICodeAnalyserPolicy
    {
        List<GraphicObject> _graphicObjects;

        public TabIndexPolicy()
        {
            _graphicObjects = new List<GraphicObject>();
        }

        public void AnalyseLine(string line, int lineIndex)
        {
        
            throw new NotImplementedException();
        }

        public List<AnalysisColumn> GetColumns()
        {
            throw new NotImplementedException();
        }

        public List<AnalysisData> GetData()
        {
            throw new NotImplementedException();
        }
    }
}
