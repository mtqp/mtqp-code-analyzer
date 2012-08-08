using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Analyzer
{
    public interface ICodeAnalyserPolicy : IAnalysisListParser
    {
        void AnalyseLine(string line, int lineIndex);
        string Name { get; }
    }
}
