using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer
{
    public interface IAnalysisListParser
    {
        List<AnalysisColumn> GetColumns();
        List<AnalysisData> GetData();
    }
}
