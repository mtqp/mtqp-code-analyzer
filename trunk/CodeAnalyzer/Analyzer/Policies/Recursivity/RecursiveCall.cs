using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Analyzer
{
    public class RecursiveCall
    {
        public string Line {get;set;}
        public int Index {get;set;}
        public RecursiveCall(string line, int lineIndex)
        {
            this.Line = line;
            this.Index = lineIndex;
        }
    }
}
