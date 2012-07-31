using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Line
{
    public class LineUtils
    {
        public static int Identation(string line)
        {
            return line.Length - line.TrimStart().Length;
        }

    }
}
