using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CodeAnalyzer.Line;

namespace CodeAnalyzer.Analyzer
{
    public class CycleWithPolicy : ISortable
    {
        private const string FOR_CYCLE = "for (";
        private const string FOR_CYCLE_SPACED = "for(";
        private const string WHILE_CYCLE = "while (";
        private const string WHILE_CYCLE_SPACED = "while(";
        private const string FOREACH_CYCLE = "foreach (";
        private const string FOREACH_CYCLE_SPACED = "foreach(";

        private const string KEY_OPEN = "{";
        private const string KEY_CLOSE = "}";

        private int _startLine;
        private int? _endLine;
        private int _currentLine;
        private int _identationIndex;
        private bool _isClosed;
        private string _policy;
        private Dictionary<int,string> _matchedPolicy;

        /*
         * FALTA DEFINIR LA IGUALDAD!!!!
         */

        public CycleWithPolicy(int startLine, int identationIndex, string policy)
        {
            _startLine = startLine;
            _currentLine = startLine;
            _identationIndex = identationIndex;
            _matchedPolicy = new Dictionary<int, string>();
            _policy = policy;
        }

        public int StartLine
        {
            get { return _startLine; }
        }

        public int? EndLine
        {
            get { return _endLine; }
        }

        public int IdentationIndex
        {
            get { return _identationIndex; }
        }

        public Dictionary<int, string> LineMatchPolicy
        {
            get { return _matchedPolicy; }
        }

        public static bool StartsCycle(string line)
        {
            string trimmed = line.Trim();
            bool startsCycle = trimmed.StartsWith(FOR_CYCLE) || trimmed.StartsWith(FOREACH_CYCLE) || trimmed.StartsWith(WHILE_CYCLE);
            startsCycle |= trimmed.StartsWith(FOR_CYCLE_SPACED) || trimmed.StartsWith(FOREACH_CYCLE_SPACED) || trimmed.StartsWith(WHILE_CYCLE_SPACED);
            return startsCycle;
        }

        public int CountMatchingPolicy
        {
            get { return _matchedPolicy.Count; }
        }

        public bool IsClosed
        {
            get { return _isClosed; }
        }

        public void ProcessLine(string line, int lineIndex)
        {
            if (_isClosed)
                throw new Exception("Cannot process line in cycle, it has been closed");
            _currentLine = lineIndex;

            if (line.Contains(_policy))
                _matchedPolicy.Add(_currentLine, line);

            _isClosed = _identationIndex == LineUtils.Identation(line);
            _isClosed &= line.Trim() != KEY_OPEN;

            if (_isClosed)
                _endLine = lineIndex;
        }

        public override bool Equals(object obj)
        {
            return this._startLine == ((CycleWithPolicy)obj)._startLine;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            throw new NotImplementedException();
            //return base.ToString();
        }

        #region ISortable Members

        public int Id
        {
            get { return _startLine; }
        }

        #endregion
    }
}
