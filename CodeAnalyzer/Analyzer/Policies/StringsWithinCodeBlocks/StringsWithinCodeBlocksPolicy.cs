using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CodeAnalyzer.Graph;
using CodeAnalyzer.Line;

namespace CodeAnalyzer.Analyzer
{
    public class StringsWithinCodeBlocksPolicy : ICodeAnalyserPolicy
    {
        /*
         * Primero la hacemos para q busque solo dentro de fors, whiles y foreaches
         * y de ahi vemos como hacemos para generalizarlo!
         */
        private string _policyMatch;
        private Graph<CycleWithPolicy> _graph;

        public StringsWithinCodeBlocksPolicy(string policyMatch)
        {
            _policyMatch = policyMatch;
            _graph = new Graph<CycleWithPolicy>();
        }

        public void AnalyseLine(string line, int lineIndex)
        {
            int identationIndex = LineUtils.Identation(line);

            if (CycleWithPolicy.StartsCycle(line))
            {
                CycleWithPolicy cycle = new CycleWithPolicy(lineIndex, identationIndex, _policyMatch);
                Node<CycleWithPolicy> actualNode = _graph.Iterator.ActualNode;
                bool isNestedCycle = _graph.Count >= 1 && actualNode != null && actualNode.Equals(cycle) && !actualNode.Data.IsClosed;

                _graph.AddNode(cycle);
                if (isNestedCycle) //Hay al menos dos nodos en el arbol!
                {
                    CycleWithPolicy father = _graph.Iterator.ActualNode.Data;
                    _graph.Relate(father, cycle);
                    _graph.Iterator.StepIntoChild(cycle);
                }
            }
            else 
            {
                bool iteringWithinCycle = _graph.Iterator.ActualNode != null;
                if (iteringWithinCycle)
                    _graph.Iterator.ActualNode.Data.ProcessLine(line, lineIndex);

                if (_graph.Iterator.ActualNode != null && _graph.Iterator.ActualNode.Data.IsClosed)
                    _graph.Iterator.RollbackToFather();
            }
        }

        #region IAnalysisListParser Members

        public List<AnalysisColumn> GetColumns()
        {
            List<AnalysisColumn> columns = new List<AnalysisColumn>();
            AnalysisColumn colIdentation = new AnalysisColumn("Identation", 0);
            AnalysisColumn colLine = new AnalysisColumn("Line",1);
            AnalysisColumn colMatchingLine = new AnalysisColumn("Description", 2);
            columns.Add(colIdentation);
            columns.Add(colLine);
            columns.Add(colMatchingLine);
            return columns;
        }

        public List<AnalysisData> GetData()
        {
            List<AnalysisData> analysisData = new List<AnalysisData>();

            List<CycleWithPolicy> cycles = _graph.GetNodesByDFS();
            foreach (CycleWithPolicy cycle in cycles)
            {
                if (cycle.LineMatchPolicy.Count > 0)
                {

                    List<string> startData = new List<string>() { cycle.IdentationIndex.ToString(), cycle.StartLine.ToString(), "cycle starts" };
                    analysisData.Add(new AnalysisData(startData));

                    foreach (KeyValuePair<int, string> match in cycle.LineMatchPolicy)
                    {
                        List<string> matchData = new List<string>() { string.Empty, match.Key.ToString(), match.Value };
                        analysisData.Add(new AnalysisData(matchData));
                    }

                    string endLine = cycle.EndLine.HasValue ? cycle.EndLine.Value.ToString() : "cycle hasn't ended yet";
                    List<string> endData = new List<string>() { cycle.IdentationIndex.ToString(), endLine, "cycle ends" };
                    analysisData.Add(new AnalysisData(endData));
                }
            }
            
            return analysisData;
        }

        #endregion
    }
}
