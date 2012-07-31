using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;

namespace CodeAnalyzer
{
    public class AnalysisData
    {
        private int _index = 0;
        private List<string> _data;
        private Color _color = Color.Black;

        public AnalysisData(List<string> orderedDataColumn) 
        {
            if (orderedDataColumn != null)
                _data = orderedDataColumn;
            else
                _data = new List<string>();
        }

        public AnalysisData(List<string> orderedDataColumn, Color color)
            :this(orderedDataColumn)
        {
            _color = color;
        }

        public Color Color
        {
            get { return _color; }
            set { _color = value; }
        }

        public bool AllDataRetrieved
        {
            get { return _index == _data.Count; }
        }

        public string RetrieveDataColumn()
        {
            if (_index < _data.Count)
            {
                string dataColumn = _data[_index];
                _index++;
                return dataColumn;
            }
            else
                throw new Exception("Retrieving data that is out of bounds");
        }


    }
}
