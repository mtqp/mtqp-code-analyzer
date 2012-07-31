using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;

namespace CodeAnalyzer
{
    public class AnalysisColumn : ISortable
    {
        private string _columnName;
        private int _order;
        
        public AnalysisColumn(string columnName, int order)
        {
            _columnName = columnName;
            _order = order;
        }

        public void SetOrder(int order)
        {
            _order = order;
        }

        public string ColumnName { get { return _columnName; } }
        public int Order { get { return _order; } }

        #region ISortable Members

        public int Id
        {
            get { return _order; }
        }

        #endregion
    }
}
