using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace CodeAnalyzer
{
    public partial class AnalysisList : UserControl
    {
        private IAnalysisListParser _iAnalysisList;

        public AnalysisList()
        {
            InitializeComponent();
        }

        public void FillData(IAnalysisListParser iAnalisysList)
        {
            _iAnalysisList = iAnalisysList;
            lvwAnalysis.Clear();
            LoadColumnHeaders();
            LoadData();
        }

        private void LoadColumnHeaders()
        {
            List<AnalysisColumn> columns = _iAnalysisList.GetColumns();
            columns = Sort.IdiotSort<AnalysisColumn>(columns);

            foreach (AnalysisColumn column in columns)
            {
                ColumnHeader header = new ColumnHeader();
                //header.index = column.Order;
                header.AutoResize(ColumnHeaderAutoResizeStyle.ColumnContent);
                header.Text = column.ColumnName;
                header.Tag = column;
                lvwAnalysis.Columns.Add(header);
            }
        }

        private void LoadData()
        {
            List<AnalysisData> analysisDataList = _iAnalysisList.GetData();
            foreach (AnalysisData analysisData in analysisDataList)
            {
                ListViewItem lviData = new ListViewItem(analysisData.RetrieveDataColumn());
                lviData.ForeColor = analysisData.Color;
                while (!analysisData.AllDataRetrieved)
                {
                    lviData.SubItems.Add(analysisData.RetrieveDataColumn());
                }

                lvwAnalysis.Items.Add(lviData);
            }

            AutosizeColumns();
        }

        private void AutosizeColumns()
        {
            foreach (ColumnHeader header in lvwAnalysis.Columns)
                header.AutoResize(ColumnHeaderAutoResizeStyle.HeaderSize);
        }
    }
}
