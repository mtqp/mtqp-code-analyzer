using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using CodeAnalyzer.Enums;
using CodeAnalyzer.IO;
using System.IO;
using CodeAnalyzer.Analyzer;

namespace CodeAnalyzer
{
    public partial class frmFilterParameters : Form
    {
        private const string INCORRECT_PATH = "The path is not valid";
        private const string CORRECT_PATH = "The path is correct";
        private Color CORRECT_COLOR = Color.Black;
        private Color ERROR_COLOR = Color.Red;

        CodeFile _codeFile;

        public frmFilterParameters()
        {
            InitializeComponent();

            SetFileLoadingState(FileLoaderStateEnum.Undefined);
        }

        private void btnLoad_Click(object sender, EventArgs e)
        {
            if ((FileLoaderStateEnum)btnLoad.Tag != FileLoaderStateEnum.Loaded)
                ValidateBeginConditions();
            else
                AnalyseCode();
        }

        private void btnReset_Click(object sender, EventArgs e)
        {
            SetFileLoadingState(FileLoaderStateEnum.Undefined);
            if(_codeFile!=null)
                _codeFile.Close();
        }

        private void frmFilterParameters_FormClosed(object sender, FormClosedEventArgs e)
        {
            if (_codeFile != null)
                _codeFile.Close();
        }

        private void AnalyseCode()
        {
            LoadPolicies();
            _codeFile.Process();

            foreach (ICodeAnalyserPolicy policy in _codeFile.Policies)
            {
                /*
                 * AHORA SOLO EXISTE UNA POLITICA... POR ESO TIENE SENTIDO SETEAR EL .FILLDATA
                 * DENTRO DEL FOR
                 */

                analysisList.FillData(policy);
            }
        }

        private void LoadPolicies()
        {
            StringsWithinCodeBlocksPolicy stringSearchPolicy =  new StringsWithinCodeBlocksPolicy(txtMatch.Text);
            _codeFile.LoadPolicies(stringSearchPolicy);
        }

        private void ValidateBeginConditions()
        {
            IOOperationResultEnum ioResult;
            _codeFile = IOHandler.TryOpen(txtPath.Text, FileMode.Open, out ioResult);

            if (ioResult == IOOperationResultEnum.Success)
                SetFileLoadingState(FileLoaderStateEnum.Loaded);
            else
                SetFileLoadingState(FileLoaderStateEnum.Failed);
        }
        
        private void SetFileLoadingState(FileLoaderStateEnum fileLoaderStateEnum)
        {
            btnLoad.Tag = fileLoaderStateEnum;
            btnReset.Visible = fileLoaderStateEnum == FileLoaderStateEnum.Loaded;
            btnLoad.Text = fileLoaderStateEnum == FileLoaderStateEnum.Loaded ? "Analyze" : "Load";
            switch (fileLoaderStateEnum)
            { 
                case FileLoaderStateEnum.Loaded:
                    lblSearchPathSuccess.Text = CORRECT_PATH;
                    lblSearchPathSuccess.ForeColor = CORRECT_COLOR;
                    break;
                case FileLoaderStateEnum.Failed:
                    lblSearchPathSuccess.Text = INCORRECT_PATH;
                    lblSearchPathSuccess.ForeColor = ERROR_COLOR;
                    break;
                case FileLoaderStateEnum.Undefined:
                    lblSearchPathSuccess.Text = string.Empty;
                    break;
                default:
                    throw new Exception("Unhandled file loading state");
            }
        }
    }
}
