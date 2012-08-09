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
        IOHandler _ioHandler;
        List<CodeFile> _codeFiles;

        public frmFilterParameters()
        {
            InitializeComponent();
            _codeFiles = new List<CodeFile>();
            _ioHandler = new IOHandler();
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
            Reset();
        }

        private void Reset()
        {
            SetFileLoadingState(FileLoaderStateEnum.Undefined);
            CloseAllFiles();
            tabPages.TabPages.Clear();
        }

        private void CloseAllFiles()
        {
            if (_codeFiles != null)
                foreach (CodeFile codeFile in _codeFiles)
                    codeFile.Close();
        }

        private void frmFilterParameters_FormClosed(object sender, FormClosedEventArgs e)
        {
            CloseAllFiles();
        }

        private void AnalyseCode()
        {
            List<string> csFiles = new List<string>();
            if (chkRecursively.Checked)
                csFiles = _ioHandler.GetAllFiles(txtPath.Text);
            else
                csFiles.Add(txtPath.Text);

            foreach (string stringFile in csFiles)
            {
                IOOperationResultEnum ioResult;
                CodeFile codeFile = IOHandler.TryOpen(stringFile, FileMode.Open, out ioResult);

                if (ioResult == IOOperationResultEnum.Success)
                {
                    _codeFiles.Add(codeFile);
                    AnalyseCodeFile(codeFile);
                }
                else
                    Reset();
            }
        }

        private void AnalyseCodeFile(CodeFile codeFile)
        {
            LoadPolicies(codeFile);
            codeFile.Process();

            foreach (ICodeAnalyserPolicy policy in codeFile.Policies)
            {
                if (policy.GetData().Count > 0)
                {
                    AnalysisList list = new AnalysisList();
                    list.FillData(policy);
                    string title = codeFile.Name + " - " + policy.Name;
                    ShowAnalysisList(list, title);
                }
            }
        }

        private void ShowAnalysisList(AnalysisList analysisList, string title)
        {
            analysisList.Dock = DockStyle.Fill;

            TabPage page = new TabPage(title);
            page.Controls.Add(analysisList);
            tabPages.TabPages.Add(page);
        }
        private void LoadPolicies(CodeFile codeFile)
        {
            //StringsWithinCodeBlocksPolicy stringSearchPolicy =  new StringsWithinCodeBlocksPolicy(txtMatch.Text);
            //_codeFile.LoadPolicies(stringSearchPolicy);

            RecursivityPolicy recursivityPolicy = new RecursivityPolicy();
            codeFile.LoadPolicies(recursivityPolicy);
        }

        private void ValidateBeginConditions()
        {
            if (!chkRecursively.Checked)
            {
                IOOperationResultEnum ioResult;
                CodeFile codeFile = IOHandler.TryOpen(txtPath.Text, FileMode.Open, out ioResult);

                if (ioResult == IOOperationResultEnum.Success)
                {
                    SetFileLoadingState(FileLoaderStateEnum.Loaded);
                    codeFile.Close();
                }
                else
                    SetFileLoadingState(FileLoaderStateEnum.Failed);
            }
            else
            {
                SetFileLoadingState(FileLoaderStateEnum.Loaded);
            }

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
