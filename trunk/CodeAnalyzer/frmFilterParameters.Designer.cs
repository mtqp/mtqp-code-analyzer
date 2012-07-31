using CodeAnalyzer.IO;
namespace CodeAnalyzer
{
    partial class frmFilterParameters
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.grpSearchParameters = new System.Windows.Forms.GroupBox();
            this.lblMatching = new System.Windows.Forms.Label();
            this.txtMatch = new System.Windows.Forms.TextBox();
            this.btnReset = new System.Windows.Forms.Button();
            this.btnLoad = new System.Windows.Forms.Button();
            this.lblSearchPathSuccess = new System.Windows.Forms.Label();
            this.lblExplanatory = new System.Windows.Forms.Label();
            this.txtPath = new System.Windows.Forms.TextBox();
            this.lblPath = new System.Windows.Forms.Label();
            this.grpResults = new System.Windows.Forms.GroupBox();
            this.tabPages = new System.Windows.Forms.TabControl();
            this.tabAnalysis = new System.Windows.Forms.TabPage();
            this.analysisList = new CodeAnalyzer.AnalysisList();
            this.grpSearchParameters.SuspendLayout();
            this.grpResults.SuspendLayout();
            this.tabPages.SuspendLayout();
            this.tabAnalysis.SuspendLayout();
            this.SuspendLayout();
            // 
            // grpSearchParameters
            // 
            this.grpSearchParameters.Controls.Add(this.lblMatching);
            this.grpSearchParameters.Controls.Add(this.txtMatch);
            this.grpSearchParameters.Controls.Add(this.btnReset);
            this.grpSearchParameters.Controls.Add(this.btnLoad);
            this.grpSearchParameters.Controls.Add(this.lblSearchPathSuccess);
            this.grpSearchParameters.Controls.Add(this.lblExplanatory);
            this.grpSearchParameters.Controls.Add(this.txtPath);
            this.grpSearchParameters.Controls.Add(this.lblPath);
            this.grpSearchParameters.Dock = System.Windows.Forms.DockStyle.Top;
            this.grpSearchParameters.Location = new System.Drawing.Point(0, 0);
            this.grpSearchParameters.Name = "grpSearchParameters";
            this.grpSearchParameters.Size = new System.Drawing.Size(678, 136);
            this.grpSearchParameters.TabIndex = 0;
            this.grpSearchParameters.TabStop = false;
            this.grpSearchParameters.Text = "Search parameters";
            // 
            // lblMatching
            // 
            this.lblMatching.AutoSize = true;
            this.lblMatching.Location = new System.Drawing.Point(6, 24);
            this.lblMatching.Name = "lblMatching";
            this.lblMatching.Size = new System.Drawing.Size(40, 13);
            this.lblMatching.TabIndex = 7;
            this.lblMatching.Text = "Match:";
            // 
            // txtMatch
            // 
            this.txtMatch.Location = new System.Drawing.Point(50, 21);
            this.txtMatch.Name = "txtMatch";
            this.txtMatch.Size = new System.Drawing.Size(520, 20);
            this.txtMatch.TabIndex = 6;
            // 
            // btnReset
            // 
            this.btnReset.Location = new System.Drawing.Point(591, 53);
            this.btnReset.Name = "btnReset";
            this.btnReset.Size = new System.Drawing.Size(75, 23);
            this.btnReset.TabIndex = 5;
            this.btnReset.Text = "Reset";
            this.btnReset.UseVisualStyleBackColor = true;
            this.btnReset.Click += new System.EventHandler(this.btnReset_Click);
            // 
            // btnLoad
            // 
            this.btnLoad.Location = new System.Drawing.Point(591, 24);
            this.btnLoad.Name = "btnLoad";
            this.btnLoad.Size = new System.Drawing.Size(75, 23);
            this.btnLoad.TabIndex = 4;
            this.btnLoad.Text = "Load";
            this.btnLoad.UseVisualStyleBackColor = true;
            this.btnLoad.Click += new System.EventHandler(this.btnLoad_Click);
            // 
            // lblSearchPathSuccess
            // 
            this.lblSearchPathSuccess.AutoSize = true;
            this.lblSearchPathSuccess.Location = new System.Drawing.Point(47, 82);
            this.lblSearchPathSuccess.Name = "lblSearchPathSuccess";
            this.lblSearchPathSuccess.Size = new System.Drawing.Size(107, 13);
            this.lblSearchPathSuccess.TabIndex = 3;
            this.lblSearchPathSuccess.Text = "Search path success";
            // 
            // lblExplanatory
            // 
            this.lblExplanatory.AutoSize = true;
            this.lblExplanatory.Location = new System.Drawing.Point(47, 109);
            this.lblExplanatory.Name = "lblExplanatory";
            this.lblExplanatory.Size = new System.Drawing.Size(387, 13);
            this.lblExplanatory.TabIndex = 2;
            this.lblExplanatory.Text = "Actualmente, sólo busca aparición de texto dentro de ciclos para un sólo archivo";
            // 
            // txtPath
            // 
            this.txtPath.Location = new System.Drawing.Point(50, 49);
            this.txtPath.Name = "txtPath";
            this.txtPath.Size = new System.Drawing.Size(520, 20);
            this.txtPath.TabIndex = 1;
            // 
            // lblPath
            // 
            this.lblPath.AutoSize = true;
            this.lblPath.Location = new System.Drawing.Point(12, 52);
            this.lblPath.Name = "lblPath";
            this.lblPath.Size = new System.Drawing.Size(32, 13);
            this.lblPath.TabIndex = 0;
            this.lblPath.Text = "Path:";
            // 
            // grpResults
            // 
            this.grpResults.Controls.Add(this.tabPages);
            this.grpResults.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.grpResults.Location = new System.Drawing.Point(0, 136);
            this.grpResults.Name = "grpResults";
            this.grpResults.Size = new System.Drawing.Size(678, 372);
            this.grpResults.TabIndex = 1;
            this.grpResults.TabStop = false;
            this.grpResults.Text = "Results";
            // 
            // tabPages
            // 
            this.tabPages.Controls.Add(this.tabAnalysis);
            this.tabPages.Location = new System.Drawing.Point(6, 19);
            this.tabPages.Name = "tabPages";
            this.tabPages.SelectedIndex = 0;
            this.tabPages.Size = new System.Drawing.Size(666, 347);
            this.tabPages.TabIndex = 0;
            // 
            // tabAnalysis
            // 
            this.tabAnalysis.Controls.Add(this.analysisList);
            this.tabAnalysis.Location = new System.Drawing.Point(4, 22);
            this.tabAnalysis.Name = "tabAnalysis";
            this.tabAnalysis.Padding = new System.Windows.Forms.Padding(3);
            this.tabAnalysis.Size = new System.Drawing.Size(658, 321);
            this.tabAnalysis.TabIndex = 0;
            this.tabAnalysis.Text = "Analysis result";
            this.tabAnalysis.UseVisualStyleBackColor = true;
            // 
            // analysisList
            // 
            this.analysisList.Dock = System.Windows.Forms.DockStyle.Fill;
            this.analysisList.Location = new System.Drawing.Point(3, 3);
            this.analysisList.Name = "analysisList";
            this.analysisList.Size = new System.Drawing.Size(652, 315);
            this.analysisList.TabIndex = 0;
            // 
            // frmFilterParameters
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(678, 508);
            this.Controls.Add(this.grpResults);
            this.Controls.Add(this.grpSearchParameters);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
            this.MaximizeBox = false;
            this.Name = "frmFilterParameters";
            this.Text = "CodeAnalyser V0.1 (Beta)";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.frmFilterParameters_FormClosed);
            this.grpSearchParameters.ResumeLayout(false);
            this.grpSearchParameters.PerformLayout();
            this.grpResults.ResumeLayout(false);
            this.tabPages.ResumeLayout(false);
            this.tabAnalysis.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private AnalysisList analysisList;
        private System.Windows.Forms.GroupBox grpSearchParameters;
        private System.Windows.Forms.GroupBox grpResults;
        private System.Windows.Forms.Label lblPath;
        private System.Windows.Forms.TextBox txtPath;
        private System.Windows.Forms.Label lblExplanatory;
        private System.Windows.Forms.Label lblSearchPathSuccess;
        private System.Windows.Forms.Button btnLoad;
        private System.Windows.Forms.Button btnReset;
        private System.Windows.Forms.TextBox txtMatch;
        private System.Windows.Forms.Label lblMatching;
        private System.Windows.Forms.TabControl tabPages;
        private System.Windows.Forms.TabPage tabAnalysis;
    }
}

