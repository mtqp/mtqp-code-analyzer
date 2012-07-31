namespace CodeAnalyzer
{
    partial class AnalysisList
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.lvwAnalysis = new System.Windows.Forms.ListView();
            this.SuspendLayout();
            // 
            // lvwAnalysis
            // 
            this.lvwAnalysis.Alignment = System.Windows.Forms.ListViewAlignment.Left;
            this.lvwAnalysis.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lvwAnalysis.HideSelection = false;
            this.lvwAnalysis.Location = new System.Drawing.Point(0, 0);
            this.lvwAnalysis.Name = "lvwAnalysis";
            this.lvwAnalysis.Size = new System.Drawing.Size(581, 261);
            this.lvwAnalysis.TabIndex = 0;
            this.lvwAnalysis.UseCompatibleStateImageBehavior = false;
            this.lvwAnalysis.View = System.Windows.Forms.View.Details;
            // 
            // AnalysisList
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.lvwAnalysis);
            this.Name = "AnalysisList";
            this.Size = new System.Drawing.Size(581, 261);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.ListView lvwAnalysis;
    }
}
