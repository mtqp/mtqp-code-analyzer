using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace CodeAnalyzer.Analyzer
{
    public enum PropertyEnum
    {
        [Description("Size")]
        Size,
        [Description("Location")]
        XY,
        [Description("TabIndex")]
        TabIndex
    }
}
