using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Analyzer.Policies
{
    public class TabIndexPolicy : ICodeAnalyserPolicy
    {
        List<GraphicObject> _graphicObjects;

        public TabIndexPolicy()
        {
            _graphicObjects = new List<GraphicObject>();
        }

        public void AnalyseLine(string line, int lineIndex)
        {
            /// ------------------------------------------------------
            /// Supone inicialmente que es un designer normal de .NET, 
            /// donde todos los news los crea al principio
            /// ------------------------------------------------------

            if (GraphicObjectBuilder.LineCreatesInstance(line))
            {
                GraphicObject graphic = GraphicObjectBuilder.CreateInstance(line);
                _graphicObjects.Add(graphic);
            }
            else
            {
                string objectName = GraphicObjectBuilder.GetObjectNameFromLine(line);
                GraphicObject graphic = GetGraphicObject(objectName);

                if (graphic != null)
                    graphic.TrySetProperty(line);
            }
        }

        private GraphicObject GetGraphicObject(string objectName)
        {
            foreach (GraphicObject graphic in _graphicObjects)
                if (graphic.Name == objectName)
                    return graphic;

            return null;
        }
        
        public List<AnalysisColumn> GetColumns()
        {
            throw new NotImplementedException();
        }

        public List<AnalysisData> GetData()
        {
            throw new NotImplementedException();
        }
    }
}
