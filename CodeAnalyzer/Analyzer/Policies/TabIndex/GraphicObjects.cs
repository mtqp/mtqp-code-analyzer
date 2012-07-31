using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;

namespace CodeAnalyzer.Analyzer.Policies
{
    public class GraphicObject
    {
        private string _type;
        private string _name;
        private Size _size;
        private Point _initPoint;

        public GraphicObject(string type, string name)
        {
            _type = type;
            _name = name;
        }

        public Size Size
        {
            get { return _size; }
            set { _size = value; }
        }

        public Point InitPoint
        {
            get { return _initPoint; }
            set { _initPoint = value; }
        }

        public override bool Equals(object obj)
        {
            GraphicObject graphicObj = (GraphicObject)obj;

            return this._type == graphicObj._type && this._name == graphicObj._name;
        }
    }
}
