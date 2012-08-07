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
        private int? _tabIndex;

        public GraphicObject(string type, string name)
        {
            _type = type;
            _name = name;
            _tabIndex = new Nullable<int>();
        }

        public void TrySetProperty(string line)
        {


            //GraphicPropertyEnum.Size


            //TrySetSize(line);
            //TrySetPoint(line);
            //TrySetTabIndex(line);
        }

        public string Type
        {
            get { return _type; }
        }

        public string Name
        {
            get { return _name; }
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

        public int? TabIndex
        {
            get { return _tabIndex; }
            set { _tabIndex = value; }
        }

        public override bool Equals(object obj)
        {
            GraphicObject graphicObj = (GraphicObject)obj;

            return this._type == graphicObj._type && this._name == graphicObj._name;
        }
    }
}
