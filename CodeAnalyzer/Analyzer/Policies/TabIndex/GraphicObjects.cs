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
        private Point _location;
        private int? _tabIndex;

        public GraphicObject(string type, string name)
        {
            _type = type;
            _name = name;
            _tabIndex = new Nullable<int>();
        }

        public List<PropertyEnum> TrySetProperties(string line)
        {
            List<PropertyEnum> propertiesSet = new List<PropertyEnum>();

            if (TrySetProperty(line, PropertyEnum.Size))
                propertiesSet.Add(PropertyEnum.Size);
            if (TrySetProperty(line, PropertyEnum.XY))
                propertiesSet.Add(PropertyEnum.XY);
            if (TrySetProperty(line, PropertyEnum.TabIndex))
                propertiesSet.Add(PropertyEnum.TabIndex);

            return propertiesSet;
        }

        private bool TrySetProperty(string line, PropertyEnum property)
        {
            bool couldSetProperty = false;

            if (line.Contains(_name))
            {
                string propertyDescription = EnumDescription.GetDescription(property);
                couldSetProperty = line.Contains("." + propertyDescription);
            }

            if (couldSetProperty)
            {
                switch (property)
                {
                    case PropertyEnum.Size:
                        _size = GetSizeFromLine(line);
                        break;
                    case PropertyEnum.XY:
                        _location = GetLocationFromLine(line);
                        break;
                    case PropertyEnum.TabIndex:
                        _tabIndex = GetTabIndexFromLine(line);
                        break;
                    default:
                        throw new Exception("Try and set not implemented property");
                }
            }

            return couldSetProperty;
        }

        private int? GetTabIndexFromLine(string line)
        {
            throw new NotImplementedException();
        }

        private Point GetLocationFromLine(string line)
        {
            throw new NotImplementedException();
        }

        private Size GetSizeFromLine(string line)
        {
            throw new NotImplementedException();
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
            get { return _location; }
            set { _location = value; }
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
