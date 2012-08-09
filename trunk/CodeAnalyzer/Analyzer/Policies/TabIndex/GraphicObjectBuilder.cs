using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Analyzer.Policies
{
    public class GraphicObjectBuilder
    {
        public static GraphicObject CreateInstance(string line)
        {
            if(!LineCreatesInstance(line))
                throw new Exception("Cannot create instance");

            string type = GetObjectTypeFromLine(line);
            string name = GetObjectNameFromLine(line);

            return new GraphicObject(type, name);
        }

        public static string GetObjectNameFromLine(string line)
        {
            // -------------------------------------------
            //PRESUPONE QUE ESTA CORRECTAMENTE FORMATEADO!
            // -------------------------------------------
            int positionEqual = line.IndexOf("=");
            string substring = line.Substring(0, positionEqual).Trim();

            string[] leftElements = substring.Split(' ');
            string objectName = leftElements[leftElements.Length - 1];

            return objectName;
        }

        public static string GetObjectTypeFromLine(string line)
        {
            const int OFFSET_NEW = 4;

            int positionNew = line.IndexOf(" new ");
            string substring = line.Trim().Substring(positionNew + OFFSET_NEW, line.Length + positionNew);

            // --> busca el minimo indice entre object<otherObject>() y object();
            // --> por si se utiliza generics
            int endIndex = Math.Min(substring.IndexOf("<"), substring.IndexOf("("));
            
            string objectType = substring.Substring(0, endIndex);
            return objectType;
        }

        internal static bool LineCreatesInstance(string line)
        {
            return line.Contains(" new ");
        }
    }
}
