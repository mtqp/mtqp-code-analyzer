using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace CodeAnalyzer
{
    /// <summary>
    /// Clase con un método para obtener la descripción de una enumeración (con el atributo Description).
    /// </summary>
    public class EnumDescription
    {
        /// <summary>
        /// Devuelve la descripción de una enumeración.
        /// </summary>
        /// <param name="val">El valor del Enum</param>
        /// <returns>La descripcion</returns>
        public static string GetDescription(Enum val)
        {
            DescriptionAttribute eda = (DescriptionAttribute)Attribute.GetCustomAttribute(val.GetType().GetField(val.ToString()), typeof(DescriptionAttribute));

            return eda == null ? String.Empty : eda.Description;
        }
    }
}
