using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Configuracion;
using enfoke.Eges.Persistence;
using System.Linq;
using enfoke.Data.DisconnectedSupport;
using enfoke.Data;
using enfoke.Data.Reference;
using NHibernate;
using enfoke.Data.Filters;
using enfoke.Utils;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Utils;
using enfoke.AOP;
using enfoke.Eges.Persistance;
using enfoke.UI.Settings;

namespace enfoke.Eges.Data
{
    public class ConfigurationDalc : Dalc, IService
    {
        protected ConfigurationDalc(NotConstructable dummy) : base(dummy) { }

        #region Configuracion

        public EntityCollection<UsuarioConfiguracion> CargarConfiguracionUsuarioCliente()
        {
            EntityCollection<UsuarioConfiguracion> confs = new EntityCollection<UsuarioConfiguracion>();
            // Agrega el filtro por pc y usuario
            string nombrePc = enfoke.Connector.Context.ClientMachineName;
            Filter filter = new Filter();
            filter.Add(new OpenParenthesis());
            AgregaFiltroPorUsuarioYPc(filter, nombrePc);
            filter.Add(BooleanOp.And, UsuarioConfiguracion.Properties.EsServidor, "=", false);
            filter.Add(new CloseParenthesis());
            // Agrega el filtro por usuario y pc nula
            filter.Add(new OpenParenthesis(BooleanOp.Or));
            AgregaFiltroPorUsuarioYPc(filter, null);
            filter.Add(BooleanOp.And, UsuarioConfiguracion.Properties.EsServidor, "=", false);
            filter.Add(new CloseParenthesis());
            // Hace la consulta
            return dalEngine.GetManyByFilter<UsuarioConfiguracion>(filter);
        }

        public UsuarioConfiguracion UsuarioConfiguracionReadByUsuarioAndClave(int userId, string key)
        {
            Filter filter = new Filter();
            filter.Add(UsuarioConfiguracion.Properties.UsuarioId, "=", userId);
            filter.Add(BooleanOp.And, UsuarioConfiguracion.Properties.Nombre, "=", key.Trim());
            return dalEngine.GetByFilter<UsuarioConfiguracion>(filter);
        }

        private static void AgregaFiltroPorUsuarioYPc(Filter filter, string nombrePc)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            filter.Add(UsuarioConfiguracion.Properties.UsuarioId, "=", user.Id);
            filter.Add(BooleanOp.And, UsuarioConfiguracion.Properties.NombrePc, (String.IsNullOrEmpty(nombrePc) ? "IS" : "="), nombrePc);
        }

        [RequiresTransaction]
        protected virtual void GuardarConfiguracionUsuario(string nombrePc, UsuarioConfiguracion configuracion)
        {
            // Borra items con igual key
            SecurityUser user = Security.Current.UserInfo.User;
            if (configuracion.Id == 0)
            {
                // Si es un item nuevo, toma la precaucion de borrar existentes
                Filter filter = new Filter();
                AgregaFiltroPorUsuarioYPc(filter, nombrePc);
                filter.Add(BooleanOp.And, UsuarioConfiguracion.Properties.Nombre, "=", configuracion.Nombre);
                EntityCollection<UsuarioConfiguracion> existentes = dalEngine.GetManyByFilter<UsuarioConfiguracion>(filter);
                foreach (UsuarioConfiguracion existente in existentes)
                    dalEngine.Delete(existente);
            }
            // Se asegura que tenga la pc y el usuario bien seteados
            configuracion.UsuarioId = user.Id;
            configuracion.NombrePc = nombrePc;
            // Graba (el existente, o el nuevo)
            configuracion = dalEngine.Update(configuracion);
        }

        [RequiresTransaction]
        public virtual void GuardarConfiguracionUsuario(UsuarioConfiguracion userConf)
        {
            GuardarConfiguracionUsuario(null, userConf);
        }
        [RequiresTransaction]
        public virtual void GuardarConfiguracionUsuario(Dictionary<string, string> datosConfiguracion)
        {
            string nombrePc = enfoke.Connector.Context.ClientMachineName;
            GuardarConfiguracionUsuarioPorDiccionario(datosConfiguracion, nombrePc);
        }
        [RequiresTransaction]
        public virtual void GuardarConfiguracionGlobalUsuario(Dictionary<string, string> datosConfiguracion)
        {
            string nombrePc = null;
            GuardarConfiguracionUsuarioPorDiccionario(datosConfiguracion, nombrePc);
        }

        private void GuardarConfiguracionUsuarioPorDiccionario(Dictionary<string, string> datosConfiguracion, string nombrePc)
        {
            foreach (KeyValuePair<string, string> kvp in datosConfiguracion)
            {
                UsuarioConfiguracion configuracion = new UsuarioConfiguracion();
                configuracion.Nombre = kvp.Key;
                configuracion.Valor = kvp.Value;
                GuardarConfiguracionUsuario(nombrePc, configuracion);
            }
        }

        [Private]
        public Configuracion CargarConfiguracion()
        {
            Configuracion ret = new Configuracion();

            // [JR] [19/08/2008]
            // Particularmente para una cuestion de la pantalla de Detalle del Turno debo saber si la empresa es CIMED.
            // Claramente esta logica no deberia reproducirse, pero es una solución puntual para una situación particular.
            ret.Aplicacion.EmpresaImplementacion = GetEmpresa();

            // Obtengo todos los Parametros
           EntityCollection<IParametro> parametros = IParametroReadAllParametro();

            // Cargo la Configuracion
            RecursiveLoad(ret, parametros);

            ret.Aplicacion.LogoFondo = CargaImagen(ret, "FONDO.jpg");
            ret.Reportes.Logo = CargaImagen(ret, "REPORTES.jpg");

            CargaLogoImpresionMonocromo(ret);

            return ret;
        }

        
        private void CargaLogoImpresionMonocromo(Configuracion ret)
        {
            ret.Reportes.LogoMonocromo = CargaImagen(ret, "REPORTES_MONOCROMO.bmp", true);
        }
        private DatosPlantilla CargaImagen(Configuracion config, string imagen)
        {
            return CargaImagen(config, imagen, false);
        }
        private DatosPlantilla CargaImagen(Configuracion config, string imagen, bool doNotThrowException)
        {
            string empresa_entorno = config.Aplicacion.EmpresaImplementacion.Trim() + "_" + config.Aplicacion.Entorno.Trim();
            string imagenes = config.Paths.Imagenes;
            string logoFondo = System.IO.Path.Combine(imagenes, empresa_entorno + "_" + imagen);

            if (enfoke.IO.File.Exists(logoFondo))
                return new DatosPlantilla(enfoke.IO.File.ReadAllBytes(logoFondo));
            else
                if (doNotThrowException)
                    return null;
                else
                    throw new System.IO.FileNotFoundException("Archivo no encontrado", logoFondo);
        }

        private string GetEmpresa()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT par_valor ");
            sb.Append("FROM parametro ");
            sb.Append("WHERE par_empresa IS NULL ");
            sb.Append("AND par_entorno IS NULL ");
            sb.Append("AND par_nombre = 'EMPRESA'");

            return (string)dalEngine.Connection.ExecuteScalar(sb.ToString());
        }

        public void TestReadErr()
        {
            const int idMAX = 4893;
            IQuery q = enfoke.Context.Data.Session.CreateQuery("from ObraSocial o where 1 >= 1 / (" + idMAX + " - o.Id )");
            q.SetMaxResults(50);
            IList<ObraSocial> lista = q.List<ObraSocial>();
            int id = lista[lista.Count - 1].Id;
            int i = id + 1;
        }

        [Private]
        public void RecursiveLoad(object ret, EntityCollection<IParametro> parametros)
        {
            // Va recursivamente cargándo los valores
            if (ret == null)
                return;

            // Primero los Fields
            LoadFieldParametersToObjectAndRecurse(ret, parametros);

            // Después las Properties
            LoadPropertyParametersToObjectAndRecurse(ret, parametros);
        }

        private bool ResolveValue(MapeoConfiguracionAttribute attribute, EntityCollection<IParametro> parametros, Type elementType, out object value)
        {
            if (attribute.IsTableMapping)
            {
                IEntityCollection collection = dalEngine.GetAll(attribute.TableType);
                if (attribute.Operation == Operation.None)
                    // Es una tabla entera
                    value = collection;
                else if (attribute.Operation == Operation.MoreThanOne)
                    value = (collection.Count > 1);
                else
                    throw new InvalidOperationException("Operación inválida de mapeo.");
            }
            else
            {
                // Tiene algo para cargar...
                object dbvalue = ParametroUtils.ReadValorByNombre(parametros, attribute.DatabaseTag);
                if (dbvalue != null)
                    value = ParseValor(elementType, dbvalue);
                else if (attribute.DefaultValueDefined)
                {
                    value = elementType == typeof(TimeSpan) ? TimeSpan.FromSeconds(Convert.ToDouble(attribute.DefaultValue.ToString())) : attribute.DefaultValue;
                }
                else
                {
                    value = null;
                    return false;
                }
            }
            return true;
        }

        private void LoadPropertyParametersToObjectAndRecurse(object ret, EntityCollection<IParametro> parametros)
        {
            Type type = ret.GetType();
            foreach (PropertyInfo property in type.GetProperties())
            {
                object[] attributes = property.GetCustomAttributes(typeof(MapeoConfiguracionAttribute), false);
                if (attributes.Length > 0)
                {
                    MapeoConfiguracionAttribute attribute = (MapeoConfiguracionAttribute)attributes[0];
                    object value;
                    if (ResolveValue(attribute, parametros, property.PropertyType, out value))
                        property.SetValue(ret, value, null);
                }
                else
                {
                    // Se fija si lo recorre...
                    object[] skipAttributes = property.GetCustomAttributes(typeof(SinMapeoConfiguracionAttribute), false);
                    if (skipAttributes.Length == 0 && !property.PropertyType.IsValueType)
                        RecursiveLoad(property.GetValue(ret, null), parametros);
                }
            }
        }

        private void LoadFieldParametersToObjectAndRecurse(object ret, EntityCollection<IParametro> parametros)
        {
            Type type = ret.GetType();
            foreach (FieldInfo field in type.GetFields())
            {
                object[] attributes = field.GetCustomAttributes(typeof(MapeoConfiguracionAttribute), false);
                if (attributes.Length > 0)
                {
                    MapeoConfiguracionAttribute attribute = (MapeoConfiguracionAttribute)attributes[0];
                    object value;
                    if (ResolveValue(attribute, parametros, field.FieldType, out value))
                    {
                        // Si son de diferentes types, trata de castearlo
                        if (value != null && value.GetType() != field.FieldType)
                            value = Convert.ChangeType(value, field.FieldType);
                        field.SetValue(ret, value);
                    }
                }
                else
                {
                    // Se fija si lo recorre...
                    if (!field.FieldType.IsValueType)
                        RecursiveLoad(field.GetValue(ret), parametros);
                }
            }
        }

        private static object ParseValor(Type tipo, object valor)
        {
            try
            {
                if (tipo == typeof(bool))
                    return valor.ToString().Trim() == "1" || valor.ToString().Trim().ToLower() == "true";
                if (tipo == typeof(int))
                    return int.Parse(valor.ToString());
                if (tipo == typeof(string[]))
                    return valor.ToString().Split('|');
                if (tipo == typeof(TimeSpan))
                {
                    string[] timeData = valor.ToString().Split(':');
                    return new TimeSpan(Convert.ToInt32(timeData[0]), Convert.ToInt32(timeData[1]), Convert.ToInt32(timeData[2]));
                }
                if (tipo == typeof(DateTime))
                    return DateTime.ParseExact(valor.ToString(), "ddMMyyyy", System.Globalization.CultureInfo.InvariantCulture);
                if (tipo == typeof(string))
                    return valor.ToString();
                // Si o si divide el punto
                if (tipo == typeof(decimal))
                    return decimal.Parse((string)valor, System.Globalization.CultureInfo.InvariantCulture);
                
                return valor;
            }
            catch (Exception ex)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Error al Convertir el Valor al Tipo de Dato del Parámetro.");
                sb.Append("Exception: ").AppendLine(ex.Message);
                sb.Append("[(").Append(tipo.ToString()).Append(")").Append(valor.ToString()).Append("]").AppendLine();
                sb.Append("Por favor revise la configuración.");

                throw new Exception(sb.ToString());
            }
        }
        #endregion

        #region Parametro
        private EntityCollection<ParametroView> ParametroReadAll()
        {
            ReadManyCommand<ParametroView> readCmd = new ReadManyCommand<ParametroView>(dalEngine);

            EntityCollection<ParametroView> parametros = readCmd.Execute();

            // Ordeno por Nombre
            parametros.Sort(new Comparison<ParametroView>(delegate(ParametroView left, ParametroView right)
            {
                return left.Nombre.CompareTo(right.Nombre);
            }));

            return parametros;
        }

        private EntityCollection<IParametro> IParametroReadAllParametro()
        {
            EntityCollection<ParametroView> parametros = ParametroReadAll();
            EntityCollection<IParametro> result = new EntityCollection<IParametro>();

            foreach (ParametroView parametro in parametros)
                result.Add(parametro);

            return result;
        }

        [Private]
        public ParametroView ParametroReadByNombre(string nombre)
        {
            EntityCollection<ParametroView> paramtros = dalEngine.GetManyByProperty<ParametroView>(ParametroView.Properties.Nombre, nombre);

            if (paramtros.Count > 0)
                return paramtros[0];
            return null;
        }

        [Private]
        public void ParametroUpdate(ParametroUpdate parametro)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            // Seteo fecha y usuario de Actualizacion
            parametro.ActualizacionFecha = enfoke.Time.Now;
            parametro.ActualizacionUsuario = user.Id;


            dalEngine.Update<ParametroUpdate>(parametro);
        }

        public EntityCollection<Parametro> ParametroReadByParameters(bool soloVisibles)
        {
            var parametros = from parametro in dalEngine.Query<Parametro>() where (!soloVisibles || parametro.Visible) select parametro;
            return parametros.ToEntityCollection();
        }

        #endregion

        #region SubRed




[Private]
        #endregion

        #region ColorIndicador
        public ReadAllCollection<ColorIndicador> ColorIndicadorReadAll()
        {
            EntityCollection<ColorIndicador> datos = dalEngine.GetAll<ColorIndicador>(new[] { ColorIndicador.Properties.Tag, ColorIndicador.Properties.Minimo },
                                                                                      new[] { SortOrder.Ascending, SortOrder.Ascending });
            return new ReadAllCollection<ColorIndicador>(datos);
        }
        #endregion

        #region Benchmark

        
        [RequiresTransaction]
        public virtual void BenchmarkDelete(Benchmark b)
        {
            EntityCollection<BenchmarkItem> items = dalEngine.GetManyByProperty<BenchmarkItem>(BenchmarkItem.Properties.Benchmark.Id, b.Id);
            dalEngine.Delete(items);
            dalEngine.Delete(b);
        }
        [RequiresTransaction]
        public virtual void BenchmarkUpdate(Benchmark b, List<BenchmarkItem> items)
        {
            dalEngine.Update(b);
            foreach (BenchmarkItem listItem in items)
                listItem.Benchmark = b;
            dalEngine.UpdateCollection(new EntityCollection<BenchmarkItem>(items));
        }

        #endregion
    }
}

