using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using enfoke.Eges;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;

using enfoke.Eges.Persistence.DAL;
using System.Linq;
using enfoke.Eges.Reserva;
using enfoke.Eges.Security;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Data.DisconnectedSupport;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.AOP;
using enfoke.Eges.Persistance;
using enfoke.Data.Reference;

namespace enfoke.Eges.Data
{
    public class ServiciosDalc : Dalc, IService
    {
        protected ServiciosDalc(NotConstructable dummy) : base(dummy) { }

        #region Autorizaciones

        [Private]
        public string[] GetMailsByAutorizacionGrupo(int grupoID, int centroID)
        {
            // Trae los usuario-grupo
            EntityCollection<AutorizacionGrupoUsuario> col = new EntityCollection<AutorizacionGrupoUsuario>();

            ReadManyCommand<AutorizacionGrupoUsuario> readCmd = new ReadManyCommand<AutorizacionGrupoUsuario>(dalEngine);
            readCmd.Filter = new Filter();
            readCmd.Filter.Add(AutorizacionGrupoUsuario.Properties.Grupo, "=", grupoID);
            readCmd.Filter.Add(BooleanOp.And, AutorizacionGrupoUsuario.Properties.Centro, "=", centroID);
            col = readCmd.Execute();

            List<string> temp = new List<string>();
            foreach (AutorizacionGrupoUsuario a in col)
            {
                if (!temp.Contains(a.Usuario.Mail))
                    temp.Add(a.Usuario.Mail);
            }

            return temp.ToArray();
        }






        [Private]
        public Autorizacion AutorizacionUpdate(Autorizacion aut)
        {
            // Logueo todos los evento de autorización del turno
            AutorizacionLogTurno(aut);

            return dalEngine.Update<Autorizacion>(aut);
        }

        public void AutorizacionLogTurno(Autorizacion aut)
        {
            if (aut.Estado.Id == (int)AutorizacionEstadoEnum.Autorizado)
            {
                TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
                String log = String.Empty;

                if (aut.Evento.Id == (int)AutorizacionEventoEnum.Sobreturno)
                    log = "Autorización de sobreturno por el usuario " + aut.Autorizante.Name;
                else if (aut.Evento.Id == (int)AutorizacionEventoEnum.DebeOrden)
                    log = "Autorización de debe orden por el usuario " + aut.Autorizante.Name;
                else if (aut.Evento.Id == (int)AutorizacionEventoEnum.ModificacionValorizacion)
                    log = "Autorización de modificación de valorización por el usuario " + aut.Autorizante.Name;
                else if (aut.Evento.Id == (int)AutorizacionEventoEnum.DerivanteNoHabilitado)
                    log = "Autorización de médico derivante no habilitado por el usuario " + aut.Autorizante.Name;

                if (aut.Turno != null)
                    TurnosDalc.LogRegistrar((int)LogEventoEnum.AutorizacionTurnos, log, aut.Turno.Id);
            }
        }

        public EntityCollection<Autorizacion> AutorizacionReadByUsuarioDelGrupoDelEvento(SecurityUser user)
        {
            EntityCollection<AutorizacionView> col =
            dalEngine.GetManyByProperty<AutorizacionView>
                (AutorizacionView.Properties.UsuarioDelGrupoDelEvento, user.Id);

            EntityCollection<Autorizacion> ret = new EntityCollection<Autorizacion>();
            foreach (AutorizacionView view in col)
            {
                Autorizacion auth = new Autorizacion(view);
                ret.Add(auth);
            }

            return ret;
        }

        [RequiresTransaction]
        protected internal virtual void AutorizacionGrupoUsuarioUpdate(SecurityUser user, EntityCollection<AutorizacionGrupoUsuario> gruposAutorizacionesUsuario)
        {
            // Elimino los existentes que pertenecen al usuario.
            EntityCollection<AutorizacionGrupoUsuario> existentes = AutorizacionGrupoUsuarioReadByUsuario(user.Id);
            dalEngine.Delete(existentes);


            // Inserto los nuevos y los viejos (a los viejos les volví el id a 0
            dalEngine.UpdateCollection<AutorizacionGrupoUsuario>(gruposAutorizacionesUsuario);
        }

        /// <summary>
        /// Obtengo los AutorizacionGrupoUsuario de un Usuario
        /// </summary>
        /// <param name="userID">Id del Usuario en cuestion</param>
        /// <returns>Listado de AutorizacionGrupoUsuario del Usuario</returns>
        public EntityCollection<AutorizacionGrupoUsuario> AutorizacionGrupoUsuarioReadByUsuario(int userID)
        {
            return dalEngine.GetManyByProperty<AutorizacionGrupoUsuario>
                (AutorizacionGrupoUsuario.Properties.Usuario, userID);
        }









        #endregion

        #region PlantillaInformeView
        /// <summary>
        /// Obtiene los datos de las Plantillas asociadas a las practicas de un Turno
        /// </summary>
        /// <param name="practicasId">ID del Turno</param>
        /// <returns>Las planillas</returns>
        public EntityCollection<PlantillaInformeView> PlantillaInformeReadByTurno(int turnoID)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Obtengo las Prácticas del Turno
            EntityCollection<PracticaTurno> PTs = TurnosDalc.PracticaTurnoReadByTurno(turnoID, PracticaTurnoTipoEnum.Todas);
            List<int> practicas = new List<int>();
            for (int i = 0; i < PTs.Count; i++)
                practicas.Add(PTs[i].Practica.Id);

            // Obtengo todas las Plantillas
            Filter filter = new Filter();
            filter.Add(PlantillaInformeView.Properties.PracticaId, "IN", practicas.ToArray());
            Sort sort = new Sort();
            sort.Add(PlantillaInformeView.Properties.Practica);
            // Listo
            return dalEngine.GetManyByFilter<PlantillaInformeView>(filter, sort);
        }
        public PlantillasPracticas PlantillaPracticasReadByTurnoInforme(TurnoInforme turnoInforme)
        {
            EntityCollection<Practica> PTs = ObtenerPracticasDeTurnoInforme(turnoInforme);
            PlantillasPracticas ret = new PlantillasPracticas();
            ret.Practicas = PTs.GetIds();
            ret.Plantillas = PlantillaInformeReadByTurnoAndMedico(turnoInforme, PTs);
            return ret;
        }
        private EntityCollection<PlantillaInformeView> PlantillaInformeReadByTurnoAndMedico(TurnoInforme turnoInforme, EntityCollection<Practica> PTs)
        {
            int medicoId = turnoInforme.Informante.Id;
            string hql = "SELECT DISTINCT pli FROM PlantillaInformeView pli " +
                         "WHERE pli.PracticaId IN (:practicasId) " +
                         "AND (pli.MedicoId  = :medicoId OR pli.MedicoId IS NULL) " +
                         "AND pli.Deshabilitado = false " +
                         "ORDER BY pli.Description ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("practicasId", PTs.GetIds());
            query.SetInt32("medicoId", medicoId);

            EntityCollection<PlantillaInformeView> plantillas = dalEngine.GetManyByQuery<PlantillaInformeView>(query);
            EntityCollection<PlantillaInformeView> ret = PlantillasSinDuplicadasPorArchivo(plantillas);
            return ret;
        }

        private static EntityCollection<PlantillaInformeView> PlantillasSinDuplicadasPorArchivo(EntityCollection<PlantillaInformeView> plantillas)
        {
            EntityCollection<PlantillaInformeView> ret = new EntityCollection<PlantillaInformeView>();
            IList<String> ids = new List<String>();
            if (plantillas != null && plantillas.Count > 0)
            {
                ret.Add(plantillas[0]);
                ids.Add(plantillas[0].File);
                foreach (PlantillaInformeView planti in plantillas)
                {
                    if (!ids.Contains(planti.File))
                    {
                        ret.Add(planti);
                        ids.Add(planti.File);
                    }
                }
            }
            return ret;
        }

        public EntityCollection<Practica> ObtenerPracticasDeTurnoInforme(TurnoInforme turnoInforme)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            InformesDalc informesDalc = Context.Session.InformesDalc;

            List<int> turnoIds = new List<int>();

            TurnoInforme turnoInformPrincipal = informesDalc.TurnoInformePrincipalReadByTurnoInforme(turnoInforme);
            EntityCollection<TurnoInformeUnificado> informesUnificados =
                informesDalc.TurnoInformesUnificadosSecundariosReadByTurnoInformePrincipalId(turnoInformPrincipal.Id);

            turnoIds.Add(turnoInformPrincipal.TurnoID);

            if (informesUnificados != null)
                foreach (TurnoInformeUnificado infUnificado in informesUnificados)
                {
                    if (turnoIds.Contains(infUnificado.TurnoId) == false)
                        turnoIds.Add(infUnificado.TurnoId);
                }

            // Obtengo las Prácticas del Turno
            EntityCollection<PracticaTurno> PTs = TurnosDalc.PracticaTurnoReadByTurno(turnoIds, PracticaTurnoTipoEnum.Todas);
            SortedList<int, Practica> practicas = new SortedList<int, Practica>();
            foreach (PracticaTurno pt in PTs)
            {
                if (practicas.ContainsKey(pt.Practica.Id) == false)
                    practicas.Add(pt.Practica.Id, pt.Practica);
            }
            return new EntityCollection<Practica>(practicas.Values);
        }

        public PlantillaInformeView PlantillaInformeReadVacia(string tagPlantillaVacia)
        {
            return dalEngine.GetByProperty<PlantillaInformeView>(PlantillaInformeView.Properties.Description, tagPlantillaVacia);
        }

        #endregion

        #region Servicio
        public EntityCollection<Servicio> ServicioReadAll()
        {
            return dalEngine.GetAll<Servicio>(Servicio.Properties.Name);
        }

        public EntityCollection<Servicio> ServiciosReadByIds(IList<int> ids)
        {
            return dalEngine.GetManyByProperty<Servicio>(Servicio.Properties.Id, ids);
        }

        public EntityCollection<Servicio> ServiciosReadByTipo(TipoServicioEnum tipoServicio)
        {
            return dalEngine.GetManyByProperty<Servicio>(Servicio.Properties.TipoServicio, (int)tipoServicio, Servicio.Properties.Name);
        }

        /*Busca dado un tag, si existe una entrada en la BD con el mismo.*/
        public bool ExistServicioByTag(string tag)
        {
            Servicio serv = dalEngine.GetByProperty<Servicio>(Servicio.Properties.Tag, tag);
            return serv != null;
        }

        /// <summary>
        /// Devuelve todos los servicios puros.
        /// </summary>
        /// <returns>Todos los servicios</returns>
        public EntityCollection<Servicio> ServicioReadAllPuros()
        {
            return dalEngine.GetManyByProperty<Servicio>(Servicio.Properties.Puro, true, Servicio.Properties.Name);
        }

        public ReadAllCollection<ServicioName> ServicioNameReadAll()
        {
            EntityCollection<ServicioName> servicios = dalEngine.GetAll<ServicioName>(ServicioName.Properties.Name);
            return new ReadAllCollection<ServicioName>(servicios);
        }

        /// <summary>
        /// Devuelve todos los servicios que contienen equipos de la sucursal.
        /// </summary>
        /// <param name="sucursal">Sucursal de los Equipos de los Servicios</param>
        /// <returns>Todos los servicios</returns>
        public EntityCollection<Servicio> ServicioReadByEquiposSucursal(int sucursalId)
        {
            IQuery query = dalEngine.CreateQuery(
                    "select distinct e.Servicio from " +
                    "Equipo e where e.Sucursal.Id = :sucursalId " +
                    "order by e.Servicio.Name asc");
            query.SetParameter("sucursalId", sucursalId);
            return dalEngine.GetManyByQuery<Servicio>(query);
        }

        /// <summary>
        /// Devuelve el servicio asociado a un turno.
        /// </summary>
        /// <param name="turnoId">El id del turno</param>
        /// <returns>El servicio asociado a un turno</returns>
        [AnonymousMethod()]
        public Servicio ServicioReadByTurno(int turnoId)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            TurnoLight turno = dalEngine.GetById<TurnoLight>(turnoId);
            if (turno.EquipoId.HasValue)
            {
                Equipo equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);
                return equipo.Servicio;
            }
            else
                return null;
        }

        /// <summary>
        /// Actualizo un Servicio
        /// </summary>
        /// <param name="servicio">Servicio a Actualizar</param>




        [Private]
        #endregion

        #region UnidadArancelaria
        /// <summary>
        /// Retorna todas las unidades arancelarias del sistema
        /// </summary>
        /// <returns>Todas las unidades arancelarias del sistema</returns>
        public EntityCollection<UnidadArancelaria> UnidadArancelariaReadAll()
        {
            return dalEngine.GetAll<UnidadArancelaria>(UnidadArancelaria.Properties.Descripcion);
        }

        public EntityCollection<UnidadArancelaria> UnidadArancelariaReadByCodes(List<string> codes)
        {
            if (codes == null || codes.Count == 0)
                return new EntityCollection<UnidadArancelaria>();

            string hql = "select ua from UnidadArancelaria ua where upper(ltrim(rtrim(ua.Codigo))) in (:codes)";
            IQuery query = dalEngine.CreateQuery(hql);
            List<string> upperCodes = new List<string>();
            foreach (string code in codes)
                upperCodes.Add(code.ToUpper().Trim());

            query.SetParameterList("codes", upperCodes);
            return dalEngine.GetManyByQuery<UnidadArancelaria>(query);
        }

        public UnidadArancelaria UnidadArancelariaReadByCode(string code)
        {
            EntityCollection<UnidadArancelaria> unidades = UnidadArancelariaReadByCodes(new List<string>() { code });
            if (unidades.Count > 0)
                return unidades[0];

            return null;
        }

        /// <summary>
        /// Agrega todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El UnidadArancelariaCollection con todos los tipos a agregar</param>
        public void UnidadArancelariaAdd(EntityCollection<UnidadArancelaria> novedades)
        {
            dalEngine.UpdateCollection(novedades);
        }

        /// <summary>
        /// Actualiza todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El UnidadArancelariaCollection con todos los tipos a actualizar</param>
        public void UnidadArancelariaUpdate(EntityCollection<UnidadArancelaria> novedades)
        {
            dalEngine.UpdateCollection(novedades);
        }

        /// <summary>
        /// Elimina todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El UnidadArancelariaCollection con todos los tipos a eliminar</param>
        public void UnidadArancelariaDelete(EntityCollection<UnidadArancelaria> novedades)
        {
            dalEngine.Delete(novedades);
        }





        #endregion

        #region Consentimiento
        /// <summary>
        /// Retorna todas los consentimientos existentes
        /// </summary>
        /// <returns>Todos los consentimientos existentes</returns>
        public ReadAllCollection<Consentimiento> ConsentimientoReadAll()
        {
            return new ReadAllCollection<Consentimiento>(dalEngine.GetAll<Consentimiento>(Consentimiento.Properties.Name));
        }

        public EntityCollection<Consentimiento> ConsentimientoReadAll(bool soloVigentes)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("FROM Consentimiento con").Append(" ");
            if (soloVigentes)
                hql.Append("WHERE con.Deleted = :deleted").Append(" ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            if (soloVigentes)
                query.SetBoolean("deleted", false);

            return dalEngine.GetManyByQuery<Consentimiento>(query);
        }

        #endregion

        #region AutorizacionEstado
        [Private]
        public EntityCollection<AutorizacionEstado> AutorizacionEstadoReadAll()
        {
            return dalEngine.GetAll<AutorizacionEstado>(AutorizacionEstado.Properties.Descripcion);
        }





        public EntityCollection<AutorizacionEstado> AutorizacionEstadoReadForFormResolucion()
        {
            Filter filter = new Filter();
            filter.Add(AutorizacionEstado.Properties.Id, "<>",
                            (int)AutorizacionEstadoEnum.Ingresado);
            return dalEngine.GetManyByFilter<AutorizacionEstado>(filter);
        }
        #endregion

        #region TipoServicio
        public ReadAllCollection<TipoServicio> TipoServicioReadAll()
        {
            EntityCollection<TipoServicio> tipos = dalEngine.GetAll<TipoServicio>(TipoServicio.Properties.Name);
            return new ReadAllCollection<TipoServicio>(tipos);
        }
        #endregion

        public EntityCollection<Servicio> ServicioReadByName(string name)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select ser from  Servicio ser ");
            hqlBuilder.Append("where ser.Name like '%" + name + "%'");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<Servicio>(query);
        }
        
        public EntityCollection<Hl7TipoInterlocutorInfo> TipoInterlocutorReadByInterlocutor(Hl7Interlocutor interlocutor, bool emisores)
        {
            if (emisores)
                return (from intEvent in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where intEvent.Interlocutor.Id == interlocutor.Id && !intEvent.Deleted && intEvent.InterlocutorEmisor != null select new Hl7TipoInterlocutorInfo(intEvent.InterlocutorEmisor, intEvent.GrupoEvento)).ToEntityCollection<Hl7TipoInterlocutorInfo>();
            else
                return (from intEvent in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where intEvent.Interlocutor.Id == interlocutor.Id && !intEvent.Deleted && intEvent.InterlocutorReceptor != null select new Hl7TipoInterlocutorInfo(intEvent.InterlocutorReceptor, intEvent.GrupoEvento)).ToEntityCollection<Hl7TipoInterlocutorInfo>();
        }

        public EntityCollection<Hl7TipoGrupoEvento> TipoGrupoEventoReadByTipoIntelocutor(IHL7TipoInterlocutor tipo)
        {
            if (tipo is Hl7InterlocutorEmisor)
                return (from intEvent in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where intEvent.InterlocutorEmisor.Id == tipo.Id && !intEvent.Deleted select intEvent.GrupoEvento).Distinct().ToEntityCollection<Hl7TipoGrupoEvento>();
            else
                return (from intEvent in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where intEvent.InterlocutorReceptor.Id == tipo.Id && !intEvent.Deleted select intEvent.GrupoEvento).Distinct().ToEntityCollection<Hl7TipoGrupoEvento>();
        }

        public Hl7CampoFijo CampoFijoReadByTraduccion(Hl7InterlocutorTraduccion trad)
        {
            return (from fix in dalEngine.Query<Hl7CampoFijo>() where fix.TraduccionId == trad.Id select fix).FirstOrDefault();
        }

        public EntityCollection<Hl7Codigo> CodigosReadByTraduccion(Hl7InterlocutorTraduccion trad)
        {
            return (from code in dalEngine.Query<Hl7Codigo>() where code.TraduccionId == trad.Id select code).ToEntityCollection();
        }

        public EntityCollection<Hl7CampoValores> ValoresReadByCampo(Hl7Campo campo)
        {
            return (from val in dalEngine.Query<Hl7CampoValores>() where val.Campo.Id == campo.Id select val).ToEntityCollection();
        }

        public Hl7InterlocutorTraduccion TraduccionReadByInterlocutorAndCampo(Hl7Campo campo, Hl7Interlocutor interlocutor)
        {
            return (from trad in dalEngine.Query<Hl7InterlocutorTraduccion>() where trad.InterlocutorId == interlocutor.Id && campo.Id == trad.Valor.Campo.Id select trad).FirstOrDefault();
        }

        public EntityCollection<Hl7InterlocutorEventoEquipo> EventoEquipoReadByTipoEventoInterlocutor(Hl7TipoInterlocutorInfo tipoEventoInterlocutor)
        {
            if (tipoEventoInterlocutor.TipoInterlocutor is Hl7InterlocutorEmisor)
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoEquipo>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorEmisor.Id && !tev.Deleted select eq).ToEntityCollection();
            else
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoEquipo>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorReceptor.Id && !tev.Deleted select eq).ToEntityCollection();
        }

        public EntityCollection<Hl7InterlocutorEventoServicio> EventoServicioReadByTipoEventoInterlocutor(Hl7TipoInterlocutorInfo tipoEventoInterlocutor)
        {
            if (tipoEventoInterlocutor.TipoInterlocutor is Hl7InterlocutorEmisor)
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoServicio>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorEmisor.Id && !tev.Deleted select eq).ToEntityCollection();
            else
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoServicio>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorReceptor.Id && !tev.Deleted select eq).ToEntityCollection();
        }

        public EntityCollection<Hl7InterlocutorEventoSucursal> EventoSucursalReadByTipoEventoInterlocutor(Hl7TipoInterlocutorInfo tipoEventoInterlocutor)
        {
            if (tipoEventoInterlocutor.TipoInterlocutor is Hl7InterlocutorEmisor)
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoSucursal>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorEmisor.Id && !tev.Deleted select eq).ToEntityCollection();
            else
                return (from eq in dalEngine.Query<Hl7InterlocutorEventoSucursal>() join tev in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on eq.TipoEventoInterlocutorId equals tev.Id where tev.GrupoEvento.Id == tipoEventoInterlocutor.GrupoEvento.Id && tipoEventoInterlocutor.TipoInterlocutor.Id == tev.InterlocutorReceptor.Id && !tev.Deleted select eq).ToEntityCollection();
        }

        public EntityCollection<HL7Mensaje> Hl7MensajesReadByInterlocutorGrupoFecha(Hl7Interlocutor interlocutor, Hl7TipoGrupoEvento grupo, DateTime fecha)
        {
            EntityCollection<HL7Mensaje> response = new EntityCollection<HL7Mensaje>();
            if (grupo == null)
                response = (from men in dalEngine.Query<HL7Mensaje>()
                            join tgv in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on men.TipoEventoInterlocutorId equals tgv.Id
                            where tgv.Interlocutor.Id == interlocutor.Id && men.FechaEnviado > fecha.Date.AddSeconds(-1) && men.FechaEnviado < fecha.Date.AddDays(1)
                            select men).ToEntityCollection();
            else
                response = (from men in dalEngine.Query<HL7Mensaje>()
                            join tgv in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() on men.TipoEventoInterlocutorId equals tgv.Id
                            where tgv.Interlocutor.Id == interlocutor.Id && tgv.GrupoEvento.Id == grupo.Id
                            && men.FechaEnviado > fecha.Date.AddSeconds(-1) && men.FechaEnviado < fecha.Date.AddDays(1)
                            select men).ToEntityCollection();

            foreach (HL7Mensaje mensaje in response)
            {
                mensaje.Mensaje = DeserialzeMessageInfo(mensaje, HL7Mensaje.Properties.Mensaje);
                if (mensaje.Respuesta != null)
                    mensaje.Respuesta = DeserialzeMessageInfo(mensaje, HL7Mensaje.Properties.Respuesta);
            }
            return response;
        }

        private static byte[] DeserialzeMessageInfo(HL7Mensaje mensaje, IPropertyReference prop)
        {
            object value = LobUpdater.ReadBlob<HL7Mensaje, object>(mensaje, prop);
            if (value is string)
                return Encoding.Default.GetBytes((string)value);
            else // por error algunos quedaron serializados como byte[]
                return (byte[])value;
        }

        public void hl7TipoGrupoEventoInterlocutorDelete(Hl7TipoInterlocutorInfo iHL7TipoInterlocutor, Hl7Interlocutor interlocutor)
        {
            Hl7TipoGrupoEventoInterlocutor current = null;
            if (iHL7TipoInterlocutor.TipoInterlocutor is Hl7InterlocutorEmisor)
                current = (from evt in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where evt.InterlocutorEmisor.Id == iHL7TipoInterlocutor.TipoInterlocutor.Id && interlocutor.Id == evt.Interlocutor.Id && evt.GrupoEvento.Id == iHL7TipoInterlocutor.GrupoEvento.Id select evt).FirstOrDefault();
            else
                current = (from evt in dalEngine.Query<Hl7TipoGrupoEventoInterlocutor>() where evt.InterlocutorReceptor.Id == iHL7TipoInterlocutor.TipoInterlocutor.Id && interlocutor.Id == evt.Interlocutor.Id && evt.GrupoEvento.Id == iHL7TipoInterlocutor.GrupoEvento.Id select evt).FirstOrDefault();

            if (current != null)
            {
                current.Deleted = true;
                Context.Session.Dalc.Update(current);
            }
        }

        public EntityCollection<ServicioEspecialidad> ServicioEspecialidadReadByServicio(int servicioId)
        {
            return dalEngine.GetManyByProperty<ServicioEspecialidad>(ServicioEspecialidad.Properties.Servicio.Id, servicioId);
        }
        public EntityCollection<ServicioEspecialidad> ServicioEspecialidadReadAll()
        {
            return dalEngine.GetAll<ServicioEspecialidad>(ServicioEspecialidad.Properties.Nombre);
        }

        public void ServicioEspecialidadDelete(IEntityCollection especialidades)
        {
            dalEngine.Delete(especialidades);
        }

        public void ServicioEspecialidadDelete(ServicioEspecialidad especialidad)
        {
            dalEngine.Delete(especialidad);
        }

        public ServicioEspecialidad GetServicioEspecialidadByTurnoId(int turnoId)
        {
            IQueryable<ServicioEspecialidad> queryServicioEspecialidad = from pt in dalEngine.Query<PracticaTurno>()
                                                                         join pra in dalEngine.Query<Practica>()
                                                                             on pt.Practica.Id equals pra.Id
                                                                         join se in dalEngine.Query<ServicioEspecialidad>()
                                                                             on pra.ServicioEspecialidad.Id equals se.Id
                                                                         where pt.TurnoId == turnoId
                                                                         select se;
            ServicioEspecialidad servicioEspecialidad = queryServicioEspecialidad.First();
            return servicioEspecialidad;
        }

        public Autorizacion AutorzacionUpdateConReserva(Autorizacion autorizacion, TurnoReservaInfo reserva)
        {
            if (reserva != null)
                return LobUpdater.UpdateBlob<Autorizacion>(autorizacion, Autorizacion.Properties.DBAgenda, reserva);
            else
                return AutorizacionUpdate(autorizacion);
        }

        public TurnoReservaInfo ReservaReadByAutorizacion(Autorizacion autorizacion)
        {
            try
            {
                if (autorizacion.DBAgenda == null)
                    autorizacion = dalEngine.GetById<Autorizacion>(autorizacion.Id);

                return LobUpdater.ReadBlob<Autorizacion, TurnoReservaInfo>(autorizacion, Autorizacion.Properties.DBAgenda);
            }
            catch (Exception)
            {
                throw new enfokeDataException("La previsualización de agenda no esta disponible.");
            }
        }

        [RequiresTransaction]
        public virtual void ServicioEspecialidadUpdate(ServicioConEquipo servicio, EntityCollection<TipoInforme> tiChequeados)
        {
            if (servicio.Id == 0)
            { //--> estoy creando un servicio, por lo tanto debo crear un nuevo protocolo
                EntityCollection<Sucursal> sucursales = Context.Session.Dalc.GetAll<Sucursal>();
                EntityCollection<SucursalProtocolo> sp = new EntityCollection<SucursalProtocolo>();

                foreach (Sucursal suc in sucursales)
                    sp.Add(new SucursalProtocolo(servicio.Tag, suc.Id));

                Context.Session.Dalc.UpdateCollection<SucursalProtocolo>(sp);
            }

            dalEngine.Update<ServicioConEquipo>(servicio);
            EntityCollection<ServicioEspecialidad> especialidadEliminadas = new EntityCollection<ServicioEspecialidad>(servicio.Especialidades.DeletedItems);
            Servicio servicioOriginal = Context.Session.Dalc.GetById<Servicio>(servicio.Id);
            foreach (ServicioEspecialidad especialidad in servicio.Especialidades)
                especialidad.Servicio = servicioOriginal;

            if (especialidadEliminadas.Count > 0)
                dalEngine.Delete(especialidadEliminadas);

            ServicioEspecialidadUpdateMany(servicio.Especialidades);
            UpdateTipoInformeServicios(servicio.Id, tiChequeados);
        }

        private void UpdateTipoInformeServicios(int servicioId, EntityCollection<TipoInforme> tiChequeados)
        {
            if (servicioId != 0)
                Context.Session.InformesDalc.TipoInformeServicioDeleteByServicioId(servicioId);

            EntityCollection<TipoInformeServicio> tisCollection = new EntityCollection<TipoInformeServicio>();
            foreach (TipoInforme ti in tiChequeados)
            {
                TipoInformeServicio tis = new TipoInformeServicio(servicioId, ti.Id);
                tisCollection.Add(tis);
            }

            Context.Session.InformesDalc.TipoInformeServicioUpdateMany(tisCollection);
        }
        public void ServicioEspecialidadUpdateMany(EntityCollection<ServicioEspecialidad> especialidades)
        {
            dalEngine.UpdateCollection(especialidades);
        }

        public EntityCollection<ServicioMensajeriaServicio> ServiciosPorMensajeriaReadByServicioSMSId(int servicioSMSId)
        {
            return dalEngine.GetManyByProperty<ServicioMensajeriaServicio>(ServicioMensajeriaServicio.Properties.ServicioMensajeriaId, servicioSMSId, ServicioMensajeriaServicio.Properties.Servicio.Name);
        }

        public void ServicioMensajeriaServicioDeleteByServicioSMS(int servicioId)
        {
            EntityCollection<ServicioMensajeriaServicio> deleted = dalEngine.GetManyByProperty<ServicioMensajeriaServicio>(ServicioMensajeriaServicio.Properties.ServicioMensajeriaId, servicioId);
            dalEngine.DeleteBatchByIds<ServicioMensajeriaServicio>(deleted.GetIds());
        }

        public void ServicioMensajeriaServicioUpdate(EntityCollection<ServicioMensajeriaServicio> collecion)
        {
            dalEngine.UpdateCollection(collecion);
        }

        public EntityCollection<CondicionServicio> CondicionServicioReadByCondicionId(int condicionId)
        {
            return Context.Session.Dalc.GetManyByProperty<CondicionServicio>(CondicionServicio.Properties.Condicion.Id, condicionId);
        }

        [RequiresTransaction]
        public virtual void CondicionServicioUpdate(Condicion condicion, EntityCollection<Servicio> servicios)
        {
            EntityCollection<CondicionServicio> condicionesServicioAEliminar = CondicionServicioReadByCondicionId(condicion.Id);
            dalEngine.Delete(condicionesServicioAEliminar);

            EntityCollection<CondicionServicioLight> condicionesServicioAAgregar = new EntityCollection<CondicionServicioLight>();
            foreach (Servicio servicio in servicios)
            {
                CondicionServicioLight condicionServicio = new CondicionServicioLight();
                condicionServicio.CondicionId = condicion.Id;
                condicionServicio.ServicioId = servicio.Id;
                condicionesServicioAAgregar.Add(condicionServicio);
            }
            dalEngine.UpdateCollection<CondicionServicioLight>(condicionesServicioAAgregar);
        }
    }
}

