using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using enfoke.AOP;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;

using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Data.DisconnectedSupport;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class EquiposDalc : Dalc, IService
    {
        protected EquiposDalc(NotConstructable dummy) : base(dummy) { }

        #region EquipoCupo

        public IList<string> EquipoCupoReadDistinctLeyendas()
        {
            string hql = "select distinct eq.Leyenda from EquipoCupo eq where eq.Deleted = false order by eq.Leyenda";
            IQuery query = dalEngine.CreateQuery(hql);
            return query.List<string>();
        }
        public EntityCollection<EquipoCupo> EquipoCupoReadByObraSocialSucursalAndEquipo(
                        SucursalName sucursal, Equipo equipo,
                        EquipoCupoTipoEnum filterTipo, bool soloVigentes)
        {
            Filter filter = new Filter();
            filter.Add(EquipoCupo.Properties.Equipo.Sucursal.Id, "=", sucursal.Id);
            if (filterTipo != EquipoCupoTipoEnum.Undefined)
                filter.Add(BooleanOp.And, EquipoCupo.Properties.TipoDb, "=", (int)filterTipo);
            if (equipo != null)
                filter.Add(BooleanOp.And, EquipoCupo.Properties.Equipo.Id, "=", equipo.Id);
            filter.Add(BooleanOp.And, EquipoCupo.Properties.Deleted, "=", false);
            if (soloVigentes)
            {
                //filter.Add(new OpenParenthesis(BooleanOp.And));
                //filter.Add(EquipoCupo.Properties.Horario.FechaInicio, "<=", enfoke.Time.Now.Date);
                filter.Add(BooleanOp.And, EquipoCupo.Properties.Horario.FechaFin, ">=", enfoke.Time.Now.Date);
                //filter.Add(new CloseParenthesis());
            }
            // Pone el orden
            Sort sort = new Sort();
            sort.Add(EquipoCupo.Properties.Equipo.Descripcion);
            sort.Add(EquipoCupo.Properties.Horario.FechaInicio);
            // Devuelve resultados
            return dalEngine.GetManyByFilter<EquipoCupo>(filter, sort);
        }
        /// <summary>
        /// Hace la baja lógica de un cupo.
        /// </summary>
        public void EquipoCupoDelete(EquipoCupo cupo)
        {
            cupo.Deleted = true;
            dalEngine.Update(cupo);
        }

        /// <summary>
        /// Actualiza un cupo verificando que no se superponga con otro.
        /// </summary>
        [RequiresNewTransaction]
		  public virtual EquipoCupo EquipoCupoUpdate(EquipoCupo cupo)
        {
            // Busco otros cupos no eliminados del mismo equipo (que no sea el cupo en cuestion)
            Filter filter = new Filter();
            filter.Add(EquipoCupo.Properties.Equipo.Id, "=", cupo.Equipo.Id);
            filter.Add(BooleanOp.And, EquipoCupo.Properties.Deleted, "=", false);
            if (cupo.Id != 0)
                filter.Add(BooleanOp.And, EquipoCupo.Properties.Id, "<>", cupo.Id);

            EntityCollection<EquipoCupo> cuposDelEquipo = dalEngine.GetManyByFilter<EquipoCupo>(filter);

            // Solo chequeo si hay otros cupos del equipo
            if (cuposDelEquipo.Count > 0)
            {
                TimeSet cupoRecibido = HorarioUtils.Expand(cupo.Horario);

                // Compara con el recibido...
                foreach (EquipoCupo cupoEvaluado in cuposDelEquipo)
                {
                    TimeSet cupoEvaluadoTs = HorarioUtils.Expand(cupoEvaluado.Horario, cupo.Horario.FechaInicio, cupo.Horario.FechaFin);
                    if (cupoEvaluadoTs.Intersect(cupoRecibido).Count > 0)
                        throw new NotLoggeableException("El cupo indicado se superpone con un cupo existente.\n\nCupo: " + cupoEvaluado.ToString() + ".");
                }
            }

            // Graba el horario y el cupo
            dalEngine.Update(cupo.Horario);
            return dalEngine.Update(cupo);
        }

        #endregion

        public void ExcepcionEquipoHorarioUpdateComoNoMandaMensajes(EntityCollection<Turno> turnos)
        { 
            List<int> eehIds = new List<int>();
            foreach (Turno turno in turnos)
            {
                if (!eehIds.Contains(turno.ExcepcionEquipoHorarioId))
                    eehIds.Add(turno.ExcepcionEquipoHorarioId);
            }

            if (eehIds.Count == 0)
                return;

            EntityCollection<ExcepcionHorarioEquipo> excepcionHorarioEquipo = ExcepcionHorarioEquipoReadByEquipo(eehIds);
            foreach (ExcepcionHorarioEquipo eeh in excepcionHorarioEquipo)
                eeh.EnviaMensajesAHuerfanos = false;

            dalEngine.UpdateCollection(excepcionHorarioEquipo);
        }

        [Private]
        public EntityCollection<EquipoPuntoVenta> EquiposExcluidosPorCaja(int cajaId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Results.EquipoPuntoVenta (pve.Equipo, cpv.PuntoVenta) from PuntoVentaEquipo pve, CajaPuntoVenta cpv ");
            hqlBuilder.Append("where pve.PuntoVentaId = cpv.PuntoVenta.Id ");
            hqlBuilder.Append("and cpv.CajaId = " + cajaId.ToString());
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<EquipoPuntoVenta>(query);
        }

        public EntityCollection<Equipo> EquiposReadByDescripcion(IList<string> equipos)
        {
            if (equipos == null || equipos.Count == 0)
                return new EntityCollection<Equipo>();

            List<string> upperCodes = new List<string>();
            foreach (string code in equipos)
                upperCodes.Add(code.Trim().ToUpper());

            string hql = "select equ from Equipo equ where upper(ltrim(rtrim(equ.Descripcion))) IN (:equipos)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("equipos", upperCodes);
            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        private EntityCollection<InformeDeSMS> ConstruirInformes(EntityCollection<Turno> turnosAfectados, EntityCollection<Paciente> pacientes)
        {
            EntityCollection<InformeDeSMS> response = new EntityCollection<InformeDeSMS>();
            Context.Session.TurnosDalc.SortByShitDate(turnosAfectados);
            foreach (Paciente paciente in pacientes)
            {
                List<Turno> turnos = new List<Turno>(this.TurnosPorPaciente(turnosAfectados, paciente));
                InformeDeSMS informe = new InformeDeSMS(paciente);
                informe.TurnosAfectados = turnos;
                Equipo equipo = turnos[0].Equipo;
                informe.Servicio = equipo.Servicio;
                informe.Centro = equipo.Sucursal;
                informe.FechaPrimerTurno = turnos[0].Fecha;
                informe.Mensaje = turnos[0].MensajeAdicionado;
                response.Add(informe);
            }
            return response;
        }

        private IEnumerable<Turno> TurnosPorPaciente(EntityCollection<Turno> informesAfectados, Paciente paciente)
        {
            return informesAfectados.FindAll(delegate(Turno turno) { return turno.Orden.PacienteId == paciente.Id; });
        }

        private EntityCollection<Paciente> ObtenerPacientesDeTurnosAfectados(EntityCollection<Turno> turnosInformeAfectados)
        {
            if (turnosInformeAfectados.Count == 0)
                return new EntityCollection<Paciente>();

            List<int> idTurnos = new List<int>();
            List<IIdentificable> turnos = new List<IIdentificable>(turnosInformeAfectados.Count);
            foreach (Turno turno in turnosInformeAfectados)
            {
                idTurnos.Add(turno.Id);
                turnos.Add(turno);
            }

            SQLBlockBuilder<IIdentificable> blockBuilder = new SQLBlockBuilder<IIdentificable>(turnos);
            string turnosIds = blockBuilder.BuildConstrainBlock("tur.Id");
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select distinct pac ");
            hqlBuilder.Append("from Paciente pac, Turno tur ");
            hqlBuilder.AppendFormat("where {0} and ", turnosIds);
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            EntityCollection<Paciente> pacientes = dalEngine.GetManyByQuery<Paciente>(query);
            return pacientes;
        }

        public EntityCollection<PuntoVentaEquipo> PuntoVentaEquipoReadByPuntoVenta(int puntoVentaId)
        { 
            Filter filter = new Filter();
            filter.Add(PuntoVentaEquipo.Properties.PuntoVentaId, " = ", puntoVentaId);
            return dalEngine.GetManyByFilter<PuntoVentaEquipo>(filter);
        }

        #region Equipo
        /// <summary>
        /// Devuelve los Equipos de un Servicio
        /// </summary>
        /// <param name="servicioId">Servicio del cual se necesitan los Equipos</param>
        /// <returns>Equipos del Servicio</returns>
        public EntityCollection<Equipo> EquipoReadByServicio(int servicioId)
        {
            return this.EquipoReadByServicioIDAndSucursalID(servicioId, (int?)null);
        }

        public EntityCollection<EquipoName> EquipoNameReadByServicioIdAndSucursalId(int servicioId, int sucursalId) {
            EntityCollection<EquipoName> equipos = (from equ in dalEngine.Query<Equipo>()
                                              where equ.Sucursal.Id == sucursalId && equ.Servicio.Id == servicioId
                                              select new EquipoName(equ.Id, equ.Descripcion, equ.Tag)).ToEntityCollection();
            return equipos;
        }

        public Servicio ServicioReadByEquipoId(int equipoId)
        {
            var query = from equipo in dalEngine.Query<Equipo>() where equipo.Id == equipoId select equipo.Servicio;
            return query.First();
        }

        public EntityCollection<Equipo> EquiposReadBySinAtrasos()
        {
            StringBuilder hqlBuilder = new StringBuilder();
            // Si existe equipo que esta marcado como mandando mensajes
            hqlBuilder.Append("Select equ from Equipo equ where ");
            hqlBuilder.Append("equ.ControlaRetraso = true and ");
            hqlBuilder.Append("equ.EnvioMailRetraso = true and ");
            // Y ya no tiene mas retrasos
            hqlBuilder.Append("not exists (Select tur from Turno tur, TurnoLog tlog where tur.EstadoTurnoID = :turno and ");
            hqlBuilder.Append("tur.Id = tlog.TurnoId and ");
            hqlBuilder.Append("tlog.InicioPracticaFecha is null and ");
            hqlBuilder.Append("tur.EquipoId = equ.Id and ");
            string conversionDeMinutos = enfoke.Utils.SqlPortable.DateAddMinutes("tur.Fecha", "equ.ToleranciaRetraso");
            hqlBuilder.Append(conversionDeMinutos +  " < :fechaHasta and ");
            hqlBuilder.Append("tur.Fecha >= :fechaDesde) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("turno", (int)EstadoTurnoEnum.Recepcionado);
            query.SetDateTime("fechaHasta", enfoke.Time.Now);
            query.SetDateTime("fechaDesde", enfoke.Time.Now.AddHours(-12));
            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        [Private]
        public EntityCollection<Equipo> EquipoReadByServicioPacientePracticasAndRestriccionDMO(
            List<int> practicas, Servicio servicio,
            SucursalName sucursal, decimal? peso, decimal? altura, bool? claustrofobico,
            bool aplicaRestriccionEcoMamoDMO,
            string[] equiposRestringidos)
        {
            // Arma el string
            string hql = "select ep.Equipo from EquipoPractica ep where " +
                            " ep.Equipo.Servicio.Id = :servicioId " +
                            " AND ep.Equipo.Deleted = false " +
                            " AND ep.Practica.Id = (:practica0) ";
            // Arma la parte de practicas subsiguientes
            // (la #0 es la principal)
            for (int subsiguiente = 1; subsiguiente < practicas.Count; subsiguiente++)
            {
                hql += " AND EXISTS (SELECT eqp from EquipoPractica eqp where " +
                            " eqp.Equipo = ep.Equipo AND eqp.Practica.Id = :practica" + subsiguiente.ToString() + ")";
            }
            // Le pone las condiciones opcionales
            if (sucursal != null)
                hql += " AND ep.Equipo.Sucursal.Id = :sucursal ";
            if (peso.HasValue && peso.Value > 0)
                hql += " AND (ep.Equipo.PesoMaximo = 0 OR ep.Equipo.PesoMaximo >= :peso) ";
            if (altura.HasValue && altura.Value > 0)
                hql += " AND (ep.Equipo.AlturaMaxima = 0 OR ep.Equipo.AlturaMaxima >= :altura) ";
            if (claustrofobico.HasValue && claustrofobico.Value)
                hql += " AND ep.Equipo.AceptaClaustroficos = true ";
            if (aplicaRestriccionEcoMamoDMO)
                hql += " AND ep.Equipo.Tag NOT IN (:equiposTag) ";

            hql += " ORDER BY ep.Equipo.Descripcion";
            // Arma el query
            IQuery query = dalEngine.CreateQuery(hql);
            // Pone los parámetros
            query.SetParameter("servicioId", servicio);
            query.SetParameter("practica0", practicas[0]);
            // Setea los parámetros para los filtros de subsiguientes
            for (int subsiguiente = 1; subsiguiente < practicas.Count; subsiguiente++)
                query.SetParameter("practica" + subsiguiente.ToString(), practicas[subsiguiente]);

            if (sucursal != null)
                query.SetParameter("sucursal", sucursal.Id);
            if (peso.HasValue && peso.Value > 0)
                query.SetParameter("peso", peso.Value);
            if (altura.HasValue && altura.Value > 0)
                query.SetParameter("altura", altura.Value);

            if (aplicaRestriccionEcoMamoDMO)
                query.SetParameterList("equiposTag", equiposRestringidos);
            // Devuelve el query
            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        public EntityCollection<Equipo> EquipoReadByServicioPacientePracticasAndRestriccionDMO(SucursalName sucursal, TurnoReservaInfo tri, decimal? peso, decimal? altura, bool? claustrofobico, bool consideraSubsiguientes, bool aplicaRestriccionEcoMamoDMO, string[] equiposRestringidos)
        {
            // Pone las practicas involucradas en una lista...
            List<int> practicas = new List<int>();
            foreach (PracticaInfo pi in tri.PracticaInfos)
                practicas.AddRange(ListarPracticasInvolucradas(pi, consideraSubsiguientes));

            return EquipoReadByServicioPacientePracticasAndRestriccionDMO(practicas, tri.PracticaInfoUnica.Practica.ServicioEspecialidad.Servicio, sucursal, peso, altura, claustrofobico, aplicaRestriccionEcoMamoDMO, equiposRestringidos);

        }

        public EntityCollection<Equipo> EquipoReadByServicioPacientePracticasAndRestriccionDMO(SucursalName sucursal, PracticaInfo practicaInfo, decimal? peso, decimal? altura, bool? claustrofobico, bool consideraSubsiguientes, bool aplicaRestriccionEcoMamoDMO, string[] equiposRestringidos)
        {
            if(practicaInfo.Practica == null)
                return new EntityCollection<Equipo>();
            
            // Pone las practicas involucradas en una lista...
            List<int> practicas = ListarPracticasInvolucradas(practicaInfo, consideraSubsiguientes);

            return EquipoReadByServicioPacientePracticasAndRestriccionDMO(practicas, practicaInfo.Practica.ServicioEspecialidad.Servicio, sucursal, peso, altura, claustrofobico, aplicaRestriccionEcoMamoDMO, equiposRestringidos);
        }

        private List<int> ListarPracticasInvolucradas(PracticaInfo practicaInfo, bool consideraSubsiguientes)
        {
            List<int> practicas = new List<int>();
            if(practicaInfo.Practica != null)
                practicas.Add(practicaInfo.Practica.Id);
            if (consideraSubsiguientes)
                foreach (ExposicionInfo ei in practicaInfo.Exposiciones)
                    if (ei.Exposicion != null)
                        practicas.Add(ei.Exposicion.Id);
            return practicas;
        }

        internal List<int> EquiposIdsPorSector(Sector sector, bool mostrarEliminados)
        {
            string hql = "select distinct e.Id from Equipo e, SectorServicio ss "
                        + "where e.Servicio = ss.Servicio and "
                        + "e.Sucursal.Id = :sucursal and ss.Sector.Id = :sector ";

            if (!mostrarEliminados)
                hql += "and e.Deleted = false";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("sector", sector.Id);
            query.SetInt32("sucursal", sector.Sucursal.Id);
            return (List<int>)query.List<int>();
        }

        internal EntityCollection<Equipo> EquiposPorSector(Sector sector)
        {
            string hql = "select distinct e from Equipo e, SectorServicio ss "
                        + "where e.Servicio = ss.Servicio and "
                        + "e.Sucursal.Id = :sucursal and ss.Sector.Id = :sector "
                        + "and e.Deleted = :delete";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("sector", sector.Id);
            query.SetInt32("sucursal", sector.Sucursal.Id);
            query.SetBoolean("delete", false);

            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        /// <summary>
        /// Devuelve los Equipos de un Servicio y una Sucursal
        /// </summary>
        /// <param name="servicioId">Servicio a buscar</param>
        /// <param name="sucursal">Sucursal a buscar</param>
        /// <returns>Equipos del Servicio</returns>
        public EntityCollection<Equipo> EquipoReadByServicioIDAndSucursal(int servicioId, SucursalName sucursal)
        {
            return EquipoReadByServicioIDAndSucursalID(servicioId, sucursal != null ? sucursal.Id : (int?)null);
        }

        /// <summary>
        /// Devuelve los Equipos de un Servicio y una Sucursal
        /// </summary>
        /// <param name="servicioId">Servicio a buscar</param>
        /// <param name="sucursalId">Sucursal a buscar</param>
        /// <returns>Equipos del Servicio</returns>
        public EntityCollection<Equipo> EquipoReadByServicioIDAndSucursalID(int? servicioId, int? sucursalId)
        {
            Filter filter = new Filter();

            filter.Add(Equipo.Properties.Deleted, "=", false);
            if (servicioId.HasValue == true)
                filter.Add(BooleanOp.And, Equipo.Properties.Servicio.Id, "=", servicioId);
            if (sucursalId.HasValue == true)
                filter.Add(BooleanOp.And, Equipo.Properties.Sucursal.Id, "=", sucursalId.Value);

            Sort sort = new Sort();
            sort.Add(Equipo.Properties.Descripcion);

            return dalEngine.GetManyByFilter<Equipo>(filter, sort);
        }

        public EntityCollection<Equipo> EquipoReadBySucursalID(int sucursalID)
        {
            return EquipoReadByServicioIDAndSucursalID((int?)null, sucursalID);
        }

        /// <summary>
        /// Devuelve todos los equipos vigentes.
        /// </summary>
        /// <returns>Todos los equipos</returns>
        [AnonymousMethod]
        public EntityCollection<Equipo> EquipoReadAll()
        {
            return this.EquipoReadAll(true);
        }

        /// <summary>
        /// Devuelve todos los equipos ordenados por Id.
        /// </summary>
        /// <returns>Todos los equipos</returns>
        [Private]
        public EntityCollection<Equipo> EquipoReadAllSortedById()
        {
            return dalEngine.GetAll<Equipo>(Equipo.Properties.Id);
        }

        /// <summary>
        /// Devuelve todos los equipos.
        /// </summary>
        /// <param name="soloVigentes">Marca si traigo solo los Vigentes [T] o Todos [F]</param>
        /// <returns>Todos los equipos</returns>
        public EntityCollection<Equipo> EquipoReadAll(bool soloVigentes)
        {
            if (soloVigentes)
            {
                return dalEngine.GetManyByProperty<Equipo>(Equipo.Properties.Deleted, 0, Equipo.Properties.Descripcion);
            }
            else
                return dalEngine.GetAll<Equipo>(Equipo.Properties.Descripcion);
        }

        [AnonymousMethod()]
        public Equipo EquipoReadById(int equipoId)
        {
            // Se toma la libertad de cachearlo por thread
            Equipo ret = EntityThreadCache<Equipo>.GetItem(equipoId);
            if (ret == null)
            {
                ret = dalEngine.GetById<Equipo>(equipoId);
                if (ret != null)
                    EntityThreadCache<Equipo>.SetItem(equipoId, ret);
            }
            return ret;
        }

        public EntityCollection<Equipo> EquipoReadByIds(IList<int> equiposIds)
        {
            return dalEngine.GetManyByIds<Equipo>(equiposIds);
        }

        public int SucursalIdReadByEquipoId(int equipoId)
        {
            string hql = "SELECT e.Sucursal.Id FROM Equipo e WHERE e.Id = :equipoId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipoId);

            object ret = query.UniqueResult();

            if (ret != null)
                return int.Parse(ret.ToString());

            throw new Exception("El Equipo no tiene una sucursal parametrizada o no existe el equipo.");
        }

        public Equipo EquipoReadByTag(string tag)
        {
            return dalEngine.GetByProperty<Equipo>(Equipo.Properties.Tag, tag);
        }

        [AnonymousMethod()]
        public EntityCollection<Equipo> EquipoReadByTags(List<string> tags)
        {
            return dalEngine.GetManyByPropertyList<Equipo>(Equipo.Properties.Tag, tags);
        }

        /// <summary>
        /// Devuelve los equipos en los cuales un médico puede trabajar.
        /// </summary>
        /// <param name="userId">El médico</param>
        /// <returns>Los equipos</returns>
        public EntityCollection<Equipo> EquipoReadByMedico(int medicoId)
        {
            return EquipoReadByMedicoIdAndCentroId(medicoId, null);
        }
        
        [Private]
        public EntityCollection<Equipo> EquiposReadByServicioAndPaciente(int servicioId, Paciente paciente, SucursalName sucursal)
        {
            EntityCollection<Equipo> equipos = EquipoReadByServicioIDAndSucursal(servicioId, sucursal);
            EntityCollection<Equipo> filteredEquipos = new EntityCollection<Equipo>();

            foreach (Equipo equipo in equipos)
            {
                bool vaPorPeso = (equipo.PesoMaximo == 0 || equipo.PesoMaximo >= paciente.Peso);
                bool vaPorAltura = (equipo.AlturaMaxima == 0 || equipo.AlturaMaxima >= paciente.Altura);
                bool vaPorClaustrofobia = (!paciente.Claustrofobico || equipo.AceptaClaustroficos);

                if (vaPorAltura && vaPorClaustrofobia && vaPorPeso)
                    filteredEquipos.Add(equipo);
            }

            return filteredEquipos;
        }
        /// <summary>
        /// Devuelve los equipos en los cuales un médico puede trabajar.
        /// </summary>
        /// <param name="medicoId">El médico</param>
        /// <param name="centroId">El centro sobre el que se quieren buscar los equipos. Si es null, busca en todos los centros</param>
        /// <returns>Los equipos</returns>
        public EntityCollection<Equipo> EquipoReadByMedicoIdAndCentroId(int idMedico, int? idCentro)
        {
            string hql = "SELECT DISTINCT e FROM Equipo e JOIN e.Sucursal s, MedicoPractica mp " +
                         "WHERE e.Servicio = mp.Practica.ServicioEspecialidad.Servicio " +
                         "AND mp.MedicoId = :medico " +
                         "AND e.Deleted = false ";

            if (idCentro.HasValue)
                hql += "AND s.Id = :centroId ";

            hql += "ORDER BY e.Descripcion ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("medico", idMedico);
            if (idCentro.HasValue)
                query.SetParameter("centroId", idCentro);

            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        public EntityCollection<Equipo> EquipoReadByPracticas(List<int> practicasIds)
        {
            if (practicasIds.Count == 0)
                return new EntityCollection<Equipo>();

            string hql = "SELECT DISTINCT e FROM Equipo e, Practica p " +
                         "WHERE e.Servicio = p.ServicioEspecialidad.Servicio " +
                         "AND e.Deleted = false " +
                         "AND p.Id in (:practicas)" +
                         "ORDER BY e.Descripcion ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("practicas", practicasIds);

            return dalEngine.GetManyByQuery<Equipo>(query);
        }

        [AnonymousMethod()]
        public EntityCollection<Equipo> EquipoReadByIdPracticasAndIdCentro(List<int> idsPracticas, int? idCentro)
        {
            if (idsPracticas.Count == 0)
                return new EntityCollection<Equipo>();

            StringBuilder hql = new StringBuilder();
            hql.Append("SELECT DISTINCT e FROM Equipo e JOIN e.Sucursal s, Practica p ");
            hql.Append("WHERE e.Servicio = p.ServicioEspecialidad.Servicio ");
            hql.Append("AND e.Deleted = false ");

            SQLBlockBuilder<int> practIds = new SQLBlockBuilder<int>(idsPracticas);
            string practIdsAux = practIds.BuildConstrainBlock("p.Id");
            hql.AppendFormat(" and {0}", practIdsAux);

            if (idCentro.HasValue)
                hql.Append("AND s.Id = :centroId ");

            hql.Append("ORDER BY e.Descripcion ASC ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (idCentro.HasValue)
                query.SetParameter("centroId", idCentro);

            return dalEngine.GetManyByQuery<Equipo>(query);
        }
        public EntityCollection<Equipo> EquipoReadByName(string text)
        {
            return EquipoReadByName(text, false);
        }

        public EntityCollection<Equipo> EquipoReadByParameters(string descripcion, int? servicioId, List<int> centrosIds, TipoAgendaEnum? tipoAgenda)
        {
            ReadManyCommand<Equipo> readCmd = new ReadManyCommand<Equipo>(dalEngine);

            Filter filter = new Filter();

            if(!String.IsNullOrEmpty(descripcion))
                filter.Add(BooleanOp.And, Equipo.Properties.Descripcion, "LIKE", descripcion.Trim().Replace(" ", "%") + "%");

            if (servicioId.HasValue)
                filter.Add(BooleanOp.And, Equipo.Properties.Servicio.Id, "=", servicioId.Value);

            if (centrosIds.Count > 0)
            {
                if(centrosIds.Count==1)
                    filter.Add(BooleanOp.And, Equipo.Properties.Sucursal.Id, "=", centrosIds[0]);
                else
                    filter.Add(BooleanOp.And, Equipo.Properties.Sucursal.Id, "in", centrosIds);
            }

            if (tipoAgenda.HasValue)
                filter.Add(BooleanOp.And, Equipo.Properties.DbTipoAgenda, "IN", new short[] {
                                            (short)tipoAgenda.Value, (short) TipoAgendaEnum.Ambas});

            filter.Add(BooleanOp.And, Equipo.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<Equipo> EquipoReadByName(string text, bool soloVigentes)
        {
            ReadManyCommand<Equipo> readCmd = new ReadManyCommand<Equipo>(dalEngine);

            Filter filter = new Filter();

            // se filtra el nombre
            filter.Add(Equipo.Properties.Descripcion, "LIKE", text.Trim().Replace(" ", "%") + "%");

            // Se excluye a los eliminados
            if (soloVigentes)
                filter.Add(BooleanOp.And, Equipo.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            EntityCollection<Equipo> col = readCmd.Execute();

            return col;
        }

        public EntityCollection<Equipo> EquipoReadByFormatoXML(FormatoXMLEnum formato)
        {
            ReadManyCommand<Equipo> readCmd = new ReadManyCommand<Equipo>(dalEngine);

            Filter filter = new Filter();

            // se filtra el formato
            filter.Add(Equipo.Properties.FormatoXML, "=", (int)formato);

            readCmd.Filter = filter;

            EntityCollection<Equipo> col = readCmd.Execute();

            return col;
        }

        public void EquipoDelete(Equipo e)
        {
            e.Deleted = true;
            EquipoUpdate(e);
        }

        public void EquipoUndelete(Equipo e)
        {
            e.Deleted = false;
            EquipoUpdate(e);
        }

        public Equipo EquipoUpdate(Equipo e)
        {

            bool nuevo = (e.Id == 0);
            e = dalEngine.Update<Equipo>(e);

            if(nuevo)
            {
                EquipoActualizacionUpdate(new EquipoActualizacion {EquipoId = e.Id, Fecha = enfoke.Time.Now});
            }

            return e;
        }

        #endregion

        #region TipoNovedadEquipo


        /// <summary>
        /// [RQ] Agrega un nuevo tipo de novedades proporcionado.
        /// </summary>
        /// <param name="tipoNovedadEquipo">El tipo de novedad a agregar</param>
        public void TipoNovedadEquipoAdd(TipoNovedadEquipo tipoNovedadEquipo)
        {
            dalEngine.Update<TipoNovedadEquipo>(tipoNovedadEquipo);
        }

        /// <summary>
        /// [RQ] Actualiza el tipo de novedades proporcionado.
        /// </summary>
        /// <param name="tipoNovedadEquipo">El tipo de novedad a actualizar</param>
        public void TipoNovedadEquipoUpdate(TipoNovedadEquipo tipoNovedadEquipo)
        {
            dalEngine.Update<TipoNovedadEquipo>(tipoNovedadEquipo);
        }


        #endregion

        #region EquipoPractica

        public EntityCollection<EquipoPractica> EquipoPracticaReadByEquipoAndPractica(Equipo e, Practica p)
        {
            ReadManyCommand<EquipoPractica> readCmd = new ReadManyCommand<EquipoPractica>(dalEngine);

            Filter filter = new Filter();
            filter.Add(EquipoPractica.Properties.Equipo.Id,
                "=", e.Id);

            filter.Add(BooleanOp.And, EquipoPractica.Properties.Practica.Id,
                "=", p.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<EquipoPractica> EquipoPracticaReadByPracticaId(int practicaId)
        {
            return dalEngine.GetManyByProperty<EquipoPractica>(EquipoPractica.Properties.Practica.Id, practicaId);
        }

        public EntityCollection<Equipo> EquiposReadBySucursales(EntityCollection<SucursalName> sucursales)
        {
            return dalEngine.GetManyByPropertyList<Equipo>(Equipo.Properties.Sucursal.Id, sucursales.GetIds());
        }

        public EntityCollection<EquipoPractica> EquiposReadByPracticas(List<int> practicas)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select eqp from EquipoPractica eqp where eqp.Practica.Id in (:practicas)");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("practicas", practicas);
            return dalEngine.GetManyByQuery<EquipoPractica>(query);
        }

        public EntityCollection<EquipoPractica> EquipoPracticaReadByEquipoId(int equipoId)
        {
            return dalEngine.GetManyByProperty<EquipoPractica>(EquipoPractica.Properties.Equipo.Id, equipoId);
        }

        public EntityCollection<EquipoPracticaDuracionEquipo> EquipoPracticaAndDuracionReadByEquipo(int equipoId)
        {
            EntityCollection<EquipoPractica> practicas = EquipoPracticaReadByEquipoId(equipoId);
            return FillResultado(equipoId, practicas);
        }

        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquiposReadByEnvioDeMensajes()
        {
            return dalEngine.GetManyByProperty<ExcepcionHorarioEquipo>(ExcepcionHorarioEquipo.Properties.EnviaMensajesAHuerfanos, true);
        }

        private EntityCollection<EquipoPracticaDuracionEquipo> FillResultado(int equipoId, EntityCollection<EquipoPractica> practicas)
        {
            EntityCollection<EquipoPracticaDuracionEquipo> resultado = new EntityCollection<EquipoPracticaDuracionEquipo>();
            foreach (EquipoPractica practica in practicas)
            {
                EquipoPracticaDuracion epd = EquipoPracticaDuracionRead(equipoId, practica.Practica.Id);
                EquipoPracticaDuracionEquipo item = new EquipoPracticaDuracionEquipo();
                if (epd == null)
                    item.DuracionEquipo = null;
                else
                    item.DuracionEquipo = epd.Duracion;

                item.DuracionPractica = practica.Practica.Duracion.TotalMinutes;
                item.EquipoId = equipoId;
                item.EquipoPracticaId = practica.Id;
                item.NombrePractica = practica.Practica.Name;
                item.PracticaId = practica.Practica.Id;

                resultado.Add(item);
            }

            return resultado;
        }

        public void EquipoPracticaUpdate(EntityCollection<EquipoPractica> epc)
        {
            dalEngine.UpdateCollection<EquipoPractica>(epc);
        }

        public void EquipoPracticaDelete(EntityCollection<EquipoPractica> epc)
        {
            dalEngine.Delete(epc);
        }
        #endregion

        #region EquipoPracticaDuracion

        /// <summary>
        /// Retorna la duración de una práctica en un equipo
        /// </summary>
        /// <param name="equipoId">Equipo a buscar</param>
        /// <param name="practicaId">Práctica a buscar</param>
        /// <returns>La duración para la práctica en el equipo, si se encuentra, o null en caso contrario</returns>
        public EquipoPracticaDuracion EquipoPracticaDuracionRead(int equipoId, int practicaId)
        {
            ReadManyCommand<EquipoPracticaDuracion> readCmd = new ReadManyCommand<EquipoPracticaDuracion>(dalEngine);

            // se crea el filtro por servicioId
            Filter filter = new Filter();
            filter.Add(EquipoPracticaDuracion.Properties.EquipoId,
                "=", equipoId);

            filter.Add(BooleanOp.And, EquipoPracticaDuracion.Properties.PracticaId,
                "=", practicaId);

            readCmd.Filter = filter;

            EntityCollection<EquipoPracticaDuracion> col = readCmd.Execute();
            if (col.Count == 0)
                return null;
            else
                return col[0];
        }

        /// <summary>
        /// Devuelve todas las relaciones de equipo-practica-duracion.
        /// </summary>
        /// <returns>Las relaciones</returns>
        public EntityCollection<EquipoPracticaDuracion> EquipoPracticaDuracionReadAll()
        {
            ReadManyCommand<EquipoPracticaDuracion> readCmd = new ReadManyCommand<EquipoPracticaDuracion>(dalEngine);

            return readCmd.Execute();
        }

        /// <summary>
        /// Crea o modifica una relación de equipo-practica-duracion.
        /// </summary>
        public void EquipoPracticaDuracionUpdate(EquipoPracticaDuracion epd)
        {
            dalEngine.Update<EquipoPracticaDuracion>(epd);
        }

        #endregion

        #region ExcepcionHorarioEquipo
        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoReadByEquipo(DateTime from, DateTime to, int equipoId)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioEquipo.Properties.Horario.FechaInicio,
                "<", to.AddDays(1.0));

            filter.Add(BooleanOp.And, ExcepcionHorarioEquipo.Properties.Horario.FechaFin,
                ">", from.AddDays(-1.0));

            filter.Add(BooleanOp.And, ExcepcionHorarioEquipo.Properties.EquipoId,
                "=", equipoId);

            ReadManyCommand<ExcepcionHorarioEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioEquipo>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoReadByEquipo(int equipoId, bool soloVigentes)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioEquipo.Properties.EquipoId, "=", equipoId);
            if(soloVigentes)
                filter.Add(BooleanOp.And, ExcepcionHorarioEquipo.Properties.Horario.FechaFin, ">=", enfoke.Time.Now.Date);

            ReadManyCommand<ExcepcionHorarioEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioEquipo>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoReadByEquipos(DateTime from, DateTime to, List<int> equiposId)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioEquipo.Properties.Horario.FechaInicio,
                "<", to.AddDays(1.0));

            filter.Add(BooleanOp.And, ExcepcionHorarioEquipo.Properties.Horario.FechaFin,
                ">", from.AddDays(-1.0));

            filter.Add(BooleanOp.And, ExcepcionHorarioEquipo.Properties.EquipoId,
                "IN", equiposId.ToArray());

            Sort sort = new Sort();
            sort.Add(ExcepcionHorarioEquipo.Properties.EquipoId);
            return dalEngine.GetManyByFilter<ExcepcionHorarioEquipo>(filter, sort);
        }

        /// <summary>
        /// [RQ] Trae la lista de todas las novedades (excepcion-horario-equipo) del equipo
        /// </summary>
        /// <param name="equipoId">El equipo</param>
        /// <returns>La colección de novedades (excepcion-horario-equipo) del equipo</returns>
        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoReadByEquipo(int equipoId)
        {
            return dalEngine.GetManyByProperty<ExcepcionHorarioEquipo>(ExcepcionHorarioEquipo.Properties.EquipoId, equipoId);
        }

        /// <summary>
        /// [RQ] Agrega una colección de (excepcion-horario-equipo) del equipo
        /// </summary>
        /// <param name="ehec">La colección de novedades a agregar</param>
        /// <param name="user">Usuario alta</param>
        /// <returns>La colección de novedades (excepcion-horario-equipo) agregadas</returns>
        [RequiresTransaction]
		public virtual EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoUpdateMany(EntityCollection<ExcepcionHorarioEquipo> ehec)
        {
            // Elimino los que se eliminaron
            ExcepcionHorarioEquipoDelete(ehec.DeletedItems, ehec, enfoke.Time.Now.Date);

            for (int i = 0; i < ehec.Count; i++)
            {
                if (ehec[i].Id == 0)
                {

                    // Inserto
                    ehec[i].Horario = dalEngine.Update<Horario>(ehec[i].Horario);
                    ehec[i] = dalEngine.Update(ehec[i]);

                    // Seteo el Grupo
                    ehec[i].Grupo = ehec[i].Horario.Id;
                    ehec[i] = dalEngine.Update(ehec[i]);
                }
            }

            return ehec;
        }

        /// <summary>
        /// [RQ] Elimina una colección de (excepcion-horario-equipo) del equipo
        /// </summary>
        /// <param name="ehec">La colección de novedades a eliminar</param>
        public void ExcepcionHorarioEquipoDeleteMany(EntityCollection<ExcepcionHorarioEquipo> ehec)
        {
            dalEngine.Delete(ehec);
        }


        private void ExcepcionHorarioEquipoDelete(List<ExcepcionHorarioEquipo> eheDelete, EntityCollection<ExcepcionHorarioEquipo> ehesNuevos, DateTime fecha)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            if (eheDelete.Count > 0)
            {
                List<int> gruposEliminados = new List<int>();
                for (int i = 0; i < eheDelete.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (eheDelete[i].Id > 0)
                    {
                        if (!gruposEliminados.Contains(eheDelete[i].Grupo.Value))
                        {
                            // Recorro todos los MEGs del grupo
                            EntityCollection<ExcepcionHorarioEquipo> ehes = this.ExcepcionHorarioEquipoByGrupo(eheDelete[i].Grupo.Value);

                            /*
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es menor o igual a ayer, no hago nada
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es mayor a ayer, pongo la fecha hasta como ayer
                             * - si la fecha desde es mayor a ayer, lo elimino
                             * */
                            DateTime ayer = fecha.AddDays(-1).Date;
                            for (int j = 0; j < ehes.Count; j++)
                            {
                                ExcepcionHorarioEquipo ehe = ehes[j];

                                if (ehe.FechaInicio.Date <= ayer)
                                {
                                    if (ehe.FechaFin.Date > ayer)
                                    {
                                        ehe.FechaFin = ayer;

                                        if (ehesNuevos == null || !EsModificacion(ehe, ehesNuevos))
                                        {
                                            ehe.UpdateUser = user.Id;


                                            ehe.Horario = dalEngine.Update<Horario>(ehe.Horario);


                                            ehe = dalEngine.Update<ExcepcionHorarioEquipo>(ehe);
                                        }
                                        else
                                        {
                                            dalEngine.Delete(ehe);
                                        }
                                    }
                                }
                                else
                                {
                                    dalEngine.Delete(ehe);
                                }
                            }

                            gruposEliminados.Add(eheDelete[i].Grupo.Value);
                        }
                    }
                }
            }
        }

        private bool EsModificacion(ExcepcionHorarioEquipo originalEnBase, EntityCollection<ExcepcionHorarioEquipo> nuevosAInsertar)
        {
            bool modificacion = false;
            for (int i = 0; !modificacion && i < nuevosAInsertar.Count; i++)
            {
                if (nuevosAInsertar[i].Dias == originalEnBase.Dias &&
                    nuevosAInsertar[i].EquipoId == originalEnBase.EquipoId &&
                    nuevosAInsertar[i].FechaInicio == originalEnBase.FechaInicio &&
                    nuevosAInsertar[i].FrecuenciaSemanal == originalEnBase.FrecuenciaSemanal &&
                    nuevosAInsertar[i].HoraInicio == originalEnBase.HoraInicio)
                    modificacion = true;
            }

            return modificacion;
        }

        private EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoByGrupo(int grupo)
        {
            ReadManyCommand<ExcepcionHorarioEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioEquipo>(dalEngine);
            Filter filter = new Filter();

            // filtro por horarios con el mismo grupo
            filter.Add(BooleanOp.And,
                ExcepcionHorarioEquipo.Properties.Horario.Grupo,
                "=", grupo);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(ExcepcionHorarioEquipo.Properties.Id, SortingDirection.Desc);
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        #endregion

        #region Equipo Actualizacion
        /// <summary>
        /// [GG]Actualizacion para la pantalla de tecnico
        /// </summary>
        /// <param name="equipoTag"></param>
        /// <returns></returns>
        public EquipoActualizacion EquipoActualizacionReadByEquipo(int equipo)
        {
            return dalEngine.GetByProperty<EquipoActualizacion>(EquipoActualizacion.Properties.EquipoId, equipo);
        }

        public EquipoActualizacion EquipoActualizacionReadByServicio(int servicioId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select ea from EquipoActualizacion ea, Equipo equ where equ.Id = ea.EquipoId and equ.Servicio.Id = :servicioId order by ea.Fecha desc ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("servicioId", servicioId);
            EntityCollection<EquipoActualizacion> equiposActualizacion = dalEngine.GetManyByQuery<EquipoActualizacion>(query);
            if (equiposActualizacion != null && equiposActualizacion.Count > 0)
                return equiposActualizacion[0];
            else
                return null;
        }

        /// <summary>
        /// [GG]Actualizacion para la pantalla de tecnico
        /// </summary>
        [Private]
        public EquipoActualizacion EquipoActualizacionUpdate(EquipoActualizacion eqa)
        {
            if (!(eqa.Id > 0))
            {
                EquipoActualizacion eqaInternal = EquipoActualizacionReadByEquipo(eqa.EquipoId);

                if (eqaInternal != null && eqaInternal.Id > 0)
                {
                    eqa.Id = eqaInternal.Id;
                }
            }


            return dalEngine.Update<EquipoActualizacion>(eqa);
        }
        #endregion

        public EntityCollection<EquipoPracticaDuracion> EquiposPracticaDuracionRead(List<int> equiposId, List<int> practicasId)
        {
            // se crea el filtro por servicioId
            Filter filter = new Filter();
            filter.Add(EquipoPracticaDuracion.Properties.EquipoId,
                "IN", equiposId);
            filter.Add(BooleanOp.And, EquipoPracticaDuracion.Properties.PracticaId,
                "IN", practicasId);

            return dalEngine.GetManyByFilter<EquipoPracticaDuracion>(filter);
        }

        public EntityCollection<InformeDeSMS> InformesSMSReadByNovedadDeEquipo(EntityCollection<Turno> turnosAfectados, List<int> centrosIds, List<int> serviciosIds)
        {
            if (centrosIds.Count == 0 || serviciosIds.Count == 0)
                return new EntityCollection<InformeDeSMS>();

            EntityCollection<Paciente> pacientes = this.ObtenerPacientesDeTurnosAfectados(turnosAfectados);
            return this.ConstruirInformes(turnosAfectados, pacientes);
        }

        //EquipoName
        public List<int> EquipoIdsReadByServicioIdAndSucursalId(int? servicioId, List<int> centrosIds, bool mostrarEliminados)
        {
            bool primero = true;
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct equ.Id from Equipo equ ");

            if (!mostrarEliminados)
            {
                hqlBuilder.Append(" where equ.Deleted = false ");
                primero = false;
            }

            if (servicioId.HasValue)
            {
                hqlBuilder.Append(primero ? "where " : "and ").Append("  equ.Servicio.Id = :servicio ");
                primero = false;
            }

            if (centrosIds.Count > 0)
            {
                if(centrosIds.Count == 1)
                    hqlBuilder.Append(primero ? "where " : "and ").Append(" equ.Sucursal.Id = :sucursal ");
                else
                    hqlBuilder.Append(primero ? "where " : "and ").Append(" equ.Sucursal.Id in (:sucursal) ");
            }
               

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (servicioId.HasValue)
                query.SetInt32("servicio", servicioId.Value);
            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    query.SetInt32("sucursal", centrosIds[0]);
                else
                    query.SetParameterList("sucursal", centrosIds);
            }

            return (List<int>)query.List<int>();
        }

        //Worklist
        public EntityCollection<WorklistEquipo> GetWorklistEquipoByWorklistId(int worklistId)
        {
            return Context.Session.Dalc.GetManyByProperty<WorklistEquipo>(WorklistEquipo.Properties.Worklist.Id, worklistId);
        }

        [AnonymousMethod]
        public EntityCollection<Worklist> WorklistReadAllWithObjects()
        {
            EntityCollection<Worklist> ret = dalEngine.GetAll<Worklist>();
            SortedMultipartData<WorklistEquipo, int> relaciones = new SortedMultipartData<WorklistEquipo, int>(WorklistEquipo.Properties.Worklist.Id);
            relaciones.Add(dalEngine.GetAll<WorklistEquipo>(WorklistEquipo.Properties.Worklist));
            foreach (Worklist list in ret)
            {
                foreach (WorklistEquipo we in relaciones.GetManyBySorted(list.Id))
                    list.Equipos.Add(we.Equipo);
            }
            ret.SortByProperty(Worklist.Properties.DisplayName);
            return ret;
        }
        [RequiresTransaction]
		public virtual void WorklistDelete(Worklist worklist)
        {
            WorklistEquipoDeleteByWorklist(worklist);
            dalEngine.Delete(worklist);
        }

        private void WorklistEquipoDeleteByWorklist(Worklist worklist)
        {
            EntityCollection<WorklistEquipo> relaciones = dalEngine.GetManyByProperty<WorklistEquipo>(WorklistEquipo.Properties.Worklist, worklist);
            dalEngine.Delete(relaciones);
        }

        [RequiresTransaction]
		  public virtual void WorklistUpdate(Worklist worklist)
        {
            // Graba o actualiza el item
            dalEngine.Update(worklist);
            // Borra las relaciones existentes
            WorklistEquipoDeleteByWorklist(worklist);
            // Graba las actuales
            EntityCollection<WorklistEquipo> weCollection = new EntityCollection<WorklistEquipo>();
            foreach (EquipoName equipo in worklist.Equipos)
            {
                WorklistEquipo we = new WorklistEquipo();
                we.Equipo = equipo;
                we.Worklist = worklist;
                weCollection.Add(we);
            }
            Context.Session.Dalc.UpdateCollection<WorklistEquipo>(weCollection);
            // Listo
        }

        [Private]
        public EntityCollection<EquipoCupo> EquipoCupoReadByEquipos(DateTime from, DateTime to, IEnumerable<int> equipoIds)
        {
            Filter filter = new Filter();
            filter.Add(EquipoCupo.Properties.Equipo.Id, "IN", equipoIds);
            filter.Add(BooleanOp.And, EquipoCupo.Properties.Deleted, "=", false);
            filter.Add(BooleanOp.And, EquipoCupo.Properties.Horario.FechaInicio,
                                          "<", to.AddDays(1.0));
            filter.Add(BooleanOp.And, EquipoCupo.Properties.Horario.FechaFin,
                                            ">", from.AddDays(-1.0));
            Sort sort = new Sort();
            sort.Add(EquipoCupo.Properties.Equipo.Id);
            return dalEngine.GetManyByFilter<EquipoCupo>(filter, sort);
        }

        public EntityCollection<EquipoPractica> EquipoPracticaReadByMedicoIdPracticaNameAndPracticaCode(int equipoId, string practicaName, string practicaCode)
        {
            string hql = "SELECT DISTINCT ep FROM EquipoPractica ep " +
                         "WHERE ep.Equipo.Id = :idEquipo ";

            if (!String.IsNullOrEmpty(practicaName))
                hql += "AND ep.Practica.Name LIKE :practica ";

            if (!String.IsNullOrEmpty(practicaCode))
                hql += "AND ep.Practica.Code LIKE :code ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idEquipo", equipoId);
            if (!String.IsNullOrEmpty(practicaName))
                query.SetParameter("practica", practicaName.Trim().Replace(' ', '%').ToUpper() + "%");
            if (!String.IsNullOrEmpty(practicaCode))
                query.SetParameter("code", practicaCode.Trim().Replace(' ', '%').ToUpper() + "%");

            return dalEngine.GetManyByQuery<EquipoPractica>(query);
        }

        public bool EquipoTieneTurnosNoCancelados(int equipoId)
        {
            string hql = "SELECT count(tur.Id) FROM Turno tur, EstadoTurno est WHERE tur.EstadoTurnoID = est.Id AND est.Cancelado = false AND tur.EquipoId = :equipoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipoId);
            object ret = query.UniqueResult();

            if (ret != null)
                return (int.Parse(ret.ToString()) > 0);

            return false;
        }

        public bool EquipoTienePracticasRelacionadas(int equipoId)
        {
            string hql = "SELECT count(ep.Id) FROM EquipoPractica ep WHERE ep.Practica.Deleted = false AND ep.Equipo.Id = :equipoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipoId);
            object ret = query.UniqueResult();

            if (ret != null)
                return (int.Parse(ret.ToString()) > 0);

            return false;
        }

        public EntityCollection<ExcepcionHorarioEquipo> ExcepcionHorarioEquipoReadByEquipo(List<int> eehIds)
        {
            Filter filter = new Filter();
            filter.Add(ExcepcionHorarioEquipo.Properties.Id, "IN", eehIds);
            return dalEngine.GetManyByFilter<ExcepcionHorarioEquipo>(filter);
        }
    }
}

