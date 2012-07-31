using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using enfoke.AOP;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.DisconnectedSupport;
using enfoke.Data.Filters;
using enfoke.Data.Reference;
using enfoke.Eges.Auditoria;
using enfoke.Eges.DICOM;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Result;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistance;
using enfoke.Eges.Persistence;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Eges.HL7.Client;

namespace enfoke.Eges.Data
{
    public class TurnosDalc : Dalc, IService
    {
        protected TurnosDalc(NotConstructable dummy) : base(dummy) { }

        public MedicoEquipoInterfaztxtDM MedicoEquipoInterfaztxtDMReadByMedicoIdEquipoId(int medicoId, int equipoId)
        {
            MedicoEquipoInterfaztxtDM medEquipoInterfaz = (from med in dalEngine.Query<MedicoEquipoInterfaztxtDM>()
                                                           where med.MedicoId == medicoId
                                                           && med.EquipoId == equipoId
                                                           select med).FirstOrDefault<MedicoEquipoInterfaztxtDM>();
            return medEquipoInterfaz;
        }
        // SeguimientoTurno

        [Private]
        public EntityCollection<SeguimientoAccion> SeguimientoAccionReadAllCollection()
        {
            return dalEngine.GetAll<SeguimientoAccion>();
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaReadByTurno(int turnoId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select plp from PlanPracticaPrecio as plp ");
            hql.Append("where plp.Id in (select valItem.PlanPracticaUsadoId from ValorizacionItem as valItem where valItem.Valorizacion.Deleted = false ");
            hql.Append("and valItem.Cantidad > 0 and valItem.Valorizacion.Turno.Id = ").Append(turnoId.ToString()).Append(")");
            IQuery query = dalEngine.CreateQuery(hql.ToString());

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        [Private]
        public EntityCollection<SeguimientoMotivo> SeguimientoMotivoReadAllCollection()
        {
            return dalEngine.GetAll<SeguimientoMotivo>();
        }

        [Private]
        public EntityCollection<SeguimientoTurnoView> SeguimientoTurnoViewRead(TipoSeguimientoEnum tipoSeguimiento, DateTime? fechaDesde,
            DateTime? fechaHasta,
            string paciente,
            string servicio,
            string medico,
            string practica,
            string obraSocial,
            List<int> centrosIds,
            string equipo,
            int? accion,
            bool cerrados,
            int maxRows)
        {
            Filter filter = new Filter();
            IPropertyReference propiedadFiltro = null;
            IPropertyReference propiedadImportancia = null;

            if (fechaDesde.HasValue)
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.FechaTurno, ">=", fechaDesde.Value);
            }

            if (fechaHasta.HasValue)
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.FechaTurno, "<", fechaHasta.Value.AddDays(1));
            }

            if (!cerrados)
            {
                List<int> estados = new List<int>() { (int)EstadoTurnoEnum.Reservado, (int)EstadoTurnoEnum.Ausente, (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.ARecitar, (int)EstadoTurnoEnum.RecitadoPendiente };
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.FechaAccionSiguiente, " not is ", null);
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.EstadoId, " in ", estados);
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                propiedadFiltro = SeguimientoTurnoView.Properties.PacienteApellidoNombre;
                propiedadImportancia = SeguimientoTurnoView.Properties.PacienteImportancia;

                if (paciente.Contains(" "))
                {
                    string apellido = paciente.Split(' ')[0];
                    string nombre = paciente.Split(' ')[1];
                    paciente = paciente.Replace(' ', '%') + "%";
                }

                Privacy.AddConfidentialFilter(paciente, filter, propiedadFiltro, propiedadImportancia, BooleanOp.And);
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.ServicioDescripcion, "LIKE", servicio.Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(medico))
            {
                if (medico.Contains(" "))
                {
                    string apellido = medico.Split(' ')[0];
                    string nombre = medico.Split(' ')[1];
                    filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.MedicoApellido, "LIKE", apellido + "%");
                    filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.MedicoNombre, "LIKE", nombre + "%");
                }
                else
                {
                    filter.Add(new OpenParenthesis(BooleanOp.And));
                    filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.MedicoApellido, "LIKE", medico + "%");
                    filter.Add(BooleanOp.Or, SeguimientoTurnoView.Properties.MedicoNombre, "LIKE", medico + "%");
                    filter.Add(new CloseParenthesis());
                }
            }

            if (!String.IsNullOrEmpty(practica))
            {
                propiedadFiltro = SeguimientoTurnoView.Properties.PracticaDescripcion;
                propiedadImportancia = SeguimientoTurnoView.Properties.PacienteImportancia;
                Privacy.AddConfidentialFilter(practica.Replace(" ", "%") + "%", filter, propiedadFiltro, propiedadImportancia, BooleanOp.And);
            }

            if (!String.IsNullOrEmpty(obraSocial))
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.ObraSocialNombre, "LIKE", obraSocial.Replace(" ", "%") + "%");
            }

            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.SucursalId, "=", centrosIds[0]);
                else
                    filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.SucursalId, "in", centrosIds);

            }

            if (!String.IsNullOrEmpty(equipo))
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.EquipoDescripcion, "LIKE", equipo.Replace(" ", "%") + "%");
            }

            if (accion.HasValue && accion > 0)
            {
                filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.SeguimientoAccionSiguiente.Id, "=", accion);
            }

            filter.Add(BooleanOp.And, SeguimientoTurnoView.Properties.TipoSeguimiento.Id, "=", (int)tipoSeguimiento);

            Sort sort = new Sort();
            sort.Add(SeguimientoTurnoView.Properties.FechaAccionSiguiente, SortingDirection.Asc);
            sort.Add(SeguimientoTurnoView.Properties.FechaAccionUltima, SortingDirection.Desc);

            ReadManyCommand<SeguimientoTurnoView> readCmd = new ReadManyCommand<SeguimientoTurnoView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            EntityCollection<SeguimientoTurnoView> turnos = readCmd.Execute();
            turnos.SortByProperty(SeguimientoTurnoView.Properties.FechaTurno);
            return turnos;
        }

        public SeguimientoTurno SeguimientoTurnoReadByIdTurno(int turnoId, TipoSeguimientoEnum seguimiento)
        {
            ReadManyCommand<SeguimientoTurno> readCmd = new ReadManyCommand<SeguimientoTurno>(dalEngine);

            Filter filter = new Filter();
            filter.Add(SeguimientoTurno.Properties.Turno.Id, "=", turnoId);
            filter.Add(BooleanOp.And, SeguimientoTurno.Properties.TipoSeguimiento.Id, "=", (int)seguimiento);

            readCmd.Filter = filter;

            EntityCollection<SeguimientoTurno> sts = readCmd.Execute();

            if (sts == null || sts.Count == 0)
            {
                return null;
            }

            return sts[0];
        }

        private SeguimientoAccion ObtenerSeguimientoAccion(int id)
        {
            return dalEngine.GetById<SeguimientoAccion>(id);
        }



        /// <summary>
        /// Devuelve los turnos disponibles
        /// </summary>
        /// <param name="date">Día desde donde buscar el turno</param>
        /// <returns>Turnos del dia solicitado</returns>
        public EntityCollection<Turno> TurnosReadByFechas(DateTime fechaDesde, DateTime fechaHasta)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            // se crea el filtro por la fecha, quitando la hora
            filter.Add(Turno.Properties.Fecha, ">=", fechaDesde.Date);
            filter.Add(BooleanOp.And, Turno.Properties.Fecha, "<", fechaHasta.Date.AddDays(1));
            filter.Add(BooleanOp.And, Turno.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "IN", new int[] { (int)EstadoTurnoEnum.Reservado });
            filter.Add(BooleanOp.And, Turno.Properties.EsHuerfano, "=", false);
            filter.Add(BooleanOp.And, Turno.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.Normal);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<Turno> TurnosControlDiarioReadByFechas(DateTime fechaDesde, DateTime fechaHasta)
        {
            Filter filter = new Filter();
            // se crea el filtro por la fecha, quitando la hora
            filter.Add(Turno.Properties.FechaControlDiario, ">=", fechaDesde.Date);
            filter.Add(BooleanOp.And, Turno.Properties.FechaControlDiario, "<", fechaHasta.Date.AddDays(1));
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);
            return dalEngine.GetManyByFilter<Turno>(filter);
        }

        public EntityCollection<InformeDeSMS> InformeDeSMSReadByRecordatorio(int diasDeAnticipacion, List<int> centrosIds, List<int> serviciosIds, string mensaje, DateTime proximaEjecucion)
        {
            if (centrosIds.Count == 0 || serviciosIds.Count == 0)
                return new EntityCollection<InformeDeSMS>();

            EntityCollection<Turno> turnosAfectados = this.ObtenerTurnosAfectados(diasDeAnticipacion, centrosIds, serviciosIds, (int)(proximaEjecucion.Date - enfoke.Time.Now.Date).TotalDays);
            EntityCollection<Paciente> pacientes = this.ObtenerPacientesDeTurnosAfectados(turnosAfectados);
            return this.ConstruirInformes(turnosAfectados, pacientes, mensaje);
        }

        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnosIdsOrdenadasPorTipo(List<int> ids)
        {
            if (ids.Count <= 0)
                return new EntityCollection<PracticaTurno>();

            Filter filter = new Filter();
            filter.Add(PracticaTurno.Properties.TurnoId, "IN", ids);
            Sort sort = new Sort();
            sort.Add(PracticaTurno.Properties.Tipo, SortingDirection.Asc);
            return dalEngine.GetManyByFilter<PracticaTurno>(filter, sort);
        }

        public EntityCollection<TurnoLight> TurnoLightReadByFechas(DateTime? fechaDesde, DateTime? fechaHasta)
        {
            ReadManyCommand<TurnoLight> readCmd = new ReadManyCommand<TurnoLight>(dalEngine);
            Filter filter = new Filter();
            if (!fechaDesde.HasValue && !fechaHasta.HasValue)
                throw new enfokeDataException("La consulta requiere al menos 1 filtro de fecha.");

            if (fechaDesde.HasValue)
                filter.Add(TurnoLight.Properties.Fecha, ">=", fechaDesde.Value.Date);
            if (fechaHasta.HasValue)
                filter.Add(BooleanOp.And, TurnoLight.Properties.Fecha, "<", fechaHasta.Value.Date.AddDays(1));

            filter.Add(BooleanOp.And, TurnoLight.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, TurnoLight.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, TurnoLight.Properties.EstadoTurnoID, "NOT IN", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<Paciente> PacientesReadByPhoneNumber(List<string> phones)
        {
            if (phones == null || phones.Count == 0)
                return new EntityCollection<Paciente>();

            SQLBlockBuilder<string> phonesBlock = new SQLBlockBuilder<string>(phones);
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select pac from Paciente pac ");
            hqlBuilder.Append("where ");
            hqlBuilder.AppendFormat(phonesBlock.BuildConstrainBlock("{0}"), SQLPortable.StringConcat("pac.CodigoArea", "pac.TelefonoMovil"));
            hqlBuilder.Append(" and pac.NumeroValido = true");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<Paciente>(query);
        }

        public EntityCollection<Paciente> PacienteReadByTelefonoAndCodigoDeArea(string telefono, int codigoPais, int codigoArea, int pacienteExcluidoId)
        {
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, Paciente.Properties.TelefonoMovil, "=", telefono);
            filter.Add(BooleanOp.And, Paciente.Properties.CodigoPais, "=", codigoPais);
            filter.Add(BooleanOp.And, Paciente.Properties.CodigoArea, "=", codigoArea);
            filter.Add(BooleanOp.And, Paciente.Properties.Id, "<>", pacienteExcluidoId);
            filter.Add(BooleanOp.And, Paciente.Properties.NumeroValido, "=", true);
            return dalEngine.GetManyByFilter<Paciente>(filter);
        }

        public EntityCollection<InformeDeSMS> InformeDeSMSReadByConfirmacion(int diasDeAnticipacion, List<int> centrosIds, List<int> serviciosIds, string mensaje, DateTime proximaEjecucion)
        {
            if (centrosIds.Count == 0 || serviciosIds.Count == 0)
                return new EntityCollection<InformeDeSMS>();

            EntityCollection<Turno> turnosAfectados = this.ObtenerTurnosParaConfirmacion(diasDeAnticipacion, centrosIds, serviciosIds, (int)(proximaEjecucion.Date - enfoke.Time.Now.Date).TotalDays);
            EntityCollection<Paciente> pacientes = this.ObtenerPacientesDeTurnosAfectados(turnosAfectados);
            return this.ConstruirInformes(turnosAfectados, pacientes, mensaje);
        }

        public EntityCollection<Turno> TurnosReadByConfirmacionAndFechaLogOrReprogramado(int logId, DateTime fechaLog)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tur from Turno tur, MensajeLogTurno mlt, TurnoUpdateTipoConfirmacion ttc ");
            hqlBuilder.Append("where tur.Id = mlt.TurnoId and ");
            hqlBuilder.Append("mlt.MensajeLogId = :logId and ");
            hqlBuilder.Append("tur.Id = ttc.Id and ");

            // que el turno este en estado a confirmar
            hqlBuilder.Append("(tur.TipoConfirmacionID = :aConfirmar or ");
            // que haya sido reprogramado para el mismo día (fuera del query se valida que no se hayan modificado las practicas)
            hqlBuilder.Append("(tur.TipoTurnoId = :reprogramado and ");
            hqlBuilder.Append("(select count(orig.Id) from Turno orig where orig.TurnoOriginalID = tur.Id) > 0)) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("reprogramado", (int)TipoTurnoEnum.Reprogramado);
            query.SetInt32("aConfirmar", (int)TipoConfirmacionEnum.AConfirmar);
            query.SetInt32("logId", logId);
            EntityCollection<Turno> posiblesAfectados = dalEngine.GetManyByQuery<Turno>(query);
            return posiblesAfectados;
        }

        private EntityCollection<InformeDeSMS> ConstruirInformes(EntityCollection<Turno> turnosAfectados, EntityCollection<Paciente> pacientes, string mensaje)
        {
            EntityCollection<InformeDeSMS> response = new EntityCollection<InformeDeSMS>();
            EquiposDalc equipoDalc = Context.Session.EquiposDalc;
            SortByShitDate(turnosAfectados);
            List<Equipo> equipoCache = new List<Equipo>();
            List<Sucursal> sucursalCache = new List<Sucursal>();
            foreach (Paciente paciente in pacientes)
            {
                List<Turno> turnos = new List<Turno>(this.TurnosPorPaciente(turnosAfectados, paciente));
                InformeDeSMS informe = new InformeDeSMS(paciente);
                informe.TurnosAfectados = turnos;
                Equipo equipo = equipoCache.Find(delegate(Equipo equ) { return equ.Id == turnos[0].EquipoId.Value; });
                if (equipo == null)
                {
                    equipo = equipoDalc.EquipoReadById(turnos[0].EquipoId.Value);
                    equipoCache.Add(equipo);
                }

                Sucursal sucursal = sucursalCache.Find(delegate(Sucursal suc) { return suc.Id == equipo.Sucursal.Id; });
                if (sucursal == null)
                {
                    sucursal = dalEngine.GetById<Sucursal>(equipo.Sucursal.Id);
                    sucursalCache.Add(sucursal);
                }

                informe.Servicio = equipo.Servicio;
                informe.DireccionDeCentro = sucursal.Domicilio;
                informe.Centro = equipo.Sucursal;
                informe.FechaPrimerTurno = turnos[0].Fecha;
                informe.Mensaje = mensaje;
                response.Add(informe);
            }
            return response;
        }

        internal void SortByShitDate(EntityCollection<Turno> turnosAfectados)
        {
            if (turnosAfectados == null)
                return;

            turnosAfectados.Sort(delegate(Turno left, Turno right)
            {
                return left.Fecha.GetValueOrDefault(DateTime.MinValue).CompareTo(right.Fecha.GetValueOrDefault(DateTime.MinValue));
            });
        }

        private EntityCollection<Paciente> ObtenerPacientesDeTurnosAfectados(EntityCollection<Turno> turnosAfectados)
        {
            if (turnosAfectados.Count == 0)
                return new EntityCollection<Paciente>();

            List<IIdentificable> turnos = new List<IIdentificable>(turnosAfectados.Count);
            foreach (Turno turno in turnosAfectados)
                turnos.Add(turno);

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

        private EntityCollection<Turno> ObtenerTurnosParaConfirmacion(int diasDeAnticipacion, List<int> centrosIds, List<int> serviciosIds, int diasParaProximaEjecucion)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tur from Turno tur, Paciente pac, Equipo equ, Sucursal suc ");
            hqlBuilder.Append("where tur.Fecha >= :fechaAnticipadaDesde and ");
            hqlBuilder.Append("tur.Fecha < :fechaAnticipadaHasta and ");
            hqlBuilder.Append("tur.EquipoId = equ.Id and ");
            hqlBuilder.Append("equ.Sucursal.id = suc.Id and ");
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id and ");
            hqlBuilder.Append("tur.TipoConfirmacionID = :aConfirmar and ");
            hqlBuilder.Append("pac.NumeroValido = true and ");
            hqlBuilder.Append("tur.EstadoTurnoID = :estadoReservado and ");
            hqlBuilder.Append("equ.Servicio.Id in (:servicios) and ");
            hqlBuilder.Append("equ.Sucursal.Id in (:sucursales) and ");
            hqlBuilder.Append("tur.EsHuerfano = false and ");
            hqlBuilder.Append("(pac.UltimoAvisoEnviado is null or pac.UltimoAvisoEnviado <> :fechaActual) ");
            hqlBuilder.Append("order by tur.Fecha desc");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetDateTime("fechaAnticipadaDesde", enfoke.Time.Now.Date.AddDays(diasDeAnticipacion));
            query.SetDateTime("fechaAnticipadaHasta", enfoke.Time.Now.Date.AddDays(diasDeAnticipacion).AddDays(diasParaProximaEjecucion));
            query.SetDateTime("fechaActual", enfoke.Time.Now.Date);
            query.SetParameterList("servicios", serviciosIds);
            query.SetParameterList("sucursales", centrosIds);
            query.SetInt32("aConfirmar", (int)TipoConfirmacionEnum.AConfirmar);
            query.SetInt32("estadoReservado", (int)EstadoTurnoEnum.Reservado);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        private EntityCollection<Turno> ObtenerTurnosAfectados(int diasDeAnticipacion, List<int> centrosIds, List<int> serviciosIds, int diasParaProximaEjecucion)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tur from Turno tur, Paciente pac, Equipo equ, Sucursal suc ");
            hqlBuilder.Append("where tur.Fecha >= :fechaAnticipadaDesde and ");
            hqlBuilder.Append("equ.Sucursal.id = suc.Id and ");
            hqlBuilder.Append("tur.Fecha < :fechaAnticipadaHasta and ");
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id and ");
            hqlBuilder.Append("pac.NumeroValido = true and ");
            hqlBuilder.Append("pac.CodigoPais is not null and ");
            hqlBuilder.Append("tur.EquipoId = equ.Id and ");
            hqlBuilder.Append("tur.EstadoTurnoID = :estadoReservado and ");
            hqlBuilder.Append("equ.Servicio.Id in (:servicios) and ");
            hqlBuilder.Append("equ.Sucursal.Id in (:sucursales) ");
            hqlBuilder.Append("order by tur.Fecha desc");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetDateTime("fechaAnticipadaDesde", enfoke.Time.Now.Date.AddDays(diasDeAnticipacion));
            query.SetDateTime("fechaAnticipadaHasta", enfoke.Time.Now.Date.AddDays(diasDeAnticipacion).AddDays(diasParaProximaEjecucion));
            query.SetParameterList("servicios", serviciosIds);
            query.SetParameterList("sucursales", centrosIds);
            query.SetInt32("estadoReservado", (int)EstadoTurnoEnum.Reservado);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        private IEnumerable<Turno> TurnosPorPaciente(EntityCollection<Turno> turnosAfectados, Paciente paciente)
        {
            return turnosAfectados.FindAll(delegate(Turno turno) { return turno.Orden.PacienteId == paciente.Id; });
        }

        public EntityCollection<MensajeLog> MensajeLogReadByFilters(string paciente, SucursalName centro, Servicio servicio, DateTime fecha, int servicioMensajeriaId, List<int> estados)
        {
            EntityCollection<MensajeLog> response = this.ObtenerLogsConRespuesta(paciente, centro, servicio, ref fecha, servicioMensajeriaId, estados);
            response.AddRange(this.ObtenerLogsSinRespuesta(paciente, centro, servicio, ref fecha, servicioMensajeriaId, estados));
            return response;
        }

        private EntityCollection<MensajeLog> ObtenerLogsSinRespuesta(string paciente, SucursalName centro, Servicio servicio, ref DateTime fecha, int servicioMensajeriaId, List<int> estados)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.MensajeLog(men.Id, pac, suc, ser, men.FechaEnvio, men.Estado, men.EstadoDescripcion, bat, men.TurnosEnMensaje, men.TurnosAfectados) ");
            this.SelectComunMensajeLog(paciente, centro, servicio, hqlBuilder, servicioMensajeriaId, estados);
            hqlBuilder.Append(" and men.Respuesta is null and men.EsRespuesta = false ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            this.ParametrosComunesMensajeLog(paciente, centro, servicio, fecha, query, servicioMensajeriaId, estados);
            return dalEngine.GetManyByQuery<MensajeLog>(query);
        }

        private EntityCollection<MensajeLog> ObtenerLogsConRespuesta(string paciente, SucursalName centro, Servicio servicio, ref DateTime fecha, int servicioMensajeriaId, List<int> estados)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.MensajeLog(men.Id, pac, suc, ser, men.FechaEnvio, men.Respuesta, men.Estado, men.EstadoDescripcion, bat, men.TurnosEnMensaje, men.TurnosAfectados) ");
            this.SelectComunMensajeLog(paciente, centro, servicio, hqlBuilder, servicioMensajeriaId, estados);
            hqlBuilder.Append(" and men.Respuesta is not null ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            this.ParametrosComunesMensajeLog(paciente, centro, servicio, fecha, query, servicioMensajeriaId, estados);
            return dalEngine.GetManyByQuery<MensajeLog>(query);
        }

        private void ParametrosComunesMensajeLog(string paciente, SucursalName centro, Servicio servicio, DateTime fecha, IQuery query, int servicioMensajeriaId, List<int> estados)
        {
            if (!string.IsNullOrEmpty(paciente))
                query.SetString("paciente", "%" + paciente.Replace(' ', '%') + "%");

            if (centro != null)
                query.SetInt32("sucId", centro.Id);

            if (servicio != null)
                query.SetInt32("serId", servicio.Id);

            query.SetDateTime("fechaEnvioDesde", fecha.Date);
            query.SetDateTime("fechaEnvioHasta", fecha.Date.AddDays(1));
            query.SetInt32("servicioId", servicioMensajeriaId);
            if (estados.Count > 0)
                query.SetParameterList("estados", estados);
        }

        private void SelectComunMensajeLog(string paciente, SucursalName centro, Servicio servicio, StringBuilder hqlBuilder, int servicioMensajeriaId, IList<int> estados)
        {
            hqlBuilder.Append("from MensajeLog men join men.Batch bat, Paciente pac, Servicio ser, SucursalName suc  ");
            hqlBuilder.Append("where men.PacienteId = pac.Id and ");
            hqlBuilder.Append("men.CentroId = suc.Id and ");
            hqlBuilder.Append("men.ServicioId = ser.Id and ");
            if (!string.IsNullOrEmpty(paciente))
                hqlBuilder.Append("pac.ApellidoNombre like :paciente and ");

            if (centro != null)
                hqlBuilder.Append("suc.Id = :sucId and ");

            if (servicio != null)
                hqlBuilder.Append("ser.Id = :serId  and ");

            hqlBuilder.Append("men.FechaEnvio >= :fechaEnvioDesde and men.FechaEnvio < :fechaEnvioHasta and ");
            hqlBuilder.Append("men.Batch.ServicioMensajeria.Id = :servicioId");
            if (estados.Count > 0)
                hqlBuilder.Append(" and men.Estado in (:estados) ");
        }

        public EntityCollection<TurnoExpirado> TurnosReadByExpiracionDeAlertaPorSucursalYTurno(int? sucursalId, int? servicioId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.Result.TurnoExpirado (tur.Fecha, tlog.RecepcionFecha, pac.ApellidoNombre, equ.Descripcion, equ.Servicio.Name, equ.Sucursal.Name, tur.Id) ");
            hqlBuilder.Append("from Turno as tur, TurnoLog as tlog, Equipo as equ, Paciente as pac where tur.EstadoTurnoID = :turno ");
            hqlBuilder.Append("and tur.Id = tlog.TurnoId ");
            hqlBuilder.Append("and tlog.InicioPracticaFecha is null ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            string conversionDeMinutos = SQLPortable.DateAddMinutes("tur.Fecha", "equ.ToleranciaRetraso");
            hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
            hqlBuilder.Append("and :fechaHasta >= " + conversionDeMinutos + " ");
            hqlBuilder.Append("and equ.ControlaRetraso = true ");
            if (sucursalId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :sucursal ");

            if (servicioId.HasValue)
                hqlBuilder.Append("and equ.Servicio.Id = :servicio ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("turno", (int)EstadoTurnoEnum.Recepcionado);
            query.SetDateTime("fechaHasta", enfoke.Time.Now);
            query.SetDateTime("fechaDesde", enfoke.Time.Now.AddHours(-12));
            if (sucursalId.HasValue)
                query.SetInt32("sucursal", sucursalId.Value);

            if (servicioId.HasValue)
                query.SetInt32("servicio", servicioId.Value);

            EntityCollection<TurnoExpirado> results = dalEngine.GetManyByQuery<TurnoExpirado>(query);
            PracticasDalc practicasDalc = Context.Session.PracticasDalc;
            foreach (TurnoExpirado turnoExpirado in results)
            {
                PracticaTurno praticaTurno = PracticaTurnoReadByTurno(turnoExpirado.Id, PracticaTurnoTipoEnum.Principal)[0];
                turnoExpirado.PracticaPrincipal = praticaTurno.Practica.Name;
                turnoExpirado.Retraso = (int)(enfoke.Time.Now - turnoExpirado.Turno.Value).TotalMinutes;
            }

            return results;
        }

        public EntityCollection<Turno> TurnosReadByExpiracionDeAlerta()
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select tur from Turno tur, TurnoLog tlog, Equipo equ where tur.EstadoTurnoID = :turno ");
            hqlBuilder.Append("and tur.Id = tlog.TurnoId ");
            hqlBuilder.Append("and tlog.InicioPracticaFecha is null ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            string conversionDeMinutos = SQLPortable.DateAddMinutes("tur.Fecha", "equ.ToleranciaRetraso");
            hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
            hqlBuilder.Append("and :fechaHasta >= " + conversionDeMinutos + " ");
            hqlBuilder.Append("and equ.ControlaRetraso = true ");
            hqlBuilder.Append("and not exists(select t.Id from Turno t where t.TurnoOriginalID = tur.Id) ");


            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("turno", (int)EstadoTurnoEnum.Recepcionado);
            query.SetDateTime("fechaHasta", enfoke.Time.Now);
            query.SetDateTime("fechaDesde", enfoke.Time.Now.AddHours(-12));
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public SeguimientoTurno SeguimientoTurnoUpdateGlobal(int turnoId, int? accionActualId, int? accionSiguienteId, DateTime? fechaSiguiente, string observaciones, TipoSeguimientoEnum tipoSeguimiento)
        {
            List<int> turnosId = new List<int>();
            turnosId.Add(turnoId);

            return SeguimientoTurnoUpdateGlobal(turnosId, accionActualId, accionSiguienteId, fechaSiguiente, observaciones, tipoSeguimiento, string.Empty)[0];
        }


        public SeguimientoTurno SeguimientoTurnoUpdateGlobal(List<int> turnosId, int? accionActualId, int? accionSiguienteId, DateTime? fechaSiguiente, string observaciones, TipoSeguimientoEnum tipoSeguimiento)
        {
            return SeguimientoTurnoUpdateGlobal(turnosId, accionActualId, accionSiguienteId, fechaSiguiente, observaciones, tipoSeguimiento, string.Empty)[0];
        }

        public void SeguimientoTurnoUpdateObservaciones(int turnoId, TipoSeguimientoEnum tipoSeguimiento, string observaciones)
        {
            dalEngine.UpdatePropertyBatchByFilter<SeguimientoTurno>(new Filter
                                                                        {
                                                                            {
                                                                                SeguimientoTurno.Properties.Turno.Id, "=",
                                                                                turnoId
                                                                                },
                                                                            {
                                                                                BooleanOp.And,
                                                                                SeguimientoTurno.Properties.
                                                                                TipoSeguimiento.Id, "=",
                                                                                (int) tipoSeguimiento
                                                                                }
                                                                        }, SeguimientoTurno.Properties.Observaciones,
                                                                    observaciones);
        }

        public virtual SeguimientoTurno SeguimientoTurnoUpdateGlobal(int turnoId, int? accionActualId, int? accionSiguienteId, DateTime? fechaSiguiente, string observaciones, TipoSeguimientoEnum tipoSeguimiento, string mensajeLogForjado)
        {
            List<int> turnosId = new List<int>();
            turnosId.Add(turnoId);

            return SeguimientoTurnoUpdateGlobal(turnosId, accionActualId, accionSiguienteId, fechaSiguiente, observaciones, tipoSeguimiento, mensajeLogForjado)[0];
        }

        [RequiresTransaction]
        public virtual EntityCollection<SeguimientoTurno> SeguimientoTurnoUpdateGlobal(List<int> turnosId, int? accionActualId, int? accionSiguienteId, DateTime? fechaSiguiente, string observaciones, TipoSeguimientoEnum tipoSeguimiento, string mensajeLogForjado)
        {
            int turnoActual = 0;

            try
            {
                SeguimientoTurno st = null;
                int eventoId = SeguimientoTurnosDefinirEventoId(tipoSeguimiento);
                Dictionary<int, string> logDescripciones = new Dictionary<int, string>();
                int indice = 0;

                // Veo si ya existe uno.
                ReadManyCommand<SeguimientoTurno> readCmd = new ReadManyCommand<SeguimientoTurno>(dalEngine);
                readCmd.Filter = new Filter
                                    {
                                        {SeguimientoTurno.Properties.Turno.Id, "IN", turnosId.ToArray() },
                                        {
                                            BooleanOp.And, SeguimientoTurno.Properties.TipoSeguimiento.Id, "=",
                                            (int) tipoSeguimiento
                                            }
                                    };
                EntityCollection<SeguimientoTurno> sts = readCmd.Execute();

                if (sts == null)
                    sts = new EntityCollection<SeguimientoTurno>();

                Dictionary<int, SeguimientoAccion> _seguimientos = new Dictionary<int, SeguimientoAccion>();
                foreach (int turnoId in turnosId)
                {
                    turnoActual = turnoId;
                    // Si el turno no tiene seguimiento, lo creo
                    st = null;
                    foreach (SeguimientoTurno seguiTurno in sts)
                    {
                        if (seguiTurno.Turno.Id == turnoId)
                        {
                            st = seguiTurno;

                            if (accionActualId.HasValue)
                            {
                                if (accionActualId.Value > 0)
                                {
                                    st.LastSeguimientoAccion = ObtenerSeguimientoAccion(accionActualId.Value);
                                    st.LastFechaAccion = enfoke.Time.Now;
                                }
                                else
                                {
                                    st.LastSeguimientoAccion = null;
                                    st.LastFechaAccion = null;
                                }
                            }


                            if (accionSiguienteId.HasValue && accionSiguienteId.Value > 0 && !(st.LastSeguimientoAccion != null && !st.LastSeguimientoAccion.PermiteProximaAccion))
                            {
                                if (!_seguimientos.ContainsKey(accionSiguienteId.Value))
                                    _seguimientos.Add(accionSiguienteId.Value, dalEngine.GetById<SeguimientoAccion>(accionSiguienteId.Value));

                                st.SigSeguimientoAccion = _seguimientos[accionSiguienteId.Value];
                                st.SigFechaAccion = (fechaSiguiente.HasValue) ? fechaSiguiente.Value : enfoke.Time.Now;
                            }
                            else
                            {
                                st.SigSeguimientoAccion = null;
                                st.SigFechaAccion = null;
                            }

                            break;
                        }
                    }

                    if (st == null)
                    {
                        //Nuevo seguimiento a insertarse.
                        st = new SeguimientoTurno();
                        st.Turno = Context.Session.Dalc.GetById<Turno>(turnoId);

                        st.TipoSeguimiento = new TipoSeguimiento { Id = (int)tipoSeguimiento };

                        if (accionActualId.HasValue && accionActualId.Value > 0)
                        {
                            st.LastSeguimientoAccion = ObtenerSeguimientoAccion(accionActualId.Value);
                            st.LastFechaAccion = enfoke.Time.Now;
                        }
                        if (accionSiguienteId.HasValue && accionSiguienteId.Value > 0 && !(st.LastSeguimientoAccion != null && !st.LastSeguimientoAccion.PermiteProximaAccion))
                        {
                            if (!_seguimientos.ContainsKey(accionSiguienteId.Value))
                                _seguimientos.Add(accionSiguienteId.Value, dalEngine.GetById<SeguimientoAccion>(accionSiguienteId.Value));

                            st.SigSeguimientoAccion = _seguimientos[accionSiguienteId.Value];
                            st.SigFechaAccion = (fechaSiguiente.HasValue) ? fechaSiguiente.Value : enfoke.Time.Now;
                        }

                        sts.Add(st);
                    }

                    st.UpdateDate = enfoke.Time.Now;
                    st.Observaciones = observaciones;

                    // Registro el evento.
                    string log = SeguimientoTurnosDefineTextoLog(tipoSeguimiento, mensajeLogForjado, st);
                    logDescripciones[indice] = log;

                    indice++;
                }// Aca se termina el if de turnosIDs

                List<int?> listaTurnosIds = new List<int?>();
                foreach (int id in turnosId)
                    listaTurnosIds.Add((int?)id);

                LogRegistrar(eventoId, logDescripciones, listaTurnosIds);


                //Guardo el seguimiento.
                return dalEngine.UpdateCollection<SeguimientoTurno>(sts);
            }
            catch (Exception ex)
            {
                throw new NotLoggeableException("Error al guardar el seguimiento del turno [" + turnoActual.ToString() + "].", ex);
            }
        }

        private string SeguimientoTurnosDefineTextoLog(TipoSeguimientoEnum tipoSeguimiento, string mensajeLogForjado, SeguimientoTurno st)
        {
            string log = String.Empty;

            switch ((int)tipoSeguimiento)
            {
                case (int)TipoSeguimientoEnum.Ausentes:
                    log = "Ausente.";
                    break;
                case (int)TipoSeguimientoEnum.Huerfanos:
                    log = "Huérfano.";
                    break;
                case (int)TipoSeguimientoEnum.Confirmaciones:
                    log = "Confirmación.";
                    break;
            }

            if (string.IsNullOrEmpty(mensajeLogForjado))
            {
                if (st.LastSeguimientoAccion == null)
                    log = "Se registra el turno como " + log;
                else
                    log = string.IsNullOrEmpty(st.LastSeguimientoAccion.MensajeLog) ? st.LastSeguimientoAccion.Descripcion : st.LastSeguimientoAccion.MensajeLog;
            }
            else
                log = mensajeLogForjado;

            return log;
        }

        private int SeguimientoTurnosDefinirEventoId(TipoSeguimientoEnum tipoSeguimiento)
        {
            int eventoId = 0;
            switch ((int)tipoSeguimiento)
            {
                case (int)TipoSeguimientoEnum.Ausentes:
                    eventoId = (int)LogEventoEnum.SeguimientoAusentes;
                    break;
                case (int)TipoSeguimientoEnum.Huerfanos:
                    eventoId = (int)LogEventoEnum.SeguimientoHuerfanos;
                    break;
                case (int)TipoSeguimientoEnum.Confirmaciones:
                    eventoId = (int)LogEventoEnum.SeguimientoConfirmaciones;
                    break;
            }
            return eventoId;
        }

        // Feriado
        /// <summary>
        /// [RQ] Inserto un Feriado
        /// </summary>
        /// <param name="feriado">Feriado a Insertar</param>
        /// <returns>El Feriado insertado</returns>




        [Private]
        public FeriadoSucursal FeriadoSucursalInsert(FeriadoSucursal feriadoSucursal)
        {
            feriadoSucursal = dalEngine.Update<FeriadoSucursal>(feriadoSucursal);
            return feriadoSucursal;
        }

        [Private]
        [RequiresTransaction]
        public virtual void FeriadoSucursalDeleteByFeriado(Feriado feriado)
        {
            EntityCollection<FeriadoSucursal> feriadoSucursalColeccion = dalEngine.GetManyByProperty<FeriadoSucursal>(FeriadoSucursal.Properties.Feriado.Id, feriado.Id);

            dalEngine.Delete(feriadoSucursalColeccion);
        }

        public void FeriadoSucusarlDeleteByBatch(EntityCollection<FeriadoSucursal> feriadoSucursalColeccion)
        {
            dalEngine.Delete(feriadoSucursalColeccion);
        }

        public EntityCollection<FeriadoSucursal> FeriadoSucursalReadByFeriado(Feriado feriado)
        {
            return dalEngine.GetManyByProperty<FeriadoSucursal>(FeriadoSucursal.Properties.Feriado.Id, feriado.Id);
        }

        public EntityCollection<FeriadoSucursal> FeriadoSucursalReadByFeriados(EntityCollection<Feriado> feriados)
        {
            return dalEngine.GetManyByPropertyList<FeriadoSucursal>(FeriadoSucursal.Properties.Feriado.Id, feriados.GetIds());
        }

        public EntityCollection<FeriadoSucursal> FeriadoSucursalReadByFeriadosAndSucursal(EntityCollection<Feriado> feriados, int sucursalID)
        {
            if (feriados.Count <= 0)
                return new EntityCollection<FeriadoSucursal>();

            ReadManyCommand<FeriadoSucursal> readCmd = new ReadManyCommand<FeriadoSucursal>(dalEngine);
            Filter filter = new Filter();
            if (feriados.Count > 0)
                filter.Add(BooleanOp.And, FeriadoSucursal.Properties.Feriado.Id, "IN", feriados.GetIds());

            filter.Add(BooleanOp.And, FeriadoSucursal.Properties.Sucursal.Id, "=", sucursalID);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }











        // AutorizacionPlanilla
        /// <summary>
        /// Devuelve todas las planillas que se encuentren en cierto estado
        /// </summary>
        /// <param name="estado">El estado en que se deben encontrar las planillas</param>
        /// <returns>Devuelve todas las planillas que se encuentren en el estado dado</returns>
        public EntityCollection<AutorizacionPlanilla> AutorizacionPlanillaReadByEstado(AutorizacionPlanillaEstadosEnum estado)
        {
            ReadManyCommand<AutorizacionPlanilla> readCmd = new ReadManyCommand<AutorizacionPlanilla>(dalEngine);

            Filter filter = new Filter();
            filter.Add(AutorizacionPlanilla.Properties.EstadoID, "=", (int)estado);

            readCmd.Filter = filter;

            EntityCollection<AutorizacionPlanilla> planillas = readCmd.Execute();
            CargarObjetosPlanillas(planillas);
            return planillas;
        }

        /// <summary>
        /// Devuelve todas las planillas de autorizacion del turno
        /// </summary>
        /// <returns>Todas las planillas de autorizacion del turno</returns>
        public EntityCollection<AutorizacionPlanilla> AutorizacionPlanillaReadByTurno(int turnoID)
        {
            return AutorizacionPlanillaReadByTurno(turnoID, true);
        }

        [Private]
        public EntityCollection<AutorizacionPlanilla> AutorizacionPlanillaReadByTurno(int turnoID, bool cargarObjetos)
        {
            Filter filter = new Filter();
            filter.Add(AutorizacionPlanilla.Properties.TurnoID, "=", turnoID);

            ReadManyCommand<AutorizacionPlanilla> readCmd = new ReadManyCommand<AutorizacionPlanilla>(dalEngine);
            readCmd.Filter = filter;

            EntityCollection<AutorizacionPlanilla> planillas = readCmd.Execute();
            if (cargarObjetos)
                CargarObjetosPlanillas(planillas);
            return planillas;
        }

        private void CargarObjetosPlanillas(EntityCollection<AutorizacionPlanilla> planillas)
        {
            MedicosDalc MedicosDalc = Context.Session.MedicosDalc;
            GeografiaDalc GeografiaDalc = Context.Session.GeografiaDalc;

            for (int i = 0; i < planillas.Count; i++)
            {
                planillas[i].Estado = dalEngine.GetById<AutorizacionPlanillaEstado>(planillas[i].EstadoID);
                planillas[i].Turno = TurnoReadById(planillas[i].TurnoID);
                planillas[i].Turno.Orden.Paciente = PacienteReadById(planillas[i].Turno.Orden.PacienteId);
                if (planillas[i].Turno.Orden.Paciente.LocalidadID.HasValue)
                    planillas[i].Turno.Orden.Paciente.Localidad = dalEngine.GetById<Localidad>(planillas[i].Turno.Orden.Paciente.LocalidadID.Value);
                if (planillas[i].MedicoSolicitanteID > 0)
                    planillas[i].MedicoSolicitante = MedicosDalc.MedicoAsociacionReadById(planillas[i].MedicoSolicitanteID);
                if (planillas[i].MedicoInformanteID > 0)
                    planillas[i].MedicoInformante = MedicosDalc.MedicoReadById(planillas[i].MedicoInformanteID);

                planillas[i].Items = AutorizacionPlanillaItemReadByPlanilla(planillas[i].Id);
            }
        }


        // Metodos Servicio IOMA
        public EntityCollection<AutorizacionPlanilla> AutorizacionPlanillaReadPendientesEnvio()
        {
            Filter filter = new Filter();
            filter.Add(AutorizacionPlanilla.Properties.Enviar, "=", 1);

            filter.Add(BooleanOp.And, AutorizacionPlanilla.Properties.EstadoID, "=", (int)AutorizacionPlanillaEstadosEnum.NoEnviado);

            ReadManyCommand<AutorizacionPlanilla> readCmd = new ReadManyCommand<AutorizacionPlanilla>(dalEngine);
            readCmd.Filter = filter;

            EntityCollection<AutorizacionPlanilla> planillas = readCmd.Execute();

            CargarObjetosPlanillas(planillas);
            return planillas;
        }

        public EntityCollection<Turno> TurnosReservadoNoRecitadosByOrdenAbierta(int ordenAbierta)
        {
            string hql = "SELECT t FROM Turno t WHERE t.Orden.Id = :id AND t.TipoTurnoId <> :recitado AND (t.EstadoTurnoID = :reservado OR t.EstadoTurnoID = :reservadoProvisorio) ORDER BY t.Fecha";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("id", ordenAbierta);
            query.SetInt32("reservado", (int)EstadoTurnoEnum.Reservado);
            query.SetInt32("recitado", (int)TipoTurnoEnum.Recitado);
            query.SetInt32("reservadoProvisorio", (int)EstadoTurnoEnum.ReservaProvisoria);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnosByOrdenAbierta(int ordenAbierta)
        {
            string hql = "SELECT t FROM Turno t WHERE t.Orden.Id = :id ORDER BY t.Fecha";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("id", ordenAbierta);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<AutorizacionPlanilla> AutorizacionPlanillaReadPendientesProcesamiento(int horasDesdeEnvio, int tomarTurnosDesdeDias)
        {
            string hql = "SELECT ap FROM AutorizacionPlanilla ap, Turno t, EstadoTurno et " +
                    "WHERE t.Id = ap.TurnoID " +
                    "AND t.EstadoTurnoID = et.Id " +
                    "AND ap.Enviar = 1 " +
                    "AND ap.EstadoID = :pendiente " +
                    "AND ap.IDSOL is not null " +
                    "AND t.CreateDate > :fechaInicio " +
                    "AND t.TipoAutorizacionID = :autorizacion " +
                    "AND (et.Atendido = 1 " +
                    "  OR et.Pendiente = 1) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("pendiente", (int)AutorizacionPlanillaEstadosEnum.Pendiente);
            query.SetInt32("autorizacion", (int)TipoAutorizacionEnum.EsperaAutorizacionRemota);
            query.SetDateTime("fechaInicio", enfoke.Time.Now.Date.AddDays(-tomarTurnosDesdeDias));

            EntityCollection<AutorizacionPlanilla> planillas = dalEngine.GetManyByQuery<AutorizacionPlanilla>(query);
            CargaObjetosPlanillas.Cargar(planillas);
            return RemoverPlanillasMuyNuevas(horasDesdeEnvio, planillas);
        }

        private EntityCollection<AutorizacionPlanilla> RemoverPlanillasMuyNuevas(int horasDesdeEnvio, EntityCollection<AutorizacionPlanilla> planillas)
        {
            if (horasDesdeEnvio == 0)
                return planillas;

            DateTime fechaEnvioPlanillas = enfoke.Time.Now.AddHours(-horasDesdeEnvio);

            EntityCollection<AutorizacionPlanilla> planillasEnviadas = new EntityCollection<AutorizacionPlanilla>();
            foreach (AutorizacionPlanilla ap in planillas)
            {
                if ((ap.Items.Count > 0
                    && ap.Items[0].FechaEnvio.HasValue
                    && ap.Items[0].FechaEnvio.Value < fechaEnvioPlanillas))
                    planillasEnviadas.Add(ap);
            }

            return planillasEnviadas;
        }



        /// <summary>
        /// Actualiza los datos de la planilla en la base de datos
        /// </summary>
        /// <param name="planilla">La planilla a actualizar</param>
        [RequiresTransaction]
        public virtual void AutorizacionPlanillaUpdate(AutorizacionPlanilla planilla, bool actualizarItems)
        {
            if (actualizarItems)
                AutorizacionPlanillaUpdateWithItems(planilla, AuditableActionsEnum.Update);
            else
                planilla = AutorizacionPlanillaUpdate(planilla);
        }

        public AutorizacionPlanilla AutorizacionPlanillaUpdate(AutorizacionPlanilla planilla)
        {
            return AutorizacionPlanillaUpdate(planilla, AuditableActionsEnum.Update);
        }

        [RequiresTransaction]
        protected internal virtual AutorizacionPlanilla AutorizacionPlanillaUpdate(AutorizacionPlanilla planilla, AuditableActionsEnum action)
        {

            // Guardo
            planilla = dalEngine.Update<AutorizacionPlanilla>(planilla);

            foreach (AutorizacionPlanillaItem item in planilla.Items)
                AutorizacionPlanillaItemUpdate(item);

            return planilla;
        }

        /// <summary>
        /// Actualiza planillas para un turno existente
        /// </summary>
        /// <param name="planillas">Las planillas a insertar</param>
        /// <param name="turno">El turno al cual pertenencerán las planillas</param>
        /// <param name="user">Usuario para auditoría</param>
        [RequiresTransaction]
        public virtual void AutorizacionPlanillaUpdate(EntityCollection<AutorizacionPlanilla> planillas, int turnoID)
        {
            SavePlanillas(planillas, turnoID, true);
        }

        [RequiresTransaction]
        protected internal virtual void SavePlanillas(EntityCollection<AutorizacionPlanilla> planillas, int? turnoID, bool soloUpdate)
        {
            AuditableActionsEnum action = soloUpdate ? AuditableActionsEnum.Update : AuditableActionsEnum.Add | AuditableActionsEnum.Update;

            for (int i = 0; i < planillas.Count; i++)
            {
                AutorizacionPlanilla planilla = planillas[i];

                // Asigno el Turno
                if (turnoID.HasValue)
                    planilla.TurnoID = turnoID.Value;

                //if (planilla.Enviar)
                planilla = AutorizacionPlanillaUpdateWithItems(planilla, action);
            }
        }
        [RequiresTransaction]
        protected virtual AutorizacionPlanilla AutorizacionPlanillaUpdateWithItems(AutorizacionPlanilla planilla, AuditableActionsEnum action)
        {
            // Guardo la Planilla
            planilla = AutorizacionPlanillaUpdate(planilla, action);

            for (int j = 0; j < planilla.Items.Count; j++)
            {
                AutorizacionPlanillaItem item = planilla.Items[j];

                // Chequeo la Solicitud
                if (String.IsNullOrEmpty(item.Solicitud))
                    item.Solicitud = "[NO ESPECIFICADO]";

                // Asigno la Planilla
                item.Planilla = planilla;

                // Guardo el Item
                item = AutorizacionPlanillaItemUpdate(item, action);
            }

            return planilla;
        }


        // AutorizacionPlanillaItem
        public AutorizacionPlanillaItem AutorizacionPlanillaItemUpdate(AutorizacionPlanillaItem item)
        {
            return AutorizacionPlanillaItemUpdate(item, AuditableActionsEnum.Update);
        }
        [RequiresTransaction]
        protected internal virtual AutorizacionPlanillaItem AutorizacionPlanillaItemUpdate(AutorizacionPlanillaItem item, AuditableActionsEnum action)
        {

            // Guardo
            item = dalEngine.Update<AutorizacionPlanillaItem>(item);

            return item;
        }

        [Private]
        public EntityCollection<AutorizacionPlanillaItem> AutorizacionPlanillaItemReadByPlanilla(int planillaID)
        {
            return dalEngine.GetManyByProperty<AutorizacionPlanillaItem>(AutorizacionPlanillaItem.Properties.Planilla.Id, planillaID);
        }

        // Condicion
        public CondicionInfoCollection CondicionInfoReadByServicioId(int servicioId)
        {
            CondicionInfoCollection ret = new CondicionInfoCollection();

            EntityCollection<Condicion> condiciones = CondicionReadByServicioId(servicioId);
            // Por cada condicion, creo el info con las respuestas
            foreach (Condicion c in condiciones)
                ret.Add(CreateCondicionInfo(c));

            return ret;
        }

        public CondicionInfoCollection CondicionInfoReadByPracticaId(int practicaId)
        {
            CondicionInfoCollection ret = new CondicionInfoCollection();

            EntityCollection<Condicion> condiciones = CondicionReadByPracticaId(practicaId);
            // Por cada condicion, creo el info con las respuestas
            foreach (Condicion c in condiciones)
                ret.Add(CreateCondicionInfo(c));

            return ret;
        }

        private CondicionInfo CreateCondicionInfo(Condicion c)
        {
            if (c == null)
                return null;

            // Trae las respuestas
            EntityCollection<CondicionRespuesta> respuestas = CondicionRespuestaReadByCondicion(c);
            // creo los infos de las repsuestsda
            List<CondicionRespuestaInfo> respuestasInfo = new List<CondicionRespuestaInfo>();
            foreach (CondicionRespuesta r in respuestas)
                respuestasInfo.Add(new CondicionRespuestaInfo(r, CreateCondicionInfo(r.SiguienteCondicion)));

            return new CondicionInfo(c, respuestasInfo);
        }

        public EntityCollection<Condicion> CondicionReadByServicioId(int servicioId)
        {
            string hql = "select distinct cs.Condicion from CondicionServicio cs "
                    + " WHERE cs.Servicio.Id = :servicioId and cs.Condicion.IncluirEnComportamientos = true ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("servicioId", servicioId);
            return dalEngine.GetManyByQuery<Condicion>(query);
        }

        public EntityCollection<Condicion> CondicionReadByServicios(EntityCollection<Servicio> colS)
        {
            if (colS == null || colS.Count == 0)
                return new EntityCollection<Condicion>();

            // Armo el listado con los IDs de las practicas
            List<int> IDs = new List<int>();
            foreach (Servicio s in colS)
                IDs.Add(s.Id);

            string hql = "select distinct cs.Condicion from CondicionServicio cs "
                    + " WHERE cs.Servicio.Id IN (:servicios)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("servicios", IDs);
            return dalEngine.GetManyByQuery<Condicion>(query);
        }

        public EntityCollection<Condicion> CondicionReadByPracticaId(int practicaId)
        {
            string hql = "select distinct cp.Condicion from CondicionPractica cp "
                    + " WHERE cp.Practica.Id = :practicaId and cp.Condicion.IncluirEnComportamientos = true ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaId", practicaId);
            return dalEngine.GetManyByQuery<Condicion>(query);
        }

        public EntityCollection<Condicion> CondicionReadByPracticas(EntityCollection<Practica> colP)
        {
            if (colP == null || colP.Count == 0)
                return new EntityCollection<Condicion>();

            // Armo el string con los Ids de las practicas
            List<int> practicasId = new List<int>();
            foreach (Practica p in colP)
                practicasId.Add(p.Id);

            string hql = "select distinct cp.Condicion from CondicionPractica cp "
                    + " WHERE cp.Practica.Id IN (:practicas)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("practicas", practicasId);
            return dalEngine.GetManyByQuery<Condicion>(query);
        }

        public EntityCollection<CondicionRespuesta> CondicionRespuestaReadByCondicion(Condicion c)
        {
            PracticasDalc PracticasDalc = Context.Session.PracticasDalc;

            ReadManyCommand<CondicionRespuesta> readCmd = new ReadManyCommand<CondicionRespuesta>(dalEngine);

            Filter filter = new Filter();
            filter.Add(CondicionRespuesta.Properties.Condicion.Id, "=", c.Id);

            readCmd.Filter = filter;
            EntityCollection<CondicionRespuesta> col = readCmd.Execute();
            foreach (CondicionRespuesta r in col)
                if (r.PracticaReemplazo != null)
                    r.PracticaReemplazo = dalEngine.GetById<Practica>(r.PracticaReemplazo.Id);

            return col;
        }

        [AnonymousMethod()]
        public EntityCollection<CondicionTurno> CondicionTurnoReadByTurnosIds(List<int> turnosIds)
        {
            string hql = "SELECT ct FROM CondicionTurno ct, Turno tur WHERE ct.TurnoID = tur.Id AND tur.Id IN (:turnosIds) ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("turnosIds", turnosIds);
            return dalEngine.GetManyByQuery<CondicionTurno>(query);
        }

        [AnonymousMethod()]
        public EntityCollection<CondicionTurno> CondicionTurnoReadByOrden(int ordenID)
        {
            string hql = "SELECT ct FROM CondicionTurno ct, Turno tur WHERE ct.TurnoID = tur.Id AND tur.Orden.Id = :ordenID ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ordenID", ordenID);
            return dalEngine.GetManyByQuery<CondicionTurno>(query);
        }

        [AnonymousMethod()]
        public EntityCollection<CondicionTurno> CondicionTurnoReadByTurno(int turnoID)
        {
            return dalEngine.GetManyByProperty<CondicionTurno>(CondicionTurno.Properties.TurnoID, turnoID);
        }

        [AnonymousMethod()]
        public EntityCollection<CondicionTurno> CondicionTurnoReadByTurnosId(List<int> turnosID)
        {
            return dalEngine.GetManyByPropertyList<CondicionTurno>(CondicionTurno.Properties.TurnoID, turnosID);
        }

        [RequiresTransaction]
        protected internal virtual void CondicionTurnoDeleteByTurno(int turnoID)
        {
            EntityCollection<CondicionTurno> CTs = CondicionTurnoReadByTurno(turnoID);
            if (CTs.Count == 0)
                return;


            dalEngine.Delete(CTs);
        }

        [Private]
        public EntityCollection<Condicion> CondicionReadForcomportamientos()
        {
            return dalEngine.GetManyByProperty<Condicion>(Condicion.Properties.IncluirEnComportamientos, true, Condicion.Properties.Descripcion);
        }


        // Paciente
        public EntityCollection<Paciente> PacienteSearchByName(string name)
        {
            ReadManyCommand<Paciente> readCmd = new ReadManyCommand<Paciente>(dalEngine);

            Filter filter = new Filter();
            filter.Add(Paciente.Properties.Apellido, "LIKE", name.Trim().Replace(" ", "%") + "%");

            filter.Add(BooleanOp.And, Paciente.Properties.Deleted, "=", false);

            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<Paciente> PacienteReadByLastAndFirstName(string lastAndFirstName)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select pac from Paciente pac where ");
            hqlBuilder.Append(SQLPortable.StringConcat("pac.Apellido", "pac.Nombre"));
            hqlBuilder.Append(" like '%");
            hqlBuilder.Append(lastAndFirstName.Replace(' ', '%'));
            hqlBuilder.Append("%'");
            hqlBuilder.Append(" order by pac.Apellido desc, pac.Nombre desc");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<Paciente>(query);
        }

        public EntityCollection<Paciente> PacienteSearchByLastNameFirstNameandDNI(string apellido, string name, string dni, TipoDocumento tipoDocumento)
        {
            ReadManyCommand<Paciente> readCmd = new ReadManyCommand<Paciente>(dalEngine);
            IPropertyReference propiedadFiltro = null;
            IPropertyReference propiedadImportancia = null;

            Filter filter = new Filter();

            if (String.IsNullOrEmpty(apellido.Trim()) == false)
            {
                propiedadFiltro = Paciente.Properties.Apellido;
                propiedadImportancia = Paciente.Properties.Importancia;
                Privacy.AddConfidentialFilter(apellido.Trim().Replace(" ", "%") + "%", filter, propiedadFiltro, propiedadImportancia, null);
            }

            if (String.IsNullOrEmpty(dni.Trim()) == false)
            {
                int dniNumerico;
                if (int.TryParse(dni.Trim(), out dniNumerico) == false)
                    throw new NotLoggeableException("El número de documento sólo puede contener números.");

                propiedadFiltro = Paciente.Properties.Dni;
                propiedadImportancia = Paciente.Properties.Importancia;
                Privacy.AddConfidentialFilter(dni.Trim(), filter, propiedadFiltro, propiedadImportancia, BooleanOp.And);
            }

            if (String.IsNullOrEmpty(name.Trim()) == false)
            {
                propiedadFiltro = Paciente.Properties.Nombre;
                propiedadImportancia = Paciente.Properties.Importancia;
                Privacy.AddConfidentialFilter(name.Trim().Replace(" ", "%") + "%", filter, propiedadFiltro, propiedadImportancia, BooleanOp.And);
            }

            if (tipoDocumento.Id > 0)
                filter.Add(BooleanOp.And, Paciente.Properties.TipoDocumentoId, "=", tipoDocumento.Id);

            filter.Add(BooleanOp.And, Paciente.Properties.Deleted, "=", false);

            Sort sort = new Sort();
            SortItem sortItem2 = new SortItem(Paciente.Properties.Apellido, SortingDirection.Asc);
            SortItem sortItem1 = new SortItem(Paciente.Properties.Nombre, SortingDirection.Asc);
            sort.Add(sortItem2);
            sort.Add(sortItem1);

            readCmd.Sort = sort;
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        /// <summary>
        /// Chequeo si existe un paciente con el mismo documento
        /// </summary>
        /// <param name="tipo">tipo de documento</param>
        /// <param name="numero">numero de documento</param>
        /// <param name="idPaciente">id del paciente a obviar</param>
        /// <returns>True/False</returns>
        public bool PacienteExisteConDocumento(int tipo, int numero, int idPaciente)
        {
            // Cuando el paciente tiene el tipo de documento SIN DOCUMENTO
            // no se realizar la busqueda y devuelvo falso
            if (tipo != (int)TipoDocumentoPacienteEnum.SinDocumento)
            {
                ReadManyCommand<Paciente> readCmd = new ReadManyCommand<Paciente>(dalEngine);

                Filter filter = new Filter();

                filter.Add(Paciente.Properties.TipoDocumentoId, "=", tipo);
                filter.Add(BooleanOp.And, Paciente.Properties.Dni, "=", numero);
                filter.Add(BooleanOp.And, Paciente.Properties.Id, "!=", idPaciente);
                filter.Add(BooleanOp.And, Paciente.Properties.Deleted, "=", false);

                readCmd.Filter = filter;

                // Si encontre algo es porque existe
                return readCmd.Execute().Count > 0;
            }
            else
                return false;
        }

        [AnonymousMethod()]
        public Paciente PacienteReadById(int id)
        {
            return dalEngine.GetById<Paciente>(id);
        }

        [Private]
        public EntityCollection<Paciente> PacientesReadByIds(IEnumerable<int> ids)
        {
            Filter filter = new Filter();
            filter.Add(Paciente.Properties.Id, "IN", ids);
            return dalEngine.GetManyByFilter<Paciente>(filter);
        }

        public Paciente PacienteUpdate(Paciente paciente, bool pacienteModificado, int? ordenID, TurnoInfoInternacion infoInternacion, bool loguearEventosPaciente)
        {
            return PacienteUpdate(paciente, pacienteModificado, ordenID, infoInternacion, false, false, false, loguearEventosPaciente);
        }

        public void PacientesUpdateByCollection(EntityCollection<Paciente> pacientes)
        {
            dalEngine.UpdateCollection(pacientes);
        }

        public void PacienteDelete(Paciente pac)
        {

            pac.Deleted = true;
            dalEngine.Update<Paciente>(pac);
        }

        public EntityCollection<PacienteLight> PacienteReadByRead(
            DateTime? fechaDesde, DateTime? fechaHasta, string apellido, string nombre, TipoDocumento tipoDocumento, string documento, bool mostrarEliminados)
        {
            Filter filter = new Filter();

            if (fechaDesde.HasValue)
                filter.Add(BooleanOp.And, PacienteLight.Properties.CreateDate, ">=", fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                filter.Add(BooleanOp.And, PacienteLight.Properties.CreateDate, "<", fechaHasta.Value.Date.AddDays(1));

            if (fechaHasta.HasValue)
                filter.Add(BooleanOp.And, PacienteLight.Properties.CreateDate, "<", fechaHasta.Value.Date.AddDays(1));

            if (!String.IsNullOrEmpty(documento))
                Privacy.AddConfidentialFilter(documento + "%", filter, PacienteLight.Properties.Dni, PacienteLight.Properties.Importancia, BooleanOp.And);

            if (tipoDocumento != null && tipoDocumento.Id > 0)
                filter.Add(BooleanOp.And, PacienteLight.Properties.TipoDocumentoId, "=", tipoDocumento.Id);

            if (!String.IsNullOrEmpty(apellido))
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                Privacy.AddConfidentialFilter(apellido.Trim().Replace(" ", "%") + "%", filter, PacienteLight.Properties.Apellido, PacienteLight.Properties.Importancia, BooleanOp.And);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);

            }

            if (!String.IsNullOrEmpty(nombre))
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);

                Privacy.AddConfidentialFilter(nombre.Trim().Replace(" ", "%") + "%", filter, PacienteLight.Properties.Nombre, PacienteLight.Properties.Importancia, BooleanOp.And);

                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);

            }

            if (!mostrarEliminados)
            {
                OpenParenthesis open2 = new OpenParenthesis(BooleanOp.And);
                filter.Add(open2);

                filter.Add(BooleanOp.And, PacienteLight.Properties.Deleted, "<>", true);
                filter.Add(BooleanOp.Or, PacienteLight.Properties.Deleted, "is", null);

                CloseParenthesis close2 = new CloseParenthesis();
                filter.Add(close2);
            }

            EntityCollection<PacienteLight> pacientes = PacienteReadByFilter(filter);
            return pacientes;
        }

        private EntityCollection<PacienteLight> PacienteReadByFilter(Filter filter)
        {
            ReadManyCommand<PacienteLight> readCmd = new ReadManyCommand<PacienteLight>(dalEngine);

            if (filter != null)
                readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(PacienteLight.Properties.Nombre, SortingDirection.Asc);
            sort.Add(PacienteLight.Properties.Apellido, SortingDirection.Asc);

            readCmd.Sort = sort;

            readCmd.MaxResults = 501;

            return readCmd.Execute();
        }

        public EntityCollection<Paciente> PacienteReadByDocumentoTipoYApellidoNombre(int? documento, int? tipoId, string apellidoNombre)
        {
            if (!documento.HasValue && !tipoId.HasValue && string.IsNullOrEmpty(apellidoNombre))
                throw new enfokeTrappedException("La consulta de paciente debe tener o filtro de dni o apellido del paciente.");

            Filter filter = new Filter();
            if (documento.HasValue)
                filter.Add(BooleanOp.And, Paciente.Properties.Dni, "=", documento.Value);

            if (tipoId.HasValue)
                filter.Add(BooleanOp.And, Paciente.Properties.TipoDocumentoId, "=", tipoId.Value);

            if (!string.IsNullOrEmpty(apellidoNombre))
                filter.Add(BooleanOp.And, Paciente.Properties.ApellidoNombre, "like", "%" + apellidoNombre.Replace(' ', '%') + "%");

            EntityCollection<Paciente> pacientes = dalEngine.GetManyByFilter<Paciente>(filter);
            return pacientes;
        }

        [RequiresTransaction]
        public virtual Paciente PacienteUpdate(Paciente paciente, bool pacienteModificado, int? ordenId, TurnoInfoInternacion infoInternacion, bool incrementoTurnosRealizados, bool incrementoTurnosAusentes, bool incrementoTurnosCancelados, bool loguearEventosPaciente)
        {
            paciente.ApellidoNombre = paciente.Apellido.Trim() + " " + paciente.Nombre.Trim();

            if (paciente.TipoDocumentoId == (int)TipoDocumentoPacienteEnum.SinDocumento)
                paciente.Dni = 0;

            bool isNew = paciente.Id == 0;
            if (paciente.Localidad == null && paciente.LocalidadID.HasValue)
                paciente.Localidad = Context.Session.Dalc.GetById<Localidad>(paciente.LocalidadID.GetValueOrDefault());
            else
                paciente.Localidad = new Localidad();
           
            paciente = dalEngine.Update<Paciente>(paciente);
            if (isNew)
                Context.EventProcessor.ProcessEvent<RegistrarPaciente, HL7Paciente>(new HL7Paciente(paciente, Security.Current.UserInfo.User));
            else
                Context.EventProcessor.ProcessEvent<ActualizarPaciente, HL7Paciente>(new HL7Paciente(paciente, Security.Current.UserInfo.User));
            if (ordenId.HasValue)
            {
                Orden orden = dalEngine.GetById<Orden>(ordenId.Value);

                // Guardo la info de internacion
                if (infoInternacion != null)
                {
                    infoInternacion = this.TurnoInfoInternacionUpdate(infoInternacion);
                    orden.InfoInternacion = infoInternacion.Id;
                    dalEngine.Update(orden);
                }

                if (loguearEventosPaciente)
                    if (pacienteModificado)
                        LogRegistrar((int)LogEventoEnum.ModificacionDatosPaciente, "Se modificaron los datos del paciente [" + paciente.Id.ToString() + "].", orden);
                    else
                    {
                        LogRegistrar((int)LogEventoEnum.ConsultaDatosPaciente,
                                     "Se consultaron los datos del paciente [" + paciente.Id.ToString() + "].", orden);
                    }

                // Log de incremento de Indicadores
                if (incrementoTurnosRealizados)
                    LogRegistrar((int)LogEventoEnum.ModificacionDatosPaciente, "Se incrementó el valor del indicador Turnos_Realizados del paciente [" + paciente.Id.ToString() + "][Valor Actual: " + paciente.TurnosRealizados.ToString() + "].", orden);
                if (incrementoTurnosAusentes)
                    LogRegistrar((int)LogEventoEnum.ModificacionDatosPaciente, "Se incrementó el valor del indicador Turnos_Ausentes del paciente [" + paciente.Id.ToString() + "][Valor Actual: " + paciente.TurnosAusentes.ToString() + "].", orden);
                if (incrementoTurnosCancelados)
                    LogRegistrar((int)LogEventoEnum.ModificacionDatosPaciente, "Se incrementó el valor del indicador Turnos_Cancelados del paciente [" + paciente.Id.ToString() + "][Valor Actual: " + paciente.TurnosCancelados.ToString() + "].", orden);
            }
            return paciente;
        }

        public decimal PacienteFillMontoAdeudado(int idPaciente)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.ResultadoOperacion(SUM(t.ImporteAPagar), SUM(t.ImportePagado)) " +
                         "FROM Turno t, Paciente p " +
                         "WHERE t.Orden.PacienteId = p.Id " +
                         "AND t.Orden.PagoDiferidoClienteId IS NULL " +
                         "AND p.Id = :idPaciente " +
                         "AND t.EstadoTurnoID != :idEstado ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idPaciente", idPaciente);
            query.SetParameter("idEstado", (int)EstadoTurnoEnum.Cancelado);

            ResultadoOperacion result = dalEngine.GetByQuery<ResultadoOperacion>(query);

            if (result != null)
                return result.GetResta();
            else
                return 0;
        }

        public Paciente PacienteGenerarNumeroHistoriaClinica(Paciente paciente)
        {
            Paciente pacienteOriginal = PacienteReadById(paciente.Id);

            if (pacienteOriginal.HistoriaClinica.HasValue)
                paciente.HistoriaClinica = pacienteOriginal.HistoriaClinica;
            else
            {
                string hql = "select max(pac.HistoriaClinica) from Paciente pac where pac.Deleted = false";

                object objHistoriaClinica = dalEngine.CreateQuery(hql).UniqueResult();
                int nroHistoriaClinica = (objHistoriaClinica != null) ? Convert.ToInt32(objHistoriaClinica) + 1 : 1;

                dalEngine.UpdatePropertyBatchByIds<Paciente>(new List<int> { paciente.Id },
                                                                    Paciente.Properties.HistoriaClinica, nroHistoriaClinica);
                paciente.HistoriaClinica = nroHistoriaClinica;
            }

            return paciente;
        }



        // PracticaTurno
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurno(List<int> turnoIds, PracticaTurnoTipoEnum tipo)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PracticaTurno.Properties.TurnoId, "IN", turnoIds.ToArray());

            if (tipo != PracticaTurnoTipoEnum.Todas)
                filter.Add(BooleanOp.And, PracticaTurno.Properties.Tipo, "=", (int)tipo);

            // No deberia de pasar que haya una practica con cantidad igual a cero del tipo principal
            // Cuando es asi a esa practica se la pone del tipo Exposicion, pero por las dudas... para prevenir.
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Cantidad, " > ", 0);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Retorna todas las prácticas asociadas del turno
        /// </summary>
        /// <param name="turnoId">Turno del cual buscar las prácticas</param>
        /// <param name="tipo">Tipo de practica a buscar</param>
        /// <returns>Todas las prácticas asociadas al turno</returns>
        [AnonymousMethod()]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurno(int turnoId, PracticaTurnoTipoEnum tipo)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);

            if (tipo != PracticaTurnoTipoEnum.Todas)
                filter.Add(BooleanOp.And, PracticaTurno.Properties.Tipo, "=", (int)tipo);

            // No deberia de pasar que haya una practica con cantidad igual a cero del tipo principal
            // Cuando es asi a esa practica se la pone del tipo Exposicion, pero por las dudas... para prevenir.
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Cantidad, " > ", 0);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        //[AnonymousMethod()]
        //public EntityCollection<PracticaTurnoProtocolo> PracticaTurnoProtocoloReadByTurno(int turnoId, PracticaTurnoTipoEnum tipo)
        //{
        //    EntityCollection<PracticaTurno> practicasTurno = PracticaTurnoReadByTurno(turnoId, tipo);
        //    List<int> turnosIds = new List<int>();
        //    foreach (PracticaTurno practicaTurno in practicasTurno)
        //        if (!turnosIds.Contains(practicaTurno.TurnoId))
        //            turnosIds.Add(practicaTurno.TurnoId);

        //    EntityCollection<PracticaTurnoProtocolo> turnosIdProtocolos = new EntityCollection<PracticaTurnoProtocolo>();
        //    if (turnosIds.Count > 0)
        //    {   //get protocolos
        //        IQuery query = CreateQueryPracticaTurnoProtocolo(turnosIds);
        //        turnosIdProtocolos = dalEngine.GetManyByQuery<PracticaTurnoProtocolo>(query);
        //    }

        //    EntityCollection<PracticaTurnoProtocolo> practicasTurnoProtocolo = new EntityCollection<PracticaTurnoProtocolo>();
        //    foreach (PracticaTurno practicaTurno in practicasTurno)
        //    {   //prepara los datos a devolver
        //        string protocolo = GetProtocolo(turnosIdProtocolos, practicaTurno.TurnoId);
        //        practicasTurnoProtocolo.Add(new PracticaTurnoProtocolo(practicaTurno, protocolo));
        //    }

        //    return practicasTurnoProtocolo;
        //}

        //private string GetProtocolo(EntityCollection<PracticaTurnoProtocolo> turnoIdProtocolos, int turnoId)
        //{
        //    foreach (PracticaTurnoProtocolo ptp in turnoIdProtocolos)
        //        if (ptp.TurnoId == turnoId)
        //            return ptp.Protocolo;
        //    return string.Empty;
        //}

        private IQuery CreateQueryPracticaTurnoProtocolo(List<int> turnosIds)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Results.PracticaTurnoProtocolo(turno.Id,turno.Orden.Protocolo.ProtocoloFull) ");
            hqlBuilder.Append("from Turno turno ");
            hqlBuilder.Append("where turno.Id in (:turnosIds)");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnosIds", turnosIds);
            return query;
        }


        [AnonymousMethod()]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurno(int turnoId)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [AnonymousMethod()]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnoAndRegion(int turnoId, int regionId)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);
            filter.Add(BooleanOp.And, PracticaTurno.Properties.RegionInformeID, "=", regionId);
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Cantidad, ">", 0);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public List<int> PracticasIdsReadByTurno(int turnoId)
        {
            string hql = "SELECT DISTINCT pt.Practica.Id " +
                         "FROM PracticaTurnoHQL pt " +
                         "WHERE pt.Turno.DeleteFlag = 0 " +
                         "AND pt.Cantidad > 0 " +
                         "AND pt.Turno.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("idTurno", turnoId);

            return (List<int>)query.List<int>();
        }

        public EntityCollection<Practica> PracticasReadByTurno(int turnoId)
        {
            string hql = "SELECT pt.Practica " +
                         "FROM PracticaTurnoHQL pt " +
                         "WHERE pt.Turno.DeleteFlag = 0 " +
                         "AND pt.Cantidad > 0 " +
                         "AND pt.Turno.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("idTurno", turnoId);

            // Todo lo siguiente es para hacer el DISTINCT que no puedo hacer en el HQO porque la practica tiene un BLOP
            EntityCollection<Practica> col = dalEngine.GetManyByQuery<Practica>(query);
            EntityCollection<Practica> result = new EntityCollection<Practica>();
            Hashtable metidas = new Hashtable();
            foreach (Practica practica in col)
            {
                if (metidas[practica.Id] == null)
                {
                    result.Add(practica);
                    metidas.Add(practica.Id, true);
                }
            }

            return result;

        }

        public List<int> PracticasReadByTurnoIdAndServicioIdWithRequiereProfesionalAnd(int turnoId, int servicioId)
        {
            string hql = "SELECT DISTINCT pt.Practica.Id " +
                         "FROM PracticaTurnoHQL pt " +
                         "WHERE pt.Turno.DeleteFlag = 0 " +
                         "AND pt.Cantidad > 0 " +
                         "AND pt.Turno.Id = :idTurno " +
                         "AND pt.Practica.ServicioEspecialidad.Servicio.Id = :idServicio " +
                         "AND pt.Practica.RequiereProfesional = true ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("idTurno", turnoId);
            query.SetParameter("idServicio", servicioId);

            return (List<int>)query.List<int>();
        }

        /// <summary>
        /// [GG] Devuelve la practica correspondiente al turno y a la región del informe si es que la hay.
        /// </summary>
        /// <param name="turnoId"></param>
        /// <param name="tipo"></param>
        /// <returns></returns>
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnoAndRegion(int turnoId, PracticaTurnoTipoEnum tipo, int? regionId)
        {
            if (!regionId.HasValue)
                return PracticaTurnoReadByTurno(turnoId, tipo);
            else
            {
                ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

                Filter filter = new Filter();

                filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);

                if (tipo != PracticaTurnoTipoEnum.Todas)
                    filter.Add(BooleanOp.And, PracticaTurno.Properties.Tipo, "=", (int)tipo);

                filter.Add(BooleanOp.And, PracticaTurno.Properties.RegionInformeID, "=", regionId.Value);

                Sort sort = new Sort();
                sort.Add(PracticaTurno.Properties.Tipo, SortingDirection.Asc);

                readCmd.Filter = filter;
                readCmd.Sort = sort;

                return readCmd.Execute();
            }
        }

        public EntityCollection<PracticaTurnoForUpdateMedicoInformante> PracticaTurnoForUpdateMedicoInformanteReadByIds(List<int> practicaTurnosIds)
        {
            return dalEngine.GetManyByIds<PracticaTurnoForUpdateMedicoInformante>(practicaTurnosIds);
        }

        [Private]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnoInformante(int turnoID, int informanteID)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoID);
            readCmd.Filter.Add(BooleanOp.And, PracticaTurno.Properties.MedicoInformante, "=", informanteID);

            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnoInformanteAndRegion(int turnoID, int informanteID, RegionInforme region)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoID);
            readCmd.Filter.Add(BooleanOp.And, PracticaTurno.Properties.MedicoInformante, "=", informanteID);

            if (region == null)
                readCmd.Filter.Add(BooleanOp.And, PracticaTurno.Properties.RegionInformeID, " IS ", null);
            else
                readCmd.Filter.Add(BooleanOp.And, PracticaTurno.Properties.RegionInformeID, "=", region.Id);

            return readCmd.Execute();
        }

        [Private]
        public PracticaTurno PracticaTurnoReadByTurnoAndPractica(int turnoId, int practicaId, PracticaAdicional practicaAdicional)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);

            filter.Add(BooleanOp.And, PracticaTurno.Properties.Practica.Id, "=", practicaId);

            if (practicaAdicional != null)
                filter.Add(BooleanOp.And, PracticaTurno.Properties.PracticaAdicional, "=", practicaAdicional.Id);
            else
                filter.Add(BooleanOp.And, PracticaTurno.Properties.PracticaAdicional, " is ", null);

            readCmd.Filter = filter;

            EntityCollection<PracticaTurno> ret = readCmd.Execute();
            if (ret.Count == 0)
                return null;
            else
                return ret[0];
        }


        [Private]
        public PracticaTurnoForUpdateCantidadTipo PracticaTurnoReadByTurnoAndPracticaForUpdateCantidadTipo(int turnoId, int practicaId)
        {
            Filter filter = new Filter();
            filter.Add(PracticaTurnoForUpdateCantidadTipo.Properties.TurnoId, "=", turnoId);
            filter.Add(BooleanOp.And, PracticaTurnoForUpdateCantidadTipo.Properties.PracticaId, "=", practicaId);

            EntityCollection<PracticaTurnoForUpdateCantidadTipo> ret = dalEngine.GetManyByFilter<PracticaTurnoForUpdateCantidadTipo>(filter);
            if (ret.Count == 0)
                return null;
            else if (ret.Count > 1)
                throw new Exception("La consulta solo debe devolver un valor para la práctica " + practicaId + " del turno " + turnoId);
            return ret[0];
        }

        [Private]
        public PracticaTurno PracticaTurnoReadByTurnoAndPractica(int turnoId, int practicaId)
        {
            Filter filter = new Filter();
            filter.Add(PracticaTurno.Properties.TurnoId, "=", turnoId);
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Practica.Id, "=", practicaId);

            EntityCollection<PracticaTurno> ret = dalEngine.GetManyByFilter<PracticaTurno>(filter);
            if (ret.Count == 0)
                return null;
            else if (ret.Count > 1)
                throw new Exception("La consulta solo debe devolver un valor para la práctica  " + practicaId + " del turno " + turnoId);
            return ret[0];
        }

        public void PracticaTurnoDeleteByTurno(int turnoId)
        {
            EntityCollection<PracticaTurno> oldPracticas = PracticaTurnoReadByTurno(turnoId, PracticaTurnoTipoEnum.Todas);

            if (oldPracticas.Count > 0)
            {
                dalEngine.Delete(oldPracticas);
            }
        }






        public EntityCollection<PracticaTurno> PracticaTurnoReadByIds(List<int> ids)
        {
            ReadManyCommand<PracticaTurno> readCmd = new ReadManyCommand<PracticaTurno>(dalEngine);

            readCmd.Filter = new Filter();
            readCmd.Filter.Add(PracticaTurno.Properties.TurnoId, "IN", ids.ToArray());

            return readCmd.Execute();
        }

        public void PracticaTurnoUpdateMany(EntityCollection<PracticaTurno> practicaTurnos)
        {
            dalEngine.UpdateCollection<PracticaTurno>(practicaTurnos);
        }

        public TurnoInfoInternacion TurnoInfoInternacionUpdate(TurnoInfoInternacion infoInternacion)
        {
            return dalEngine.Update<TurnoInfoInternacion>(infoInternacion);
        }

        public TurnoInfoInternacion TurnoInfoInternacionoReadById(int id)
        {
            return dalEngine.GetById<TurnoInfoInternacion>(id);
        }

        /// <summary>
        /// Elimina las PTs que no existen en la valorizacion de admision (2da)
        /// </summary>
        /// <param name="turnoID">ID del Turno en cuestion</param>
        [RequiresTransaction]
        protected internal virtual void PracticaTurnoDeleteExtras(int turnoID, int valorizacionPrefaID)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            // Obtengo la Valorización de Admision
            Entities.Valorizacion valorizacion = ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turnoID, (int)ValorizacionTiposEnum.Admision);

            if (valorizacion != null)
            {
                // Obtengo los Items de la Valorización
                EntityCollection<ValorizacionItem> items = valorizacion.Items;

                // Obtengo las PracticaTurno del turno [para chequear si se agregaron practicas]
                EntityCollection<PracticaTurno> PTs = new EntityCollection<PracticaTurno>();

                PTs.AddRange(PracticaTurnoReadByTurno(turnoID, PracticaTurnoTipoEnum.Todas));
                PTs.AcceptChanges();

                // Saco las PTs de los items de la valorizacion
                foreach (ValorizacionItem item in items)
                    PTs.Remove(item.PracticaTurno);

                PTs.AcceptChanges();

                // Chequeo si quedaron PTs sin agregar
                if (PTs.Count > 0)
                {

                    for (int i = 0; i < PTs.Count; i++)
                    {
                        EliminarPracticaTurnos(valorizacionPrefaID, ValorizacionesDalc, PTs, i);
                    }
                }
            }
        }

        [MinuteTimeout]
        [RequiresTransaction]
        protected internal virtual void EliminarPracticaTurnos(int valorizacionPrefaID, ValorizacionesDalc ValorizacionesDalc, EntityCollection<PracticaTurno> PTs, int i)
        {
            List<int> itemIds = new List<int>();

            // Elimino los Items de Valorizacion de la PT
            EntityCollection<ValorizacionItem> VIsPT = ValorizacionesDalc.ValorizacionItemReadByPracticaTurno(PTs[i].Id);

            if (VIsPT != null && VIsPT.Count > 0)
                foreach (ValorizacionItem item in VIsPT)
                    itemIds.Add(item.Id);

            EntityCollection<ValorizacionItemInsumo> vii = ValorizacionesDalc.ValorizacionItemInsumoReadByValorizacionItemId(itemIds);
            EntityCollection<ValorizacionItemCobInsumo> vici = ValorizacionesDalc.ValorizacionItemCobInsumoReadByValorizacionItemIds(itemIds);
            List<int> pts = new List<int>();
            pts.Add(PTs[i].Id);
            EntityCollection<TurnoDocumentacion> docs = TurnoDocumentacionReadByPracticasTurno(pts);
            dalEngine.Delete(vii);
            dalEngine.Delete(vici);
            dalEngine.Delete(VIsPT);
            dalEngine.Delete(docs);
            dalEngine.Delete(PTs[i]);
        }


        // Protocolo
        /// <summary>
        /// Crea un nuevo protocolo a partir de un servicioId
        /// </summary>
        /// <param name="turno">El turno al que pertenecera el protocolo</param>
        /// <param name="servicioId">El servicioId del protocolo</param>
        /// <returns>Un nuevo protocolo</returns>
        [RequiresTransaction]
        public virtual Protocolo ProtocoloCreateNew(Turno turno)
        {
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            if (turno.Equipo == null)
                turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);

            // Obtengo el Proximo Protocolo del Servicio
            SucursalProtocolo proximo = SucursalProtocoloObtenerSiguiente(turno.Equipo);

            return ProtocoloCreate(turno.Equipo, proximo.UltimoProtocolo);
        }

        internal IList<int> ProtocolosIdsReadByCodigo(string protocolo)
        {
            string hql = "select pro.Id from Protocolo pro where pro.ProtocoloFull like :protocolo ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetString("protocolo", protocolo.Trim().Replace(" ", "%") + "%");
            return query.List<int>();
        }

        private Protocolo ProtocoloCreate(Equipo equipo, int numero)
        {
            // Creo el nuevo Protocolo
            Protocolo protocolo = new Protocolo(equipo, numero);


            // Guardo
            return dalEngine.Update<Protocolo>(protocolo);
        }







        // NumeroEpo



        public EntityCollection<NumeroEpo> NumeroEpoReadByPartidoIds(List<int> partidoIds)
        {
            if (partidoIds == null || partidoIds.Count == 0)
                return new EntityCollection<NumeroEpo>();

            SQLBlockBuilder<int> itemsPartido = new SQLBlockBuilder<int>(partidoIds);
            string partIds = itemsPartido.BuildConstrainBlock("epo.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append(" select epo from NumeroEpo as epo ");
            hql.AppendFormat(" where {0}", partIds);

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<NumeroEpo>(query);
        }


        /// <summary>
        /// [RQ] Devuelve todos los NumeroEpo.
        /// </summary>
        /// <returns>Todas los NumeroEpo</returns>
        public ReadAllCollection<NumeroEpo> NumeroEpoReadAll()
        {
            return new ReadAllCollection<NumeroEpo>(dalEngine.GetAll<NumeroEpo>(NumeroEpo.Properties.Id));
        }


        // EstadoTurno
        // tiene un cache local para no buscar
        // lo mismo en varias invocaciones
        private SortedDictionary<long, Color> _colorByEstadoAndTipoAutorizacion;
        private Color GetColorByEstadoAndTipoAutorizacion(int estadoTurnoID, int tipoAutorizacionID, bool tratarGuardiaInternacion)
        {
            if (tratarGuardiaInternacion)
                return Color.LightBlue;

            long requestKey = estadoTurnoID + ((long)0x1000000) * tipoAutorizacionID;
            // lo busca en el cache
            Color ret;
            if (_colorByEstadoAndTipoAutorizacion != null)
            {
                if (_colorByEstadoAndTipoAutorizacion.TryGetValue(requestKey, out ret))
                    return ret;
            }
            // hace la consulta
            TipoAutorizacion ta = dalEngine.GetById<TipoAutorizacion>(tipoAutorizacionID);
            EstadoTurno et = EstadoTurnoReadById(estadoTurnoID);

            if (estadoTurnoID == (int)EstadoTurnoEnum.Reservado && ta.Color.HasValue)
                ret = Color.FromArgb(ta.Color.Value);
            else if (et.Color.HasValue)
                ret = Color.FromArgb(et.Color.Value);
            else
                ret = Color.White;
            // lo busca en el cache
            if (_colorByEstadoAndTipoAutorizacion == null)
                _colorByEstadoAndTipoAutorizacion = new SortedDictionary<long, Color>();
            _colorByEstadoAndTipoAutorizacion.Add(requestKey, ret);
            // devuelve    
            return ret;
        }






        //[Private]
        public EstadoTurno EstadoTurnoReadById(int id)
        {
            // Se toma la libetad de cachearse por thread...
            EstadoTurno ret = EntityThreadCache<EstadoTurno>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<EstadoTurno>(id);
                if (ret != null)
                    EntityThreadCache<EstadoTurno>.SetItem(id, ret);
            }
            return ret;
        }

        /// <summary>
        /// Retorna los estados siguientes a un estado dado. El método es Private porque para 
        /// obtenerse estos datos desde el cliente debe llamar a la clase Workflow.
        /// </summary>
        /// <param name="id">Id del estado</param>
        /// <returns>El estado con el id indicado</returns>
        [Private]
        public EntityCollection<EstadoTurno> EstadoTurnoObtenerSiguientes(int estadoID)
        {
            return EstadoTurnoObtenerSiguientes(estadoID, 0);
        }

        /// <summary>
        /// Retorna los estados siguientes a un estado dado. El método es Private porque para 
        /// obtenerse estos datos desde el cliente debe llamar a la clase Workflow.
        /// </summary>
        /// <param name="id">Id del estado</param>
        /// <param name="circuitoId">Id del circuito invocante</param>
        /// <returns>El estado con el id indicado</returns>
        [Private]
        public EntityCollection<EstadoTurno> EstadoTurnoObtenerSiguientes(int estadoID, int circuitoID)
        {

            // IMPORTANTE: si se introduce un cambio en este método, reflejar esa lógica en 
            // la clase Workflow dentro de ClientComponents/Cache.
            IQuery query = dalEngine.CreateQuery("SELECT DISTINCT ewf.Destino " +
                "FROM EstadoTurnoWorkflow ewf, EstadoTurno est " +
                "WHERE ewf.Destino = est.Id " +
                "and ewf.Origen = :estadoID " +
                "AND (:circuitoID <= 0 " +
                "OR  est.CircuitoId = :circuitoID) ");

            query.SetInt32("estadoID", estadoID);
            query.SetInt32("circuitoID", circuitoID);

            return dalEngine.GetManyByQuery<EstadoTurno>(query);
        }

        [AnonymousMethod]
        public ReadAllCollection<EstadoTurnoWorkflow> EstadoTurnoWorkflowReadAll()
        {
            return new ReadAllCollection<EstadoTurnoWorkflow>(dalEngine.GetAll<EstadoTurnoWorkflow>(EstadoTurnoWorkflow.Properties.Destino.Id));
        }

        /// <summary>
        /// Setea un nuevo estado para el turno y valida el cambio.
        /// </summary>
        /// <param name="turno">Turno a actualizar</param>
        /// <param name="estadoId">id del estado</param>
        [Private]
        public void TurnoAvanzarEstado(Turno turno, int estadoId, ValorizacionItemModalidadCoseguroEnum? modalidadCoseguro)
        {
            EstadoTurno stat = EstadoTurnoReadById(estadoId);
            if (stat == null)
                throw new Exception("No existe el estado [" + estadoId + "].");

            TurnoAvanzarEstado(turno, stat, modalidadCoseguro);
        }


        /// <summary>
        /// Avanza el turno al estado indicado, realizando todas las operaciones necesarias para ello.
        /// </summary>
        /// <param name="turno">Turno al que se le va a cambiar el estado.</param>
        /// <param name="estado">Estado al que se va a pasar el turno.</param>
        /// <param name="modalidadCoseguro">Obligatorio solo para "Si estoy Cancelando, Actualizo los Topes que Apliquen - Solo para turnosIds NO Provisorios"</param>
        [AnonymousMethod()]
        [RequiresTransaction]
        public virtual void TurnoAvanzarEstado(Turno turno, EstadoTurno estado, ValorizacionItemModalidadCoseguroEnum? modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;
            CajaDalc CajaDalc = Context.Session.CajaDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            if (turno == null)
                throw new Exception("El turno no puede ser null.");

            if (estado == null)
                throw new Exception("El estado no puede ser null.");

            // Chequeo que en efecto sea un cambio de estado
            if (turno.EstadoTurnoID != estado.Id)
            {
                // Obtengo la Colección de Posibles Estados
                EntityCollection<EstadoTurno> posibles = EstadoTurnoObtenerSiguientes(turno.EstadoTurnoID);

                // Chequeo que sea un cambio de estado valido
                if (posibles.Contains(estado))
                {
                    try
                    {
                        // No permito cancelar un turno con cobranza vigente
                        // Solo permito cancelar la ultima recitacion no cancelada
                        // No permito cancelar un turno si tiene recitaciones no canceladas
                        if (estado.Id == (int)EstadoTurnoEnum.Cancelado)
                        {

                            if (turno.CobranzaVigenteID.HasValue)
                            {

                                MovimientoCaja mov = dalEngine.GetById<MovimientoCaja>(turno.CobranzaVigenteID.Value);

                                // Chequeo que no sea un movimiento de devolución o anulación
                                if (mov.TipoMovimientoCajaID != (int)TipoMovimientoCajaEnum.Devolucion && mov.TipoMovimientoCajaID != (int)TipoMovimientoCajaEnum.Anulacion)
                                    throw new StatusException(
                                        "No se puede cancelar el turno dado que tiene una cobranza vigente.");
                            }

                            if (turno.TipoTurnoId == (int)TipoTurnoEnum.Recitado)
                            {
                                Turno posterior = TurnoReadByOriginalId(turno.TurnoOriginalID.Value, true);

                                // Si el turno actual y el "posterior" del original no son el mismo, cancelo
                                if (posterior != null && !turno.Equals(posterior))
                                    throw new StatusException("No se puede cancelar el turno ya que existe otra recitación, del turno original, no cancelada.");
                            }
                            else if (TurnoReadRecitacionesNoCanceladas(turno.Id).Count > 0)
                                throw new StatusException("No se puede cancelar el turno ya que tiene recitaciónes no canceladas.");
                        }

                        // Si es de una orden multiple no cerrada, entonces el turno no puede ser informado
                        if (estado.Id == (int)EstadoTurnoEnum.Informado && turno.Orden.EsMultiple)
                        {
                            Orden tom = turno.Orden;
                            if (tom != null && tom.Estado != (byte)OrdenEstadoEnum.Cerrada)
                                throw new StatusException("No se puede informar un turno de una orden múltiple hasta que la misma no se encuentre cerrada.");
                        }

                        // Me fijo que el paciente no sea No Grato
                        if (estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                        {
                            Paciente pac = Context.Session.TurnosDalc.PacienteReadById(turno.Orden.PacienteId);
                            if (pac.Importancia == (int)ImportanciaEnum.PersonaNoGrata)
                                throw new StatusException("No se puede recepcionar el turno ya que el paciente es una persona no grata.");
                        }

                        // Verifico que la orden este abierta. Si es asi aumento cumplidas y cierro si corresponde
                        if (estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                        {
                            Orden orden = turno.Orden;

                            if (turno.TipoTurnoId != (int)TipoTurnoEnum.Recitado)
                            {
                                if (orden.EsMultiple)
                                    orden.CantCumplidas++;
                                else
                                    orden.CantCumplidas = 1; // Aunque haya muchos turnosIds en la misma orden, siempre cantSesiones y cumplidas es 1
                            }

                            if (orden.Estado == (byte)OrdenEstadoEnum.Abierta && orden.CantCumplidas == orden.CantSesiones)
                                orden.Estado = (byte)OrdenEstadoEnum.Cerrada;

                            dalEngine.Update(orden);
                        }

                        // No permito cambiar el estado si la orden está entregada en facturación, 
                        // el estado actual es facturable y se lo pretende pasar a uno no facturable.
                        if (OrdenEnFacturacionConEstadoFacturableYFuturoNoFacturable(turno, estado))
                            throw new StatusException("La orden se encuentra en poder de facturación.");

                        // Obtengo el Turno Guardado
                        Turno dbTurno = TurnoReadById(turno.Id);


                        // Traigo el Estado Actual en el Historico
                        EstadoTurnoHistorico actual = EstadoTurnoHistoricoReadLast(turno);

                        if (actual == null)
                        {
                            CrearEstadoTurnoHistorico(false, dbTurno, null);
                            actual = EstadoTurnoHistoricoReadLast(turno);
                        }

                        // Actualizo el Estado en el Turno
                        dbTurno.EstadoTurnoID = estado.Id;

                        // Si estoy Cancelando, asigno el Motivo del Turno recibido
                        if (estado.Id == (int)EstadoTurnoEnum.Cancelado)
                        {
                            dbTurno.MotivoID = turno.MotivoID;
                            dbTurno.ObservacionesCancelacion = turno.ObservacionesCancelacion;

                            GuardarSeguimientoCancelacion(turno.Id, (EstadoTurnoMotivoEnum)turno.MotivoID);
                        }

                        // [JR] Si estoy Recepcionando y no tengo, asigno el Protocolo del Turno Recibido
                        if (estado.Id == (int)EstadoTurnoEnum.Recepcionado && dbTurno.Orden.Protocolo == null)
                            dbTurno.Orden.Protocolo = turno.Orden.Protocolo;

                        // Actualizo las sumarizaciones parciales
                        SummarizeTurnosFacturacion summarize = SummarizeTurnosFacturacion.GetInstance();
                        summarize.ChangeShiftState(dbTurno);

                        // Actualizo la orden
                        dalEngine.Update<Orden>(dbTurno.Orden);

                        // Actualizo el Turno
                        dbTurno = TurnoUpdateAndRefresh(dbTurno);

                        // Guardo en el Historico
                        CrearEstadoTurnoHistorico(false, dbTurno, actual.Id);

                        // Actualizo la TurnoLog
                        TurnoLogFechasEnum? tle = TurnoLog.TurnoLogFechasEnumGetByEstado(estado.Id);
                        if (tle.HasValue)
                            TurnoLogUpdate(turno.Id, tle.Value, true);

                        // Actualizo los indicadores del Paciente en caso que el estado del turno pase a Ausente o Cancelado.
                        // Ausente
                        if (estado.Id == (int)EstadoTurnoEnum.Ausente &&
                            turno.EsHuerfano == false)
                        {
                            IncrementarIndicadoresPaciente(turno, false, true, false);
                        }

                        // Cancelado
                        if (estado.Id == (int)EstadoTurnoEnum.Cancelado)
                        {
                            if (turno.MotivoID.HasValue && turno.Fecha.HasValue)
                            {
                                // Si el motivo es cancelado por el paciente con menos de 24 horas de anticipacion a la hora del turno.
                                if ((turno.MotivoID.Value == (int)EstadoTurnoMotivoEnum.Paciente && turno.Fecha.Value.AddHours(-24) < enfoke.Time.Now) || turno.MotivoID.Value == (int)EstadoTurnoMotivoEnum.PacienteLlegoTarde)
                                    IncrementarIndicadoresPaciente(turno, false, false, true);
                            }

                            EnviarMensajeCancelacion(turno);
                        }

                        // [JR] Recitados
                        // Si estoy modificando el estado de un recitado, tambien opero sobre su original
                        if (turno.TipoTurnoId == (int)TipoTurnoEnum.Recitado)
                        {
                            int? statusOriginal = null;

                            // Si estoy cancelando, marco como ARecitar a su original
                            // Si estoy ausentando, marco como RecitadoVencido a su original
                            // Si estoy recepcionando, marco como RecitadoCumplido a su original
                            if (estado.Id == (int)EstadoTurnoEnum.Cancelado)
                            {
                                statusOriginal = (int)EstadoTurnoEnum.ARecitar;
                            }
                            else if (estado.Id == (int)EstadoTurnoEnum.Ausente)
                                statusOriginal = (int)EstadoTurnoEnum.RecitadoVencido;
                            else if (estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                            {
                                statusOriginal = (int)EstadoTurnoEnum.RecitadoCumplido;

                                // Ademas lo logueo [piso la fecha de recepcion y blanqueo inicio y fin de practica]
                                TurnoLogUpdate(turno.TurnoOriginalID.Value, TurnoLogFechasEnum.Recepcion, true);
                                TurnoLogUpdate(turno.TurnoOriginalID.Value, TurnoLogFechasEnum.InicioPractica, false);
                                TurnoLogUpdate(turno.TurnoOriginalID.Value, TurnoLogFechasEnum.FinPractica, false);
                            }

                            if (statusOriginal.HasValue)
                            {
                                turno.TurnoOriginal = TurnoReadById(turno.TurnoOriginalID.Value);
                                if (statusOriginal.Value == (int)EstadoTurnoEnum.ARecitar)
                                    TurnoRevertirARecitarPorCancelacion(turno.TurnoOriginal);
                                else
                                {
                                    TurnoAvanzarEstado(turno.TurnoOriginal, EstadoTurnoReadById(statusOriginal.Value), modalidadCoseguro);
                                }
                            }
                        }


                        // [JR] Topes
                        // Si estoy Cancelando, Actualizo los Topes que Apliquen - Solo para turnosIds NO Provisorios
                        if (estado.Id == (int)EstadoTurnoEnum.Cancelado &&
                            turno.TipoTurnoId != (int)TipoTurnoEnum.Provisorio &&
                            turno.TipoTurnoId != (int)TipoTurnoEnum.EnEspera && turno.TipoTurnoId != (int)TipoTurnoEnum.Presupuesto)
                        {
                            try
                            {
                                // Obtengo el Plan, Necesito la OS
                                if (turno.Orden.ObraSocialPlan == null)
                                    turno.Orden.ObraSocialPlan = ObrasSocialesDalc.ObraSocialPlanReadById(turno.Orden.ObraSocialPlanId);

                                // Chequeo que existan topes para la OS
                                if (ObraSocialHasTopes(turno.Orden.ObraSocialPlan.ObraSocial.Id))
                                {
                                    // Obtengo la Practica Principal - Para contarla y por el Servicio
                                    PracticaTurno ptp = PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Principal)[0];

                                    // Cuento la Practica Principal
                                    int practicas = (int)Decimal.Round(ptp.Cantidad, MidpointRounding.AwayFromZero);

                                    // Cuento las Practicas Subsiguientes
                                    EntityCollection<PracticaTurno> PTs = PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Exposicion);
                                    foreach (PracticaTurno pt in PTs)
                                        practicas += (int)Decimal.Round(pt.Cantidad, MidpointRounding.AwayFromZero);

                                    // Obtengo el monto de las prácticas
                                    int monto = ValorizacionesDalc.ObtenerTotalOSValorizacionPresupuesto(turno, modalidadCoseguro.Value);

                                    // Los niego para restar (poner disponible)
                                    practicas *= -1;
                                    monto *= -1;

                                    // Actualizo los Topes que Apliquen
                                    TopesUpdate(turno.Fecha.Value, turno.Orden.ObraSocialPlan.ObraSocial.Id, ptp.Practica.ServicioEspecialidad.Servicio.Id, turno.EquipoId.GetValueOrDefault(0), ptp.MedicoInformante.Id, practicas, monto);
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Error al actualizar los topes.", ex);
                            }
                        }


                        // [JR] EquipoGeneraArchivo
                        // Si estoy recepcionando, chequeo si el equipo genera archivo
                        if (estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                        {
                            if (turno.EquipoId.HasValue)
                            {
                                try
                                {
                                    if (turno.Equipo == null)
                                        turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);

                                    switch (turno.Equipo.FormatoXML)
                                    {
                                        case (int)FormatoXMLEnum.NoGenera:
                                            break;
                                        case (int)FormatoXMLEnum.AGFA:
                                            Turno turnoUtilizar = turno;

                                            // Si es un recitado, tengo que utilizar el original
                                            if (turno.TipoTurnoId == (int)TipoTurnoEnum.Recitado)
                                            {
                                                if (turno.TurnoOriginal == null)
                                                    turno.TurnoOriginal = TurnoReadById(turno.TurnoOriginalID.Value);

                                                turnoUtilizar = turno.TurnoOriginal;
                                            }

                                            if (turnoUtilizar.Orden.Paciente == null)
                                                turnoUtilizar.Orden.Paciente = PacienteReadById(turnoUtilizar.Orden.PacienteId);

                                            string protocolo = turnoUtilizar.Orden.Protocolo.ProtocoloFull.Replace("-", String.Empty);

                                            if (turnoUtilizar.Equipo == null)
                                                turnoUtilizar.Equipo = EquiposDalc.EquipoReadById(turnoUtilizar.EquipoId.Value);

                                            if (estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                                            {
                                                string sexo = String.Empty;
                                                switch (turnoUtilizar.Orden.Paciente.Sexo)
                                                {
                                                    case Sexo.Femenino:
                                                        sexo = "F";
                                                        break;
                                                    case Sexo.Masculino:
                                                        sexo = "M";
                                                        break;
                                                }

                                                ArchivoAGFAAgregarPaciente(turnoUtilizar.Equipo, turnoUtilizar.Orden.Paciente.Apellido, turnoUtilizar.Orden.Paciente.Nombre, turnoUtilizar.Orden.Paciente.Dni.ToString(), protocolo, sexo, turnoUtilizar.Orden.Paciente.FechaDeNacimiento.Value.ToString("yyyyMMdd"));
                                            }
                                            else if (estado.Id == (int)EstadoTurnoEnum.FinPractica || estado.Id == (int)EstadoTurnoEnum.Informado)
                                                ArchivoAGFAFinPracticaPaciente(protocolo);
                                            break;
                                        default:
                                            break;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    throw new Exception("Error al actualizar el archivo del equipo.", ex);
                                }
                            }
                        }

                    }
                    catch (NotTranslatedFieldException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        if (ex is StatusException)
                            throw new StatusException("No se pudo actualizar el estado del turno." + Environment.NewLine + ex.Message, ex);
                        else
                            throw new Exception("No se pudo actualizar el estado del turno.", ex);
                    }
                }
                else
                {
                    throw new StatusException("No puede cambiar el estado del turno de " + EstadoTurnoReadById(turno.EstadoTurnoID).Name + " a " + estado.Name);
                }
            }
        }

        private void EnviarMensajeCancelacion(Turno turno)
        {
            if (turno.Equipo == null && turno.EquipoId.HasValue)
                turno.Equipo = Context.Session.Dalc.GetById<Equipo>(turno.EquipoId.Value);
            if (turno.Orden.Paciente == null)
                turno.Orden.Paciente = Context.Session.Dalc.GetById<Paciente>(turno.Orden.PacienteId);
            if (turno.Orden.Paciente.Localidad == null && turno.Orden.Paciente.LocalidadID.HasValue)
                turno.Orden.Paciente.Localidad = Context.Session.Dalc.GetById<Localidad>(turno.Orden.Paciente.LocalidadID.Value);
            else
                turno.Orden.Paciente.Localidad = new Localidad();

            HL7Turno hl7Turno = new HL7Turno(turno.Orden.Paciente, turno, Data.Security.Current.UserInfo.User);
            Context.EventProcessor.ProcessEvent<CancelacionTurno, HL7Turno>(hl7Turno);
        }

        private bool OrdenEnFacturacionConEstadoFacturableYFuturoNoFacturable(Turno turno, EstadoTurno estadoFuturo)
        {
            EstadoTurno original = EstadoTurnoReadById(turno.EstadoTurnoID);

            return turno.Orden.EntregaOrden == TipoEntregaOrdenEnum.Entregada &&
                   original.Facturable == true &&
                   estadoFuturo.Facturable == false;
        }

        /// <summary>
        /// Revierte el estado del turno hasta queda en estado arecitar
        /// </summary>
        /// <param name="turnoOriginal">turno original</param>
        public void TurnoRevertirARecitarPorCancelacion(Turno turnoOriginal)
        {
            // Traigo el Último Turno del Historico
            EstadoTurnoHistorico actual = EstadoTurnoHistoricoReadLast(turnoOriginal);
            while (turnoOriginal.EstadoTurnoID != (int)EstadoTurnoEnum.ARecitar)
            {
                // Controlar si es correcto mandar este cirucito
                TurnoRevertirEstado(turnoOriginal, actual.Estado.CircuitoId);
            }
        }

        [RequiresTransaction]
        public virtual void CrearEstadoTurnoHistorico(bool isRollback, Turno turno, int? estadoPrevioId)
        {
            // Esto lo hago porque parece que cuando hago el insert de la entidad con id 0 me da error.
            if (turno.EstadoTurnoID == (int)EstadoTurnoEnum.Ninguno)
            {
                EstadoTurnoHistoricoLight ethl = new EstadoTurnoHistoricoLight();

                SecurityUser user = Security.Current.UserInfo.User;
                // Asigno los nuevos datos del Historico
                ethl.TurnoId = turno.Id;
                ethl.EstadoId = turno.EstadoTurnoID;
                ethl.Fecha = enfoke.Time.Now;
                ethl.UsuarioId = user.Id;
                ethl.MotivoID = turno.MotivoID;
                ethl.HistoricoPrevioId = estadoPrevioId;

                // Guardo
                ethl = dalEngine.Update<EstadoTurnoHistoricoLight>(ethl);
            }
            else
            {
                EstadoTurnoHistorico eth = new EstadoTurnoHistorico();
                SecurityUser user = Security.Current.UserInfo.User;
                // Asigno los nuevos datos del Historico
                eth.TurnoId = turno.Id;
                eth.Estado = EstadoTurnoReadById(turno.EstadoTurnoID);
                eth.Fecha = enfoke.Time.Now;
                eth.User = user;
                eth.MotivoID = turno.MotivoID;
                eth.HistoricoPrevioId = estadoPrevioId;

                // Guardo
                eth = dalEngine.Update<EstadoTurnoHistorico>(eth);
            }
            // Registro Log
            EstadoTurno actual = EstadoTurnoReadById(turno.EstadoTurnoID);
            string log = String.Format("Se {0} el estado del turno a {1}", isRollback ? "revirtió" : "modificó", actual.Name);
            if (turno.MotivoID.HasValue)
            {
                EstadoTurnoMotivo etm = EstadoTurnoMotivoReadById(turno.MotivoID.Value);
                log += " (" + etm.Name + ")";
                bool isShiftCancel = ((EstadoTurnoEnum)turno.EstadoTurnoID) == EstadoTurnoEnum.Cancelado;
                if (isShiftCancel && !String.IsNullOrEmpty(turno.ObservacionesCancelacion))
                    log += ". Observación: " + turno.ObservacionesCancelacion.Trim();
            }

            LogRegistrar((int)LogEventoEnum.CambioEstado, log, turno.Id);
        }

        public EstadoTurnoHistorico EstadoTurnoHistoricoReadLast(Turno turno)
        {
            EntityCollection<EstadoTurnoHistorico> historicos = dalEngine.GetManyByProperty<EstadoTurnoHistorico>(EstadoTurnoHistorico.Properties.TurnoId, turno.Id, EstadoTurnoHistorico.Properties.Id, SortOrder.Descending);

            if (historicos != null && historicos.Count > 0)
            {
                if (historicos[0].HistoricoPrevioId.HasValue)
                    historicos[0].HistoricoPrevio = dalEngine.GetById<EstadoTurnoHistorico>(historicos[0].HistoricoPrevioId.Value);

                return historicos[0];
            }
            else
                return null;
        }







        [AnonymousMethod()]
        public Turno TurnoRevertirEstado(Turno turno, CircuitoEnum circuito)
        {
            return TurnoRevertirEstado(turno, (int)circuito);
        }

        /**
         * [JR] Creo esta Sobrecarga para decidir si comienzo/termino una Transaction o no
         * Lo necesito ya que la transaccion la manejo yo desde otra rutina
         * */
        /// <summary>
        /// Vuelve el turno a su estado anterior
        /// </summary>
        /// <param name="turno">El turno al que cambiar de estado</param>
        /// <param name="circuito">El circuito desde donde se realiza la accion</param>
        /// <returns>El turno actualizado</returns>
        [AnonymousMethod()]
        public Turno TurnoRevertirEstado(Turno turno, int circuito)
        {
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            // Traigo el Último Turno del Historico
            EstadoTurnoHistorico actual = EstadoTurnoHistoricoReadLast(turno);
            if (!actual.HistoricoPrevioId.HasValue)
                throw new Exception("No se pudo revertir porque no hay estado previo para el turno.");

            // Chequeo que desde el Circuito Actual se pueda realizar el Rollback
            if (actual.Estado.CircuitoId != circuito)
                throw new Exception("No se puede revertir un turno que no fue iniciado.");

            EstadoTurnoHistorico previo = actual.HistoricoPrevio;

            // Actualizo el Estado en el Turno
            turno.EstadoTurnoID = previo.Estado.Id;

            // Chequeo si tengo que desmarcar la practica finalizada
            if (previo.Estado.Id == (int)EstadoTurnoEnum.Recepcionado || previo.Estado.Id == (int)EstadoTurnoEnum.InicioPractica)
            {
                if (turno.Equipo == null)
                    turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);

                if (turno.Equipo.FormatoXML == (int)FormatoXMLEnum.AGFA)
                    ArchivoAGFARevertirFinPracticaPaciente(turno.Orden.Protocolo.ProtocoloFull.Replace("-", String.Empty));
            }

            // Guardo el Estado Previo
            int? previoEstadoId = previo.HistoricoPrevioId.HasValue ? previo.HistoricoPrevioId : (int?)null;

            // Actualizo el Historico
            CrearEstadoTurnoHistorico(true, turno, previoEstadoId);

            // Si Vuelvo de Recepcionado, descarto el Protocolo
            if (actual.Estado.Id == (int)EstadoTurnoEnum.Recepcionado)
                if (turno.TipoTurnoId != (int)TipoTurnoEnum.Recitado)
                    turno.Orden.Protocolo = null;

            // Actualizo el Turno
            return TurnoUpdateAndRefresh(turno);
        }

        internal void CambiarEstadoAlTurnoSiCorrespondeSegunElDelInforme(TurnoInforme informe, ValorizacionItemModalidadCoseguroEnum? modalidadCoseguro)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;

            bool pasar = true;

            // Chequeo que el resto de los informes también estén en estado igual o posterior
            EntityCollection<TurnoInforme> informes = InformesDalc.TurnoInformeReadByTurno(informe.TurnoID);

            for (int i = 0; pasar == true && i < informes.Count; i++)
            {
                TurnoInforme ti = informes[i];

                // Salteo el informe actual
                if (ti.Id != informe.Id)
                {
                    // Si el estado es el mismo, esta todo bien, sino lo busco via workflow
                    if (ti.EstadoInforme.Equals(informe.EstadoInforme))
                        continue;
                    else if (ti.CircuitoInforme == null)
                        pasar = false;
                    else if (InformesDalc.EstadoInformeBuscarEnSiguientes(informe.EstadoInforme, ti.EstadoInforme, ti.CircuitoInforme.Id) == false)
                        pasar = false;
                }
            }

            if (pasar && informe.EstadoInforme.EstadoTurno != null)
            {
                Turno turno = TurnoReadById(informe.TurnoID);
                TurnoAvanzarEstado(turno, informe.EstadoInforme.EstadoTurno, modalidadCoseguro);
            }
        }



        // TipoDocumento
        [AnonymousMethod()]
        public EntityCollection<TipoDocumento> TipoDocumentoReadAll()
        {
            return dalEngine.GetAll<TipoDocumento>();
        }

        [AnonymousMethod()]
        public TipoDocumento TipoDocumentoReadById(int id)
        {
            return dalEngine.GetById<TipoDocumento>(id);
        }


        // TipoRecitado
        [AnonymousMethod()]
        public EntityCollection<TipoRecitado> TipoRecitadoReadAll()
        {
            ReadManyCommand<TipoRecitado> readCmd = new ReadManyCommand<TipoRecitado>(dalEngine);

            return readCmd.Execute();
        }







        // TipoTurno
        public ReadAllCollection<TipoTurno> TipoTurnoReadAll()
        {
            return new ReadAllCollection<TipoTurno>(dalEngine.GetAll<TipoTurno>(
                TipoTurno.Properties.Id));
        }












        // Turno

        public EntityCollection<Turno> TurnosReadByMedico(int medicoId)
        {
            string hql = "select distinct t from Turno t, PracticaTurno pt "
                        + "where t.Id = pt.TurnoId "
                        + "AND pt.Medico.Id = :medicoId "
                        + "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("medicoId", medicoId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public List<int> TurnoIdsReadByComprobanteId(int comprobanteId)
        {
            // Traigo todos los protocolos que estan metidos
            // en el comprobante del pasado en el parámetro

            StringBuilder hql = new StringBuilder(" select distinct prt.Turno.Id ");
            hql.Append("from PracticaTurnoHQL prt ");
            hql.Append(" inner join prt.ComprobanteItem coi ");
            hql.Append("   where coi.ComprobanteID = :comprobanteId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            List<int> turnosId = (List<int>)query.List<int>();
            return turnosId;
        }

        public EntityCollection<Turno> TurnoProtocoloReadByTurnos(List<int> turnosId)
        {
            if (turnosId == null || turnosId.Count == 0)
                return new EntityCollection<Turno>();

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(turnosId);
            string turnos = blockBuilder.BuildConstrainBlock("tur.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Turno(tur.Id, tur.Orden.Protocolo.ProtocoloFull) ");
            hql.Append(" from Turno tur ");
            hql.AppendFormat(" where {0} ", turnos);
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnoPlanNameByTurnos(List<int> turnosId)
        {
            if (turnosId == null || turnosId.Count == 0)
                return new EntityCollection<Turno>();

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(turnosId);
            string turnos = blockBuilder.BuildConstrainBlock("tur.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Turno(tur.Id, osp.Name, osp.ObraSocial.Name) ");
            hql.Append(" from Turno tur, ObraSocialPlan osp ");
            hql.Append(" where tur.Orden.ObraSocialPlanId = osp.Id ");
            hql.AppendFormat(" and {0} ", turnos);
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<Turno>(query);
        }
        // Huerfanos


        [Private]
        public EntityCollection<Turno> TurnosAHuerfanosReadByFechaFechaAndNotInSucursales(DateTime dia, EntityCollection<SucursalName> sucursales)
        {
            if (sucursales.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tur from Turno tur, Equipo equ");
            hqlBuilder.Append(" where tur.EquipoId = equ.Id");
            hqlBuilder.Append(" and tur.EstadoTurnoID = :estado");
            hqlBuilder.Append(" and tur.EsHuerfano = false");
            hqlBuilder.Append(" and tur.Deleted = false");
            hqlBuilder.Append(" and tur.Fecha >= :dia");
            hqlBuilder.Append(" and tur.Fecha < :diaSiguiente");
            if (sucursales != null && sucursales.Count > 0)
                hqlBuilder.Append(" and equ.Sucursal.Id NOT in (:sucursales)");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetDateTime("dia", dia.Date);
            query.SetDateTime("diaSiguiente", dia.Date.AddDays(1));
            query.SetInt32("estado", (int)EstadoTurnoEnum.Reservado);

            if (sucursales != null && sucursales.Count > 0)
                query.SetParameterList("sucursales", sucursales.GetIds());

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        /// <summary>
        /// Devuelve los turnosIds disponibles para un equipo y cualquier medicoId.
        /// </summary>
        /// <param name="desde">Día desde donde buscar el turno</param>
        /// <param name="hasta">Día hasta donde buscar el turno</param>
        /// <param name="equipoId">Equipo del cual se desea obtener los turnosIds disponibles</param>
        /// <returns>Turnos asignados para el equipo en el período dado</returns>
        [Private]
        public List<int> TurnoIdsReadByEquipo(DateTime desde, DateTime hasta, int equipoId)
        {
            // Busco 2 horas antes de lo que debo y despues filtro para quedarme con lo que corresponde
            // Si es lo hago dentro del querty no entra por indice y tarda mucho
            desde = desde.AddHours(-2);

            string hql = "SELECT distinct new enfoke.Eges.Entities.Results.TurnoForHuerfanos(t.Id, t.Fecha, t.DuracionSeconds) " +
                         "FROM Turno t " +
                         "WHERE t.Fecha BETWEEN :desde AND :hasta " +
                         "AND t.Activo = true " +
                         "AND t.Deleted = false " +
                         "AND t.EstadoTurnoID NOT IN (:estados) " +
                         "AND t.EquipoId = :idEquipo ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1));
            query.SetParameterList("estados", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });
            query.SetParameter("idEquipo", equipoId);

            // Ahora vuelvo la hora desde y me fijo segun la duracion del turno, cuales no corresponden devolver
            List<int> ret = new List<int>();
            EntityCollection<TurnoForHuerfanos> huerfanosPosibles = dalEngine.GetManyByQuery<TurnoForHuerfanos>(query);
            desde = desde.AddHours(2);
            foreach (TurnoForHuerfanos turHuerfano in huerfanosPosibles)
            {
                if (turHuerfano.FechaTurno.AddSeconds(turHuerfano.DuracionSeconds) >= desde)
                    ret.Add(turHuerfano.Id);
            }

            return ret;
        }

        /// <summary>
        /// Devuelve los turnosIds disponibles para un equipo y cualquier medicoId.
        /// </summary>
        /// <param name="desde">Día desde donde buscar el turno</param>
        /// <param name="hasta">Día hasta donde buscar el turno</param>
        /// <param name="equipoId">Equipo del cual se desea obtener los turnosIds disponibles</param>
        /// <returns>Turnos asignados para el equipo en el período dado</returns>
        [Private]
        public EntityCollection<Turno> TurnoReadByEquipo(DateTime desde, DateTime hasta, int equipoId)
        {
            return TurnosReadByIds(TurnoIdsReadByEquipo(desde, hasta, equipoId));
        }


        /// <summary>
        /// Obtiene los turnosIds en base a los id de sus informes
        /// </summary>
        /// <param name="turnoInformeIds"></param>
        /// <returns></returns>
        public EntityCollection<Turno> TurnosReadByInformesUnificadosIds(List<int> turnoInformeIds)
        {
            if (turnoInformeIds == null || turnoInformeIds.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hql = new StringBuilder();
            hql.Append(" select tur ");
            hql.Append(" from Turno tur, TurnoInforme tui ");
            hql.Append(" where  tui.TurnoID = tur.Id ");
            hql.Append(" and  (tui.Id in (:turnoInformeIds) ");
            hql.Append(" or  tui.TurnoInformePrincipalID in (:turnoInformeIds))");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnoInformeIds", turnoInformeIds);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        /// <summary>
        /// Devuelve los turnosIds disponibles
        /// </summary>
        /// <param name="date">Día desde donde buscar el turno</param>
        /// <returns>Turnos del dia solicitado</returns>
        public EntityCollection<Turno> TurnoReadByFecha(DateTime fecha)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            // se crea el filtro por la fecha, quitando la hora
            filter.Add(Turno.Properties.Fecha, ">=", fecha.Date);
            filter.Add(BooleanOp.And, Turno.Properties.Fecha, "<", fecha.Date.AddDays(1));
            filter.Add(BooleanOp.And, Turno.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "NOT IN", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<Turno> TurnosReadByDiaAndSucursales(DateTime dia, EntityCollection<SucursalName> sucursales)
        {
            if (sucursales.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tur from Turno tur, Equipo equ, PracticaTurno ptu");
            hqlBuilder.Append(" where tur.EquipoId = equ.Id");
            hqlBuilder.Append(" and ptu.TurnoId= tur.Id");
            hqlBuilder.Append(" and tur.EsHuerfano = false");
            hqlBuilder.Append(" and tur.Deleted = false");
            hqlBuilder.Append(" and tur.EstadoTurnoID = :estado");
            hqlBuilder.Append(" and tur.Fecha >= :dia");
            hqlBuilder.Append(" and tur.Fecha < :diaSiguiente");
            if (sucursales != null && sucursales.Count > 0)
                hqlBuilder.Append(" and equ.Sucursal.Id in (:sucursales)");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetDateTime("dia", dia.Date);
            query.SetDateTime("diaSiguiente", dia.Date.AddDays(1));
            query.SetInt32("estado", (int)EstadoTurnoEnum.Reservado);

            if (sucursales != null && sucursales.Count > 0)
                query.SetParameterList("sucursales", sucursales.GetIds());

            return dalEngine.GetManyByQuery<Turno>(query);
        }


        public decimal TurnoSumaImporteOrdenMedica(List<int> turnosId)
        {
            decimal importe = 0;
            EntityCollection<Turno> turnos = dalEngine.GetManyByIds<Turno>(turnosId);
            foreach (Turno turno in turnos)
                importe = importe + turno.ImporteOrdenMedica;

            return importe;
        }

        /// <summary>
        /// Actualiza o da de alta un turno
        /// </summary>
        [AnonymousMethod()]
        public void TurnoUpdate(Turno turno)
        {
            TurnoUpdateAndRefresh(turno);
        }

        /// <summary>
        /// Actualiza o da de alta un turno
        /// </summary>
        /// <param name="turno">Turno a actualizar</param>
        /// <returns>El turno actualizado</returns>
        public Turno TurnoUpdateAndRefresh(Turno turno)
        {
            // [JR] Chequeo Reservado
            // Porque estaban sucediendo cosas raras agregamos un chequeo
            // Si estoy guardando un turno reservado y ya existia (sin ser provisorio), doy error
            Turno dbTurno = null;

            if (turno.Id != 0)
            {
                dbTurno = TurnoReadById(turno.Id);

                if (turno.EstadoTurnoID == (int)EstadoTurnoEnum.Reservado)
                {
                    if (dbTurno != null &&
                        (!(dbTurno.MotivoID.GetValueOrDefault(0) == (int)EstadoTurnoMotivoEnum.Reprogramacion || dbTurno.EstadoTurnoID == (int)EstadoTurnoEnum.Ninguno || dbTurno.EstadoTurnoID == (int)EstadoTurnoEnum.ReservaProvisoria || dbTurno.EstadoTurnoID == (int)EstadoTurnoEnum.Reservado)))
                        throw new Exception("Imposible guardar el turno con estado Reservado.");

                    if (dbTurno != null && turno.EstadoTurnoID == (int)EstadoTurnoEnum.Reservado && dbTurno.MotivoID.GetValueOrDefault(0) == (int)EstadoTurnoMotivoEnum.Reprogramacion)
                        turno.MotivoID = null;
                }

                // Hubo cambio en inhibición de informe, por lo tanto logueo.
                if (dbTurno != null && dbTurno.TipoInhibicionEntregaID != turno.TipoInhibicionEntregaID)
                {
                    LoguearCambioEnInhibicionInforme(turno.Id, turno.TipoInhibicionEntregaID);
                }
            }


            turno = dalEngine.Update<Turno>(turno);

            if (turno.Orden != null && turno.Orden.Id > 0)
            {
                turno.Orden = dalEngine.Update(turno.Orden);
            }

            if ((dbTurno != null && turno.TipoConfirmacion != null && dbTurno.TipoConfirmacionID != turno.TipoConfirmacionID) || (dbTurno == null && turno.TipoConfirmacionID > 0))
            {
                GuardarSeguimientoConfirmacion(turno.Id, (int)turno.TipoConfirmacionID);
            }

            return turno;
        }

        private void GuardarSeguimientoCancelacion(int turnoId, EstadoTurnoMotivoEnum estadoTurnoMotivo)
        {
            ReadManyCommand<SeguimientoTurno> readCmd = new ReadManyCommand<SeguimientoTurno>(dalEngine);

            Filter filter = new Filter();
            filter.Add(SeguimientoTurno.Properties.Turno.Id, "=", turnoId);
            readCmd.Filter = filter;

            EntityCollection<SeguimientoTurno> sts = readCmd.Execute();

            int? idSeguimientoAccion = (int)SeguimientoAccionEnum.Cancelar;

            if (estadoTurnoMotivo == EstadoTurnoMotivoEnum.Reprogramacion)
            {
                idSeguimientoAccion = (int)SeguimientoAccionEnum.Reprogramar;
            }

            foreach (SeguimientoTurno st in sts)
            {
                List<int> turnoIds = new List<int>();
                turnoIds.Add(turnoId);

                SeguimientoTurnoUpdateGlobal(turnoIds, idSeguimientoAccion, null, null, string.Empty, (TipoSeguimientoEnum)st.TipoSeguimiento.Id);
            }
        }

        public void LoguearCambioEnInhibicionInforme(int turnoId, int tipoInhibicionEntregaID)
        {
            string msg = string.Empty;

            switch (tipoInhibicionEntregaID)
            {
                case (int)TipoInhibicionEntregaEnum.InhibidaManualmente:
                    msg = "La entrega del infome se ha inhibido manualmente.";
                    break;
                case (int)TipoInhibicionEntregaEnum.InhibidaPorDebeOrden:
                    msg = "La entrega del infome se ha inhibido por Debe Orden.";
                    break;
                case (int)TipoInhibicionEntregaEnum.NoInhibida:
                    msg = "La entrega del infome se ha desinhibido.";
                    break;
            }

            LogRegistrar((int)LogEventoEnum.ModificacionDatosTurno, msg, turnoId);
        }

        private void LoguearSiCambioConceptoOrdenMedica(int turnoId, int debeOrdenMedica)
        {
            LoguearSiCambioConceptoOrdenMedica(turnoId, debeOrdenMedica, false);
        }

        private void LoguearSiCambioConceptoOrdenMedica(int turnoId, int debeOrdenMedica, bool forzarLog)
        {
            if (forzarLog || debeOrdenMedica != (int)(TurnoReadById(turnoId)).Orden.DebeOrdenMedica)
            {
                LogRegistrar((int)LogEventoEnum.ModificacionDatosTurno, "Se modificó el concepto de orden médica a: " +
                (dalEngine.GetById<DebeOrdenMedica>((int)debeOrdenMedica)).Name + ".", turnoId);
            }
        }

        /// <summary>
        /// Actualiza el Log de un Turno
        /// </summary>
        /// <param name="turnoID">ID del Turno a Actualizar el Log</param>
        /// <param name="log">Dato a Actualizar en el Log</param>
        /// <param name="setear">Marca si estoy logueando o des-logueando</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [AnonymousMethod()]
        public void TurnoLogUpdate(int turnoID, TurnoLogFechasEnum log, bool setear, DateTime? fecha)
        {
            List<int> turnosId = new List<int>();
            turnosId.Add(turnoID);
            TurnoLogItemUpdateBatch(turnosId, log, setear, fecha);
        }


        /// <summary>
        /// Actualiza el Log de un Turno
        /// </summary>
        /// <param name="turnoID">ID del Turno a Actualizar el Log</param>
        /// <param name="log">Dato a Actualizar en el Log</param>
        /// <param name="setear">Marca si estoy logueando o des-logueando</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [AnonymousMethod()]
        public void TurnoLogUpdate(int turnoID, TurnoLogFechasEnum log, bool setear)
        {
            this.TurnoLogUpdate(turnoID, log, setear, null);
        }

        public void TurnoLogUpdateFechasWizard(int turnoId, DateTime? inicioWizard)
        {
            List<int> turnosIds = new List<int>();
            turnosIds.Add(turnoId);
            TurnosLogUpdateFechasWizard(turnosIds, inicioWizard);
        }

        [RequiresTransaction]
        public virtual void TurnosLogUpdateFechasWizard(List<int> turnosId, DateTime? inicioWizard)
        {
            this.TurnoLogItemUpdateBatch(turnosId, TurnoLogFechasEnum.InicioWizard, true, inicioWizard);
            this.TurnoLogItemUpdateBatch(turnosId, TurnoLogFechasEnum.FinWizard, true);
        }

        /// <summary>
        /// Actualiza el Log de un Turno
        /// </summary>
        /// <param name="turnoID">ID del Turno a Actualizar el Log</param>
        /// <param name="log">Dato a Actualizar en el Log</param>
        /// <param name="setear">Marca si estoy logueando o des-logueando</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [Private]
        public void TurnoLogItemUpdateBatch(List<int> turnosId, TurnoLogFechasEnum log, bool setear)
        {
            this.TurnoLogItemUpdateBatch(turnosId, log, setear, null);
        }

        /// <summary>
        /// Actualiza el Log de un Turno
        /// </summary>
        /// <param name="turnoID">ID del Turno a Actualizar el Log</param>
        /// <param name="log">Dato a Actualizar en el Log</param>
        /// <param name="setear">Marca si estoy logueando o des-logueando</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [Private]
        public void TurnoLogItemUpdateBatch(List<int> turnosId, TurnoLogFechasEnum log, bool setear, DateTime? fecha)
        {
            int? userValue = null;
            if (setear)
            {
                SecurityUser user = Security.Current.UserInfo.User;
                userValue = user.Id;
            }

            TurnoLogItemUpdateBatch(turnosId, log, setear, fecha, userValue);
        }

        [Private]
        public void TurnoLogItemUpdateBatch(List<int> turnosId, TurnoLogFechasEnum log, bool setear, DateTime? fecha, int? userId)
        {
            try
            {
                DateTime? dateValue = fecha.HasValue ? fecha.Value : enfoke.Time.Now;
                IPropertyReference propiedadFecha;
                IPropertyReference propiedadUsuario = null;
                // Lo modifica
                switch (log)
                {
                    case TurnoLogFechasEnum.Autorizacion:
                        propiedadFecha = TurnoLog.Properties.AutorizacionFecha;
                        propiedadUsuario = TurnoLog.Properties.AutorizacionUsuario;
                        break;
                    case TurnoLogFechasEnum.Cancelado:
                        propiedadFecha = TurnoLog.Properties.CanceladoFecha;
                        propiedadUsuario = TurnoLog.Properties.CanceladoUsuario;
                        break;
                    case TurnoLogFechasEnum.Confirmacion:
                        propiedadFecha = TurnoLog.Properties.ConfirmacionFecha;
                        propiedadUsuario = TurnoLog.Properties.ConfirmacionUsuario;
                        break;
                    case TurnoLogFechasEnum.ControlFacturacion:
                        propiedadFecha = TurnoLog.Properties.ControlFacturacionFecha;
                        propiedadUsuario = TurnoLog.Properties.ControlFacturacionUsuario;
                        break;
                    case TurnoLogFechasEnum.EntregaInforme:
                        propiedadFecha = TurnoLog.Properties.EntregaInformeFecha;
                        propiedadUsuario = TurnoLog.Properties.EntregaInformeUsuario;
                        break;
                    case TurnoLogFechasEnum.FinPractica:
                        propiedadFecha = TurnoLog.Properties.FinPracticaFecha;
                        propiedadUsuario = TurnoLog.Properties.FinPracticaUsuario;
                        break;
                    case TurnoLogFechasEnum.Informado:
                        propiedadFecha = TurnoLog.Properties.InformadoFecha;
                        propiedadUsuario = TurnoLog.Properties.InformadoUsuario;
                        break;
                    case TurnoLogFechasEnum.InformeEnMesa:
                        propiedadFecha = TurnoLog.Properties.InformeEnMesaFecha;
                        propiedadUsuario = TurnoLog.Properties.InformeEnMesaUsuario;
                        break;
                    case TurnoLogFechasEnum.InformeEnRecepcion:
                        propiedadFecha = TurnoLog.Properties.InformeEnRecepcionFecha;
                        propiedadUsuario = TurnoLog.Properties.InformeEnRecepcionUsuario;
                        break;
                    case TurnoLogFechasEnum.InicioPractica:
                        propiedadFecha = TurnoLog.Properties.InicioPracticaFecha;
                        propiedadUsuario = TurnoLog.Properties.InicioPracticaUsuario;
                        break;
                    case TurnoLogFechasEnum.OrdenEnMesa:
                        propiedadFecha = TurnoLog.Properties.OrdenEnMesaFecha;
                        propiedadUsuario = TurnoLog.Properties.OrdenEnMesaUsuario;
                        break;
                    case TurnoLogFechasEnum.PedidoAutorizacion:
                        propiedadFecha = TurnoLog.Properties.PedidoAutorizacionFecha;
                        break;
                    case TurnoLogFechasEnum.Recepcion:
                        propiedadFecha = TurnoLog.Properties.RecepcionFecha;
                        propiedadUsuario = TurnoLog.Properties.RecepcionUsuario;
                        break;
                    case TurnoLogFechasEnum.Recitado:
                        propiedadFecha = TurnoLog.Properties.RecitadoFecha;
                        propiedadUsuario = TurnoLog.Properties.RecitadoUsuario;
                        break;
                    case TurnoLogFechasEnum.Reserva:
                        propiedadFecha = TurnoLog.Properties.ReservaFecha;
                        propiedadUsuario = TurnoLog.Properties.ReservaUsuario;
                        break;
                    case TurnoLogFechasEnum.FinWizard:
                        propiedadFecha = TurnoLog.Properties.FinWizardFecha;
                        break;
                    case TurnoLogFechasEnum.InicioWizard:
                        propiedadFecha = TurnoLog.Properties.InicioWirzardFecha;
                        break;
                    default:
                        throw new Exception("Valor del item de Enum de log " + log.ToString() + " no reconocido.");
                }

                // Setea la fecha
                dalEngine.UpdatePropertyBatchByProperty<TurnoLog>(TurnoLog.Properties.TurnoId, (IList<int>)turnosId, propiedadFecha, dateValue);

                // Setea el dato de usuario
                if (propiedadUsuario != null)
                    dalEngine.UpdatePropertyBatchByProperty<TurnoLog>(TurnoLog.Properties.TurnoId, (IList<int>)turnosId, propiedadUsuario, userId);
            }
            catch (Exception ex)
            {
                throw new Exception("Error al actualizar el log de turnos.", ex);
            }
        }

        [Private]
        private void FechaYUsuarioParaTurnoLog(TipoControlFacturacionEnum tipo, ref int? userValue, ref DateTime? dateValue)
        {
            userValue = null;
            dateValue = null;
            switch (tipo)
            {
                case TipoControlFacturacionEnum.NoControlado:
                case TipoControlFacturacionEnum.AFacturar:
                case TipoControlFacturacionEnum.Particular:
                    userValue = null;
                    dateValue = null;
                    break;
                default:
                    userValue = Security.Current.UserInfo.UserId;
                    dateValue = enfoke.Time.Now;
                    break;
            }
        }

        public TurnoLog TurnoLogReadByTurno(int turnoID)
        {
            return dalEngine.GetByProperty<TurnoLog>(TurnoLog.Properties.TurnoId, turnoID);
        }

        public EntityCollection<OrdenEntregaPlaca> OrdenEntregaPlacaByOrdenesId(List<int> ordenesId)
        {
            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(ordenesId);
            string ordenes = blockBuilder.BuildConstrainBlock("tur.Orden.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Results.OrdenEntregaPlaca(tur.Orden.Id, tui.EntregaPlacas) ");
            hql.Append(" from TurnoLight tur , TurnoInforme tui ");
            hql.Append(" where tui.TurnoID = tur.Id ");
            hql.AppendFormat(" and {0} ", ordenes);
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<OrdenEntregaPlaca>(query);
        }

        public EntityCollection<OrdenProtocolo> OrdenProtocoloReadByOrdenesId(List<int> ordenesId)
        {
            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(ordenesId);
            string ordenes = blockBuilder.BuildConstrainBlock("ord.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Results.OrdenProtocolo(ord.Id, pro.ProtocoloFull) ");
            hql.Append(" from OrdenLight ord, Protocolo pro ");
            hql.AppendFormat(" where ord.ProtocoloId = pro.Id and {0} ", ordenes);
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<OrdenProtocolo>(query);
        }

        [RequiresTransaction]
        public virtual void OrdenUpdateDatosEntregaExclusionLote(List<int> ids)
        {
            dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.LoteTraslado, null);
            dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.PosicionEnLote, null);
        }

        [RequiresTransaction]
        public virtual void OrdenUpdateDatosEntrega(List<int> ids, LoteTraslado lote, TipoEntregaOrdenEnum tipoEntregaOrden)
        {
            dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.LoteTraslado, (lote == null ? null : (int?)lote.Id));
            dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.dbEntregaOrdenId, (int)tipoEntregaOrden);

            // Si estoy eliminandolo del lote entonces reseteo la posicion
            if (lote == null)
                dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.PosicionEnLote, null);
        }

        [RequiresTransaction]
        public virtual void OrdenUpdateDatosEntrega(List<int> ids, TipoEntregaOrdenEnum tipoEntregaOrden)
        {
            dalEngine.UpdatePropertyBatchByIds<Orden>(ids, Orden.Properties.dbEntregaOrdenId, (int)tipoEntregaOrden);
        }






        private IList<int> OrdenIdsReadByTurnoIds(List<int> turnoIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select tur.Orden.Id ");
            hql.Append(" from Turno tur ");
            hql.Append(" where tur.Id in (:turnoIds) ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnoIds", turnoIds);
            return query.List<int>();
        }

        public Orden OrdenReadByTurnoId(int turnoId)
        {
            string hql = "select tur.Orden from Turno tur WHERE tur.Id = :turnoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoId", turnoId);
            return dalEngine.GetByQuery<Orden>(query);
        }


        public OrdenUpdateDiagnostico OrdenUpdateDiagnosticoReadByTurnoId(int turnoId)
        {
            string hql = "select oud from Turno tur, OrdenUpdateDiagnostico oud WHERE tur.Orden.Id = oud.Id and tur.Id = :turnoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoId", turnoId);
            return dalEngine.GetByQuery<OrdenUpdateDiagnostico>(query);
        }

        public EntityCollection<TurnoLight> TurnoLightReadByOrdenId(int ordenId)
        {
            string hql = "from TurnoLight tur WHERE tur.Orden.Id = :ordenId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ordenId", ordenId);
            return dalEngine.GetManyByQuery<TurnoLight>(query);
        }

        public bool TurnoExisteSinDistanciaMinimaDeMinutosEntreCentrosReadByTurnosPosibles(PracticaInfoCollection practicasInfo, int minutosDistancia)
        {
            return TurnoExisteSinDistanciaMinimaDeMinutosEntreCentrosReadByTurnosPosibles(practicasInfo, minutosDistancia, new List<int>());
        }

        /// <summary>
        /// Determina si existe o no otro turno sobre el mimso paciente en otro centro, donde la distancia de minutos
        /// entre uno y otro no supere la distancia minima parametrizada.
        /// </summary>
        /// <param name="posiblesTurnos">Posibles turnosIds seleccionados de la Agenda.</param>
        /// <param name="minutosDistancia">Cantidad de minutos para la distancia minima entre turnosIds.</param>
        /// <returns>TRUE si existe otro turno sin respetar el minimo parametrizado, FALSE si no se presenta la situacion.</returns>
        public bool TurnoExisteSinDistanciaMinimaDeMinutosEntreCentrosReadByTurnosPosibles(PracticaInfoCollection practicasInfo, int minutosDistancia, List<int> idsTurnosReprogrmados)
        {
            bool existeAlMenosUno = false;
            foreach (PracticaInfo pi in practicasInfo)
            {
                foreach (PosibleTurno pt in pi.PosiblesTurnos)
                {
                    string hql = "SELECT t FROM Turno t, Equipo e " +
                                 "WHERE t.EquipoId = e.Id " +
                                 "AND t.Fecha > :fechaMenos " +
                                 "AND t.Fecha < :fechaMas " +
                                 "AND t.Orden.PacienteId = :idPaciente " +
                                 "AND e.Sucursal != :sucursal " +
                                 "AND t.EstadoTurnoID = :estadoTurno ";

                    if (idsTurnosReprogrmados.Count > 0)
                        hql += " AND t.Id NOT IN (:idsTurnosReprogrmados)";

                    IQuery query = dalEngine.CreateQuery(hql);
                    query.SetParameter("fechaMenos", pt.StartDate.AddMinutes(-minutosDistancia));
                    query.SetParameter("fechaMas", pt.EndDate.AddMinutes(minutosDistancia));
                    query.SetParameter("idPaciente", pi.Paciente.Id);
                    query.SetParameter("sucursal", pt.Equipo.Sucursal);
                    if (idsTurnosReprogrmados.Count > 0)
                        query.SetParameterList("idsTurnosReprogrmados", idsTurnosReprogrmados);
                    query.SetParameter("estadoTurno", (int)EstadoTurnoEnum.Reservado);

                    query.SetMaxResults(1);

                    // Hace la consulta
                    EntityCollection<Turno> turnos = dalEngine.GetManyByQuery<Turno>(query);

                    existeAlMenosUno = ((turnos.Count > 0) || existeAlMenosUno);
                }
            }
            return existeAlMenosUno;
        }






        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="idObraSocialPlanParticulares">ID de la Obra Social Particular por si se tiene que usar para poder el importe a pagar.</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [Private]
        [RequiresTransaction]
        public virtual TurnoUpdateDatosTurno TurnoUpdate(Orden orden, TurnoUpdateDatosTurno turno, int idObraSocialPlanParticulares, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            // Asigno al campo ImporteAPagar lo que corresponde segun el tipo DebeOrdenMedica.
            turno.AsignarImportesAPagar(ObrasSocialesDalc.TipoPlanMayorIVAReadByObraSocialPlan(idObraSocialPlanParticulares),
                                        ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turno.Id, (int)ValorizacionTiposEnum.Admision), modalidadCoseguro);

            TurnoUpdateDatosTurno turnoOriginal = dalEngine.GetById<TurnoUpdateDatosTurno>(turno.Id);
            bool actualizarLog = true;

            if (TypeUtils.PublicInstancePropertiesEqual(turno, turnoOriginal))
                actualizarLog = false;

            // Actualizo
            dalEngine.Update<Orden>(orden);
            turno = dalEngine.Update<TurnoUpdateDatosTurno>(turno);

            // Registro Log
            if (actualizarLog)
                LogRegistrar((int)LogEventoEnum.ModificacionDatosTurno, "Se modificaron los datos del turno.", turno.Id);

            return turno;
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="turnoInformes">Coleccion de informes</param>
        /// <param name="idObraSocialPlanParticulares">ID de la Obra Social Particular por si se tiene que usar para poder el importe a pagar.</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [Private]
        [RequiresTransaction]
        public virtual TurnoUpdateDatosTurno TurnoUpdate(Orden orden, TurnoUpdateDatosTurno turno, EntityCollection<TurnoInforme> turnoInformes, int idObraSocialPlanParticulares, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            if (!turno.DebeOrdenMedica.HasValue)
                throw new Exception("El valor DebeOrdenMedica debe estar seteado.");


            LoguearSiCambioConceptoOrdenMedica(turno.Id, turno.DebeOrdenMedica.Value);
            dalEngine.UpdateCollection<TurnoInforme>(turnoInformes);

            return TurnoUpdate(orden, turno, idObraSocialPlanParticulares, modalidadCoseguro);
        }

        /// <summary>
        /// Actualiza los "Datos del Turno". No se debe usar para órdenes múltiples.
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [RequiresTransaction]
        public virtual TurnoUpdateObservaciones TurnoUpdate(TurnoUpdateObservaciones turno, OrdenUpdateDiagnostico orden)
        {
            if (turno.Observaciones != null && turno.Observaciones.Length > Turno.MAX_LONG_OBSERVACIONES)
                throw new NotLoggeableException("La longitud de las observaciones ingresadas supera el máximo permitido.");

            // Chequeo si realmente hay diferencias
            if (RegistrarLogCambioDiagnosticoObservaciones(turno, orden))
            {

                // Actualizo
                turno = dalEngine.Update<TurnoUpdateObservaciones>(turno);

                dalEngine.Update<OrdenUpdateDiagnostico>(orden);

                turno.Diagnostico = orden.Diagnostico;
            }
            LogRegistrar((int)LogEventoEnum.ModificacionDatosTurno, "Se modificaron los datos del turno.", turno.Id);
            return turno;
        }

        [RequiresTransaction]
        public virtual TurnoUpdateObservaciones TurnoUpdate(TurnoUpdateObservaciones turno)
        {
            if (turno.Observaciones != null && turno.Observaciones.Length > Turno.MAX_LONG_OBSERVACIONES)
                throw new NotLoggeableException("La longitud de las observaciones ingresadas supera el máximo permitido.");

            // Chequeo si realmente hay diferencias
            if (RegistrarLogCambioObservaciones(turno))
            {

                // Actualizo
                turno = dalEngine.Update<TurnoUpdateObservaciones>(turno);
                LogRegistrar((int)LogEventoEnum.ModificacionDatosTurno, "Se modificaron los datos del turno.", turno.Id);
            }
            return turno;
        }

        [RequiresTransaction]
        public virtual OrdenUpdateDiagnostico OrdenUpdate(OrdenUpdateDiagnostico orden)
        {
            // Chequeo si realmente hay diferencias
            if (RegistrarLogCambioDiagnostico(orden))
            {
                // Actualizo
                orden = dalEngine.Update<OrdenUpdateDiagnostico>(orden);
            }

            return orden;
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [RequiresTransaction]
        public virtual TurnoUpdateRequiereCobranza TurnoUpdate(TurnoUpdateRequiereCobranza turno)
        {

            // Actualizo
            turno = dalEngine.Update<TurnoUpdateRequiereCobranza>(turno);

            return turno;
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [RequiresTransaction]
        public virtual TurnoUpdateTipoAutorizacion TurnoUpdate(TurnoUpdateTipoAutorizacion turno, bool bDesdeProcesoAutomatico)
        { //, string userNameProcesoAutomatico

            // Log
            string log = "Se modificó el tipo de autorización al valor ";
            bool setear = true;
            switch (turno.TipoAutorizacionID)
            {
                case (int)TipoAutorizacionEnum.AAutorizar:
                    log += "'A Autorizar'";
                    setear = false;
                    break;
                case (int)TipoAutorizacionEnum.AAutorizarRemoto:
                    log += "'A Autorizar Remoto'";
                    break;
                case (int)TipoAutorizacionEnum.Autorizado:
                    log += "'Autorizado'";
                    break;
                case (int)TipoAutorizacionEnum.EsperaAutorizacionRemota:
                    log += "'Espera Autorización Remota'";
                    break;
                case (int)TipoAutorizacionEnum.NoAutorizado:
                    log += "'No Autorizado'";
                    break;
            }
            LogRegistrar((int)LogEventoEnum.AutorizacionTurnos, log, turno.Id);

            // Logueo en el turno
            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.Autorizacion, setear);


            // Actualizo
            turno = dalEngine.Update<TurnoUpdateTipoAutorizacion>(turno);

            SecurityUser user = Security.Current.UserInfo.User;
            // Si se modifico manualmente a Autorizado o NoAutorizado
            if (bDesdeProcesoAutomatico)
            {
                EntityCollection<AutorizacionPlanilla> planillasActualizar = new EntityCollection<AutorizacionPlanilla>();

                // Marco las planillas para que no sean enviadas
                EntityCollection<AutorizacionPlanilla> planillas = AutorizacionPlanillaReadByTurno(turno.Id, false);
                foreach (AutorizacionPlanilla planilla in planillas)
                {
                    // Solo marco a No Enviar las que estaban marcadas a Enviar
                    if (planilla.Enviar)
                    {
                        planilla.Enviar = false;

                        planillasActualizar.Add(planilla);
                    }
                }

                // Actualizo las Planillas
                if (planillasActualizar.Count > 0)
                {
                    planillasActualizar = dalEngine.UpdateCollection<AutorizacionPlanilla>(planillasActualizar);
                }
            }

            return turno;
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [RequiresTransaction]
        public virtual TurnoUpdateTipoConfirmacion TurnoUpdate(TurnoUpdateTipoConfirmacion turno)
        {
            // Log
            string log = "Se modificó el tipo de confirmación al valor ";
            bool setear = true;
            switch (turno.TipoConfirmacionID)
            {
                case (int)TipoConfirmacionEnum.AConfirmar:
                    log += "'A Confirmar'";
                    setear = false;
                    break;
                case (int)TipoConfirmacionEnum.Confirmado:
                    log += "'Confirmado'";
                    break;
                case (int)TipoConfirmacionEnum.NoConfirmado:
                    log += "'No Confirmado'";
                    break;
            }
            LogRegistrar((int)LogEventoEnum.ConfirmacionTurnos, log, turno.Id);

            // Logueo en el turno
            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.Confirmacion, setear);


            // Actualizo
            turno = dalEngine.Update<TurnoUpdateTipoConfirmacion>(turno);

            GuardarSeguimientoConfirmacion(turno.Id, turno.TipoConfirmacionID);

            return turno;
        }

        private void GuardarSeguimientoConfirmacion(int turnoId, int tipoConfirmacionId)
        {
            int? accionActualId = null;
            int? accionFuturaId = null;
            DateTime? fechaFutura = null;

            switch (tipoConfirmacionId)
            {
                case (int)TipoConfirmacionEnum.AConfirmar:
                    accionActualId = (int)SeguimientoAccionEnum.AConfirmar;
                    accionFuturaId = (int)SeguimientoAccionEnum.LlamarAlPaciente;
                    fechaFutura = enfoke.Time.Now;
                    break;
                case (int)TipoConfirmacionEnum.Confirmado:
                    accionActualId = (int)SeguimientoAccionEnum.Confirmar;
                    break;
                case (int)TipoConfirmacionEnum.NoConfirmado:
                    accionActualId = (int)SeguimientoAccionEnum.NoConfirmar;
                    break;
            }


            List<int> turnoIds = new List<int>();
            turnoIds.Add(turnoId);

            SeguimientoTurnoUpdateGlobal(turnoIds, accionActualId, accionFuturaId, fechaFutura,
                                    string.Empty, TipoSeguimientoEnum.Confirmaciones);
        }

        [RequiresTransaction]
        public virtual void TurnoUpdateTCF(int turnoID, TipoControlFacturacionEnum tipo)
        {
            TurnosUpdateTCF(new List<int>(new int[] { turnoID }), tipo, true);
        }

        [RequiresTransaction]
        public virtual void TurnosUpdateTCF(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion)
        {
            TurnosUpdateTCF(turnosId, tipoControlFacturacion, false, null, true);
        }

        [RequiresTransaction]
        public virtual void TurnosUpdateTCF(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, bool grabarTurnoLog)
        {
            TurnosUpdateTCF(turnosId, tipoControlFacturacion, false, null, grabarTurnoLog);
        }

        [Private]
        [RequiresTransaction]
        public virtual void TurnosUpdateTCFParaPeriodos(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, bool grabarTurnoLog, IList<DateTime?> periodDates)
        {
            // 1. Si el tipo es Pre Facturado, los protocolos que estan Debitados los pongo en A Re Facturar, por eso llamo a esta misma funcion.
            if (tipoControlFacturacion == TipoControlFacturacionEnum.PreFacturado)
            {
                List<int> turnosDebitados = ObtenerTurnoDebitados(turnosId);
                if (turnosDebitados.Count > 0)
                {
                    // Si esta debitado, no lo paso a A Facturar
                    turnosId.RemoveAll(turnoID => turnosDebitados.Contains(turnoID));
                    // Pero si lo paso a A Refacturar
                    TurnosUpdateTCFParaPeriodos(turnosDebitados, TipoControlFacturacionEnum.AReFacturar, grabarTurnoLog, periodDates);
                }
            }

            if (turnosId.Count <= 0)
                return;

            // 2. Actualizacion la sumarizacion (antes que nada para no perder el valor anterior)
            this.UpdateShiftSummarization(turnosId, tipoControlFacturacion, true, periodDates);
            if (tipoControlFacturacion != TipoControlFacturacionEnum.Particular)
                dalEngine.UpdatePropertyBatchByIds<TurnoHQL>(turnosId, TurnoHQL.Properties.TipoControlFacturacionAnteriorAParticularId, (int)tipoControlFacturacion);
            // 3. ejecuto la actualizacion normal.
            this.ActualizarTurnos(turnosId, tipoControlFacturacion, true);
        }

        private List<int> ObtenerTurnoDebitados(List<int> turnosId)
        {
            var query = from turDebitado in dalEngine.Query<Turno>() where turnosId.Contains(turDebitado.Id) && turDebitado.TipoControlFacturacion.Id == (int)TipoControlFacturacionEnum.Debitado select turDebitado.Id;
            return query.ToList();
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turnosIds">Datos de los Turnos a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [Private]
        [RequiresTransaction]
        public virtual void TurnosUpdateTCFDesdePeriodos(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, IList<DateTime?> periodDates)
        {
            // 1. Actualizacion la sumarizacion (antes que nada para no perder el valor anterior)
            this.UpdateShiftSummarization(turnosId, tipoControlFacturacion, false, periodDates);
            if (tipoControlFacturacion != TipoControlFacturacionEnum.Particular)
                dalEngine.UpdatePropertyBatchByIds<TurnoHQL>(turnosId, TurnoHQL.Properties.TipoControlFacturacionAnteriorAParticularId, (int)tipoControlFacturacion);
            // 2. ejecuto la actualizacion normal.
            this.ActualizarTurnos(turnosId, tipoControlFacturacion, true);
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turnosIds">Datos de los Turnos a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [Private]
        [RequiresTransaction]
        public virtual void TurnosUpdateTCF(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, bool incluirEnPeriodo, DateTime? periodo, bool grabarTurnoLog)
        {
            // 1. Actualizacion la sumarizacion (antes que nada para no perder el valor anterior)
            this.UpdateShiftSummarization(turnosId, tipoControlFacturacion, incluirEnPeriodo, new List<DateTime?> { periodo });
            if (tipoControlFacturacion != TipoControlFacturacionEnum.Particular)
                dalEngine.UpdatePropertyBatchByIds<TurnoHQL>(turnosId, TurnoHQL.Properties.TipoControlFacturacionAnteriorAParticularId, (int)tipoControlFacturacion);

            // 2. ejecuto la actualizacion normal.
            this.ActualizarTurnos(turnosId, tipoControlFacturacion, grabarTurnoLog);
        }

        private void ActualizarTurnos(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, bool grabarTurnoLog)
        {
            // 1. Hace el cambio de estado
            dalEngine.UpdatePropertyBatchByIds<TurnoHQL>(turnosId, TurnoHQL.Properties.TipoControlFacturacionId, (int)tipoControlFacturacion);

            // 2. Se fija si guarda en el log
            LogEvento le = GetLogEvento((int)LogEventoEnum.ControlFacturacion);
            if (le.Habilitado)
            {
                string log = "Se modificó el control de facturación al valor '" + EnumDescription.GetDescription(tipoControlFacturacion) + "'";
                foreach (int turnoid in turnosId)
                    // Log
                    LogRegistrar(le, log, turnoid);
            }

            // 3. Guarda en la turnolog
            bool setear = (tipoControlFacturacion != TipoControlFacturacionEnum.NoControlado);
            if (tipoControlFacturacion != TipoControlFacturacionEnum.Debitado && tipoControlFacturacion != TipoControlFacturacionEnum.BajaDefinitiva && grabarTurnoLog)
            {
                DateTime? fecha = new DateTime?();
                int? userId = new int?();
                FechaYUsuarioParaTurnoLog(tipoControlFacturacion, ref userId, ref fecha);
                TurnoLogItemUpdateBatch(turnosId, TurnoLogFechasEnum.ControlFacturacion, setear, fecha, userId);
            }
        }

        public Turno TurnoReadByOrden(Orden order)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            Filter filter = new Filter();
            filter.Add(Turno.Properties.Orden.Id, "=", order.Id);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);
            readCmd.Filter = filter;

            Turno turno = readCmd.Execute()[0];
            turno.TipoControlFacturacionAnterior = turno.TipoControlFacturacion;
            turno.EstadoAnterior = (EstadoTurnoEnum)turno.EstadoTurnoID;
            turno.Orden = order;

            return turno;

        }

        private void UpdateShiftSummarization(List<int> turnosId, TipoControlFacturacionEnum tipoControlFacturacion, bool paraPeriodo, IList<DateTime?> periodo)
        {
            FacturacionDalc invoiceDalc = Context.Session.FacturacionDalc;
            TipoControlFacturacion currentTypeInvoiceControl = invoiceDalc.TipoControlFacturacionReadById((int)tipoControlFacturacion);
            EntityCollection<Turno> turnos = this.TurnosReadByIds(turnosId);
            foreach (Turno shift in turnos)
            {
                shift.EstadoAnterior = (EstadoTurnoEnum)shift.EstadoTurnoID;
                shift.TipoControlFacturacionAnterior = shift.TipoControlFacturacion;
                shift.TipoControlFacturacion = currentTypeInvoiceControl;
            }

            SummarizeTurnosFacturacion summarize = SummarizeTurnosFacturacion.GetInstance();
            if (periodo.Count > 0 && periodo[0].HasValue)
            {
                if (paraPeriodo)
                    summarize.ChangeShiftsStatesToPeriod(turnos, periodo);
                else
                    summarize.ChangeShiftsStatesFromPeriod(turnos, periodo);
            }
            else
                summarize.ChangeShiftsStates(turnos);
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turno">Datos del Turno a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        /// <returns>El Objeto de Datos del Turno actualizado</returns>
        [Private]
        [RequiresTransaction]
        public virtual TurnoUpdateRecitado TurnoUpdate(TurnoUpdateRecitado turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Obtengo el Status
            EstadoTurno aRecitar = EstadoTurnoReadById((int)EstadoTurnoEnum.ARecitar);

            // Actualizo el estado del turno - Historico
            Turno turnoDB = TurnoReadById(turno.Id);
            TurnoAvanzarEstado(turnoDB, aRecitar, modalidadCoseguro);

            // Obtengo el estado seteado al turno
            // Si el turpo turno es sin reserva, el estado será RecitadoPendiente, caso contrario... ARecitar.
            if (TipoTurno.TipoTurnoSinReserva(turnoDB.TipoTurnoId))
            {
                aRecitar = EstadoTurnoReadById(turno.EstadoTurnoID);
                TurnoAvanzarEstado(turnoDB, aRecitar, modalidadCoseguro);
            }

            if (!String.IsNullOrEmpty(turnoDB.Observaciones))
                turnoDB.Observaciones = turnoDB.Observaciones.Trim();
            if (!String.IsNullOrEmpty(turno.Observaciones))
                turno.Observaciones = turno.Observaciones.Trim();

            bool observacionesModificadas = false;
            if (String.IsNullOrEmpty(turnoDB.Observaciones) == false ||
                String.IsNullOrEmpty(turno.Observaciones) == false)
                observacionesModificadas = turnoDB.Observaciones != turno.Observaciones;


            // Actualizo
            turno = dalEngine.Update<TurnoUpdateRecitado>(turno);

            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.Recitado, true);

            // Registro Log
            LogRegistrar((int)LogEventoEnum.TurnoRecitado, "Se recitó el turno. Tipo Recitado: " + dalEngine.GetById<TipoRecitado>(turno.TipoRecitadoID.Value).Name, turno.Id);
            if (observacionesModificadas)
                LogRegistrar((int)LogEventoEnum.ModificacionDiagnosticoObservaciones, "Se modificó el diagnostico y las observaciones.", turno.Id);

            return turno;
        }

        /// <summary>
        /// Actualiza los "Datos del Turno"
        /// </summary>
        /// <param name="turnosIds">Turnos a Actualizar</param>
        /// <param name="user">Usuario de la Actualizacion</param>
        [RequiresTransaction]
        public virtual void TurnoUpdateMany(EntityCollection<TurnoUpdateHuerfano> turnos)
        {

            // Actualizo
            dalEngine.UpdateCollection<TurnoUpdateHuerfano>(turnos);

            List<int> turnosIds = turnos.GetIds();
            SeguimientoTurnoUpdateGlobal(turnosIds, null, (int)SeguimientoAccionEnum.LlamarAlPaciente, enfoke.Time.Now, string.Empty, TipoSeguimientoEnum.Huerfanos);
        }

        [RequiresTransaction]
        public virtual void TurnoUpdateMany(EntityCollection<Turno> turnos)
        {

            // Actualizo
            dalEngine.UpdateCollection<Turno>(turnos);
        }

        [Private]
        public bool TurnoExisteSuperpuestoByEquipoForReserva(DateTime desde, DateTime hasta, int equipoId, bool ocupadosIncluyenTurnosHuerfanos)
        {
            string hql = "from Turno t where "
                        + " t.Activo = true AND t.EsSobreturno = false "
                        + " AND t.EquipoId = :equipoId AND t.Deleted = false "
                        + " AND t.EstadoTurnoID != :cancelado"
                        + " AND t.Fecha < :hasta "
                        + (ocupadosIncluyenTurnosHuerfanos ? "" : " AND t.EsHuerfano = 0 ")
                        + " order by t.Fecha DESC";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipoId);
            query.SetParameter("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetParameter("hasta", hasta);
            query.SetMaxResults(1);
            // Hace la consulta
            EntityCollection<Turno> anterior = dalEngine.GetManyByQuery<Turno>(query);
            if (anterior.Count == 0)
                return false;
            else
                return anterior[0].FechaFin > desde;
        }
        [Private]
        public EntityCollection<Turno> TurnoReadByMedicoForHuerfanos(DateTime desde, DateTime hasta, int medicoId)
        {
            return TurnoReadByEquipoAndMedicoForHuerfanos(desde, hasta, medicoId, null);
        }

        [Private]
        public List<int> TurnoIdsReadByMedicoForHuerfanos(DateTime desde, DateTime hasta, int medicoId)
        {
            return TurnoIdsReadByEquipoAndMedicoForHuerfanos(desde, hasta, medicoId, null);
        }

        [Private]
        public List<int> TurnoIdsReadByEquipoAndMedicoForHuerfanos(DateTime desde, DateTime hasta, int medicoId, int? equipoId)
        {
            return TurnoIdsReadByEquipoAndMedicoForHuerfanos(desde, hasta, medicoId, equipoId, false);
        }

        [Private]
        public List<int> TurnoIdsReadByEquipoAndMedicoForHuerfanos(DateTime desde, DateTime hasta, int medicoId, int? equipoId, bool huerfano)
        {
            // Busco 2 horas antes de lo que debo y despues filtro para quedarme con lo que corresponde
            // Si es lo hago dentro del querty no entra por indice y tarda mucho
            desde = desde.AddHours(-2);

            string hql = "SELECT distinct new enfoke.Eges.Entities.Results.TurnoForHuerfanos(t.Id, t.Fecha, t.DuracionSeconds) " +
                         "FROM Turno t, PracticaTurno pt " +
                         "WHERE t.Id = pt.TurnoId " +
                         "AND t.Fecha BETWEEN :desde AND :hasta " +
                //"AND FECHAHASTA(t.Fecha, t.DuracionSeconds) >= :desde AND t.Fecha <= :hasta " +
                         "AND (pt.Medico.Id = :medicoId) " + // OR pt.MedicoInformante.Id = :userId) " +
                         "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = :huerfano";
            if (equipoId.HasValue)
                hql += " AND t.EquipoId = :equipoId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1).AddMinutes(-1));
            query.SetParameter("medicoId", medicoId);
            query.SetBoolean("huerfano", huerfano);
            if (equipoId.HasValue)
                query.SetParameter("equipoId", equipoId.Value);

            // Ahora vuelvo la hora desde y me fijo segun la duracion del turno, cuales no corresponden devolver
            List<int> ret = new List<int>();
            EntityCollection<TurnoForHuerfanos> huerfanosPosibles = dalEngine.GetManyByQuery<TurnoForHuerfanos>(query);
            desde = desde.AddHours(2);
            foreach (TurnoForHuerfanos turHuerfano in huerfanosPosibles)
            {
                if (turHuerfano.FechaTurno.AddSeconds(turHuerfano.DuracionSeconds) >= desde)
                    ret.Add(turHuerfano.Id);
            }

            return ret;
        }

        public EntityCollection<Turno> TurnoReadByEquipoAndMedicoForHuerfanos(DateTime desde, DateTime hasta, int medicoId, int? equipoId)
        {
            string hql = "SELECT distinct t " +
                         "FROM Turno t, PracticaTurno pt " +
                         "WHERE t.Id = pt.TurnoId AND t.Fecha >= :desde ";
            if (hasta != DateTime.MinValue)
                hql += "AND t.Fecha < :hasta ";
            hql += "AND pt.Medico.Id = :medicoId " +
                         "AND t.Activo = true AND t.Deleted = false " +
                         "AND t.EsHuerfano = false " +
                         "AND t.EstadoTurnoID = :estadoTurno ";
            if (equipoId.HasValue)
                hql += " AND t.EquipoId = :equipoId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            if (hasta != DateTime.MinValue)
                query.SetParameter("hasta", hasta.Date.AddDays(1).AddMinutes(-1));
            query.SetParameter("medicoId", medicoId);
            query.SetParameter("estadoTurno", (int)EstadoTurnoEnum.Reservado);

            if (equipoId.HasValue)
                query.SetParameter("equipoId", equipoId.Value);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnoReadByMedicoAndPracticaForHuerfanos(int medicoId, int practicaId)
        {
            List<int> practicasIds = new List<int>();
            practicasIds.Add(practicaId);

            return TurnoReadByMedicoAndPracticaForHuerfanos(medicoId, practicasIds);
        }

        public EntityCollection<Turno> TurnoReadByMedicoAndPracticaForHuerfanos(int medicoId, List<int> practicasIds)
        {
            SQLBlockBuilder<int> inClause = new SQLBlockBuilder<int>(practicasIds);

            StringBuilder hql = new StringBuilder();
            hql.Append("select distinct t from Turno t, PracticaTurno pt ");
            hql.Append("where t.Id = pt.TurnoId AND t.Fecha >= :now ");
            hql.Append("AND pt.Medico.Id = :medicoId AND ");
            hql.Append(inClause.BuildConstrainBlock("pt.Practica.Id"));
            hql.Append("AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("now", enfoke.IO.Time.Now);
            query.SetParameter("medicoId", medicoId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        /// <summary>
        /// Devuelve los ids de los turnosIds asignados a un medicoId y a una prática en un periodo.
        /// </summary>
        /// <param name="desde">Inicio del Periodo</param>
        /// <param name="hasta">Fin del Periodo</param>
        /// <param name="medicoId">Medico del cual se desea obtener los turnosIds existentes [como actuante o informante]</param>
        /// <param name="practicaId">La práctica</param>
        /// <returns>Ids Turnos</returns>
        public List<int> TurnoIdsReadByMedicoAndPracticaForHuerfanos(DateTime desde, DateTime hasta, int medicoId, int practicaId)
        {
            // Busco 2 horas antes de lo que debo y despues filtro para quedarme con lo que corresponde
            // Si es lo hago dentro del querty no entra por indice y tarda mucho
            desde = desde.AddHours(-2);

            string hql = "SELECT distinct new enfoke.Eges.Entities.Results.TurnoForHuerfanos(t.Id, t.Fecha, t.DuracionSeconds) " +
                         "FROM Turno t, PracticaTurno pt " +
                         "where t.Id = pt.TurnoId AND t.Fecha >= :desde  AND t.Fecha < :hasta " +
                         "AND (pt.Medico.Id = :medicoId) " + //OR pt.MedicoInformante.Id = :userId) 
                         "AND pt.Practica.Id = :practicaId " +
                         "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1));
            query.SetParameter("medicoId", medicoId);
            query.SetParameter("practicaId", practicaId);

            // Ahora vuelvo la hora desde y me fijo segun la duracion del turno, cuales no corresponden devolver
            List<int> ret = new List<int>();
            EntityCollection<TurnoForHuerfanos> huerfanosPosibles = dalEngine.GetManyByQuery<TurnoForHuerfanos>(query);
            desde = desde.AddHours(2);
            foreach (TurnoForHuerfanos turHuerfano in huerfanosPosibles)
            {
                if (turHuerfano.FechaTurno.AddSeconds(turHuerfano.DuracionSeconds) >= desde)
                    ret.Add(turHuerfano.Id);
            }

            return ret;
        }

        /// <summary>
        /// Devuelve los turnosIds asignados a un medicoId y a una prática en un periodo.
        /// </summary>
        /// <param name="desde">Inicio del Periodo</param>
        /// <param name="hasta">Fin del Periodo</param>
        /// <param name="medicoId">Medico del cual se desea obtener los turnosIds existentes [como actuante o informante]</param>
        /// <param name="practicaId">La práctica</param>
        /// <returns>Turnos</returns>
        public EntityCollection<Turno> TurnoReadByMedicoAndPracticaForHuerfanos(DateTime desde, DateTime hasta, int medicoId, int practicaId)
        {
            return TurnosReadByIds(TurnoIdsReadByMedicoAndPracticaForHuerfanos(desde, hasta, medicoId, practicaId));
        }

        public EntityCollection<Turno> TurnoReadByPlanAndPracticaForHuerfanos(int planId, int practicaId, DateTime fechaDesde, DateTime? fechaHasta)
        {
            string hql = "select distinct t from Turno t, PracticaTurno pt "
                + "where t.Id = pt.TurnoId "
                + "AND pt.Practica.Id = :practicaId "
                + "AND t.Orden.ObraSocialPlanId = :ospId "
                + "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false "
                + "AND t.Fecha >= :now "
                + "AND t.Fecha > :desde ";
            if (fechaHasta.HasValue)
                hql += "AND t.Fecha < :hasta ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ospId", planId);
            query.SetParameter("practicaId", practicaId);
            query.SetParameter("now", enfoke.Time.Now);
            query.SetParameter("desde", fechaDesde);
            if (fechaHasta.HasValue)
                query.SetParameter("hasta", fechaHasta.Value);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnoReadByPlanForHuerfanos(DateTime fecha, int planId)
        {
            string hql = "from Turno t "
                        + "where t.Fecha >= :fecha "
                        + "AND t.Orden.ObraSocialPlanId = :ospId "
                        + "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", fecha);
            query.SetParameter("ospId", planId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnoReadByEquipoAndPracticaForHuerfanos(int equipoId, int practicaId)
        {
            string hql = "select distinct t from Turno t, PracticaTurno pt"
                + " where t.Id = pt.TurnoId AND t.Fecha >= :now"
                + " AND pt.Practica.Id = :practicaId"
                + " AND t.EquipoId = :equipoId"
                + " AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("now", enfoke.Time.Now);
            query.SetParameter("equipoId", equipoId);
            query.SetParameter("practicaId", practicaId);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        [Private]
        public void TurnoUpdateBatchHuerfano(List<int> idTurnos, bool esHuerfano)
        {
            dalEngine.UpdatePropertyBatchByIds<Turno>(idTurnos, Turno.Properties.EsHuerfano, esHuerfano);

            if (esHuerfano)
            {
                foreach (int idTurno in idTurnos)
                {
                    List<int> turnoIds = new List<int>();
                    turnoIds.Add(idTurno);
                    SeguimientoTurnoUpdateGlobal(turnoIds, null, (int)SeguimientoAccionEnum.LlamarAlPaciente, enfoke.Time.Now,
                                                            string.Empty, TipoSeguimientoEnum.Huerfanos);
                }
            }
        }

        [Private]
        public void TurnoUpdateBatchEquipo(List<int> idTurnos, int idEquipo)
        {
            dalEngine.UpdatePropertyBatchByIds<Turno>(idTurnos, Turno.Properties.EquipoId, idEquipo);
        }

        [Private]
        [RequiresTransaction]
        public virtual void TurnoAsociadoEntrevistaDelete(int id, int motivo, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            Turno turno = TurnoReadById(id);
            turno.MotivoID = motivo;

            if (turno.EstadoTurnoID == (int)EstadoTurnoEnum.Recepcionado)
                throw new Exception("No se puede cancelar el turno asociado ya que este fue recepcionado");

            // Obtengo el Status Cancelado
            EstadoTurno cancelado = EstadoTurnoReadById((int)EstadoTurnoEnum.Cancelado);

            // [JR] En vez de Borrar, Cancelo el turno
            TurnoAvanzarEstado(turno, cancelado, modalidadCoseguro);
        }

        [RequiresTransaction]
        [Private]
        public virtual void TurnoDelete(int id, int motivo, string observacionesCancelacion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            Turno turno = TurnoReadById(id);
            turno.MotivoID = motivo;
            turno.ObservacionesCancelacion = observacionesCancelacion;

            // Obtengo el Status Cancelado
            EstadoTurno cancelado = EstadoTurnoReadById((int)EstadoTurnoEnum.Cancelado);

            // [JR] En vez de Borrar, Cancelo el turno
            TurnoAvanzarEstado(turno, cancelado, modalidadCoseguro);

        }

        [RequiresTransaction]
        public virtual void TurnosDelete(int[] id, int motivo, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            for (int i = 0; i < id.Length; i++)
                TurnoDelete(id[i], motivo, null, modalidadCoseguro);
        }

        public EntityCollection<Turno> TurnoReadByCombo(int comboId)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.ComboId, "=", comboId);

            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);

            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Turno.Properties.Orden.PacienteId, SortingDirection.Asc);
            sort.Add(Turno.Properties.Fecha, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Devuelvo los turnosIds del Combo del mismo Paciente
        /// </summary>
        /// <param name="comboID">Id del Combo de los Turnos</param>
        /// <param name="turnoID">Id del Turno a Ignorar</param>
        /// <param name="pacienteID">Id del Paciente de los Turnos del Combo</param>
        /// <returns>Todos los Turnos del Combo del mismo Paciente (Sin incluir el actual)</returns>
        public EntityCollection<Turno> TurnosComboReadByTurno(int turnoID, int comboID, int pacienteID)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.ComboId, "=", comboID);
            filter.Add(BooleanOp.And, Turno.Properties.Id, "!=", turnoID);
            filter.Add(BooleanOp.And, Turno.Properties.Orden.PacienteId, "=", pacienteID);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Turno.Properties.Fecha, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        [AnonymousMethod()]
        public Turno TurnoReadById(int id)
        {
            Turno turno = dalEngine.GetById<Turno>(id);
            turno.TipoControlFacturacionAnterior = turno.TipoControlFacturacion;
            turno.EstadoAnterior = (EstadoTurnoEnum)turno.EstadoTurnoID;
            return turno;
        }






        public OrdenUpdateDiagnostico OrdenUpdateDiagnosticoReadById(int id)
        {
            return dalEngine.GetById<OrdenUpdateDiagnostico>(id);
        }

        /// <summary>
        /// Retorna un turno por su original id
        /// </summary>
        /// <param name="originalId">id del turno original</param>
        /// <returns>El turno correspondiente</returns>
        public Turno TurnoReadByOriginalId(int originalId, bool sinCancelados)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.TurnoOriginalID, "=", originalId);

            if (sinCancelados)
                filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Turno.Properties.Id, SortingDirection.Desc);

            readCmd.Sort = sort;

            EntityCollection<Turno> turnos = readCmd.Execute();

            if (turnos.Count > 0)
                return turnos[0];
            else
                return null;
        }

        [Private]
        public EntityCollection<Turno> TurnoReadRecitacionesNoCanceladas(int originalId)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(Turno.Properties.TurnoOriginalID, "=", originalId);

            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);

            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.Recitado);

            return readCmd.Execute();
        }

        /// <summary>
        /// Devuelve los turnosIds de la obra social indicada en el rango de tiempo indicado
        /// </summary>
        /// <param name="date">Día desde donde buscar el turno</param>
        /// <returns>Turnos del dia solicitado</returns>
        public EntityCollection<Turno> TurnoReadByObraSocialAndDate(ObraSocial os, DateTime from, DateTime? to)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            EntityCollection<ObraSocialPlan> ospc = ObrasSocialesDalc.ObraSocialPlanReadByObraSocial(os.Id, true);
            EntityCollection<Turno> ret = new EntityCollection<Turno>();
            foreach (ObraSocialPlan osp in ospc)
            {
                EntityCollection<Turno> tc = TurnoReadByObraSocialPlanAndDate(osp, from, to);
                ret.AddRange(tc);
            }

            return ret;
        }

        public EntityCollection<Turno> TurnoReadByObraSocialPlanAndDate(ObraSocialPlan osp, DateTime from, DateTime? to)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.Orden.ObraSocialPlanId, "=", osp.Id);

            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);

            filter.Add(BooleanOp.And, Turno.Properties.Fecha, ">=", from);

            if (to.HasValue)
                filter.Add(BooleanOp.And, Turno.Properties.Fecha, "<=", to.Value);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<Turno> TurnoReadForAusentes(DateTime to)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(Turno.Properties.Fecha, "<=", to);

            // Creo la lista de estados a buscar
            List<int> estados = new List<int>();
            estados.Add((int)EstadoTurnoEnum.Reservado);
            estados.Add((int)EstadoTurnoEnum.ReservaProvisoria);

            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "IN", estados.ToArray());

            // Ignoro los de Tipo Recitado, Espontáneo, Extraordinario y Guardia
            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.TipoTurnoId, "NOT IN",
                               new int[]
                                   {
                                       (int) TipoTurnoEnum.Recitado, (int) TipoTurnoEnum.Espontaneo,
                                       (int) TipoTurnoEnum.Extraordinario, (int) TipoTurnoEnum.Guardia
                                   });

            EntityCollection<Turno> turnos = readCmd.Execute();

            //Agrego los Recitados vencidos.
            turnos.AddRange(TurnoReadForRecitadosVencidos(to));

            return turnos;
        }

        private EntityCollection<Turno> TurnoReadForRecitadosVencidos(DateTime to)
        {
            EntityCollection<Turno> turnosRecitadoVencido = new EntityCollection<Turno>();

            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            readCmd.Filter = new Filter();
            readCmd.Filter.Add(Turno.Properties.EstadoTurnoID, "=", (int)EstadoTurnoEnum.RecitadoPendiente);
            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", false);
            readCmd.Filter.Add(BooleanOp.And, Turno.Properties.Fecha, "<=", to);

            foreach (Turno orig in readCmd.Execute())
            {
                string hql = "SELECT DISTINCT t " +
                             "FROM Turno t, Turno orig " +
                             "WHERE t.TurnoOriginalID = orig.Id " +
                             "AND t.EstadoTurnoID IN (:estadosReserva) " +
                             "AND t.Deleted = false " +
                             "AND t.TipoTurnoId = :tipoRecitado " +
                             "AND t.Fecha > :fechaDesde " +
                             "AND orig.Id = :origid ";

                IQuery query = dalEngine.CreateQuery(hql);
                query.SetParameterList("estadosReserva", new int[]

                                                         {
                                                             (int) EstadoTurnoEnum.Reservado,
                                                             (int) EstadoTurnoEnum.ReservaProvisoria
                                                         });
                query.SetParameter("tipoRecitado", (int)TipoTurnoEnum.Recitado);
                query.SetParameter("fechaDesde", to);
                query.SetParameter("origid", orig.Id);

                if (dalEngine.GetManyByQuery<Turno>(query).Count == 0)
                {
                    turnosRecitadoVencido.Add(orig);
                }
            }

            return turnosRecitadoVencido;
        }


        public Turno TurnoReadByProtocolo(string strProtocolo)
        {
            Protocolo protocolo = new Protocolo();
            protocolo = ProtocoloReadByProtocolo(strProtocolo);

            // Turno
            // Obtengo el Turno según el número de Protocolo
            Turno turno = null;
            try
            {
                turno = TurnoReadByProtocolo(protocolo.Id);

                if (turno == null)
                    throw new Exception();

                turno.Orden.Paciente = PacienteReadById(turno.Orden.PacienteId);
            }
            catch
            {
                throw new NotLoggeableException("Error al Obtener el Turno del Protocolo " + protocolo.ProtocoloFull);
            }


            return turno;
        }

        public Orden OrdenReadByProtocolo(string strProtocolo)
        {
            Turno turno = TurnoReadByProtocolo(strProtocolo);
            return OrdenReadByTurnoId(turno.Id);
        }

        public Protocolo ProtocoloReadByProtocolo(string strProtocolo)
        {
            ServiciosDalc ServiciosDalc = Context.Session.ServiciosDalc;

            // Acondiciono lo Recibido

            strProtocolo = strProtocolo.Trim();

            // Saco los Guiones
            while (strProtocolo.IndexOf('-') >= 0)
                strProtocolo = strProtocolo.Remove(strProtocolo.IndexOf('-'), 1);

            if (strProtocolo.Length > 16)
                throw new NotLoggeableException("El Protocolo puede tener como Máximo 16 dígitos.");

            // Si vino un Protocolo con Región, elimino este dato
            if (strProtocolo.Length == 16 || strProtocolo.Length == 13)
                strProtocolo = strProtocolo.Substring(0, strProtocolo.Length - 2);

            // Padeo lo Ingresado a 14 Caracteres
            strProtocolo = strProtocolo.PadLeft(14, '0');



            // Separo TAGs
            // Separo en Sucursal, Origen y Número de Protocolo
            string tagSucursal = strProtocolo.Substring(0, 3);
            string origen = strProtocolo.Substring(3, 3);
            string numero = strProtocolo.Substring(6);

            if (numero.CompareTo("00000000") == 0)
                throw new NotLoggeableException("El Número del Protocolo No puede ser 0.");

            // Me quedo solamente con los numeros (saco ceros)
            tagSucursal = tagSucursal.TrimStart('0');

            if (origen.CompareTo("000") != 0)
                origen = origen.TrimStart('0');

            numero = numero.TrimStart('0');



            // Sucursal
            // Obtengo la Sucursal según el Tag ingresado
            SucursalName sucursal = null;
            try
            {
                if (!String.IsNullOrEmpty(tagSucursal))
                    sucursal = SucursalNameReadByTag(tagSucursal);

                // Si no encontre, seteo como sucursal la "unica"
                if (sucursal == null)
                {
                    EntityCollection<SucursalName> sucursales = SucursalNameReadAll().Collection;

                    // [GG] Si hay una sola sucursal entonces puedo determinar que es la única.
                    if (sucursales.Count == 1)
                        sucursal = sucursales[0];
                }

                if (sucursal == null)
                    throw new Exception();
            }
            catch
            {
                throw new NotLoggeableException("Error al Obtener la Sucursal [" + tagSucursal + "].");
            }


            // Protocolo
            // Obtengo el Protocolo según el Origen y el Número
            Protocolo protocolo = null;
            try
            {
                protocolo = ProtocoloReadBySucursalOrigenAndNumero(sucursal, origen, int.Parse(numero));

                if (protocolo == null)
                    throw new Exception();
            }
            catch (Exception ex)
            {
                throw new NotLoggeableException("Error al Obtener el Protocolo." + Environment.NewLine + "Es probable que el protocolo no exista.", ex);
            }


            return protocolo;
        }

        public EntityCollection<Turno> TurnosReadByIds(List<int> idsTurnos)
        {
            EntityCollection<Turno> turnos = new EntityCollection<Turno>();
            List<List<int>> turIdsMenor1000 = LinqInClause.SplitIntoBucketsForOracle(idsTurnos);
            foreach (List<int> turIds in turIdsMenor1000)
            {
                EntityCollection<Turno> turnosTmp = dalEngine.GetManyByIds<Turno>(turIds);
                turnos.AddRange(turnosTmp);
            }

            return turnos;
        }

        // TurnoLight

        public EntityCollection<TurnoLight> TurnoLightRead(DateTime fecha, List<TurnoLightSearch> args, int statusToExclude, bool cargaObjectos, bool ocupadosIncludeTurnosHuerfanos)
        {
            string hql = "SELECT DISTINCT t FROM TurnoLight t, PracticaTurno pt " +
             "WHERE pt.TurnoId = t.Id " +
             "AND t.Fecha >= :fecha AND t.Fecha < :fechaMasUno " +
             "AND pt.Tipo = :tipo " +
             "AND t.EstadoTurnoID <> :estado " +
             (ocupadosIncludeTurnosHuerfanos ? "" : " AND t.EsHuerfano = 0 ") +
             "AND t.Activo = true ";
            if (args.Count > 0)
                hql += "AND (";
            string partialQuery;
            for (int n = 0; n < args.Count; n++)
            {
                partialQuery = "";
                if (args[n].idsCategorias != null && args[n].idsCategorias.Count > 0)
                    partialQuery += "AND pt.Medico.CategoriaMedico IN (:categorias" + n.ToString() + ") ";
                if (args[n].idsMedicos != null && args[n].idsMedicos.Count > 0)
                    partialQuery += "AND pt.Medico IN (:medicos" + n.ToString() + ") ";
                if (args[n].idsEquipos != null && args[n].idsEquipos.Count > 0)
                    partialQuery += "AND t.EquipoId IN (:equipos" + n.ToString() + ") ";
                if (partialQuery != "")
                {
                    // Saltea el AND inicial
                    hql += "(" + partialQuery.Substring(4) + ") OR ";
                }
            }
            if (hql.EndsWith(" OR "))
                hql = hql.Substring(0, hql.Length - 3);
            if (args.Count > 0)
                hql += ")";
            if (hql.EndsWith(" AND ()"))
                hql = hql.Substring(0, hql.Length - 6);
            hql += " ORDER BY t.Fecha";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", fecha);
            query.SetParameter("fechaMasUno", fecha.AddDays(1));
            query.SetInt32("tipo", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("estado", statusToExclude);
            for (int n = 0; n < args.Count; n++)
            {
                if (args[n].idsCategorias != null && args[n].idsCategorias.Count > 0)
                    query.SetParameterList("categorias" + n.ToString(), args[n].idsCategorias);
                if (args[n].idsMedicos != null && args[n].idsMedicos.Count > 0)
                    query.SetParameterList("medicos" + n.ToString(), args[n].idsMedicos);
                if (args[n].idsEquipos != null && args[n].idsEquipos.Count > 0)
                    query.SetParameterList("equipos" + n.ToString(), args[n].idsEquipos);
            }
            EntityCollection<TurnoLight> turnos = dalEngine.GetManyByQuery<TurnoLight>(query);
            if (cargaObjectos)
                CargaObjetosTurnoLight(turnos);

            return turnos;
        }

        public EntityCollection<TurnoLight> TurnoLightRead(DateTime fechaDesde, DateTime fechaHastaSinIncluirla, List<int> medicos, List<int> equipos, List<int> statusToExclude, bool cargaObjectos, bool ordenaPorFecha)
        {
            string hql = "SELECT DISTINCT t FROM TurnoLight t, PracticaTurno pt " +
                "WHERE pt.TurnoId = t.Id " +
                "AND t.Fecha >= :fecha AND t.Fecha < :fechaMasUno " +
                "AND pt.Tipo = :tipo " +
                "AND t.EstadoTurnoID NOT IN (:estado) " +
                "AND t.Activo = true ";
            if (medicos != null && medicos.Count > 0)
                hql += "AND pt.Medico IN (:medicos) ";
            if (equipos != null && equipos.Count > 0)
                hql += "AND t.EquipoId IN (:equipos) ";
            if (ordenaPorFecha)
                hql += " ORDER BY t.Fecha";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", fechaDesde);
            query.SetParameter("fechaMasUno", fechaHastaSinIncluirla);
            query.SetInt32("tipo", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameterList("estado", statusToExclude);

            if (medicos != null && medicos.Count > 0)
                query.SetParameterList("medicos", medicos);
            if (equipos != null && equipos.Count > 0)
                query.SetParameterList("equipos", equipos);

            EntityCollection<TurnoLight> turnos = dalEngine.GetManyByQuery<TurnoLight>(query);
            if (cargaObjectos)
                CargaObjetosTurnoLight(turnos);
            else
                // Pacientes los trae siempre
                CargaPacientesTurnoLight(turnos);
            return turnos;
        }

        [Private]
        public void CargaObjetosTurnoLight(EntityCollection<TurnoLight> turnos)
        {
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            // Trae los práctica turno
            EntityCollection<PracticaTurno> pts = dalEngine.GetManyByPropertyList<PracticaTurno>(PracticaTurno.Properties.TurnoId, turnos.GetIds());
            pts.SortByProperty(PracticaTurno.Properties.TurnoId);
            // Los agrega ordenados
            SortedMultipartData<PracticaTurno, int> practicasTurno = new SortedMultipartData<PracticaTurno, int>(PracticaTurno.Properties.TurnoId);
            practicasTurno.Add(pts);

            foreach (TurnoLight turno in turnos)
            {
                PracticaTurno pt = null;
                foreach (PracticaTurno practicaTurno in practicasTurno.GetManyListBySorted(turno.Id))
                    if (practicaTurno.Tipo == (int)PracticaTurnoTipoEnum.Principal)
                    {
                        pt = practicaTurno;
                        break;
                    }
                if (pt == null)
                    throw new Exception("No se pudo encontrar la practica_turno principal para el turno '" + turno.Id.ToString() + "'.");

                turno.Practica = pt.Practica.Name;

                turno.Status = EstadoTurnoReadById(turno.EstadoTurnoID).Name;
                turno.MedicoID = pt.Medico.Id;
                turno.Medico = pt.Medico.ApyN;
                if (turno.EquipoId.HasValue)
                    turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value).Descripcion;
            }
            CargaPacientesTurnoLight(turnos);
        }
        private void CargaPacientesTurnoLight(EntityCollection<TurnoLight> turnos)
        {
            // Se trae todos los pacientes juntos...
            List<int> pacientes = new List<int>();
            foreach (TurnoLight turno in turnos)
                pacientes.Add(turno.Orden.PacienteId);
            EntityCollection<Paciente> pacs = dalEngine.GetManyByIds<Paciente>(pacientes);
            pacs.SortByProperty(Paciente.Properties.Id);
            foreach (TurnoLight turno in turnos)
            {
                int nPaciente = pacs.BinarySearchByProperty(Paciente.Properties.Id, turno.Orden.PacienteId);
                turno.Paciente = pacs[nPaciente].ApellidoNombre;

                // Si es confidencial y el usuario no ve =>
                if (pacs[nPaciente].IsConfidential && enfoke.Context.Security.DenyConfidential)
                    turno.Practica = turno.Paciente = "******";
            }
        }

        public EntityCollection<TurnoLight> TurnoLightReadByPaciente(int pacienteId)
        {
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, TurnoLight.Properties.Orden.PacienteId, "=", pacienteId);

            filter.Add(BooleanOp.And, TurnoLight.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, TurnoLight.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, TurnoLight.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);

            ReadManyCommand<TurnoLight> readCmd = new ReadManyCommand<TurnoLight>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<TurnoLight> TurnoLightReadByPacientes(List<int> pacientesId)
        {
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, TurnoLight.Properties.Orden.PacienteId, "IN", pacientesId);

            filter.Add(BooleanOp.And, TurnoLight.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, TurnoLight.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, TurnoLight.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);

            ReadManyCommand<TurnoLight> readCmd = new ReadManyCommand<TurnoLight>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<Turno> TurnosReadByPaciente(int pacienteId)
        {
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, Turno.Properties.Orden.PacienteId, "=", pacienteId);
            filter.Add(BooleanOp.And, Turno.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", false);
            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "!=", (int)EstadoTurnoEnum.Cancelado);
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<Turno> TurnosReservadosReadByPaciente(int pacienteId)
        {
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, Turno.Properties.Orden.PacienteId, "=", pacienteId);
            filter.Add(BooleanOp.And, Turno.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", false);
            filter.Add(BooleanOp.And, Turno.Properties.EquipoId, "is not", null);
            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "=", (int)EstadoTurnoEnum.Reservado);
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<TurnoLight> TurnoLightReadByIds(List<int> turnosID)
        {
            return dalEngine.GetManyByIds<TurnoLight>(turnosID);
        }


        // TurnoAdmisionView

        [MinuteTimeout()]
        public virtual EntityCollection<TurnoAdmisionView> TurnoAdmisionViewReadHQL(DateTime? fecha, string practica, string paciente, string protocolo, int? medicoId, string obraSocial, bool reservados, bool recepcionados, bool cancelados, bool ausentes, Sector sector, List<int> centrosIds, string dni, int? servicioId, int maxRows)
        {
            if (string.IsNullOrEmpty(paciente) && string.IsNullOrEmpty(dni) && string.IsNullOrEmpty(protocolo) && !fecha.HasValue)
                throw new NotLoggeableException("La búsqueda sin fecha debe filtrar al menos por paciente o protocolo (3 o mas caracteres).");

            List<int> equipos;
            EntityCollection<Turno> turnos = ConsultaGeneralTurnos(fecha, fecha, practica, paciente, protocolo, medicoId, obraSocial, reservados, recepcionados, cancelados, ausentes, sector, centrosIds, dni, servicioId, maxRows, (int?)null, out equipos);
            return ConstruirRespuestas(maxRows, equipos, turnos);
        }

        [MinuteTimeout()]
        // no borrar este metodo esta de prueba para tomar mediciones en produccion y luego intercambiar el actual con este.
        public virtual EntityCollection<TurnoAdmisionView> TurnoAdmisionViewReadHQLByCombo(Sector sector, int? centro, int? servicio, int comboId, int maxRows)
        {
            List<int> centrosIds = centro.HasValue ? new List<int>() { centro.Value } : new List<int>();
            List<int> equipos = ObtenerEquiposIdsSeleccionados(sector, centrosIds, servicio);
            EntityCollection<Turno> turnos = TurnoReadByCombo(comboId);
            return ConstruirRespuestas(maxRows, equipos, TurnosDeAdmisionIdsReadByTurnosIds(equipos, turnos.GetIds(), true, true, true, true, maxRows));
        }

        private EntityCollection<Turno> ConsultaGeneralTurnos(DateTime? fechaDesde, DateTime? fechaHasta, string practica, string paciente, string protocolo, int? medicoId, string obraSocial, bool reservados, bool recepcionados, bool cancelados, bool ausentes, Sector sector, List<int> centrosIds, string dni, int? servicioId, int maxRows, int? equipoId, out List<int> equipos)
        {
            if (equipoId.HasValue)
            {
                equipos = new List<int>();
                equipos.Add(equipoId.Value);
            }
            else
                equipos = ObtenerEquiposIdsSeleccionados(sector, centrosIds, servicioId);

            EntityCollection<Turno> turnos = new EntityCollection<Turno>();
            if (EsFiltroMultiple(paciente, dni, protocolo, practica, obraSocial, medicoId))
            {
                turnos = TurnosDeAdmisionIdsReadByFilter(fechaDesde, fechaHasta, medicoId, equipos, reservados, recepcionados, cancelados, ausentes, maxRows,
                    paciente, int.Parse(string.IsNullOrEmpty(dni) ? "0" : dni), protocolo, practica, obraSocial);
            }
            else if (!string.IsNullOrEmpty(paciente) || !string.IsNullOrEmpty(dni))
            {
                dni = string.IsNullOrEmpty(dni) ? "0" : dni;
                List<int> turnosIds = this.TurnosIdsReadByPacientes(fechaDesde, fechaHasta, paciente, int.Parse(dni), maxRows, false);
                turnos = TurnosDeAdmisionIdsReadByTurnosIds(fechaDesde, fechaHasta, medicoId, equipos, turnosIds, reservados, recepcionados, cancelados, ausentes, maxRows);
            }
            else if (!string.IsNullOrEmpty(protocolo))
            {
                List<int> turnosIds = this.TurnosIdsReadByProtocolo(protocolo, maxRows);
                turnos = TurnosDeAdmisionIdsReadByTurnosIds(fechaDesde, fechaHasta, medicoId, equipos, turnosIds, reservados, recepcionados, cancelados, ausentes, maxRows);
            }
            else if (!string.IsNullOrEmpty(practica))
            {
                List<int> turnosIds = this.TurnosIdsReadByPracticaAndDate(fechaDesde, fechaHasta, practica, maxRows);
                turnos = TurnosDeAdmisionIdsReadByTurnosIds(fechaDesde, fechaHasta, medicoId, equipos, turnosIds, reservados, recepcionados, cancelados, ausentes, maxRows);
            }
            else if (!string.IsNullOrEmpty(obraSocial))
            {
                EntityCollection<ObraSocial> obrasSociales = Context.Session.ObrasSocialesDalc.ObraSocialSearchByName(obraSocial);
                List<int> turnosIds = this.TurnosIdsReadByObraSocialAndDate(obrasSociales.GetIds(), fechaDesde, fechaHasta, maxRows);
                turnos = TurnosDeAdmisionIdsReadByTurnosIds(fechaDesde, fechaHasta, medicoId, equipos, turnosIds, reservados, recepcionados, cancelados, ausentes, maxRows);
            }
            else
                turnos = TurnosDeAdmisionIdsReadByFilter(fechaDesde, fechaHasta, medicoId, equipos, reservados, recepcionados, cancelados, ausentes, maxRows);

            if (turnos.Count > 0 && (recepcionados || cancelados))
                ObtenerProtocolos(turnos);

            return turnos;
        }

        private void ObtenerProtocolos(EntityCollection<Turno> turnos)
        {
            List<int> ordenes = new List<int>();
            foreach (Turno tur in turnos)
                if (tur.Orden != null)
                    ordenes.Add(tur.Orden.Id);

            EntityCollection<OrdenProtocolo> ordenesProtocolo = OrdenProtocoloReadByOrdenesId(ordenes);

            foreach (OrdenProtocolo ordenProtocolo in ordenesProtocolo)
            {
                Predicate<Turno> predicate = delegate(Turno compare)
                {
                    if (compare.Orden == null)
                        return false;
                    else
                        return compare.Orden.Id == ordenProtocolo.OrdenId;
                };

                foreach (Turno turno in turnos.FindAll(predicate))
                {
                    turno.Protocolo = ordenProtocolo.Protocolo;
                }
            }
        }

        private bool EsFiltroMultiple(string paciente, string dni, string protocolo, string practica, string obraSocial, int? medicoId)
        {
            int cant = 0;
            string[] filtro = new string[] { string.IsNullOrEmpty(paciente) ? dni : paciente, protocolo, practica, obraSocial };
            for (int i = 0; i < filtro.Length; i++)
                if (!string.IsNullOrEmpty(filtro[i]))
                    cant++;

            if (medicoId.HasValue)
                cant++;

            return cant > 1;
        }

        private EntityCollection<Turno> TurnosDeAdmisionIdsReadByTurnosIds(List<int> equiposIds, List<int> turnosIds, bool reservados, bool recepcionados, bool cancelados, bool ausentes, int maxRows)
        {
            if (equiposIds == null || equiposIds.Count == 0)
                return new EntityCollection<Turno>();

            if (turnosIds == null || turnosIds.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Turno(tur.EmbarazadaOBebe, tur.DiscapacidadTemporal, tur.CantidadRecitaciones, ");
            hqlBuilder.Append("tur.ComboId, tur.DuracionSeconds, tur.Fecha, tur.CantPracticasAnestesia, tur.CantPracticasContraste, tur.TurnoOriginalID, ");
            hqlBuilder.Append("tur.EsSobreturno, tur.TipoConfirmacionID, tur.TipoAutorizacionID,tur.MotivoID, tur.TipoTurnoId, tur.Id, tur.InsumosDetallados, tur.EsGuardia, ");
            hqlBuilder.Append("tur.Orden.Id, est, pt.Practica.Name, pt.Medico.Apellido, pt.Medico.Name, pt.Medico.Id, ");
            hqlBuilder.Append("pac.ApellidoNombre, pac.Id, pac.Importancia, pac.DiscapacidadPermanente, tur.Orden.ObraSocialPlanId, pt.Id, tur.EquipoId) from ");
            hqlBuilder.Append("Turno tur, Turno orig, PracticaTurno pt, Paciente pac, EstadoTurno est, ObraSocialPlan osp ");
            hqlBuilder.Append("where (case when tur.TipoTurnoId <> :recitado then tur.Id else tur.TurnoOriginalID end) = orig.Id and ");
            hqlBuilder.Append("(case when tur.EstadoTurnoID <> :cancelado then orig.EstadoTurnoID else tur.EstadoTurnoID end) = est.Id and ");
            hqlBuilder.Append("pt.TurnoId = tur.id and ");
            hqlBuilder.Append("tur.Orden.ObraSocialPlanId = osp.Id and ");
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id and ");
            hqlBuilder.Append("pt.Tipo = :tipoPracticaTurno and ");
            hqlBuilder.Append("(tur.MotivoID is null or tur.MotivoID <> :espontaneosCancelados) ");
            hqlBuilder.Append("and tur.Id in (:turnos) ");
            hqlBuilder.Append("and ( " + resolverFiltroEstados(ausentes, cancelados, reservados, recepcionados) + ")");
            hqlBuilder.Append(" and tur.EquipoId in (:equipos) ");
            hqlBuilder.Append(" order by tur.Fecha ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetParameterList("equipos", equiposIds);
            query.SetInt32("espontaneosCancelados", (int)EstadoTurnoMotivoEnum.EspontaneoCancelado);
            query.SetParameter("tipoPracticaTurno", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetInt32("recitado", (int)TipoTurnoEnum.Recitado);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<Turno>(query);
        }


        private EntityCollection<Turno> TurnosDeAdmisionIdsReadByFilter(DateTime? dateFrom, DateTime? dateTo, int? medicoId, IList<int> equiposIds, bool reservados, bool recepcionados, bool cancelados, bool ausentes, int maxRows,
            string paciente, int dni, string protocolo, string practica, string obraSocial)
        {
            if (equiposIds == null || equiposIds.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Turno(tur.EmbarazadaOBebe, tur.DiscapacidadTemporal, tur.CantidadRecitaciones, ");
            hqlBuilder.Append("tur.ComboId, tur.DuracionSeconds, tur.Fecha, tur.CantPracticasAnestesia, tur.CantPracticasContraste, tur.TurnoOriginalID, ");
            hqlBuilder.Append("tur.EsSobreturno, tur.TipoConfirmacionID, tur.TipoAutorizacionID,tur.MotivoID, tur.TipoTurnoId, tur.Id, tur.InsumosDetallados, tur.EsGuardia, ");
            hqlBuilder.Append("ord.Id, est, pt.Practica.Name, pt.Medico.Apellido, pt.Medico.Name, pt.Medico.Id, ");
            hqlBuilder.Append("pac.ApellidoNombre, pac.Id, pac.Importancia, pac.DiscapacidadPermanente, ord.ObraSocialPlanId, pt.Id, tur.EquipoId, ord) from ");
            hqlBuilder.Append("Turno tur join tur.Orden ord, Turno orig, PracticaTurno pt, Paciente pac, EstadoTurno est, ObraSocialPlan osp ");
            hqlBuilder.Append("where (case when tur.TipoTurnoId <> :recitado then tur.Id else tur.TurnoOriginalID end) = orig.Id and ");
            hqlBuilder.Append("(case when tur.EstadoTurnoID <> :cancelado then orig.EstadoTurnoID else tur.EstadoTurnoID end) = est.Id and ");
            hqlBuilder.Append("pt.TurnoId = tur.id and ");
            hqlBuilder.Append("ord.ObraSocialPlanId = osp.Id and ");
            hqlBuilder.Append("ord.PacienteId = pac.Id and ");
            hqlBuilder.Append("pt.Tipo = :tipoPracticaTurno and ");
            hqlBuilder.Append("(tur.MotivoID is null or tur.MotivoID <> :espontaneosCancelados) ");
            hqlBuilder.Append(" and (" + resolverFiltroEstados(ausentes, cancelados, reservados, recepcionados) + ") ");

            if (dateFrom.HasValue)
                hqlBuilder.Append(" and tur.Fecha >= :fechaDesde ");

            if (dateTo.HasValue)
                hqlBuilder.Append(" and tur.Fecha < :fechaHasta ");

            if (medicoId.HasValue)
                hqlBuilder.Append(" and pt.Medico.Id = :medico ");

            if (!string.IsNullOrEmpty(practica))
                hqlBuilder.Append("and pt.Practica.TipoPractica.Id = :tipoPractica and pt.Practica.Name like :practica ");

            if (!String.IsNullOrEmpty(paciente))
                hqlBuilder.Append(" and pac.ApellidoNombre LIKE :name ");

            if (dni > 0)
                hqlBuilder.Append(" and pac.Dni = :dni ");

            if (!string.IsNullOrEmpty(protocolo))
                hqlBuilder.Append(" and ord.Protocolo.ProtocoloFull = :protocolo ");

            if (!string.IsNullOrEmpty(obraSocial))
                hqlBuilder.Append(" and osp.ObraSocial.Name like :obraSocial ");

            hqlBuilder.Append(" and tur.EquipoId in (:equipos) ");
            hqlBuilder.Append(" order by tur.Fecha desc ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());

            if (!string.IsNullOrEmpty(practica))
            {
                query.SetString("practica", "%" + practica.Trim().Replace(" ", "%") + "%");
                query.SetInt32("tipoPractica", (int)TipoPracticaEnum.Practica);
            }

            if (!string.IsNullOrEmpty(obraSocial))
            {
                query.SetString("obraSocial", "%" + obraSocial.Trim().Replace(" ", "%") + "%");
            }

            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);

            if (!string.IsNullOrEmpty(protocolo))
                query.SetParameter("protocolo", protocolo);

            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.Date.AddDays(1));

            if (medicoId.HasValue)
                query.SetInt32("medico", medicoId.Value);

            if (!String.IsNullOrEmpty(paciente))
                query.SetParameter("name", paciente.Replace(' ', '%') + "%");

            if (dni > 0)
                query.SetInt32("dni", dni);

            query.SetParameterList("equipos", new List<int>(equiposIds));
            query.SetParameter("tipoPracticaTurno", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("espontaneosCancelados", (int)EstadoTurnoMotivoEnum.EspontaneoCancelado);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetInt32("recitado", (int)TipoTurnoEnum.Recitado);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        private static List<int> ObtenerEquiposIdsSeleccionados(Sector sector, List<int> centrosIds, int? servicioId)
        {
            List<int> equipos = new List<int>();

            // agrego los equipos del sector en caso de ser filtro
            if (sector != null && !servicioId.HasValue)
                equipos = Context.Session.EquiposDalc.EquiposIdsPorSector(sector, true);

            if (equipos == null || equipos.Count == 0)
                equipos = Context.Session.EquiposDalc.EquipoIdsReadByServicioIdAndSucursalId(servicioId, centrosIds, true);

            return equipos;
        }

        private List<int> TurnosIdsReadByProtocolo(string protocolo, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select tur.id ");
            hqlBuilder.Append("from TurnoLight tur, Protocolo pro ");
            hqlBuilder.Append("where tur.Orden.ProtocoloId = pro.Id and pro.ProtocoloFull = :protocolo ");
            hqlBuilder.Append("and tur.Activo = true and tur.Deleted = false  ");
            // hqlBuilder.Append("order by tur.Fecha desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetString("protocolo", protocolo);
            query.SetMaxResults(maxRows);
            return new List<int>(query.List<int>());
        }

        private List<int> TurnosIdsReadByPracticaAndDate(DateTime? dateFrom, DateTime? dateTo, string practica, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select tur.id ");
            hqlBuilder.Append("from Turno tur, PracticaTurno pt ");
            hqlBuilder.Append("where pt.TurnoId = tur.id and pt.Practica.TipoPractica.Id = :tipoPractica and pt.Practica.Name like :practica ");
            hqlBuilder.Append("and   pt.Practica.Deleted = false ");
            hqlBuilder.Append("and tur.Activo = true and tur.Deleted = false  ");
            if (dateFrom.HasValue)
                hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
            if (dateTo.HasValue)
                hqlBuilder.Append("and tur.Fecha < :fechaHasta ");
            hqlBuilder.Append("order by tur.Fecha desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);
            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.Date.AddDays(1));
            query.SetString("practica", "%" + practica.Trim().Replace(" ", "%") + "%");
            query.SetInt32("tipoPractica", (int)TipoPracticaEnum.Practica);
            query.SetMaxResults(maxRows);
            return new List<int>(query.List<int>());
        }

        internal List<int> TurnosIdsReadByObraSocialAndDate(IList<int> osIds, DateTime? dateFrom, DateTime? dateTo, int maxRows)
        {
            if (osIds == null || osIds.Count == 0)
                return new List<int>();

            if (maxRows > 1000)
                maxRows = 1000;

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select tur.id ");
            hqlBuilder.Append("from Turno tur, ObraSocialPlan osp ");
            hqlBuilder.Append("where tur.Orden.ObraSocialPlanId = osp.Id and osp.ObraSocial.Id in (:obrasSociales) ");
            hqlBuilder.Append("and tur.Activo = true and tur.Deleted = false ");
            if (dateFrom.HasValue)
                hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
            if (dateTo.HasValue)
                hqlBuilder.Append("and tur.Fecha < :fechaHasta ");
            hqlBuilder.Append("order by tur.Fecha desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);
            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.Date.AddDays(1));
            query.SetParameterList("obrasSociales", new EntityCollection<int>(osIds));
            query.SetMaxResults(maxRows);
            return new List<int>(query.List<int>());
        }

        private List<int> TurnosIdsReadByPacientes(DateTime? dateFrom, DateTime? dateTo, string name, int dni, int maxRows, bool noTraerCancelados)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tur.id from Turno tur, Paciente pac where ");
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id and ");
            hqlBuilder.Append("tur.Activo = true and tur.Deleted = false ");
            if (noTraerCancelados)
                hqlBuilder.Append("and tur.EstadoTurnoID <> :cancelado ");

            if (dateFrom.HasValue)
                hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
            if (dateTo.HasValue)
                hqlBuilder.Append("and tur.Fecha < :fechaHasta ");
            if (!String.IsNullOrEmpty(name))
                hqlBuilder.Append(" and pac.ApellidoNombre LIKE :name ");
            if (dni > 0)
                hqlBuilder.Append(" and pac.Dni = :dni ");


            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());

            if (noTraerCancelados)
                query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);
            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.AddDays(1).Date);
            if (!String.IsNullOrEmpty(name))
                query.SetParameter("name", name.Replace(' ', '%') + "%");
            if (dni > 0)
                query.SetInt32("dni", dni);

            query.SetMaxResults(maxRows);
            return new List<int>(query.List<int>());
        }

        private EntityCollection<TurnoAdmisionView> ConstruirRespuestas(int maxRows, IList<int> equiposIds, EntityCollection<Turno> turnosAfectados)
        {
            if (turnosAfectados == null || turnosAfectados.Count == 0)
                return new EntityCollection<TurnoAdmisionView>();

            EntityCollection<Equipo> equipos = Context.Session.EquiposDalc.EquipoReadByIds(equiposIds);
            EntityCollection<TurnoAdmisionView> response = this.CrearRespuestas(turnosAfectados, equipos);
            SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorEquipos;
            SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorOSPlanes;
            IList<int> combos = TurnosIdsConCombos(turnosAfectados.GetIds(), maxRows);
            IList<int> escaneos = TurnosIdsConEscaneos(turnosAfectados.GetIds(), maxRows);
            IList<int> infoInternacion = TurnosIdsConInfoInternacion(turnosAfectados.GetIds(), maxRows);
            AgruparPorEquipoYOSPlan(turnosAfectados, combos, escaneos, infoInternacion, response, out turnosPorEquipos, out turnosPorOSPlanes);
            AgregarEquipos(turnosPorEquipos, equipos);
            AgregarObraSocial(turnosPorOSPlanes);
            AgregarTipoAutorizacionYEstadoTurnoMotivo(response);
            //   AgregarRelacionConTurnoInforme(response);
            return response;
        }

        private void AgregarRelacionConTurnoInforme(EntityCollection<TurnoAdmisionView> response)
        {
            List<int> turnosIds = (from tav in response select tav.Id).ToList();
            int entregadoSinInformeID = (int)EstadoInformeEnum.EntregadoSinInforme;

            var turIdsRetiroYEstado = (from ti in dalEngine.Query<TurnoInforme>()
                                       where turnosIds.Contains(ti.TurnoID)
                                       select new { TurnoId = ti.TurnoID, RetiroPlacas = ti.EntregaPlacas, EstadoInformeId = ti.EstadoInforme.Id });

            foreach (TurnoAdmisionView tav in response)
            {
                tav.YaRetiroPlacas = (from tri in turIdsRetiroYEstado where tav.Id == tri.TurnoId select tri.RetiroPlacas).FirstOrDefault();
                EntityCollection<int> entregadosSinInforme = (from tri in turIdsRetiroYEstado
                                                              where
                                                               tav.Id == tri.TurnoId &&
                                                               tri.EstadoInformeId == entregadoSinInformeID
                                                              select tri.EstadoInformeId).ToEntityCollection();
                tav.HayInformesEntregadosSinInformes = entregadosSinInforme.Count > 0;
            }
        }

        private IList<int> TurnosIdsConCombos(List<int> turnosIds, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            if (turnosIds == null || turnosIds.Count == 0)
                return new List<int>();

            if (turnosIds.Count > 1000)
                turnosIds.RemoveRange(1000, turnosIds.Count - 1000);

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tur.id from Turno tur, Turno tur2 where tur.ComboId = tur2.ComboId and tur.Id <> tur2.Id ");
            hqlBuilder.Append(" and tur.Id in (:turnos) and tur2.EstadoTurnoID <> :cancelado ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetMaxResults(maxRows);
            return query.List<int>();
        }


        public EntityCollection<TurnoInforme> TurnosInformeIdPrometido(List<int> turnosIds, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            if (turnosIds == null || turnosIds.Count == 0)
                return new EntityCollection<TurnoInforme>();

            if (turnosIds.Count > 1000)
                turnosIds.RemoveRange(1000, turnosIds.Count - 1000);

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct new enfoke.Eges.Entities.TurnoInforme(tui.TurnoID, tui.Prometido, tui.EstadoInforme.EsVisible) from TurnoInforme tui  ");
            hqlBuilder.Append(" where  tui.TurnoID in (:turnos) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<TurnoInforme>(query);
        }
        public IList<int> TurnosIdsConInformes(List<int> turnosIds, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            if (turnosIds == null || turnosIds.Count == 0)
                return new List<int>();

            if (turnosIds.Count > 1000)
                turnosIds.RemoveRange(1000, turnosIds.Count - 1000);

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tui.TurnoID from TurnoInforme tui where tui.EstadoInforme.EsVisible = true ");
            hqlBuilder.Append(" and tui.TurnoID in (:turnos) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetMaxResults(maxRows);
            return query.List<int>();
        }

        public IList<int> TurnosIdsReadByProtocolo(string protocolo)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tur.Id from Turno tur join tur.Orden ord join ord.Protocolo pro where tur.Deleted = false and pro.ProtocoloFull = :protocolo ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetString("protocolo", protocolo);
            return query.List<int>();
        }

        private IList<int> TurnosIdsConEscaneos(List<int> turnosIds, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            if (turnosIds == null || turnosIds.Count == 0)
                return new List<int>();

            if (turnosIds.Count > 1000)
                turnosIds.RemoveRange(1000, turnosIds.Count - 1000);

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tur.id from Turno tur, OrdenPaginaEscaneo ope where tur.Orden.Id = ope.OrdenId ");
            hqlBuilder.Append(" and tur.Id in (:turnos) and tur.EstadoTurnoID <> :cancelado ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetMaxResults(maxRows);
            return query.List<int>();
        }

        private IList<int> TurnosIdsConInfoInternacion(List<int> turnosIds, int maxRows)
        {
            if (maxRows > 1000)
                maxRows = 1000;

            if (turnosIds == null || turnosIds.Count == 0)
                return new List<int>();

            if (turnosIds.Count > 1000)
                turnosIds.RemoveRange(1000, turnosIds.Count - 1000);

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct tur.id from Turno tur, TurnoInfoInternacion tii where tur.Orden.InfoInternacion = tii.Id ");
            hqlBuilder.Append(" and tur.Id in (:turnos) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds);
            query.SetMaxResults(maxRows);
            return query.List<int>();
        }

        private void AgregarTipoAutorizacionYEstadoTurnoMotivo(EntityCollection<TurnoAdmisionView> response)
        {
            EntityCollection<TipoAutorizacion> tipoAutorizaciones = dalEngine.GetAll<TipoAutorizacion>();
            EntityCollection<EstadoTurnoMotivo> estadosTurnosMotivos = EstadoTurnoMotivoReadAll(false);
            foreach (TurnoAdmisionView view in response)
            {
                view.MezclarConTipoAutorizacion(tipoAutorizaciones.FindByKey(view.TipoAutorizacionID));
                if (view.EstadoMotivoId.HasValue)
                    view.MezclarConEstadoTurnoMotivo(estadosTurnosMotivos.FindByKey(view.EstadoMotivoId.Value));

                view.SetColorByEstadoAndTipoAutorizacion();
            }
        }

        private static void AgregarObraSocial(SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorOSPlans)
        {

            EntityCollection<ObraSocialPlanLight> osPlanesDeTurnos = Context.Session.ObrasSocialesDalc.PlanLightReadByIds(new List<int>(turnosPorOSPlans.Keys));
            foreach (ObraSocialPlanLight osp in osPlanesDeTurnos)
            {
                foreach (TurnoAdmisionView turno in turnosPorOSPlans[osp.Id])
                    turno.ObraSocial = osp.ObraSocialName.Name;
            }
        }

        private static void AgruparPorEquipoYOSPlan(EntityCollection<Turno> turnosAfectados, IList<int> combos, IList<int> escaneos, IList<int> infoInternacion, EntityCollection<TurnoAdmisionView> response, out SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorEquipos, out SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorOSPlans)
        {
            turnosPorEquipos = new SortedDictionary<int, EntityCollection<TurnoAdmisionView>>();
            turnosPorOSPlans = new SortedDictionary<int, EntityCollection<TurnoAdmisionView>>();
            for (int index = 0; index < turnosAfectados.Count; index++)
            {
                TurnoAdmisionView current = response[index];
                current.EsCombo = combos.Contains(current.Id);
                current.Escaneos = escaneos.Contains(current.Id);
                current.TieneInfoInternacion = infoInternacion.Contains(current.Id);
                EntityCollection<TurnoAdmisionView> turnosPorEquipo;
                if (!turnosPorEquipos.TryGetValue(turnosAfectados[index].EquipoId.GetValueOrDefault(), out turnosPorEquipo))
                {
                    turnosPorEquipo = new EntityCollection<TurnoAdmisionView>();
                    turnosPorEquipos.Add(turnosAfectados[index].EquipoId.GetValueOrDefault(), turnosPorEquipo);
                }

                turnosPorEquipo.Add(current);

                EntityCollection<TurnoAdmisionView> turnosPorOsPlan;
                if (!turnosPorOSPlans.TryGetValue(turnosAfectados[index].OSPlanId, out turnosPorOsPlan))
                {
                    turnosPorOsPlan = new EntityCollection<TurnoAdmisionView>();
                    turnosPorOSPlans.Add(turnosAfectados[index].OSPlanId, turnosPorOsPlan);
                }

                turnosPorOsPlan.Add(current);
            }
        }

        private void AgregarEquipos(SortedDictionary<int, EntityCollection<TurnoAdmisionView>> turnosPorEquipos, EntityCollection<Equipo> equipos)
        {
            foreach (Equipo equipo in equipos)
            {
                EntityCollection<TurnoAdmisionView> turnos;
                if (turnosPorEquipos.TryGetValue(equipo.Id, out turnos))
                {
                    foreach (TurnoAdmisionView view in turnos)
                        view.MezclarConEquipo(equipo);
                }
            }
        }

        private EntityCollection<TurnoAdmisionView> CrearRespuestas(EntityCollection<Turno> turnosAfectados, EntityCollection<Equipo> equipos)
        {
            EntityCollection<TurnoAdmisionView> response = new EntityCollection<TurnoAdmisionView>();
            for (int index = 0; index < turnosAfectados.Count; index++)
            {
                Turno turno = turnosAfectados[index];
                ///--> obtengo el bool entregaPlaca para el turno
                ///--> si me da null la busqueda, entregaPlaca queda en false.
                bool entregaPlaca = false;
                Equipo equipo = turno.EquipoId.HasValue ? equipos.FindByKey(turno.EquipoId.Value) : null;
                if (equipo != null)
                    entregaPlaca = equipo.Servicio.EntregaPlacas;

                TurnoAdmisionView view = new TurnoAdmisionView(turno.EmbarazadaOBebe.GetValueOrDefault(), turno.DiscapacidadTemporal.GetValueOrDefault(), turno.CantidadRecitaciones, turno.ComboId, turno.DuracionSeconds, turno.Fecha, turno.CantPracticasAnestesia, turno.CantPracticasContraste, turno.TurnoOriginalID, turno.EsSobreturno, turno.TipoConfirmacionID, turno.TipoAutorizacionID, turno.Motivo, turno.TipoTurnoId, turno.Id, turno.InsumosDetallados,
                    turno.EsGuardia, turno.Orden.Id, turno.Protocolo, turno.Paciente, turno.PacienteId, turno.ImportanciaPaciente, turno.DiscapacidadPaciente,
                    turno.PracticaName, turno.MedicoApellido, turno.MedicoNombre, turno.MedicoId, turno.PracticaTurnoId, turno.EstadoTurno, entregaPlaca);

                response.Add(view);
            }

            return response;
        }

        private EntityCollection<Turno> TurnosDeAdmisionIdsReadByFilter(DateTime? dateFrom, DateTime? dateTo, int? medicoId, IList<int> equiposIds, bool reservados, bool recepcionados, bool cancelados, bool ausentes, int maxRows)
        {
            if (equiposIds == null || equiposIds.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Turno(tur.EmbarazadaOBebe, tur.DiscapacidadTemporal, tur.CantidadRecitaciones, ");
            hqlBuilder.Append("tur.ComboId, tur.DuracionSeconds, tur.Fecha, tur.CantPracticasAnestesia, tur.CantPracticasContraste, tur.TurnoOriginalID, ");
            hqlBuilder.Append("tur.EsSobreturno, tur.TipoConfirmacionID, tur.TipoAutorizacionID,tur.MotivoID, tur.TipoTurnoId, tur.Id, tur.InsumosDetallados, tur.EsGuardia, ");
            hqlBuilder.Append("ord.Id, est, pt.Practica.Name, pt.Medico.Apellido, pt.Medico.Name, pt.Medico.Id, ");
            hqlBuilder.Append("pac.ApellidoNombre, pac.Id, pac.Importancia, pac.DiscapacidadPermanente, ord.ObraSocialPlanId, pt.Id, tur.EquipoId, ord) from ");
            hqlBuilder.Append("Turno tur join tur.Orden ord, Turno orig, PracticaTurno pt, Paciente pac, EstadoTurno est, ObraSocialPlan osp ");
            hqlBuilder.Append("where (case when tur.TipoTurnoId <> :recitado then tur.Id else tur.TurnoOriginalID end) = orig.Id and ");
            hqlBuilder.Append("(case when tur.EstadoTurnoID <> :cancelado then orig.EstadoTurnoID else tur.EstadoTurnoID end) = est.Id and ");
            hqlBuilder.Append("pt.Tipo = :ptTipoPractica and ");
            hqlBuilder.Append("pt.TurnoId = tur.id and ");
            hqlBuilder.Append("ord.ObraSocialPlanId = osp.Id and ");
            hqlBuilder.Append("ord.PacienteId = pac.Id and ");

            hqlBuilder.Append("(tur.MotivoID is null or tur.MotivoID <> :espontaneosCancelados) ");



            if (dateFrom.HasValue)
                hqlBuilder.Append(" and tur.Fecha >= :fechaDesde ");

            if (dateTo.HasValue)
                hqlBuilder.Append(" and tur.Fecha < :fechaHasta ");

            if (medicoId.HasValue)
                hqlBuilder.Append(" and pt.Medico.Id = :medico ");

            hqlBuilder.Append(" and (" + resolverFiltroEstados(ausentes, cancelados, reservados, recepcionados) + ") ");
            hqlBuilder.Append(" and tur.EquipoId in (:equipos) ");
            hqlBuilder.Append(" order by tur.Fecha  ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);

            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.Date.AddDays(1));

            if (medicoId.HasValue)
                query.SetInt32("medico", medicoId.Value);

            query.SetLockMode("tur", LockMode.None);
            query.SetLockMode("ord", LockMode.None);
            query.SetLockMode("orig", LockMode.None);
            query.SetLockMode("pt", LockMode.None);
            query.SetLockMode("pac", LockMode.None);
            query.SetLockMode("osp", LockMode.None);

            query.SetParameterList("equipos", new List<int>(equiposIds));
            query.SetParameter("ptTipoPractica", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("espontaneosCancelados", (int)EstadoTurnoMotivoEnum.EspontaneoCancelado);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetInt32("recitado", (int)TipoTurnoEnum.Recitado);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<Turno>(query);
        }

        private EntityCollection<Turno> TurnosDeAdmisionIdsReadByTurnosIds(DateTime? dateFrom, DateTime? dateTo, int? medicoId, List<int> equiposIds, List<int> turnosIds, bool reservados, bool recepcionados, bool cancelados, bool ausentes, int maxRows)
        {
            if (equiposIds == null || equiposIds.Count == 0)
                return new EntityCollection<Turno>();

            if (turnosIds == null || turnosIds.Count == 0)
                return new EntityCollection<Turno>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.Turno(tur.EmbarazadaOBebe, tur.DiscapacidadTemporal, tur.CantidadRecitaciones, ");
            hqlBuilder.Append("tur.ComboId, tur.DuracionSeconds, tur.Fecha, tur.CantPracticasAnestesia, tur.CantPracticasContraste, tur.TurnoOriginalID, ");
            hqlBuilder.Append("tur.EsSobreturno, tur.TipoConfirmacionID, tur.TipoAutorizacionID,tur.MotivoID, tur.TipoTurnoId, tur.Id, tur.InsumosDetallados, tur.EsGuardia, ");
            hqlBuilder.Append("ord.Id, est, pt.Practica.Name, pt.Medico.Apellido, pt.Medico.Name, pt.Medico.Id, ");
            hqlBuilder.Append("pac.ApellidoNombre, pac.Id, pac.Importancia, pac.DiscapacidadPermanente, ord.ObraSocialPlanId, pt.Id, tur.EquipoId, ord) from ");
            hqlBuilder.Append("Turno tur join tur.Orden ord, Turno orig, PracticaTurno pt, Paciente pac, EstadoTurno est, ObraSocialPlan osp ");
            hqlBuilder.Append("where (case when tur.TipoTurnoId <> :recitado then tur.Id else tur.TurnoOriginalID end) = orig.Id and ");
            hqlBuilder.Append("(case when tur.EstadoTurnoID <> :cancelado then orig.EstadoTurnoID else tur.EstadoTurnoID end) = est.Id and ");
            hqlBuilder.Append("pt.Tipo = :ptTipoPractica and ");
            hqlBuilder.Append("pt.TurnoId = tur.id and ");
            hqlBuilder.Append("ord.ObraSocialPlanId = osp.Id and ");
            hqlBuilder.Append("ord.PacienteId = pac.Id and ");
            hqlBuilder.Append("pt.Tipo = :tipoPracticaTurno and ");
            hqlBuilder.Append("(tur.MotivoID is null or tur.MotivoID <> :espontaneosCancelados) ");
            hqlBuilder.Append("and tur.Id in (:turnos) ");
            if (dateFrom.HasValue)
                hqlBuilder.Append(" and tur.Fecha >= :fechaDesde ");
            if (dateTo.HasValue)
                hqlBuilder.Append(" and tur.Fecha < :fechaHasta ");

            if (medicoId.HasValue)
                hqlBuilder.Append(" and pt.Medico.Id = :medico ");

            hqlBuilder.Append("and ( " + resolverFiltroEstados(ausentes, cancelados, reservados, recepcionados) + ")");
            hqlBuilder.Append(" and tur.EquipoId in (:equipos) ");
            hqlBuilder.Append(" order by tur.Fecha ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (dateFrom.HasValue)
                query.SetDateTime("fechaDesde", dateFrom.Value.Date);
            if (dateTo.HasValue)
                query.SetDateTime("fechaHasta", dateTo.Value.AddDays(1).Date);

            if (medicoId.HasValue)
                query.SetInt32("medico", medicoId.Value);

            query.SetParameterList("turnos", new List<int>(turnosIds));
            query.SetParameter("ptTipoPractica", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameterList("equipos", new List<int>(equiposIds));
            query.SetInt32("espontaneosCancelados", (int)EstadoTurnoMotivoEnum.EspontaneoCancelado);
            query.SetParameter("tipoPracticaTurno", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetInt32("recitado", (int)TipoTurnoEnum.Recitado);
            query.SetMaxResults(maxRows);

            query.SetLockMode("tur", LockMode.None);
            query.SetLockMode("ord", LockMode.None);
            query.SetLockMode("orig", LockMode.None);
            query.SetLockMode("pt", LockMode.None);
            query.SetLockMode("osp", LockMode.None);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        private string resolverFiltroEstados(bool ausentes, bool cancelados, bool reservados, bool recepcionados)
        {
            string condicionOr = String.Empty;
            if (ausentes)
                condicionOr = " est.Ausente = true";

            if (cancelados)
                condicionOr += ((!String.IsNullOrEmpty(condicionOr)) ? " OR " : "") + " est.Cancelado = true";

            // Los recitado Pendiente y Arecitar se ven tambien como reservados
            if (reservados)
            {
                condicionOr += ((!String.IsNullOrEmpty(condicionOr)) ? " OR " : "") + " est.Pendiente = true";
                condicionOr += ((!String.IsNullOrEmpty(condicionOr)) ? " OR " : "") + " est.Id = " + ((int)EstadoTurnoEnum.ARecitar).ToString();
                condicionOr += ((!String.IsNullOrEmpty(condicionOr)) ? " OR " : "") + " est.Id = " + ((int)EstadoTurnoEnum.RecitadoPendiente).ToString();
            }

            if (recepcionados)
                condicionOr += ((!String.IsNullOrEmpty(condicionOr)) ? " OR " : "") + " est.Atendido = true";

            return condicionOr;
        }

        public EntityCollection<Paciente> PacienteSearchByNameAndDNI(string name, int dni)
        {
            ReadManyCommand<Paciente> readCmd = new ReadManyCommand<Paciente>(dalEngine);
            Filter filter = new Filter();
            if (!string.IsNullOrEmpty(name))
                Privacy.AddConfidentialFilter(name.Trim().Replace(" ", "%") + "%", filter, Paciente.Properties.ApellidoNombre, Paciente.Properties.Importancia, BooleanOp.And);
            if (dni > 0)
                Privacy.AddConfidentialFilter(dni.ToString(), "=", filter, Paciente.Properties.Dni, Paciente.Properties.Importancia, BooleanOp.And);

            filter.Add(BooleanOp.And, Paciente.Properties.Deleted, "=", false);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }


        /// <summary>
        /// Trae el listado de turnosIds para mostrar en admision que pertenezcan
        /// a un combo en particular
        /// </summary>
        /// <param name="comboId">El Id del combo</param>
        /// <returns>Listado de turnosIds</returns>
        public EntityCollection<TurnoAdmisionView> TurnoAdmisionViewReadByCombo(int comboId)
        {
            EntityCollection<TurnoAdmisionView> turnos = TurnoAdmisionViewReadHQLByCombo(null, null, null, comboId, 100);
            foreach (TurnoAdmisionView t in turnos)
                t.Color = GetColorByEstadoAndTipoAutorizacion(t.EstadoTurno.Id, t.TipoAutorizacionID, false);
            return turnos;
        }


        private EntityCollection<TurnoAdmisionView> TurnoAdmisionViewRead(Filter filter, Sort sort, int maxRows)
        {
            ReadManyCommand<TurnoAdmisionView> readCmd = new ReadManyCommand<TurnoAdmisionView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            EntityCollection<TurnoAdmisionView> turnos = readCmd.Execute();

            foreach (TurnoAdmisionView t in turnos)
                t.Color = GetColorByEstadoAndTipoAutorizacion(t.EstadoTurno.Id, t.TipoAutorizacionID, false);

            return turnos;
        }


        private List<int> GetStatusEnumRangeInt(EstadoTurnoEnum[] statuses)
        {
            List<int> ret = new List<int>();
            for (int i = 0; i < statuses.Length; i++)
                ret.Add((int)statuses[i]);

            return ret;
        }

        // TurnoAutorizacionView
        /// <summary>
        /// [RQ] Trae los turnosIds para el formulario de autorizaciones, filtra por fecha y tiene una búsqueda
        /// </summary>
        /// <param name="provisorios">Marca si busco Provisorios, Sino son Normales</param>
        /// <param name="turnoDesde">Fecha Turno Desde</param>
        /// <param name="turnoHasta">Fecha Turno Hasta</param>
        /// <param name="cargaDesde">Fecha Carga Desde [Opcional]</param>
        /// <param name="cargaHasta">Fecha Carga Hasta [Opcional]</param>
        /// <param name="searchString">Texto a buscar</param>
        /// <param name="searchType">Tipo de Busqueda [Campo donde buscar]</param>
        /// <returns>La colección de turnosIds según los criterios de búsqueda</returns>
        [MinuteTimeout]
        public virtual EntityCollection<TurnoAutorizacionView> TurnoAutorizacionViewRead(bool provisorios, DateTime turnoDesde, DateTime turnoHasta, DateTime? cargaDesde, DateTime? cargaHasta, string medico, string paciente, string obraSocial, string servicio, List<int> centrosIds, int tipoAutorizacion, int maxRows)
        {
            ReadManyCommand<TurnoAutorizacionView> readCmd = new ReadManyCommand<TurnoAutorizacionView>(dalEngine);

            Filter filter = new Filter();

            if (provisorios)
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Fecha, " IS ", null);
            else
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Fecha, ">=", turnoDesde.Date);

                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Fecha, "<", turnoHasta.Date.AddDays(1));
            }
            if (tipoAutorizacion > 0)
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.TipoAutorizacionID, "=", tipoAutorizacion);

            if (cargaDesde.HasValue)
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.FechaCreacion, ">=", cargaDesde.Value.Date);

            if (cargaHasta.HasValue)
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.FechaCreacion, "<", cargaHasta.Value.AddDays(1).Date);

            if (!String.IsNullOrEmpty(medico))
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Medico, "LIKE", medico.Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Paciente, "LIKE", paciente.Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(obraSocial))
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.ObraSocial, "LIKE", obraSocial.Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.Servicio, "LIKE", servicio.Replace(" ", "%") + "%");
            }

            if (centrosIds.Count > 0)
            {
                filter.Add(BooleanOp.And, TurnoAutorizacionView.Properties.SucursalId, "IN", centrosIds);
            }

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(TurnoAutorizacionView.Properties.Fecha, SortingDirection.Asc);
            readCmd.Sort = sort;
            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            return readCmd.Execute();
        }

        // InformesTurnos
        /// <summary>
        /// [RQ] Obtengo la cantidad de informes de cada tipo por servicioId para una obra social
        /// en base a todos los comprobantes facturados de un año/mes en particular
        /// </summary>
        /// <param name="ano">Año del que se tomarán los comprobantes</param>
        /// <param name="mes">Mes del que se tomarán los comprobantes</param>
        /// <param name="obraSocialID">la obra social para la cual traer los resultados</param>
        /// <returns>La cantidad de cada tipo de informe por servicioId para una obra social</returns>
        public EntityCollection<InformeObraSocialView> InformeObraSocialViewReadByFechaAndObraSocial(int ano, int mes, int obraSocialID)
        {
            ReadManyCommand<InformeObraSocialView> readCmd = new ReadManyCommand<InformeObraSocialView>(dalEngine);

            Filter filter = new Filter();

            // Filtro OS
            filter.Add(InformeObraSocialView.Properties.ObraSocialID, "=", obraSocialID);

            // Filtro Año
            filter.Add(BooleanOp.And, InformeObraSocialView.Properties.Ano, "=", ano);

            // Filtro Mes
            filter.Add(BooleanOp.And, InformeObraSocialView.Properties.Mes, "=", mes);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(InformeObraSocialView.Properties.Servicio, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }


        [Private]
        public EntityCollection<TurnoTimePeriod> TurnoTimePeriodReadByIds(List<int> turnoIDs)
        {
            return dalEngine.GetManyByIds<TurnoTimePeriod>(turnoIDs);
        }

        [Private]
        public EntityCollection<TurnoTimePeriod> TurnoTimePeriodReadByEquipo(DateTime desde, DateTime hasta, int equipoId)
        {
            ReadManyCommand<TurnoTimePeriod> readCmd = new ReadManyCommand<TurnoTimePeriod>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Fecha, ">=", desde.Date);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Fecha, "<", hasta.AddDays(1).Date);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.EquipoId, "=", equipoId);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.EstadoTurnoID, "NOT IN", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [Private]
        public EntityCollection<TurnoTimePeriod> TurnoTimePeriodReadByEquipos(DateTime desde, DateTime hasta, List<int> equiposId, bool ocupadosIncluyenTurnosHuerfanos)
        {
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Fecha, ">=", desde.Date);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Fecha, "<", hasta.AddDays(1).Date);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.EquipoId, "IN", equiposId);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.EstadoTurnoID, "NOT IN", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });

            if (ocupadosIncluyenTurnosHuerfanos == false)
                filter.Add(BooleanOp.And, TurnoTimePeriod.Properties.EsHuerfano, "=", false);

            Sort sort = new Sort();
            sort.Add(TurnoTimePeriod.Properties.EquipoId);

            return dalEngine.GetManyByFilter<TurnoTimePeriod>(filter, sort);
        }

        // Protocolo
        /// <summary>
        /// Retorna un Protocolo por su servicioId y numero
        /// </summary>
        /// <param name="Equipo">Equipo del Turno del Protocolo buscado</param>
        /// <param name="numero">Número del Protocolo buscado</param>
        /// <returns>El Protocolo con el servicioId y número indicados</returns>
        public Protocolo ProtocoloReadByEquipoAndNumero(Equipo equipo, int numero)
        {
            if (equipo.Sucursal.NumeracionPorServicio)
                return ProtocoloReadBySucursalOrigenAndNumero(equipo.Sucursal, equipo.Servicio.Tag, numero);
            else
                return ProtocoloReadBySucursalOrigenAndNumero(equipo.Sucursal, SucursalProtocolo.OrigenPorDefecto, numero);
        }

        /// <summary>
        /// Retorna un Protocolo por su servicioId y numero
        /// </summary>
        /// <param name="Sucursal">Sucursal del Protocolo buscado</param>
        /// <param name="origen">Origen del Protocolo buscado</param>
        /// <param name="numero">Número del Protocolo buscado</param>
        /// <returns>El Protocolo con el servicioId y número indicados</returns>
        public Protocolo ProtocoloReadBySucursalOrigenAndNumero(SucursalName sucursal, string origen, int numero)
        {
            ReadManyCommand<Protocolo> readCmd = new ReadManyCommand<Protocolo>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Protocolo.Properties.SucursalID, "=", sucursal.Id);

            if (!String.IsNullOrEmpty(origen))
                filter.Add(BooleanOp.And, Protocolo.Properties.Origen, "=", origen);

            filter.Add(BooleanOp.And, Protocolo.Properties.Numero, "=", numero);

            readCmd.Filter = filter;

            EntityCollection<Protocolo> protos = readCmd.Execute();

            if (protos.Count > 0)
                return protos[0];
            else
                return null;
        }

        /// <summary>
        /// Inserto un Protocolo
        /// </summary>
        /// <param name="equipo">Equipo del Turno del Protocolo</param>
        /// <param name="numero">Número del Protocolo</param>
        /// <returns>Un Objeto Protocolo</returns>
        [RequiresTransaction]
        public virtual Protocolo ProtocoloInsert(Equipo equipo, int numero)
        {
            // Obtengo cual Seria el Proximo Protocolo del Servicio
            SucursalProtocolo proximo = SucursalProtocoloReadBySucursalAndOrigen(equipo);

            // Valido que el Protocolo Manual no sea Superior al Proximo Automatico
            if (numero > proximo.UltimoProtocolo)
                throw new Exception("El número de protocolo ingresado [" + numero.ToString() + "] no puede ser mayor al próximo automático [" + proximo.UltimoProtocolo.ToString() + "].");

            // Obtengo el nuevo Protocolo
            Protocolo protocolo = ProtocoloCreate(equipo, numero);

            return protocolo;
        }

        public Protocolo ProtocoloReadByOrden(int ordenId)
        {
            Orden orden = dalEngine.GetById<Orden>(ordenId);

            return orden.Protocolo;
        }

        /// <summary>
        /// Devuelve el protocolo del turno. Si el turno pertenece a una orden multiple, trae el protocolo de la misma
        /// </summary>
        /// <param name="turnoID"></param>
        /// <returns></returns>
        public Protocolo ProtocoloReadByTurno(int turnoID)
        {
            string hql = "select tur.Orden.Protocolo from Turno tur WHERE tur.Id = :turnoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoId", turnoID);
            return dalEngine.GetByQuery<Protocolo>(query);
        }

        public Protocolo ProtocoloReadByProtocolo(int protocoloID)
        {
            return dalEngine.GetById<Protocolo>(protocoloID);
        }


        // Turno

        private DatosTurnoPracticaLight CrearDatoTurnoPractica(TurnoLight turno, PracticaTurno practicaTurno)
        {
            DatosTurnoPracticaLight dtpl = new DatosTurnoPracticaLight();
            dtpl.TurnoId = turno.Id;
            dtpl.PracticaId = practicaTurno.Practica.Id;
            dtpl.Practica = practicaTurno.Practica.Name;
            dtpl.Fecha = turno.Fecha;
            dtpl.PacienteNombre = turno.Paciente;
            dtpl.EquipoId = turno.EquipoId;
            dtpl.TipoTurno = turno.TipoTurnoID;
            return dtpl;
        }


        public Turno TurnoReadByPresupuestosId(int presupuestoId)
        {
            return dalEngine.GetByProperty<Turno>(Turno.Properties.PresupuestoId, presupuestoId);
        }

        public EntityCollection<DatosTurnoPracticaLight> DatosTurnosPracticaLightReadByTurnos(List<int> turnosId, bool soloPracticaPrincipal)
        {
            if (turnosId.Count <= 0)
                return new EntityCollection<DatosTurnoPracticaLight>();

            EntityCollection<DatosTurnoPracticaLight> datosPTL =
                (from turno in dalEngine.Query<TurnoLight>()
                 join practicaTurno in dalEngine.Query<PracticaTurno>() on turno.Id equals practicaTurno.TurnoId
                 where turnosId.Contains(turno.Id) && (!soloPracticaPrincipal || practicaTurno.Tipo == (int)PracticaTurnoTipoEnum.Principal)
                 select CrearDatoTurnoPractica(turno, practicaTurno)).ToEntityCollection();

            EntityCollection<int> equiposIds = GetEquiposEnDPTL(datosPTL);
            EntityCollection<Equipo> datosEquipos = Context.Session.Dalc.GetManyByIds<Equipo>(equiposIds);
            MapearEquiposEnDatosTurnoPractica(datosEquipos, datosPTL);
            return datosPTL;
        }

        private EntityCollection<int> GetEquiposEnDPTL(EntityCollection<DatosTurnoPracticaLight> datosPTL)
        {
            EntityCollection<int> equiposIDs = new EntityCollection<int>();
            foreach (DatosTurnoPracticaLight dtpl in datosPTL)
                if (dtpl.EquipoId.HasValue)
                    equiposIDs.Add(dtpl.EquipoId.Value);
            return equiposIDs;
        }

        private void MapearEquiposEnDatosTurnoPractica(EntityCollection<Equipo> equipos, EntityCollection<DatosTurnoPracticaLight> dtpl)
        {
            foreach (DatosTurnoPracticaLight dtp in dtpl)
            {
                Equipo equipoInDTP = GetEquipo(equipos, dtp.EquipoId);
                if (equipoInDTP == null) continue;
                dtp.Centro = equipoInDTP.Sucursal.Name;
                dtp.CentroId = equipoInDTP.Sucursal.Id;
            }
        }

        private Equipo GetEquipo(EntityCollection<Equipo> equipos, int? equipoId)
        {
            if (!equipoId.HasValue)
                return null;
            foreach (Equipo equ in equipos)
                if (equ.Id == equipoId)
                    return equ;
            return null;
        }

        public EntityCollection<DatosTurnoPractica> DatosTurnoPracticaReadByTurnos(List<int> turnosId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DatosTurnoPractica(tur.Id, pt.Practica.Name, tur.Fecha, pt.Medico.Apellido, pt.Medico.Name ) " +
              "from Turno tur, PracticaTurno pt " +
              "where tur.Id = pt.TurnoId " +
              "and tur.Id IN (:turnosId) " +
              "AND pt.Tipo = :tipoPractica ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameterList("turnosId", turnosId);
            query.SetParameter("tipoPractica", (int)PracticaTurnoTipoEnum.Principal);

            return dalEngine.GetManyByQuery<DatosTurnoPractica>(query);

        }

        public DatosTurnoPractica DatosTurnoPracticaReadByTurno(int turnoId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DatosTurnoPractica(tur.Id, pt.Practica.Name, tur.Fecha, pt.Medico.Apellido, pt.Medico.Name ) " +
              "from Turno tur, PracticaTurno pt " +
              "where tur.Id = pt.TurnoId " +
              "and tur.Id = :turnoId " +
              "AND pt.Tipo = :tipoPractica ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetInt32("turnoId", turnoId);
            query.SetParameter("tipoPractica", (int)PracticaTurnoTipoEnum.Principal);

            return dalEngine.GetByQuery<DatosTurnoPractica>(query);
        }

        public EntityCollection<DatosTurnoPractica> DatosTurnoPracticaReadByFechaPlanCentroPractica(DateTime fechaDesde, DateTime fechaHasta, List<int> planesId, List<int> centrosId, List<int> practicasId, bool excluirModificadosFacturacion, bool excluirRecepcionados, bool excluirConCobranzaEnCaja, bool excluirPreFacturados, bool excluirAFacturar)
        {
            List<int> tiposControlFacturacion = new List<int>();
            tiposControlFacturacion.Add((int)TipoControlFacturacionEnum.PreFacturado);
            tiposControlFacturacion.Add((int)TipoControlFacturacionEnum.Facturado);

            List<int> estadosTurno = new List<int>();
            estadosTurno.Add((int)EstadoTurnoEnum.Cancelado);
            estadosTurno.Add((int)EstadoTurnoEnum.Ausente);

            if (excluirRecepcionados == true)
                estadosTurno.Add((int)EstadoTurnoEnum.Recepcionado);

            string hql = "SELECT new enfoke.Eges.Entities.Results.DatosTurnoPractica(tur.Id, tur.OrdenLight , proOrig, mov, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.ObraSocialPlan.Name, pt.Practica.Name, tur.Fecha, tur.Estado.Id, tur.TipoTurno, proOrig.Id , tur.Orden.Paciente.Nombre, tur.Orden.Paciente.Apellido  ) " +
              "from TurnoHQL tur left join tur.Original as turOrig left join turOrig.Orden.Protocolo as proOrig left join tur.MovimientoCaja mov, PracticaTurno pt " +
              "where tur.Id = pt.TurnoId " +
              "and tur.Fecha >= :fechaDesde " +
              "and tur.Fecha < :fechaHasta " +
              "and tur.Fecha < :fechaHasta " +
              "and tur.TipoTurno != :recitado " +
              "and tur.Presupuesto is null " + // No se pueden revalorizar turno con presupuesto
              "and tur.Equipo.Sucursal.Id in (:centrosId) " +
              "and (";

            for (int planes = 0; planes < planesId.Count; planes++)
                hql += " tur.Orden.ObraSocialPlan.Id = :planesId" + planes.ToString() + " OR";

            hql = hql.Substring(0, hql.Length - 2);
            hql += " ) and (";

            for (int practicas = 0; practicas < practicasId.Count; practicas++)
                hql += " pt.Practica.Id = :practicasId" + practicas.ToString() + " OR";

            hql = hql.Substring(0, hql.Length - 2);
            hql += " ) ";

            if (excluirModificadosFacturacion)
                hql += "and not exists (from ValorizacionItem vi where vi.Valorizacion.Turno.Id = tur.Id and vi.Valorizacion.Deleted = false and vi.Valorizacion.Tipo.Id = :ValorizacionTipoPrefacturacion and (vi.Modificado not is null or vi.Valorizacion.Modificado not is null)) ";

            if (excluirAFacturar == true)
                hql += "and tur.TipoControlFacturacion.Id != :tipoAFacturar ";

            if (excluirPreFacturados)
                hql += "and tur.TipoControlFacturacion.Id not in (:tiposControlFacturacion) ";

            if (excluirConCobranzaEnCaja)
                hql += "and not exists (from Formulario frm where frm.TurnoID = tur.Id and frm.Numero is not null and frm.FechaAnulacion is null and frm.TipoFormularioID != :TipoFormularioNotaCredito) ";

            hql += "and tur.Estado.Id not in (:estadosTurno) ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (excluirConCobranzaEnCaja)
                query.SetInt32("TipoFormularioNotaCredito", (int)TipoFormularioEnum.NotaCredito);

            if (excluirModificadosFacturacion)
                query.SetInt32("ValorizacionTipoPrefacturacion", (int)ValorizacionTiposEnum.Prefacturacion);

            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1));
            query.SetParameterList("centrosId", centrosId);
            query.SetParameter("recitado", (int)TipoTurnoEnum.Recitado);

            for (int practicas = 0; practicas < practicasId.Count; practicas++)
                query.SetParameter("practicasId" + practicas.ToString(), practicasId[practicas]);

            for (int planes = 0; planes < planesId.Count; planes++)
                query.SetParameter("planesId" + planes.ToString(), planesId[planes]);

            query.SetParameterList("estadosTurno", estadosTurno);

            if (excluirPreFacturados)
                query.SetParameterList("tiposControlFacturacion", tiposControlFacturacion);

            if (excluirAFacturar == true)
                query.SetParameter("tipoAFacturar", (int)TipoControlFacturacionEnum.AFacturar);

            return dalEngine.GetManyByQuery<DatosTurnoPractica>(query);
        }

        public Turno TurnoReadByTurnoInformeId(int turnoInformeId)
        {
            string hql = "SELECT  tur FROM TurnoInforme tui, Turno tur WHERE tui.TurnoID = tur.Id AND tui.Id = :turnoInformeId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoInformeId", turnoInformeId);

            return dalEngine.GetByQuery<Turno>(query);
        }

        /// <summary>
        /// Retorna un turno por su protocolo
        /// </summary>
        /// <param name="protocolo">Protocolo del turno buscado</param>
        /// <returns>El turno con el protocolo indicado</returns>
        private Turno TurnoReadByProtocolo(int protocoloID)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter { { Turno.Properties.Deleted, "=", 0 }, { BooleanOp.And, Turno.Properties.Orden.Protocolo, "=", protocoloID } };

            readCmd.Filter = filter;

            EntityCollection<Turno> turnos = readCmd.Execute();
            if (turnos.Count > 0)
                foreach (Turno tur in turnos)
                    if (tur.EstadoTurnoID != (int)EstadoTurnoEnum.Cancelado)
                        return tur;

            return turnos.Count > 0 ? turnos[0] : null;
        }

        /// <summary>
        /// Retorno si el Turno esta en algún Comprobante No Anulado
        /// </summary>
        /// <param name="turno">Turno buscado</param>
        /// <returns>True/False</returns>
        public bool TurnoExisteEnComprobanteNoAnulado(int turno)
        {
            const string hql = "SELECT  com " +
                               "from Comprobante com " +
                               ", ComprobanteItem coi " +
                               ", PracticaTurno ptu " +
                               "where coi.ComprobanteID = com.Id " +
                               "and ptu.Id = coi.PracticaTurnoID " +
                               "and ptu.TurnoId = :turno " +
                               "and com.FechaAnulacion is null ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("turno", turno);

            EntityCollection<Comprobante> comprobantes = dalEngine.GetManyByQuery<Comprobante>(query);

            if (comprobantes != null && comprobantes.Count > 0)
                return true;
            else
                return false;
        }

        /// <summary>
        /// Retorno la Cantidad de Turnos No Facturados correspondientes al Plan
        /// </summary>
        /// <param name="ospID">ID del ObraSocialPlan del PP</param>
        /// <param name="practicaID">ID de la práctica del PP</param>
        /// <param name="fechaDesde">Fecha Desde del PP</param>
        /// <param name="fechaHasta">Fecha Hasta del PP [Fecha Desde del Siguiente]</param>
        /// <returns>Cantidad de Turnos No Facturados correspondientes al Plan</returns>
        [Private]
        public int GetCantTurnosNoFacturados(int ospID, int? practicaID, DateTime? fechaDesde, DateTime? fechaHasta)
        {
            // Obtengo la Cantidad de Turnos No Facturados correspondientes al Plan
            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT COUNT(DISTINCT tur_id) ");
            sql.Append("FROM ( ");
            sql.Append("SELECT tur_id ");
            sql.Append("FROM turno ");
            sql.Append("INNER JOIN estado_turno ON tur_estado = est_id ");
            sql.Append("INNER JOIN orden ON tur_orden_id = ord_id ");
            sql.Append("INNER JOIN tipo_turno ON tur_tipo_turno_id = ttu_id ");
            // Del Plan en Cuestion
            sql.Append("WHERE ord_osp_id = ").Append(ospID).Append(" ");
            // Estados Atendidos
            sql.Append("AND est_atendido = 1 ");
            // Tipos de Turno Facturables
            sql.Append("AND ttu_es_facturable = 1 ");
            // De las fechas del Convenio
            if (fechaDesde.HasValue)
                sql.Append("AND tur_fecha >= " + SQLPortable.ToDbDate(fechaDesde.Value, true));
            if (fechaHasta.HasValue)
                sql.Append(" AND tur_fecha < " + SQLPortable.ToDbDate(fechaHasta.Value, true));
            sql.Append(") t ");
            sql.Append("INNER JOIN practica_turno ON prt_turno_id = tur_id ");
            sql.Append("LEFT JOIN comprobante_item ON coi_practica_turno_id = prt_id ");
            sql.Append("LEFT JOIN comprobante ON coi_comprobante_id = com_id ");
            sql.Append("LEFT JOIN factura ON fac_com_id = com_id and fac_fecha_anulacion is null");
            // Sin Comprobante o que el Comprobante no este Cancelado ni Facturado
            sql.Append(" WHERE (com_id IS NULL ");
            sql.Append("OR (com_fecha_anulacion IS NULL AND fac_id IS NULL)) ");
            // de la práctica en Cuestion
            if (practicaID.HasValue)
                sql.Append("AND prt_practica_id = ").Append(practicaID.Value).Append(" ");

            object res = dalEngine.Connection.ExecuteScalar(sql.ToString());
            if (res == null || res == DBNull.Value)
                return 0;
            else
                return int.Parse(res.ToString());
        }

        /// <summary>
        /// Pongo el Turno en Informe en Mesa 
        /// </summary>
        /// <param name="turno">Turno a Recibir</param>
        [Private]
        [RequiresTransaction]
        public virtual void TurnoRecibirEnMesa(Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Actualizo el Estado del Turno sin Transaccionar (la manejo aca)
            this.TurnoAvanzarEstado(turno, this.EstadoTurnoReadById((int)EstadoTurnoEnum.InformeEnMesa), modalidadCoseguro);

            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.InformeEnMesa, true);
        }

        /// <summary>
        /// Pongo el Turno como No Controlado, Vuelvo al Estado Anterior desde Facturacion
        /// </summary>
        /// <param name="turno">Turno a Revertir</param>
        [RequiresTransaction]
        public virtual Turno TurnoRevertirDesdeMesa(Turno turno)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;

            // Pongo el Turno como No Controlado o Particular (dependiendo de la obra social)
            if (ObrasSocialesDalc.ObraSocialEsParticularReadByObraSocialPlanId(turno.Orden.ObraSocialPlanId))
                turno.TipoControlFacturacion = FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.Particular);
            else
                turno.TipoControlFacturacion = FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.NoControlado);


            // Vuelvo al Estado Anterior desde Facturacion [Este guarda el Turno]
            turno = TurnoRevertirEstado(turno, CircuitoEnum.Facturacion);

            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.InformeEnMesa, false);

            return turno;
        }

        /// <summary>
        /// Pongo el Turno en Informe en Recepcion
        /// </summary>
        /// <param name="turno">Turno a Recibir</param>
        /// <param name="user">Usuario de la Operacion</param>
        [Private]
        [RequiresTransaction]
        public virtual void TurnoRecibirEnRecepcion(Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Actualizo el Estado del Turno sin Transaccionar (la manejo aca)
            this.TurnoAvanzarEstado(turno, this.EstadoTurnoReadById((int)EstadoTurnoEnum.InformeEnRecepcion), modalidadCoseguro);

            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.InformeEnRecepcion, true);
        }

        /// <summary>
        /// Vuelvo al Estado Anterior desde Admision
        /// </summary>
        /// <param name="turno">Turno a Revertir</param>
        /// <param name="user">Usuario de la Operacion</param>
        [RequiresTransaction]
        public virtual void TurnoRevertirDesdeRecepcion(Turno turno)
        {
            // Vuelvo al Estado Anterior desde Admision [Este guarda el Turno]
            turno = TurnoRevertirEstado(turno, CircuitoEnum.Admision);

            TurnoLogUpdate(turno.Id, TurnoLogFechasEnum.InformeEnRecepcion, false);
        }

        public EntityCollection<DatosTurnoPracticaLight> DatosTurnoPracticaLightReadByPacienteAndPracticas(int idPaciente, List<int> idsPracticasPrincipales, EstadoTurnoEnum estado)
        {
            return DatosTurnoPracticaLightReadByPacienteAndPracticas(idPaciente, idsPracticasPrincipales, estado, new List<int>());
        }

        public EntityCollection<DatosTurnoPracticaLight> DatosTurnoPracticaLightReadByPacienteAndPracticas(int idPaciente, List<int> idsPracticasPrincipales, EstadoTurnoEnum estado, List<int> idsTurnosReprogrmados)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DatosTurnoPracticaLight(t.Id, pt.Practica.Id, pt.Practica.Name, t.Fecha, t.Equipo.Sucursal.Id, t.Equipo.Sucursal.Name, t.Orden.Paciente.Id, t.Orden.Paciente.Nombre, t.Orden.Paciente.Apellido) " +
                         "FROM TurnoHQL t " +
                         "INNER JOIN t.PracticaTurno pt " +
                         "LEFT JOIN t.Original orig " +
                         "WHERE t.Id = pt.TurnoId " +
                         "AND t.Estado.Id = :idEstado " +
                         "AND pt.Tipo = :tipoPractica " +
                         "AND t.Orden.Paciente.Id = :idPaciente " +
                         "AND (orig is null or orig.Estado.Id = :idEstado) " +
                         "AND pt.Practica.Id IN (:idsPracticas) ";

            if (idsTurnosReprogrmados.Count > 0)
                hql += "AND t.Id NOT IN (:idsTurnosReprogrmados) ";

            hql += "ORDER BY t.Fecha desc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idEstado", (int)estado);
            query.SetParameter("tipoPractica", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameter("idPaciente", idPaciente);
            query.SetParameterList("idsPracticas", idsPracticasPrincipales.ToArray());
            if (idsTurnosReprogrmados.Count > 0)
                query.SetParameterList("idsTurnosReprogrmados", idsTurnosReprogrmados);

            return dalEngine.GetManyByQuery<DatosTurnoPracticaLight>(query);
        }


        // TurnoEstado
        /// <summary>
        /// Retorno todos los Turnos en un Estado en Particular
        /// </summary>
        /// <param name="estado">Estado de los Turnos a Buscar</param>
        /// <returns>Los Turnos en el Estado dado</returns>
        public EntityCollection<Turno> TurnoReadByEstado(int estado)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.Deleted, "=", 0);

            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "=", estado);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Retorno todos los Turnos Entre dos Estados
        /// </summary>
        /// <param name="estadoDesde">Estado Minimo de los Turnos a Buscar</param>
        /// <param name="estadoHasta">Estado Maximo de los Turnos a Buscar</param>
        /// <returns>Los Turnos entre los Estados dados</returns>
        public EntityCollection<Turno> TurnoReadByEstado(int estadoDesde, int estadoHasta)
        {
            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.Deleted, "=", 0);

            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, ">=", estadoDesde);

            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "<=", estadoHasta);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }


        // InformeEnMesaView
        /// <summary>
        /// Retorno todos los Informes en Mesa con los Filtros dados
        /// </summary>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <param name="fechaDesde">Filtro por Fecha del Turno</param>
        /// <param name="fechaHasta">Filtro por Fecha del Turno</param>
        /// <param name="entregaOrdenId">Filtro por EntregaOrdenId, si es -1 no filtra</param>
        /// <returns>Los Turnos en Estado Informe en Mesa que apliquen al filtro</returns>
        [MinuteTimeout]
        public virtual EntityCollection<InformeEnMesaView> InformeEnMesaViewRead(string txtBusqueda, InformeEnMesaSearchTypeEnum tipoBusqueda, DateTime fechaDesde, DateTime fechaHasta, int entregaOrdenId)
        {
            return InformeEnMesaViewRead(txtBusqueda, tipoBusqueda, fechaDesde, fechaHasta, entregaOrdenId, null);
        }

        /// <summary>
        /// Retorno todos los Informes en Mesa con los Filtros dados
        /// </summary>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <param name="fechaDesde">Filtro por Fecha del Turno</param>
        /// <param name="fechaHasta">Filtro por Fecha del Turno</param>
        /// <param name="entregaOrdenId">Filtro por EntregaOrdenId, si es -1 no filtra</param>
        /// <param name="idTurno">Filtro por id de turno, null no busca por ningun turno en particular.</param>
        /// <returns>Los Turnos en Estado Informe en Mesa que apliquen al filtro</returns>
        [MinuteTimeout]
        public virtual EntityCollection<InformeEnMesaView> InformeEnMesaViewRead(string txtBusqueda, InformeEnMesaSearchTypeEnum tipoBusqueda, DateTime fechaDesde, DateTime fechaHasta, int entregaOrdenId, int? idTurno)
        {
            string search = txtBusqueda.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<InformeEnMesaView> readCmd = new ReadManyCommand<InformeEnMesaView>(dalEngine);

            Filter filter = new Filter();

            if (!String.IsNullOrEmpty(txtBusqueda))
            {
                if (tipoBusqueda == InformeEnMesaSearchTypeEnum.Cualquiera)
                {
                    OpenParenthesis open = new OpenParenthesis();
                    filter.Add(open);
                    filter.Add(InformeEnMesaView.Properties.Protocolo, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnMesaView.Properties.Paciente, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnMesaView.Properties.Practica, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnMesaView.Properties.ObraSocial, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnMesaView.Properties.Informante, " LIKE ", search);
                    CloseParenthesis close = new CloseParenthesis();
                    filter.Add(close);
                }
                else
                {
                    switch (tipoBusqueda)
                    {
                        case InformeEnMesaSearchTypeEnum.Protocolo:
                            string nvoProtocolo = String.Concat("00000000000", txtBusqueda).Trim();
                            search = nvoProtocolo.Substring(nvoProtocolo.Length - 11, 3) + "-" + nvoProtocolo.Substring(nvoProtocolo.Length - 8);

                            filter.Add(InformeEnMesaView.Properties.Protocolo, " = ", search);
                            break;

                        case InformeEnMesaSearchTypeEnum.Paciente:
                            filter.Add(InformeEnMesaView.Properties.Paciente, " LIKE ", search);
                            break;

                        case InformeEnMesaSearchTypeEnum.Practica:
                            filter.Add(InformeEnMesaView.Properties.Practica, " LIKE ", search);
                            break;

                        case InformeEnMesaSearchTypeEnum.OS:
                            filter.Add(InformeEnMesaView.Properties.ObraSocial, " LIKE ", search);
                            break;

                        case InformeEnMesaSearchTypeEnum.Medico:
                            filter.Add(InformeEnMesaView.Properties.Informante, " LIKE ", search);
                            break;
                    }
                }
            }

            filter.Add(BooleanOp.And, InformeEnMesaView.Properties.FechaTurno, ">=", fechaDesde);

            filter.Add(BooleanOp.And, InformeEnMesaView.Properties.FechaTurno, "<", fechaHasta.AddDays(1));

            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);

            filter.Add(InformeEnMesaView.Properties.EstadoId, "=", (int)EstadoTurnoEnum.InformeEnMesa);

            filter.Add(BooleanOp.Or, InformeEnMesaView.Properties.EntregaOrdenId, "=", entregaOrdenId);

            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            if (idTurno.HasValue)
                filter.Add(BooleanOp.And, InformeEnMesaView.Properties.Id, "=", idTurno.Value);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(InformeEnMesaView.Properties.Fecha, SortingDirection.Asc);

            readCmd.Sort = sort;

            EntityCollection<InformeEnMesaView> informes = readCmd.Execute();
            for (int i = 0; i < informes.Count; i++)
                informes[i].Estado = EstadoTurnoReadById(informes[i].EstadoId);

            return informes;
        }


        // ProtocoloNoRecibidoView
        /// <summary>
        /// Retorno todos los Turnos con cierta Fecha de Vencimiento de Entrega del Informe y OS
        /// </summary>
        /// <param name="vencimiento">Fecha de Vencimiento de Entrega del Informe Desde</param>
        /// <param name="vencimiento">Fecha de Vencimiento de Entrega del Informe Hasta</param>
        /// <param name="vencimiento">Obra Social</param>
        /// <returns>Los Turnos con la Fecha</returns>
        public EntityCollection<ProtocoloNoRecibidoView> ProtocoloNoRecibidoReadByVencimientoAndOS(DateTime vtoDesde, DateTime vtoHasta, string oS, int? idCentro)
        {
            ReadManyCommand<ProtocoloNoRecibidoView> readCmd = new ReadManyCommand<ProtocoloNoRecibidoView>(dalEngine);

            Filter filter = new Filter();

            // se crea el filtro por la fecha, quitando la hora
            filter.Add(ProtocoloNoRecibidoView.Properties.FechaVencimientoInforme, ">=", vtoDesde.Date);

            filter.Add(BooleanOp.And, ProtocoloNoRecibidoView.Properties.FechaVencimientoInforme, "<", vtoHasta.Date.AddDays(1));

            // se filtra el nombre de la OS
            if (!string.IsNullOrEmpty(oS.Trim()))
                filter.Add(BooleanOp.And, ProtocoloNoRecibidoView.Properties.ObraSocial, "LIKE", '%' + oS.Trim() + '%');

            if (idCentro.HasValue)
                filter.Add(BooleanOp.And, ProtocoloNoRecibidoView.Properties.CentroId, "=", idCentro.Value);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(ProtocoloNoRecibidoView.Properties.Id, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }


        // InformeEnRecepcionView
        /// <summary>
        /// Retorno todos los Informes en Recepción con los Filtros dados
        /// </summary>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <param name="fechaDesde">Filtro Fecha Desde</param>
        /// <param name="fechaHasta">Filtro Fecha Hasta</param>
        /// <returns>Los Turnos en Estado Informe en Recepción que apliquen al filtro</returns>
        [MinuteTimeout]
        public virtual EntityCollection<InformeEnRecepcionView> InformeEnRecepcionViewRead(string txtBusqueda, InformeEnRecepcionSearchTypeEnum tipoBusqueda, DateTime fechaDesde, DateTime fechaHasta)
        {
            string search = txtBusqueda.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<InformeEnRecepcionView> readCmd = new ReadManyCommand<InformeEnRecepcionView>(dalEngine);

            Filter filter = new Filter();

            if (!String.IsNullOrEmpty(txtBusqueda))
            {
                if (tipoBusqueda == InformeEnRecepcionSearchTypeEnum.Cualquiera)
                {
                    OpenParenthesis open = new OpenParenthesis();
                    filter.Add(open);
                    filter.Add(InformeEnRecepcionView.Properties.Protocolo, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnRecepcionView.Properties.Paciente, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnRecepcionView.Properties.Practica, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnRecepcionView.Properties.ObraSocial, " LIKE ", search);
                    filter.Add(BooleanOp.Or, InformeEnRecepcionView.Properties.Informante, " LIKE ", search);
                    CloseParenthesis close = new CloseParenthesis();
                    filter.Add(close);
                }
                else
                {
                    switch (tipoBusqueda)
                    {
                        case InformeEnRecepcionSearchTypeEnum.Protocolo:
                            string nvoProtocolo = String.Concat("00000000000", txtBusqueda).Trim();
                            search = nvoProtocolo.Substring(nvoProtocolo.Length - 11, 3) + "-" + nvoProtocolo.Substring(nvoProtocolo.Length - 8);

                            filter.Add(InformeEnRecepcionView.Properties.Protocolo, " = ", search);
                            break;

                        case InformeEnRecepcionSearchTypeEnum.Paciente:
                            filter.Add(InformeEnRecepcionView.Properties.Paciente, " LIKE ", search);
                            break;

                        case InformeEnRecepcionSearchTypeEnum.Practica:
                            filter.Add(InformeEnRecepcionView.Properties.Practica, " LIKE ", search);
                            break;

                        case InformeEnRecepcionSearchTypeEnum.OS:
                            filter.Add(InformeEnRecepcionView.Properties.ObraSocial, " LIKE ", search);
                            break;

                        case InformeEnRecepcionSearchTypeEnum.Medico:
                            filter.Add(InformeEnRecepcionView.Properties.Informante, " LIKE ", search);
                            break;
                    }
                }
            }

            filter.Add(BooleanOp.And, InformeEnRecepcionView.Properties.FechaTurno, " >= ", fechaDesde);

            filter.Add(BooleanOp.And, InformeEnRecepcionView.Properties.FechaTurno, "<", fechaHasta.AddDays(1));

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(InformeEnRecepcionView.Properties.Fecha, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }


        // EstadoTurnoMotivo
        /// <summary>
        /// Retorno un EstadoTurnoMotivo para un ID
        /// </summary>
        /// <param name="id">ID del EstadoTurnoMotivo</param>
        /// <returns>El EstadoTurnoMotivo correspondiente</returns>
        public EstadoTurnoMotivo EstadoTurnoMotivoReadById(int id)
        {
            // Se toma la libetad de cachearse por thread...
            EstadoTurnoMotivo ret = EntityThreadCache<EstadoTurnoMotivo>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<EstadoTurnoMotivo>(id);
                if (ret != null)
                    EntityThreadCache<EstadoTurnoMotivo>.SetItem(id, ret);
            }
            return ret;
        }

        /// <summary>
        /// Retorno todos los EstadoTurnoMotivo
        /// </summary>
        /// <returns>Todos los EstadoTurnoMotivo</returns>
        public EntityCollection<EstadoTurnoMotivo> EstadoTurnoMotivoReadAll(bool soloManuales)
        {
            if (soloManuales)
                return dalEngine.GetManyByProperty<EstadoTurnoMotivo>(EstadoTurnoMotivo.Properties.Manual, true, EstadoTurnoMotivo.Properties.Name);

            return dalEngine.GetAll<EstadoTurnoMotivo>(EstadoTurnoMotivo.Properties.Name);
        }


        // TipoInhibicionEntrega



















        public EntityCollection<TipoInhibicionEntrega> TipoInhibicionEntregaByTurnos(List<int> turnosIDs)
        {
            string hql = "SELECT tie FROM Turno tur, TipoInhibicionEntrega tie WHERE tur.TipoInhibicionEntregaID = tie.Id AND tur.Id IN (:turnosIds)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("turnosIds", turnosIDs);
            return dalEngine.GetManyByQuery<TipoInhibicionEntrega>(query);
        }


        // DatoReporteCargaHoraria
        /// <summary>
        /// Obtengo los datos para el Reporte de Carga Horaria de Equipos
        /// </summary>
        /// <param name="dia">Dia del Reporte</param>
        /// <param name="equipo">Equipo (Opcional)</param>
        /// <returns></returns>
        public EntityCollection<DatosReporteCargaHorariaView> DatosReporteCargaHorariaRead(DateTime dia, int equipo, int centro)
        {
            TimeSpan longitud = new TimeSpan(0, 15, 0);

            TimePeriod periodo = new TimePeriod();
            TimePeriod periodoBusqueda = new TimePeriod();

            periodo.Length = longitud;
            periodoBusqueda.Length = new TimeSpan(24, 0, 0);

            periodo.StartDate = dia.Date;
            periodoBusqueda.StartDate = dia.Date;

            EntityCollection<DatosReporteCargaHorariaView> retorno = new EntityCollection<DatosReporteCargaHorariaView>();

            EntityCollection<DatosReporteCargaHorariaView> drchCollection = DatosReporteCargaHorariaRead(periodoBusqueda, equipo, centro);

            if (drchCollection != null && drchCollection.Count > 0)
            {
                while (periodo.StartDate.Day == dia.Day)
                {
                    foreach (DatosReporteCargaHorariaView item in drchCollection)
                    {
                        if (item.Fecha.HasValue)
                            if (periodo.Contains(item.Fecha.Value))
                            {
                                item.Hora = DateTimeUtils.FormatTime(periodo.StartDate);

                                retorno.Add(item);
                            }
                    }

                    periodo.StartDate = periodo.StartDate.Add(longitud);
                }
            }

            return retorno;
        }

        private EntityCollection<DatosReporteCargaHorariaView> DatosReporteCargaHorariaRead(TimePeriod dia, int equipo, int centro)
        {
            //IQuery query = dalEngine.CreateQuery("FROM DatosReporteCargaHorariaView drch WHERE drch.Fecha >= :StartDate AND drch.Fecha < :EndDate AND drch.EquipoId = :IdEquipo ");
            //query.SetParameter("StartDate", SQLPortable.ToDbDate(dia.StartDate, true));
            //query.SetParameter("EndDate", SQLPortable.ToDbDate(dia.EndDate, true));
            //query.SetParameter("IdEquipo", equipo);

            //return dalEngine.GetManyByQuery<DatosReporteCargaHorariaView>(query); 

            ReadManyCommand<DatosReporteCargaHorariaView> readCmd = new ReadManyCommand<DatosReporteCargaHorariaView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(DatosReporteCargaHorariaView.Properties.Fecha, " >= ", dia.StartDate);

            filter.Add(BooleanOp.And, DatosReporteCargaHorariaView.Properties.Fecha, " < ", dia.EndDate);

            if (equipo > 0)
                filter.Add(BooleanOp.And, DatosReporteCargaHorariaView.Properties.EquipoId, " = ", equipo);

            if (centro > 0)
                filter.Add(BooleanOp.And, DatosReporteCargaHorariaView.Properties.SucursalID, " = ", centro);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }


        // TurnoTecnico - GG
        /// <summary>
        /// [GG] Lo hago asi porque no pude mandar en el filter un IN con los tags de equipos que son String
        /// </summary>
        /// <param name="equipoTag"></param>
        /// <returns></returns>
        [AnonymousMethod()]
        [MinuteTimeout]
        public virtual EntityCollection<TurnoConsultorioSimpleView> TurnoTecnicoViewRead(List<string> equipoTag, bool consultorioValidaPacienteConDeuda)
        {
            DateTime fecha = enfoke.Time.Now;
            int[] estados = new int[]
                                {
                                    (int) EstadoTurnoEnum.InicioPractica,
                                    (int) EstadoTurnoEnum.FinPractica,
                                    (int) EstadoTurnoEnum.Recepcionado,
                                    (int) EstadoTurnoEnum.RecitadoCumplido,
                                    (int) EstadoTurnoEnum.EntregadoSinInforme
                                };

            int[] estadosExcluidos = new int[]
                                {
                                    (int) EstadoTurnoEnum.Cancelado,
                                };

            ReadManyCommand<TurnoConsultorioSimpleView> readCmd = new ReadManyCommand<TurnoConsultorioSimpleView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(TurnoConsultorioSimpleView.Properties.Fecha, ">=", fecha.Date.AddDays(-1));

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.Fecha, "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.EstadoId, "IN", estados);

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.EstadoId, "NOT IN", estadosExcluidos);

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.EquipoTag, "IN", equipoTag.ToArray());

            if (consultorioValidaPacienteConDeuda)
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));
                filter.Add(TurnoConsultorioSimpleView.Properties.RequiereCobranza, "=", false);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.CobranzaVigenteID, "IS NOT", null);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.PagoDiferidoClienteId, "IS NOT", null);
                filter.Add(new CloseParenthesis());
            }

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(TurnoConsultorioSimpleView.Properties.Fecha, SortingDirection.Asc);
            sort.Add(sortItem);

            readCmd.Filter = filter;
            readCmd.Sort = sort;

            return readCmd.Execute();
        }



        // TurnoConsultorioView
        /// <summary>
        /// Trae el listado de turnosIds para mostrar en consultorio
        /// </summary>
        /// <param name="fecha">La fecha a buscar</param>
        /// <param name="searchString">Busqueda</param>
        /// <param name="searchType">Tipo Busqueda</param>
        /// <param name="medicoId">id del Medico</param>
        /// <returns>listado de turnos</returns>
        [MinuteTimeout]
        public virtual EntityCollection<ITurnoConsultorio> TurnoConsultorioViewRead(DateTime fecha, string searchString, TurnoConsultorioSearchTypeEnum searchType, int medicoId, int centroId, int servicioId, Sector sector, bool soloGuardiaInternacion)
        {
            string search = searchString.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<TurnoConsultorioView> readCmd = new ReadManyCommand<TurnoConsultorioView>(dalEngine);

            readCmd.Filter = SetFilterTurnoConsultorioView(searchString, fecha, searchType, search, medicoId, centroId, servicioId, sector, soloGuardiaInternacion);
            ;
            readCmd.Sort = SetSortTurnoConsultorioView();

            EntityCollection<TurnoConsultorioView> turnos = readCmd.Execute();

            EntityCollection<TurnoConsultorioView> retorno = AgruparPracticas(turnos);

            EntityCollection<ITurnoConsultorio> col = new EntityCollection<ITurnoConsultorio>();
            foreach (TurnoConsultorioView t in retorno)
            {
                bool tratarGuardiaInternacion = (t.TipoTurnoId == (int)TipoTurnoEnum.GuardiaEspontaneo || t.TipoTurnoId == (int)TipoTurnoEnum.Guardia || t.InfoInternacionId.HasValue);
                t.Color = GetColorByEstadoAndTipoAutorizacion(t.EstadoId, t.TipoAutorizacionID, tratarGuardiaInternacion);
                col.Add(t);
            }

            return col;
        }

        private void Consultorio_BeforeDenyConfidential(object sender, BeforeDenyConfidentialArgs args)
        {
            if (args.ConfidentialEntity is ITurnoConsultorio)
            {
                ITurnoConsultorio filaDatos = (ITurnoConsultorio)args.ConfidentialEntity;

                if (Security.Current.UserInfo.User.IsMedico)
                {
                    int medicoActualId = Security.Current.UserInfo.User.Medico.Id;

                    if (filaDatos.MedicoId == medicoActualId ||
                             filaDatos.MedicoInformanteId == medicoActualId ||
                            (filaDatos.MedicoTecnicoId.HasValue && filaDatos.MedicoTecnicoId.Value == medicoActualId))
                        args.ShowConfidentialResult = true;
                }
            }
        }

        [MinuteTimeout]
        public virtual EntityCollection<ITurnoConsultorio> TurnoConsultorioSimpleViewRead(DateTime fecha, string searchString, TurnoConsultorioSearchTypeEnum searchType, int medicoId, int centroId, int servicioId, Sector sector, bool soloGuardiaInternacion)
        {
            enfoke.Context.Security.BeforeDenyConfidential = new BeforeDenyConfidentialHandler(Consultorio_BeforeDenyConfidential);

            string search = searchString.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<TurnoConsultorioSimpleView> readCmd = new ReadManyCommand<TurnoConsultorioSimpleView>(dalEngine);

            readCmd.Filter = SetFilterTurnoConsultorioSimpleView(searchString, fecha, searchType, search, medicoId, centroId, servicioId, sector, soloGuardiaInternacion);
            readCmd.Sort = SetSortTurnoConsultorioSimpleView();

            EntityCollection<TurnoConsultorioSimpleView> turnos = readCmd.Execute();
            EntityCollection<ITurnoConsultorio> col = new EntityCollection<ITurnoConsultorio>();
            foreach (TurnoConsultorioSimpleView t in turnos)
            {
                bool tratarGuardiaInternacion = (t.TipoTurnoId == (int)TipoTurnoEnum.GuardiaEspontaneo || t.TipoTurnoId == (int)TipoTurnoEnum.Guardia || t.InfoInternacionId.HasValue);
                t.Color = GetColorByEstadoAndTipoAutorizacion(t.EstadoId, t.TipoAutorizacionID, tratarGuardiaInternacion);
                col.Add(t);
            }
            return col;
        }

        private Sort SetSortTurnoConsultorioView()
        {
            Sort sort = new Sort();
            sort.Add(TurnoConsultorioView.Properties.Fecha, SortingDirection.Asc);
            sort.Add(TurnoConsultorioView.Properties.Id, SortingDirection.Asc);
            sort.Add(TurnoConsultorioView.Properties.PracticaTipo, SortingDirection.Asc);
            return sort;
        }

        private Sort SetSortTurnoConsultorioSimpleView()
        {
            Sort sort = new Sort();
            sort.Add(TurnoConsultorioSimpleView.Properties.Fecha, SortingDirection.Asc);
            sort.Add(TurnoConsultorioSimpleView.Properties.Id, SortingDirection.Asc);
            return sort;
        }

        private Filter SetFilterTurnoConsultorioView(string searchString, DateTime fecha, TurnoConsultorioSearchTypeEnum searchType, string search, int medicoId, int centroId, int servicioId, Sector sector, bool soloGuardiaInternacion)
        {
            Filter filter = new Filter();

            filter.Add(TurnoConsultorioView.Properties.Fecha, ">=", fecha.Date);

            filter.Add(BooleanOp.And, TurnoConsultorioView.Properties.Fecha, "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, TurnoConsultorioView.Properties.EstadoId, "<>", (int)EstadoTurnoEnum.Cancelado);


            ExistsFilterItem<TurnoConsultorioView> filterTurnoConsultorioViewJoin =
                new ExistsFilterItem<TurnoConsultorioView>(BooleanOp.And, TurnoConsultorioView.Properties.Id, TurnoConsultorioView.Properties.Id);

            if (!String.IsNullOrEmpty(searchString))
            {
                BooleanOp booleanOp = (searchType == TurnoConsultorioSearchTypeEnum.Cualquiera ? BooleanOp.Or : BooleanOp.And);
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filterTurnoConsultorioViewJoin.Add(open);

                if (searchType == TurnoConsultorioSearchTypeEnum.Medico || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.Medico, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Paciente || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.Paciente, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Servicio || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.Servicio, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Equipo || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.Equipo, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.MedicoInformante || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.MedicoInformante, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.MedicoTecnico || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.MedicoTecnico, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Centro || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filterTurnoConsultorioViewJoin.Add(booleanOp, TurnoConsultorioView.Properties.Centro, "LIKE ", search);

                CloseParenthesis close = new CloseParenthesis();
                filterTurnoConsultorioViewJoin.Add(close);
            }
            if (medicoId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filterTurnoConsultorioViewJoin.Add(open);
                filterTurnoConsultorioViewJoin.Add(TurnoConsultorioView.Properties.MedicoId, "=", medicoId);
                filterTurnoConsultorioViewJoin.Add(BooleanOp.Or, TurnoConsultorioView.Properties.MedicoTecnicoId, "=", medicoId);
                filterTurnoConsultorioViewJoin.Add(BooleanOp.Or, TurnoConsultorioView.Properties.MedicoInformanteId, "=", medicoId);
                CloseParenthesis close = new CloseParenthesis();
                filterTurnoConsultorioViewJoin.Add(close);
            }


            if (centroId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filterTurnoConsultorioViewJoin.Add(open);
                filterTurnoConsultorioViewJoin.Add(TurnoConsultorioView.Properties.CentroId, "=", centroId);
                CloseParenthesis close = new CloseParenthesis();
                filterTurnoConsultorioViewJoin.Add(close);
            }

            if (servicioId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filterTurnoConsultorioViewJoin.Add(open);
                filterTurnoConsultorioViewJoin.Add(TurnoConsultorioView.Properties.ServicioId, "=", servicioId);
                CloseParenthesis close = new CloseParenthesis();
                filterTurnoConsultorioViewJoin.Add(close);
            }

            if (soloGuardiaInternacion)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filterTurnoConsultorioViewJoin.Add(open);
                filterTurnoConsultorioViewJoin.Add(TurnoConsultorioView.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.Guardia);
                filterTurnoConsultorioViewJoin.Add(BooleanOp.Or, TurnoConsultorioView.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.GuardiaEspontaneo);
                filterTurnoConsultorioViewJoin.Add(BooleanOp.Or, TurnoConsultorioView.Properties.InfoInternacionId, "IS NOT", null);
                CloseParenthesis close = new CloseParenthesis();
                filterTurnoConsultorioViewJoin.Add(close);
            }

            filter.Add(filterTurnoConsultorioViewJoin);

            return filter;
        }

        private Filter SetFilterTurnoConsultorioSimpleView(string searchString, DateTime fecha, TurnoConsultorioSearchTypeEnum searchType, string search, int medicoId, int centroId, int servicioId, Sector sector, bool soloGuardiaInternacion)
        {
            Filter filter = new Filter();

            filter.Add(TurnoConsultorioSimpleView.Properties.Fecha, ">=", fecha.Date);

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.Fecha, "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, TurnoConsultorioSimpleView.Properties.EstadoId, "<>", (int)EstadoTurnoEnum.Cancelado);




            if (!String.IsNullOrEmpty(searchString) && searchString != "%%" && searchString != "%")
            {
                BooleanOp booleanOp = (searchType == TurnoConsultorioSearchTypeEnum.Cualquiera ? BooleanOp.Or : BooleanOp.And);
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);

                if (searchType == TurnoConsultorioSearchTypeEnum.Medico || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.Medico, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Paciente || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.Paciente, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Servicio || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.Servicio, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Equipo || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.Equipo, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.MedicoInformante || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.MedicoInformante, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.MedicoTecnico || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.MedicoTecnico, "LIKE ", search);

                if (searchType == TurnoConsultorioSearchTypeEnum.Centro || searchType == TurnoConsultorioSearchTypeEnum.Cualquiera)
                    filter.Add(booleanOp, TurnoConsultorioSimpleView.Properties.Centro, "LIKE ", search);

                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            if (medicoId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(TurnoConsultorioSimpleView.Properties.MedicoId, "=", medicoId);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.MedicoTecnicoId, "=", medicoId);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.MedicoInformanteId, "=", medicoId);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }


            if (servicioId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(TurnoConsultorioSimpleView.Properties.ServicioId, "=", servicioId);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            if (centroId > 0)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(TurnoConsultorioSimpleView.Properties.CentroId, "=", centroId);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            if (soloGuardiaInternacion)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(TurnoConsultorioSimpleView.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.Guardia);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.TipoTurnoId, "=", (int)TipoTurnoEnum.GuardiaEspontaneo);
                filter.Add(BooleanOp.Or, TurnoConsultorioSimpleView.Properties.InfoInternacionId, "IS NOT", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            return filter;
        }

        private EntityCollection<TurnoConsultorioView> AgruparPracticas(EntityCollection<TurnoConsultorioView> turnos)
        {
            // Predicado
            Predicate<TurnoConsultorioView> predicatePrincipal = compare => compare.PracticaTipo == (int)PracticaTurnoTipoEnum.Principal;


            EntityCollection<TurnoConsultorioView> principales = new EntityCollection<TurnoConsultorioView>();

            principales.AddRange(turnos.FindAll(predicatePrincipal));

            foreach (TurnoConsultorioView tcvPrincipal in principales)
            {
                // predicado
                Predicate<TurnoConsultorioView> predicateExposicionAdicional = delegate(TurnoConsultorioView compare)
                {
                    return compare.Id == tcvPrincipal.Id &&
                           compare.PracticaTipo != (int)PracticaTurnoTipoEnum.Principal;
                };


                foreach (TurnoConsultorioView tcvExposicionAdicional in turnos.FindAll(predicateExposicionAdicional))
                {
                    tcvPrincipal.Practica += "\n";
                    tcvPrincipal.Practica += tcvExposicionAdicional.Practica;
                }
            }

            return principales;
        }


        // Feriado









        /// <param name="fecha">Fecha del Feriado</param>
        /// <returns>El Feriado, si existe</returns>
        public Feriado FeriadoReadByFecha(DateTime fecha)
        {
            ReadManyCommand<Feriado> readCmd = new ReadManyCommand<Feriado>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, Feriado.Properties.Dia, "=", fecha.Date);

            readCmd.Filter = filter;

            EntityCollection<Feriado> feriados = readCmd.Execute();

            if (feriados.Count > 0)
                return feriados[0];
            else
                return null;
        }

        /// <summary>
        /// Obtengo los Feriados en un Período de Tiempo
        /// </summary>
        /// <param name="desde">Fecha desde de los Feriados</param>
        /// <returns>Colección de Feriados Posteriores a la Fecha</returns>
        public EntityCollection<Feriado> FeriadosReadByFecha(DateTime desde)
        {
            ReadManyCommand<Feriado> readCmd = new ReadManyCommand<Feriado>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, Feriado.Properties.Dia, ">=", desde.Date);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Feriados en un Período de Tiempo
        /// </summary>
        /// <param name="desde">Fecha desde de los Feriados</param>
        /// <param name="hasta">Fecha hasta de los Feriados</param>
        /// <returns>Colección de Feriados en el Período</returns>
        public EntityCollection<Feriado> FeriadosReadByFecha(DateTime desde, DateTime hasta)
        {
            ReadManyCommand<Feriado> readCmd = new ReadManyCommand<Feriado>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, Feriado.Properties.Dia, ">=", desde.Date);
            filter.Add(BooleanOp.And, Feriado.Properties.Dia, "<", hasta.Date.AddDays(1));

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public Feriado FeriadoReadByFechaAndSucursal(DateTime date, int sucId)
        {
            String hql = "Select fe from FeriadoSucursal fs join fs.Feriado fe where fe.Dia = :dia and fs.SucursalId = :sucId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetDateTime("dia", date.Date);
            query.SetInt32("sucId", sucId);
            return dalEngine.GetByQuery<Feriado>(query);
        }



        // Dias Laborales
        public DateTime SumarDiasLaborales(int? sucursalID, DateTime dia, int cantDias)
        {
            return SumarDiasLaborales(sucursalID, dia, cantDias, ComportamientoTipo.EntregaInformeDiasHabiles.Ninguno);
        }

        public DateTime SumarDiasLaborales(int? sucursalID, DateTime dia, int cantDias, ComportamientoTipo.EntregaInformeDiasHabiles habiles)
        {
            return SumarDiasLaborales(sucursalID, dia, cantDias, habiles, true);
        }

        private DateTime SumarDiasLaborales(int? sucursalID, DateTime dia, int cantDias, ComportamientoTipo.EntregaInformeDiasHabiles habiles, bool operacionSuma)
        {
            int valor = operacionSuma ? 1 : -1;

            Calendario calendario = CalendarioGetByFecha(dia, dia.AddDays(cantDias * 10 * valor));

            DateTime nuevoDia = dia;
            int cantSumados = 0;
            while (cantSumados < cantDias)
            {
                nuevoDia = nuevoDia.AddDays(valor);
                if (calendario.EsDiaLaboral(sucursalID, nuevoDia, habiles))
                    cantSumados++;
            }

            return nuevoDia;
        }


        // Log










        /// <summary>
        /// Retorno todas los eventos
        /// </summary>
        /// <returns>Todos los eventos</returns>
        public EntityCollection<LogEvento> LogEventoReadAll()
        {
            return dalEngine.GetAll<LogEvento>(LogEvento.Properties.Descripcion);
        }

        public EntityCollection<enfoke.Eges.Entities.LogDisplay> LogDisplayReadByTurno(int turnoID)
        {
            ReadManyCommand<enfoke.Eges.Entities.LogDisplay> readCmd = new ReadManyCommand<enfoke.Eges.Entities.LogDisplay>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, enfoke.Eges.Entities.LogDisplay.Properties.Turno.Id, "=", turnoID);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(enfoke.Eges.Entities.LogDisplay.Properties.FechaHora, SortingDirection.Asc);
            sort.Add(enfoke.Eges.Entities.LogDisplay.Properties.Id, SortingDirection.Asc);

            readCmd.Sort = sort;

            EntityCollection<enfoke.Eges.Entities.LogDisplay> logs = readCmd.Execute();

            return logs;
        }

        public EntityCollection<enfoke.Eges.Entities.Log> LogReadByTurnoIdAndEventoId(int turnoId, int eventoId)
        {
            SeguridadDalc SeguridadDalc = Context.Session.SeguridadDalc;

            string hql = "from Log as lg " +
                         "where lg.EventoID = :eventoId " +
                         "and lg.TurnoID = :turnoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoId", turnoId);
            query.SetParameter("eventoId", eventoId);
            EntityCollection<enfoke.Eges.Entities.Log> logs = dalEngine.GetManyByQuery<enfoke.Eges.Entities.Log>(query);
            // Cargo los objetos
            if (logs != null && logs.Count > 0)
            {
                foreach (enfoke.Eges.Entities.Log log in logs)
                {
                    log.Usuario = SeguridadDalc.SecurityUserReadById(log.UsuarioID.Value);
                    log.Evento = dalEngine.GetById<LogEvento>(log.EventoID);
                }
            }
            else
                return new EntityCollection<enfoke.Eges.Entities.Log>();

            return logs;
        }

        public EntityCollection<enfoke.Eges.Entities.LogDisplay> LogDisplayReadByTurnoIdAndEventoId(
            int turnoId, int eventoId)
        {
            ReadManyCommand<LogDisplay> readCmd = new ReadManyCommand<LogDisplay>(dalEngine);
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, LogDisplay.Properties.Turno.Id, "=", turnoId);
            filter.Add(BooleanOp.And, LogDisplay.Properties.Evento.Id, "=", eventoId);

            readCmd.Filter = filter;
            EntityCollection<LogDisplay> logDisplays = readCmd.Execute();

            return logDisplays;
        }

        [Private]
        public EntityCollection<LogDisplay> LogDisplayByFiltros(
            DateTime fechaDesde, DateTime fechaHasta, int? evento_id,
            int? usr_id, string protocolo, string paciente)
        {
            ReadManyCommand<LogDisplay> readCmd = new ReadManyCommand<LogDisplay>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, LogDisplay.Properties.FechaHora, ">=", fechaDesde);
            filter.Add(BooleanOp.And, LogDisplay.Properties.FechaHora, "<", fechaHasta.Date.AddDays(1));

            if (evento_id.HasValue)
                filter.Add(BooleanOp.And, LogDisplay.Properties.Evento.Id, "=", evento_id.Value);

            if (usr_id.HasValue)
                filter.Add(BooleanOp.And, LogDisplay.Properties.Usuario.Id, "=", usr_id.Value);

            if (!String.IsNullOrEmpty(protocolo))
                filter.Add(BooleanOp.And, LogDisplay.Properties.Turno.Orden.Protocolo.ProtocoloFull, "=", protocolo);

            if (!String.IsNullOrEmpty(paciente))
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));
                filter.Add(LogDisplay.Properties.Turno.Orden.Paciente.ApellidoNombre, "LIKE", paciente.Trim().Replace(" ", "%") + "%");
                filter.Add(BooleanOp.Or, LogDisplay.Properties.Turno.Orden.Paciente.Apellido, "LIKE", paciente.Trim().Replace(" ", "%") + "%");
                filter.Add(BooleanOp.Or, LogDisplay.Properties.Turno.Orden.Paciente.Nombre, "LIKE", paciente.Trim().Replace(" ", "%") + "%");
                filter.Add(new CloseParenthesis());
            }

            Sort sort = new Sort();
            sort.Add(LogDisplay.Properties.Id, SortingDirection.Asc);
            sort.Add(LogDisplay.Properties.FechaHora, SortingDirection.Asc);

            readCmd.Sort = sort;
            readCmd.Filter = filter;
            EntityCollection<LogDisplay> logDisplays = readCmd.Execute();

            return logDisplays;
        }

        /// <summary>
        /// Retorna los logsIds por filtro
        /// </summary>
        /// <param name="fechaDesde">fecha desde</param>
        /// <param name="fechaHasta">fecha hasta</param>
        /// <param name="protocolo">nro. protocolo</param>
        /// <param name="evento_id">evento</param>
        /// <param name="usr_id">usuario</param>
        /// <returns>Logs encontrados</returns>
        [Private]
        public EntityCollection<enfoke.Eges.Entities.Log> LogReadByFiltros(DateTime fechaDesde, DateTime fechaHasta, int? evento_id, int? usr_id, string protocolo, string paciente)
        {
            SeguridadDalc SeguridadDalc = Context.Session.SeguridadDalc;

            string hql = "SELECT l FROM Log l, Turno t ";

            if (!String.IsNullOrEmpty(paciente.Trim()))
                hql += ", Paciente p ";

            hql += "WHERE l.TurnoID = t.Id ";

            hql += "AND l.FechaHora >= :fechaDesde AND l.FechaHora < :fechaHasta ";

            if (evento_id.HasValue)
                hql += "AND l.EventoID = :evento ";

            if (usr_id.HasValue)
                hql += "AND l.UsuarioID = :usuario ";

            if (!String.IsNullOrEmpty(protocolo))
                hql += "AND t.Orden.Protocolo.ProtocoloFull = :protocolo ";

            if (!String.IsNullOrEmpty(paciente))
                hql += "AND t.Orden.PacienteId = p.Id AND p.ApellidoNombre LIKE :paciente ";

            hql += "ORDER BY l.FechaHora ASC, l.Id ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            if (evento_id.HasValue)
                query.SetParameter("evento", evento_id.Value);
            if (usr_id.HasValue)
                query.SetParameter("usuario", usr_id.Value);
            if (!String.IsNullOrEmpty(protocolo))
                query.SetString("protocolo", protocolo);
            if (!String.IsNullOrEmpty(paciente))
                query.SetString("paciente", paciente + "%");

            EntityCollection<enfoke.Eges.Entities.Log> logs = dalEngine.GetManyByQuery<enfoke.Eges.Entities.Log>(query);

            // Cargo los objetos
            foreach (enfoke.Eges.Entities.Log log in logs)
            {
                log.Usuario = SeguridadDalc.SecurityUserReadById(log.UsuarioID.Value);
                log.Evento = dalEngine.GetById<LogEvento>(log.EventoID);

                if (log.TurnoID.HasValue)
                    log.Turno = TurnoReadById(log.TurnoID.Value);
            }

            return logs;
        }

        /// <summary>
        /// Guardo un grupo de Logs
        /// </summary>

        [AnonymousMethod()]
        [RequiresTransaction]
        public virtual void LogRegistrar(List<LogBackground> logs)
        {
            foreach (LogBackground lb in logs)
            {
                List<int?> turnoIds = new List<int?>();
                turnoIds.Add(lb.TurnoID);

                Dictionary<int, string> descripciones = new Dictionary<int, string>();
                descripciones[0] = lb.Descricpcion;

                LogRegistrar(lb.EventoID, descripciones, turnoIds, lb.OrdenID, lb.Fecha);
            }
        }

        /// <summary>
        /// Registro de log
        /// </summary>
        /// <param name="eventoId">evento</param>
        /// <param name="descripcion">descripcion del log</param>
        /// <param name="userId">usuario de la accion</param>
        /// <param name="turnoId">turno</param>
        [AnonymousMethod()]
        public void LogRegistrar(int eventoId, string descripcion, int? turnoId)
        {
            List<int?> turnosIds = new List<int?>();
            turnosIds.Add(turnoId);

            Dictionary<int, string> logDescripcion = new Dictionary<int, string>();
            logDescripcion.Add(0, descripcion);



            LogRegistrar(eventoId, logDescripcion, turnosIds, null, null);
        }

        /// <summary>
        /// Registro de log
        /// </summary>
        /// <param name="eventoId">evento</param>
        /// <param name="descripcion">descripcion del log</param>
        /// <param name="userId">usuario de la accion</param>
        /// <param name="turnosId">turnos</param>
        [AnonymousMethod()]
        public void LogRegistrar(int eventoId, Dictionary<int, string> descripciones, List<int?> turnosId)
        {
            LogRegistrar(eventoId, descripciones, turnosId, null, null);
        }


        /// <summary>
        /// Registro de log
        /// </summary>
        /// <param name="eventoId">evento</param>
        /// <param name="descripcion">descripcion del log</param>
        /// <param name="userId">usuario de la accion</param>
        /// <param name="turnoId">turno</param>
        [AnonymousMethod()]
        public void LogRegistrar(LogEvento le, string descripcion, int? turnoId)
        {
            List<int?> turnoIds = new List<int?>();
            turnoIds.Add(turnoId);

            Dictionary<int, string> descripciones = new Dictionary<int, string>();
            descripciones[0] = descripcion;

            LogRegistrar(le, descripciones, turnoIds, null);
        }

        /// <summary>
        /// Registro de log
        /// </summary>
        /// <param name="eventoId">evento</param>
        /// <param name="descripcion">descripcion del log</param>
        /// <param name="userId">usuario de la accion</param>
        /// <param name="turnoId">turno</param>
        [AnonymousMethod()]
        public void LogRegistrar(int eventoId, string descripcion, OrdenUpdateDiagnostico orden)
        {
            Dictionary<int, string> descripciones = new Dictionary<int, string>();
            descripciones[0] = descripcion;

            LogRegistrar(eventoId, descripciones, null, orden.Id, null);
        }

        /// <summary>
        /// Registro de log
        /// </summary>
        /// <param name="eventoId">evento</param>
        /// <param name="descripcion">descripcion del log</param>
        /// <param name="userId">usuario de la accion</param>
        /// <param name="turnoId">turno</param>
        [AnonymousMethod()]
        public void LogRegistrar(int eventoId, string descripcion, Orden orden)
        {
            Dictionary<int, string> descripciones = new Dictionary<int, string>();
            descripciones[0] = descripcion;

            LogRegistrar(eventoId, descripciones, null, (orden != null) ? orden.Id : (int?)null, null);
        }

        [RequiresTransaction]
        [AnonymousMethod()]
        protected virtual void LogRegistrar(int eventoId, Dictionary<int, string> descripciones, List<int?> turnosId, int? ordenId, DateTime? fechaLog)
        {
            if (eventoId != (int)LogEventoEnum.SeguimientoHuerfanos && turnosId != null && turnosId.Count > 1)
                throw new Exception("Se trababa con muchos turnos solo para cuando no afecta a la orden porque solo se hizo para eso el multiturno.");

            List<int?> _turnosIds = new List<int?>();

            // Si el evento se registra a nivel de orden, loguea a todos los turnos
            if (eventoId == (int)LogEventoEnum.ModificacionDiagnosticoObservaciones
                || eventoId == (int)LogEventoEnum.EstudiosAnteriores
                || eventoId == (int)LogEventoEnum.FechaEmisionOrden
                || eventoId == (int)LogEventoEnum.InformeEnvioDomicilio
                || eventoId == (int)LogEventoEnum.EntregaOrden
                || eventoId == (int)LogEventoEnum.ModificacionDatosPaciente
                || eventoId == (int)LogEventoEnum.ConsultaDatosPaciente
                || eventoId == (int)LogEventoEnum.IngresaRevierteOrdenEnLoteFacturacion
            )
            {
                int? turnoId = turnosId != null ? turnosId[0] : null;
                string descripcion = descripciones[0];

                if (!ordenId.HasValue && !turnoId.HasValue && eventoId != (int)LogEventoEnum.ModificacionDatosPaciente && eventoId != (int)LogEventoEnum.ConsultaDatosPaciente)
                    throw new Exception("Se desea loguear un evento de Orden pero no se especifico el Id de la misma.");

                EntityCollection<Turno> turnos = null;
                if (ordenId.HasValue)
                    turnos = TurnosReadByOrdenId(ordenId.Value);
                else if (turnosId != null && turnosId.Count > 0 && turnosId[0].HasValue)
                {
                    Orden ord = OrdenReadByTurnoId(turnoId.Value);
                    turnos = TurnosReadByOrdenId(ord.Id);
                }

                if (turnos != null)
                {
                    foreach (Turno turno in turnos)
                    {
                        _turnosIds.Add(turno.Id);
                        descripciones[_turnosIds.Count - 1] = descripcion;
                    }

                }
            }
            else
                _turnosIds = turnosId;


            LogEvento le = GetLogEvento(eventoId);
            LogRegistrar(le, descripciones, _turnosIds, fechaLog);
        }

        [RequiresTransaction]
        [AnonymousMethod()]
        protected virtual void LogRegistrar(LogEvento le, Dictionary<int, string> descripciones, List<int?> turnosId, DateTime? fechaLog)
        {
            if (le.Habilitado)
            {
                EntityCollection<enfoke.Eges.Entities.Log> entidades = new EntityCollection<Entities.Log>();
                // Me puede venir las descripciones para los turnos
                if (turnosId != null)
                {
                    for (int i = 0; i < turnosId.Count; i++)
                        entidades.Add(LogCrearEntidad(le, descripciones[i], fechaLog, turnosId[i]));
                }
                else // O bien las descripciones ya separadas por turno
                {
                    foreach (KeyValuePair<int, string> turnoIdDescripcion in descripciones)
                        entidades.Add(LogCrearEntidad(le, turnoIdDescripcion.Value, fechaLog, turnoIdDescripcion.Key));
                }

                entidades = dalEngine.UpdateCollection<enfoke.Eges.Entities.Log>(entidades);
            }
        }

        private static Entities.Log LogCrearEntidad(LogEvento le, string descripcion, DateTime? fechaLog, int? turnoId)
        {
            // Ingreso el Log
            enfoke.Eges.Entities.Log log = new enfoke.Eges.Entities.Log();
            log.EventoID = le.Id;
            log.Descricpcion = descripcion;
            SecurityUser user = Security.Current.UserInfo.User;
            if (user != null)
                log.UsuarioID = user.Id;
            log.TurnoID = turnoId;
            log.FechaHora = fechaLog.HasValue ? fechaLog.Value : enfoke.Time.Now;
            return log;
        }

        public LogEvento GetLogEvento(int eventoId)
        {
            LogEvento ret = EntityThreadCache<LogEvento>.GetItem(eventoId);
            if (ret == null)
            {
                ret = dalEngine.GetById<LogEvento>(eventoId);
                if (ret != null)
                    EntityThreadCache<LogEvento>.SetItem(eventoId, ret);
            }
            return ret;
        }






        // TipoAutorizacion











        // TipoConfirmacion







        // Wizard
        /// <summary>
        /// Finalizo el Wizard sin Cobranza
        /// </summary>
        /// <param name="turno">Turno</param>
        /// <param name="user">Usuario de la Operación</param>
        /// <returns>El turno Modificado</returns>
        [Private]
        [RequiresTransaction]
        public virtual EntityCollection<Turno> FinalizarWizardOrdenMultipleAbierta(Orden orden, Turno turno, int idObraSocialPlanParticular, DateTime? fechaViejaPreescripcion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            FinalizarWizardInicioComunOrdenMultipleAbierta(orden, turno, idObraSocialPlanParticular, modalidadCoseguro);

            if (fechaViejaPreescripcion.HasValue)
                LogRegistrar((int)LogEventoEnum.FechaEmisionOrden, "Update Fecha Emisión de Orden. <" + fechaViejaPreescripcion.Value.ToString() + "> cambio por <" + orden.FechaEmisionOrden.Value.ToString() + ">.", turno.Id);

            return FinalizarWizardFinComunOrdenMultipleAbierta(orden, turno, modalidadCoseguro);
        }

        /// <summary>
        /// Finalizo el Wizard sin Cobranza
        /// </summary>
        /// <param name="turno">Turno</param>
        /// <param name="user">Usuario de la Operación</param>
        /// <returns>El turno Modificado</returns>
        [Private]
        public virtual EntityCollection<Turno> FinalizarWizard(Orden orden, EntityCollection<Turno> turnos, List<DatosCobranzaEnRecepcion> datosCobranza, int idObraSocialPlanParticular, DateTime? fechaViejaPreescripcion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro, bool crearFormulariosRM, decimal porcentajeIVA)
        {
            FinalizarWizardInicioComun(orden, turnos, idObraSocialPlanParticular, modalidadCoseguro);

            if (fechaViejaPreescripcion.HasValue)
                foreach (Turno turno in turnos)
                    LogRegistrar((int)LogEventoEnum.FechaEmisionOrden, "Update Fecha Emisión de Orden. <" + fechaViejaPreescripcion.Value.ToString() + "> cambio por <" + orden.FechaEmisionOrden.Value.ToString() + ">.", turno.Id);

            if (datosCobranza != null)
            {
                foreach (DatosCobranzaEnRecepcion cobranza in datosCobranza)
                {
                    foreach (Turno turno in turnos)
                    {
                        if (turno.Id == cobranza.TurnoId)
                        {
                            if (cobranza.ModalidadCobranza == DatosCobranzaEnRecepcion.ModalidadCobranzaEnum.Normal)
                                turno.CobranzaVigente = (Context.Session.CajaDalc).CobranzaCreate(turno, cobranza.Factura, cobranza.Recibos, cobranza.Pagos, cobranza.CajaUsuario, cobranza.CondicionIVA, porcentajeIVA, crearFormulariosRM, cobranza.FacturaNumeracion, modalidadCoseguro);
                            else
                                turno.CobranzaVigente = (Context.Session.CajaDalc).DepositoCreate(turno, cobranza.Factura, cobranza.Pagos, cobranza.CajaUsuario, (cobranza.ModalidadCobranza == DatosCobranzaEnRecepcion.ModalidadCobranzaEnum.AnticipadaTotal), cobranza.FacturaNumeracion);

                            turno.CobranzaVigenteID = turno.CobranzaVigente.Id;
                        }
                    }
                }
            }

            return FinalizarWizardFinComun(orden, turnos, modalidadCoseguro);
        }

        [RequiresTransaction]
        protected virtual bool DebeCrearProtocolo(Turno turno)
        {
            return (turno.Orden.Protocolo == null);
        }

        private void FinalizarWizardInicioComun(Orden orden, EntityCollection<Turno> turnos, int idObraSocialPlanParticular, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;



            foreach (Turno turno in turnos)
            {
                turno.Orden = orden;

                // Se asegura de tener acceso a los datos
                TurnoLogLockByTurno(turno.Id);

                // Si no tiene, obtengo el Protocolo
                if (orden.Protocolo == null || orden.Protocolo.Id <= 0)
                    orden.Protocolo = ProtocoloCreateNew(turno);

                // Guardo la Valorizacion
                ValorizacionesDalc.InsertValorizacion(turno.Valorizacion, turno, orden.ObraSocialPlan);
                if (turno.Valorizacion != null && turno.Valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Admision)
                    ValorizacionesDalc.ValorizacionGenerarPreFacturacion(turno, false);

                // Se encarga de asignarle al campo ImporteAPagar del turno lo que corresponde en base al tipo DebeOrdenMedica.
                // Se le pasa por parametro la obraSocialPlanParticulares por las dudas que el tipo DebeOrden lo necesite.
                turno.AsignarImportesAPagar(ObrasSocialesDalc.TipoPlanMayorIVAReadByObraSocialPlan(idObraSocialPlanParticular), modalidadCoseguro);
            }
        }

        private void FinalizarWizardInicioComunOrdenMultipleAbierta(Orden orden, Turno turno, int idObraSocialPlanParticular, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Asi no se pisa despues
            turno.Orden = orden;

            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            // Si no tiene, obtengo el Protocolo
            if (DebeCrearProtocolo(turno))
                orden.Protocolo = ProtocoloCreateNew(turno);

            // Se asegura de tener acceso a los datos
            TurnoLogLockByTurno(turno.Id);

            // Guardo la Valorizacion
            ValorizacionesDalc.InsertValorizacion(turno.Valorizacion, turno, turno.Orden.ObraSocialPlan);

            if (turno.Valorizacion != null && turno.Valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Admision)
                ValorizacionesDalc.ValorizacionGenerarPreFacturacion(turno, false);

            // Se encarga de asignarle al campo ImporteAPagar del turno lo que corresponde en base al tipo DebeOrdenMedica.
            // Se le pasa por parametro la obraSocialPlanParticulares por las dudas que el tipo DebeOrden lo necesite.
            turno.AsignarImportesAPagar(ObrasSocialesDalc.TipoPlanMayorIVAReadByObraSocialPlan(idObraSocialPlanParticular), modalidadCoseguro);
        }

        private EntityCollection<Turno> FinalizarWizardFinComun(Orden orden, EntityCollection<Turno> turnos, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<Turno> turnosResultado = new EntityCollection<Turno>();
            dalEngine.Update(orden);

            // Documentacion de la orden
            if (orden.OrdenDocumentacion != null && orden.OrdenDocumentacion.Count > 0)
                OrdenDocumentacionUpdateMany(orden.OrdenDocumentacion);

            foreach (Turno turno in turnos)
            {
                TurnoUpdateObservaciones tur = new TurnoUpdateObservaciones(turno.Id);
                tur.Observaciones = turno.Observaciones;
                TurnoUpdate(tur);

                OrdenUpdateDiagnostico ordenUpdateDiagnostico = OrdenUpdateDiagnosticoReadByTurnoId(turno.Id);
                ordenUpdateDiagnostico.Diagnostico = orden.Diagnostico;
                OrdenUpdate(ordenUpdateDiagnostico);

                RegistrarLogCambioDiagnosticoObservaciones(tur, ordenUpdateDiagnostico);

                LoguearSiCambioConceptoOrdenMedica(turno.Id, (int)orden.DebeOrdenMedica, true);

                if (turno.EstadoTurnoID != (int)EstadoTurnoEnum.Recepcionado)
                {
                    // Marco el Turno como Recepcionado
                    try
                    {
                        this.TurnoAvanzarEstado(turno, this.EstadoTurnoReadById((int)EstadoTurnoEnum.Recepcionado), modalidadCoseguro);
                        turno.EstadoTurnoID = (int)EstadoTurnoEnum.Recepcionado;

                        IncrementarIndicadoresPaciente(turno, true, false, false);

                    }
                    catch (StatusException ex)
                    {
                        throw ex;
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("No se puede cambiar de estado el turno.", ex);
                    }

                    // Creo los Informes
                    GuardarInformesTurno(turno);
                }

                turnosResultado.Add(TurnoUpdateAndRefresh(turno));

                AsociarPracticaTurno(turno);
            }

            return turnosResultado;
        }

        private void AsociarPracticaTurno(Turno turno)
        {
            EntityCollection<PracticaTurno> practicaTurnos = this.PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Todas);
            GuardarPracticaTurnoIdEnPracticaValidacion(turno.Id, practicaTurnos);
            GuardarPracticaTurnoEnTurnoDocumentacion(turno, practicaTurnos);
        }

        private void GuardarPracticaTurnoEnTurnoDocumentacion(Turno turno, EntityCollection<PracticaTurno> practicaTurnos)
        {
            // Guardo la documentación
            if (turno.TurnoDocumentacion != null && turno.TurnoDocumentacion.Count > 0)
            {
                for (int i = turno.TurnoDocumentacion.Count - 1; i >= 0; i--)
                {
                    TurnoDocumentacion doc = turno.TurnoDocumentacion[i];
                    PracticaTurno praTurno = PracticaTurnoCorrespondienteDocumentacion(practicaTurnos, doc);
                    if (praTurno != null)
                        doc.PracticaTurno = praTurno;
                    else
                        turno.TurnoDocumentacion.RemoveAt(i);


                }
                TurnoDocumentacionUpdateMany(turno.TurnoDocumentacion);
            }
        }

        private static PracticaTurno PracticaTurnoCorrespondienteDocumentacion(EntityCollection<PracticaTurno> practicaTurnos, TurnoDocumentacion doc)
        {
            Predicate<PracticaTurno> predicate =
            delegate(PracticaTurno compare)
            {
                return compare.Practica.Id == doc.PracticaTurno.Practica.Id &&
                    (compare.PracticaAdicional == null ? 0 : compare.PracticaAdicional.Id) ==
                    (doc.PracticaTurno.PracticaAdicional == null ? 0 : doc.PracticaTurno.PracticaAdicional.Id);
            }
                ;
            PracticaTurno praTurno = practicaTurnos.Find(predicate);
            return praTurno;
        }

        /// <summary>
        /// Si en la valorizacion del wizars se agregan practicas que luego se registran, esta funcion guarda el PracticaTurnoId generado al salir del Wizard en la PracticaValidadion
        /// </summary>
        /// <param name="turnoId"></param>
        private void GuardarPracticaTurnoIdEnPracticaValidacion(int turnoId, EntityCollection<PracticaTurno> practicaTurnos)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;
            ValidadoresDalc ValidadoresDalc = Context.Session.ValidadoresDalc;

            Turno tur = TurnoReadById(turnoId);
            if (tur.UltimoLoteValidacion.HasValue)
            {
                EntityCollection<PracticaValidacion> ptCol = ValidadoresDalc.PracticaValidacionReadByLoteValidacionId(tur.UltimoLoteValidacion.Value);

                foreach (PracticaValidacion practicaValidacion in ptCol)
                {
                    if (practicaValidacion.PracticaTurnoId == 0)
                    {
                        if (practicaTurnos == null)
                            practicaTurnos = this.PracticaTurnoReadByTurno(turnoId, PracticaTurnoTipoEnum.Todas);

                        foreach (PracticaTurno pt in practicaTurnos)
                        {
                            int? practicaAdicionalId = pt.PracticaAdicional != null ? (int?)pt.PracticaAdicional.Id : null;

                            PlanPracticaPrecio pp = ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaEquipoAndAdicional(tur.Orden.ObraSocialPlanId, pt.Practica.Id, practicaAdicionalId, null, true, enfoke.Time.Now);
                            if (pp == null)
                                continue;

                            if (practicaValidacion.CodigoPractica == pp.CodigoInterno)
                            {
                                practicaValidacion.PracticaTurnoId = pt.Id;
                                dalEngine.Update(practicaValidacion);
                                break;
                            }
                        }
                    }
                }

            }
        }

        [RequiresTransaction]
        protected virtual EntityCollection<Turno> FinalizarWizardFinComunOrdenMultipleAbierta(Orden orden, Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<Turno> turnosResultado = new EntityCollection<Turno>();
            dalEngine.Update(orden);
            TurnoUpdateObservaciones tur = new TurnoUpdateObservaciones(turno.Id);
            tur.Observaciones = turno.Observaciones;
            TurnoUpdate(tur);

            OrdenUpdateDiagnostico ordenUpdateDiagnostico = new OrdenUpdateDiagnostico(orden.Id);
            ordenUpdateDiagnostico.Diagnostico = orden.Diagnostico;
            RegistrarLogCambioDiagnosticoObservaciones(tur, ordenUpdateDiagnostico);

            LoguearSiCambioConceptoOrdenMedica(turno.Id, (int)orden.DebeOrdenMedica, true);

            if (turno.EstadoTurnoID != (int)EstadoTurnoEnum.Recepcionado)
            {
                // Marco el Turno como Recepcionado
                try
                {
                    this.TurnoAvanzarEstado(turno, this.EstadoTurnoReadById((int)EstadoTurnoEnum.Recepcionado), modalidadCoseguro);

                    turno.EstadoTurnoID = (int)EstadoTurnoEnum.Recepcionado;

                    IncrementarIndicadoresPaciente(turno, true, false, false);

                }
                catch (StatusException ex)
                {
                    throw ex;
                }
                catch (Exception ex)
                {
                    throw new Exception("No se puede cambiar de estado el turno.", ex);
                }

                GuardarInformesTurno(turno);

                // Guardo la documentación
                if (turno.TurnoDocumentacion != null && turno.TurnoDocumentacion.Count > 0)
                    TurnoDocumentacionUpdateMany(turno.TurnoDocumentacion);

                turnosResultado.Add(TurnoUpdateAndRefresh(turno));
            }

            if (orden.OrdenDocumentacion != null && orden.OrdenDocumentacion.Count > 0)
                OrdenDocumentacionUpdateMany(orden.OrdenDocumentacion);

            return turnosResultado;
        }

        public void IncrementarIndicadoresPaciente(Turno turno, bool turnosRealizados, bool turnosAusentes, bool turnosCancelados)
        {
            if (turno.Orden.Paciente == null)
                turno.Orden.Paciente = PacienteReadById(turno.Orden.PacienteId);

            if (turnosRealizados)
                turno.Orden.Paciente.IncrementarTurnosRealizados();

            if (turnosAusentes)
                turno.Orden.Paciente.IncrementarTurnosAusentes();

            if (turnosCancelados)
                turno.Orden.Paciente.IncrementarTurnosCancelados();

            PacienteUpdate(turno.Orden.Paciente, false, turno.Orden.Id, null, turnosRealizados, turnosAusentes, turnosCancelados, false);
        }

        private bool RegistrarLogCambioDiagnostico(OrdenUpdateDiagnostico orden)
        {
            Orden ordenDB = dalEngine.GetById<Orden>(orden.Id);

            if (!String.IsNullOrEmpty(ordenDB.Diagnostico))
                ordenDB.Diagnostico = ordenDB.Diagnostico.Trim();
            if (!String.IsNullOrEmpty(orden.Diagnostico))
                orden.Diagnostico = orden.Diagnostico.Trim();

            bool hayCambio = false;
            if (!string.IsNullOrEmpty(ordenDB.Diagnostico) || !string.IsNullOrEmpty(orden.Diagnostico))
                hayCambio = ordenDB.Diagnostico != orden.Diagnostico;

            if (hayCambio)
                LogRegistrar((int)LogEventoEnum.ModificacionDiagnosticoObservaciones, "Se modificó el diagnostico.", orden);

            return hayCambio;
        }

        private bool RegistrarLogCambioObservaciones(TurnoUpdateObservaciones turno)
        {
            Turno turnoDB = TurnoReadById(turno.Id);

            if (!String.IsNullOrEmpty(turnoDB.Observaciones))
                turnoDB.Observaciones = turnoDB.Observaciones.Trim();
            if (!String.IsNullOrEmpty(turno.Observaciones))
                turno.Observaciones = turno.Observaciones.Trim();

            bool hayCambio = false;
            if (!string.IsNullOrEmpty(turnoDB.Observaciones) || !string.IsNullOrEmpty(turno.Observaciones))
                hayCambio = turnoDB.Observaciones != turno.Observaciones;

            if (hayCambio)
                LogRegistrar((int)LogEventoEnum.ModificacionDiagnosticoObservaciones, "Se modificó las observaciones.", turno.Id);

            return hayCambio;
        }

        private bool RegistrarLogCambioDiagnosticoObservaciones(TurnoUpdateObservaciones turno, OrdenUpdateDiagnostico orden)
        {
            TurnoUpdateObservaciones turnoDBObs = dalEngine.GetById<TurnoUpdateObservaciones>(turno.Id);
            OrdenUpdateDiagnostico ordenDBDIag = OrdenUpdateDiagnosticoReadById(orden.Id);

            if (!String.IsNullOrEmpty(ordenDBDIag.Diagnostico))
                ordenDBDIag.Diagnostico = ordenDBDIag.Diagnostico.Trim();
            if (!String.IsNullOrEmpty(turnoDBObs.Observaciones))
                turnoDBObs.Observaciones = turnoDBObs.Observaciones.Trim();
            if (!String.IsNullOrEmpty(orden.Diagnostico))
                orden.Diagnostico = orden.Diagnostico.Trim();
            if (!String.IsNullOrEmpty(turno.Observaciones))
                turno.Observaciones = turno.Observaciones.Trim();

            bool hayCambio = false;
            if (ordenDBDIag.Diagnostico != orden.Diagnostico)
            {
                hayCambio = true;
                LogRegistrar((int)LogEventoEnum.ModificacionDiagnosticoObservaciones, "Se modificó el diagnostico.", orden);
            }
            if (turnoDBObs.Observaciones != turno.Observaciones)
            {
                hayCambio = true;
                LogRegistrar((int)LogEventoEnum.ModificacionDiagnosticoObservaciones, "Se modificaron las observaciones.", turno.Id);
            }

            return hayCambio;
        }

        private void GuardarInformesTurno(Turno turno)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;

            EntityCollection<TurnoInforme> informes = new EntityCollection<TurnoInforme>();
            if (turno.Informes.Count < 1)
                informes = CrearInformesTurno(turno, true);
            else
                informes = turno.Informes;


            informes = dalEngine.UpdateCollection<TurnoInforme>(informes);

            foreach (TurnoInforme informe in informes)
            {
                // Creo el Log del Informe
                TurnoInformeLog til = new TurnoInformeLog(informe.Id);


                til = dalEngine.Update<TurnoInformeLog>(til);

                // Grabo el Historico de Estados
                InformesDalc.EstadoInformeHistoricoCreate(false, informe);

                // [GG] Si el informante en practica turno es diferente al de turno informe los igualo.
                if (informe.RegionInforme != null)
                {
                    EntityCollection<PracticaTurno> pts = PracticaTurnoReadByTurnoAndRegion(turno.Id, PracticaTurnoTipoEnum.Todas, informe.RegionInforme.Id);
                    EntityCollection<PracticaTurnoForUpdateMedicoInformante> ptForUpdate = PracticaTurnoForUpdateMedicoInformanteReadByIds(pts.GetIds());
                    foreach (PracticaTurnoForUpdateMedicoInformante pt in ptForUpdate)
                    {
                        if (informe.Informante != null)
                        {
                            pt.MedicoInformante = informe.Informante.Medico;
                            dalEngine.Update(pt);
                        }
                    }
                }

            }
        }






        /// <summary>
        /// Creo los informes en Memoria
        /// </summary>
        /// <param name="turno">Turno</param>
        /// <returns>Colección de Informes</returns>
        /// <remarks>
        /// Método utilizado para la reasignación.
        /// Necesitaba los informes pero el turno reservado aun no los tiene.
        /// Por eso los creo en memoria para utilizarlos en el método de reasignación.
        /// </remarks>
        [Private]
        public EntityCollection<TurnoInforme> CrearInformesTurno(Turno turno)
        {
            return CrearInformesTurno(turno, false);
        }

        [RequiresTransaction]
        public virtual EntityCollection<TurnoInforme> CrearInformesTurno(Turno turno, bool actualizarPracticaTurno)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;
            SeguridadDalc SeguridadDalc = Context.Session.SeguridadDalc;

            // Obtengo las PracticaTurno
            EntityCollection<PracticaTurno> PTs = PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Todas);

            // Tomo las practicasTurnos que tenga asociada la valorizacion por si en el wizard de admision 
            // se agregaron o quitaron (seteando la cantidad) practicas.
            // Porque en este momento, no están reflejados en la base los cambios hechos en el wizard 
            if (turno.Valorizacion != null && turno.Valorizacion.Valorizacion != null)
                PTs = turno.Valorizacion.GetPracticasTurnoCCMC();

            CanalEntregaInforme canalNormal = dalEngine.GetById<CanalEntregaInforme>((int)CanalEntregaInformeEnum.EntregaCanalNormal);
            EstadoInforme estadoPendiente = InformesDalc.EstadoInformeReadById((int)EstadoInformeEnum.Pendiente);
            TipoEntregaInforme tipoNoEntregado = dalEngine.GetById<TipoEntregaInforme>((int)TipoEntregaInformeEnum.NoEntregado);

            EntityCollection<TurnoInforme> informes = new EntityCollection<TurnoInforme>();

            // Me quedo con la fecha maxima para actualizar a nivel turno
            int diasInforme = 0;

            // Creo el/los informes relacionados al turno
            if (PTs[0].Practica.ServicioEspecialidad.Servicio.InformantePorTurno)
            {
                diasInforme = GetDiasInforme(PTs, diasInforme);

                // Busco la PracticaPrincipal
                Predicate<PracticaTurno> predicate = delegate(PracticaTurno compare)
                {
                    return compare.Tipo == (int)PracticaTurnoTipoEnum.Principal;
                };

                PracticaTurno ptp = PTs.Find(predicate);

                if (ptp == null)
                {
                    throw new NotLoggeableException("No se encuenta la práctica principal");
                }

                SecurityUser informante = SeguridadDalc.SecurityUserReadByMedico(ptp.MedicoInformante.Id);

                // Determino cual es la fecha de entrega del informe contemplando dias segun Centro y Centro/Servicio
                turno.FechaEntregaInforme = DeterminarFechaEntregaInforme(turno, diasInforme);

                informes.Add(new TurnoInforme(canalNormal, estadoPendiente, tipoNoEntregado, turno.FechaEntregaInforme.Value, informante, turno.Id, true));
            }
            else
            {
                // Dias para los Informes y Actualizacion de PTs
                // Obtengo los Dias para el Informe de cada Región
                Hashtable diasInformePorRegion = new Hashtable();

                EntityCollection<PracticaTurno> PTsActualizar = new EntityCollection<PracticaTurno>();

                foreach (PracticaTurno pt in PTs)
                {
                    if (pt.Practica.InformeRequerido)
                    {
                        if (pt.Practica.RegionInformeId.HasValue)
                        {
                            diasInforme = GetDiasInforme(diasInforme, pt, diasInformePorRegion);

                            pt.RegionInformeID = pt.Practica.RegionInformeId;

                            // Si corresponde, actualizo las PTs
                            if (actualizarPracticaTurno)
                                PTsActualizar.Add(pt);
                        }
                        else
                            throw new NotLoggeableException("La práctica " + pt.Practica.Name + " no tiene una región de informe definida por lo cual no resulta posible crear el informe.");
                    }
                }

                // Si corresponde, actualizo las PTs
                if (PTsActualizar.Count > 0)
                {
                    PTsActualizar = dalEngine.UpdateCollection<PracticaTurno>(PTsActualizar);
                }


                // Calculo la Fecha de Entrega para el Turno
                DateTime fecha = DateTime.MinValue;

                // Creo los informes
                List<int> regiones = new List<int>();
                foreach (PracticaTurno pt in PTs)
                {
                    if (pt.Practica.InformeRequerido)
                    {
                        TurnoInforme informe = CrearInformeTurno(regiones, pt.Practica.RegionInformeId, turno, diasInformePorRegion, canalNormal, estadoPendiente, tipoNoEntregado, pt.MedicoInformante.Id);
                        if (informe != null)
                        {
                            informes.Add(informe);

                            // Me quedo con la Mayor fecha de Entrega de los Informes
                            if (informe.FechaEntrega.Value > fecha)
                                fecha = informe.FechaEntrega.Value;
                        }
                    }
                }

                if (fecha == DateTime.MinValue)
                    fecha = DeterminarFechaEntregaInforme(turno, diasInforme);
                turno.FechaEntregaInforme = fecha;
            }

            return informes;
        }

        [RequiresTransaction]
        protected virtual TurnoInforme CrearInformeTurno(List<int> regiones, int? regionID, Turno turno, Hashtable diasInforme, CanalEntregaInforme canalNormal, EstadoInforme estadoPendiente, TipoEntregaInforme tipoNoEntregado, int medicoID)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;
            SeguridadDalc SeguridadDalc = Context.Session.SeguridadDalc;

            // Creo el informe si la práctica tiene región y la misma aún no fue agregada al turno
            if (regionID.HasValue && !regiones.Contains(regionID.Value))
            {
                RegionInforme region = dalEngine.GetById<RegionInforme>(regionID.Value);

                // Guardo la región dentro de las ya generadas
                regiones.Add(regionID.Value);

                DateTime fecha = DeterminarFechaEntregaInforme(turno, regionID.Value, (int)diasInforme[regionID.Value]);
                SecurityUser informante = SeguridadDalc.SecurityUserReadByMedico(medicoID);

                return new TurnoInforme(region, canalNormal, estadoPendiente, tipoNoEntregado, fecha, informante, turno.Id, false);
            }
            else
                return null;
        }

        public void TurnoInformeUpdateFechaEntrega(int turnId, DateTime nuevaFecha)
        {
            Turno turno = TurnoReadById(turnId);
            turno.Fecha = nuevaFecha;
            TurnoInformeUpdateFechaEntrega(turno);
        }

        public void TurnoInformeUpdateFechaEntrega(int turnId)
        {
            Turno turno = TurnoReadById(turnId);
            TurnoInformeUpdateFechaEntrega(turno);
        }

        public void TurnoInformeUpdateFechaEntrega(Turno turno)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;

            EntityCollection<PracticaTurno> PTs = PracticaTurnoReadByTurno(turno.TurnoOriginalID.GetValueOrDefault(turno.Id), PracticaTurnoTipoEnum.Todas);
            EntityCollection<TurnoInforme> TIs = InformesDalc.TurnoInformeReadByTurno(turno.TurnoOriginalID.GetValueOrDefault(turno.Id));
            EntityCollection<TurnoInforme> TIsUpdate = new EntityCollection<TurnoInforme>();

            if (TIs != null)
            {
                if (PTs[0].Practica.ServicioEspecialidad.Servicio.InformantePorTurno)
                {
                    int diasEntrega = 0;
                    foreach (TurnoInforme ti in TIs)
                    {
                        diasEntrega = GetDiasInforme(PTs, diasEntrega);
                        ti.FechaEntrega = DeterminarFechaEntregaInforme(turno, diasEntrega);
                        TIsUpdate.Add(ti);
                    }
                }
                else
                {
                    Hashtable diasInformePorRegion = new Hashtable();
                    int diasInforme = GetDiasInforme(diasInformePorRegion, PTs);

                    foreach (TurnoInforme ti in TIs)
                    {
                        if (ti.RegionInforme != null && diasInformePorRegion[ti.RegionInforme.Id] != null)
                        {
                            ti.FechaEntrega = DeterminarFechaEntregaInforme(turno, ti.RegionInforme.Id, (int)diasInformePorRegion[ti.RegionInforme.Id]);
                            TIsUpdate.Add(ti);
                        }
                    }
                }
            }

            if (TIsUpdate != null)
                dalEngine.UpdateCollection(TIsUpdate);
        }

        private int GetDiasInforme(EntityCollection<PracticaTurno> PTs, int diasInforme)
        {
            // Me quedo con la máxima cantidad de dias
            foreach (PracticaTurno pt in PTs)
                if (pt.Practica.InformeRequerido && pt.Practica.DiasInforme > diasInforme)
                    diasInforme = pt.Practica.DiasInforme;

            return diasInforme;
        }

        private int GetDiasInforme(Hashtable diasInformePorRegion, EntityCollection<PracticaTurno> PTs)
        {
            int diasInforme = 0;
            foreach (PracticaTurno pt in PTs)
                if (pt.Practica.RegionInformeId.HasValue)
                    diasInforme = GetDiasInforme(diasInforme, pt, diasInformePorRegion);

            return diasInforme;
        }

        private int GetDiasInforme(int diasInforme, PracticaTurno pt, Hashtable diasInformePorRegion)
        {
            bool pisar = true;
            if (diasInformePorRegion[pt.Practica.RegionInformeId] != null)
                pisar = (pt.Practica.DiasInforme > (int)diasInformePorRegion[pt.Practica.RegionInformeId]);

            if (pisar)
                diasInformePorRegion[pt.Practica.RegionInformeId] = pt.Practica.DiasInforme;

            // Me quedo con la máxima cantidad de dias
            if ((int)diasInformePorRegion[pt.Practica.RegionInformeId] > diasInforme)
                diasInforme = (int)diasInformePorRegion[pt.Practica.RegionInformeId];

            return diasInforme;
        }

        [Private]
        public DateTime DeterminarFechaEntregaInforme(int turnoID, DateTime fechaTurno, int diasInforme)
        {
            Turno turno = TurnoReadById(turnoID);

            return DeterminarFechaEntregaInforme(turno, fechaTurno, diasInforme);
        }

        public DateTime DeterminarFechaEntregaInforme(Turno turno)
        {
            IQueryable<Practica> queryPractica = from prt in dalEngine.Query<PracticaTurno>()
                                                 where prt.TurnoId == turno.Id
                                                 select prt.Practica;
            Practica practica = queryPractica.First();
            return DeterminarFechaEntregaInforme(turno, practica.DiasInforme);
        }


        public DateTime DeterminarFechaEntregaInforme(Turno turno, int diasInforme)
        {
            return DeterminarFechaEntregaInforme(turno, turno.Fecha.Value, diasInforme);
        }

        public DateTime DeterminarFechaEntregaInforme(Turno turno, int? regionID, int diasInforme)
        {
            return DeterminarFechaEntregaInforme(turno, regionID, turno.Fecha.Value, diasInforme);
        }

        public DateTime DeterminarFechaEntregaInforme(Turno turno, DateTime fechaBase, int diasInforme)
        {
            return DeterminarFechaEntregaInforme(turno, null, fechaBase, diasInforme);
        }

        private DateTime DeterminarFechaEntregaInforme(Turno turno, int? regionID, DateTime fechaBase, int diasInforme)
        {
            // Necesito el Equipo para aplicar Comportamientos
            if (turno.Equipo == null && turno.EquipoId.HasValue)
            {
                EquiposDalc EquiposDalc = Context.Session.EquiposDalc;
                turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);
            }

            // Obtengo las Practicas de la Region para aplicar Comportamientos
            List<int> practicas = Practica.GetIDsFromCollection(PracticaTurnoReadByTurnoAndRegion(turno.Id, PracticaTurnoTipoEnum.Todas, regionID));

            int? osID = turno.Orden != null && turno.Orden.ObraSocialPlan != null ? turno.Orden.ObraSocialPlan.ObraSocial.Id : (int?)null;

            return DeterminarFechaEntregaInforme(fechaBase, turno.Fecha, diasInforme, practicas, turno.Equipo, osID);
        }

        public DateTime DeterminarFechaEntregaInforme(DateTime fechaTurno, int diasInforme, List<int> practicas, Equipo equipo, int? osID)
        {
            return DeterminarFechaEntregaInforme(fechaTurno, fechaTurno, diasInforme, practicas, equipo, osID);
        }

        private DateTime DeterminarFechaEntregaInforme(DateTime fechaBase, DateTime? fechaTurno, int diasInforme, List<int> practicas, Equipo equipo, int? osID)
        {
            ComportamientosDalc comportamientosDalc = Context.Session.ComportamientosDalc;

            int? servicioID = equipo != null ? equipo.Servicio.Id : (int?)null;
            int? sucursalID = equipo != null ? equipo.Sucursal.Id : (int?)null;

            // Obtengo los Comportamientos Habilitados
            EntityCollection<Comportamiento> comportamientos = comportamientosDalc.ComportamientosHabilitadosReadByTipoWithContents(ComportamientoTipo.TiposEnum.EntregaInformes);

            // Obtengo la configuracion de Dias Habiles y Horas a Sumar
            ComportamientoTipo.EntregaInformeDiasHabiles habiles = ComportamientoTipo.EntregaInformeDiasHabiles.Ninguno;
            int diasComportamientos = 0;
            foreach (Comportamiento comportamiento in comportamientos)
            {
                habiles |= (ComportamientoTipo.EntregaInformeDiasHabiles)comportamiento.AplicarAEntregaInforme(ComportamientoTipo.EtapaEntregaInforme.ParametrizacionDiasHabiles, fechaBase, fechaTurno, practicas, servicioID, osID, sucursalID);
                diasComportamientos += (int)comportamiento.AplicarAEntregaInforme(ComportamientoTipo.EtapaEntregaInforme.ModificacionDiasEntrega, fechaBase, fechaTurno, practicas, servicioID, osID, sucursalID);
            }

            // Sumo la cantidad de Dias Habiles segun el Nomenclador y los Comportamientos
            DateTime fecha = SumarDiasLaborales(sucursalID, fechaBase, diasInforme + diasComportamientos, habiles);

            // Actualizo la Hora de Entrega del Informe segun los Comportamientos
            foreach (Comportamiento comportamiento in comportamientos)
                fecha = (DateTime)comportamiento.AplicarAEntregaInforme(ComportamientoTipo.EtapaEntregaInforme.ModificacionHoraEntrega, fecha, fechaTurno, practicas, servicioID, osID, sucursalID);

            return fecha;
        }


        // TreeViewTurno
        /// <summary>
        /// Obtengo los Datos para el Arbol de un Turno
        /// </summary>
        /// <param name="turno">ID del Turno</param>
        /// <returns>Datos para el Arbol</returns>
        public TreeViewTurno TreeViewTurnoRead(int turno)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;

            TreeViewTurno tvt = dalEngine.GetById<TreeViewTurno>(turno);

            // Traigo las Adicionales y las Subsiguientes
            if (tvt != null)
            {
                int turnoUtilizar = tvt.TipoTurnoID == (int)TipoTurnoEnum.Recitado ? tvt.TurnoOriginalID.Value : tvt.Id;

                tvt.Adicionales = PracticaTurnoReadByTurno(turnoUtilizar, PracticaTurnoTipoEnum.Adicional);
                tvt.Subsiguientes = PracticaTurnoReadByTurno(turnoUtilizar, PracticaTurnoTipoEnum.Exposicion);
                tvt.Informes = InformesDalc.TurnoInformeReadByTurno(turnoUtilizar);

                // Chequeo si tiene planillas, para poner la fecha de envio
                EntityCollection<AutorizacionPlanilla> planillas = AutorizacionPlanillaReadByTurno(turnoUtilizar);
                tvt.FechaEnvioPlanillas = null;
                foreach (AutorizacionPlanilla planilla in planillas)
                {
                    foreach (AutorizacionPlanillaItem item in planilla.Items)
                    {
                        if (item.FechaEnvio.HasValue)
                        {
                            tvt.FechaEnvioPlanillas = item.FechaEnvio;
                            break;
                        }
                    }

                    if (tvt.FechaEnvioPlanillas.HasValue)
                        break;
                }

            }

            return tvt;

            //return null;
        }


        // Topes










        /// <summary>
        /// Retorno todos los Topes
        /// </summary>
        /// <returns>Todos los Tope</returns>
        public EntityCollection<Tope> TopeReadAll()
        {
            ReadManyCommand<Tope> readCmd = new ReadManyCommand<Tope>(dalEngine);

            Sort sort = new Sort();
            sort.Add(Tope.Properties.ObraSocialID, SortingDirection.Asc);
            sort.Add(Tope.Properties.ServicioID, SortingDirection.Asc);
            sort.Add(Tope.Properties.MedicoID, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Busco Topes Vigentes por OS, Servicio y Medico
        /// </summary>
        /// <param name="soloVigentes">Marca si traigo solo los Vigentes o no</param>
        /// <param name="os">Id de la Obra Social</param>
        /// <param name="servicioId">Id del Servicio</param>
        /// <param name="medicoId">Id del Médico</param>
        /// <returns>Colección de Topes Vigentes</returns>
        [Private]
        public EntityCollection<Tope> TopeReadByObraSocialServicioAndMedico(bool soloVigentes, int? os, int? servicio, int? medico)
        {
            return TopeReadByObraSocialServicioEquipoMedicoAndFecha(soloVigentes, os, servicio, false, null, false, medico, false, null, null);
        }

        /// <summary>
        /// Busco Topes Vigentes por OS, Servicio, Medico y Fechas
        /// </summary>
        /// <param name="soloVigentes">Marca si traigo solo los Vigentes o no</param>
        /// <param name="osId">Id de la Obra Social</param>
        /// <param name="servicioId">Id del Servicio</param>
        /// <param name="sinServicio">Si esta en true, ademas de buscar segun el ID del servicioId, busca sin servicioId</param>
        /// <param name="equipoId">Id del Equipo</param>
        /// <param name="sinEquipo">Si el Id del Equipo es null y la marca esta en True, busca con Equipo null</param>
        /// <param name="userId">Id del Médico</param>
        /// <param name="sinMedico">Si el Id del Medico es null y la marca esta en True, busca con Medico null</param>
        /// <param name="fechaDesde">Fecha Desde para ver si hay Topes</param>
        /// <param name="fechaHasta">Fecha Hasta para ver si hay Topes</param>
        /// <returns>Colección de Topes Vigentes</returns>
        [Private]
        public EntityCollection<Tope> TopeReadByObraSocialServicioEquipoMedicoAndFecha(bool soloVigentes, int? osId, int? servicioId, bool sinServicio, int? equipoId, bool sinEquipo, int? medicoId, bool sinMedico, DateTime? fechaDesde, DateTime? fechaHasta)
        {
            ReadManyCommand<Tope> readCmd = new ReadManyCommand<Tope>(dalEngine);

            Filter filter = new Filter();
            filter.Add(BooleanOp.And, Tope.Properties.Vigente, "=", 1);

            if (osId.HasValue)
                filter.Add(BooleanOp.And, Tope.Properties.ObraSocialID, "=", osId.Value);

            AddFilterForPropertyByIdOrAll(filter, Tope.Properties.ServicioID, servicioId, sinServicio);
            AddFilterForPropertyByIdOrAll(filter, Tope.Properties.EquipoID, equipoId, sinEquipo);
            AddFilterForPropertyByIdOrAll(filter, Tope.Properties.MedicoID, medicoId, sinMedico);

            readCmd.Filter = filter;

            EntityCollection<Tope> topes = null;

            if (fechaDesde.HasValue && fechaHasta.HasValue)
            {
                topes = new EntityCollection<Tope>();
                EntityCollection<Tope> topesSinFiltrar = readCmd.Execute();

                foreach (Tope tope in topesSinFiltrar)
                {
                    if (tope.Horario == null || tope.Horario.Id != tope.HorarioID)
                        tope.Horario = dalEngine.GetById<Horario>(tope.HorarioID);

                    DateTime fd = fechaDesde.Value.Date;
                    DateTime fh = fechaHasta.Value.Date;

                    if ((tope.FechaInicio >= fd && tope.FechaInicio <= fh)
                        || (tope.FechaFin >= fd && tope.FechaFin <= fh)
                        || (fd >= tope.FechaInicio && fd <= tope.FechaFin)
                        || (fh >= tope.FechaInicio && fh <= tope.FechaFin))
                        topes.Add(tope);
                }
            }
            else
                topes = readCmd.Execute();

            if (soloVigentes)
            {
                EntityCollection<Tope> topesVigentes = new EntityCollection<Tope>();

                foreach (Tope tope in topes)
                {
                    if (tope.Horario == null || tope.Horario.Id != tope.HorarioID)
                        tope.Horario = dalEngine.GetById<Horario>(tope.HorarioID);

                    if (tope.FechaFin >= enfoke.Time.Today)
                        topesVigentes.Add(tope);
                }

                return topesVigentes;
            }
            else
                return topes;
        }

        private static void AddFilterForPropertyByIdOrAll(Filter filter, IPropertyReference property, int? id, bool nullValue)
        {
            if (id.HasValue || nullValue)
            {
                if (id.HasValue && nullValue)
                {
                    filter.Add(new OpenParenthesis(BooleanOp.And));
                    filter.Add(property, "=", id.Value);
                    filter.Add(BooleanOp.Or, property, " IS ", null);
                    filter.Add(new CloseParenthesis());
                }
                else if (id.HasValue)
                    filter.Add(BooleanOp.And, property, "=", id.Value);
                else
                    filter.Add(BooleanOp.And, property, " IS ", null);
            }
        }

        /// <summary>
        /// Busco los Topes para la OS
        /// </summary>
        /// <param name="os">Id de la Obra Social</param>
        /// <returns>Cantidad de Topes de la OS</returns>
        [Private]
        public bool ObraSocialHasTopes(int os)
        {
            Filter filter = new Filter();

            filter.Add(Tope.Properties.Vigente, "=", 1);
            filter.Add(BooleanOp.And, Tope.Properties.ObraSocialID, "=", os);

            return dalEngine.GetManyByFilter<Tope>(filter, 1).Count > 0;
        }











        /// <summary>
        /// Retorno todos los TopeDetalle de un Tope
        /// </summary>
        /// <param name="tope">Id del Tope</param>
        /// <returns>Todos los TopeDetalle de un Tope</returns>
        public EntityCollection<TopeDetalle> TopeDetalleReadByTope(int tope)
        {
            return dalEngine.GetManyByProperty<TopeDetalle>(TopeDetalle.Properties.Tope.Id, tope, TopeDetalle.Properties.Fecha);
        }

        [RequiresTransaction]
        public virtual void TopesUpdate(DateTime fecha, int os, int servicio, int equipo, int medico, int practicas, int monto)
        {
            ReadManyCommand<Tope> readCmd = new ReadManyCommand<Tope>(dalEngine);

            Filter filter = new Filter();

            // Topes de la OS
            filter.Add(Tope.Properties.ObraSocialID, "=", os);

            // Del Servicio dado o Todos
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(Tope.Properties.ServicioID, "=", servicio);
            filter.Add(BooleanOp.Or, Tope.Properties.ServicioID, " IS ", null);
            filter.Add(new CloseParenthesis());

            // Del Equipo dado o Todos
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(Tope.Properties.EquipoID, "=", equipo);
            filter.Add(BooleanOp.Or, Tope.Properties.EquipoID, " IS ", null);
            filter.Add(new CloseParenthesis());

            // Del Medico dado o Todos
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(Tope.Properties.MedicoID, "=", medico);
            filter.Add(BooleanOp.Or, Tope.Properties.MedicoID, " IS ", null);
            filter.Add(new CloseParenthesis());

            filter.Add(BooleanOp.And, Tope.Properties.Vigente, "=", 1);

            readCmd.Filter = filter;
            EntityCollection<Tope> topes = readCmd.Execute();

            foreach (Tope tope in topes)
            {
                if (tope.Horario == null || tope.Horario.Id != tope.HorarioID)
                    tope.Horario = dalEngine.GetById<Horario>(tope.HorarioID);

                DateTime fechaTurno = fecha.Date;
                DateTime fechaDesde = tope.FechaInicio.Date;
                DateTime fechaHasta = tope.FechaFin.Date;

                DateTime horaTurno = enfoke.Time.Today.Date.Add(fecha.TimeOfDay);
                DateTime horaDesde = enfoke.Time.Today.Date.Add(tope.HoraInicio);
                DateTime horaHasta = enfoke.Time.Today.Date.Add(tope.HoraFin);

                // Chequeo si el Turno cae dentro del Rango
                if (
                    ((((int)tope.Dias) & ((int)DiaDeSemanaConverter.FromDayOfWeek(fechaTurno.DayOfWeek))) > 0)
                    && (fechaTurno >= fechaDesde && fechaTurno <= fechaHasta)
                    && (horaTurno >= horaDesde && horaTurno <= horaHasta)
                    )
                {
                    if (tope.TipoAplicacion == Tope.TipoAplicacionEnum.PeriodoCompleto)
                    {
                        TopeUpdate tu = dalEngine.GetById<TopeUpdate>(tope.Id);
                        switch (tope.TipoTopeID)
                        {
                            case (int)TipoTopeEnum.CantPacientes:
                                tu.Utilizado = tope.Utilizado + Math.Sign(practicas);
                                break;
                            case (int)TipoTopeEnum.CantPracticas:
                                tu.Utilizado = tope.Utilizado + practicas;
                                break;
                            case (int)TipoTopeEnum.Monto:
                                tu.Utilizado = tope.Utilizado + monto;
                                break;
                        }


                        tu = dalEngine.Update<TopeUpdate>(tu);
                    }
                    else
                    {
                        EntityCollection<TopeDetalle> detalles = TopeDetalleReadByTope(tope.Id);

                        foreach (TopeDetalle td in detalles)
                        {
                            // Asigno el tope para que el Detalle tenga el Horario
                            td.Tope = tope;

                            // Chequeo si el Turno actual corresponde a algun Detalle
                            bool aplica = false;
                            switch (tope.TipoAplicacion)
                            {
                                case Tope.TipoAplicacionEnum.PorDia:
                                    aplica = td.Fecha.Date == fecha.Date;
                                    break;
                                case Tope.TipoAplicacionEnum.PorMes:
                                    aplica = td.Fecha.Month == fecha.Month;
                                    break;
                            }

                            if (aplica)
                            {
                                TopeDetalleUpdate(tope, practicas, monto, td);
                            }
                        }
                    }
                }
            }
        }


        protected void TopeDetalleUpdate(Tope tope, int practicas, int monto, TopeDetalle td)
        {
            TopeDetalleUpdate tdu = dalEngine.GetById<TopeDetalleUpdate>(td.Id);
            switch (tope.TipoTopeID)
            {
                case (int)TipoTopeEnum.CantPacientes:
                    tdu.Utilizado = td.Utilizado + Math.Sign(practicas);
                    break;
                case (int)TipoTopeEnum.CantPracticas:
                    tdu.Utilizado = td.Utilizado + practicas;
                    break;
                case (int)TipoTopeEnum.Monto:
                    tdu.Utilizado = td.Utilizado + monto;
                    break;
            }


            tdu = dalEngine.Update<TopeDetalleUpdate>(tdu);
        }

        /// <summary>
        /// Creo un nuevo Tope de Prácticas
        /// </summary>
        /// <param name="tope">Tope a Crear</param>
        /// <param name="user">Usuario de la Operacion</param>
        /// <returns>El Tope creado</returns>
        [RequiresTransaction]
        public virtual Tope TopeInsert(Tope tope, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Inserto el Horario
            tope.Horario = HorarioInsert(tope.Horario);
            tope.HorarioID = tope.Horario.Id;

            // Seteo datos de inserción
            tope.Vigente = true;
            tope.Utilizado = 0;

            // Busco Practicas Existenes segun si es Periodo Completo o si es Por Dia (Inserto Dias)
            EntityCollection<Turno> turnos = TraeTurnosParaTope(tope, tope.FechaInicio, tope.FechaFin);
            if (tope.TipoAplicacion == Tope.TipoAplicacionEnum.PeriodoCompleto)
                tope.Utilizado = TopeCalculaUtilizado(tope, turnos, modalidadCoseguro);


            // Inserto el Tope
            tope = dalEngine.Update<Tope>(tope);

            // Agrega detalle si es necesario
            if (tope.TipoAplicacion != Tope.TipoAplicacionEnum.PeriodoCompleto)
            {
                // Inserto los Detalles
                DateTime fecha = tope.FechaInicio.Date;
                while (fecha <= tope.FechaFin.Date)
                {
                    if (tope.TipoAplicacion == Tope.TipoAplicacionEnum.PorDia)
                    {
                        // Creo el detalle solo si corresponde el dia de la semana
                        if ((((int)tope.Dias) & ((int)DiaDeSemanaConverter.FromDayOfWeek(fecha.DayOfWeek))) > 0)
                        {
                            EntityCollection<Turno> turnosDelDia = FiltrarTurnosDelDia(turnos, fecha);
                            int utilizado = TopeCalculaUtilizado(tope, turnosDelDia, modalidadCoseguro);

                            CreateTopeDetalle(tope, fecha, utilizado);
                        }

                        // Avanzo 1 dia
                        fecha = fecha.AddDays(1);
                    }
                    else if (tope.TipoAplicacion == Tope.TipoAplicacionEnum.PorMes)
                    {
                        // Creo el detalle para el 1ro del mes
                        fecha = new DateTime(fecha.Year, fecha.Month, 1);

                        EntityCollection<Turno> turnosDelMes = FiltrarTurnosDelMes(turnos, fecha);
                        int utilizado = TopeCalculaUtilizado(tope, turnosDelMes, modalidadCoseguro);

                        CreateTopeDetalle(tope, fecha, utilizado);

                        // Avanzo 1 mes
                        fecha = fecha.AddMonths(1);
                    }
                }
            }

            return tope;
        }

        protected virtual void CreateTopeDetalle(Tope tope, DateTime fecha, int utilizado)
        {
            TopeDetalle td = new TopeDetalle();
            td.Tope = tope;
            td.Fecha = fecha;
            td.Inicial = tope.Inicial;
            td.Utilizado = utilizado;

            // Inserto el detalle
            td = dalEngine.Update<TopeDetalle>(td);
        }

        private EntityCollection<Turno> FiltrarTurnosDelDia(EntityCollection<Turno> turnos, DateTime dia)
        {
            EntityCollection<Turno> resultado = new EntityCollection<Turno>();

            // Si están en rango, los agrega...
            foreach (Turno turno in turnos)
                if (turno.Fecha >= dia.Date && turno.Fecha < dia.Date.AddDays(1))
                    resultado.Add(turno);

            return resultado;
        }

        private EntityCollection<Turno> FiltrarTurnosDelMes(EntityCollection<Turno> turnos, DateTime dia)
        {
            EntityCollection<Turno> resultado = new EntityCollection<Turno>();

            // Si es del mes, lo agrega...
            foreach (Turno turno in turnos)
                if (turno.Fecha.Value.Month == dia.Month)
                    resultado.Add(turno);

            return resultado;
        }

        private int TopeCalculaUtilizado(Tope tope, EntityCollection<Turno> turnos, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            int utilizado = 0;
            switch (tope.TipoTopeID)
            {
                case (int)TipoTopeEnum.CantPacientes:
                    // Cuento Turnos
                    utilizado = turnos.Count;
                    break;
                case (int)TipoTopeEnum.CantPracticas:
                    // Sumo Cantidad de Practicas de los Turnos
                    utilizado = ObtenerCantidadPracticas(turnos);
                    break;
                case (int)TipoTopeEnum.Monto:
                    // Obtengo el monto de las prácticas de cada turno
                    foreach (Turno turno in turnos)
                        utilizado += ValorizacionesDalc.ObtenerTotalOSValorizacionPresupuesto(turno, modalidadCoseguro);
                    break;
            }

            return utilizado;
        }

        private int ObtenerCantidadPracticas(EntityCollection<Turno> turnos)
        {
            int cantidadPracticas = 0;
            // Trae el total de practicas...
            foreach (Turno turno in turnos)
            {
                string hql = "SELECT SUM(pt.Cantidad) FROM PracticaTurno pt WHERE pt.TurnoId = :turnoId";
                IQuery query = dalEngine.CreateQuery(hql);
                query.SetParameter("turnoId", turno.Id);
                cantidadPracticas += ObjAsInt(query.UniqueResult());
            }

            return cantidadPracticas;
        }

        private int ObjAsInt(object p)
        {
            if (p is decimal)
                return (int)((decimal)p);
            else if (p is int)
                return (int)p;
            else if (p is long)
                return (int)((long)p);
            else if (p is byte)
                return (int)((byte)p);
            else
                return (int)p;
        }

        private EntityCollection<Turno> TraeTurnosParaTope(Tope tope, DateTime desde, DateTime hasta)
        {
            EntityCollection<Turno> resultado = new EntityCollection<Turno>();

            string hql = "SELECT DISTINCT t from PracticaTurno pt, Turno t, ObraSocialPlan osp, Equipo e where pt.TurnoId = t.Id "
                    + " AND osp.Id = t.Orden.ObraSocialPlanId and t.EquipoId = e.Id AND t.EstadoTurnoID = :estado "
                    + " AND osp.ObraSocial.Id = :obraSocialId AND t.Fecha >= :desde AND "
                    + " t.Fecha < :hasta ";
            if (tope.ServicioID.HasValue)
                hql += " AND e.Servicio.Id = :servicioId ";
            if (tope.EquipoID.HasValue)
                hql += " AND e.Id = :equipoId ";
            if (tope.MedicoID.HasValue)
                hql += " AND pt.MedicoInformante.Id = :medicoId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("estado", (int)EstadoTurnoEnum.Reservado);
            query.SetParameter("obraSocialId", tope.ObraSocialID);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1));
            if (tope.ServicioID.HasValue)
                query.SetParameter("servicioId", tope.ServicioID);
            if (tope.EquipoID.HasValue)
                query.SetParameter("equipoId", tope.EquipoID.Value);
            if (tope.MedicoID.HasValue)
                query.SetParameter("medicoId", tope.MedicoID.Value);

            EntityCollection<Turno> turnos = dalEngine.GetManyByQuery<Turno>(query);

            // Si están en rango del horario, los agrega...
            foreach (Turno turno in turnos)
                if (turno.Fecha.Value.TimeOfDay >= desde.TimeOfDay &&
                    (turno.Fecha.Value.TimeOfDay < hasta.TimeOfDay || hasta.TimeOfDay.TotalMinutes == 0))
                    resultado.Add(turno);

            return resultado;
        }

        /// <summary>
        /// Elimino un Tope (Marca el Tope como No Vigente)
        /// </summary>
        /// <param name="tope">Tope a Eliminar</param>
        /// <param name="user">Usuario de la Operacion</param>
        [RequiresTransaction]
        public virtual void TopeDelete(int topeID)
        {
            // Obtengo el Tope
            Tope tope = dalEngine.GetById<Tope>(topeID);

            // Lo marco como No Vigente
            tope.Vigente = false;


            // Actualizo el Tope
            tope = dalEngine.Update<Tope>(tope);

            // Obtengo y actualizo los detalles
            EntityCollection<TopeDetalle> detalles = TopeDetalleReadByTope(topeID);

            int userId = Security.Current.UserInfo.User.Id;
            foreach (TopeDetalle td in detalles)
            {
                Audit.AuditDelete(td, userId);
                dalEngine.Update(td);
            }
        }


        // Horario










        /// <summary>
        /// Actualiza los datos del horario
        /// </summary>
        /// <param name="horario">Horario a actulizar</param>
        /// <returns>El Horario Actualizado</returns>
        private Horario HorarioInsert(Horario horario)
        {
            // Pongo el Id en 0 --> Insert
            if (horario.Id != 0)
                throw new Exception("El horario debe ser nuevo para realizar un insert.");

            return dalEngine.Update(horario);
        }







        // ArchivoAGFA
        /// <summary>
        /// Agrego un Paciente para el Archivo de AGFA
        /// </summary>
        /// <param name="equipo">Equipo del Turno</param>
        /// <param name="apellido">Apellido del Paciente</param>
        /// <param name="nombre">Nombre del Paciente</param>
        /// <param name="documento">Documento del Paciente</param>
        /// <param name="protocolo">DNI del Paciente</param>
        /// <param name="protocolo">Protocolo del Turno del Paciente</param>
        /// <param name="sexo">Sexo del Paciente</param>
        /// <param name="fechaNacimiento">Fecha de Nacimiento del Paciente</param>
        /// <returns>El Registro del ArchivoAGFA Creado</returns>
        public void ArchivoAGFAAgregarPaciente(Equipo equipo, string apellido, string nombre, string documento, string protocolo, string sexo, string fechaNacimiento)
        {
            EntityCollection<ArchivoAGFA> pacientes = ArchivoAGFAReadByProtocolo(protocolo);

            if (pacientes.Count == 0)
            {
                ArchivoAGFA paciente = new ArchivoAGFA();
                paciente.Equipo = equipo;
                paciente.Apellido = apellido;
                paciente.Nombre = nombre;
                paciente.Documento = documento;
                paciente.Protocolo = protocolo;
                paciente.Sexo = sexo;
                paciente.FechaNacimiento = fechaNacimiento;
                paciente.FechaArchivo = enfoke.Time.Today;
                paciente.Enviado = false;
                paciente.PracticaFinalizada = false;


                paciente = dalEngine.Update<ArchivoAGFA>(paciente);
            }
        }

        public void ArchivoAGFAFinPracticaPaciente(string protocolo)
        {
            EntityCollection<ArchivoAGFA> pacientes = ArchivoAGFAReadByProtocolo(protocolo);

            if (pacientes.Count > 0)
            {
                for (int i = 0; i < pacientes.Count; i++)
                    pacientes[i].PracticaFinalizada = true;


                dalEngine.UpdateCollection<ArchivoAGFA>(pacientes);
            }
        }

        private void ArchivoAGFARevertirFinPracticaPaciente(string protocolo)
        {
            EntityCollection<ArchivoAGFA> pacientes = ArchivoAGFAReadByProtocolo(protocolo);

            if (pacientes.Count > 0)
            {
                for (int i = 0; i < pacientes.Count; i++)
                {
                    pacientes[i].PracticaFinalizada = false;
                    pacientes[i].Enviado = false;
                }


                dalEngine.UpdateCollection<ArchivoAGFA>(pacientes);
            }
        }

        private EntityCollection<ArchivoAGFA> ArchivoAGFAReadByProtocolo(string protocolo)
        {
            ReadManyCommand<ArchivoAGFA> readCmd = new ReadManyCommand<ArchivoAGFA>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(ArchivoAGFA.Properties.Protocolo, "=", protocolo);

            readCmd.Filter.Add(BooleanOp.And, ArchivoAGFA.Properties.PracticaFinalizada, "=", "0");

            EntityCollection<ArchivoAGFA> pacientes = readCmd.Execute();
            return pacientes;
        }

        /// <summary>
        /// Obtengo todos los Pacientes a ser Enviados al Equipo
        /// </summary>
        /// <param name="equipoID">ID del Equipo a buscar Pacientes</param>
        /// <returns>Colección de Registros para el XML</returns>
        [Private]
        public EntityCollection<ArchivoAGFA> ArchivoAGFAObtenerPacientes(Equipo equipo)
        {
            ReadManyCommand<ArchivoAGFA> readCmd = new ReadManyCommand<ArchivoAGFA>(dalEngine);

            // Busco todos los que tienen el Enviado en 0 o que sean de hoy (esto es por si se carga uno a ultima hora)
            // Y chequeo que la practica no este finalizada
            Filter filter = new Filter();

            filter.Add(ArchivoAGFA.Properties.Equipo, "=", equipo.Id);
            filter.Add(BooleanOp.And, ArchivoAGFA.Properties.PracticaFinalizada, "=", 0);

            OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
            filter.Add(open);

            filter.Add(ArchivoAGFA.Properties.Enviado, "=", 0);

            filter.Add(BooleanOp.Or, ArchivoAGFA.Properties.FechaArchivo, "=", enfoke.Time.Today);

            CloseParenthesis close = new CloseParenthesis();
            filter.Add(close);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(ArchivoAGFA.Properties.Id, SortingDirection.Asc);
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Actualizo los Pacientes
        /// </summary>
        /// <param name="paciente">Los Pacientes enviados al Archivo</param>
        [Private]
        public void ArchivoAGFAUpdate(EntityCollection<ArchivoAGFAUpdate> pacientes)
        {
            dalEngine.UpdateCollection<ArchivoAGFAUpdate>(pacientes);
        }

        [AnonymousMethod]
        public EntityCollection<WorklistView> WorklistViewReadByConditions(
            string equipoSender,
            List<DICOMCondition> conditions)
        {
            Filter filter = new Filter();
            foreach (DICOMCondition c in conditions)
            {
                if (c != null && c.Value != null)
                {
                    if (c.Property == FilterEnum.WorklistName)
                    {
                        ExistsFilterItem<WorklistEquipo> filterExists = new ExistsFilterItem<WorklistEquipo>(
                            BooleanOp.And,
                            WorklistView.Properties.EquipoId, WorklistEquipo.Properties.Equipo.Id);
                        filterExists.Add(WorklistEquipo.Properties.Worklist.DisplayName, c.Operator, c.Value);
                        filter.Add(filterExists);
                    }
                    else
                    {
                        IPropertyReference prop;
                        switch (c.Property)
                        {
                            case FilterEnum.ExamDateAndTime:
                                prop = WorklistView.Properties.Fecha;
                                break;
                            case FilterEnum.Forename:
                                prop = WorklistView.Properties.Nombre;
                                break;
                            case FilterEnum.Surname:
                                prop = WorklistView.Properties.Apellido;
                                break;
                            case FilterEnum.EquipoId:
                                prop = WorklistView.Properties.EquipoId;
                                break;
                            case FilterEnum.PatientID:
                                prop = WorklistView.Properties.PacienteId;
                                int i;
                                if (Int32.TryParse(c.Value.ToString(), out i) == false)
                                    throw new Exception("El valor para identificador de paciente debe ser numérico (Valor no válido: '" + c.Value.ToString() + "').");
                                break;
                            default:
                                prop = null;
                                break;
                        }
                        if (prop != null)
                            filter.Add(BooleanOp.And,
                                prop, c.Operator, c.Value);
                    }
                }
            }
            return dalEngine.GetManyByFilter<WorklistView>(filter);
        }


        // TurnoBloqueoReserva
        /// <summary>
        /// Setea un Bloqueo de un Turno
        /// </summary>
        /// <param name="desde">Comienzo Bloqueo</param>
        /// <param name="hasta">Fin Bloqueo</param>
        /// <param name="medicoId">Medico Bloqueo</param>
        /// <param name="equipo">Equipo Bloqueo</param>
        /// <param name="user">Usuario Bloqueo</param>
        /// <returns>El TurnoBloqueo correspondiente [null si hay error - no se pudo bloquear]</returns>
        [Private]
        [RequiresTransaction]
        public virtual TurnoBloqueoReserva TurnoBloqueoReservaAdd(int userId, DateTime desde, DateTime hasta, int equipoID, TimeSpan tolerancia)
        {
            TurnoBloqueoReserva bloqueo = null;

            // Creo las fechas desde y hasta y Now sin segundos
            DateTime fechaDesde = new DateTime(desde.Year, desde.Month, desde.Day, desde.Hour, desde.Minute, 0, 0);
            DateTime fechaHasta = new DateTime(hasta.Year, hasta.Month, hasta.Day, hasta.Hour, hasta.Minute, 0, 0);
            DateTime now = enfoke.Time.Now;
            DateTime ahora = now.Subtract(new TimeSpan(0, 0, 0, now.Second, now.Millisecond));

            // Chequeo si el Turno a Bloquear no se encuentra Bloqueado
            string hql = "from TurnoBloqueoReserva t "
                        + "where t.EquipoId = :equipoId "
                        + "and t.FechaLiberado is null "
                        + "and t.UserId <> :userId "
                        + "and ((t.FechaDesde >= :fechaDesde and t.FechaDesde < :fechaHasta) "
                        + "     or (t.FechaHasta > :fechaDesde and t.FechaHasta <= :fechaHasta))";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipoID);
            query.SetParameter("userId", userId);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta);

            EntityCollection<TurnoBloqueoReserva> problemas = dalEngine.GetManyByQuery<TurnoBloqueoReserva>(query);

            bool ok = true;
            for (int i = 0; ok && i < problemas.Count; i++)
            {
                // Obtengo la diferencia entre ahora y la fecha del bloqueo
                TimeSpan dif = ahora.Subtract(problemas[i].FechaBloqueo);

                // Si el Problema ya esta "vencido" (diferencia >= tolerancia), lo libero
                // Sino, informo el problema (ok = false)
                if (dif.CompareTo(tolerancia) >= 0)
                {
                    problemas[i].FechaLiberado = enfoke.Time.Now;
                    problemas[i].Liberado = (int)TurnoBloqueoReservaLiberadoEnum.Automatico;
                    problemas[i] = dalEngine.Update(problemas[i]);
                }
                else
                    ok = false;
            }

            if (ok)
            {
                // No habia bloqueos, creo el nuevo y lo inserto
                SecurityUser user = Security.Current.UserInfo.User;
                bloqueo = new TurnoBloqueoReserva();
                bloqueo.FechaDesde = desde;
                bloqueo.FechaHasta = hasta;
                bloqueo.EquipoId = equipoID;
                bloqueo.UserId = user.Id;
                bloqueo.FechaBloqueo = ahora;
                bloqueo.FechaLiberado = null;
                bloqueo.Liberado = (int)TurnoBloqueoReservaLiberadoEnum.NoLiberado;


                bloqueo = dalEngine.Update<TurnoBloqueoReserva>(bloqueo);
            }
            else
                bloqueo = null;

            return bloqueo;
        }

        /// <summary>
        /// Libero un grupo de Bloqueos
        /// </summary>
        /// <param name="bloqueos">Bloqueos a Liberar</param>
        [RequiresTransaction]
        public virtual void TurnoBloqueoReservaFree(EntityCollection<TurnoBloqueoReserva> bloqueos)
        {
            for (int i = 0; i < bloqueos.Count; i++)
            {
                if (bloqueos[i] != null)
                {
                    // Obtengo el Bloqueo de la BD
                    TurnoBloqueoReserva dbBloqueo = dalEngine.GetById<TurnoBloqueoReserva>(bloqueos[i].Id);

                    // Si sigue sin ser liberado, lo libero
                    if (dbBloqueo.Liberado == (int)TurnoBloqueoReservaLiberadoEnum.NoLiberado)
                    {
                        dbBloqueo.FechaLiberado = enfoke.Time.Now;
                        dbBloqueo.Liberado = (int)TurnoBloqueoReservaLiberadoEnum.Usuario;


                        dbBloqueo = dalEngine.Update<TurnoBloqueoReserva>(dbBloqueo);
                    }
                }
            }
        }

        [Private]
        public void TurnoBloqueoReservaPurge()
        {
            ReadManyCommand<TurnoBloqueoReserva> readCmd = new ReadManyCommand<TurnoBloqueoReserva>(dalEngine);

            // Filtro por Fecha - Anterior a Ahora menos 1 dia (ayer)
            readCmd.Filter = new Filter();
            readCmd.Filter.Add(TurnoBloqueoReserva.Properties.FechaBloqueo, "<", enfoke.Time.Now.AddDays(-1));

            EntityCollection<TurnoBloqueoReserva> bloqueos = readCmd.Execute();

            if (bloqueos.Count > 0)
            {
                dalEngine.Delete(bloqueos);
            }
        }


        // ConsultaTurnoView
        /// <summary>
        /// Trae el listado de turnosIds para la consulta general
        /// </summary>
        /// <param name="fecha">La fecha a buscar</param>
        /// <param name="paciente">Busqueda por Paciente</param>
        /// <param name="protocolo">Busqueda por Protocolo</param>
        /// <param name="practica">Busqueda por Practica</param>
        /// <param name="obraSocial">Busqueda por Obra Social</param>
        /// <param name="medicoId">Busqueda por Medico</param>
        /// <param name="mostrarCancelados">Marca si Muestra Cancelados</param>
        /// <param name="sucursal">Busqueda por Sucursal</param>
        /// <returns>Turnos</returns>
        [Timeout(120)]
        // no borrar este metodo esta de prueba para tomar mediciones en produccion y luego intercambiar el actual con este.
        public virtual EntityCollection<ConsultaTurnoView> ConsultaTurnoViewReadHQL(DateTime? fechaDesde, DateTime? fechaHasta, string paciente, string protocolo, string practica, string obraSocial, int? medicoId, bool mostrarCancelados, List<int> centrosIds, string dni, int? servicioId, int? equipo, int maxRows)
        {
            List<int> equipos;
            EntityCollection<Turno> turnos = ConsultaGeneralTurnos(fechaDesde, fechaHasta, practica, paciente, protocolo, medicoId, obraSocial, true, true, mostrarCancelados, true, null, centrosIds, dni, servicioId, maxRows, equipo, out equipos);
            EntityCollection<TurnoAdmisionView> views = ConstruirRespuestas(maxRows, equipos, turnos);
            EntityCollection<ConsultaTurnoView> result = ConstruirConsultas(views, maxRows);
            return result;
        }

        private EntityCollection<ConsultaTurnoView> ConstruirConsultas(EntityCollection<TurnoAdmisionView> views, int maxRows)
        {
            IList<int> practicaTurnos = PracticasTurnosIds(views);
            EntityCollection<MedicoLight> informantes = Context.Session.MedicosDalc.MedicoFromPracticaTurnoReadByProperty(PracticaTurno.Properties.MedicoInformante.Id.Name, practicaTurnos);
            EntityCollection<MedicoLight> tecnicos = Context.Session.MedicosDalc.MedicoFromPracticaTurnoReadByProperty(PracticaTurno.Properties.MedicoTecnico.Id.Name, practicaTurnos);
            foreach (TurnoAdmisionView view in views)
            {
                MedicoLight informante = informantes.Find(delegate(MedicoLight med) { return med.PracticaTurnoId == view.PracticaTurnoId; });
                view.MezclarMedicoInformante(informante);
                MedicoLight tecnico = tecnicos.Find(delegate(MedicoLight med) { return med.PracticaTurnoId == view.PracticaTurnoId; });
                view.MezclarMedicoTecnico(tecnico);
            }

            EntityCollection<TurnoInforme> turnosConInformes = TurnosInformeIdPrometido(views.GetIds(), maxRows);
            EntityCollection<ConsultaTurnoView> response = new EntityCollection<ConsultaTurnoView>();
            ReadAllCollection<TipoTurno> tipoTurnos = this.TipoTurnoReadAll();
            foreach (TurnoAdmisionView view in views)
            {
                ConsultaTurnoView consulta = new ConsultaTurnoView(view);
                consulta.TipoTurno = tipoTurnos.Collection.FindByKey(consulta.TipoTurnoID).Name;

                IEnumerable<TurnoInforme> tuis = turnosConInformes.FindAll(delegate(TurnoInforme turnoInforme) { return turnoInforme.TurnoID == view.Id; });
                if (tuis != null)
                    foreach (TurnoInforme tui in tuis)
                    {
                        if (tui.EstadoVisible)
                            consulta.InformeGenerado = true;

                        if (!consulta.InformePrometido)
                            consulta.InformePrometido = tui.Prometido;
                    }

                response.Add(consulta);
            }


            return response;
        }

        private IList<int> PracticasTurnosIds(EntityCollection<TurnoAdmisionView> views)
        {
            List<int> practicaturnosids = new List<int>();
            foreach (TurnoAdmisionView view in views)
                practicaturnosids.Add(view.PracticaTurnoId);

            return practicaturnosids;
        }

        public EntityCollection<ConsultaTurnoView> ConsultaTurnoViewReadByTurnosIds(List<int> idsTurnos)
        {
            if (idsTurnos == null || idsTurnos.Count == 0)
                return new EntityCollection<ConsultaTurnoView>();

            ReadManyCommand<ConsultaTurnoView> readCmd = new ReadManyCommand<ConsultaTurnoView>(dalEngine);

            readCmd.Filter = new Filter();
            readCmd.Filter.Add(ConsultaTurnoView.Properties.Id, "IN", idsTurnos.ToArray());

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ConsultaTurnoView.Properties.FechaCompleta, SortingDirection.Desc);

            return readCmd.Execute();
        }

        public ConsultaTurnoView ConsultaTurnoViewReadByPresupuestoId(int presupuestoId)
        {
            Turno turno = dalEngine.GetByProperty<Turno>(Turno.Properties.PresupuestoId, presupuestoId);
            return dalEngine.GetById<ConsultaTurnoView>(turno.Id);
        }



        // Sucursal
        /// <summary>
        /// Retorno todas las Sucursales
        /// </summary>
        /// <returns>Todas las Sucursal</returns>
        public EntityCollection<Sucursal> SucursalReadAll()
        {
            return dalEngine.GetAll<Sucursal>(Sucursal.Properties.Name);
        }
        /// <summary>
        /// Retorno todas las Sucursales
        /// </summary>
        /// <returns>Todas las Sucursal</returns>
        public ReadAllCollection<SucursalName> SucursalNameReadAll()
        {
            return new ReadAllCollection<SucursalName>(dalEngine.GetAll<SucursalName>(Sucursal.Properties.Name));
        }













        /// <summary>
        /// Retorno una Sucursal para un Name
        /// </summary>
        /// <param name="name">Name de Sucursal</param>
        /// <returns>La Sucursal correspondiente</returns>
        public Sucursal SucursalReadByName(string name)
        {
            return dalEngine.GetByProperty<Sucursal>(Sucursal.Properties.Name, name);
        }

        private SucursalName SucursalNameReadByTag(string tag)
        {
            // Predicate para buscar los Sucursales por Tag
            Predicate<SucursalName> predicate = delegate(SucursalName compare)
            {
                return compare.Tag.ToUpper().TrimStart('0').Trim() == tag.ToUpper();
            };

            return SucursalNameReadAll().Collection.Find(predicate);

            /* [GG] Cambiar
            return dalEngine.GetByProperty<Sucursal>(Sucursal.Properties.Tag, tag);
            */
        }






        /// <summary>
        /// Actualizo la Sucursal
        /// </summary>
        /// <param name="sucursal">Sucursal a Actualizar</param>
        public void SucursalUpdate(Sucursal sucursal)
        {
            dalEngine.Update<Sucursal>(sucursal);
        }


        // SucursalProtocolo

        internal SucursalProtocolo SucursalProtocoloReadBySucursalAndOrigen(Equipo equipo)
        {
            if (equipo.Sucursal.NumeracionPorServicio)
                return SucursalProtocoloReadBySucursalAndOrigen(equipo.Sucursal.Id, equipo.Servicio.Tag);
            else
                return SucursalProtocoloReadBySucursalAndOrigen(equipo.Sucursal.Id, SucursalProtocolo.OrigenPorDefecto);
        }

        private SucursalProtocolo SucursalProtocoloReadBySucursalAndOrigen(int sucursal, string origen)
        {
            // Obtengo el SucursalProtocolo de la Sucursal y el Origen
            IQuery query = dalEngine.CreateQuery("from SucursalProtocolo p "
                          + "where p.SucursalID = :sucursal and p.Origen = :origen");
            query.SetParameter("sucursal", sucursal);
            query.SetParameter("origen", origen);
            return dalEngine.GetByQuery<SucursalProtocolo>(query);
        }

        internal SucursalProtocolo SucursalProtocoloObtenerSiguiente(Equipo equipo)
        {
            if (equipo.Sucursal.NumeracionPorServicio)
                return SucursalProtocoloObtenerSiguiente(equipo.Sucursal.Id, equipo.Servicio.Tag);
            else
                return SucursalProtocoloObtenerSiguiente(equipo.Sucursal.Id, SucursalProtocolo.OrigenPorDefecto);
        }

        private SucursalProtocolo SucursalProtocoloObtenerSiguiente(int sucursal, string origen)
        {
            // Consulta cuál le corresponde
            SucursalProtocolo numero = SucursalProtocoloReadBySucursalAndOrigen(sucursal, origen);

            if (numero != null && numero.Id > 0)
            {
                // Lo lockea
                enfoke.Context.Data.Session.LockWithWait<SucursalProtocolo>(numero.Id, 2000);
                // Lo vuelve a leer (por si cambió entre el read y el lockeo)
                enfoke.Context.Data.Session.Refresh(numero);
            }

            // Incremento
            if (numero != null)
                numero.UltimoProtocolo++;
            else
                numero = new SucursalProtocolo(origen, sucursal);
            // Guardo
            return dalEngine.Update<SucursalProtocolo>(numero);
        }


        // SucursalServicio
        public EntityCollection<SucursalServicio> SucursalServicioReadBySucursalID(int idSucursal)
        {
            return dalEngine.GetManyByProperty<SucursalServicio>(SucursalServicio.Properties.Sucursal.Id, idSucursal);
        }

        public EntityCollection<SucursalServicio> SucursalServicioReadByServicioID(int idServicio)
        {
            return dalEngine.GetManyByProperty<SucursalServicio>(SucursalServicio.Properties.Servicio.Id, idServicio);
        }


        // Sector
        /// <summary>
        /// Retorno todos los Sectores
        /// </summary>
        /// <returns>Todos los Sectores</returns>
        public EntityCollection<Sector> SectorReadAll()
        {
            return dalEngine.GetAll<Sector>(Sector.Properties.Name);
        }

        /// <summary>
        /// Retorno todos los Sectores
        /// </summary>
        /// <param name="sucursal">Sucursal</param>
        /// <returns>Todos los Sectores</returns>
        public EntityCollection<Sector> SectoresReadBySucursal(int sucursalId)
        {
            return dalEngine.GetManyByProperty<Sector>(Sector.Properties.Sucursal, sucursalId, Sector.Properties.Name);
        }

        /// <summary>
        /// [GG] Retorno todos los sectores correspondientes a un tipo de sector
        /// </summary>
        /// <param name="idTipoSector"></param>
        /// <returns></returns>
        public EntityCollection<Sector> SectorReadByTipoSector(int idTipoSector)
        {
            EntityCollection<Sector> col = new EntityCollection<Sector>();

            StringBuilder hql = new StringBuilder(" Select sts.Sector from SectorTipoSector sts ");

            if (!idTipoSector.Equals(int.MinValue))
                hql.Append("WHERE sts.TipoSector.Id = :tipoSector ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetInt32("tipoSector", idTipoSector);

            col = dalEngine.GetManyByQuery<Sector>(query);

            return col;
        }

        public EntityCollection<Sector> SectorReadBySucursal(int sucursalId)
        {
            return dalEngine.GetManyByProperty<Sector>(Sector.Properties.Sucursal, sucursalId);
        }

        /// <summary>
        /// [GG] Retorno todos los sectores correspondientes a un tipo de sector y a un centro
        /// </summary>
        /// <param name="idTipoSector"></param>
        /// <returns></returns>
        public EntityCollection<Sector> SectorReadByTipoSectorAndCentro(int idTipoSector, int idCentro)
        {
            EntityCollection<Sector> col = new EntityCollection<Sector>();

            StringBuilder hql = new StringBuilder(" Select sts.Sector from SectorTipoSector sts ");
            hql.Append(" WHERE sts.TipoSector.Id = :tipoSector ");
            hql.Append(" AND sts.Sector.Sucursal.Id = :centro ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetInt32("tipoSector", idTipoSector);
            query.SetInt32("centro", idCentro);

            col = dalEngine.GetManyByQuery<Sector>(query);

            return col;
        }

        public EntityCollection<Sector> SectoresTipoEntregaReadByCentroId(int idCentro)
        {
            return SectorReadByTipoSectorAndCentro((int)TipoSectorEnum.EntregaInformes, idCentro);
        }











        /// <summary>
        /// Actualizo los Sectores
        /// </summary>
        /// <param name="sectores">Los Sectores a Actualizar</param>
        /// <returns>La coleccion actualizada</returns>
        public void SectorUpdateMany(EntityCollection<Sector> sectores)
        {
            dalEngine.UpdateCollection<Sector>(sectores);

            //return sectores;
        }

        public Sector SectorUpdate(Sector sector)
        {
            sector = dalEngine.Update<Sector>(sector);

            return sector;
        }

        [RequiresTransaction]
        public virtual Sector SectorUpdate(Sector sector, EntityCollection<TipoSector> tiposSector)
        {
            // Actualizo el sector
            Sector sec = SectorUpdate(sector);

            // Borro los TipoSector asociados originalmente al Sector si es que no existen en esta nueva lista de TipoSector enviada.
            EntityCollection<TipoSector> tiposSectorOriginales = TipoSectorReadBySectorId(sec.Id);
            foreach (TipoSector tso in tiposSectorOriginales)
                if (!tiposSector.Contains(tso))
                    dalEngine.Delete(SectorTipoSectorReadBySectorIdAndTipoSectorId(sec.Id, tso.Id));

            // Inserto los TipoSector asociados al Sector que no existieran originalmente.
            foreach (TipoSector ts in tiposSector)
            {
                SectorTipoSector sts = SectorTipoSectorReadBySectorIdAndTipoSectorId(sec.Id, ts.Id);
                if (sts == null)
                    dalEngine.Update(new SectorTipoSector(sec, ts));
            }

            return sec;
        }

        public bool SectorHasTipoSector(int idSector, TipoSectorEnum tipoSectorEnum)
        {
            string hql = "FROM SectorTipoSector sts " +
                "WHERE sts.Sector.Id = :idSector " +
                "AND sts.TipoSector.Id = :idTipoSector";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);
            query.SetParameter("idTipoSector", (int)tipoSectorEnum);
            query.SetMaxResults(1);

            return (dalEngine.GetByQuery<SectorTipoSector>(query) != null);
        }

        private SectorTipoSector SectorTipoSectorReadBySectorIdAndTipoSectorId(int idSector, int idTipoSector)
        {
            string hql = "FROM SectorTipoSector sts " +
                         "WHERE sts.Sector.Id = :idSector " +
                         "AND sts.TipoSector.Id = :idTipoSector ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);
            query.SetParameter("idTipoSector", idTipoSector);

            EntityCollection<SectorTipoSector> sectorTipoSector = dalEngine.GetManyByQuery<SectorTipoSector>(query);

            if (sectorTipoSector.Count > 0)
                return sectorTipoSector[0];

            return null;
        }

        /// <summary>
        /// [CB] Retorno los Tipos de Sector a los que pertenece un Sector
        /// </summary>
        /// <param name="idTipoSector"></param>
        /// <returns></returns>
        public EntityCollection<TipoSector> TipoSectorReadBySectorId(int idSector)
        {
            string hql = "SELECT sts.TipoSector FROM SectorTipoSector sts " +
                         "WHERE sts.Sector.Id = :idSector";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);

            return dalEngine.GetManyByQuery<TipoSector>(query);
        }

        /// <summary>
        /// [GG] Retorno los Tipos de Sector a los que pertenece un Sector
        /// </summary>
        /// <param name="idTipoSector"></param>
        /// <returns></returns>
        public EntityCollection<TipoSector> TipoSectorReadByTag(string tagTipoSector)
        {
            string hql = "SELECT sts.TipoSector FROM SectorTipoSector sts " +
                         "WHERE sts.TipoSector.Tag = :tagTipoSector";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("tagTipoSector", tagTipoSector);

            return dalEngine.GetManyByQuery<TipoSector>(query);
        }








        // SectorServicio
        /// <summary>
        /// Retorno los Servicios de un Sector
        /// </summary>
        /// <param name="idSector">ID de Sector</param>
        /// <returns>Los Servicios correspondientes</returns>
        public EntityCollection<Servicio> SectorServicioReadBySector(int idSector)
        {
            string hql = "SELECT s.Servicio FROM SectorServicio s WHERE s.Sector.Id = :idSector ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);

            EntityCollection<Servicio> servicios = dalEngine.GetManyByQuery<Servicio>(query);

            servicios.AcceptChanges();
            return servicios;

        }

        /// <summary>
        /// Actualizo los Servicios de un Sector
        /// </summary>
        /// <param name="sector">El Sector</param>
        /// <param name="servicios">Los Servicios</param>
        /// <returns>La coleccion actualizada</returns>
        [RequiresTransaction]
        public virtual void SectorServicioUpdateMany(Sector sector, IList<Servicio> servicios)
        {
            EntityCollection<Servicio> serviciosSector = SectorServicioReadBySector(sector.Id);

            // Elimino los Servicios que saque
            EntityCollection<SectorServicio> serviciosEliminar = new EntityCollection<SectorServicio>();
            foreach (Servicio servicio in serviciosSector)
                if (!servicios.Contains(servicio))
                    serviciosEliminar.Add(SectorServicioReadBySectorAndServicio(sector.Id, servicio.Id));


            dalEngine.Delete(serviciosEliminar);

            // Agrego los Servicios que no estaban
            EntityCollection<SectorServicio> serviciosAgregar = new EntityCollection<SectorServicio>();
            foreach (Servicio servicio in servicios)
                if (!serviciosSector.Contains(servicio))
                    serviciosAgregar.Add(new SectorServicio(sector, servicio));


            dalEngine.UpdateCollection<SectorServicio>(serviciosAgregar);
        }

        private SectorServicio SectorServicioReadBySectorAndServicio(int idSector, int idServicio)
        {
            string hql = "FROM SectorServicio ss WHERE ss.Sector.Id = :idSector AND ss.Servicio.Id = :idServicio ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);
            query.SetParameter("idServicio", idServicio);
            query.SetMaxResults(1);

            return dalEngine.GetByQuery<SectorServicio>(query);
        }


        // TurnoNoInformado
        /// <summary>
        /// Trae el listado de turnosIds para mostrar en no informados
        /// </summary>
        /// <param name="searchString">Busqueda</param>
        /// <param name="searchType">Tipo Busqueda</param>
        /// <param name="medicoId">id del Medico</param>
        /// <returns>listado de turnosIds</returns>
        [MinuteTimeout]
        public virtual EntityCollection<TurnoNoInformadoView> TurnoNoInformadoViewRead(string searchString, TurnoNoInformadoSearchTypeEnum searchType, int medicoId)
        {
            string search = searchString.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<TurnoNoInformadoView> readCmd = new ReadManyCommand<TurnoNoInformadoView>(dalEngine);

            readCmd.Filter = new Filter();

            OpenParenthesis open = new OpenParenthesis();
            readCmd.Filter.Add(open);

            readCmd.Filter.Add(TurnoNoInformadoView.Properties.MedicoId, "=", medicoId);

            readCmd.Filter.Add(BooleanOp.Or, TurnoNoInformadoView.Properties.MedicoTecnicoId, "=", medicoId);

            readCmd.Filter.Add(BooleanOp.Or, TurnoNoInformadoView.Properties.MedicoInformanteId, "=", medicoId);

            CloseParenthesis close = new CloseParenthesis();
            readCmd.Filter.Add(close);

            if (!String.IsNullOrEmpty(searchString))
            {
                BooleanOp op = (searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera ? BooleanOp.Or : BooleanOp.And);
                OpenParenthesis openp = new OpenParenthesis(BooleanOp.And);
                readCmd.Filter.Add(openp);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Medico || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Medico, "LIKE", search);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Paciente || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Paciente, "LIKE", search);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Servicio || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Servicio, "LIKE", search);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Equipo || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Equipo, "LIKE", search);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Practica || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Practica, "LIKE", search);

                if (searchType == TurnoNoInformadoSearchTypeEnum.Centro || searchType == TurnoNoInformadoSearchTypeEnum.Cualquiera)
                    readCmd.Filter.Add(op, TurnoNoInformadoView.Properties.Centro, "LIKE", search);

                CloseParenthesis closep = new CloseParenthesis();
                readCmd.Filter.Add(closep);
            }

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(TurnoNoInformadoView.Properties.Fecha, SortingDirection.Asc);

            return readCmd.Execute();
        }


        // TurnoConfirmacion
        [MinuteTimeout]
        public virtual EntityCollection<TurnoConfirmacionView> TurnoConfirmacionViewRead(DateTime desde, DateTime hasta, string searchString, TurnoConfirmacionSearchTypeEnum searchType, bool incluirProvisorios, int maxRows)
        {
            string search = searchString.Trim().Replace(" ", "%") + "%";

            Filter filter = new Filter();

            filter.Add(new OpenParenthesis());
            filter.Add(TurnoConfirmacionView.Properties.Fecha, ">=", desde.Date);
            filter.Add(BooleanOp.And, TurnoConfirmacionView.Properties.Fecha, "<", hasta.Date.AddDays(1));
            filter.Add(BooleanOp.Or, TurnoConfirmacionView.Properties.Fecha, " IS ", null);
            filter.Add(new CloseParenthesis());

            if (!incluirProvisorios)
            {
                filter.Add(BooleanOp.And, TurnoConfirmacionView.Properties.TipoTurnoID, "<>", (int)TipoTurnoEnum.Provisorio);
            }

            // Filtro TurnoConfirmacionSearchTypeEnum
            if (!String.IsNullOrEmpty(searchString))
            {
                BooleanOp op = (searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera ? BooleanOp.Or : BooleanOp.And);
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);

                if (searchType == TurnoConfirmacionSearchTypeEnum.Medico || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.Medico, "LIKE", search);

                if (searchType == TurnoConfirmacionSearchTypeEnum.Paciente || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.Paciente, "LIKE", search);

                if (searchType == TurnoConfirmacionSearchTypeEnum.Servicio || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.Servicio, "LIKE", search);

                if (searchType == TurnoConfirmacionSearchTypeEnum.Equipo || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.Equipo, "LIKE", search);

                if (searchType == TurnoConfirmacionSearchTypeEnum.OS || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.ObraSocial, "LIKE", search);

                if (searchType == TurnoConfirmacionSearchTypeEnum.Centro || searchType == TurnoConfirmacionSearchTypeEnum.Cualquiera)
                    filter.Add(op, TurnoConfirmacionView.Properties.Centro, "LIKE", search);

                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }


            Sort sort = new Sort();
            sort.Add(TurnoConfirmacionView.Properties.Fecha, SortingDirection.Asc);

            ReadManyCommand<TurnoConfirmacionView> readCmd = new ReadManyCommand<TurnoConfirmacionView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            return readCmd.Execute();
        }


        // PacienteEspera

        public EntityCollection<PacienteEspera> PacienteEsperaReadByFilters(DateTime? fechaCargaDesde, DateTime? fechaCargaHasta, string paciente, string dni, string servicio, string obraSocial, string medico, List<int> centrosIds)
        {
            StringBuilder hql = new StringBuilder("SELECT new enfoke.Eges.Entities.Results.PacienteEspera(COUNT(t.Id), ");
            hql.Append("pt.Turno.Estado.Id, ");
            hql.Append("pt.Turno.Estado.Name, ");
            hql.Append("pt.Turno.CreateDate, ");
            hql.Append("pt.Turno.Id, ");
            hql.Append("med.Apellido, ");
            hql.Append("med.Name, ");
            hql.Append("med.Id, ");
            hql.Append("pt.Turno.Orden.ObraSocialPlan.ObraSocial.Name, ");
            hql.Append("pt.Turno.Orden.ObraSocialPlan.ObraSocial.Id, ");
            hql.Append("pt.Turno.Observaciones, ");
            hql.Append("pt.Turno.Orden.Paciente.ApellidoNombre, ");
            hql.Append("pt.Turno.Orden.Paciente.Id, ");
            hql.Append("pt.Turno.Orden.Paciente.Dni, ");
            hql.Append("pt.Practica.Name, ");
            hql.Append("pt.Practica.Id, ");
            hql.Append("pt.Practica.ServicioEspecialidad.Servicio.Name, ");
            hql.Append("pt.Practica.ServicioEspecialidad.Servicio.Id, ");
            hql.Append("pt.Turno.CreateUser, ");
            hql.Append("suc.Name, ");
            hql.Append("suc.Id ) ");
            hql.Append("FROM PracticaTurnoHQL AS pt JOIN pt.Turno.Estado AS e ");
            hql.Append("LEFT JOIN pt.Turno.TurnoEsperaSucursalHQL AS tes ");
            hql.Append("LEFT JOIN tes.Sucursal AS suc ");
            hql.Append("LEFT JOIN pt.Medico AS med ");
            hql.Append(", Turno AS t ");
            hql.Append("WHERE t.ComboId = pt.Turno.ComboId ");
            hql.Append("AND pt.Cantidad > 0 ");
            hql.Append("AND pt.Tipo.Id = :practicaTurnoTipoID ");
            hql.Append("AND pt.Turno.Estado.Id = :estadoTurnoID ");
            hql.Append("AND t.EstadoTurnoID != :estadoTurnoCanceladoID ");

            if (fechaCargaDesde.HasValue == true)
                hql.Append("AND pt.Turno.CreateDate >= :fechaDesde ");
            if (fechaCargaHasta.HasValue == true)
                hql.Append("AND pt.Turno.CreateDate < :fechaHasta ");
            if (string.IsNullOrEmpty(paciente) == false)
                hql.Append("AND pt.Turno.Orden.Paciente.ApellidoNombre LIKE :pacienteApellidoNombre ");
            if (string.IsNullOrEmpty(dni) == false)
                hql.Append("AND pt.Turno.Orden.Paciente.Dni LIKE :pacienteDNI ");
            if (string.IsNullOrEmpty(servicio) == false)
                hql.Append("AND pt.Practica.ServicioEspecialidad.Servicio.Name LIKE :servicioName ");
            if (string.IsNullOrEmpty(obraSocial) == false)
                hql.Append("AND pt.Turno.Orden.ObraSocialPlan.ObraSocial.Name LIKE :obraSocialName ");
            if (string.IsNullOrEmpty(medico) == false)
            {
                hql.Append("AND " + SQLPortable.StringConcat("pt.Medico.Apellido", " ", "pt.Medico.Name"));
                hql.Append("LIKE :medicoApyN ");
            }
            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    hql.Append(" AND suc.Id = :sucursal ");
                else
                    hql.Append(" AND suc.Id in (:sucursal) ");
            }

            hql.Append("GROUP BY t.ComboId, e.Id, e.Name, pt.Turno.CreateDate, pt.Turno.Id, med.Apellido, med.Name, med.Id, ");
            hql.Append("pt.Turno.Orden.ObraSocialPlan.ObraSocial.Name, pt.Turno.Orden.ObraSocialPlan.ObraSocial.Id, pt.Turno.Observaciones, ");
            hql.Append("pt.Turno.Orden.Paciente.ApellidoNombre, pt.Turno.Orden.Paciente.Id, pt.Turno.Orden.Paciente.Dni, ");
            hql.Append("pt.Practica.Name, pt.Practica.Id, pt.Practica.ServicioEspecialidad.Servicio.Name, pt.Practica.ServicioEspecialidad.Servicio.Id, pt.Turno.CreateUser, suc.Name, suc.Id ");
            hql.Append("ORDER BY pt.Turno.CreateDate ASC, pt.Turno.Orden.Paciente.ApellidoNombre ASC, suc.Name ASC");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("practicaTurnoTipoID", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameter("estadoTurnoID", (int)EstadoTurnoEnum.ReservaProvisoria);
            query.SetParameter("estadoTurnoCanceladoID", (int)EstadoTurnoEnum.Cancelado);
            if (fechaCargaDesde.HasValue == true)
                query.SetParameter("fechaDesde", fechaCargaDesde.Value.Date);
            if (fechaCargaHasta.HasValue == true)
                query.SetParameter("fechaHasta", fechaCargaHasta.Value.Date.AddDays(1));
            if (string.IsNullOrEmpty(paciente) == false)
                query.SetParameter("pacienteApellidoNombre", paciente.Trim().Replace(" ", "%") + "%");
            if (string.IsNullOrEmpty(dni) == false)
                query.SetParameter("pacienteDNI", dni.Trim().Replace(" ", "%") + "%");
            if (string.IsNullOrEmpty(servicio) == false)
                query.SetParameter("servicioName", servicio.Trim().Replace(" ", "%") + "%");
            if (string.IsNullOrEmpty(obraSocial) == false)
                query.SetParameter("obraSocialName", obraSocial.Trim().Replace(" ", "%") + "%");
            if (string.IsNullOrEmpty(medico) == false)
                query.SetParameter("medicoApyN", medico.Trim().Replace(" ", "%") + "%");
            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    query.SetParameter("sucursal", centrosIds[0]);
                else
                    query.SetParameterList("sucursal", centrosIds);
            }

            EntityCollection<PacienteEspera> pacientesEnEspera = dalEngine.GetManyByQuery<PacienteEspera>(query);
            AgregarSecurityUser(pacientesEnEspera);

            return ObtenerSoloDistintos(pacientesEnEspera);

        }

        private void AgregarSecurityUser(EntityCollection<PacienteEspera> pacientesEnEspera)
        {
            List<int> userIDs = new List<int>();
            foreach (PacienteEspera pac in pacientesEnEspera)
            {
                if (pac.UsuarioCargaID.HasValue)
                    userIDs.Add(pac.UsuarioCargaID.Value);
            }

            Dictionary<int, SecurityUser> diccUser = new Dictionary<int, SecurityUser>();
            if (userIDs.Count > 0)
            {
                var usuarios = from user in dalEngine.Query<SecurityUser>() where userIDs.Contains(user.Id) select user;
                diccUser = usuarios.ToDictionary(user => user.Id, user => user);
            }
            foreach (PacienteEspera pac in pacientesEnEspera)
            {
                if (pac.UsuarioCargaID.HasValue)
                    pac.SecurityUser = diccUser[pac.UsuarioCargaID.Value];
            }
        }

        private EntityCollection<PacienteEspera> ObtenerSoloDistintos(EntityCollection<PacienteEspera> pacientesEnEspera)
        {
            EntityCollection<PacienteEspera> result = new EntityCollection<PacienteEspera>();

            foreach (PacienteEspera pe in pacientesEnEspera)
            {
                // predicate
                Predicate<PacienteEspera> predicate = delegate(PacienteEspera compare)
                {
                    return compare.TurnoID == pe.TurnoID;
                };


                PacienteEspera aux = result.Find(predicate);
                if (aux == null)
                    result.Add(pe);
                else
                    aux.AgregarSucursal(pe.Sucursal);

            }

            return result;
        }



        // Reasignacion de Medicos
        public EntityCollection<ReasignacionMedicoView> ReasignacionMedicoViewRead(DateTime fecha, int medicoId, TipoParticipacionMedico tipo)
        {
            Filter filter = new Filter();

            filter.Add(ReasignacionMedicoView.Properties.Fecha, ">=", fecha.Date);

            filter.Add(BooleanOp.And, ReasignacionMedicoView.Properties.Fecha, "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, ReasignacionMedicoView.Properties.EstadoId, "<>", (int)EstadoTurnoEnum.Cancelado);

            string column = string.Empty;
            switch (tipo)
            {
                case TipoParticipacionMedico.Actuante:
                    column = "MedicoId";
                    break;
                case TipoParticipacionMedico.Informante:
                    column = "MedicoInformanteId";
                    break;
                case TipoParticipacionMedico.Tecnico:
                    column = "MedicoTecnicoId";
                    break;
                default:
                    throw new Exception("El TipoParticipacionMedico indicado no es válido.");
            }

            filter.Add(BooleanOp.And, column, "=", medicoId);

            Sort sort = new Sort();
            sort.Add(ReasignacionMedicoView.Properties.Fecha, SortingDirection.Asc);

            ReadManyCommand<ReasignacionMedicoView> readCmd = new ReadManyCommand<ReasignacionMedicoView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public void ReasignarMedicoActuante(int turnoID, Medico medicoDestino)
        {
            // Leo las practicas del turno.
            EntityCollection<PracticaTurno> practicasTurnos =
                PracticaTurnoReadByTurno(turnoID, PracticaTurnoTipoEnum.Todas);

            // A cada practica le seteo el nuevo medicoId
            foreach (PracticaTurno pt in practicasTurnos)
                pt.Medico = medicoDestino;

            PracticaTurnoUpdateMany(practicasTurnos);
        }

        public void ReasignarMedicoActuante(int turnoID, int medicoDestinoID)
        {
            MedicosDalc MedicosDalc = Context.Session.MedicosDalc;
            ReasignarMedicoActuante(turnoID, MedicosDalc.MedicoReadById(medicoDestinoID));
        }

        /// <summary>
        /// Actualizo las modificaciones por la Reasignacion de Médicos
        /// </summary>
        /// <param name="practicas">Colección de PracticaTurno modificadas</param>
        /// <param name="items">Colección de ValorizacionItem modificados</param>
        /// <param name="informes">Colección de TurnoInforme modificados</param>
        /// <param name="recibosEliminar">Colección de ReciboMedico existentes a eliminar</param>
        /// <param name="recibosInsertar">Colección de ReciboMedico nuevos</param>
        /// <param name="user">Usuario de la Operació</param>
        [Private]
        [RequiresTransaction]
        public virtual void ReasignarInformantes(EntityCollection<TurnoInforme> informes,
          EntityCollection<PracticaTurno> practicas,
          EntityCollection<ValorizacionItem> items,
          EntityCollection<ReciboMedico> recibosEliminar,
          EntityCollection<ReciboMedico> recibosInsertar)
        {

            // Actualizo los TurnoInforme
            informes = dalEngine.UpdateCollection<TurnoInforme>(informes);

            // Logueo el Cambio en el Turno
            LogReasignarInformante(practicas);

            // Actualizo las PracticaTurno
            practicas = dalEngine.UpdateCollection<PracticaTurno>(practicas);

            // Actualizo las ValorizacionItem
            dalEngine.UpdateCollection<ValorizacionItem>(items);

            // Elimino los ReciboMedico Viejos
            dalEngine.Delete(recibosEliminar);

            foreach (ReciboMedico recibo in recibosInsertar)
                recibo.FechaCreacion = enfoke.Time.Now;

            // Inserto los ReciboMedico Nuevos
            recibosInsertar = dalEngine.UpdateCollection<ReciboMedico>(recibosInsertar);
        }

        private void LogReasignarInformante(EntityCollection<PracticaTurno> practicas)
        {
            Dictionary<int, string> logByTurno = new Dictionary<int, string>();

            foreach (PracticaTurno pt in practicas)
            {
                if (logByTurno.ContainsKey(pt.TurnoId))
                    logByTurno[pt.TurnoId] += "\r\nEl médico informante de la " + pt.Practica.Name + " es ahora " + pt.MedicoInformante.ApyN + ".";
                else
                    logByTurno.Add(pt.TurnoId, "El médico informante de la " + pt.Practica.Name + " es ahora " + pt.MedicoInformante.ApyN + ".");
            }

            LogRegistrar((int)LogEventoEnum.ReasignacionMedicoInformante, logByTurno, null);
        }

        /// <summary>
        /// Reasigno el medicoId tecnico del turno
        /// </summary>
        /// <param name="turnoIDs">IDs de los Turnos a Reasignar</param>
        /// <param name="medicoDestino">Nuevo Medico</param>
        public virtual void ReasignarTecnicos(IList<int> turnoIDs, Medico medicoDestino)
        {
            var query = from pt in dalEngine.Query<PracticaTurno>()
                        where turnoIDs.Contains(pt.TurnoId)
                        select pt;

            EntityCollection<PracticaTurno> practicasTurno = query.ToEntityCollection();
            Context.Session.Dalc.UpdateBatch<PracticaTurno>(practicasTurno, PracticaTurno.Properties.MedicoTecnico, medicoDestino);
        }

        /// <summary>
        /// Reasigno el medicoId tecnico del turno
        /// </summary>
        /// <param name="turnoIDs">IDs de los Turnos a Reasignar</param>
        /// <param name="medicoTecnicoDestino">Nuevo Medico</param>
        public virtual void ReasignarTecnicosYActuante(List<int> turnoIDs, Medico medicoTecnicoDestino, Medico medicoActuanteDestino)
        {
            EntityCollection<PracticaTurno> practicasTurno = new EntityCollection<PracticaTurno>();

            List<List<int>> turIdsMenor1000 = LinqInClause.SplitIntoBucketsForOracle(turnoIDs);
            foreach (List<int> turIds in turIdsMenor1000)
            {
                var query = from pt in dalEngine.Query<PracticaTurno>()
                            where turnoIDs.Contains(pt.TurnoId)
                            select pt;

                EntityCollection<PracticaTurno> ptsTMP = query.ToEntityCollection();
                if (ptsTMP != null && ptsTMP.Count > 0)
                    practicasTurno.AddRange(ptsTMP);
            }

            foreach (PracticaTurno pt in practicasTurno)
            {
                pt.MedicoTecnico = medicoTecnicoDestino;
                pt.Medico = medicoActuanteDestino;
            }

            Context.Session.Dalc.UpdateCollection(practicasTurno);
        }


        // TurnoHuerfano

        public EntityCollection<TurnoHuerfanoView> TurnoHuerfanoViewRead(DateTime? fechaDesde, DateTime? fechaHasta, String medico, String paciente, int centro,
            int servicio, String obraSocial, String equipo)
        {
            bool primerParametro = true;

            Filter filter = new Filter();
            if (fechaDesde.HasValue == true)
            {
                filter.Add(TurnoHuerfanoView.Properties.Fecha, ">=", fechaDesde.Value.Date);
                primerParametro = false;
            }

            if (fechaHasta.HasValue == true)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.Fecha, "<", fechaHasta.Value.Date.AddDays(1));
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.Fecha, "<", fechaHasta.Value.Date.AddDays(1));
            }

            if (String.IsNullOrEmpty(medico) == false)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.Medico, "LIKE", "%" + medico.Trim().Replace(' ', '%') + "%");
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.Medico, "LIKE", "%" + medico.Trim().Replace(' ', '%') + "%");

            }

            if (String.IsNullOrEmpty(paciente) == false)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.Paciente, "LIKE", "%" + paciente.Trim().Replace(' ', '%') + "%");
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.Paciente, "LIKE", "%" + paciente.Trim().Replace(' ', '%') + "%");
            }

            if (String.IsNullOrEmpty(obraSocial) == false)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.ObraSocial, "LIKE", "%" + obraSocial + "%");
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.ObraSocial, "LIKE", "%" + obraSocial + "%");
            }

            if (servicio > 0)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.ServicioId, "=", servicio);
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.ServicioId, "=", servicio);
            }

            if (centro > 0)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.CentroId, "=", centro);
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.CentroId, "=", centro);
            }

            if (String.IsNullOrEmpty(equipo) == false)
            {
                if (primerParametro == true)
                {
                    filter.Add(TurnoHuerfanoView.Properties.Equipo, "LIKE", "%" + equipo + "%");
                    primerParametro = false;
                }
                else
                    filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.Equipo, "LIKE", "%" + equipo + "%");
            }

            Sort sort = new Sort();
            sort.Add(TurnoHuerfanoView.Properties.Fecha, SortingDirection.Asc);

            ReadManyCommand<TurnoHuerfanoView> readCmd = new ReadManyCommand<TurnoHuerfanoView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            return readCmd.Execute();
        }

        /// <summary>
        /// Trae el listado de turnosIds huerfanos entre las fechas
        /// </summary>
        /// <param name="fechaDesde">La fecha de inicio</param>
        /// <param name="fechaHasta">La fecha de finalización</param>
        /// <returns>Lista de turnosIds</returns>
        [Private]
        public EntityCollection<TurnoHuerfanoView> TurnoHuerfanoViewReadByPeriodo(DateTime fechaDesde, DateTime fechaHasta)
        {
            Filter filter = new Filter();

            filter.Add(TurnoHuerfanoView.Properties.Fecha, ">=", fechaDesde);

            filter.Add(BooleanOp.And, TurnoHuerfanoView.Properties.Fecha, "<=", fechaHasta);

            Sort sort = new Sort();
            sort.Add(TurnoHuerfanoView.Properties.Fecha, SortingDirection.Asc);

            ReadManyCommand<TurnoHuerfanoView> readCmd = new ReadManyCommand<TurnoHuerfanoView>(dalEngine);
            readCmd.Filter = filter;
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<TurnoHuerfano> TurnoHuerfanoReadByIds(List<int> turnoIds)
        {
            if (turnoIds.Count < 1)
                return new EntityCollection<TurnoHuerfano>();

            string hql = "SELECT new enfoke.Eges.Entities.Results.TurnoHuerfano(pt.Turno.Id, " +
                                                                               "pt.Turno.Orden.Paciente.Id, " +
                                                                               "pt.Turno.Orden.Paciente.ApellidoNombre, " +
                                                                               "pt.Turno.Fecha, " +
                                                                               "pt.Turno.Duracion, " +
                                                                               "pt.Practica.Id, " +
                                                                               "pt.Practica.Name, " +
                                                                               "pt.Practica.ServicioEspecialidad.Servicio.Id, " +
                                                                               "pt.Practica.ServicioEspecialidad.Servicio.Name, " +
                                                                               "pt.Turno.Equipo.Sucursal.Id, " +
                                                                               "pt.Turno.Equipo.Sucursal.Name, " +
                                                                               "pt.Turno.Orden.ObraSocialPlan.ObraSocial.Id, " +
                                                                               "pt.Turno.Orden.ObraSocialPlan.ObraSocial.Name, " +
                                                                               "pt.Turno.Orden.ObraSocialPlan.Id, " +
                                                                               "pt.Turno.Orden.ObraSocialPlan.Name, " +
                                                                               "tt.Id, " +
                                                                               "tt.Name, " +
                                                                               "pt.Turno.Equipo.Id, " +
                                                                               "pt.Turno.Equipo.Descripcion, " +
                                                                               "pt.Medico.Id, " +
                                                                               "pt.Medico.Name, " +
                                                                               "pt.Medico.Apellido, " +
                                                                               "mi.Id, " +
                                                                               "mi.Name, " +
                                                                               "mi.Apellido, " +
                                                                               "mt.Id, " +
                                                                               "mt.Name, " +
                                                                               "mt.Apellido) " +
                "FROM TipoTurno tt, PracticaTurnoHQL pt " +
                "LEFT JOIN pt.MedicoTecnico mt " +
                "LEFT JOIN pt.MedicoInformante mi " +
                "WHERE pt.Tipo.Id = :tipo " +
                "AND pt.Turno.TipoTurno = tt.Id " +
                "AND pt.Cantidad > 0 " +
                "AND pt.Turno.Id IN (:idsTurnos) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("tipo", PracticaTurnoTipoEnum.Principal);
            query.SetParameterList("idsTurnos", turnoIds);

            EntityCollection<TurnoHuerfano> turnoHuerfanos = dalEngine.GetManyByQuery<TurnoHuerfano>(query);

            return turnoHuerfanos;
        }



        // GrupoEstudios
        public EntityCollection<GrupoEstudios> GrupoEstudiosReadHabilitados()
        {
            return GrupoEstudiosReadAll(true);
        }
        public EntityCollection<GrupoEstudios> GrupoEstudiosReadAll()
        {
            return GrupoEstudiosReadAll(false);
        }
        public EntityCollection<GrupoEstudios> GrupoEstudiosReadAll(bool soloHabilitados)
        {
            if (soloHabilitados)
                return dalEngine.GetManyByProperty<GrupoEstudios>(GrupoEstudios.Properties.Habilitado, true, GrupoEstudios.Properties.Name);
            return dalEngine.GetAll<GrupoEstudios>(GrupoEstudios.Properties.Name);
        }

        [AnonymousMethod()]
        public GrupoEstudios GrupoEstudiosReadById(int id)
        {
            return dalEngine.GetById<GrupoEstudios>(id);
        }

        [RequiresTransaction]
        public virtual GrupoEstudios GrupoEstudiosUpdate(GrupoEstudios ge, EntityCollection<EstudioAnterior> estudios)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            ge.UpdateUser = user.Id;
            ge.UpdateDate = enfoke.Time.Now;


            // Actualizo el Grupo
            ge = dalEngine.Update<GrupoEstudios>(ge);

            // Elimino los Estudios existentes

            EntityCollection<GrupoEstudiosEstudioAnterior> existentes = dalEngine.GetManyByProperty<GrupoEstudiosEstudioAnterior>(GrupoEstudiosEstudioAnterior.Properties.GrupoEstudios.Id, ge.Id);


            dalEngine.Delete(existentes);

            // Inserto los nuevos Estudios
            EntityCollection<GrupoEstudiosEstudioAnterior> nuevos = new EntityCollection<GrupoEstudiosEstudioAnterior>();
            foreach (EstudioAnterior ea in estudios)
            {
                GrupoEstudiosEstudioAnterior geea = new GrupoEstudiosEstudioAnterior();
                geea.GrupoEstudios = ge;
                geea.EstudioAnterior = ea;

                nuevos.Add(geea);
            }

            nuevos = dalEngine.UpdateCollection<GrupoEstudiosEstudioAnterior>(nuevos);

            return ge;
        }


        // EstudioAnterior
        public EntityCollection<EstudioAnterior> EstudioAnteriorReadAll()
        {
            return dalEngine.GetAll<EstudioAnterior>(EstudioAnterior.Properties.Name);
        }

        [AnonymousMethod()]
        public EntityCollection<EstudioAnterior> EstudioAnteriorReadByGrupoEstudios(int grupoID)
        {
            string hql = "select distinct geea.EstudioAnterior "
                            + "from GrupoEstudiosEstudioAnterior geea "
                            + "where geea.GrupoEstudios.Id = :grupoId "
                            + "order by geea.EstudioAnterior.Name asc";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("grupoId", grupoID);

            return dalEngine.GetManyByQuery<EstudioAnterior>(query);
        }






        public EstudioAnterior EstudioAnteriorUpdate(EstudioAnterior ea)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            ea.UpdateUser = user.Id;
            ea.UpdateDate = enfoke.Time.Now;


            return dalEngine.Update<EstudioAnterior>(ea);
        }

        public void EstudioAnteriorUpdateMany(EntityCollection<EstudioAnterior> eas)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            foreach (EstudioAnterior ea in eas)
            {
                ea.UpdateUser = user.Id;
                ea.UpdateDate = enfoke.Time.Now;
            }


            dalEngine.UpdateCollection<EstudioAnterior>(eas);
        }


        // TurnoEstudiosView
        public EntityCollection<TurnoEstudiosView> TurnoEstudiosViewRead(DateTime? fecha, string paciente, string medico, TurnoEstudiosEstadosEnum? estado, List<int> centrosIds, string servicio)
        {
            string strPaciente = paciente.Trim().Replace(" ", "%") + "%";
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT new enfoke.Eges.Entities.Results.TurnoEstudiosView(te.Autorizado, ");
            sb.Append("et.Name, ");
            sb.Append("t.Estado.Id, ");
            sb.Append("t.Fecha, ");
            sb.Append("tl.ReservaFecha, ");
            sb.Append("te.FechaAutorizacion, ");
            sb.Append("te.FechaDevolucion, ");
            sb.Append("te.FechaEnvioMedico, ");
            sb.Append("te.FechaEnvioRecepcion, ");
            sb.Append("te.FechaRecepcion, ");
            sb.Append("te.GrupoEstudios.Id, ");
            sb.Append("t.Id, ");
            sb.Append("til.EntregadoFecha, ");
            sb.Append("med.Apellido, ");
            sb.Append("med.Name, ");
            sb.Append("med.Id, ");
            sb.Append("te.ObservacionesMedico, ");
            sb.Append("pac.ApellidoNombre, ");
            sb.Append("pra.Name, ");
            sb.Append("t.TipoTurno.Id, ");
            sb.Append("te.Id) ");
            sb.Append("FROM TurnoHQL t ");
            sb.Append("LEFT JOIN t.TurnoInforme ti ");
            sb.Append("LEFT JOIN ti.TurnoInformeLogs til ");
            sb.Append(", TurnoEstudios te, TurnoLog tl, Paciente pac, PracticaTurno pt, ");
            sb.Append("Medico med, EstadoTurno et, Practica pra ");
            if (centrosIds.Count > 0)
                sb.Append(", Equipo eq ");
            sb.Append("WHERE te.Id = t.Orden.TurnoEstudiosID AND tl.TurnoId = t.Id AND pac.Id = t.Orden.Paciente.Id ");
            if (centrosIds.Count > 0)
                sb.Append("AND t.Equipo.Id = eq.Id ");
            sb.Append("AND pt.TurnoId = t.Id AND med.Id = te.Medico AND et.Id = t.Estado.Id ");
            sb.Append("AND pra.Id = pt.Practica ");
            sb.Append("AND t.Orden.TurnoEstudiosID IS NOT NULL ");
            sb.Append("AND pt.Tipo = :ptTipo ");
            sb.Append("AND t.Estado.Id != :estadoTurno ");

            if (estado.HasValue)
            {
                switch (estado.Value)
                {
                    case TurnoEstudiosEstadosEnum.Pendiente:
                        sb.Append("AND te.FechaRecepcion IS NULL ");
                        break;
                    case TurnoEstudiosEstadosEnum.Recibido:
                        sb.Append("AND te.FechaRecepcion IS NOT NULL ");
                        break;
                    case TurnoEstudiosEstadosEnum.EnviadoMedico:
                        sb.Append("AND te.FechaEnvioMedico IS NOT NULL ");
                        break;
                    case TurnoEstudiosEstadosEnum.Evaluado:
                        sb.Append("AND te.Autorizado IS NOT NULL ");
                        break;
                    case TurnoEstudiosEstadosEnum.EnviadoRecepcion:
                        sb.Append("AND te.FechaEnvioRecepcion IS NOT NULL ");
                        break;
                    case TurnoEstudiosEstadosEnum.Devuelto:
                        sb.Append("AND te.FechaDevolucion IS NOT NULL ");
                        break;
                }
            }

            if (fecha.HasValue)
            {
                sb.Append("AND ((t.Fecha IS NOT NULL AND t.Fecha BETWEEN :desde AND :hasta) OR ");
                sb.Append("(t.Fecha IS NULL AND tl.ReservaFecha BETWEEN :desde AND :hasta)) ");
            }

            if (!string.IsNullOrEmpty(paciente.Trim()))
                sb.Append("AND pac.ApellidoNombre LIKE :paciente ");

            string medicoApellido = string.Empty;
            string medicoNombre = string.Empty;

            if (!String.IsNullOrEmpty(medico))
            {
                sb.AppendFormat(" and med.{0} like :medicoApellido ", Medico.Properties.Apellido.Name);

                if (medico.Trim().Contains(" "))
                {
                    medicoApellido = medico.Trim().Split(' ')[0];
                    medicoNombre = medico.Trim().Split(' ')[1];

                    sb.AppendFormat(" and med.{0} like :medicoNombre ", Medico.Properties.Name.Name);
                }
                else
                {
                    medicoApellido = medico.Trim();
                }
            }

            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    sb.Append(" AND (eq.Sucursal.Id = :sucursalId) ");
                else
                    sb.Append(" AND (eq.Sucursal.Id in (:sucursalId)) ");
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                sb.Append(" AND (pra.ServicioEspecialidad.Servicio.Name LIKE :servicioDesc) ");
            }

            IQuery query = dalEngine.CreateQuery(sb.ToString());
            query.SetParameter("estadoTurno", EstadoTurnoEnum.Cancelado);
            query.SetParameter("ptTipo", PracticaTurnoTipoEnum.Principal);

            if (fecha.HasValue)
            {
                query.SetParameter("desde", fecha.Value.Date);
                query.SetParameter("hasta", fecha.Value.Date.AddDays(1));
            }

            if (!string.IsNullOrEmpty(paciente.Trim()))
                query.SetParameter("paciente", strPaciente);


            if (!String.IsNullOrEmpty(medicoApellido))
            {
                query.SetString("medicoApellido", medicoApellido.Trim().Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(medicoNombre))
            {
                query.SetString("medicoNombre", medicoNombre.Trim().Replace(" ", "%") + "%");
            }

            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    query.SetInt32("sucursalId", centrosIds[0]);
                else
                    query.SetParameterList("sucursalId", centrosIds);
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                query.SetString("servicioDesc", servicio + '%');
            }

            EntityCollection<TurnoEstudiosView> datos = dalEngine.GetManyByQuery<TurnoEstudiosView>(query);

            datos.Sort(delegate(TurnoEstudiosView d1, TurnoEstudiosView d2) { return d1.Fecha.CompareTo(d2.Fecha); });

            return datos;
        }


        // TurnoEstudios

        private TurnoEstudios TurnoEstudiosUpdate(TurnoEstudios te)
        {
            te = dalEngine.Update<TurnoEstudios>(te);
            return te;
        }

        [AnonymousMethod()]
        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosRecibirEstudios(int turnoID, TurnoEstudios te, EntityCollection<TurnoEstudiosEstudioAnterior> TEEAs)
        {
            // Asigno la Fecha y Usuario de Recepcion
            SecurityUser user = Security.Current.UserInfo.User;
            te.FechaRecepcion = enfoke.Time.Now;
            te.UsuarioRecepcion = user;

            // Guardo el TurnoEstudios
            te = TurnoEstudiosUpdate(te);

            // Elimino los TEEAs existentes [por las modificaciones]

            EntityCollection<TurnoEstudiosEstudioAnterior> TEEAsExistenes = TurnoEstudiosEstudioAnteriorReadByTurnoEstudios(te.Id);
            dalEngine.Delete(TEEAsExistenes);

            // Guardo los TEEAs
            for (int i = 0; i < TEEAs.Count; i++)
                TEEAs[i].TurnoEstudios = te;


            TEEAs = dalEngine.UpdateCollection<TurnoEstudiosEstudioAnterior>(TEEAs);

            // Actualizo la orden
            Orden orden = OrdenReadByTurnoId(turnoID);
            OrdenUpdateEstudios ordenEstudios = new OrdenUpdateEstudios(orden.Id, te.Id);
            dalEngine.Update<OrdenUpdateEstudios>(ordenEstudios);

            // Actualizo el Log
            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se recibieron estudios anteriores.", orden);

            return te;
        }

        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosDevolverEstudios(int turnoID, TurnoEstudios te)
        {
            te = TurnoEstudiosUpdate(te);

            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se devolvieron los estudios anteriores.", turnoID);

            return te;
        }

        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosRevertirDevolverEstudios(TurnoEstudios te)
        {
            te = TurnoEstudiosUpdate(te);
            Turno turno = dalEngine.GetByProperty<Turno>(Turno.Properties.Orden.TurnoEstudiosID, te.Id);

            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se revirtio la devolución de los estudios anteriores.", (turno != null) ? (int?)turno.Id : (int?)null);

            return te;
        }

        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosAutorizarProvisorio(int turnoID, TurnoEstudios te, bool autorizado)
        {
            te = TurnoEstudiosUpdate(te);

            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se " + (autorizado ? "autorizó" : "rechazó") + " el provisorio.", turnoID);

            return te;
        }
        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosEnviarMedico(int turnoID, TurnoEstudios te)
        {
            te = TurnoEstudiosUpdate(te);

            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se enviaron los estudios anteriores al médico.", turnoID);

            return te;
        }
        [RequiresTransaction]
        public virtual TurnoEstudios TurnoEstudiosEnviarRecepcion(int turnoID, TurnoEstudios te)
        {
            te = TurnoEstudiosUpdate(te);

            LogRegistrar((int)LogEventoEnum.EstudiosAnteriores, "Se enviaron los estudios anteriores a recepción.", turnoID);

            return te;
        }

        [AnonymousMethod()]
        public EntityCollection<TurnoEstudiosEstudioAnterior> TurnoEstudiosEstudioAnteriorReadByTurnoEstudios(int turnoEstudiosID)
        {
            return dalEngine.GetManyByProperty<TurnoEstudiosEstudioAnterior>(TurnoEstudiosEstudioAnterior.Properties.TurnoEstudios.Id, turnoEstudiosID);
        }
        [RequiresTransaction]
        public virtual void CreateOrdenEstudios(int ordenId, TurnoEstudios te)
        {
            // Guardo el TurnoEstudios
            te = TurnoEstudiosUpdate(te);

            // Actualizo el Turno
            OrdenUpdateEstudios orden = new OrdenUpdateEstudios(ordenId, te.Id);


            orden = dalEngine.Update<OrdenUpdateEstudios>(orden);
        }


        // TurnoEntrevista
        public TurnoEntrevista TurnoEntrevistaGetByAnyTurno(int turnoID)
        {

            ReadManyCommand<TurnoEntrevista> readCmd = new ReadManyCommand<TurnoEntrevista>(dalEngine);

            Filter filter = new Filter();

            filter.Add(TurnoEntrevista.Properties.TurnoPrincipal, "=", turnoID);
            filter.Add(BooleanOp.Or, TurnoEntrevista.Properties.TurnoEntrevista, "=", turnoID);

            readCmd.Filter = filter;

            EntityCollection<TurnoEntrevista> turnoEntrevista = readCmd.Execute();

            if (turnoEntrevista.Count > 0)
                return turnoEntrevista[0];
            else
                return null;

        }

        public TurnoEntrevista TurnoEntrevistaGetById(int turnoEntrevistaID)
        {
            return (turnoEntrevistaID > 0) ? dalEngine.GetById<TurnoEntrevista>(turnoEntrevistaID) : null;
        }
        public void TurnoEntrevistaUpdate(TurnoEntrevista turnoEntrevista)
        {
            dalEngine.Update(turnoEntrevista);
        }
        [RequiresTransaction]
        public virtual void TurnoEntrevistaValorUpdateAll(int turnoEntrevistaId, EntityCollection<TurnoEntrevistaValor> turnoEntrevistaValores)
        {
            // Borra los pre-existentes
            EntityCollection<TurnoEntrevistaValor> preexistentes =
                            dalEngine.GetManyByProperty<TurnoEntrevistaValor>(
                            TurnoEntrevistaValor.Properties.TurnoEntrevistaId,
                                turnoEntrevistaId);
            dalEngine.Delete(preexistentes);
            // Guarda lo recibido
            foreach (TurnoEntrevistaValor tev in turnoEntrevistaValores)
                tev.TurnoEntrevistaId = turnoEntrevistaId;
            dalEngine.UpdateCollection(turnoEntrevistaValores);
        }



        // TurnoSeleccionView
        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByPaciente(int pacienteID, List<int> centrosIds)
        {
            StringBuilder hql = new StringBuilder("from TurnoSeleccionView t ");
            hql.Append("where t.PacienteID = :PacienteID ");
            hql.Append("and (t.EstadoID != :Cancelado OR ");
            hql.Append("      (t.EstadoID = :Cancelado AND t.EstadoMotivoID != :Reprogramacion)) ");
            hql.Append("and t.TipoTurnoID != :presupuesto ");
            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    hql.Append("and t.CentroId is not null and t.CentroId = :centrosIds ");
                else
                    hql.Append("and t.CentroId is not null and t.CentroId in (:centrosIds) ");
            }
            hql.Append(" order by t.Fecha desc, t.Practica asc");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("PacienteID", pacienteID);
            query.SetInt32("presupuesto", (int)TipoTurnoEnum.Presupuesto);
            query.SetInt32("Cancelado", (int)EstadoTurnoEnum.Cancelado);
            query.SetInt32("Reprogramacion", (int)EstadoTurnoMotivoEnum.Reprogramacion);
            if (centrosIds.Count > 0)
            {
                if (centrosIds.Count == 1)
                    query.SetInt32("centrosIds", centrosIds[0]);
                else
                    query.SetParameterList("centrosIds", centrosIds);
            }
            // Hace la consulta hql
            return dalEngine.GetManyByQuery<TurnoSeleccionView>(query);
        }

        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByIdTurnoWithTurnosCombos(int idTurno)
        {
            string hql = "SELECT tur FROM Turno tur JOIN Turno t WHERE t.ComboID = tur.ComboID AND t.Id = :idTurno";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);

            return dalEngine.GetManyByQuery<TurnoSeleccionView>(query);
        }

        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByIdCombo(int idCombo)
        {
            return dalEngine.GetManyByProperty<TurnoSeleccionView>(TurnoSeleccionView.Properties.ComboID, idCombo, TurnoSeleccionView.Properties.Practica);
        }
        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByIdOrdenMultiple(int OrdenMultipleId)
        {
            return dalEngine.GetManyByProperty<TurnoSeleccionView>(TurnoSeleccionView.Properties.OrdenId, OrdenMultipleId, TurnoSeleccionView.Properties.Fecha, SortOrder.Descending);
        }

        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByTurnoIds(List<int> turnosId)
        {
            if (turnosId.Count == 0)
                return new EntityCollection<TurnoSeleccionView>();

            string hql = "select tv from TurnoSeleccionView tv WHERE tv.Id IN (:ids) ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("ids", turnosId);
            return dalEngine.GetManyByQuery<TurnoSeleccionView>(query);

        }

        /// <summary>
        /// Busco todos los turnosIds asociados a un nro de presupuesto [en la valorizacion]
        /// </summary>
        /// <param name="presupuesto">Nro de Presupuesto</param>
        /// <returns>Turnos asociados al Presupuesto</returns>
        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByPresupuesto(int presupuesto)
        {
            string hql = "select distinct t.Id from Valorizacion v JOIN v.Turno t "
                        + "WHERE v.NroPresupuesto = :presupuesto AND v.Deleted = false "
                        + "AND t.Activo = true AND t.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("presupuesto", presupuesto);

            List<int> turnoIDs = new List<int>(query.List<int>());

            if (turnoIDs.Count > 0)
            {
                hql = "select tv from TurnoSeleccionView tv WHERE tv.Id IN (:ids) ";
                query = dalEngine.CreateQuery(hql);
                query.SetParameterList("ids", turnoIDs);

                return dalEngine.GetManyByQuery<TurnoSeleccionView>(query);
            }
            else
                return new EntityCollection<TurnoSeleccionView>();
        }


        // EncuestaPregunta
        public EntityCollection<EncuestaPregunta> EncuestaPreguntaReadAll()
        {
            return EncuestaPreguntaReadAll(false);
        }

        public EntityCollection<EncuestaPregunta> EncuestaPreguntaReadAll(bool soloVigentes)
        {
            if (soloVigentes)
                return dalEngine.GetManyByProperty<EncuestaPregunta>(EncuestaPregunta.Properties.Eliminada, false, EncuestaPregunta.Properties.Descripcion);
            return dalEngine.GetAll<EncuestaPregunta>(EncuestaPregunta.Properties.Descripcion);
        }

        public EntityCollection<EncuestaPregunta> EncuestaPreguntaUpdateMany(EntityCollection<EncuestaPregunta> EPs)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            foreach (EncuestaPregunta ep in EPs)
            {
                ep.UpdateUser = user.Id;
                ep.UpdateDate = enfoke.Time.Now;
            }


            return dalEngine.UpdateCollection<EncuestaPregunta>(EPs);
        }

        internal EntityCollection<EncuestaPregunta> EncuestaPreguntaReadVigentesByServicio(int servicioID)
        {
            string hql = "SELECT eps.EncuestaPregunta FROM EncuestaPreguntaServicio eps " +
                "WHERE eps.ServicioID = :idServicio AND eps.EncuestaPregunta.Eliminada = false ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idServicio", servicioID);

            return dalEngine.GetManyByQuery<EncuestaPregunta>(query);
        }


        // EncuestaRespuesta
        public EntityCollection<EncuestaRespuesta> EncuestaRespuestaReadByPregunta(int preguntaID)
        {
            return EncuestaRespuestaReadByPregunta(preguntaID, false);
        }

        private EntityCollection<EncuestaRespuesta> EncuestaRespuestaReadByPregunta(int preguntaID, bool soloVigentes)
        {
            ReadManyCommand<EncuestaRespuesta> readCmd = new ReadManyCommand<EncuestaRespuesta>(dalEngine);

            readCmd.Filter = new Filter();

            // Filtro por el Pregunta
            readCmd.Filter = new Filter();
            readCmd.Filter.Add(EncuestaRespuesta.Properties.EncuestaPreguntaID, "=", preguntaID);

            // Filtro las Eliminadas
            if (soloVigentes)
                readCmd.Filter.Add(BooleanOp.And, EncuestaRespuesta.Properties.Eliminada, "=", false);

            // Ordeno por Orden y Descripcion
            readCmd.Sort = new Sort();
            readCmd.Sort.Add(EncuestaRespuesta.Properties.Orden, SortingDirection.Asc);
            readCmd.Sort.Add(EncuestaRespuesta.Properties.Descripcion, SortingDirection.Asc);

            return readCmd.Execute();
        }

        public EntityCollection<EncuestaRespuesta> EncuestaRespuestaUpdateMany(EntityCollection<EncuestaRespuesta> ERs, int preguntaID)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            foreach (EncuestaRespuesta er in ERs)
            {
                er.EncuestaPreguntaID = preguntaID;

                er.UpdateUser = user.Id;
                er.UpdateDate = enfoke.Time.Now;
            }


            return dalEngine.UpdateCollection<EncuestaRespuesta>(ERs);
        }


        // OrdenEncuesta

        [RequiresTransaction]
        public virtual OrdenEncuesta OrdenEncuestaUpdateWithRespuestas(OrdenEncuesta et)
        {
            bool nueva = et.Id == 0;

            // Audito
            SecurityUser user = Security.Current.UserInfo.User;
            et.UpdateUser = user.Id;
            et.UpdateDate = enfoke.Time.Now;


            // Guardo
            et = dalEngine.Update<OrdenEncuesta>(et);

            if (et.Respuestas.Count > 0)
            {
                // Audito
                foreach (OrdenEncuestaRespuesta etr in et.Respuestas)
                {
                    if (nueva)
                        etr.OrdenEncuestaID = et.Id;

                    if (nueva || etr.Changed)
                    {
                        etr.UpdateUser = user.Id;
                        etr.UpdateDate = enfoke.Time.Now;
                    }
                }

                // Guardo
                et.Respuestas = dalEngine.UpdateCollection<OrdenEncuestaRespuesta>(et.Respuestas);
            }

            return et;
        }

        public OrdenEncuesta OrdenEncuestaReadByProtocolo(string protocolo)
        {
            // Obtengo la Orden
            Orden orden = OrdenReadByProtocolo(protocolo);

            // Obtengo la Encuesta del Turno
            OrdenEncuesta et = OrdenEncuestaReadByOrden(orden.Id);

            if (et != null)
            {
                // Obtengo las Respuestas
                et.Respuestas = OrdenEncuestaRespuestaReadByOrdenEncuesta(et.Id);

                // Obtengo las Respuestas de cada Pregunta
                foreach (OrdenEncuestaRespuesta etr in et.Respuestas)
                    etr.EncuestaPregunta.Respuestas = EncuestaRespuestaReadByPregunta(etr.EncuestaPregunta.Id, true);
            }

            return et;
        }

        private OrdenEncuesta OrdenEncuestaReadByOrden(int ordenID)
        {
            EntityCollection<OrdenEncuesta> col = dalEngine.GetManyByProperty<OrdenEncuesta>(OrdenEncuesta.Properties.OrdenID, ordenID);

            if (col.Count > 0)
                return col[0];
            else
                return null;
        }


        // EncuestaPreguntaServicio
        [RequiresTransaction]
        public virtual void EncuestaPreguntaServicioUpdateServicio(int servicioID, EntityCollection<EncuestaPreguntaServicio> preguntas)
        {
            // Obtengo las Existentes
            EntityCollection<EncuestaPreguntaServicio> preguntasDB = dalEngine.GetManyByProperty<EncuestaPreguntaServicio>(EncuestaPreguntaServicio.Properties.ServicioID, servicioID);

            // Elimino las Existentes
            if (preguntasDB.Count > 0)
            {
                dalEngine.Delete(preguntasDB);
            }

            // Inserto las Nuevas
            if (preguntas.Count > 0)
            {
                SecurityUser user = Security.Current.UserInfo.User;
                foreach (EncuestaPreguntaServicio eps in preguntas)
                {
                    eps.UpdateUser = user.Id;
                    eps.UpdateDate = enfoke.Time.Now;
                }


                preguntas = dalEngine.UpdateCollection<EncuestaPreguntaServicio>(preguntas);
            }
        }

        public EntityCollection<EncuestaPreguntaServicio> EncuestaPreguntaServicioReadVigentesByServicio(int servicioID)
        {
            string hql = "FROM EncuestaPreguntaServicio eps " +
                "WHERE eps.ServicioID = :idServicio " +
                "AND eps.EncuestaPregunta.Eliminada = false " +
                "ORDER BY eps.Orden ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idServicio", servicioID);

            return dalEngine.GetManyByQuery<EncuestaPreguntaServicio>(query);
        }

        public EntityCollection<EncuestaPreguntaServicio> EncuestaPreguntaServicioReadVigentesByOrdenID(int ordenID)
        {
            string hql = "SELECT eps " +
                "FROM EncuestaPreguntaServicio eps, TurnoHQL t " +
                "WHERE t.Equipo.Servicio.Id = eps.ServicioID " +
                "AND eps.EncuestaPregunta.Eliminada = false " +
                "AND t.Orden.Id = :idOrden " +
                "ORDER BY eps.Orden ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idOrden", ordenID);

            return dalEngine.GetManyByQuery<EncuestaPreguntaServicio>(query);
        }


        // OrdenEncuestaRespuesta
        private EntityCollection<OrdenEncuestaRespuesta> OrdenEncuestaRespuestaReadByOrdenEncuesta(int encuestaID)
        {
            return dalEngine.GetManyByProperty<OrdenEncuestaRespuesta>(OrdenEncuestaRespuesta.Properties.OrdenEncuestaID, encuestaID);
        }


        // TurnoBloqueoAdmision

        [Private]
        public TurnoBloqueoAdmision TurnoBloqueoAdmisionReadByTurno(int turnoID)
        {
            List<int> turnosIds = new List<int>();
            turnosIds.Add(turnoID);

            EntityCollection<TurnoBloqueoAdmision> col = TurnoBloqueoAdmisionReadByTurno(turnosIds);

            if (col != null && col.Count > 0)
                return col[0];

            return null;
        }

        [Private]
        public EntityCollection<TurnoBloqueoAdmision> TurnoBloqueoAdmisionReadByTurno(List<int> turnosIDs)
        {
            EntityCollection<TurnoBloqueoAdmision> ret = new EntityCollection<TurnoBloqueoAdmision>();
            EntityCollection<TurnoBloqueoAdmision> col = dalEngine.GetManyByPropertyList<TurnoBloqueoAdmision>(TurnoBloqueoAdmision.Properties.TurnoID, turnosIDs);
            col.SortByProperty(TurnoBloqueoAdmision.Properties.TurnoID);

            // Uno por turno
            int turnoAnterior = 0;
            foreach (TurnoBloqueoAdmision turnoBloqueoAdmision in col)
            {
                if (turnoBloqueoAdmision.TurnoID != turnoAnterior)
                {
                    ret.Add(turnoBloqueoAdmision);
                    turnoAnterior = turnoBloqueoAdmision.TurnoID;
                }
            }

            return ret;
        }





        [Private]
        public void TurnoBloqueoAdmisionDelete(int turnoID)
        {
            List<int> turnosIds = new List<int>();
            turnosIds.Add(turnoID);

            TurnoBloqueoAdmisionDelete(turnosIds);
        }
        [Private]
        public void TurnoBloqueoAdmisionDelete(List<int> turnosID)
        {
            EntityCollection<TurnoBloqueoAdmision> tbas = TurnoBloqueoAdmisionReadByTurno(turnosID);

            // Si existe el bloque para el turno, lo elimino
            if (tbas != null && tbas.Count > 0)
            {
                dalEngine.Delete(tbas);
            }
        }

        [Private]
        public void TurnoBloqueoAdmisionPurge()
        {
            ReadManyCommand<TurnoBloqueoAdmision> readCmd = new ReadManyCommand<TurnoBloqueoAdmision>(dalEngine);

            // Filtro por Fecha - Anterior a Ahora menos 1 dia (ayer)
            readCmd.Filter = new Filter();
            readCmd.Filter.Add(TurnoBloqueoAdmision.Properties.Fecha, "<", enfoke.Time.Now.AddDays(-1));

            EntityCollection<TurnoBloqueoAdmision> bloqueos = readCmd.Execute();

            if (bloqueos.Count > 0)
            {
                dalEngine.Delete(bloqueos);
            }
        }

        //  TurnoLog locker
        /// <summary>
        /// Este método puede ser llamado por transacciones que afecten a un turno para indicar que se está
        /// haciendo un proceso sobre los datos de ese turno. Al lockear el registro de TurnoLog de este turno
        /// las demás operaciones deben esperar evitando las probabilidades de deadlock.
        /// </summary>
        /// <param name="turnoID"></param>
        private TurnoLog TurnoLogLockByTurno(int turnoID)
        {
            // Obtengo el Log
            TurnoLog turnoLog = TurnoLogReadByTurno(turnoID);
            // Se asegura de trabajar en un registro sobre el que tiene derecho a trabajar.
            enfoke.Context.Data.Session.LockWithWait<TurnoLog>(turnoLog.Id, 3500);
            // Devuelve
            return turnoLog;
        }

        // Sistema de Colas
        public void TurnoLogSistemaColaUpdate(int turnoId, TipoDeColaEnum tipoCola, int? colaNumero, int? colaError, DateTime? fechaSolicitud, DateTime? fechaLlamado, bool? llegoTarde, bool? atencionDirecta)
        {
            List<int> turnosId = new List<int>();
            turnosId.Add(turnoId);

            Dictionary<int, bool?> llegaTardePorTurno = new Dictionary<int, bool?>();
            llegaTardePorTurno.Add(turnoId, llegoTarde);

            TurnoLogSistemaColaUpdate(turnosId, tipoCola, colaNumero, colaError, fechaSolicitud, fechaLlamado, llegaTardePorTurno, atencionDirecta);
        }
        [RequiresTransaction]
        public virtual void TurnoLogSistemaColaUpdate(List<int> turnosId, TipoDeColaEnum tipoCola, int? colaNumero, int? colaError, DateTime? fechaSolicitud, DateTime? fechaLlamado, Dictionary<int, bool?> llegaTardePorTurno, bool? atencionDirecta)
        {
            foreach (int turnoID in turnosId)
            {
                // Obtengo el Log
                TurnoLog turnoLog = TurnoLogLockByTurno(turnoID);

                switch (tipoCola)
                {
                    case TipoDeColaEnum.Entrega:
                        turnoLog.ColaEntregaNumero = colaNumero;
                        turnoLog.ColaEntregaErrorId = colaError;
                        turnoLog.FechaSolicitudColaEntrega = fechaSolicitud;
                        turnoLog.FechaLlamadaColaEntrega = fechaLlamado;
                        break;

                    case TipoDeColaEnum.Turno:
                        turnoLog.ColaTurnoNumero = colaNumero;
                        turnoLog.ColaTurnoErrorId = colaError;
                        turnoLog.FechaSolicitudColaTurno = fechaSolicitud;
                        turnoLog.FechaLlamadaColaTurno = fechaLlamado;
                        break;

                    case TipoDeColaEnum.SinTurno:
                        turnoLog.ColaSinTurnoNumero = colaNumero;
                        turnoLog.ColaSinTurnoErrorId = colaError;
                        turnoLog.FechaSolicitudColaSinTurno = fechaSolicitud;
                        turnoLog.FechaLlamadaColaSinTurno = fechaLlamado;
                        break;
                }

                turnoLog.LlegoTarde = (llegaTardePorTurno != null && llegaTardePorTurno.ContainsKey(turnoID)) ? (bool?)llegaTardePorTurno[turnoID] : (bool?)null;
                turnoLog.AtencionDirecta = atencionDirecta;

                int turnoId = turnosId.Find(delegate(int turnoDeColeccion) { return turnoDeColeccion == turnoLog.TurnoId; });
                ModificarTurnoLogDependiendoDelTipoControlFacturacion(turnoLog, turnoId);


                // Actualizo
                turnoLog = dalEngine.Update<TurnoLog>(turnoLog);
            }


        }

        /// <summary>
        /// Modifica los campos ControlFacturacionFecha y ControlFacturacionUsuario de acuerdo con el tipoControlFacturacion del turno.
        /// </summary>
        /// <param name="turnoLog">Turno log a modificar.</param>
        /// <param name="turnoId">Turno original.</param>
        [Private]
        public void ModificarTurnoLogDependiendoDelTipoControlFacturacion(TurnoLog turnoLog, int turnoId)
        {
            Turno turno = TurnoReadById(turnoId);
            ModificarTurnoLogDependiendoDelTipoControlFacturacion(turnoLog, turno);
        }

        /// <summary>
        /// Modifica los campos ControlFacturacionFecha y ControlFacturacionUsuario de acuerdo con el tipoControlFacturacion del turno.
        /// </summary>
        /// <param name="turnoLog">Turno log a modificar.</param>
        /// <param name="turnoId">Turno original.</param>
        [Private]
        public void ModificarTurnoLogDependiendoDelTipoControlFacturacion(TurnoLog turnoLog, Turno turno)
        {
            // Esto es porque necesito que en ese campo figure el facturista que controla la orden, 
            // que es el que dice que no se factura o el que lo prefactura. 
            // Como funciona ahora que los registros los actualiza siempre, 
            // me está quedando el usuario que realiza la factura, que es siempre el mismo.
            switch ((TipoControlFacturacionEnum)turno.TipoControlFacturacion.Id)
            {
                case TipoControlFacturacionEnum.NoControlado:
                case TipoControlFacturacionEnum.AFacturar:
                case TipoControlFacturacionEnum.Particular:
                    turnoLog.ControlFacturacionFecha = null;
                    turnoLog.ControlFacturacionUsuario = null;
                    break;
                case TipoControlFacturacionEnum.Facturado:
                    TurnoLog turnoLogAnterior = TurnoLogReadByTurno(turno.Id);
                    if (turnoLogAnterior != null)
                    {
                        turnoLog.ControlFacturacionFecha = turnoLogAnterior.ControlFacturacionFecha;
                        turnoLog.ControlFacturacionUsuario = turnoLogAnterior.ControlFacturacionUsuario;
                    }

                    break;
            }

        }

        [Private]
        [RequiresTransaction]
        public virtual void SistemaColaCacheItemClean(Sector sector, int turnoID, TipoDeColaEnum tipoCola)
        {
            TurnoLog turnoLog = TurnoLogReadByTurno(turnoID);
            int? colaNumero = turnoLog.ColaNumero(tipoCola);

            if (colaNumero.HasValue)
            {
                SistemaColaCache sistemaColaCache = ObtenerSistemaColaCache(sector, tipoCola, colaNumero.Value);

                if (sistemaColaCache != null)
                    dalEngine.Delete(sistemaColaCache);
            }

            TurnoLogSistemaColaUpdate(turnoID, tipoCola, null, null, null, null, null, null);
        }

        public SistemaColaCache SistemaColaCacheInsert(SistemaColaCache sistemaColaCache)
        {
            sistemaColaCache = dalEngine.Update<SistemaColaCache>(sistemaColaCache);
            return sistemaColaCache;
        }

        public SistemaColaCache ObtenerSistemaColaCache(Sector sector, TipoDeColaEnum tipoCola, int numero)
        {
            ReadManyCommand<SistemaColaCache> readCmd = new ReadManyCommand<SistemaColaCache>(dalEngine);

            Filter filter = new Filter();

            filter.Add(SistemaColaCache.Properties.Sector.Id, "=", sector.Id);

            filter.Add(BooleanOp.And, SistemaColaCache.Properties.DbTipoCola, "=", (int)tipoCola);

            filter.Add(BooleanOp.And, SistemaColaCache.Properties.Numero, "=", numero);

            readCmd.Filter = filter;

            EntityCollection<SistemaColaCache> col = readCmd.Execute();

            if (col.Count == 0)
                return null;
            else
                return col[0];
        }
        [RequiresTransaction]
        public virtual void SistemaColaCacheClean(int validezNumeroAtencion)
        {
            EntityCollection<SistemaColaCache> itemsToDelete = SistemaColaCacheVencidos(validezNumeroAtencion);


            dalEngine.Delete(itemsToDelete);
        }






        private EntityCollection<SistemaColaCache> SistemaColaCacheVencidos(int validezNumeroAtencion)
        {
            try
            {
                ReadManyCommand<SistemaColaCache> readCmd = new ReadManyCommand<SistemaColaCache>(dalEngine);

                readCmd.Filter = new Filter();
                readCmd.Filter.Add(SistemaColaCache.Properties.FechaSolicitud, "<", enfoke.Time.Now.AddMinutes(-validezNumeroAtencion));

                return readCmd.Execute();
            }
            catch (Exception e)
            {
                throw new Exception("Error al seleccionar los items de la cache del sistema de colas a elimiar.", e);
            }
        }












        // DebeOrdenMedica

        public EntityCollection<DebeOrdenMedica> DebeOrdenMedicaReadAll(bool soloSeleccionables)
        {
            if (soloSeleccionables)
            {
                Filter filter = new Filter();

                filter.Add(BooleanOp.And, DebeOrdenMedica.Properties.Id, ">", 0);

                return dalEngine.GetManyByFilter<DebeOrdenMedica>(filter);
            }
            else
                return dalEngine.GetAll<DebeOrdenMedica>();
        }

        public EntityCollection<DebeOrdenMedica> DebeOrdenMedicaByTurnos(List<int> turnosIDs)
        {
            string hql = "SELECT dom FROM Turno tur, DebeOrdenMedica dom WHERE tur.Orden.DebeOrdenMedicaId = dom.Id AND tur.Id IN (:turnosIds)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("turnosIds", turnosIDs);
            return dalEngine.GetManyByQuery<DebeOrdenMedica>(query);
        }


        // PracticaTurnoInsumo
        [AnonymousMethod()]
        public EntityCollection<PracticaTurnoInsumo> PracticaTurnoInsumoReadByTurnoID(int turnoID)
        {
            return dalEngine.GetManyByProperty<PracticaTurnoInsumo>(PracticaTurnoInsumo.Properties.PracticaTurno.TurnoId, turnoID);
        }

        public EntityCollection<PracticaTurnoInsumo> PracticaTurnoInsumoReadByPracticaTurnoID(int practicaTurnoID)
        {
            return dalEngine.GetManyByProperty<PracticaTurnoInsumo>(PracticaTurnoInsumo.Properties.PracticaTurno.Id, practicaTurnoID);
        }

        public void PracticaTurnoInsumoAdd(EntityCollection<PracticaTurnoInsumo> col)
        {
            PracticaTurnoInsumoUpdate(col);
        }

        public void PracticaTurnoInsumoUpdate(EntityCollection<PracticaTurnoInsumo> col)
        {
            dalEngine.UpdateCollection<PracticaTurnoInsumo>(col);
        }

        public void PracticaTurnoInsumoDelete(EntityCollection<PracticaTurnoInsumo> col)
        {
            dalEngine.Delete(col);
        }


        // Tipo Entrega Orden

        public EntityCollection<TipoEntregaOrden> TipoEntregaOrdenReadFacturable()
        {
            return dalEngine.GetManyByProperty<TipoEntregaOrden>(TipoEntregaOrden.Properties.EsFacturable, true);
        }

        public TipoEntregaOrdenEnum TipoEntregaOrdenEnumReadByOrdenId(int idOrden)
        {
            string hql = "SELECT o.dbEntregaOrdenId FROM Orden o WHERE o.Id = :idOrden ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idOrden", idOrden);

            object result = query.UniqueResult();

            if (result != null)
                return (TipoEntregaOrdenEnum)result;

            throw new Exception("Error al obtener el \"Tipo Entrega Orden\" [Orden ID: " + idOrden.ToString() + "] ");
        }



        // Orden

        public EntityCollection<Orden> OrdenReadByLoteId(int LoteId)
        {
            return dalEngine.GetManyByProperty<Orden>(Orden.Properties.LoteTraslado.Id, LoteId);
        }

        public void OrdenUpdatePaciente(int ordenId, int PacienteId)
        {
            Orden orden = dalEngine.GetById<Orden>(ordenId);
            orden.PacienteId = PacienteId;
            dalEngine.Update(orden);
        }

        public void OrdenUpdateDiagnostico(int ordenId, string diagnostivo)
        {
            OrdenUpdateDiagnostico orden = dalEngine.GetById<OrdenUpdateDiagnostico>(ordenId);
            orden.Diagnostico = diagnostivo;
            dalEngine.Update(orden);
        }

        public EntityCollection<Turno> TurnosReadByOrdenId(int ordenId)
        {
            return dalEngine.GetManyByProperty<Turno>(Turno.Properties.Orden.Id, ordenId);
        }

        public EntityCollection<Turno> TurnosNoCanceladosReadByOrdenId(int ordenId)
        {
            string hql = "select tur from Turno tur, EstadoTurno est where est.Id = tur.EstadoTurnoID and est.Cancelado = false and tur.Orden.Id = :omId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("omId", ordenId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<Turno> TurnosFacturablesReadByOrdenMultiple(int omId)
        {
            StringBuilder hql = new StringBuilder(" select tur from Turno tur, EstadoTurno est ");
            hql.Append(" where est.Id = tur.EstadoTurnoID ");
            hql.Append("   and est.Facturable = true ");
            hql.Append("   and tur.Orden.Id = :omId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("omId", omId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        [Private]
        public EntityCollection<VwOrdenMultipleList> OrdenListGet(OrdenEstadoEnum? estado, string practica, List<int> centrosIds, string obraSocial, string protocolo, string paciente)
        {
            string hql = "SELECT v FROM VwOrdenMultipleList v ";
            string condicion = String.Empty;

            if (estado.HasValue)
                condicion += " AND v.OrdenEstado = :estado";

            if (!String.IsNullOrEmpty(practica))
                condicion += " AND v.Practica LIKE :practica";

            if (centrosIds.Count > 0)
                condicion += " AND v.SucursalId in (:centro)";

            if (!String.IsNullOrEmpty(obraSocial))
                condicion += " AND v.ObraSocial LIKE :obraSocial";

            if (!String.IsNullOrEmpty(protocolo))
                condicion += " AND v.Protocolo LIKE :protocolo";

            if (!String.IsNullOrEmpty(paciente))
                condicion += " AND v.Paciente LIKE :paciente";

            if (condicion != String.Empty)
                hql += " WHERE " + condicion.Substring(4);

            IQuery query = dalEngine.CreateQuery(hql);

            if (estado.HasValue)
                query.SetInt32("estado", (int)estado.Value);

            if (!String.IsNullOrEmpty(practica))
                query.SetString("practica", practica + "%");

            if (centrosIds.Count > 0)
                query.SetParameterList("centro", centrosIds);

            if (!String.IsNullOrEmpty(obraSocial.Trim()))
                query.SetString("obraSocial", obraSocial + "%");

            if (!String.IsNullOrEmpty(protocolo.Trim()))
                query.SetString("protocolo", protocolo + "%");

            if (!String.IsNullOrEmpty(paciente.Trim()))
                query.SetString("paciente", paciente + "%");

            return dalEngine.GetManyByQuery<VwOrdenMultipleList>(query);
        }

        [Private]
        public void OrdenCancelar(int OrdenId)
        {
            Orden tom = dalEngine.GetById<Orden>(OrdenId);
            tom.Estado = (byte)OrdenEstadoEnum.Cancelada;
            dalEngine.Update<Orden>(tom);
        }



        // TurnoDocumentacion
        /// <summary>
        /// Devuelve toda la documentación relacionada con los turnosIds
        /// </summary>
        /// <param name="turnoId">Id de los turnosIds de referencia</param>
        /// <returns>Coleccion de TurnoDocumentacion</returns>
        public EntityCollection<TurnoDocumentacion> TurnoDocumentacionReadByTurno(List<int> turnosId)
        {
            return dalEngine.GetManyByPropertyList<TurnoDocumentacion>(TurnoDocumentacion.Properties.TurnoId, turnosId);
        }

        public EntityCollection<TurnoDocumentacion> TurnoDocumentacionReadByPracticasTurno(List<int> practicasTurno)
        {
            return dalEngine.GetManyByPropertyList<TurnoDocumentacion>(TurnoDocumentacion.Properties.PracticaTurno, practicasTurno);
        }

        /// <summary>
        /// Devuelve toda la documentación relacionada con el turno
        /// </summary>
        /// <param name="turnoId">Id del turno de referencia</param>
        /// <returns>Coleccion de TurnoDocumentacion</returns>
        public EntityCollection<TurnoDocumentacion> TurnoDocumentacionReadByTurno(int turnoId)
        {
            return dalEngine.GetManyByProperty<TurnoDocumentacion>(TurnoDocumentacion.Properties.TurnoId, turnoId);
        }

        /// <summary>
        /// Actualizacion masiva de TurnoDocumentacion
        /// </summary>
        /// <param name="turnoDocumentaciones">Colección de TurnoDocumentacion a actualizar</param>
        [RequiresTransaction]
        public virtual void TurnoDocumentacionUpdateMany(EntityCollection<TurnoDocumentacion> turnoDocumentaciones)
        {
            if (turnoDocumentaciones != null)
                foreach (TurnoDocumentacion td in turnoDocumentaciones)
                {
                    dalEngine.Update<TurnoDocumentacion>(td);
                }

        }


        /// <summary>
        /// Actualizacion masiva de TurnoDocumentacion
        /// </summary>
        /// <param name="turnoDocumentaciones">Colección de TurnoDocumentacion a actualizar</param>
        [RequiresTransaction]
        public virtual void DocumentacionUpdateMany(EntityCollection<TurnoDocumentacion> turnoDocumentaciones, EntityCollection<OrdenDocumentacion> ordenDocumentaciones)
        {
            TurnoDocumentacionUpdateMany(turnoDocumentaciones);
            OrdenDocumentacionUpdateMany(ordenDocumentaciones);
        }

        /// <summary>
        /// Actualizacion masiva de OrdenDocumentacion
        /// </summary>
        /// <param name="turnoDocumentaciones">Colección de TurnoDocumentacion a actualizar</param>
        [RequiresTransaction]
        public virtual void OrdenDocumentacionUpdateMany(EntityCollection<OrdenDocumentacion> ordenDocumentaciones)
        {
            if (ordenDocumentaciones != null)
                foreach (OrdenDocumentacion od in ordenDocumentaciones)
                {
                    dalEngine.Update<OrdenDocumentacion>(od);
                }

        }


        public TurnoDocumentacion TurnoDocumentacionReadByTurnoTag(int turnoId, String tag)
        {
            ReadManyCommand<TurnoDocumentacion> readCmd = new ReadManyCommand<TurnoDocumentacion>(dalEngine);
            Filter filter = new Filter();
            filter.Add(TurnoDocumentacion.Properties.TurnoId, "=", turnoId);
            filter.Add(BooleanOp.And, TurnoDocumentacion.Properties.PlanPracticaDocumentacion.TipoDocumentacionRequerida.Tag, "=", tag);
            filter.Add(BooleanOp.And, TurnoDocumentacion.Properties.PlanPracticaDocumentacion.DeleteFlag, "=", false);
            readCmd.Filter = filter;
            readCmd.MaxResults = 1;
            EntityCollection<TurnoDocumentacion> ret = readCmd.Execute();
            if (ret != null && ret.Count > 0)
                return ret[0];
            else
                return null;
        }



        // OrdenDocumentacion

        /// <summary>
        /// Devuelve toda la documentación relacionada con la orden
        /// </summary>
        /// <param name="turnoId">Id del turno de referencia</param>
        /// <returns>Coleccion de OrdenDocumentacion</returns>
        public EntityCollection<OrdenDocumentacion> OrdenDocumentacionReadByOrden(int ordenId)
        {
            ReadManyCommand<OrdenDocumentacion> readCmd = new ReadManyCommand<OrdenDocumentacion>(dalEngine);
            Filter filter = new Filter();
            filter.Add(OrdenDocumentacion.Properties.OrdenId, "=", ordenId);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }



        // ListadoDiarioTurnos

        public EntityCollection<ListadoDiarioTurnos> ListadoDiarioTurnosRead(DateTime fechaDesde, DateTime fechaHasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estado)
        {
            EntityCollection<ListadoDiarioTurnos> turnos = GetListadoDiarioTurnos(fechaDesde, fechaHasta, medico, servicio, centro, equipo, estado);
            // Ordeno los resultado en base a la fecha, servicio y medico
            turnos.Sort(new Comparison<ListadoDiarioTurnos>(delegate(ListadoDiarioTurnos left, ListadoDiarioTurnos right)
            {
                int retorno = left.FechaTurno.CompareTo(right.FechaTurno);
                if (retorno == 0)
                    retorno = left.Servicio.CompareTo(right.Servicio);
                if (retorno == 0)
                    retorno = left.Equipo.CompareTo(right.Equipo);
                if (retorno == 0)
                    retorno = left.Medico.CompareTo(right.Medico);

                return retorno;
            }));
            return turnos;
        }

        public EntityCollection<ListadoDiarioTurnos> ListadoDiarioTurnosReadPorEquipo(DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estado)
        {
            EntityCollection<ListadoDiarioTurnos> turnos = ListadoDiarioTurnosRead(desde, hasta, medico, servicio, centro, equipo, estado);
            // Ordeno los resultado en base a la fecha, servicio y medico
            turnos.Sort(new Comparison<ListadoDiarioTurnos>(delegate(ListadoDiarioTurnos left, ListadoDiarioTurnos right)
            {
                int retorno = left.Equipo.CompareTo(right.Equipo);
                if (retorno == 0)
                    retorno = left.FechaTurno.CompareTo(right.FechaTurno);

                return retorno;
            }));
            return turnos;
        }

        private EntityCollection<ListadoDiarioTurnos> GetListadoDiarioTurnos(DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estadoTurno)
        {
            EntityCollection<ListadoDiarioTurnos> turnos = CrearListadoDiarioTurnos(desde, hasta, medico, servicio, centro, equipo, estadoTurno);
            AgregarProtocolo(turnos);
            AgregarPracticasAdicionales(turnos);

            bool reservados, atendidos;
            reservados = true;
            atendidos = false;
            if (estadoTurno.HasValue)
            {
                reservados = estadoTurno.Value == EstadoTurnoEnum.Reservado;
                atendidos = estadoTurno.Value == EstadoTurnoEnum.Recepcionado;
            }
            AgregarExposiciones(desde, hasta, medico, servicio, centro, equipo, reservados, atendidos, turnos);

            enfoke.Data.Confidential.ClearConfidentialValues(turnos);
            return turnos;
        }

        private void AgregarProtocolo(EntityCollection<ListadoDiarioTurnos> turnos)
        {
            List<int> ordenesId = (from tur in turnos select tur.OrdenId).ToList<int>();

            if (ordenesId.Count > 0)
            {
                var ordenesIdProtocolo = from orden in dalEngine.Query<Orden>()
                                         where
                                          orden.Protocolo != null &&
                                          ordenesId.Contains(orden.Id)
                                         select new { OrdenId = orden.Id, Protocolo = orden.Protocolo.ProtocoloFull };

                foreach (var ordenIdProtocolo in ordenesIdProtocolo)
                    foreach (ListadoDiarioTurnos ldv in turnos)
                        if (ldv.OrdenId == ordenIdProtocolo.OrdenId)
                            ldv.NroProtocolo = ordenIdProtocolo.Protocolo;
            }
        }

        private EntityCollection<ListadoDiarioTurnos> CrearListadoDiarioTurnos(DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estadoTurno)
        {
            EntityCollection<ListadoDiarioTurnos> ldtVigentes = ListadoDiarioTurnosConPlanPracticaPrecioVigente(desde, hasta, medico, servicio, centro, equipo, estadoTurno);
            List<int> turnosIdVigentes = (from ldt in ldtVigentes select ldt.IdTurno).ToList();
            EntityCollection<ListadoDiarioTurnos> ldtNoVigentes = ListadoDiarioTurnosConPlanPracticaPrecioNoVigente(turnosIdVigentes, desde, hasta, medico, servicio, centro, equipo, estadoTurno);
            EntityCollection<ListadoDiarioTurnos> turnos = new EntityCollection<ListadoDiarioTurnos>(ldtVigentes);
            turnos.AddRange(ldtNoVigentes);
            return turnos;
        }

        private void AgregarExposiciones(DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, bool mostrarSoloReservados, bool mostrarSoloAtendidos, EntityCollection<ListadoDiarioTurnos> turnos)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("SELECT DISTINCT pt.Turno.Id ");
            hqlBuilder.Append("FROM PracticaTurnoHQL pt, PlanPracticaPrecio pp ");
            hqlBuilder.Append("WHERE pt.Turno.Orden.ObraSocialPlan = pp.Plan ");
            hqlBuilder.Append("AND pp.Deleted = false ");
            hqlBuilder.Append("AND pt.Tipo = :idTipoPracticaExposicion ");
            hqlBuilder.Append("AND pt.Practica = pp.Practica ");
            hqlBuilder.Append("AND pp.PracticaAdicional IS NULL ");
            hqlBuilder.Append("AND (pt.Turno.Equipo = pp.Equipo OR pp.Equipo IS NULL) ");
            hqlBuilder.Append("AND pt.Turno.Fecha >= pp.FechaDesde ");
            hqlBuilder.Append("AND (pp.FechaHasta IS NULL OR pt.Turno.Fecha <= pp.FechaHasta) ");
            hqlBuilder.Append("AND pt.Turno.Fecha >= :fechaDesde ");
            hqlBuilder.Append("AND pt.Turno.Fecha <= :fechaHasta ");
            hqlBuilder.Append("AND pt.Turno.Activo = true ");
            hqlBuilder.Append("AND pt.Turno.DeleteFlag = false ");
            hqlBuilder.Append("AND pt.Turno.Estado.Id NOT IN (:tipoTurnosExcluidos) ");
            hqlBuilder.Append("AND pt.Turno.Equipo.Sucursal.Id = :idCentro ");

            if (medico != 0)
                hqlBuilder.Append("AND pt.Medico.Id = :idMedico ");
            if (servicio != 0)
                hqlBuilder.Append("AND pt.Practica.ServicioEspecialidad.Servicio.Id = :idServicio ");
            if (equipo != 0)
                hqlBuilder.Append("AND pt.Turno.Equipo.Id = :equipoId ");
            if (mostrarSoloReservados)
            {
                hqlBuilder.Append("AND pt.Turno.Estado.Pendiente = true ");
                hqlBuilder.Append("AND pt.Tipo.Id = " + ((int)TipoPracticaEnum.Practica).ToString() + " ");
            }
            if (mostrarSoloAtendidos)
            {
                hqlBuilder.Append("AND pt.Turno.Estado.Atendido = true ");
                hqlBuilder.Append("AND pt.Cantidad > 0 ");
            }

            IQuery queryExposiciones = dalEngine.CreateQuery(hqlBuilder.ToString());
            queryExposiciones.SetParameter("idTipoPracticaExposicion", (int)PracticaTurnoTipoEnum.Exposicion);
            queryExposiciones.SetParameter("fechaDesde", desde.Date);
            queryExposiciones.SetParameter("fechaHasta", hasta.Date.AddDays(1));
            queryExposiciones.SetParameter("idCentro", centro);
            queryExposiciones.SetParameterList("tipoTurnosExcluidos", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });
            if (medico != 0)
                queryExposiciones.SetParameter("idMedico", medico);
            if (servicio != 0)
                queryExposiciones.SetParameter("idServicio", servicio);
            if (equipo != 0)
                queryExposiciones.SetInt32("equipoId", equipo);

            IList<int> turnosConExposiciones = queryExposiciones.List<int>();

            // Le agrego adelante del nombre de la practica el (+), solo para aquellos turnosIds que tengan otras practicas que sean exposiciones.
            foreach (int turnoExposicion in turnosConExposiciones)
            {
                int pos = turnos.BinarySearchByProperty(ListadoDiarioTurnos.Properties.IdTurno, turnoExposicion);
                if (pos > 0)
                    turnos[pos].Practica = turnos[pos].Practica.Insert(0, "(+) ");
            }
        }

        private void AgregarPracticasAdicionales(EntityCollection<ListadoDiarioTurnos> turnos)
        {
            List<int> turnosIds = (from ldt in turnos select ldt.IdTurno).ToList();
            if (turnosIds.Count <= 0)
                return;

            var turnoIdOtrasPracticas = (from pt in dalEngine.Query<PracticaTurno>()
                                         where
                                         turnosIds.Contains(pt.TurnoId) &&
                                         pt.Tipo != (int)PracticaTurnoTipoEnum.Principal
                                         select new { TurnoId = pt.TurnoId, PracticaName = new PracticaName(pt.Practica.Id, pt.Practica.Name) });

            foreach (var top in turnoIdOtrasPracticas)
                AddOtraPracticaInListadoDiarioTurnos(turnos, top.TurnoId, top.PracticaName);
        }

        private void AddOtraPracticaInListadoDiarioTurnos(EntityCollection<ListadoDiarioTurnos> turnos, int turnoId, PracticaName practicaName)
        {
            foreach (ListadoDiarioTurnos ldt in turnos)
                if (ldt.IdTurno == turnoId)
                {
                    if (ldt.OtrasPracticas == null)
                        ldt.OtrasPracticas = new List<string>() { practicaName.Name };
                    else
                        ldt.OtrasPracticas.Add(practicaName.Name);
                }
        }

        private EntityCollection<ListadoDiarioTurnos> ListadoDiarioTurnosConPlanPracticaPrecioNoVigente(List<int> turnosIdVigentes, DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estadoTurno)
        {
            const bool PPVIGENTE = true;
            IQuery queryVigentes = GetListadoDiarioTurno(!PPVIGENTE, desde, hasta, medico, servicio, centro, equipo, estadoTurno, turnosIdVigentes);
            return dalEngine.GetManyByQuery<ListadoDiarioTurnos>(queryVigentes);
        }

        private EntityCollection<ListadoDiarioTurnos> ListadoDiarioTurnosConPlanPracticaPrecioVigente(DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estadoTurno)
        {
            const bool PPVIGENTE = true;
            IQuery queryVigentes = GetListadoDiarioTurno(PPVIGENTE, desde, hasta, medico, servicio, centro, equipo, estadoTurno, null);
            EntityCollection<ListadoDiarioTurnos> vigentesCollection = dalEngine.GetManyByQuery<ListadoDiarioTurnos>(queryVigentes);
            return vigentesCollection;
        }

        private IQuery GetListadoDiarioTurno(bool filtraPPVigente, DateTime desde, DateTime hasta, int medico, int servicio, int centro, int equipo, EstadoTurnoEnum? estadoTurno, List<int> turnosIdVigentes)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("SELECT new enfoke.Eges.Entities.Results.ListadoDiarioTurnos(pt.Turno.Id ");
            hqlBuilder.Append(",pt.Medico.Apellido, pt.Medico.Name ");
            hqlBuilder.Append(",pt.Practica.ServicioEspecialidad.Servicio.Name ");
            hqlBuilder.Append(",pt.Turno.Fecha ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.Apellido, pt.Turno.Orden.Paciente.Nombre, pt.Turno.Orden.Paciente.Importancia ");
            hqlBuilder.Append(",pt.Turno.Orden.ObraSocialPlan.ObraSocial.Code, pt.Turno.Orden.ObraSocialPlan.ObraSocial.Name ");
            hqlBuilder.Append(",pt.Turno.Orden.ObraSocialPlan.Code ");
            hqlBuilder.Append(",pt.Turno.Orden.ObraSocialPlan.Name ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.Telefono ");
            hqlBuilder.Append(",pt.Practica.Name ");
            hqlBuilder.Append(",pt.Turno.Estado.Name ");
            hqlBuilder.Append(",pt.Turno.Equipo.Descripcion ");
            hqlBuilder.Append(",pt.MedicoInformante.Apellido, pt.MedicoInformante.Name ");
            hqlBuilder.Append(",pt.Turno.TipoAutorizacion.Name ");
            hqlBuilder.Append(",pt.Turno.Orden.DebeOrden.Codigo ");
            if (filtraPPVigente)
                hqlBuilder.Append(",pp.MedicoCobraHonorarios ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.EdadDBAnio ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.FechaDeNacimiento ");
            hqlBuilder.Append(",pt.Turno.Observaciones ");
            hqlBuilder.Append(",pt.Turno.TipoConfirmacion.Name ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.TelefonoLaboral ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.TelefonoMovil ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.HistoriaClinica ");
            hqlBuilder.Append(",pt.Turno.Duracion ");
            hqlBuilder.Append(",pt.Turno.Orden.Paciente.Email ");
            hqlBuilder.Append(",pt.Turno.Equipo.Sucursal.Name ");
            hqlBuilder.Append(",pt.Turno.Orden.NumeroAfiliado ");
            hqlBuilder.Append(", pt.Turno.Orden.Id ");//Protocolo.ProtocoloFull "); --> el protocolo se carga a partir de la orden id x afuera
            hqlBuilder.Append(") FROM PracticaTurnoHQL pt ");
            if (filtraPPVigente)
                hqlBuilder.Append(",PlanPracticaPrecio pp ");
            hqlBuilder.Append("WHERE ");
            if (filtraPPVigente)
            {
                hqlBuilder.Append("pt.Turno.Orden.ObraSocialPlan = pp.Plan ");
                hqlBuilder.Append("AND pp.Deleted = false ");
                hqlBuilder.Append("AND pt.Practica = pp.Practica ");
                hqlBuilder.Append("AND pp.PracticaAdicional IS NULL ");
                hqlBuilder.Append("AND (pt.Turno.Equipo = pp.Equipo OR pp.Equipo IS NULL) ");
                hqlBuilder.Append("AND pt.Turno.Fecha >= pp.FechaDesde ");
                hqlBuilder.Append("AND (pp.FechaHasta IS NULL OR pt.Turno.Fecha <= pp.FechaHasta) AND ");
            }
            hqlBuilder.Append(" pt.Tipo = :idTipoPracticaPrincipal ");
            hqlBuilder.Append("AND pt.Turno.Fecha >= :fechaDesde ");
            hqlBuilder.Append("AND pt.Turno.Fecha <= :fechaHasta ");
            hqlBuilder.Append("AND pt.Turno.Activo = true ");
            hqlBuilder.Append("AND pt.Turno.DeleteFlag = false ");
            hqlBuilder.Append("AND pt.Turno.Estado.Id NOT IN (:tipoTurnosExcluidos) ");
            hqlBuilder.Append("AND pt.Turno.Equipo.Sucursal.Id = :idCentro ");

            if (medico != 0)
                hqlBuilder.Append("AND pt.Medico.Id = :idMedico ");
            if (servicio != 0)
                hqlBuilder.Append("AND pt.Practica.ServicioEspecialidad.Servicio.Id = :idServicio ");
            if (equipo != 0)
                hqlBuilder.Append("AND pt.Turno.Equipo.Id = :equipoId ");

            if (estadoTurno.HasValue)
            {
                if (estadoTurno.Value == EstadoTurnoEnum.Reservado)
                {
                    hqlBuilder.Append("AND pt.Turno.Estado.Pendiente = true ");
                    hqlBuilder.Append("AND pt.Tipo.Id = " + ((int)TipoPracticaEnum.Practica).ToString() + " ");
                }
                else if (estadoTurno.Value == EstadoTurnoEnum.Recepcionado)
                {
                    hqlBuilder.Append("AND pt.Turno.Estado.Atendido = true ");
                    hqlBuilder.Append("AND pt.Cantidad > 0 ");
                }
            }

            if (turnosIdVigentes != null && turnosIdVigentes.Count > 0)
            {
                SQLBlockBuilder<int> inClauseBuilder = new SQLBlockBuilder<int>(turnosIdVigentes);
                string inClauseString = inClauseBuilder.BuildConstrainBlock("pt.Turno.Id");
                hqlBuilder.Append("AND NOT " + inClauseString + " ");
            }
            hqlBuilder.Append("ORDER BY pt.Turno.Id ASC ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter("idTipoPracticaPrincipal", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameter("fechaDesde", desde.Date);
            query.SetParameter("fechaHasta", hasta.Date.AddDays(1));
            query.SetParameter("idCentro", centro);
            query.SetParameterList("tipoTurnosExcluidos", new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente });
            if (medico != 0)
                query.SetParameter("idMedico", medico);
            if (servicio != 0)
                query.SetParameter("idServicio", servicio);
            if (equipo != 0)
                query.SetInt32("equipoId", equipo);

            return query;
        }

        // TurnoUpdateCobranzaVigente

        public void TurnoUpdateCobranzaVigenteUpdate(int turnoId, int? movimientoCajaId, bool desinhibirEntrega)
        {
            TurnoUpdateCobranzaVigente tur = TurnoUpdateCobranzaVigenteReadByTurnoId(turnoId);
            tur.CobranzaVigenteID = movimientoCajaId;
            if (desinhibirEntrega)
                tur.TipoInhibicionEntregaID = (int)TipoInhibicionEntregaEnum.NoInhibida;
            TurnoUpdateCobranzaVigenteUpdate(tur);
        }

        public TurnoUpdateCobranzaVigente TurnoUpdateCobranzaVigenteReadByTurnoId(int turnoId)
        {
            return dalEngine.GetById<TurnoUpdateCobranzaVigente>(turnoId);
        }

        public void TurnoUpdateCobranzaVigenteUpdate(TurnoUpdateCobranzaVigente turnoUpdateCobranzaVigente)
        {
            dalEngine.Update<TurnoUpdateCobranzaVigente>(turnoUpdateCobranzaVigente);
        }



        // Medico Asociasion
        public EntityCollection<Turno> TunoReadbyMedicoAsociado(int id)
        {
            Filter filter = new Filter();
            filter.Add(Turno.Properties.Orden.MedicoSolicitanteID, "=", id);

            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            readCmd.Filter = filter;
            EntityCollection<Turno> turnos = readCmd.Execute();

            return turnos;
        }

        /// <summary>
        /// Actualiza o da de alta un turno
        /// </summary>
        public void OrdenUpdateBatchByMedicoAsociacion(List<int> ordenes, int medico)
        {
            if (ordenes.Count > 0)
                dalEngine.UpdatePropertyBatchByIds<Orden>(ordenes, Orden.Properties.MedicoSolicitanteID, medico);
        }


        // paciente
        public EntityCollection<Turno> TunoReadbyPaciente(int id)
        {
            Filter filter = new Filter();
            filter.Add(Turno.Properties.Orden.PacienteId, "=", id);

            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);
            readCmd.Filter = filter;
            EntityCollection<Turno> turnos = readCmd.Execute();

            return turnos;
        }

        /// <summary>
        /// Actualiza o da de alta un turno
        /// </summary>
        public void TurnoUpdateBatchByPaciente(List<int> turnos, Paciente viejo, Paciente nuevo)
        {
            CargarLocalidad(viejo);
            CargarLocalidad(nuevo);
            Context.EventProcessor.ProcessEvent<MezclarPacientes, HL7MezclaPacientes>(new HL7MezclaPacientes(viejo, nuevo, Security.Current.UserInfo.User));
            IList<int> ordenIds = OrdenIdsReadByTurnoIds(turnos);
            dalEngine.UpdatePropertyBatchByIds<Orden>(ordenIds, Orden.Properties.PacienteId, nuevo.Id);
        }

        [Private]
        public void CargarLocalidad(Paciente paciente)
        {
            if (paciente.Localidad == null && paciente.LocalidadID.HasValue)
                paciente.Localidad = Context.Session.Dalc.GetById<Localidad>(paciente.LocalidadID.Value);
        }

        // Turno Asociación Movimiento Caja

        /// <summary>
        /// Retorna todos los turnosIds que no fueron cobrados
        /// </summary>
        /// <param name="formularioImporteNeto">Importe neto de la cobranza anticipada</param>
        /// <returns>Listado de posibles turnosIds a asociar</returns>
        public EntityCollection<TurnoPosibleAsociacionMovimientoCaja> TurnoPosibleAsociacionMovimientoCajaReadByImportePago(DateTime fecha, Decimal formularioImporteNeto, Decimal iva, int? centroId)
        {
            int[] estadosId = new int[] { (int)EstadoTurnoEnum.Cancelado, (int)EstadoTurnoEnum.Ausente };
            int tipoTurnoRecitado = (int)TipoTurnoEnum.Recitado;

            StringBuilder hql = new StringBuilder();
            hql.Append("select new enfoke.Eges.Entities.Results.TurnoPosibleAsociacionMovimientoCaja( ");
            hql.Append("tur.Fecha ");
            hql.Append(", tur.Id  ");
            hql.Append(", tur.Orden.Paciente.ApellidoNombre ");
            hql.Append(", tur.Equipo.Servicio.Name ");
            hql.Append(", pt.Practica.Name ");
            hql.Append(", tur.Equipo.Sucursal.Name ");
            hql.Append(", tur.ImporteOrdenMedica) ");
            hql.Append("from TurnoHQL tur ");
            hql.Append("inner join tur.PracticaTurno pt ");
            hql.Append("left join tur.MovimientoCaja mov ");
            hql.Append("left join tur.Original orig ");
            hql.Append("where tur.Fecha >= :fechaDesde ");
            hql.Append("and tur.Fecha < :fechaHasta ");
            hql.Append("and ((tur.Estado.Id not in (:estadosId) and tur.TipoTurno <> :tipoTurnoRecitado) or (orig is not null and orig.Estado.Id not in (:estadosId) and tur.TipoTurno  = :tipoTurnoRecitado))");
            bool reqCobranza = true;
            hql.Append("and pt.Tipo = :tipoPracticaTurno ");
            hql.Append("and pt.Cantidad > 0 ");
            hql.Append("and tur.RequiereCobranza = :reqCobranza ");

            if (centroId.HasValue)
                hql.Append("and tur.Equipo.Sucursal.Id = :centro ");

            hql.Append("and tur.ImporteOrdenMedica = :formularioImporteNeto ");
            hql.Append("and ((tur.ImportePago = 0 or tur.ImportePago is null) or ( mov is null)) ");


            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("tipoPracticaTurno", (int)PracticaTurnoTipoEnum.Principal);
            query.SetParameter("reqCobranza", reqCobranza);
            query.SetParameter("fechaDesde", fecha.Date);
            query.SetParameter("tipoTurnoRecitado", tipoTurnoRecitado);
            query.SetParameterList("estadosId", estadosId);
            query.SetParameter("fechaHasta", fecha.Date.AddDays(1));
            query.SetParameter("formularioImporteNeto", formularioImporteNeto);

            if (centroId.HasValue)
                query.SetParameter("centro", centroId.Value);


            EntityCollection<TurnoPosibleAsociacionMovimientoCaja> results = dalEngine.GetManyByQuery<TurnoPosibleAsociacionMovimientoCaja>(query);
            if (results == null || results.Count == 0)
                return new EntityCollection<TurnoPosibleAsociacionMovimientoCaja>();


            return results;
        }



        public long DemoraEnEquipo(Equipo equipo)
        {
            long cantidadTurnosRecepcionados = 0;
            string hql = "SELECT sum(t.DuracionSeconds) FROM Turno t WHERE t.EstadoTurnoID = :recepcionado AND t.EquipoId = :equipoId and t.Fecha >= :fecha ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("equipoId", equipo.Id);
            query.SetParameter("recepcionado", (int)EstadoTurnoEnum.Recepcionado);
            query.SetParameter("fecha", enfoke.Time.Now.Date);
            object obj = query.UniqueResult();

            if (obj != null)
                cantidadTurnosRecepcionados = ObjAsInt(obj);

            return cantidadTurnosRecepcionados / 60;

        }

        public void TurnoActualizarNumeroAfiliado(int turnoId, string nroAfiliado)
        {
            Orden orden = OrdenReadByTurnoId(turnoId);
            orden.NumeroAfiliado = nroAfiliado;
            dalEngine.Update<Orden>(orden);
        }

        public bool TurnoTieneInformeUnificado(int turnoId)
        {
            EntityCollection<TurnoInforme> informes = dalEngine.GetManyByProperty<TurnoInforme>(TurnoInforme.Properties.TurnoID, turnoId);
            foreach (TurnoInforme informe in informes)
            {
                if (informe.TurnoInformePrincipalID.HasValue || informe.UnificacionPrincipal)
                    return true;
            }

            return false;
        }

        // Turno en espera

        /// <summary>
        /// Busco todos los turnosIds que tienen determinada practica, sean de un paciente en particular y caen dentro de un rango de fechas.
        /// </summary>
        /// <param name="idTurno">Id de turno que no desea que busque.</param>
        /// <param name="fecha">fecha que representa la cota inferior del rango de fechas a buscar.</param>
        /// <param name="idPractica">Id de la practica.</param>
        /// <param name="idPaciente">Id del paciente.</param>
        /// <param name="diasTurnosDados">Dias (parametrizados en parametro TURNO EN ESPERA - DIAS A BUSCAR TURNOS DADOS). Representa la cota superior del rango de fechas a buscar (fecha + dias)</param>
        /// <returns>Turno alcanzados por filtros de busqueda.</returns>
        [Private]
        public EntityCollection<TurnoSeleccionView> TurnoSeleccionViewReadByFechaAndPracticaAndPaciente(
            int idTurno, DateTime fecha, int idPractica, int idPaciente, int diasTurnosDados)
        {
            ReadManyCommand<TurnoSeleccionView> readCmd = new ReadManyCommand<TurnoSeleccionView>(dalEngine);

            readCmd.Filter = new Filter(new FilterItem(TurnoSeleccionView.Properties.PracticaId, "=", idPractica));
            readCmd.Filter.Add(new FilterItem(BooleanOp.And, TurnoSeleccionView.Properties.PacienteID, "=", idPaciente));
            readCmd.Filter.Add(new FilterItem(BooleanOp.And, TurnoSeleccionView.Properties.Fecha, ">=", fecha));
            readCmd.Filter.Add(new FilterItem(BooleanOp.And, TurnoSeleccionView.Properties.Fecha, "<=", enfoke.Time.Now.Date.AddDays(diasTurnosDados)));
            readCmd.Filter.Add(new FilterItem(BooleanOp.And, TurnoSeleccionView.Properties.Id, "!=", idTurno));
            readCmd.Filter.Add(new FilterItem(BooleanOp.And, TurnoSeleccionView.Properties.TipoTurnoID, "NOT IN", new int[] {
                        (int)TipoTurnoEnum.Provisorio, (int)TipoTurnoEnum.EnEspera}));

            readCmd.Sort = new Sort(new SortItem(TurnoSeleccionView.Properties.Fecha, SortingDirection.Asc));
            readCmd.Sort.Add(new SortItem(TurnoSeleccionView.Properties.EstadoID, SortingDirection.Asc));

            return readCmd.Execute();
        }

        [Private]
        public void TurnoEsperaSucursalUpdate(EntityCollection<TurnoEsperaSucursal> turnosSucursal)
        {
            if (turnosSucursal != null && turnosSucursal.Count > 0)
                dalEngine.UpdateCollection(turnosSucursal);
        }

        [Private]
        public EntityCollection<SucursalName> TurnoEsperaSucursalReadByTurnoID(int idTurno)
        {
            string hql = "SELECT tes.Sucursal FROM TurnoEsperaSucursal tes " +
                "WHERE tes.TurnoId = :idTurno " +
                "ORDER BY tes.Sucursal.Name ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);

            return dalEngine.GetManyByQuery<SucursalName>(query);
        }


        public TipoPlan TipoPlanReadByLoteTrasladoId(int idLoteTraslado)
        {
            string hql = "SELECT v.TipoPlan " +
                         "FROM ValorizacionHQL v " +
                         "WHERE v.Deleted = false " +
                         "AND v.Tipo.Id = :idTipo " +
                         "AND v.Turno.Orden.LoteTraslado.Id = :idLoteTraslado ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTipo", (int)ValorizacionTiposEnum.Admision);
            query.SetParameter("idLoteTraslado", idLoteTraslado);

            EntityCollection<TipoPlan> tipos = dalEngine.GetManyByQuery<TipoPlan>(query);
            if (tipos != null && tipos.Count > 0)
                return tipos[0];

            return null;
        }

        public TipoPlan TipoPlanAdmisionReadByTurnoId(int idTurno)
        {
            string hql = "SELECT v.TipoPlan " +
                         "FROM ValorizacionHQL v " +
                         "WHERE v.Deleted = false " +
                         "AND v.Tipo.Id = :idTipo " +
                         "AND v.Turno.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTipo", (int)ValorizacionTiposEnum.Admision);
            query.SetParameter("idTurno", idTurno);

            return dalEngine.GetByQuery<TipoPlan>(query);
        }


        public TipoPlan TipoPlanAdmisionReadByOrdenId(int ordenId)
        {
            string hql = "SELECT v.TipoPlan " +
                         "FROM ValorizacionHQL v " +
                         "WHERE v.Deleted = false " +
                         "AND v.Tipo.Id = :idTipo " +
                         "AND v.Turno.Orden.Id = :ordenId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTipo", (int)ValorizacionTiposEnum.Admision);
            query.SetParameter("ordenId", ordenId);

            EntityCollection<TipoPlan> planes = dalEngine.GetManyByQuery<TipoPlan>(query);

            int planId = 0;
            foreach (TipoPlan tp in planes)
            {
                if (planId == 0)
                    planId = tp.Id;
                else if (planId != tp.Id)
                    throw new Exception("La orden tiene mas de un turno con distintos tipos de planes seleccionado. No se puede continuar con la operacion, consulte con el administrador de sistema");

            }

            return planes.Count > 0 ? planes[0] : null;
        }

        public TipoPlan TipoPlanReadByOrdenId(int ordenId)
        {
            string hql = "SELECT v.TipoPlan " +
                "FROM ValorizacionHQL v " +
                "WHERE v.Deleted = false " +
                "AND v.Turno.Orden.Id = :ordenId " +

                "AND v.Tipo.Id = :tipo " +
                "ORDER BY v.TipoPlan.Id DESC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ordenId", ordenId);
            query.SetParameter("tipo", (int)ValorizacionTiposEnum.Prefacturacion);

            EntityCollection<TipoPlan> planes = dalEngine.GetManyByQuery<TipoPlan>(query);

            int planId = 0;
            foreach (TipoPlan tp in planes)
            {
                if (planId == 0)
                    planId = tp.Id;
                else if (planId != tp.Id)
                    throw new Exception("La orden tiene mas de un turno con distintos tipos de planes seleccionado. No se puede continuar con la operacion, consulte con el administrador de sistema");

            }

            return planes.Count > 0 ? planes[0] : null;
        }

        public bool TurnoHasInternacionReadByTurnoId(int idTurno)
        {
            string hql = "SELECT t.Orden.InfoInternacion " +
                         "FROM Turno t " +
                         "WHERE t.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);

            object idInternacion = query.UniqueResult();

            return idInternacion != null;
        }

        public Calendario CalendarioGetByFecha(DateTime from, DateTime to)
        {
            EntityCollection<Feriado> feriados = FeriadosReadByFecha(from, to);
            EntityCollection<FeriadoSucursal> feriadoSucursal = FeriadoSucursalReadByFeriados(feriados);

            Calendario ret = new Calendario();
            ret.AddFeriados(feriados, feriadoSucursal);

            return ret;
        }

        public EntityCollection<PracticaTurno> PracticaTurnoReadByOrden(int ordenId, PracticaTurnoTipoEnum tipo)
        {
            string hql = "SELECT pt FROM PracticaTurno pt, Turno tur WHERE pt.TurnoId = tur.Id AND tur.Orden.Id = :ordenId and pt.Cantidad > :cantidad";

            if (tipo != PracticaTurnoTipoEnum.Todas)
                hql += " AND pt.Tipo = :tipo ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ordenId", ordenId);
            query.SetParameter("cantidad", 0);
            if (tipo != PracticaTurnoTipoEnum.Todas)
                query.SetParameter("tipo", (int)tipo);

            return dalEngine.GetManyByQuery<PracticaTurno>(query);
        }


        [Private]
        public EntityCollection<DatosFormWizardTurnosOrden> DatosFormWizardTurnosOrdenAgrupaDatosTurnos(int obraSocialPlanId, EntityCollection<DatosFormWizardTurnosOrden> practicas)
        {
            // Ahora me encargo de agrupar por turno todas las practicas y traer su PP para tener leyenda, norma y codigo homologado
            EntityCollection<DatosFormWizardTurnosOrden> datosAgrupadosPorTurnos = new EntityCollection<DatosFormWizardTurnosOrden>();
            DatosFormWizardTurnosOrden datosTurno;
            List<int> turnosYaAnalizados = new List<int>();
            foreach (DatosFormWizardTurnosOrden datosPractica in practicas)
            {
                datosTurno = null;

                if (!turnosYaAnalizados.Contains(datosPractica.TurnoId))
                {
                    datosAgrupadosPorTurnos.Add(datosPractica);
                    turnosYaAnalizados.Add(datosPractica.TurnoId);

                    datosTurno = datosPractica;
                }
                else
                {
                    foreach (DatosFormWizardTurnosOrden datosTurnoAgrupados in datosAgrupadosPorTurnos)
                    {
                        if (datosPractica.TurnoId == datosTurnoAgrupados.TurnoId)
                        {
                            datosTurnoAgrupados.Practica += "\n" + datosPractica.Practica;
                            datosTurnoAgrupados.CodHomologado += "\n" + datosPractica.CodHomologado;
                            datosTurnoAgrupados.CodInterno += "\n" + datosPractica.CodInterno;

                            datosTurno = datosTurnoAgrupados;
                        }
                    }
                }

                if (datosTurno != null)
                {
                    PlanPracticaPrecio pp = Context.Session.ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(obraSocialPlanId, datosPractica.PracticaId, datosPractica.Equipo == null ? 0 : datosPractica.Equipo.Id, datosPractica.Fecha);

                    if (pp != null)
                    {
                        PlanPracticaRequisito requisitos = Context.Session.ObrasSocialesDalc.PlanPracticaRequisitoVigenteReadByPlanPractica(pp.Plan.Id, pp.Practica.Id);
                        datosTurno.DatosPractica.Add(new DatosPractica(datosPractica.PracticaId, datosPractica.Practica,
                            requisitos.Norma, pp.CodigoInterno, (pp.Practica != null) ? pp.Practica.Leyenda : String.Empty));
                    }
                }
            }
            return datosAgrupadosPorTurnos;
        }

        [Private]
        public EntityCollection<DatosFormWizardTurnosOrden> DatosFormWizardTurnosOrdenFillWithAllPosibleTurnosByTurno(int turnoId, DateTime fechaTurno, int? pacienteDefinitivo)
        {
            // Primero me traigo todas las practicas sin agrupar por turno
            string hql = "SELECT new enfoke.Eges.Entities.Results.DatosFormWizardTurnosOrden(tur.Orden.Id, tur.Id, pt.Practica.Id, tur.Fecha, e1.Servicio.Name, en1, pt.Practica.Name, pt.Practica.Code, pt.Medico.Apellido, pt.Medico.Name, tur.Observaciones) " +
                         " FROM Turno tur, PracticaTurno pt, Turno tr, Equipo e1, EquipoName en1, Equipo e2 " +
                         " WHERE tr.Id = :turnoId AND tr.EquipoId = e2.Id AND tur.EquipoId = e1.Id AND e1.Id = en1.Id " +
                         " AND tur.Id = pt.TurnoId AND pt.Cantidad > :cantidadCero " +
                         " AND tur.Orden.ObraSocialPlanId = tr.Orden.ObraSocialPlanId " +
                         " AND (tur.Orden.PacienteId = :pacienteDefinitivo OR tur.Id = :turnoId2) " +
                         " AND e1.Sucursal.Id = e2.Sucursal.Id " +
                         " AND tur.Fecha < :hasta AND tur.Fecha >= :desde " +
                         " AND (tur.EstadoTurnoID = :estadoTurnoID OR tur.Id = :turnoId3)" +
                         " AND ((e1.Servicio.Id = e2.Servicio.Id AND e1.Sucursal.NumeracionPorServicio = true) OR e1.Sucursal.NumeracionPorServicio = false) " +
                         " Order by tur.Id";


            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("turnoId", turnoId);
            query.SetInt32("turnoId2", turnoId);
            query.SetInt32("turnoId3", turnoId);
            query.SetInt32("cantidadCero", 0);
            query.SetInt32("pacienteDefinitivo", pacienteDefinitivo.HasValue ? pacienteDefinitivo.Value : 0);
            query.SetInt32("estadoTurnoID", (int)EstadoTurnoEnum.Reservado);
            query.SetDateTime("desde", fechaTurno.Date);
            query.SetDateTime("hasta", fechaTurno.Date.AddDays(1));

            return dalEngine.GetManyByQuery<DatosFormWizardTurnosOrden>(query);
        }


        [Private]
        public EntityCollection<DatosFormWizardTurnosOrden> DatosFormWizardTurnosOrdenFillWithTurno(int turnoId)
        {
            List<int> ids = new List<int>();
            ids.Add(turnoId);
            EntityCollection<Entities.Valorizacion> valorizacion = Context.Session.ValorizacionesDalc.ValorizacionReadByTurnosAndTipoWithItems(ids, (int)ValorizacionTiposEnum.Prefacturacion);
            if (valorizacion == null || valorizacion.Count == 0)
            {

                valorizacion = Context.Session.ValorizacionesDalc.ValorizacionReadByTurnosAndTipoWithItems(ids, (int)ValorizacionTiposEnum.Presupuesto);
            }

            EntityCollection<DatosFormWizardTurnosOrden> results = new EntityCollection<DatosFormWizardTurnosOrden>();

            if (valorizacion != null && valorizacion.Count > 0)
            {
                Entities.Valorizacion val = valorizacion[0];
                foreach (ValorizacionItem item in val.Items)
                {
                    PlanPracticaPrecio plp = dalEngine.GetById<PlanPracticaPrecio>(item.PlanPracticaUsadoId.GetValueOrDefault(0));
                    string codigo = String.Empty;
                    string apellido = String.Empty;
                    string nombre = String.Empty;

                    if (item.PracticaTurno.Medico != null)
                    {
                        apellido = item.PracticaTurno.Medico.Apellido;
                        nombre = item.PracticaTurno.Medico.Name;
                    }

                    if (plp != null)
                        codigo = plp.CodigoInterno;

                    if (val.Turno.EquipoId.HasValue)
                    {
                        Equipo equipo = Context.Session.EquiposDalc.EquipoReadById(val.Turno.EquipoId.Value);
                        EquipoName equName = new EquipoName(equipo);
                        results.Add(new DatosFormWizardTurnosOrden(val.Turno.Orden.Id, val.Turno.Id, item.PracticaTurno.Practica.Id,
                                 item.Valorizacion.Turno.Fecha, equipo.Servicio.Name, equName, item.PracticaTurno.Practica.Name, codigo, apellido, nombre, item.Valorizacion.Turno.Observaciones));
                    }
                    else
                    {
                        results.Add(new DatosFormWizardTurnosOrden(val.Turno.Orden.Id, val.Turno.Id, item.PracticaTurno.Practica.Id,
                             item.Valorizacion.Turno.Fecha, item.PracticaTurno.Practica.Name, codigo, apellido, nombre, item.Valorizacion.Turno.Observaciones));
                    }
                }
            }

            return results;
        }

        public bool TurnoHasInternacionReadByOrdenId(int ordenId)
        {
            string hql = "SELECT o.InfoInternacion FROM Orden o WHERE o.Id = :ordenId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ordenId", ordenId);

            object idInternacion = query.UniqueResult();

            return idInternacion != null;
        }

        public EntityCollection<Turno> TurnoEspontaneosReservadosReadByOrden(int ordenId)
        {
            string hql = "FROM Turno tur WHERE tur.TipoTurnoId = :tipoEspontaneo AND tur.EstadoTurnoID = :reservado AND tur.Orden.Id = :ordenId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("tipoEspontaneo", (int)TipoTurnoEnum.Espontaneo);
            query.SetInt32("reservado", (int)EstadoTurnoEnum.Reservado);
            query.SetInt32("ordenId", ordenId);

            return dalEngine.GetManyByQuery<Turno>(query);
        }

        public EntityCollection<ServicioMensajeriaSucursal> SucursalMensajeriaReadByServicioSMSId(int servicioSMSId)
        {
            return dalEngine.GetManyByProperty<ServicioMensajeriaSucursal>(ServicioMensajeriaSucursal.Properties.ServicioMensajeriaId, servicioSMSId, ServicioMensajeriaSucursal.Properties.Sucursal.Name);
        }

        public void ServicioMensajeriaSucursalDeleteByServicioSMS(int servicioId)
        {
            EntityCollection<ServicioMensajeriaSucursal> deleted = dalEngine.GetManyByProperty<ServicioMensajeriaSucursal>(ServicioMensajeriaSucursal.Properties.ServicioMensajeriaId, servicioId);
            dalEngine.DeleteBatchByIds<ServicioMensajeriaSucursal>(deleted.GetIds());
        }

        public void ServicioMensajeriaSucursalUpdate(EntityCollection<ServicioMensajeriaSucursal> collecion)
        {
            dalEngine.UpdateCollection(collecion);
        }

        [RequiresTransaction]
        public virtual void MensajeLogTurnoUpdateAffected(EntityCollection<Turno> turnos)
        {
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, MensajeLogTurno.Properties.TurnoId, "IN", turnos.GetIds());
            EntityCollection<MensajeLogTurno> turnoAfectados = dalEngine.GetManyByFilter<MensajeLogTurno>(filter);
            dalEngine.UpdatePropertyBatch(turnoAfectados, MensajeLogTurno.Properties.TurnoAfectado, true);
        }

        public EntityCollection<Turno> TurnosReadByMensaje(int mensajeId, bool? afectado)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.Turno(tur.Orden, equ, tur.Fecha) from Turno tur, MensajeLogTurno mlt, Equipo equ ");
            hqlBuilder.Append("where tur.Id = mlt.TurnoId ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and mlt.MensajeLogId = :mensaje ");

            if (afectado.HasValue)
                hqlBuilder.Append(" and mlt.TurnoAfectado = :turnoAfectado");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("mensaje", mensajeId);
            if (afectado.HasValue)
                query.SetBoolean("turnoAfectado", afectado.Value);

            EntityCollection<Turno> turnos = dalEngine.GetManyByQuery<Turno>(query);
            ObrasSocialesDalc osDalc = Context.Session.ObrasSocialesDalc;
            foreach (Turno turno in turnos)
                turno.Orden.ObraSocialPlan = osDalc.ObraSocialPlanReadById(turno.Orden.ObraSocialPlanId);

            return turnos;
        }

        [RequiresTransaction]
        public virtual void TurnosUpdateTipoConfirmacion(IEntityCollection turnos, int tipoConfirmacion)
        {
            dalEngine.UpdatePropertyBatch(turnos, Turno.Properties.TipoConfirmacionID, tipoConfirmacion);
        }

        [RequiresTransaction]
        public virtual Turno TurnoUpdateEquipo(int turnoId, Equipo eq, Equipo eqPrevio)
        {
            PracticaTurno pt = Context.Session.TurnosDalc.PracticaTurnoReadByTurno(turnoId, PracticaTurnoTipoEnum.Principal)[0];
            int nuevaDuracion;

            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;
            EquipoPracticaDuracion duracion = EquiposDalc.EquipoPracticaDuracionRead(eq.Id, pt.Practica.Id);
            Turno t = TurnoReadById(turnoId);

            if (duracion == null)
                nuevaDuracion = (int)pt.Practica.Duracion.TotalMinutes;
            else
                nuevaDuracion = duracion.Duracion;

            t.EquipoId = eq.Id;
            t.DuracionSeconds = nuevaDuracion * 60;

            dalEngine.Update(t);

            LogRegistrar((int)LogEventoEnum.CambioEquipo,
                         "Se ha cambiado el equipo del turno de " + eqPrevio.Descripcion + " a " + eq.Descripcion +
                         ".", turnoId);

            return t;

        }

        //

        [RequiresTransaction]
        public virtual void EntregaZonaVigenciaUpdate(EntregaZonaVigencia ezv)
        {
            dalEngine.Update(ezv.EntregaZona);
            dalEngine.Update(ezv);
        }

        [RequiresTransaction]
        public virtual EntityCollection<EntregaZonaVigencia> EntregaZonaVigenciaReadAll(bool verSoloVigentes)
        {
            Filter filter = new Filter();
            filter.Add(EntregaZonaVigencia.Properties.EntregaZona.DeleteUser, " is ", null);

            EntityCollection<EntregaZonaVigencia> ret =
                dalEngine.GetManyByFilter<EntregaZonaVigencia>(filter);

            if (verSoloVigentes)
            {
                ret.RemoveAll(ezv => ezv.FechaDesde.Date > Time.Now.Date);
            }

            return ret;
        }

        [RequiresTransaction]
        public virtual void EntregaZonaDelete(EntregaZona entregaZona)
        {
            Audit.AuditDelete(entregaZona, Security.Current.UserInfo.User.Id);
            dalEngine.Update(entregaZona);
        }



        /// <summary>
        /// Se encarga de hacer los updates pertinentes al tipo control facturacion cuando se revaloriza de o a una osp privada.
        /// </summary>
        /// <param name="turno">Turno que se le updatea la revalorizacion (antes de updatearle la orden)</param>
        /// <param name="osp">OSP destino</param>
        [Private]
        public void TurnoTipoControlFacturacionUpdatePorRevalorizacion(Turno turno, ObraSocialPlan osp)
        {
            bool obraSocialAnteriorEsParticular = Context.Session.ObrasSocialesDalc.ObraSocialEsParticularReadByTurnoId(turno.Id);
            bool obraSocialNuevaEsParticular = Context.Session.ObrasSocialesDalc.ObraSocialEsParticularReadByObraSocialPlanId(osp.Id);
            if (obraSocialAnteriorEsParticular ^ obraSocialNuevaEsParticular)
            {
                if (obraSocialNuevaEsParticular)
                {
                    Context.Session.TurnosDalc.TurnoUpdateTCF(turno.Id, TipoControlFacturacionEnum.Particular);
                    turno.TipoControlFacturacion = Context.Session.FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.Particular);

                    TipoControlFacturacionEnum tcfTemp = Context.Session.FacturacionDalc.TipoControlFacturacionReadByTurnoId(turno.Id);

                    // Facturados no saco
                    if (tcfTemp == TipoControlFacturacionEnum.PreFacturado ||
                        tcfTemp == TipoControlFacturacionEnum.NoControlado ||
                        tcfTemp == TipoControlFacturacionEnum.ANoFacturar ||
                        tcfTemp == TipoControlFacturacionEnum.AFacturar)
                        Context.Session.FacturacionDalc.ExcluirUnProtocoloDeComprobante(turno.Id);
                }
                else
                {
                    Context.Session.TurnosDalc.TurnoUpdateTCF(turno.Id, TipoControlFacturacionEnum.NoControlado);
                    turno.TipoControlFacturacion = Context.Session.FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.NoControlado);
                }

            }
        }

        public EntityCollection<TurnoForHuerfanos> TurnoForHuerfanosReadByMedicoEquipoListsAndFecha(DateTime fechaDesde, DateTime fechaHasta, int? medicoId, int? equipoId)
        {
            if (!medicoId.HasValue && !equipoId.HasValue)
                return new EntityCollection<TurnoForHuerfanos>();

            string hql = "SELECT DISTINCT new enfoke.Eges.Entities.Results.TurnoForHuerfanos(t.Id, t.Fecha, pt.Medico.Id, t.EquipoId) " +
                         "FROM Turno t, PracticaTurno pt " +
                         "WHERE t.Id = pt.TurnoId AND t.Fecha >= :fechaDesde ";
            if (fechaHasta != DateTime.MinValue)
                hql += "AND t.Fecha < :fechaHasta ";

            if (medicoId.HasValue)
                hql += "AND pt.Medico.Id = :medicoId ";
            else if (equipoId.HasValue)
                hql += "AND t.EquipoId = :equipoId ";

            hql += "AND t.Activo = true AND t.Deleted = false AND t.EsHuerfano = false AND t.EstadoTurnoID = :estadoTurno ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("estadoTurno", (int)EstadoTurnoEnum.Reservado);
            if (fechaHasta != DateTime.MinValue)
                query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1).AddMinutes(-1));
            if (equipoId.HasValue)
                query.SetInt32("equipoId", equipoId.Value);
            if (medicoId.HasValue)
                query.SetInt32("medicoId", medicoId.Value);

            return dalEngine.GetManyByQuery<TurnoForHuerfanos>(query);
        }




        public List<int> TurnoIdsReadByPracticaTurnoIds(List<int> practicaTurnoIDs)
        {
            StringBuilder hql = new StringBuilder(" select distinct prt.TurnoId from PracticaTurno prt where prt.Id IN (:practicaTurnoIDs) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("practicaTurnoIDs", practicaTurnoIDs);
            List<int> turnosId = (List<int>)query.List<int>();
            return turnosId;
        }

        public List<int> TurnoIdsReadByEquipoFechaObraSocialPlanParaPresupuestos(DateTime fechaDesde, DateTime fechaHasta, int osp, int? equipoId, int pacienteId)
        {
            string hql = "select distinct tur.Id from Turno tur where tur.Orden.ObraSocialPlanId = :osp AND tur.Orden.PacienteId = :pacienteId and tur.Fecha between :fechaDesde and :fechaHasta and tur.EstadoTurnoID = :reservado and tur.PresupuestoId is null ";
            if (equipoId.HasValue)
                hql += "and tur.EquipoId = :equipoId";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetDateTime("fechaDesde", fechaDesde.Date);
            query.SetDateTime("fechaHasta", fechaHasta);
            query.SetInt32("osp", osp);
            query.SetInt32("pacienteId", pacienteId);
            query.SetInt32("reservado", (int)EstadoTurnoEnum.Reservado);
            if (equipoId.HasValue)
                query.SetInt32("equipoId", equipoId.Value);
            List<int> turnosId = (List<int>)query.List<int>();
            return turnosId;
        }

        public EntityCollection<SucursalName> GetSucursalesNameHabilitadasByUsuario(SecurityUser usuario)
        {

            EntityCollection<SucursalName> allSucursales = Context.Session.Dalc.GetAll<SucursalName>();
            EntityCollection<SucursalName> sucursalesHabilitadas = new EntityCollection<SucursalName>();
            List<int> sucIdsInhabilitadas = (from usi in dalEngine.Query<UsuarioCentroInhabilitado>()
                                             where
                                                usi.UsuarioId == usuario.Id
                                             select usi.SucursalId).ToList<int>();
            foreach (SucursalName sucursal in allSucursales)
                if (!sucIdsInhabilitadas.Contains(sucursal.Id))
                    sucursalesHabilitadas.Add(sucursal);

            return sucursalesHabilitadas;
        }

        public List<int> TurnoIdsReadByComprobanteItems(List<int> comprobanteItemIDsParaFacturado)
        {
            StringBuilder hql = new StringBuilder(" select distinct prt.Turno.Id ");
            hql.Append("from PracticaTurnoHQL prt ");
            hql.Append(" inner join prt.ComprobanteItem coi ");
            hql.Append("   where coi.Id IN (:comprobanteItemIDsParaFacturado) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("comprobanteItemIDsParaFacturado", comprobanteItemIDsParaFacturado);
            List<int> turnosId = (List<int>)query.List<int>();
            return turnosId;
        }

        internal EntityCollection<Turno> TurnosReadByPresupuestosIds(List<int> presupuestosIds)
        {
            if (presupuestosIds == null || presupuestosIds.Count <= 0)
                return new EntityCollection<Turno>();

            ReadManyCommand<Turno> readCmd = new ReadManyCommand<Turno>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Turno.Properties.PresupuestoId, "IN", presupuestosIds);
            filter.Add(BooleanOp.And, Turno.Properties.Activo, "=", true);
            filter.Add(BooleanOp.And, Turno.Properties.Deleted, "=", 0);
            filter.Add(BooleanOp.And, Turno.Properties.EstadoTurnoID, "<>", (int)EstadoTurnoEnum.Cancelado);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public bool TurnosTienenMismaComposicionPracticas(int turnoId1, int turnoId2, bool compararTipoPracticaTurno)
        {
            EntityCollection<PracticaTurno> practicasTurno1 = Context.Session.TurnosDalc.PracticaTurnoReadByTurno(turnoId1);
            EntityCollection<PracticaTurno> practicasTurno2 = Context.Session.TurnosDalc.PracticaTurnoReadByTurno(turnoId2);

            return TurnosTienenMismaComposicionPracticas(practicasTurno1, practicasTurno2, compararTipoPracticaTurno);

        }

        public bool TurnosTienenMismaComposicionPracticas(EntityCollection<PracticaTurno> pts1, EntityCollection<PracticaTurno> pts2, bool comparaTipos)
        {
            if (pts1.Count != pts2.Count)
                return false;

            bool encontro;
            foreach (PracticaTurno ptur in pts1)
            {
                encontro = false;
                foreach (PracticaTurno ppre in pts2)
                {
                    if (ppre.MismosValores(ptur, comparaTipos))
                        encontro = true;
                }

                if (!encontro)
                    return false;
            }

            return true;
        }

        public List<int> TurnosReadByBuscadorTurnosFiltrosBasicos(DateTime? fechaDesde, DateTime? fechaHasta, DateTime? fechaCreacionDesde, DateTime? fechaCreacionHasta, int? obraSocialplanId, int? equipoId, int? pacienteId, EstadoTurnoEnum? estadoTurno, List<TipoTurnoEnum> tiposTurno, bool soloTurnosSinPresupuesto, bool? mostrarCancelados)
        {
            var turnos = dalEngine.Query<Turno>();

            if (fechaDesde.HasValue)
                turnos = turnos.Where(turno => turno.Fecha != null && turno.Fecha.Value > fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                turnos = turnos.Where(turno => turno.Fecha != null && turno.Fecha.Value < fechaHasta.Value.Date.AddDays(1));

            if (fechaCreacionDesde.HasValue)
                turnos = turnos.Where(turno => turno.CreateDate != null && turno.CreateDate.Value > fechaCreacionDesde.Value.Date);

            if (fechaCreacionHasta.HasValue)
                turnos = turnos.Where(turno => turno.CreateDate != null && turno.CreateDate.Value < fechaCreacionHasta.Value.Date.AddDays(1));

            if (obraSocialplanId.HasValue)
                turnos = turnos.Where(turno => turno.Orden.ObraSocialPlanId == obraSocialplanId.Value);

            if (equipoId.HasValue)
                turnos = turnos.Where(turno => turno.EquipoId == null || turno.EquipoId == equipoId.Value);

            if (pacienteId.HasValue)
                turnos = turnos.Where(turno => turno.Orden.PacienteId == pacienteId.Value);

            if (estadoTurno.HasValue)
                turnos = turnos.Where(turno => turno.EstadoTurnoID == (int)estadoTurno.Value);

            if (tiposTurno != null)
            {
                if (tiposTurno.Count == 1)
                    turnos = turnos.Where(turno => turno.TipoTurnoId == (int)tiposTurno[0]);
                else
                    turnos = turnos.Where(turno => tiposTurno.Contains((TipoTurnoEnum)turno.TipoTurnoId));
            }

            if (soloTurnosSinPresupuesto)
                turnos = turnos.Where(turno => turno.PresupuestoId == null);

            if (mostrarCancelados.HasValue)
                turnos = turnos.Join(dalEngine.Query<EstadoTurno>(), turno => turno.EstadoTurnoID, estado => estado.Id, (turno, estado) => new { EstadoTurno = estado, Turno = turno }).Where(turnoEstado => (mostrarCancelados.Value || !turnoEstado.EstadoTurno.Cancelado)).Select(turnoEstado => turnoEstado.Turno);

            return turnos.Select(turno => turno.Id).ToList();
        }

        public List<int> TurnosIdsReadByFecha(DateTime? fechaDesde)
        {
            var query = from turno in dalEngine.Query<Turno>() where (!fechaDesde.HasValue || (turno.Fecha.Value > fechaDesde.Value)) select turno.Id;

            return query.ToList();
        }

        public PlanPracticaPrecio PlanPracticaPrincipalReadByTurno(int turnoId)
        {
            var query = from valItem in dalEngine.Query<ValorizacionItem>()
                        join plp in dalEngine.Query<PlanPracticaPrecio>() on valItem.PlanPracticaUsadoId equals plp.Id
                        where valItem.PracticaTurno.TurnoId == turnoId && valItem.PracticaTurno.Tipo == (int)PracticaTurnoTipoEnum.Principal && valItem.PracticaTurno.Cantidad > 0
                        select plp;

            return query.FirstOrDefault();
        }

        public bool TurnoTieneConsentimiento(int turnoID)
        {
            List<int> turnosIds = new List<int>();
            turnosIds.Add(turnoID);

            return TurnosTieneConsentimiento(turnosIds);
        }

        public bool TurnosTieneConsentimiento(List<int> turnosIds)
        {
            var query = from pt in dalEngine.Query<PracticaTurno>()
                        join pco in dalEngine.Query<PracticaConsentimiento>() on pt.Practica.Id equals pco.PracticaId
                        where pt.Cantidad > 0 && turnosIds.Contains(pt.TurnoId)
                        select pco.ConsentimientoId;

            return query.Count() > 0;
        }

        public EntityCollection<TurnoInforme> GetTurnoInformesByTurnoId(int turnoId)
        {
            IQueryable<TurnoInforme> queryTurnoInformes = (from turInforme in dalEngine.Query<TurnoInforme>()
                                                           where turInforme.TurnoID == turnoId
                                                           select turInforme);
            EntityCollection<TurnoInforme> turnoInformes = queryTurnoInformes.ToEntityCollection<TurnoInforme>();
            return turnoInformes;
        }

        public Paciente GetPacienteByTurnoId(int turnoId)
        {
            Paciente paciente = (from tur in dalEngine.Query<Turno>()
                                 join pac in dalEngine.Query<Paciente>()
                                    on tur.Orden.PacienteId equals pac.Id
                                 where tur.Id == turnoId
                                 select pac).FirstOrDefault<Paciente>();
            return paciente;
        }

        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurnosIds(List<int> turnosIds, bool requiereProfesional)
        {
            if (turnosIds == null || turnosIds.Count <= 0)
                return new EntityCollection<PracticaTurno>();

            EntityCollection<PracticaTurno> ret = new EntityCollection<PracticaTurno>();
            List<List<int>> turIdsMenor1000 = LinqInClause.SplitIntoBucketsForOracle(turnosIds);
            foreach (List<int> turIds in turIdsMenor1000)
            {
                var query = from pt in dalEngine.Query<PracticaTurno>()
                            where turnosIds.Contains(pt.TurnoId)
                            && pt.Cantidad > 0
                            && (!requiereProfesional || pt.Practica.RequiereProfesional)
                            select pt;

                EntityCollection<PracticaTurno> tmp = query.ToEntityCollection();
                if (tmp != null && tmp.Count > 0)
                    ret.AddRange(tmp);
            }

            return ret;
        }

        public Dictionary<int, KeyValuePair<bool?, int?>> RequierePediatraYEquipoIdByTurnosIds(List<int> turnosIds)
        {
            if (turnosIds == null || turnosIds.Count <= 0)
                return new Dictionary<int, KeyValuePair<bool?, int?>>();

            Dictionary<int, KeyValuePair<bool?, int?>> ret = new Dictionary<int, KeyValuePair<bool?, int?>>();
            List<List<int>> turIdsMenor1000 = LinqInClause.SplitIntoBucketsForOracle(turnosIds);
            foreach (List<int> turIds in turIdsMenor1000)
            {
                var query = from tur in dalEngine.Query<Turno>()
                            where turnosIds.Contains(tur.Id)
                            select new KeyValuePair<int, KeyValuePair<bool?, int?>>(tur.Id, new KeyValuePair<bool?, int?>(tur.RequierePediatra, tur.EquipoId));

                Dictionary<int, KeyValuePair<bool?, int?>> tmp = query.ToDictionary(v => v.Key, v => v.Value);
                if (tmp != null && tmp.Count > 0)
                {
                    foreach (KeyValuePair<int, KeyValuePair<bool?, int?>> keyValuePair in tmp)
                        ret.Add(keyValuePair.Key, keyValuePair.Value);
                }
            }

            return ret;
        }
    }
}
