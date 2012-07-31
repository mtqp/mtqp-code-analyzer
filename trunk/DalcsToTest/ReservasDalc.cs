using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Text;
using System.Linq;
using enfoke.Connector;
using enfoke.Eges;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using enfoke.Eges.Valorizacion;


using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Persistence;

using enfoke.Data;
using enfoke.Eges.Auditoria;
using enfoke.Data.Filters;
using enfoke.AOP;
using enfoke.Eges.Persistance;
using NHibernate;

namespace enfoke.Eges.Data
{
    public class ReservasDalc : Dalc, IService
    {
        protected ReservasDalc(NotConstructable dummy) : base(dummy) { }

        [Private]
        public Orden ActualizaInfoOrden(TurnoReservaInfo reservaInfo, PracticaInfo practicaInfo, bool nuevaOrdenObligatorio)
        {
            Orden orden = !nuevaOrdenObligatorio ? reservaInfo.Orden : null;

            if (orden == null && practicaInfo.OrdenId.HasValue && reservaInfo.MantenerOrden)
                orden = dalEngine.GetById<Orden>(practicaInfo.OrdenId.Value);

            if (orden == null || !reservaInfo.MantenerOrden)
            {
                Turno turnoOriginal = TurnoOriginalFromReserva(reservaInfo, practicaInfo);
                if (turnoOriginal != null && !turnoOriginal.EsPresupuesto() && reservaInfo.MantenerOrden)
                    orden = turnoOriginal.Orden;
                else
                    orden = new Orden();

                orden.Estado = (byte)OrdenEstadoEnum.Abierta;
            }

            orden.EsMultiple = reservaInfo.TurnoMultiple;
            orden.CantSesiones = practicaInfo.CantidadSesiones;
            orden.Diagnostico = practicaInfo.Diagnostico;
            orden.PacienteId = practicaInfo.Paciente.Id;
            orden.NumeroAfiliado = reservaInfo.Orden != null ? reservaInfo.Orden.NumeroAfiliado : practicaInfo.NumeroAfiliado;
            orden.ObraSocialPlanId = practicaInfo.ObraSocialPlan.Id;
            orden.ObraSocialPlan = Context.Session.ObrasSocialesDalc.ObraSocialPlanReadById(practicaInfo.ObraSocialPlan.Id);
            orden.ObraSocialPlanOriginalId = practicaInfo.ObraSocialPlan.Id;
            orden.ObraSocialPlanOriginal = orden.ObraSocialPlan;
            orden.FechaEmisionOrden = practicaInfo.FechaEmisionOrden.HasValue ? practicaInfo.FechaEmisionOrden : (reservaInfo.Orden != null ? reservaInfo.Orden.FechaEmisionOrden : (DateTime?)null);
            orden.FechaEmisionAutorizacion = practicaInfo.FechaEmisionAutorizacion.HasValue ? practicaInfo.FechaEmisionAutorizacion : (reservaInfo.Orden != null ? reservaInfo.Orden.FechaEmisionAutorizacion : (DateTime?)null);

            if (practicaInfo.Planillas != null && practicaInfo.Planillas.Count > 0)
            {
                if (practicaInfo.Planillas[0].MedicoSolicitante != null)
                    orden.MedicoSolicitanteID = practicaInfo.Planillas[0].MedicoSolicitante.Id;
                else if (practicaInfo.Planillas[0].MedicoSolicitanteID > 0)
                    orden.MedicoSolicitanteID = practicaInfo.Planillas[0].MedicoSolicitanteID;
            }

            if (practicaInfo.MedicoSolicitante != null)
                orden.MedicoSolicitanteID = practicaInfo.MedicoSolicitante.Id;

            if (!orden.MedicoSolicitanteID.HasValue && reservaInfo.Orden != null)
                orden.MedicoSolicitanteID = reservaInfo.Orden.MedicoSolicitanteID;

            if (reservaInfo.InfoInternacion != null && reservaInfo.RequiereInternacion != false)
                orden.InfoInternacion = reservaInfo.InfoInternacion.Id;

            // Si ya realizo todas las sesiones, entonces cierro la orden multiple 
            if (orden.EsMultiple && orden.CantSesiones == orden.CantCumplidas)
                orden.Estado = (byte)OrdenEstadoEnum.Cerrada;

            return dalEngine.Update<Orden>(orden);
        }

        [Private]
        public void ActualizaEntrevistas(TurnoEntrevista turnoEntrevista, EntityCollection<TurnoEntrevista> entrevistaDirectas)
        {
            if (turnoEntrevista != null)
                dalEngine.Update(turnoEntrevista);
            if (entrevistaDirectas.Count > 0)
                dalEngine.UpdateCollection(entrevistaDirectas);
        }

        [Private]
        public Combo ComboGetNew()
        {
            // se agrega un nuevo combo

            Combo combo = new Combo();
            return dalEngine.Update<Combo>(combo);
        }

        [Private]
        public TurnoPracticas CreaTurno(PosibleTurno pt, TurnoReservaInfo reserva, Combo combo, Orden orden, FullValorizacion valorizacion, ResultadoReserva result, enfoke.Eges.Utils.DateSuperpositionValidationHelper dateSuperpositionHelper, PracticaInfo practicaInfo, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro, int vigenciaPresupuesto)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Para un turno multiple, a cada sesion le creo su propio combo
            Combo comboAUtilizar;
            if (combo == null)
                comboAUtilizar = ComboGetNew();
            else
                comboAUtilizar = combo;

            bool turnoRequierePediatra = reserva.PacienteRequierePediatraParaServicio(practicaInfo.Practica.ServicioEspecialidad.Servicio, enfoke.Time.Now);
            Turno turno = CreateTurno(comboAUtilizar, orden, practicaInfo, pt, reserva.IsProvisorio, reserva.TipoTurno.Id, reserva.DiscapacidadTemporal, reserva.EmbarazadaOBebe, turnoRequierePediatra);

            ResuelveTurnoOriginal(reserva, practicaInfo, turno, modalidadCoseguro, vigenciaPresupuesto);

            // Marcas si mantengo las Fechas y Usuarios de Autorizacion y Confirmacion [para el Log]
            bool mantengoAutorizacion;
            bool mantengoConfirmacion;

            // Obtengo los Tipos de Autorizacion y Confirmacion
            AutorizacionPlanilla.Mail mailEnviar = ObtenerTiposConfAut(practicaInfo, turno,
                    out mantengoAutorizacion, out mantengoConfirmacion, result, reserva.IsRescheduling);

            // Guardo el Turno
            TurnosDalc.TurnoUpdate(turno);

            if (mailEnviar != AutorizacionPlanilla.Mail.Ninguno)
                result.MailsEnviar.Add(turno, mailEnviar);

            // Inserto el TurnoLog
            ActualizaTurnoLog(reserva, turno, mantengoAutorizacion, mantengoConfirmacion);

            // Logueo la Reformulacion
            if (turno.TurnoOriginalID.HasValue && reserva.CondicionesReformuladas)
                TurnosDalc.LogRegistrar((int)LogEventoEnum.ReformulacionCondicionesTurno, "Se reformularon las condiciones.", turno.Id);


            TipoTurno tipo = dalEngine.GetById<TipoTurno>(turno.TipoTurnoId);
            string strLog = "Se cargó el turno [" + tipo.Name + "] en el sistema";
            if (turno.TurnoOriginal != null)
            {
                tipo = dalEngine.GetById<TipoTurno>(turno.TurnoOriginal.TipoTurnoId);
                strLog += " a partir de un [" + tipo.Name + "]";
            }
            strLog += ".";
            TurnosDalc.LogRegistrar((int)LogEventoEnum.CargaDeNuevoTurno, strLog, turno.Id);

            // Grabo el Historico de Estados
            TurnosDalc.CrearEstadoTurnoHistorico(false, turno, null);

            // Graba las practicas
            List<PracticaTurno> pTurnos = InsertPracticas(turno, practicaInfo, pt);

            // Grabo los centrosa asociados al turno en caso del turno tipo En Espera.
            InsertCentrosAsociadosAlTurnoEnEspera(turno, reserva.Sucursales);

            // Si no estoy recitando
            if (turno.TipoTurnoId != (int)TipoTurnoEnum.Recitado)
            {
                // Valorizaciones - Informes - Encuesta - Condiciones - Planillas solo si la practica es facturable.
                valorizacion = InsertValorizacion(valorizacion, practicaInfo, turno);

                // Si es un presupuesto numerado, lo Logueo
                if (!reserva.MantenerValorizaciones && valorizacion.ValorizacionInfo.NroPresupuesto.HasValue)
                    TurnosDalc.LogRegistrar((int)LogEventoEnum.GeneracionPresupuestoNumerado, "Se generó el presupuesto número " + valorizacion.ValorizacionInfo.NroPresupuesto.Value.ToString() + ".", turno.Id);

                // Creo la Encuesta del Turno
                CrearOrdenEncuesta(practicaInfo.Practica.ServicioEspecialidad.Servicio.Id, orden.Id);

                // Graba las respuestas a las condiciones, borrando las anteriores
                InsertRespuestas(turno.Id, practicaInfo.RespuestasAsociadas);

                // Inserta las planillas
                if (practicaInfo.Planillas != null)
                    TurnosDalc.SavePlanillas(practicaInfo.Planillas, turno.Id, false);
            }

            // Practica facturable.
            bool isInvoicePractice = practicaInfo.Practica.EsFacturable.GetValueOrDefault(false);

            // Si no es Provisorio y la practica es facturable
            if (!reserva.IsTurnoSinFecha && isInvoicePractice)
                ActualizaTopes(practicaInfo, turno, modalidadCoseguro);

            return new TurnoPracticas(turno, pTurnos);
        }

        [RequiresTransaction]
        protected virtual void InsertCentrosAsociadosAlTurnoEnEspera(Turno turno, EntityCollection<SucursalName> sucursales)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            if (sucursales != null && turno.TipoTurnoId == (int)TipoTurnoEnum.EnEspera)
            {
                EntityCollection<TurnoEsperaSucursal> turnosSucursal = new EntityCollection<TurnoEsperaSucursal>();

                foreach (SucursalName sucursal in sucursales)
                    turnosSucursal.Add(new TurnoEsperaSucursal(turno.Id, sucursal));

                if (turnosSucursal.Count > 0)
                    TurnosDalc.TurnoEsperaSucursalUpdate(turnosSucursal);
            }
        }

        [RequiresTransaction]
        protected virtual void ActualizaTopes(PracticaInfo practicaInfo, Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            // Chequeo que existan topes para la OS
            if (TurnosDalc.ObraSocialHasTopes(practicaInfo.ObraSocial.Id))
            {
                // Cuento la Practica Principal
                int practicas = (int)Decimal.Round(practicaInfo.Cantidad, MidpointRounding.AwayFromZero);

                // Cuento las Practicas Subsiguientes
                foreach (ExposicionInfo exposicion in practicaInfo.Exposiciones)
                    if (exposicion.Exposicion != null)
                        practicas += (int)Decimal.Round(exposicion.Cantidad, MidpointRounding.AwayFromZero);

                // Obtengo el monto de las prácticas
                int monto = ValorizacionesDalc.ObtenerTotalOSValorizacionPresupuesto(turno, modalidadCoseguro);

                // Actualizo los Topes que Apliquen
                TurnosDalc.TopesUpdate(practicaInfo.PosibleTurno.StartDate, practicaInfo.ObraSocial.Id, practicaInfo.Practica.ServicioEspecialidad.Servicio.Id, turno.EquipoId.Value, practicaInfo.PosibleTurno.Medicos.Informante.Id, practicas, monto);
            }
        }

        [RequiresTransaction]
        protected virtual FullValorizacion InsertValorizacion(FullValorizacion valorizacion, PracticaInfo practicaInfo, Turno turno)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            // Graba las valorizaciones
            if (valorizacion == null)
                throw new Exception("No se encontró la valorización para la práctica " + practicaInfo.Practica.Name);

            if (turno.Orden.ObraSocialPlan == null)
            {
                ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;
                turno.Orden.ObraSocialPlan = ObrasSocialesDalc.ObraSocialPlanReadById(turno.Orden.ObraSocialPlanId);
            }

            ValorizacionesDalc.InsertValorizacion(valorizacion.ValorizacionInfo, turno, turno.Orden.ObraSocialPlan);
            return valorizacion;
        }

        private void ActualizaTurnoLog(TurnoReservaInfo reserva, Turno turno, bool mantengoAutorizacion, bool mantengoConfirmacion)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            TurnoLog turnoLog = new TurnoLog();
            turnoLog.TurnoId = turno.Id;

            if (reserva.IsRescheduling && turno.TurnoOriginalID.HasValue)
            {
                // Obtengo el Log del Turno Original
                TurnoLog tlOriginal = TurnosDalc.TurnoLogReadByTurno(turno.TurnoOriginalID.Value);

                turnoLog.ReservaFecha = tlOriginal.ReservaFecha;
                turnoLog.ReservaUsuario = tlOriginal.ReservaUsuario;

                // Mantengo las Fechas y Usuarios de Autorizacion y Confirmacion
                if (mantengoAutorizacion)
                {
                    turnoLog.PedidoAutorizacionFecha = tlOriginal.PedidoAutorizacionFecha;

                    turnoLog.AutorizacionFecha = tlOriginal.AutorizacionFecha;
                    turnoLog.AutorizacionUsuario = tlOriginal.AutorizacionUsuario;
                }

                if (mantengoConfirmacion)
                {
                    turnoLog.ConfirmacionFecha = tlOriginal.ConfirmacionFecha;
                    turnoLog.ConfirmacionUsuario = tlOriginal.ConfirmacionUsuario;
                }
            }
            else
            {
                SecurityUser user = Security.Current.UserInfo.User;
                turnoLog.ReservaFecha = enfoke.Time.Now;
                turnoLog.ReservaUsuario = user;
            }

            Context.Session.TurnosDalc.ModificarTurnoLogDependiendoDelTipoControlFacturacion(turnoLog, turno);
            if (!mantengoAutorizacion && (turno.TipoAutorizacionID == (int)TipoAutorizacionEnum.EsperaAutorizacionRemota || turno.TipoAutorizacionID == (int)TipoAutorizacionEnum.AAutorizarRemoto))
                turnoLog.PedidoAutorizacionFecha = enfoke.Time.Now;


            turnoLog = dalEngine.Update<TurnoLog>(turnoLog);
        }

        private void ResuelveTurnoOriginal(TurnoReservaInfo reserva, PracticaInfo practicaInfo, Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro, int vigenciaPresupuestos)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Si viene turno orginal en la reserva, es complemento
            if (reserva.TurnoOriginal != null)
            {
                turno.TurnoOriginalID = reserva.TurnoOriginal.Id;
                turno.TurnoOriginal = reserva.TurnoOriginal;

                turno.TipoTurnoId = (int)TipoTurnoEnum.ComplementoNoFacturable;

            }
            else
            {
                // Busco el turno viejo [en la PI o de la práctica Principal actual entre los viejos]
                Turno turnoViejo = TurnoOriginalFromReserva(reserva, practicaInfo);

                // Si tenia turnos viejos o estoy reprogramando, asigno el original
                // Capaz estoy dando un turno a partir de un presupuesto. Pongo la relacion y cambio el estado del presupuesto
                // Capaz estoy copiando presupuestos => no hago nada
                if (turnoViejo != null && !turno.EsPresupuesto())
                {
                    if (turnoViejo.EsPresupuesto())
                    {
                        turno.PresupuestoId = turnoViejo.Id;
                        TurnosDalc.TurnoAvanzarEstado(turnoViejo, (int)EstadoTurnoEnum.ConTurnoAsignado, modalidadCoseguro);
                    }
                    else
                    {
                        turno.TurnoOriginalID = turnoViejo.Id;
                        turno.TurnoOriginal = turnoViejo;

                        // Si es un recitado actualizo la fecha de entrega de los informes
                        if (turno.TipoTurnoId == (int)TipoTurnoEnum.Recitado && turno.Fecha.HasValue)
                            TurnosDalc.TurnoInformeUpdateFechaEntrega(turnoViejo.Id, turno.Fecha.Value);

                        // Puede que este reprogramando un turno con presupuesto (si cambio el turno o no esta mas vigente, viene creado uno nuevo)
                        if (practicaInfo.PresupuestoParaRelacionar != null)
                        {
                            turno.PresupuestoId = practicaInfo.PresupuestoParaRelacionar.Id;
                            TurnosDalc.TurnoAvanzarEstado(practicaInfo.PresupuestoParaRelacionar, (int)EstadoTurnoEnum.ConTurnoAsignado, modalidadCoseguro);

                        }
                        else if (turnoViejo.PresupuestoId.HasValue) // Si esta vigente y el turno no cambio, uso el mismo
                        {
                            Turno presupuesto = Context.Session.Dalc.GetById<Turno>(turnoViejo.PresupuestoId.Value);
                            if (presupuesto.CreateDate.Value.AddDays(vigenciaPresupuestos) > practicaInfo.PosibleTurno.StartDate)// es vigente
                                turno.PresupuestoId = turnoViejo.PresupuestoId.Value;
                        }
                    }
                }
            }

            // Si tengo original, mantengo algunos datos
            if (turno.TurnoOriginalID.HasValue)
            {
                // Mantengo los Estudios Anteriores
                if (reserva.IsRescheduling)
                    turno.Orden.TurnoEstudiosID = turno.TurnoOriginal.Orden.TurnoEstudiosID;

                // Mantengo el Numero de Afiliado
                if (String.IsNullOrEmpty(turno.Orden.NumeroAfiliado))
                    turno.Orden.NumeroAfiliado = turno.TurnoOriginal.Orden.NumeroAfiliado;
            }
        }

        private Turno TurnoOriginalFromReserva(TurnoReservaInfo reserva, PracticaInfo practicaInfo)
        {
            return practicaInfo.TurnoViejo != null ? practicaInfo.TurnoViejo : this.ObtenerTurnoViejo(reserva.TurnosViejos, practicaInfo.Practica.Id);
        }

        private AutorizacionPlanilla.Mail ObtenerTiposConfAut(PracticaInfo practicaInfo, Turno turno,
            out bool mantengoAutorizacion, out bool mantengoConfirmacion, ResultadoReserva result, bool isRescheduling)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            mantengoAutorizacion = false;
            mantengoConfirmacion = false;
            AutorizacionPlanilla.Mail retorno = AutorizacionPlanilla.Mail.Ninguno;

            // Chequeo si alguna practica exige Autorizacion o Confirmacion
            turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.NoAplica;
            turno.TipoConfirmacionID = (int)TipoConfirmacionEnum.NoAplica;

            // Si estoy recitando el comportamiento es distinto
            if (turno.TipoTurnoId == (int)TipoTurnoEnum.Recitado)
            {
                // Obtengo las exigencias de Confirmacion y Autorizacion segun el Turno Original
                ExigeConfAut exigeConfAut = ObtenerExigeConfAut(turno.TurnoOriginal);

                // Mantengo la marca de autorizacion del turno original - No evaluo
                turno.TipoAutorizacionID = turno.TurnoOriginal.TipoAutorizacionID;
                mantengoAutorizacion = true;

                // Pongo la marca de confirmacion según los datos del turno original
                if (exigeConfAut.Confirmacion)
                    turno.TipoConfirmacionID = (int)TipoConfirmacionEnum.AConfirmar;
            }
            else
            {
                // Obtengo las exigencias de Confirmacion y Autorizacion segun la PracticaInfo
                ExigeConfAut exigeConfAut = ObtenerExigeConfAut(practicaInfo);

                // Si es Reprogramación, chequeo si mantengo las Marcas del Turno Original
                if (isRescheduling)
                {
                    // Chequeo que fue lo que cambio en el Turno
                    CambiosPracticaInfo cambios = new CambiosPracticaInfo();
                    if (turno.TurnoOriginal != null)
                        cambios = ObtenerCambios(practicaInfo, turno.TurnoOriginal);

                    #region Confirmacion
                    if (turno.TurnoOriginal != null)
                    {
                        // Si hubo algun cambio, tomo la marca evaluada
                        // Sino, mantengo la marca del turno original
                        if (cambios.CambioPracticas || cambios.CambioOSoPlan || cambios.CambioDia)
                        {
                            if (exigeConfAut.Confirmacion)
                                turno.TipoConfirmacionID = (int)TipoConfirmacionEnum.AConfirmar;
                        }
                        else
                        {
                            turno.TipoConfirmacionID = turno.TurnoOriginal.TipoConfirmacionID;
                            mantengoConfirmacion = true;
                        }
                    }
                    else if (exigeConfAut.Confirmacion)
                        turno.TipoConfirmacionID = (int)TipoConfirmacionEnum.AConfirmar;
                    #endregion

                    #region Autorizacion
                    // Chequeo si tengo original (puede ser una nueva practica en la reprogramacion)
                    if (turno.TurnoOriginal != null)
                    {
                        // Obtengo las Planillas del Turno Original
                        EntityCollection<AutorizacionPlanilla> planillas = TurnosDalc.AutorizacionPlanillaReadByTurno(turno.TurnoOriginalID.Value, false);

                        // Si estoy reprogramando un provisorio, mantengo el tipo de autorizacion
                        if (turno.TurnoOriginal.TipoTurnoId == (int)TipoTurnoEnum.Provisorio ||
                            turno.TurnoOriginal.TipoTurnoId == (int)TipoTurnoEnum.EnEspera)
                        {
                            turno.TipoAutorizacionID = turno.TurnoOriginal.TipoAutorizacionID;
                            mantengoAutorizacion = true;
                        }
                        else
                        {
                            bool osAutAuto = practicaInfo.ObraSocial.AutorizacionAutomatica && (turno.TurnoOriginal.TipoAutorizacionID == (int)TipoAutorizacionEnum.Autorizado
                                          || turno.TurnoOriginal.TipoAutorizacionID == (int)TipoAutorizacionEnum.NoAutorizado);
                            bool procAutAuto = turno.TurnoOriginal.TipoAutorizacionID == (int)TipoAutorizacionEnum.AAutorizarRemoto
                                            || turno.TurnoOriginal.TipoAutorizacionID == (int)TipoAutorizacionEnum.EsperaAutorizacionRemota;

                            /**
                             * Si es de Aut. Normal [No OS Aut. Auto. ni en Proceso de Aut. Auto.]
                             * - Si Cambio Practicas o OS o Plan, evaluo
                             * - Sino mantengo
                             * Sino, Si es de Aut. Auto. o esta en Proceso de Aut. Auto.
                             * - Si Cambio Practicas o OS o Plan, evaluo
                             * - Sino, si esta en Proc. de Aut. Auto, asocio las planillas originales al nuevo turno
                             * - Sino, si es de Aut. Auto., mantengo
                             * Sino, evaluo
                             * */
                            if (!osAutAuto && !procAutAuto)
                            {
                                if (cambios.CambioPracticas || cambios.CambioOSoPlan)
                                {
                                    if (exigeConfAut.Autorizacion)
                                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizar;
                                }
                                else
                                {
                                    turno.TipoAutorizacionID = turno.TurnoOriginal.TipoAutorizacionID;
                                    mantengoAutorizacion = true;
                                }
                            }
                            else if (osAutAuto || procAutAuto)
                            {
                                if (cambios.CambioPracticas || cambios.CambioOSoPlan)
                                {
                                    if (practicaInfo.PosibleTurno.TipoAutorizacion == TipoAutorizacionEnum.AAutorizarRemoto)
                                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizarRemoto;
                                    else if (exigeConfAut.Autorizacion)
                                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizar;

                                    // Cancelo las Planillas del Turno Original
                                    for (int i = 0; i < planillas.Count; i++)
                                    {
                                        AutorizacionPlanilla planilla = planillas[i];

                                        planilla.Enviar = false;

                                        planilla = TurnosDalc.AutorizacionPlanillaUpdate(planilla);
                                    }

                                    // Mandar Mail "CAMBIO DE CODIGOS"
                                    retorno = AutorizacionPlanilla.Mail.CambioCodigos;
                                }
                                else
                                {
                                    turno.TipoAutorizacionID = turno.TurnoOriginal.TipoAutorizacionID;
                                    mantengoAutorizacion = true;

                                    // Si es de Aut. Auto, debo enviar mail
                                    if (osAutAuto)
                                    {
                                        // Chequeo si alguna planilla ya fue impresa
                                        bool planillaImpresa = AutorizacionPlanilla.AlgunaImpresa(planillas);

                                        // Chequeo por cambios en el Equipo y/o el Informante
                                        bool cambioInformanteEquipo = !turno.EquipoId.HasValue ? true : (turno.EquipoId.Value != turno.TurnoOriginal.EquipoId.Value)
                                            || (practicaInfo.PosibleTurno.Medicos.Informante.Id != turno.TurnoOriginal.MedicoInformanteID.Value);

                                        // Si Cambio Informante o Equipo, envio mail de Cambio de Inf/Equipo
                                        // Si Cambio la Fecha, envio mail de Cambio de Fecha
                                        if (!practicaInfo.MantenerInformanteEquipo && cambioInformanteEquipo)
                                        {
                                            // Mandar Mail "CAMBIO DE INFORMANTE/EQUIPO"
                                            if (planillaImpresa)
                                                retorno = AutorizacionPlanilla.Mail.CambioInformanteEquipo;
                                            else
                                                retorno = AutorizacionPlanilla.Mail.CambioInformanteEquipoAdvertencia;
                                        }
                                        else if (cambios.CambioDia)
                                        {
                                            // Mandar Mail "CAMBIO DE FECHA"
                                            if (planillaImpresa)
                                                retorno = AutorizacionPlanilla.Mail.CambioFecha;
                                            else
                                                retorno = AutorizacionPlanilla.Mail.CambioFechaAdvertencia;
                                        }
                                    }
                                }
                            }
                            else if (practicaInfo.PosibleTurno.TipoAutorizacion == TipoAutorizacionEnum.AAutorizarRemoto)
                                turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizarRemoto;
                            else if (exigeConfAut.Autorizacion)
                                turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizar;
                        }

                        // Guardo las planillas en la PI para relacionarlas al nuevo turno
                        if (mantengoAutorizacion)
                            practicaInfo.Planillas = planillas;
                    }
                    else if (practicaInfo.PosibleTurno.TipoAutorizacion == TipoAutorizacionEnum.AAutorizarRemoto)
                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizarRemoto;
                    else if (exigeConfAut.Autorizacion)
                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizar;
                    #endregion
                }
                else
                {
                    if (practicaInfo.PosibleTurno.TipoAutorizacion == TipoAutorizacionEnum.AAutorizarRemoto)
                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizarRemoto;
                    else if (exigeConfAut.Autorizacion)
                        turno.TipoAutorizacionID = (int)TipoAutorizacionEnum.AAutorizar;

                    if (exigeConfAut.Confirmacion)
                        turno.TipoConfirmacionID = (int)TipoConfirmacionEnum.AConfirmar;
                }
            }

            return retorno;
        }

        public ExigeConfAut ObtenerExigeConfAut(PracticaInfo practicaInfo)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            ExigeConfAut result = new ExigeConfAut();

            PlanPracticaPrecioReserva ppp = practicaInfo.PlanPracticaPrecio;

            // Chequeo la Practica Principal
            result.Autorizacion = (ppp != null && ppp.ObraSocialPlan != null && ppp.ObraSocialPlan.ObraSocial != null && ppp.ExigeAutorizacion);
            result.Confirmacion = (ppp != null && ppp.ObraSocialPlan != null && ppp.ObraSocialPlan.ObraSocial != null && ppp.ExigeConfirmacion);

            // Chequeo las Adicionales
            if (practicaInfo.PracticasRelacionadas != null)
                foreach (PracticaAdicional pa in practicaInfo.PracticasRelacionadas)
                {
                    PlanPracticaPrecio pp = ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(practicaInfo.ObraSocialPlan.Id, pa.Adicional.Id, practicaInfo.PosibleTurno.Equipo != null ? practicaInfo.PosibleTurno.Equipo.Id : (int?)null, practicaInfo.PosibleTurno.StartDate);

                    result.Autorizacion = result.Autorizacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeAutorizacion);
                    result.Confirmacion = result.Confirmacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeConfirmacion);
                }

            // Chequeo las subsiguientes
            foreach (ExposicionInfo ei in practicaInfo.Exposiciones)
            {
                if (ei.Exposicion != null)
                {
                    PlanPracticaPrecio pp = ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(practicaInfo.ObraSocialPlan.Id, ei.Exposicion.Id, practicaInfo.PosibleTurno.Equipo != null ? practicaInfo.PosibleTurno.Equipo.Id : (int?)null, practicaInfo.PosibleTurno.StartDate);

                    result.Autorizacion = result.Autorizacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeAutorizacion);
                    result.Confirmacion = result.Confirmacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeConfirmacion);
                }
            }

            return result;
        }






        public ReadAllCollection<HorarioSinCaching> HorarioSinCachingReadAll()
        {
            return new ReadAllCollection<HorarioSinCaching>(dalEngine.GetAll<HorarioSinCaching>());
        }
        public ExigeConfAut ObtenerExigeConfAut(Turno turno)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            ExigeConfAut result = new ExigeConfAut();

            // Chequeo las Practicas del Turno
            EntityCollection<PracticaTurno> PTs = TurnosDalc.PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Todas);
            foreach (PracticaTurno pt in PTs)
            {
                PlanPracticaPrecio pp = ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(turno.Orden.ObraSocialPlanId, pt.Practica.Id, turno.EquipoId, turno.Fecha);

                result.Autorizacion = result.Autorizacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeAutorizacion);
                result.Confirmacion = result.Confirmacion || (pp != null && pp.Plan != null && pp.Plan.ObraSocial != null && pp.ExigeConfirmacion);
            }

            return result;
        }

        public CambiosPracticaInfo ObtenerCambios(PracticaInfo practicaInfo, Turno turnoViejo)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            CambiosPracticaInfo result = new CambiosPracticaInfo();

            // Chequeo si cambio OS o Plan
            if (turnoViejo.Orden.ObraSocialPlan == null)
                turnoViejo.Orden.ObraSocialPlan = ObrasSocialesDalc.ObraSocialPlanReadById(turnoViejo.Orden.ObraSocialPlanId);

            result.CambioOSoPlan = practicaInfo.ObraSocial.Id != turnoViejo.Orden.ObraSocialPlan.ObraSocial.Id || practicaInfo.ObraSocialPlan.Id != turnoViejo.Orden.ObraSocialPlanId;

            // Chequeo si hubo cambio en la practica principal
            PracticaTurno ptp = TurnosDalc.PracticaTurnoReadByTurno(turnoViejo.Id, PracticaTurnoTipoEnum.Principal)[0];
            result.CambioPracticas = practicaInfo.Practica.Id != ptp.Practica.Id || practicaInfo.Cantidad != ptp.Cantidad;

            // Guardo el Informante
            if (ptp.MedicoInformante != null)
                turnoViejo.MedicoInformanteID = ptp.MedicoInformante.Id;

            // Si no hubo cambio, chequeo cambios en los adicionales
            if (!result.CambioPracticas)
            {
                EntityCollection<PracticaTurno> adicionales = TurnosDalc.PracticaTurnoReadByTurno(turnoViejo.Id, PracticaTurnoTipoEnum.Adicional);

                // Si no hay la misma cantidad, algo se modificó
                int countRelacionadas;
                if (practicaInfo.PracticasRelacionadas == null)
                    countRelacionadas = 0;
                else
                    countRelacionadas = practicaInfo.PracticasRelacionadas.Count;
                if (countRelacionadas != adicionales.Count)
                    result.CambioPracticas = true;
                else
                {
                    // Chequeo que esten todas las mismas adicionales
                    if (practicaInfo.PracticasRelacionadas != null)
                        foreach (PracticaAdicional pa in practicaInfo.PracticasRelacionadas)
                        {
                            PracticaTurno pta = PracticaTurno.FindPractica(adicionales, pa.Adicional.Id);

                            if (pta == null)
                            {
                                result.CambioPracticas = true;
                                break;
                            }
                        }
                }
            }

            // Si no hubo cambio, chequeo cambios en las subsiguientes
            if (!result.CambioPracticas)
            {
                EntityCollection<PracticaTurno> subsiguientes = TurnosDalc.PracticaTurnoReadByTurno(turnoViejo.Id, PracticaTurnoTipoEnum.Exposicion);

                int cantSubsiguientes = 0;
                // Chequeo que esten todas las mismas subsiguientes, y con las mismas cantidades
                foreach (ExposicionInfo ei in practicaInfo.Exposiciones)
                {
                    if (ei.Exposicion != null)
                    {
                        cantSubsiguientes++;

                        PracticaTurno pte = PracticaTurno.FindPractica(subsiguientes, ei.Exposicion.Id);

                        if (pte != null)
                            result.CambioPracticas = ei.Cantidad != pte.Cantidad;
                        else
                            result.CambioPracticas = true;

                        if (result.CambioPracticas)
                            break;
                    }
                }

                // Si la cantidad no coincide, tambien es que algo se modifico
                if (cantSubsiguientes != subsiguientes.Count)
                    result.CambioPracticas = true;
            }

            // Chequeo si cambio la fecha
            if (practicaInfo.PosibleTurno != null)
                result.CambioDia = turnoViejo.Fecha.HasValue ? practicaInfo.PosibleTurno.StartDate.Date != turnoViejo.Fecha.Value.Date : false;

            return result;
        }

        [Private]
        [RequiresTransaction]
        public virtual void CancelarTurnos(EntityCollection<Turno> turnos, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            EstadoTurno statCancelado = TurnosDalc.EstadoTurnoReadById((int)EstadoTurnoEnum.Cancelado);

            foreach (Turno turno in turnos)
            {
                try
                {
                    if (turno.EstadoTurnoID != (int)EstadoTurnoEnum.Cancelado)
                    {
                        turno.MotivoID = (int)EstadoTurnoMotivoEnum.Reprogramacion;
                        TurnosDalc.TurnoAvanzarEstado(turno, statCancelado, modalidadCoseguro);
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Error al Cancelar el Turno Viejo." + Environment.NewLine + ex.Message, ex);
                }
            }
        }

        private Turno CreateTurno(Combo combo, Orden orden, PracticaInfo practicaInfo, PosibleTurno posibleTurno, bool esProvisorio, int tipoTurnoId, bool discapacidadTemporal, bool EmbarazadaOBebe, bool requierePediatra)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;

            Turno turno = new Turno();
            turno.EsGuardia = practicaInfo.EsGuardia;
            turno.Orden = orden;
            turno.ComboId = combo.Id;
            turno.EsHuerfano = false;
            turno.Activo = true;
            turno.ObservacionPaciente = practicaInfo.ObservacionPaciente;
            turno.Observaciones = practicaInfo.Observaciones;
            turno.TextoOsParticular = practicaInfo.TextoOSAlternativa;
            turno.RequierePediatra = requierePediatra;
            if (practicaInfo.ObraSocial.EsParticular)
                turno.TipoControlFacturacion = FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.Particular);
            else
                turno.TipoControlFacturacion = FacturacionDalc.TipoControlFacturacionReadById((int)TipoControlFacturacionEnum.NoControlado);
            turno.TipoInhibicionEntregaID = (int)TipoInhibicionEntregaEnum.NoInhibida;
            turno.EsSobreturno = false;
            turno.DiscapacidadTemporal = discapacidadTemporal;
            turno.EmbarazadaOBebe = EmbarazadaOBebe;

            // Asigno el Tipo de Turno y el Estado
            turno.TipoTurnoId = tipoTurnoId;
            if (turno.TipoTurnoId == (int)TipoTurnoEnum.Presupuesto)
                turno.EstadoTurnoID = (int)EstadoTurnoEnum.Ninguno;
            else
                turno.EstadoTurnoID = esProvisorio ? (int)EstadoTurnoEnum.ReservaProvisoria : (int)EstadoTurnoEnum.Reservado;

            // Sobreturno, Equipo, Fecha, Duracion y Fecha de Entrega del Informe
            if (!esProvisorio)
            {
                if (posibleTurno.Equipo != null)
                    turno.EquipoId = posibleTurno.Equipo.Id;

                if (turno.TipoTurnoId != (int)TipoTurnoEnum.Presupuesto)
                {

                    // Asigno el horario del turno
                    turno.TimePeriod = posibleTurno.TimePeriod;
                    // Asigna si el Turno fue asignado Por Tolerancia o Normalmente
                    turno.PorTolerancia = posibleTurno.PorTolerancia;
                    // 
                    turno.EsSobreturno = posibleTurno.EsSobreturno || turno.PorTolerancia;
                    // Asigna la duracion
                    turno.Duracion = posibleTurno.Length;

                    // Cálculo de días de informe
                    int diasInforme = practicaInfo.DiasInforme;

                    if (practicaInfo.PracticasRelacionadas != null && practicaInfo.PracticasRelacionadas.Count > 0)
                    {
                        foreach (PracticaAdicional practicaAdicional in practicaInfo.PracticasRelacionadas)
                        {
                            if (practicaAdicional.Adicional != null)
                                diasInforme += practicaAdicional.Adicional.DiasInforme;
                        }
                    }

                    // Datos que se van a utilizar en control diario    
                    turno.FechaEntregaInforme = TurnosDalc.DeterminarFechaEntregaInforme(turno, diasInforme);

                    turno.FechaControlDiario = turno.Fecha.Value;

                    if (turno.Equipo != null)
                        turno.CentroControlDiario = turno.Equipo.Sucursal.Id;
                }


                if (turno.Equipo == null && turno.EquipoId.HasValue)
                    turno.Equipo = EquiposDalc.EquipoReadById(turno.EquipoId.Value);
            }
            else
                turno.FechaEntregaInforme = null;

            // Contraste y Anestesia
            turno.CantPracticasContraste = 0;
            turno.CantPracticasAnestesia = 0;

            if (practicaInfo.PracticasRelacionadas != null)
            {
                foreach (PracticaAdicional pa in practicaInfo.PracticasRelacionadas)
                {
                    if (pa.Adicional.ServicioEspecialidad.Servicio.TipoServicio == (int)TipoServicioEnum.Contraste)
                        turno.CantPracticasContraste++;
                    else if (pa.Adicional.ServicioEspecialidad.Servicio.TipoServicio == (int)TipoServicioEnum.Anestesia)
                        turno.CantPracticasAnestesia++;
                }
            }

            return turno;
        }

        public Turno ObtenerTurnoViejo(EntityCollection<Turno> turnosViejos, int practicaID)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            Turno turnoViejo = null;

            for (int i = 0; turnoViejo == null && i < turnosViejos.Count; i++)
            {
                Turno turno = turnosViejos[i];

                PracticaTurno pt = TurnosDalc.PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Principal)[0];

                // Obtengo el turno viejo de la misma practica
                if (pt.Practica.Id == practicaID)
                    turnoViejo = turno;
            }

            return turnoViejo;
        }
        [RequiresTransaction]
        public virtual void InsertRespuestas(int turnoID, List<CondicionRespuestaInfo> respuestasAsociadas)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            TurnosDalc.CondicionTurnoDeleteByTurno(turnoID);

            EntityCollection<CondicionTurno> condiciones = new EntityCollection<CondicionTurno>();
            if (respuestasAsociadas != null)
                foreach (CondicionRespuestaInfo cri in respuestasAsociadas)
                {
                    CondicionTurno ct = new CondicionTurno();
                    ct.Condicion = cri.Respuesta.Condicion;
                    ct.Respuesta = cri.Respuesta;
                    ct.TurnoID = turnoID;
                    ct.Observaciones = cri.Observaciones;

                    condiciones.Add(ct);
                }

            if (condiciones.Count > 0)
            {
                condiciones = dalEngine.UpdateCollection<CondicionTurno>(condiciones);
            }
        }

        [RequiresTransaction]
        protected virtual List<PracticaTurno> InsertPracticas(Turno turno, PracticaInfo practicaInfo, PosibleTurno posibleTurno)
        {
            List<PracticaTurno> pTurnos = new List<PracticaTurno>();
            pTurnos.Add(InsertPrincipal(turno, practicaInfo, posibleTurno));
            // Inserto las Adicionales
            pTurnos.AddRange(InsertPracticasAdicionales(turno, practicaInfo, posibleTurno));

            // Inserto los Insumos
            if (practicaInfo.PracticasInsumos != null && practicaInfo.PracticasInsumos.Count > 0)
                IngresarInsumos(practicaInfo, pTurnos);

            InsertoExposicionesSubsiguientes(turno, practicaInfo, posibleTurno);
            return pTurnos;
        }

        private void IngresarInsumos(PracticaInfo practicaInfo, List<PracticaTurno> pTurnos)
        {
            foreach (PracticaTurno practicaTurno in pTurnos)
            {
                foreach (PracticaTurnoInsumo pi in practicaInfo.PracticasInsumos)
                {
                    if (pi.PracticaId == practicaTurno.Practica.Id)
                    {
                        pi.CantidadDeInsumos = pi.Insumo.VolumenUnitario;
                        pi.PracticaTurno = practicaTurno;


                        dalEngine.Update<PracticaTurnoInsumo>(pi);
                    }
                }
            }
        }

        private void InsertoExposicionesSubsiguientes(Turno turno, PracticaInfo practicaInfo, PosibleTurno posibleTurno)
        {
            // Inserto las subsiguientes
            foreach (ExposicionInfo exposicion in practicaInfo.Exposiciones)
            {
                if (exposicion.Exposicion != null)
                {
                    PracticaTurno practicaTurno = new PracticaTurno();
                    practicaTurno.TurnoId = turno.Id;
                    practicaTurno.Practica = exposicion.Exposicion;
                    practicaTurno.Cantidad = exposicion.Cantidad;
                    practicaTurno.Tipo = (int)PracticaTurnoTipoEnum.Exposicion;
                    practicaTurno.Medico = posibleTurno.Medicos.Actuante;
                    practicaTurno.MedicoInformante = posibleTurno.Medicos.Informante;
                    practicaTurno.MedicoTecnico = posibleTurno.Medicos.Tecnico;
                    practicaTurno.RegionInformeID = exposicion.Exposicion.RegionInformeId;

                    dalEngine.Update<PracticaTurno>(practicaTurno);
                }
            }
        }

        private List<PracticaTurno> InsertPracticasAdicionales(Turno turno, PracticaInfo practicaInfo, PosibleTurno posibleTurno)
        {
            if (practicaInfo.PracticasRelacionadas == null)
                return new List<PracticaTurno>();
            List<PracticaTurno> pTurnos = new List<PracticaTurno>();
            foreach (PracticaAdicional add in practicaInfo.PracticasRelacionadas)
            {
                Practica padre = dalEngine.GetById<Practica>(add.PracticaID);
                PracticaTurno practicaTurno = practicaTurno = new PracticaTurno();
                practicaTurno.TurnoId = turno.Id;
                practicaTurno.Practica = add.Adicional;
                practicaTurno.Cantidad = 1;
                if (padre.TipoPractica.Id == (int)TipoPracticaEnum.Modulo)
                    practicaTurno.PracticaAdicional = add;

                practicaTurno.Tipo = (int)PracticaTurnoTipoEnum.Adicional;
                practicaTurno.RegionInformeID = add.Adicional.RegionInformeId;
                if (add.ProlongaTurno)
                    practicaTurno.Duracion = add.Adicional.Duracion;

                practicaTurno.Medico = add.Medico;
                if (practicaTurno.Medico == null)
                    practicaTurno.Medico = posibleTurno.Medicos.Actuante;

                if (add.Adicional.RequiereProfesional)
                {
                    practicaTurno.MedicoInformante = (Medico)posibleTurno.MedicosPorPracticaAdicional[add.Id].Informante;
                    practicaTurno.Medico = (Medico)posibleTurno.MedicosPorPracticaAdicional[add.Id].Actuante;
                }
                else
                    practicaTurno.MedicoInformante = posibleTurno.Medicos.Informante;
                practicaTurno.MedicoTecnico = (Medico)posibleTurno.MedicosPorPracticaAdicional[add.Id].Tecnico;

                practicaTurno = dalEngine.Update(practicaTurno);
                pTurnos.Add(practicaTurno);
            }

            return pTurnos;
        }

        private PracticaTurno InsertPrincipal(Turno turno, PracticaInfo practicaInfo, PosibleTurno posibleTurno)
        {
            // Inserto la principal
            PracticaTurno practicaTurno = new PracticaTurno();
            practicaTurno.TurnoId = turno.Id;
            practicaTurno.Practica = practicaInfo.Practica;
            practicaTurno.Cantidad = practicaInfo.Cantidad;
            practicaTurno.Tipo = (int)PracticaTurnoTipoEnum.Principal;
            practicaTurno.RegionInformeID = practicaInfo.Practica.RegionInformeId;
            if (posibleTurno != null)
            {
                practicaTurno.Medico = posibleTurno.Medicos.Actuante;
                practicaTurno.MedicoInformante = posibleTurno.Medicos.Informante;
                practicaTurno.MedicoTecnico = posibleTurno.Medicos.Tecnico;
            }

            return dalEngine.Update<PracticaTurno>(practicaTurno);
        }

        private void CrearOrdenEncuesta(int servicioID, int ordenID)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Obtengo la Encuesta del Servicio
            EntityCollection<EncuestaPregunta> preguntas = TurnosDalc.EncuestaPreguntaReadVigentesByServicio(servicioID);

            // Si hay Preguntas, Inserto la encuesta
            if (preguntas.Count > 0)
            {
                // Creo la OrdenEncuesta
                OrdenEncuesta et = new OrdenEncuesta();
                et.OrdenID = ordenID;
                et.NoConforme = false;

                et.Respuestas = new EntityCollection<OrdenEncuestaRespuesta>();

                // Creo las OrdenEncuestaRespuesta por cada Pregunta
                foreach (EncuestaPregunta pregunta in preguntas)
                {
                    OrdenEncuestaRespuesta etr = new OrdenEncuestaRespuesta();
                    etr.EncuestaPregunta = pregunta;
                    etr.EncuestaRespuesta = null;
                    etr.OrdenEncuestaID = et.Id;

                    et.Respuestas.Add(etr);
                }

                et = TurnosDalc.OrdenEncuestaUpdateWithRespuestas(et);
            }
        }

        public EntityCollection<ServicioTiempoTraslado> ObtenerTiemposTraslado(List<int> servicios, SucursalName sucursal)
        {
            // Los busca tiempos de traslado que conecten la lista de servicios recibida
            Filter filter = new Filter();
            filter.Add(ServicioTiempoTraslado.Properties.ServicioOrigenId, "IN", servicios.ToArray());
            filter.Add(BooleanOp.Or, ServicioTiempoTraslado.Properties.ServicioDestinoId, "IN", servicios.ToArray());
            if (sucursal != null)
                filter.Add(BooleanOp.And, ServicioTiempoTraslado.Properties.SucursalId, "=", sucursal.Id);
            return dalEngine.GetManyByFilter<ServicioTiempoTraslado>(filter);
        }

        public EntityCollection<ServicioTiempoTrasladoName> ObtenerTiemposTraslado(ServicioTiempoTrasladoName tiempoTraslado)
        {
            //Devuelve una coleccion que matchee por suc + servOrigen + servDestino + equipoOrigen + equipoDestino
            //y NO sea el que se pasa por parametro (es decir, si existe algun TiempoTraslado 
            if (tiempoTraslado == null)
                return null;

            Filter filter = new Filter();
            filter.Add(ServicioTiempoTrasladoName.Properties.Suc.Id, "=", tiempoTraslado.Suc.Id);
            filter.Add(BooleanOp.And, ServicioTiempoTrasladoName.Properties.SerIdOrigen.Id, "=", tiempoTraslado.SerIdOrigen.Id);
            filter.Add(BooleanOp.And, ServicioTiempoTrasladoName.Properties.SerIdDestino.Id, "=", tiempoTraslado.SerIdDestino.Id);
            if (tiempoTraslado.EquIdOrigen != null)
                filter.Add(BooleanOp.And, ServicioTiempoTrasladoName.Properties.EquIdOrigen.Id, "=", tiempoTraslado.EquIdOrigen.Id);
            if (tiempoTraslado.EquIdDestino != null)
                filter.Add(BooleanOp.And, ServicioTiempoTrasladoName.Properties.EquIdDestino.Id, "=", tiempoTraslado.EquIdDestino.Id);
            filter.Add(BooleanOp.And, ServicioTiempoTrasladoName.Properties.Id, "<>", tiempoTraslado.Id);

            return dalEngine.GetManyByFilter<ServicioTiempoTrasladoName>(filter);
        }


        #region Leyendas

        [RequiresTransaction]
        public virtual EntityCollection<Leyenda> LeyendaUpdateMany(EntityCollection<Leyenda> leyendas)
        {
            // Elimino los que se eliminaron
            if (leyendas.DeletedItems.Count > 0)
            {
                for (int i = 0; i < leyendas.DeletedItems.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (leyendas.DeletedItems[i].Id > 0)
                    {
                        dalEngine.Delete(leyendas.DeletedItems[i]);
                        dalEngine.Delete(leyendas.DeletedItems[i].Horario);
                    }
                }
            }

            for (int i = 0; i < leyendas.Count; i++)
            {
                if (leyendas[i].Id == 0)
                {

                    // Inserto
                    leyendas[i].Horario = dalEngine.Update<Horario>(leyendas[i].Horario);
                    leyendas[i] = dalEngine.Update(leyendas[i]);

                    // Seteo el Grupo
                    leyendas[i].Grupo = leyendas[i].Horario.Id;
                    leyendas[i] = dalEngine.Update(leyendas[i]);
                }
            }
            return leyendas;
        }

        public EntityCollection<Leyenda> LeyendaReadByEquiposOrMedicosAndDate(IEnumerable<int> equipoIds, IEnumerable<int> medicoIds, DateTime? from, DateTime? to)
        {

            Filter filter = new Filter();
            bool hasEquipos = equipoIds != null && equipoIds.GetEnumerator().MoveNext();
            bool hasMedicos = medicoIds != null && medicoIds.GetEnumerator().MoveNext();
            if (hasEquipos || hasMedicos)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                if (hasEquipos)
                    filter.Add(Leyenda.Properties.Equipo.Id, "IN", equipoIds);
                if (hasMedicos)
                    filter.Add(BooleanOp.Or, Leyenda.Properties.Medico.Id, "IN", medicoIds);

                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            // [JR] Chequeo que el FROM o el TO este en el horario de alguna leyenda
            if (from.HasValue && to.HasValue)
            {
                HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, Leyenda.Properties.Horario,
                    from.Value, to.Value);
            }

            ReadManyCommand<Leyenda> readCmd = new ReadManyCommand<Leyenda>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }
        #endregion

        #region Presupuestos


        public List<int> BusquedaPresupuestoItemReadByParameters(List<int> presupuestosIds, int? nroPresupuesto, string obraSocial, string paciente, string servicio, bool mostrarVigentes, bool mostrarVencidos, int diasVigencia, bool mostrarConTurno)
        {
            if (presupuestosIds == null || presupuestosIds.Count <= 0)
                return new List<int>();

            List<int> presusCancelados = null;

            var turnos = from tur in dalEngine.Query<Turno>()
                         where presupuestosIds.Contains(tur.Id)
                         select tur;

            if (nroPresupuesto.HasValue)
                turnos = from tur in turnos
                         join val in dalEngine.Query<Entities.Valorizacion>() on tur.Id equals val.Turno.Id
                         where val.Tipo.Id == (int)ValorizacionTiposEnum.Presupuesto && val.Deleted == false && val.NroPresupuesto == nroPresupuesto.Value
                         select tur;

            if (!String.IsNullOrEmpty(obraSocial))
                turnos = from tur in turnos
                         join osp in dalEngine.Query<Entities.ObraSocialPlan>() on tur.Orden.ObraSocialPlanId equals osp.Id
                         where osp.ObraSocial.Name.Contains(obraSocial)
                         select tur;

            if (!String.IsNullOrEmpty(paciente))
                turnos = from tur in turnos
                         join pac in dalEngine.Query<Entities.Paciente>() on tur.Orden.PacienteId equals pac.Id
                         where pac.ApellidoNombre.Contains(paciente)
                         select tur;

            if (!String.IsNullOrEmpty(servicio))
                turnos = from tur in turnos
                         join pt in dalEngine.Query<Entities.PracticaTurno>() on tur.Id equals pt.TurnoId
                         where pt.Tipo == (int)PracticaTurnoTipoEnum.Principal && pt.Practica.ServicioEspecialidad.Servicio.Name.Contains(servicio)
                         select tur;

            if (!mostrarVigentes)
            {
                presusCancelados = PresupuestosCanceladosReadByIds(presupuestosIds);

                // Si esta vacio y lo incluyo da error de SQL :S
                if (presusCancelados.Count > 0)
                    turnos = from tur in turnos where (presusCancelados.Contains(tur.Id) || tur.EstadoTurnoID == (int)EstadoTurnoEnum.ConTurnoAsignado || enfoke.Time.Today < tur.CreateDate || enfoke.Time.Today.AddDays((-1) * diasVigencia) > tur.CreateDate) select tur;
                else
                    turnos = from tur in turnos where (tur.EstadoTurnoID == (int)EstadoTurnoEnum.ConTurnoAsignado || enfoke.Time.Today.AddDays(1) < tur.CreateDate || enfoke.Time.Today.AddDays((-1) * diasVigencia) > tur.CreateDate.Value) select tur;
            }

            if (!mostrarVencidos)
            {
                if (presusCancelados == null)
                    presusCancelados = PresupuestosCanceladosReadByIds(presupuestosIds);

                // Si esta vacio y lo incluyo da error de SQL :S
                if (presusCancelados.Count > 0)
                    turnos = from tur in turnos where (presusCancelados.Contains(tur.Id) || tur.EstadoTurnoID == (int)EstadoTurnoEnum.ConTurnoAsignado || enfoke.Time.Today.AddDays((-1) * diasVigencia) < tur.CreateDate) select tur;
                else
                    turnos = from tur in turnos where (tur.EstadoTurnoID == (int)EstadoTurnoEnum.ConTurnoAsignado || enfoke.Time.Today.AddDays((-1) * diasVigencia) < tur.CreateDate) select tur;
            }

            if (!mostrarConTurno)
            {
                if (presusCancelados == null)
                    presusCancelados = PresupuestosCanceladosReadByIds(presupuestosIds);

                // Si esta vacio y lo incluyo da error de SQL :S
                if (presusCancelados.Count > 0)
                    turnos = from tur in turnos where presusCancelados.Contains(tur.Id) || tur.EstadoTurnoID != (int)EstadoTurnoEnum.ConTurnoAsignado select tur;
                else
                    turnos = from tur in turnos where tur.EstadoTurnoID != (int)EstadoTurnoEnum.ConTurnoAsignado select tur;
            }

            return turnos.OrderBy(turno => turno.CreateDate).Select(turno => turno.Id).ToList();
        }

        private List<int> PresupuestosCanceladosReadByIds(List<int> presupuestosIds)
        {
            List<int> presusCancelados;
            presusCancelados = (from pre in dalEngine.Query<Turno>()
                                join est in dalEngine.Query<EstadoTurno>() on pre.EstadoTurnoID equals est.Id
                                where presupuestosIds.Contains(pre.Id) && est.Cancelado == true
                                select pre.Id).ToList();
            return presusCancelados;
        }

        public EntityCollection<BusquedaPresupuestoItem> BusquedaPresupuestoItemReadByIds(List<int> presupuestosIds)
        {
            if (presupuestosIds == null || presupuestosIds.Count <= 0)
                return new EntityCollection<BusquedaPresupuestoItem>();

            var turNroPre = from val in dalEngine.Query<Entities.Valorizacion>()
                            join pac in dalEngine.Query<Entities.Paciente>() on val.Turno.Orden.PacienteId equals pac.Id
                            join pt in dalEngine.Query<Entities.PracticaTurno>() on val.Turno.Id equals pt.TurnoId
                            join osp in dalEngine.Query<Entities.ObraSocialPlan>() on val.Turno.Orden.ObraSocialPlanId equals osp.Id
                            where presupuestosIds.Contains(val.Turno.Id)
                                && val.Tipo.Id == (int)ValorizacionTiposEnum.Presupuesto
                                && val.Deleted == false
                                && pt.Tipo == (int)PracticaTurnoTipoEnum.Principal
                            select new BusquedaPresupuestoItem(val.NroPresupuesto, val.Turno.Id, val.Turno.CreateDate, pac.ApellidoNombre, pt.Practica.Name, osp.ObraSocial.Name, String.Empty, pt.Practica.ServicioEspecialidad.Servicio.Name, val.Turno.EstadoTurnoID);
            EntityCollection<BusquedaPresupuestoItem> result = turNroPre.ToEntityCollection();

            Dictionary<int, int> turVinculados = GetTurnosVinculados(presupuestosIds);
            Dictionary<int, string> turCentro = GetTurnosVinculadosCentros(turVinculados.Values);


            for (int i = result.Count - 1; i >= 0; i--)
            {
                if (turVinculados.ContainsKey(result[i].TurnoId))
                {
                    result[i].TurnoCargadoId = turVinculados[result[i].TurnoId];
                    result[i].Centro = turCentro[result[i].TurnoCargadoId.Value];
                }
            }

            return result;
        }

        private Dictionary<int, string> GetTurnosVinculadosCentros(Dictionary<int, int>.ValueCollection valueCollection)
        {
            if (valueCollection.Count <= 0)
                return new Dictionary<int, string>();

            var turnosCentro = from tur in dalEngine.Query<Turno>()
                               join equ in dalEngine.Query<Equipo>() on tur.EquipoId equals equ.Id
                               where valueCollection.Contains(tur.Id)
                               select new { Tur = tur.Id, Centro = equ.Sucursal.Name };
            return turnosCentro.ToDictionary(je => je.Tur, je => je.Centro);
        }

        private Dictionary<int, int> GetTurnosVinculados(List<int> presupuestosIds)
        {
            var vinculados = from pre in dalEngine.Query<Turno>()
                             join tur in dalEngine.Query<Turno>() on pre.Id equals tur.PresupuestoId
                             join est in dalEngine.Query<EstadoTurno>() on tur.EstadoTurnoID equals est.Id
                             where presupuestosIds.Contains(pre.Id) && est.Cancelado == false
                             select new { Pre = pre.Id, Tur = tur.Id };
            return vinculados.ToDictionary(je => je.Pre, je => je.Tur);
        }

        /// <summary>
        /// Obtiene el siguiente numero de presupuesto de los parametros.
        /// </summary>
        /// <param name="user">Usuario de la Operación</param>
        /// <returns>El número de presupuesto a asignar</returns>
        public int ObtenerSiguienteNumeroPresupuesto(SecurityUser user)
        {
            ConfigurationDalc ConfigurationDalc = Context.Session.ConfigurationDalc;

            ParametroView param = ConfigurationDalc.ParametroReadByNombre("NRO PRESUPUESTO");

            if (param != null)
            {
                // Obtengo e Incremento el Nro de Presupuesto
                int numero = (int)param.ValorParseado + 1;

                // Actualizo el Valor del Parametro
                ParametroUpdate pu = new ParametroUpdate(param);
                pu.Valor = numero.ToString();
                ConfigurationDalc.ParametroUpdate(pu);

                // Retorno el Nro Actual
                return numero;
            }
            else
                throw new Exception("Error de Parametrización!" + Environment.NewLine + "No se encuentran los números de presupuesto.");
        }

        #endregion


    }
}

