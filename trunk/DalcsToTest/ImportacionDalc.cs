using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;
using enfoke.Eges.Utils;
using enfoke.Data.Filters;
using enfoke.Eges.Entities.Configuracion;
using NHibernate;

using enfoke.Eges.Valorizacion;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class ImportacionDalc : Dalc, IService
    {

        protected ImportacionDalc(NotConstructable dummy) : base(dummy) { }

        public ReadAllCollection<ImportacionConversionPractica> ImportacionConversionPracticaReadAllNotDeleted
            (ImportacionTipoImportacionEnum tipoImportacion, List<int> idsServicio)
        {
            if (tipoImportacion == ImportacionTipoImportacionEnum.Odontologia)
            {
                EntityCollection<ImportacionConversionPractica> practicas =
                    dalEngine.GetManyByProperty<ImportacionConversionPractica>
                        (ImportacionConversionPractica.Properties.Deleted, false);

                return new ReadAllCollection<ImportacionConversionPractica>(practicas);
            }
            else if (tipoImportacion == ImportacionTipoImportacionEnum.Laboratorio)
            {
                string hql = "SELECT new PracticaName(p.Id, p.Name, p.Code, p.DuracionSeconds, p.TipoPractica.Id) " +
                             "FROM Practica p " +
                             "WHERE p.Deleted = false " +
                             "AND p.ServicioEspecialidad.Servicio.Id IN (:idsServicio) " +
                             "ORDER BY p.Region ASC ";

                IQuery query = dalEngine.CreateQuery(hql);
                query.SetParameterList("idsServicio", idsServicio);

                EntityCollection<PracticaName> practicas = dalEngine.GetManyByQuery<PracticaName>(query);

                EntityCollection<PracticaName> practicasMinimaRegion = GetPracticasWithMinimaRegion(practicas);

                EntityCollection<ImportacionConversionPractica> conversionPracticas = new EntityCollection<ImportacionConversionPractica>();

                foreach (PracticaName practica in practicasMinimaRegion)
                    conversionPracticas.Add(new ImportacionConversionPractica(practica));

                return new ReadAllCollection<ImportacionConversionPractica>(conversionPracticas);
            }

            return null;
        }

        private EntityCollection<PracticaName> GetPracticasWithMinimaRegion(EntityCollection<PracticaName> practicas)
        {
            EntityCollection<PracticaName> result = new EntityCollection<PracticaName>();

            foreach (PracticaName practica in practicas)
            {
                #region Predicate
                Predicate<PracticaName> predicate = delegate(PracticaName compare)
                { return compare.Code == practica.Code; };
                #endregion

                if (result.Find(predicate) == null)
                    result.Add(practica);
            }

            return result;
        }

        public ReadAllCollection<ImportacionConversionObraSocial> ImportacionConversionObraSocialReadAllNotDeleted()
        {
            EntityCollection<ImportacionConversionObraSocial> planes =
                dalEngine.GetManyByProperty<ImportacionConversionObraSocial>
                (ImportacionConversionObraSocial.Properties.Deleted, false);

            return new ReadAllCollection<ImportacionConversionObraSocial>(planes);
        }






        public ReadAllCollection<ImportacionTipoError> ImportacionTipoErrorReadAll()
        {
            EntityCollection<ImportacionTipoError> tiposError = dalEngine.GetAll<ImportacionTipoError>();

            return new ReadAllCollection<ImportacionTipoError>(tiposError);
        }

        [Private]
        public EntityCollection<ImportacionParametro> ImportacionParametroReadByTipoImportacion(ImportacionTipoImportacionEnum tipoImportacion)
        {
            return dalEngine.GetManyByProperty<ImportacionParametro>(
                ImportacionParametro.Properties.ImportacionTipoImportacionID, (int)tipoImportacion);
        }

        [Private]
        public EntityCollection<IParametro> IParametroReadImportacionParametroByTipoImportacion(ImportacionTipoImportacionEnum tipoImportacion)
        {
            EntityCollection<ImportacionParametro> parametros =
                dalEngine.GetManyByProperty<ImportacionParametro>(
                ImportacionParametro.Properties.ImportacionTipoImportacionID, (int)tipoImportacion);
            EntityCollection<IParametro> result = new EntityCollection<IParametro>();

            foreach (ImportacionParametro parametro in parametros)
                result.Add(parametro);

            return result;
        }

        /// <summary>
        /// Devuelve un paciente de la base si coincide en apellido, nombre, sexo, tipoDocumento, dni y fechaDeNacimiento y no esta borrado.
        /// </summary>
        /// <param name="paciente">Paciente a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? uno, caso contrario el paciente.</returns>
        [Private]
        public Paciente PacienteReadByPaciente(Paciente paciente, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(Paciente.Properties.ApellidoNombre, " = ", paciente.ApellidoNombre));

            filter.Add(BooleanOp.And, Paciente.Properties.Sexo, " = ", (int)paciente.Sexo);
            filter.Add(BooleanOp.And, Paciente.Properties.TipoDocumentoId, " = ", paciente.TipoDocumentoId);
            filter.Add(BooleanOp.And, Paciente.Properties.Dni, " = ", paciente.Dni);

            if (paciente.FechaDeNacimiento.HasValue)
                filter.Add(BooleanOp.And, Paciente.Properties.FechaDeNacimiento, " = ", paciente.FechaDeNacimiento.Value);

            if (deletedToo == false)
                filter.Add(BooleanOp.And, Paciente.Properties.Deleted, " = ", false);

            return dalEngine.GetByFilter<Paciente>(filter);
        }

        /// <summary>
        /// Devuelve un medicoAsociacion de la base si coincide en apellido, nombre, matriculaNacional, matriculaProvincial y no esta borrado.
        /// </summary>
        /// <param name="medicoAsociacion">MedicoAsociacion a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? uno, caso contrario el medicoAsociacion.</returns>
        [Private]
        public MedicoAsociacion MedicoAsociacionReadByMedicoAsociacion(MedicoAsociacion medicoAsociacion, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(MedicoAsociacion.Properties.LastName, " = ", medicoAsociacion.LastName));

            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.FirstName, " = ", medicoAsociacion.FirstName);
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.MatriculaNacional, " = ", medicoAsociacion.MatriculaNacional);
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.MatriculaProvincial, " = ", medicoAsociacion.MatriculaProvincial);

            if (deletedToo == false)
                filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, " = ", false);

            return dalEngine.GetByFilter<MedicoAsociacion>(filter);
        }

        /// <summary>
        /// Devuelve un protocolo de la base si coincide en numero, sucursal y servicioTag.
        /// </summary>
        /// <param name="protocolo">Protocolo a buscar en la base.</param>
        /// <returns>Null si no encontr? uno, caso contrario el protocolo.</returns>
        [Private]
        public Protocolo ProtocoloReadByProtocolo(Protocolo protocolo)
        {
            Filter filter = new Filter(new FilterItem(Protocolo.Properties.Numero, " = ", protocolo.Numero));

            filter.Add(BooleanOp.And, Protocolo.Properties.SucursalID, " = ", protocolo.SucursalID);
            filter.Add(BooleanOp.And, Protocolo.Properties.Origen, " = ", protocolo.Origen);

            return dalEngine.GetByFilter<Protocolo>(filter);
        }

        /// <summary>
        /// Devuelve un turno de la base si coincide en equipo, paciente, fecha, fechaEntregaInforme, tipoTurno, medicoSolicitante, obraSocialPlan, protocolo y no esta cancelado.
        /// </summary>
        /// <param name="turno">Turno a buscar en la base.</param>
        /// <returns>Null si no encontr? uno, caso contrario el turno.</returns>
        [Private]
        public Turno TurnoReadByTurno(Turno turno)
        {
            /*Filter filter = new Filter(new FilterItem(Turno.Properties.EstadoTurnoID, " != ", (int)EstadoTurnoEnum.Cancelado));

            filter.Add(BooleanOp.And, Turno.Properties.EquipoId, " = ", turno.EquipoId);
            filter.Add(BooleanOp.And, Turno.Properties.Orden.PacienteId, " = ", turno.Orden.PacienteId);
            filter.Add(BooleanOp.And, Turno.Properties.Fecha, " = ", turno.Fecha);
            filter.Add(BooleanOp.And, Turno.Properties.FechaEntregaInforme, " = ", turno.FechaEntregaInforme);
            filter.Add(BooleanOp.And, Turno.Properties.TipoTurnoId, " = ", turno.TipoTurnoId);
            filter.Add(BooleanOp.And, Turno.Properties.Orden.MedicoSolicitanteID, " = ", turno.Orden.MedicoSolicitanteID);
            filter.Add(BooleanOp.And, Turno.Properties.Orden.ObraSocialPlanId, " = ", turno.Orden.ObraSocialPlanId);
            filter.Add(BooleanOp.And, Turno.Properties.Orden.Protocolo.ProtocoloFull, " = ", turno.Orden.Protocolo.ProtocoloFull);

            EntityCollection<Turno> turnos = dalEngine.GetManyByFilter<Turno>(filter);

            Turno turnoResult = null;

            if (turnos != null && turnos.Count > 0)
                turnoResult = turnos[0];

            // Si encontr? uno en la base, entonces cargo propiedades
            if (turnoResult != null)
            {
                turnoResult.Orden.Paciente = Context.Session.TurnosDalc.PacienteReadById(turnoResult.Orden.PacienteId);

                if (turnoResult.Orden.MedicoSolicitanteID.HasValue)
                    turnoResult.Orden.MedicoSolicitante = Context.Session.MedicosDalc.MedicoAsociacionReadById(turnoResult.Orden.MedicoSolicitanteID.Value);
            }

            return turnoResult;*/

            return TurnoReadByProtocolo(turno.Orden.Protocolo);
        }

        /// <summary>
        /// Devuelve un turno de la base si coincide en el codigoDeProtocolo y no est? cancelado.
        /// </summary>
        /// <param name="protocolo">Protocolo en base al cual buscar turnos.</param>
        /// <returns>Null si no encontr? uno, caso contrario el turno.</returns>
        [Private]
        public Turno TurnoReadByProtocolo(Protocolo protocolo)
        {
            Filter filter = new Filter(new FilterItem(Turno.Properties.EstadoTurnoID, " != ", (int)EstadoTurnoEnum.Cancelado));

            filter.Add(BooleanOp.And, Turno.Properties.Orden.Protocolo.ProtocoloFull, " = ", protocolo.ProtocoloFull);

            EntityCollection<Turno> turnos = dalEngine.GetManyByFilter<Turno>(filter);

            Turno turnoResult = null;

            // Si encontr? turnos, tomo el primero. (Para el caso puntual de importaci?n, una orden tiene solo un turno)
            if (turnos != null && turnos.Count > 0)
                foreach (Turno tur in turnos)
                    if (turnoResult == null && (EstadoTurnoEnum)tur.EstadoTurnoID != EstadoTurnoEnum.Cancelado)
                        turnoResult = tur;

            if (turnoResult == null && turnos != null && turnos.Count > 0)
                turnoResult = turnos[0];

            // Si encontr? uno en la base, entonces cargo propiedades
            if (turnoResult != null)
            {
                turnoResult.Orden.Paciente = Context.Session.TurnosDalc.PacienteReadById(turnoResult.Orden.PacienteId);

                if (turnoResult.Orden.MedicoSolicitanteID.HasValue)
                    turnoResult.Orden.MedicoSolicitante = Context.Session.MedicosDalc.MedicoAsociacionReadById(turnoResult.Orden.MedicoSolicitanteID.Value);
            }

            return turnoResult;
        }

        /// <summary>
        /// Devuelve un turnoLog de la base si coincide en turnoId.
        /// </summary>
        /// <param name="turnoLog">TurnoLog a buscar en la base.</param>
        /// <returns>Null si no encontr? uno, caso contrario el turnoLog.</returns>
        [Private]
        public TurnoLog TurnoLogReadByTurnoLog(TurnoLog turnoLog)
        {
            return TurnoLogReadByTurnoId(turnoLog.TurnoId);
        }

        /// <summary>
        /// Devuelve un turnoLog de la base si coincide en turnoId.
        /// </summary>
        /// <param name="turnoLog">TurnoLog a buscar en la base.</param>
        /// <returns>Null si no encontr? uno, caso contrario el turnoLog.</returns>
        [Private]
        public TurnoLog TurnoLogReadByTurno(Turno turno)
        {
            return TurnoLogReadByTurnoId(turno.Id);
        }

        /// <summary>
        /// Devuelve un turnoLog de la base si coincide en turnoId.
        /// </summary>
        /// <param name="turnoLog">TurnoLog a buscar en la base.</param>
        /// <returns>Null si no encontr? uno, caso contrario el turnoLog.</returns>
        [Private]
        public TurnoLog TurnoLogReadByTurnoId(int turnoId)
        {
            return dalEngine.GetByProperty<TurnoLog>(TurnoLog.Properties.TurnoId, turnoId);
        }

        /// <summary>
        /// Devuelve una practicaTurno de la base si coincide en practicaId y turnoId.
        /// </summary>
        /// <param name="practicaTurno">PracticaTurno a buscar en la base.</param>
        /// <returns>Null si no encontr? una, caso contrario la practicaTurno.</returns>
        [Private]
        public PracticaTurno PracticaTurnoReadByPracticaTurno(PracticaTurno practicaTurno)
        {
            Filter filter = new Filter(new FilterItem(PracticaTurno.Properties.Practica.Id, " = ", practicaTurno.Practica.Id));

            filter.Add(BooleanOp.And, PracticaTurno.Properties.TurnoId, " = ", practicaTurno.TurnoId);

            /* No valido la Cantidad, porque lo pudieron haber modificado en la pantalla de valorizacion.
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Cantidad, " = ", practicaTurno.Cantidad);*/

            /* No valido el Tipo, porque as? sean de distinto tipo las practicasTurno, no puedo repetir iguales practicas.
            filter.Add(BooleanOp.And, PracticaTurno.Properties.Tipo, " = ", practicaTurno.Tipo);*/

            return dalEngine.GetByFilter<PracticaTurno>(filter);
        }

        /// <summary>
        /// Devuelve las practicasTurno de la base si coinciden en turnoId.
        /// </summary>
        /// <param name="practicaTurno">PracticaTurno a buscar en la base.</param>
        /// <returns>Null si no encontr? una, caso contrario las practicasTurno.</returns>
        [Private]
        public EntityCollection<PracticaTurno> PracticaTurnoReadByTurno(Turno turno)
        {
            EntityCollection<PracticaTurno> PracticasTurno =
                dalEngine.GetManyByProperty<PracticaTurno>(PracticaTurno.Properties.TurnoId, turno.Id);

            if (PracticasTurno != null && PracticasTurno.Count > 0)
                return PracticasTurno;

            return null;
        }

        /// <summary>
        /// Devuelve una valorizacion de la base si coincide en turnoId, tipo, obraSocialPlan y no esta borrada.
        /// </summary>
        /// <param name="valorizacion">Valorizacion a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? una, caso contrario la valorizacion.</returns>
        [Private]
        public Entities.Valorizacion ValorizacionReadByValorizacion(Entities.Valorizacion valorizacion, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(Entities.Valorizacion.Properties.Turno.Id, " = ", valorizacion.Turno.Id));

            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Tipo.Id, " = ", valorizacion.Tipo.Id);
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.ObraSocialPlan.Id, " = ", valorizacion.ObraSocialPlan.Id);

            if (deletedToo == false)
                filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Deleted, " = ", false);

            /* No valido el TipoPlan, porque lo pudieron haber modificado en la pantalla de valorizacion.
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.TipoPlan, " = ", valorizacion.TipoPlan);*/

            return dalEngine.GetByFilter<Entities.Valorizacion>(filter);
        }

        /// <summary>
        /// Devuelve las valorizaciones de la base si coinciden en turnoId.
        /// </summary>
        /// <param name="valorizacion">Valorizacion a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? una, caso contrario las valorizaciones.</returns>
        [Private]
        public EntityCollection<Entities.Valorizacion> ValorizacionReadByTurno(Turno turno, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(Entities.Valorizacion.Properties.Turno.Id, " = ", turno.Id));

            if (deletedToo == false)
                filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Deleted, " = ", false);

            return dalEngine.GetManyByFilter<Entities.Valorizacion>(filter);
        }

        /// <summary>
        /// Devuelve una valorizacionItem de la base si coincide en valorizacionId, practicaTurnoId, planPracticaUsadoId y no esta borrada.
        /// </summary>
        /// <param name="valorizacionItem">ValorizacionItem a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? una, caso contrario la valorizacionItem.</returns>
        [Private]
        public ValorizacionItem ValorizacionItemReadByValorizacionItem(ValorizacionItem valorizacionItem, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(ValorizacionItem.Properties.Valorizacion.Id, " = ", valorizacionItem.Valorizacion.Id));

            filter.Add(BooleanOp.And, ValorizacionItem.Properties.PracticaTurno.Id, " = ", valorizacionItem.PracticaTurno.Id);
            filter.Add(BooleanOp.And, ValorizacionItem.Properties.PlanPracticaUsadoId, " = ", valorizacionItem.PlanPracticaUsadoId);

            if (deletedToo == false)
                filter.Add(BooleanOp.And, ValorizacionItem.Properties.DeleteDate, " IS ", null);

            /* No valido la Cantidad, porque lo pudieron haber modificado en la pantalla de valorizacion.
            filter.Add(BooleanOp.And, ValorizacionItem.Properties.Cantidad, " = ", valorizacionItem.Cantidad);*/

            return dalEngine.GetByFilter<ValorizacionItem>(filter);
        }

        /// <summary>
        /// Devuelve las valorizacionesItems de la base si coinciden en valorizacionId y no estan borradas.
        /// </summary>
        /// <param name="valorizacionItem">ValorizacionItem a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? una, caso contrario las valorizacionesItems.</returns>
        [Private]
        public EntityCollection<ValorizacionItem> ValorizacionItemReadByValorizacion(Entities.Valorizacion valorizacion, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(ValorizacionItem.Properties.Valorizacion.Id, " = ", valorizacion.Id));

            if (deletedToo == false)
                filter.Add(BooleanOp.And, ValorizacionItem.Properties.DeleteDate, " IS ", null);

            return dalEngine.GetManyByFilter<ValorizacionItem>(filter);
        }

        /// <summary>
        /// Devuelve las valorizacionesItems de la base si coinciden en turnoId y no estan borradas.
        /// </summary>
        /// <param name="valorizacionItem">ValorizacionItem a buscar en la base.</param>
        /// <param name="deletedToo">Considera o no los borrados al momento de buscar.</param>
        /// <returns>Null si no encontr? una, caso contrario las valorizacionesItems.</returns>
        [Private]
        public EntityCollection<ValorizacionItem> ValorizacionItemReadByTurno(Turno turno, bool deletedToo)
        {
            Filter filter = new Filter(new FilterItem(ValorizacionItem.Properties.Valorizacion.Turno.Id, " = ", turno.Id));

            if (deletedToo == false)
                filter.Add(BooleanOp.And, ValorizacionItem.Properties.DeleteDate, " IS ", null);

            return dalEngine.GetManyByFilter<ValorizacionItem>(filter);
        }

        [Private]
        public void ImportacionLogLogicoSave(ImportacionLogLogico registro, ReservasDalc reservasDalc, ObrasSocialesDalc obrasSocialesDalc, TurnosDalc turnosDalc, ValorizacionesDalc valorizacionesDalc)
        {

            // Save paciente.
            registro.PacienteValidado = dalEngine.Update<Paciente>(registro.PacienteValidado);
            registro.TurnoValidado.Orden.PacienteId = registro.PacienteValidado.Id;


            // Save medicoSolicitante.
            registro.MedicoSolicitanteValidado = dalEngine.Update<MedicoAsociacion>(registro.MedicoSolicitanteValidado);
            registro.TurnoValidado.Orden.MedicoSolicitanteID = registro.MedicoSolicitanteValidado.Id;


            // Save protocolo.
            registro.ProtocoloValidado = dalEngine.Update<Protocolo>(registro.ProtocoloValidado);
            registro.TurnoValidado.Orden.Protocolo = registro.ProtocoloValidado;

            // Save combo para el turno.
            Combo combo = reservasDalc.ComboGetNew();
            registro.TurnoValidado.ComboId = combo.Id;

            // Save orden.
            registro.TurnoValidado.Orden = dalEngine.Update<Orden>(registro.TurnoValidado.Orden);


            // Save turno.
            registro.TurnoValidado = dalEngine.Update<Turno>(registro.TurnoValidado);
            registro.TurnoValidado.Orden.ObraSocialPlan = obrasSocialesDalc.ObraSocialPlanReadById(registro.TurnoValidado.Orden.ObraSocialPlanId);
            foreach (PracticaTurno pt in registro.PracticasTurnoValidado)
                pt.TurnoId = registro.TurnoValidado.Id;

            // Hago el refresh con el turnoPrincipal (FS:que ya no existe) seteado en orden.
            registro.TurnoValidado.Orden = dalEngine.Update<Orden>(registro.TurnoValidado.Orden);

            // Save turnoLog.
            List<int> turnosIds = new List<int>();
            turnosIds.Add(registro.TurnoValidado.Id);
            turnosDalc.TurnoLogItemUpdateBatch(turnosIds, TurnoLogFechasEnum.Reserva, true);
            turnosDalc.TurnoLogItemUpdateBatch(turnosIds, TurnoLogFechasEnum.Autorizacion, true);
            turnosDalc.TurnoLogItemUpdateBatch(turnosIds, TurnoLogFechasEnum.Recepcion, true);

            // Save turnoHistorico
            turnosDalc.CrearEstadoTurnoHistorico(false, registro.TurnoValidado, (int?)null);

            if (registro.BorrarExistentes)
            {
                try
                {
                    // Borro las que puedan llegar a existir.
                    foreach (Entities.Valorizacion valorizacion in registro.ValorizacionesDBToDelete)
                        valorizacionesDalc.ValorizacionDeleteFromDB(valorizacion);

                    // Borro las que puedan llegar a existir.
                    turnosDalc.PracticaTurnoDeleteByTurno(registro.TurnoValidado.Id);
                }
                catch
                {
                    throw new ImportacionRegistrosErrorAlProcesarException(
                        "La orden " + registro.ProtocoloValidado.Numero.ToString() +
                        " se encuentra en estado avanzado, por lo cual no se puede modificar su valorizaci?n.");
                }
            }

            // Save practicaTurnos.
            turnosDalc.PracticaTurnoUpdateMany(registro.PracticasTurnoValidado);

            // Inserto las nuevas.
            foreach (FullValorizacion fullValorizacion in registro.FullValorizacionesValidado)
            {
                fullValorizacion.ValorizacionInfo.Tipo = new ValorizacionTipo(ValorizacionTiposEnum.Presupuesto);
                valorizacionesDalc.InsertValorizacion(fullValorizacion.ValorizacionInfo,
                                                      registro.TurnoValidado, registro.TurnoValidado.Orden.ObraSocialPlan);

                fullValorizacion.ValorizacionInfo.Tipo = new ValorizacionTipo(ValorizacionTiposEnum.Admision);
                valorizacionesDalc.InsertValorizacion(fullValorizacion.ValorizacionInfo,
                                                      registro.TurnoValidado, registro.TurnoValidado.Orden.ObraSocialPlan);

                fullValorizacion.ValorizacionInfo.Tipo = new ValorizacionTipo(ValorizacionTiposEnum.Prefacturacion);
                valorizacionesDalc.InsertValorizacion(fullValorizacion.ValorizacionInfo,
                                                      registro.TurnoValidado, registro.TurnoValidado.Orden.ObraSocialPlan);
            }

            // Save ImportacionLog.
            foreach (ImportacionLogController controller in registro.ImportacionLogs)
            {
                controller._importacionLog.Procesado = true;


                dalEngine.Update<ImportacionLog>(controller._importacionLog);
            }

        }

        public EntityCollection<ImportacionSucursalEstacion> ImportacionSucursalEstacionReadAllNotDeleted()
        {
            return dalEngine.GetManyByProperty<ImportacionSucursalEstacion>
                (ImportacionSucursalEstacion.Properties.Deleted, false);
        }
    }
}
