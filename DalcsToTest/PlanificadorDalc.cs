using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Persistence;
using enfoke.AOP;
using NHibernate;
using enfoke.Data.DisconnectedSupport;
using System;
using System.Collections.Generic;
using enfoke.Data.Filters;
using System.Text;

namespace enfoke.Eges.Data
{
    public class PlanificadorDalc : Dalc, IService
    {
        protected PlanificadorDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<Planificador> PlanificadoresReadAll()
        {
            DalEngine engine = new DalEngine();
            return engine.GetManyByProperty<Planificador>(Planificador.Properties.Habilitado, true);
        }

        public EntityCollection<MensajeLog> LastMensajesLogReadByPacientesByBatch(EntityCollection<Paciente> pacientes, int batchId)
        {
            if (pacientes == null || pacientes.Count == 0)
                return new EntityCollection<MensajeLog>();

            List<IIdentificable> lPacientes = new List<IIdentificable>(pacientes.Count);
            foreach (Paciente pac in pacientes)
                lPacientes.Add(pac);

            SQLBlockBuilder<IIdentificable> blockBuilder = new SQLBlockBuilder<IIdentificable>(lPacientes);
            string pacientesIds = blockBuilder.BuildConstrainBlock("mlo.PacienteId");
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select   mlo ");
            hqlBuilder.Append("from     MensajeLog mlo ");
            hqlBuilder.AppendFormat("where {0} and ", pacientesIds);
            hqlBuilder.Append(" mlo.EsRespuesta = false ");
            hqlBuilder.Append("and mlo.Batch.Id = :batchId ");
            hqlBuilder.Append("and mlo.Batch.ServicioMensajeria.RequiereConfirmacion = true ");
            hqlBuilder.Append("order by mlo.Id desc ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("batchId", batchId);
            EntityCollection<MensajeLog> response = dalEngine.GetManyByQuery<MensajeLog>(query);
            return response;
        }

        public Planificador PlanificadorReadById(int planificadorId)
        {
            DalEngine engine = new DalEngine();
            return engine.GetById<Planificador>(planificadorId);
        }

        public EntityCollection<Planificador> PlanificadoresReadAllModificables()
        {
            return dalEngine.GetManyByProperty<Planificador>(Planificador.Properties.Modificable, true, Planificador.Properties.Descripcion, enfoke.Data.SortOrder.Descending);
        }

        public EntityCollection<MensajeLogBatch> MensajeLogBatchReadByEstadoEnProcesoConLimiteDeFecha(DateTime limiteParaProcesadosCompletos)
        {
            Filter filter = new Filter();
            filter.Add(MensajeLogBatch.Properties.Estado, "IN", new int[] { (int)MensajeLogBatchEstadoEnum.ENVIANDO, (int)MensajeLogBatchEstadoEnum.PROCESADO_INCOMPLETO });
            filter.Add(new OpenParenthesis(BooleanOp.Or));
            filter.Add(MensajeLogBatch.Properties.Estado, "=", (int)MensajeLogBatchEstadoEnum.PROCESADO_COMPLETO);
            filter.Add(BooleanOp.And, MensajeLogBatch.Properties.FechaEnviado, ">=", limiteParaProcesadosCompletos);
            filter.Add(new CloseParenthesis());
            return dalEngine.GetManyByFilter<MensajeLogBatch>(filter);
        }

        public EntityCollection<MensajeLogBatch> MensajeLogBatchReadByEstadoAndAntesDeFechaEnviado(IList<MensajeLogBatchEstadoEnum> estados, DateTime fechaEnviado)
        {
            Filter filter = new Filter();
            List<int> estadosIds = new List<int>(estados.Count);
            foreach (MensajeLogBatchEstadoEnum estado in estados)
                estadosIds.Add((int)estado);

            filter.Add(MensajeLogBatch.Properties.Estado, "IN", estadosIds);
            filter.Add(BooleanOp.And, MensajeLogBatch.Properties.FechaEnviado, "<=", fechaEnviado);
            return dalEngine.GetManyByFilter<MensajeLogBatch>(filter);
        }

        public EntityCollection<MensajeLog> MensajeLogReadByIds(IList<int> ids)
        {
            return dalEngine.GetManyByIds<MensajeLog>(ids);
        }

        public EntityCollection<Planificador> PlanificadoresReadByTag(string planificadorTag)
        {
            return dalEngine.GetManyByProperty<Planificador>(Planificador.Properties.Tag, planificadorTag, Planificador.Properties.HoraDesde, enfoke.Data.SortOrder.Ascending);
        }

        public void PlanificadorUpdate(Planificador planificador)
        {
            planificador.EnsureResponseLength();
            planificador = dalEngine.Update<Planificador>(planificador);
        }

        public void PlanificadorDelete(EntityCollection<Planificador> deletedEntities)
        {
            dalEngine.DeleteBatch(deletedEntities);
        }

        public EntityCollection<ServicioMensajeria> ServiciosMensajeriaReadAll()
        {
            return dalEngine.GetAll<ServicioMensajeria>();
        }

        public void MensajeLogBatchUpdateByCollection(EntityCollection<MensajeLogBatch> collection)
        {
            dalEngine.UpdateCollection(collection);
        }

        public ServicioMensajeria ServicioMensajeriaReadByTag(string tag)
        {
            return dalEngine.GetByProperty<ServicioMensajeria>(ServicioMensajeria.Properties.Tag, tag);
        }

        public void ServicioMesajeriaUpdate(ServicioMensajeria servicio)
        {
            servicio = dalEngine.Update<ServicioMensajeria>(servicio);
        }

        public void MensajeLogTurnoUpdateByCollection(EntityCollection<MensajeLogTurno> logTurnos)
        {
            dalEngine.UpdateCollection(logTurnos);
        }






        public EntityCollection<MensajeLog> MensajeLogReadByBatch(int batchId)
        {
            return dalEngine.GetManyByProperty<MensajeLog>(MensajeLog.Properties.Batch.Id, batchId);
        }

        public void MensajeLogUpdateByCollection(EntityCollection<MensajeLog> logs)
        {
            foreach (MensajeLog log in logs)
                log.Id = dalEngine.Update<MensajeLog>(log).Id;
        }
    }
}
