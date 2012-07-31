using enfoke.Connector;
using System;
using enfoke.Eges.Persistence;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistance;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using System.Collections.Generic;
using NHibernate;
using enfoke.Data;
using System.Linq;
using enfoke.AOP;
using enfoke.Eges.Utils;

namespace enfoke.Eges.Data
{
    /// <summary>
    /// Maneja los datos concernientes a las obras sociales
    /// </summary>
    public class RequisitosDalc : Dalc, IService
    {
        protected RequisitosDalc(NotConstructable dummy) : base(dummy) { }

        #region TipoDocumentacionRequerida

        /// <summary>
        /// Obtiene todos los tipos de documentación requerida
        /// </summary>
        /// <returns>Colección de tipo de documentos requeridos</returns>
        public EntityCollection<TipoDocumentacionRequerida> TipoDocumentacionRequeridaReadAll()
        {
            ReadManyCommand<TipoDocumentacionRequerida> readCmd = new ReadManyCommand<TipoDocumentacionRequerida>(dalEngine);

            Filter filter = new Filter();
            filter.Add(TipoDocumentacionRequerida.Properties.Deleted, "=", false);

            Sort sort = new Sort();
            sort.Add(TipoDocumentacionRequerida.Properties.Descripcion, SortingDirection.Asc);

            readCmd.Filter = filter;
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtiene todos los tipos de documentación requerida filtrados por el nivel de carga
        /// </summary>
        /// <param name="nivelCargaIds">Lista de ids de los niveles de carga</param>
        /// <returns>Colección de TipoDocumentacionRequerida</returns>
        [Private]
        public virtual EntityCollection<TipoDocumentacionRequerida> TipoDocumentacionRequeridaReadNivelCargaIds(List<int> nivelCargaIds)
        {
            ReadManyCommand<TipoDocumentacionRequerida> readCmd = new ReadManyCommand<TipoDocumentacionRequerida>(dalEngine);

            Filter filter = new Filter();
            filter.Add(TipoDocumentacionRequerida.Properties.NivelCarga, "in", nivelCargaIds);
            filter.Add(BooleanOp.And, TipoDocumentacionRequerida.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        #endregion

        #region ObraSocialPlanDocumentacion

        /// <summary>
        /// Obtiene todas la documentacion requerida del plan
        /// </summary>
        /// <param name="soloVigentes">estado de elinación</param>
        /// <param name="planId">Id del plan</param>
        /// <returns>Colección de ObraSocialPlanDocumentacion</returns>
        [Private]
        public virtual EntityCollection<ObraSocialPlanDocumentacion> ObraSocialPlanDocumentacionReadByPlan(bool soloVigentes, int planId)
        {
            ReadManyCommand<ObraSocialPlanDocumentacion> readCmd = new ReadManyCommand<ObraSocialPlanDocumentacion>(dalEngine);

            Filter filter = new Filter();
            AgregarFilterPlanObraSocialPlanDocumentacion(soloVigentes, planId, filter);

            if (soloVigentes)
                filter.Add(BooleanOp.And, ObraSocialPlanDocumentacion.Properties.Deleted, "=", false);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [Private]
        public virtual EntityCollection<ObraSocialPlanDocumentacion> ObraSocialPlanDocumentacionReadByPlanAndDocumento(bool soloVigentes, int planId, List<string> tagTipoDocumentacionRequerida)
        {
            ReadManyCommand<ObraSocialPlanDocumentacion> readCmd = new ReadManyCommand<ObraSocialPlanDocumentacion>(dalEngine);

            Filter filter = new Filter();

            AgregarFilterPlanObraSocialPlanDocumentacion(soloVigentes, planId, filter);

            filter.Add(BooleanOp.And, ObraSocialPlanDocumentacion.Properties.TipoDocumentacionRequerida.Tag, "IN", tagTipoDocumentacionRequerida.ToArray());

            if (soloVigentes)
                filter.Add(BooleanOp.And, ObraSocialPlanDocumentacion.Properties.Deleted, "=", false);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        private static void AgregarFilterPlanObraSocialPlanDocumentacion(bool soloVigentes, int planId, Filter filter)
        {
            ExistsFilterItem<ObraSocialPlanRequisito> plan = new ExistsFilterItem<ObraSocialPlanRequisito>(
                ObraSocialPlanDocumentacion.Properties.ObraSocialPlanRequisitoId,
                ObraSocialPlanRequisito.Properties.Id);
            plan.Add(ObraSocialPlanRequisito.Properties.Plan.Id, "=", planId);
            if (soloVigentes)
            {
                DateTime fecha = enfoke.Time.Now.Date;
                plan.Add(BooleanOp.And, ObraSocialPlanRequisito.Properties.Deleted, "=", false);
                plan.Add(BooleanOp.And, ObraSocialPlanRequisito.Properties.FechaHasta, "IS", null);
                plan.Add(BooleanOp.Or, ObraSocialPlanRequisito.Properties.FechaHasta, ">=", fecha);
            }
            filter.Add(plan);
        }

        [Private]
        public virtual EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanPracticaAndDocumento(bool soloVigentes, int planId, int practicaId, List<string> tagTipoDocumentacionRequerida)
        {
            ReadManyCommand<PlanPracticaDocumentacion> readCmd = new ReadManyCommand<PlanPracticaDocumentacion>(dalEngine);

            Filter filter = new Filter();
            AgregarFilterPlanPracticaPlanPracticaDocumentacion(planId, practicaId, soloVigentes, filter);

            filter.Add(BooleanOp.And, PlanPracticaDocumentacion.Properties.TipoDocumentacionRequerida.Tag, "IN", tagTipoDocumentacionRequerida.ToArray());

            if (soloVigentes)
                filter.Add(BooleanOp.And, PlanPracticaDocumentacion.Properties.DeleteFlag, "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        private static void AgregarFilterPlanPracticaPlanPracticaDocumentacion(int planId, int practicaId, bool soloVigentes, Filter filter)
        {
            ExistsFilterItem<PlanPracticaRequisito> plan = new ExistsFilterItem<PlanPracticaRequisito>(PlanPracticaDocumentacion.Properties.PlanPracticaRequisitoId, PlanPracticaRequisito.Properties.Id);
            plan.Add(PlanPracticaRequisito.Properties.Plan.Id, "=", planId);
            plan.Add(BooleanOp.And, PlanPracticaRequisito.Properties.Practica.Id, "=", practicaId);
            plan.Add(BooleanOp.And, PlanPracticaRequisito.Properties.Deleted, "=", false);

            if (soloVigentes)
            {
                plan.Add(BooleanOp.And, PlanPracticaRequisito.Properties.FechaDesde, "<=", enfoke.IO.Time.Now);
                plan.Add(new OpenParenthesis(BooleanOp.And));
                plan.Add(PlanPracticaRequisito.Properties.FechaHasta, "is", null);
                plan.Add(BooleanOp.Or, PlanPracticaRequisito.Properties.FechaHasta, ">", enfoke.IO.Time.Now);
                plan.Add(new CloseParenthesis());
            }

            filter.Add(plan);
        }

        /// <summary>
        /// Modificación masiva de la documentación requerida por el plan
        /// </summary>
        /// <param name="obraSocialPlanDocumentaciones">Colección de ObraSocialPlanDocumentacion a modificar</param>
        [Private]
        public virtual void ObraSocialPlanDocumentacionUpdateMany(EntityCollection<ObraSocialPlanDocumentacion> obraSocialPlanDocumentaciones)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            // Si existen datos
            if (obraSocialPlanDocumentaciones != null && obraSocialPlanDocumentaciones.Count > 0)
                dalEngine.UpdateCollection(obraSocialPlanDocumentaciones);
        }

        /// <summary>
        /// Eliminación masiva de la documentación requerida por el plan
        /// </summary>
        /// <param name="obraSocialPlanDocumentaciones">Colección de ObraSocialPlanDocumentacion a eliminar</param>
        [Private]
        public virtual void ObraSocialPlanDocumentacionDeleteMany(EntityCollection<ObraSocialPlanDocumentacion> obraSocialPlanDocumentaciones)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            // Si existen datos
            if (obraSocialPlanDocumentaciones != null && obraSocialPlanDocumentaciones.Count > 0)
            {
                foreach (ObraSocialPlanDocumentacion item in obraSocialPlanDocumentaciones)
                {
                    // Completo el flag de eliminación
                    item.DeleteDate = enfoke.Time.Now;
                    item.DeleteUser = user.Id;
                    item.Deleted = true;
                }
                // Realizo la modificación de todos los items
                dalEngine.UpdateCollection<ObraSocialPlanDocumentacion>(obraSocialPlanDocumentaciones);
            }
        }

        #endregion

        #region PlanPracticaDocumentacion
        [Private]
        public virtual EntityCollection<DatosPlanPracticaDocumentacion> DatosPlanPracticaDocumentacionReadByPlanAndPracticaAndAdicional(bool soloVigentes, PracticaInfoIds practicaInfoIds)
        {   // Por cada grupo de Ids de practicasInfo hace el Query
            DateTime hoy = enfoke.Time.Now.Date;
            EntityCollection<DatosPlanPracticaDocumentacion> datosPracticas;
            datosPracticas = GetDatosPlanPracticaDocumentacion(practicaInfoIds, hoy);
            EntityCollection<DatosPlanPracticaDocumentacion> datosPracticasReordenados = ReordenarDatosPlanPracticaDocumentacion(datosPracticas, practicaInfoIds);
            return datosPracticasReordenados;
        }

        private EntityCollection<DatosPlanPracticaDocumentacion> ReordenarDatosPlanPracticaDocumentacion(EntityCollection<DatosPlanPracticaDocumentacion> datosPracticas, PracticaInfoIds practicaInfoId)
        {
            EntityCollection<DatosPlanPracticaDocumentacion> datosOrdenados = new EntityCollection<DatosPlanPracticaDocumentacion>();
            EntityCollection<DatosPlanPracticaDocumentacion> practicaPrincipales = (from dppd in datosPracticas where dppd.PractidaId == practicaInfoId.PracticaId select dppd).ToEntityCollection();
            if (practicaPrincipales == null || practicaPrincipales.Count <= 0)
                return datosOrdenados;

            //practicasAdicionales y practicasSubsiguientes deben ser disjuntos, sino explota.
            IEnumerable<DatosPlanPracticaDocumentacion> practicasAdicionales = GetPracticasInCollection(datosPracticas, practicaInfoId.PracticasAdicionalesPracticasId);
            IEnumerable<DatosPlanPracticaDocumentacion> practicasSubsiguientes = GetPracticasInCollection(datosPracticas, practicaInfoId.PracticasSubsiguienteId);

            datosOrdenados.AddRange(practicaPrincipales);
            datosOrdenados.AddRange(ConvertirATipoPracticaSecundaria(practicasAdicionales, practicaPrincipales[0]));
            datosOrdenados.AddRange(practicasSubsiguientes);
            return datosOrdenados;
        }

        private IEnumerable<DatosPlanPracticaDocumentacion> GetPracticasInCollection(EntityCollection<DatosPlanPracticaDocumentacion> datosPracticas, List<int> practicasIds)
        {
            return (practicasIds != null) ? datosPracticas.FindAll(delegate(DatosPlanPracticaDocumentacion dato) { return practicasIds.Contains(dato.PractidaId); }) : new EntityCollection<DatosPlanPracticaDocumentacion>();
        }

        private IEnumerable<DatosPlanPracticaDocumentacion> ConvertirATipoPracticaSecundaria(IEnumerable<DatosPlanPracticaDocumentacion> practicasAdicionales, DatosPlanPracticaDocumentacion practicaPrincipal)
        {
            EntityCollection<DatosPlanPracticaDocumentacion> practicasConvertidas = new EntityCollection<DatosPlanPracticaDocumentacion>();
            if (practicasAdicionales != null)
                foreach (DatosPlanPracticaDocumentacion dppd in practicasAdicionales)
                    practicasConvertidas.Add(SwapPracticaConAdicional(dppd, practicaPrincipal));
            return practicasConvertidas;
        }

        private DatosPlanPracticaDocumentacion SwapPracticaConAdicional(DatosPlanPracticaDocumentacion dppd, DatosPlanPracticaDocumentacion practicaPrincipal)
        {
            dppd.AdicionalId = dppd.PractidaId;
            dppd.AdicionalName = dppd.PracticaName;
            dppd.PractidaId = practicaPrincipal.PractidaId;
            dppd.PracticaName = practicaPrincipal.PracticaName;
            return dppd;
        }

        private EntityCollection<DatosPlanPracticaDocumentacion> GetDatosPlanPracticaDocumentacion(PracticaInfoIds practicaInfoIds, DateTime hoy)
        {
            //Devuelve la query para obtener datos plan practica documentacion de practica principal + los adicionales/subsiguientes
            List<int> practicasIds = new List<int>();
            practicasIds.Add(practicaInfoIds.PracticaId);
            if (practicaInfoIds.PracticasAdicionalesPracticasId != null)
                practicasIds.AddRange(practicaInfoIds.PracticasAdicionalesPracticasId);
            if (practicaInfoIds.PracticasSubsiguienteId != null)
                practicasIds.AddRange(practicaInfoIds.PracticasSubsiguienteId);

            return GetDatosPlanPracticaDocumentacion(practicasIds, practicaInfoIds.ObraSocialPlanId, hoy);
        }

        public EntityCollection<DatosPlanPracticaDocumentacion> GetDatosPlanPracticaDocumentacion(List<int> practicasIds, int planId, DateTime hoy)
        {

            String hql = "select new  enfoke.Eges.Entities.Results.DatosPlanPracticaDocumentacion(ppd, ppr.Practica.Id, ppr.Practica.Name) " +
                     "from PlanPracticaDocumentacion ppd, PlanPracticaRequisito ppr " +
                     "where ppd.PlanPracticaRequisitoId = ppr.Id " +
                     "and ppd.DeleteFlag  = false " +
                     "and ppr.Deleted = false " +
                     "and ppr.FechaDesde <= :hoy " +
                     "and (ppr.FechaHasta >= :hoy or ppr.FechaHasta is null) " +
                     "and ppr.Plan.Id = :idPlan " +
                     "AND ppr.Practica.Id in (:idPracticas) " +
                     "ORDER BY ppd.TipoDocumentacionRequerida.Descripcion";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetDateTime("hoy", hoy.Date);
            query.SetInt32("idPlan", planId);
            query.SetParameterList("idPracticas", practicasIds);
            return dalEngine.GetManyByQuery<DatosPlanPracticaDocumentacion>(query);
        }

        /// <summary>
        /// Obtiene todas la documentacion requerida del plan practica
        /// </summary>
        /// <param name="soloVigentes">estado de elinación</param>
        /// <param name="planId">Id del plan practica</param>
        /// <returns>Colección de PlanPracticaDocumentacion</returns>
        [Private]
        public virtual EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanPractica(bool soloVigentes, ObraSocialPlan plan, Practica practica)
        {
            ReadManyCommand<PlanPracticaDocumentacion> readCmd = new ReadManyCommand<PlanPracticaDocumentacion>(dalEngine);

            Filter filter = new Filter();
            AgregarFilterPlanPracticaPlanPracticaDocumentacion(plan.Id, practica.Id, soloVigentes, filter);

            if (soloVigentes)
                filter.Add(BooleanOp.And, PlanPracticaDocumentacion.Properties.DeleteFlag, "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtiene todas la documentacion requerida de los plan practica en base a un plan
        /// </summary>
        /// <param name="soloVigentes">estado de elinación</param>
        /// <param name="planId">Id del plan practica</param>
        /// <returns>Colección de PlanPracticaDocumentacion</returns>
        [Private]
        public virtual EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanPractica(int plan)
        {

            String hql = "select ppd "
                        + "from  PlanPracticaDocumentacion ppd, PlanPracticaPrecio plp "
                        + "where ppd.PlanPracticaId = plp.Id "
                        + "and plp.Plan.Id = :plan "
                        + "and ppd.DeleteFlag = false ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("plan", plan);

            return dalEngine.GetManyByQuery<PlanPracticaDocumentacion>(query);

        }

        public EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByTipoDocumentacionRequerida(int tipoDocumentacionRequeridaId)
        {
            ReadManyCommand<PlanPracticaDocumentacion> readCmd = new ReadManyCommand<PlanPracticaDocumentacion>(dalEngine);

            Filter filter = new Filter();
            filter.Add(PlanPracticaDocumentacion.Properties.TipoDocumentacionRequerida.Id, "=", tipoDocumentacionRequeridaId);
            filter.Add(BooleanOp.And, PlanPracticaDocumentacion.Properties.DeleteFlag, "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanPracticasAndFechaVigencia(int osPlanId, List<int> practicasIds, DateTime fechaVigencia)
        {
            return PlanPracticaDocumentacionReadByPlanesPracticasAndFechaVigencia(new List<int>() { osPlanId }, practicasIds, fechaVigencia);
        }

        public EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanesPracticasAndFechaVigencia(List<int> osPlanesId, List<int> practicasIds, DateTime fechaVigencia)
        {
            if (practicasIds == null || practicasIds.Count == 0 || osPlanesId == null || osPlanesId.Count == 0)
                return new EntityCollection<PlanPracticaDocumentacion>();

            String hql = "select ppd "
                        + "from  PlanPracticaDocumentacion ppd, PlanPracticaRequisito plp "
                        + "where ppd.PlanPracticaRequisitoId = plp.Id "
                        + "and plp.Plan.Id IN (:planes) "
                        + "and (plp.FechaDesde <= :fechaVigencia and (plp.FechaHasta is null or plp.FechaHasta >= :fechaVigencia)) "
                        + "and plp.Deleted = false "
                        + "and plp.Practica.Id IN (:practicas) "
                        + "and  ppd.DeleteFlag = false ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("planes", osPlanesId);
            query.SetDateTime("fechaVigencia", fechaVigencia.Date);
            query.SetParameterList("practicas", practicasIds);
            return dalEngine.GetManyByQuery<PlanPracticaDocumentacion>(query);
        }

        [Private]
        public virtual EntityCollection<PlanPracticaDocumentacion> PlanPracticaDocumentacionReadByPlanPracticaIds(List<int> planPracticaRequisitosIds)
        {
            if (planPracticaRequisitosIds == null || planPracticaRequisitosIds.Count == 0)
                return new EntityCollection<PlanPracticaDocumentacion>();

            const int MAX_PARAMETERS_PER_PAGE = 1000;
            int currentPage = Math.Min(MAX_PARAMETERS_PER_PAGE, planPracticaRequisitosIds.Count);
            EntityCollection<PlanPracticaDocumentacion> response = new EntityCollection<PlanPracticaDocumentacion>();
            while (currentPage > 0)
            {
                List<int> ids = planPracticaRequisitosIds.GetRange(0, currentPage);
                EntityCollection<PlanPracticaDocumentacion> page = (from ppd in dalEngine.Query<PlanPracticaDocumentacion>() where ids.Contains(ppd.PlanPracticaRequisitoId) select ppd).ToEntityCollection();
                response.AddRange(page);
                planPracticaRequisitosIds.RemoveRange(0, currentPage);
                currentPage = Math.Min(MAX_PARAMETERS_PER_PAGE, planPracticaRequisitosIds.Count);
            }

            return response;
        }

        /// <summary>
        /// Modificación masiva de la documentación requerida por un plan practica
        /// </summary>
        /// <param name="obraSocialPlanDocumentaciones">Colección de PlanPracticaDocumentacion a modificar</param>
        [Private]
        public virtual void PlanPracticaDocumentacionUpdateMany(EntityCollection<PlanPracticaDocumentacion> planPracticaDocumentaciones)
        {
            // Si existen datos
            if (planPracticaDocumentaciones != null && planPracticaDocumentaciones.Count > 0)
                dalEngine.UpdateCollection(planPracticaDocumentaciones);
        }


        /// <summary>
        /// Eliminación masiva de la documentación requerida por un plan practica
        /// </summary>
        /// <param name="obraSocialPlanDocumentaciones">Colección de PlanPracticaDocumentacion a eliminar</param>
        [Private]
        public virtual void PlanPracticaDocumentacionDeleteMany(EntityCollection<PlanPracticaDocumentacion> planPracticaDocumentaciones)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            // Si existen datos
            if (planPracticaDocumentaciones != null && planPracticaDocumentaciones.Count > 0)
            {
                foreach (PlanPracticaDocumentacion item in planPracticaDocumentaciones)
                {

                    //[PC] [CAMBIAR E IMPLEMENTA INTERFAZ]

                    // Completo el flag de eliminación
                    item.DeleteDate = enfoke.Time.Now;
                    item.DeleteUser = user.Id;
                    item.DeleteFlag = true;
                }


                // Realizo la modificación de todos los items
                dalEngine.UpdateCollection<PlanPracticaDocumentacion>(planPracticaDocumentaciones);
            }
        }

        #endregion

        #region Requisitos


        public EntityCollection<ObraSocialPlanRequisito> ObraSocialPlanRequisitosHistoricoLeerPorPlan(ObraSocialPlan plan, bool leerDocumentacion)
        {
            return ObraSocialPlanRequisitosLeerPorPlan(plan, null, leerDocumentacion);
        }
        public EntityCollection<ObraSocialPlanRequisito> ObraSocialPlanRequisitosLeerPorPlan(ObraSocialPlan plan, DateTime? fecha, bool leerDocumentacion)
        {
            IQueryable<ObraSocialPlanRequisito> query = from requisito in this.dalEngine.Query<ObraSocialPlanRequisito>()
                                                        where requisito.Plan.Id == plan.Id
                                                        && requisito.Deleted == false
                                                        orderby requisito.FechaDesde descending
                                                        select requisito;
            if (fecha.HasValue)
                query = query.Where(requisito => (fecha.Value >= requisito.FechaDesde
                                     && (fecha.Value <= requisito.FechaHasta || null == requisito.FechaHasta)));

            EntityCollection<ObraSocialPlanRequisito> ret = query.ToEntityCollection<ObraSocialPlanRequisito>();
            if (leerDocumentacion && ret.Count > 0)
            {
                EntityCollection<ObraSocialPlanDocumentacion> docs = (from documentacion in this.dalEngine.Query<ObraSocialPlanDocumentacion>()
                                                                      where ret.GetIds().Contains(documentacion.ObraSocialPlanRequisitoId)
                                                                      && documentacion.Deleted == false
                                                                      orderby documentacion.ObraSocialPlanRequisitoId,
                                                                              documentacion.TipoDocumentacionRequerida.Id
                                                                      select documentacion
                                                                    ).ToEntityCollection<ObraSocialPlanDocumentacion>();

                SortedMultipartData<ObraSocialPlanDocumentacion, int> grupos = new SortedMultipartData<ObraSocialPlanDocumentacion, int>(ObraSocialPlanDocumentacion.Properties.ObraSocialPlanRequisitoId);
                grupos.Add(docs);
                foreach (ObraSocialPlanRequisito requisito in ret)
                    requisito.DocumentacionRequerida.AddRange(grupos.GetManyBySorted(requisito.Id));
            }
            return ret;
        }

        [RequiresTransaction]
        public virtual void ObraSocialPlanRequisitoUpdate(ObraSocialPlanRequisito requisitos)
        {
            if (requisitos == null) return;
            // 1. Limpia vigencias conflicitivas (posteriores o abiertas)
            string hql = "SELECT ospr " +
                         "FROM ObraSocialPlanRequisito ospr " +
                         "WHERE ospr.Deleted = 0 " +
                         "AND ospr.FechaDesde >= :requisitosFechaDesde " +
                         "AND ospr.Plan.Id = :requisitosPlanId ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("requisitosFechaDesde", requisitos.FechaDesde);
            query.SetParameter("requisitosPlanId", requisitos.Plan.Id);

            EntityCollection<ObraSocialPlanRequisito> posteriores = dalEngine.GetManyByQuery<ObraSocialPlanRequisito>(query);
            dalEngine.Delete(posteriores);

            #region queryEnLinq
            //EntityCollection<ObraSocialPlanRequisito> posteriores = (from requisito in dalEngine.Query<ObraSocialPlanRequisito>()
            //                                                         where requisito.Deleted == false && requisito.FechaDesde >= requisitos.FechaDesde
            //                                                         && requisito.Plan.Id == requisitos.Plan.Id
            //                                                         select requisito).ToEntityCollection<ObraSocialPlanRequisito>();
            #endregion

            string hql2 = "SELECT ospr " +
                         "FROM ObraSocialPlanRequisito ospr " +
                         "WHERE ospr.Deleted = 0 " +
                         "AND (ospr.FechaHasta >= :requisitosFechaDesde OR ospr.FechaHasta IS NULL) " +
                         "AND ospr.Plan.Id = :requisitosPlanId ";

            IQuery query2 = dalEngine.CreateQuery(hql2.ToString());
            query2.SetParameter("requisitosFechaDesde", requisitos.FechaDesde);
            query2.SetParameter("requisitosPlanId", requisitos.Plan.Id);

            EntityCollection<ObraSocialPlanRequisito> excedidos = dalEngine.GetManyByQuery<ObraSocialPlanRequisito>(query2);


            #region query2EnLinq
            //EntityCollection<ObraSocialPlanRequisito> excedidos = (from requisito in dalEngine.Query<ObraSocialPlanRequisito>()
            //                                                       where requisito.Deleted == false && (requisito.FechaHasta >= requisitos.FechaDesde ||
            //                                                       requisito.FechaHasta == null)
            //                                                       && requisito.Plan.Id == requisitos.Plan.Id
            //                                                       select requisito).ToEntityCollection<ObraSocialPlanRequisito>();
            #endregion

            DateTime limite = requisitos.FechaDesde.AddDays(-1);
            ModificarFechaHasta<ObraSocialPlanRequisito>(limite, excedidos);
            // 2. Graba norma y vigencia
            LobUpdater.UpdateClob<ObraSocialPlanRequisito>(requisitos, "Norma");

            // 3. Graba documentación
            foreach (IDocumentacionRequerida doc in requisitos.DocumentacionRequerida)
            {
                doc.RequisitosId = requisitos.Id;
                dalEngine.Update(doc);
            }
        }
        public PracticasPorPlan ArbolRequisitosReadByFecha(int obraSocialId, DateTime vigencia, bool mostrarSetsFarmacia)
        {
            LectorArbol arbol = new LectorArbol(dalEngine);
            return arbol.ArbolRequisitosReadByFecha(obraSocialId, vigencia, mostrarSetsFarmacia);
        }


        public ArbolNormasDocumentacionDiccionarios ArbolNormasDocumentacionLeerDiccionarios(ObraSocial obraSocial)
        {
            ArbolNormasDocumentacionDiccionarios ret = new ArbolNormasDocumentacionDiccionarios();
            CargaDocumentosDictionary(ret);

            foreach (ObraSocialPlan plan in Context.Session.ObrasSocialesDalc.ObraSocialPlanReadByObraSocial(obraSocial.Id))
                ret.PlanesDictionary.Add(plan.Id, plan);
            foreach (ServicioEspecialidad esp in Context.Session.Dalc.GetAll<ServicioEspecialidad>())
                ret.EspecialidadesDictionary.Add(esp.Id, esp);
            foreach (Servicio serv in Context.Session.Dalc.GetAll<Servicio>())
                ret.ServiciosDictionary.Add(serv.Id, serv);
            // Carga diccionario de prácticas
            ret.PracticaDescriptionDictionary = (from practica in dalEngine.Query<Practica>()
                                                 where practica.Deleted == false
                                                 select new
                                                 {
                                                     PracticaId = practica.Id,
                                                     PracticaDescription = practica.Code.TrimEnd() + "/"
                                            + practica.Region.TrimEnd() + " - " + practica.Name
                                                 })
                                                 .ToDictionary(p => p.PracticaId, p => p.PracticaDescription);

            return ret;
        }


        /// <summary>
        /// Obtiene la lista de los tipos de documentación requeria para los planes_practicas
        /// </summary>
        /// <returns></returns>
        public EntityCollection<TipoDocumentacionRequerida> TipoDocumentacionRequeridaReadAllForPlanPractica()
        {
            // Creo la lista de Ids de los niveles de carga para los planes_practicas
            List<int> ids = new List<int>();
            ids.Add((int)TipoDocumentacionRequeridaNivelCargaEnum.Practica);
            ids.Add((int)TipoDocumentacionRequeridaNivelCargaEnum.Ambos);

            return Context.Session.RequisitosDalc.TipoDocumentacionRequeridaReadNivelCargaIds(ids);
        }

        /// <summary>
        /// Obtiene la lista de los tipos de documentación requeria para los planes
        /// </summary>
        /// <returns></returns>
        public EntityCollection<TipoDocumentacionRequerida> TipoDocumentacionRequeridaReadAllForPlan()
        {
            // Creo la lista de Ids de los niveles de carga para los planes
            List<int> ids = new List<int>();
            ids.Add((int)TipoDocumentacionRequeridaNivelCargaEnum.Plan);
            ids.Add((int)TipoDocumentacionRequeridaNivelCargaEnum.Ambos);

            return Context.Session.RequisitosDalc.TipoDocumentacionRequeridaReadNivelCargaIds(ids);
        }
        private void CargaDocumentosDictionary(ArbolNormasDocumentacionDiccionarios ret)
        {
            ret.TiposDocumentacionRequeridaPlan = TipoDocumentacionRequeridaReadAllForPlan();
            ret.TiposDocumentacionRequeridaPlanPractica = TipoDocumentacionRequeridaReadAllForPlanPractica();

            EntityCollection<TipoDocumentacionRequerida> documentaciones = Context.Session.Dalc.GetAll<TipoDocumentacionRequerida>();
            foreach (TipoDocumentacionRequerida doc in documentaciones)
            {
                DocumentacionRequeridaAplica docAplica = new DocumentacionRequeridaAplica();
                docAplica.Documentacion = doc;
                docAplica.Aplica = DocumentacionRequeridaAplicaEnum.Ninguno;
                if (ret.TiposDocumentacionRequeridaPlan.Contains(docAplica.Documentacion))
                    docAplica.Aplica |= DocumentacionRequeridaAplicaEnum.Plan;
                if (ret.TiposDocumentacionRequeridaPlanPractica.Contains(docAplica.Documentacion))
                    docAplica.Aplica |= DocumentacionRequeridaAplicaEnum.PlanPractica;
                ret.Documentaciones.Add(docAplica);
            }
            foreach (TipoDocumentacionRequerida doc in documentaciones)
                ret.DocumentacionesDictionary.Add(doc.Id, doc);
        }

        [RequiresTransaction]
        public virtual void PlanPracticaRequisitoUpdate(PlanPracticaRequisito requisitos)
        {
            if (requisitos == null) return;
            // 1. Limpia vigencias conflicitivas (posteriores o abiertas)
            EntityCollection<PlanPracticaRequisito> posteriores = (from requisito in dalEngine.Query<PlanPracticaRequisito>()
                                                                   where requisito.Deleted == false && requisito.FechaDesde >= requisitos.FechaDesde
                                                                   && requisitos.Plan.Id == requisito.Plan.Id
                                                                   && requisitos.Practica.Id == requisito.Practica.Id
                                                                   select requisito).ToEntityCollection<PlanPracticaRequisito>();
            dalEngine.Delete(posteriores);
            EntityCollection<PlanPracticaRequisito> excedidos = (from requisito in dalEngine.Query<PlanPracticaRequisito>()
                                                                 where requisito.Deleted == false && (requisito.FechaHasta >= requisitos.FechaDesde ||
                                                                 requisito.FechaHasta == null)
                                                                    && requisitos.Plan.Id == requisito.Plan.Id
                                                                    && requisitos.Practica.Id == requisito.Practica.Id
                                                                 select requisito).ToEntityCollection<PlanPracticaRequisito>();
            DateTime limite = requisitos.FechaDesde.AddDays(-1);
            ModificarFechaHasta<PlanPracticaRequisito>(limite, excedidos);
            // 2. Graba norma y vigencia
            LobUpdater.UpdateClob<PlanPracticaRequisito>(requisitos, "Norma");

            // 3. Graba documentación
            foreach (IDocumentacionRequerida doc in requisitos.DocumentacionRequerida)
            {
                doc.RequisitosId = requisitos.Id;
                dalEngine.Update(doc);
            }
        }
        private void ModificarFechaHasta<T>(DateTime limite, EntityCollection<T> excedidos)
            where T : INormaDocumentacionRequisito
        {
            if (excedidos.Count > 0)
            {
                foreach (T req in excedidos)
                {
                    req.FechaHasta = limite;
                    dalEngine.Update((enfoke.Data.DisconnectedSupport.IEditableEntity)req);
                }
            }
        }
        public EntityCollection<PlanPracticaRequisito> PlanPracticaRequisitosHistoricoLeerPorPlanPractica(ObraSocialPlan plan, int practicaId, bool leerDocumentacion)
        {
            return PlanPracticaRequisitosLeerPorPlanPractica(practicaId, null, leerDocumentacion, plan.Id);
        }

        public EntityCollection<PlanPracticaRequisito> PlanPracticaRequisitosLeerPorPlanPractica(ObraSocialPlan plan, int practicaId, DateTime? fecha, bool leerDocumentacion)
        {
            int planId = plan.Id;
            return PlanPracticaRequisitosLeerPorPlanPractica(practicaId, fecha, leerDocumentacion, planId);
        }

        public PlanPracticaRequisito PlanPracticaRequisitosLeerPorPlanPracticaAndFecha(int planId, int practicaId, DateTime fecha, bool leerDocumentacion)
        {
            EntityCollection<PlanPracticaRequisito> requisitos = PlanPracticaRequisitosLeerPorPlanPractica(practicaId, fecha, leerDocumentacion, planId);
            if (requisitos != null && requisitos.Count > 0)
                return requisitos[0];

            return null;
        }

        public EntityCollection<PlanPracticaRequisito> PlanPracticaRequisitosLeerPorPlanPractica(int practicaId, DateTime? fecha, bool leerDocumentacion, int planId)
        {
            IQueryable<PlanPracticaRequisito> query = from requisito in this.dalEngine.Query<PlanPracticaRequisito>()
                                                      where requisito.Plan.Id == planId && requisito.Practica.Id == practicaId
                                                      && requisito.Deleted == false
                                                      orderby requisito.Id /*&& requisito.FechaDesde descending*/
                                                      select requisito;
            if (fecha.HasValue)
                query = query.Where(requisito => (fecha.Value >= requisito.FechaDesde
                                     && (fecha.Value <= requisito.FechaHasta || null == requisito.FechaHasta)));
            EntityCollection<PlanPracticaRequisito> ret = query.ToEntityCollection<PlanPracticaRequisito>();
            if (leerDocumentacion && ret.Count > 0)
            {
                EntityCollection<PlanPracticaDocumentacion> docs = (from documentacion in this.dalEngine.Query<PlanPracticaDocumentacion>()
                                                                    where ret.GetIds().Contains(documentacion.PlanPracticaRequisitoId)
                                                                    && documentacion.DeleteFlag == false
                                                                    orderby documentacion.PlanPracticaRequisitoId,
                                                                            documentacion.TipoDocumentacionRequerida.Id
                                                                    select documentacion
                                                                    ).ToEntityCollection<PlanPracticaDocumentacion>();

                int index = 0;
                foreach (PlanPracticaRequisito norma in ret)
                {
                    while (index < docs.Count && docs[index].PlanPracticaRequisitoId == norma.Id)
                    {
                        norma.DocumentacionRequerida.Add(docs[index]);
                        index++;
                    }
                }
            }

            return ret;
        }


        internal EntityCollection<PlanArbolRequisitos> PlanRequisitoArbolReadByFecha(int osId, DateTime fecha)
        {
            EntityCollection<PlanArbolRequisitos> ret = (from planRequisito in dalEngine.Query<ObraSocialPlanRequisito>()
                                                         where planRequisito.FechaDesde <= fecha && (planRequisito.FechaHasta == null || planRequisito.FechaHasta >= fecha)
                                                               && planRequisito.Plan.ObraSocial.Id == osId
                                                               && !planRequisito.Deleted
                                                         orderby

                                                         planRequisito.Id

                                                         select new PlanArbolRequisitos()
                                                         {
                                                             PlanId = planRequisito.Plan.Id,
                                                             RequisitoId = planRequisito.Id,
                                                             Norma = planRequisito.Norma,
                                                             TieneNorma = (planRequisito.Norma != null)
                                                         }).ToEntityCollection<PlanArbolRequisitos>();
            // Le agrega las documentaciones
            EntityCollection<PlanArbolDocumentacion> doc;
            if (ret.Count > 0)
            {
                doc = (from planDocumentacion in dalEngine.Query<ObraSocialPlanDocumentacion>()
                       join planRequisito in dalEngine.Query<ObraSocialPlanRequisito>()
                       on planDocumentacion.ObraSocialPlanRequisitoId equals planRequisito.Id
                       where planRequisito.FechaDesde <= fecha && (planRequisito.FechaHasta == null || planRequisito.FechaHasta >= fecha)
                              && planRequisito.Plan.ObraSocial.Id == osId
                              && !planDocumentacion.Deleted
                              && !planRequisito.Deleted
                       orderby
                       planDocumentacion.ObraSocialPlanRequisitoId,
                       planDocumentacion.TipoDocumentacionRequerida.Id

                       select new PlanArbolDocumentacion()
                       {
                           RequisitoId = planDocumentacion.ObraSocialPlanRequisitoId,
                           Observaciones = planDocumentacion.Observaciones,
                           TipoDocumentoId = planDocumentacion.TipoDocumentacionRequerida.Id
                       }).ToEntityCollection<PlanArbolDocumentacion>();
            }
            else
                doc = new EntityCollection<PlanArbolDocumentacion>();
            // Recorre los requisitos agregándo los documentos
            AgregaDocumentosEnRequisitos(ret, doc);

            return ret;
        }

        internal EntityCollection<PlanPracticaArbolRequisitos> PlanPracticaRequisitoArbolReadByFecha(int osId, DateTime fecha)
        {
            EntityCollection<PlanPracticaArbolRequisitos> ret = (from planPracticaRequisito in dalEngine.Query<PlanPracticaRequisito>()
                                                                 where planPracticaRequisito.FechaDesde <= fecha && (planPracticaRequisito.FechaHasta == null || planPracticaRequisito.FechaHasta >= fecha)
                                                                       && planPracticaRequisito.Plan.ObraSocial.Id == osId
                                                                       && !planPracticaRequisito.Deleted
                                                                 orderby

                                                                 planPracticaRequisito.Id

                                                                 select new PlanPracticaArbolRequisitos()
                                                                 {
                                                                     PracticaId = planPracticaRequisito.Practica.Id,
                                                                     PlanId = planPracticaRequisito.Plan.Id,
                                                                     RequisitoId = planPracticaRequisito.Id,
                                                                     TieneNorma = (planPracticaRequisito.Norma != null),
                                                                     Norma = planPracticaRequisito.Norma
                                                                 }).ToEntityCollection<PlanPracticaArbolRequisitos>();
            // Le agrega las documentaciones
            EntityCollection<PlanArbolDocumentacion> doc;
            if (ret.Count > 0)
            {
                doc = (from planPracticaDocumentacion in dalEngine.Query<PlanPracticaDocumentacion>()
                       join planPracticaRequisito in dalEngine.Query<PlanPracticaRequisito>()
                       on planPracticaDocumentacion.PlanPracticaRequisitoId equals planPracticaRequisito.Id
                       where planPracticaRequisito.FechaDesde <= fecha && (planPracticaRequisito.FechaHasta == null || planPracticaRequisito.FechaHasta >= fecha)
                              && planPracticaRequisito.Plan.ObraSocial.Id == osId
                              && !planPracticaRequisito.Deleted
                              && !planPracticaDocumentacion.DeleteFlag
                       orderby
                       planPracticaDocumentacion.PlanPracticaRequisitoId,
                       planPracticaDocumentacion.TipoDocumentacionRequerida.Id

                       select new PlanArbolDocumentacion()
                       {
                           RequisitoId = planPracticaDocumentacion.PlanPracticaRequisitoId,
                           TipoDocumentoId = planPracticaDocumentacion.TipoDocumentacionRequerida.Id,
                           Observaciones = planPracticaDocumentacion.Observaciones
                       }).ToEntityCollection<PlanArbolDocumentacion>();
            }
            else
                doc = new EntityCollection<PlanArbolDocumentacion>();
            // Recorre los requisitos agregándo los documentos
            AgregaDocumentosEnRequisitos(ret, doc);

            return ret;
        }

        private static void AgregaDocumentosEnRequisitos(EntityCollection<PlanPracticaArbolRequisitos> ret, EntityCollection<PlanArbolDocumentacion> doc)
        {
            int iDocumento = 0;
            if (doc.Count > 0)
            {
                for (int iRequisito = 0; iRequisito < ret.Count; iRequisito++)
                {
                    PlanPracticaArbolRequisitos req = ret[iRequisito];
                    while (doc[iDocumento].RequisitoId == req.RequisitoId)
                    {
                        if (req.DocumentacionRequerida == null) req.DocumentacionRequerida = new List<DocumentacionItem>();
                        req.DocumentacionRequerida.Add(
                            new DocumentacionItem()
                            {
                                TipoDocumentacionId = doc[iDocumento].TipoDocumentoId,
                                Observaciones = doc[iDocumento].Observaciones
                            });
                        iDocumento++;
                        if (iDocumento >= doc.Count)
                            return;
                    }
                }
            }
        }
        private static void AgregaDocumentosEnRequisitos(EntityCollection<PlanArbolRequisitos> ret, EntityCollection<PlanArbolDocumentacion> doc)
        {
            int iDocumento = 0;
            if (doc.Count > 0)
            {
                for (int iRequisito = 0; iRequisito < ret.Count; iRequisito++)
                {
                    PlanArbolRequisitos req = ret[iRequisito];
                    while (doc[iDocumento].RequisitoId == req.RequisitoId)
                    {
                        if (req.DocumentacionRequerida == null) req.DocumentacionRequerida = new List<DocumentacionItem>();
                        req.DocumentacionRequerida.Add(
                         new DocumentacionItem()
                         {
                             TipoDocumentacionId = doc[iDocumento].TipoDocumentoId,
                             Observaciones = doc[iDocumento].Observaciones
                         });
                        iDocumento++;
                        if (iDocumento >= doc.Count)
                            return;
                    }
                }
            }
        }
        [Private]
        public virtual EntityCollection<ObraSocialPlanRequisito> ObraSocialPlanRequisitosReadByPlanId(int planId, DateTime fechaVigencia)
        {
            return ObraSocialPlanRequisitosReadByPlanesIds(new List<int>() { planId }, fechaVigencia);
        }
        [Private]
        public virtual EntityCollection<ObraSocialPlanRequisito> ObraSocialPlanRequisitosReadByPlanesIds(List<int> planesId)
        {
            DateTime fechaVigencia = enfoke.Time.Now;
            return ObraSocialPlanRequisitosReadByPlanesIds(planesId, fechaVigencia);
        }
        [Private]
        public virtual EntityCollection<ObraSocialPlanRequisito> ObraSocialPlanRequisitosReadByPlanesIds(List<int> planesId, DateTime fechaVigencia)
        {
            DateTime fechaVigenciaDesde = fechaVigencia.Date.AddDays(1).AddSeconds(-1);
            DateTime fechaVigenciaHasta = fechaVigencia.Date.AddSeconds(-1);
            EntityCollection<ObraSocialPlanRequisito> ospRequisitos = new EntityCollection<ObraSocialPlanRequisito>();
            if (planesId.Count > 0)
            {
                ospRequisitos = (from requisitos in dalEngine.Query<ObraSocialPlanRequisito>()
                                 where
                                     !requisitos.Deleted &&
                                     planesId.Contains(requisitos.Plan.Id) &&
                                     requisitos.FechaDesde < fechaVigenciaDesde &&
                                     (requisitos.FechaHasta == null || requisitos.FechaHasta > fechaVigenciaHasta)
                                 select requisitos).ToEntityCollection();
            }
            if (ospRequisitos.Count > 0)
            {
                EntityCollection<ObraSocialPlanDocumentacion> docs = (from documentacion in this.dalEngine.Query<ObraSocialPlanDocumentacion>()
                                                                      where
                                                                        ospRequisitos.GetIds().Contains(documentacion.ObraSocialPlanRequisitoId) &&
                                                                        !documentacion.Deleted
                                                                      orderby documentacion.ObraSocialPlanRequisitoId
                                                                      select documentacion
                                                                    ).ToEntityCollection<ObraSocialPlanDocumentacion>();
                int index = 0;
                foreach (ObraSocialPlanRequisito requisito in ospRequisitos)
                {
                    while (index < docs.Count && docs[index].ObraSocialPlanRequisitoId == requisito.Id)
                    {
                        requisito.DocumentacionRequerida.Add(docs[index]);
                        index++;
                    }
                }
            }
            return ospRequisitos;
        }

        public PlanPracticaRequisitoHC ActualizarRequisito(EntityCollection<PlanPracticaRequisitoHC> requisitos, PlanPracticaRequisitoHC nuevo)
        {
            IList<PlanPracticaRequisitoHC> actualizados = VigenciaUtils<PlanPracticaRequisitoHC>.ObtenerModificaciones(requisitos, nuevo, false, null);
            actualizados.Remove(nuevo);
            LobUpdater.UpdateClob<PlanPracticaRequisitoHC>(nuevo, "Norma");
            dalEngine.UpdateCollection(actualizados.ToEntityCollection());
            return nuevo;
        }

        #endregion
    }
}
