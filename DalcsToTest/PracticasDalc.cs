using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;
using System.Linq;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Eges.Entities.Results;
using enfoke.Data.Reference;
using enfoke.Data;
using enfoke.Eges.Auditoria;
using enfoke.Data.Filters;
using enfoke.AOP;
using enfoke.Eges.Persistance;
using System.Collections;
using enfoke.Localization;
using enfoke.Data.DisconnectedSupport;

namespace enfoke.Eges.Data
{
    public class PracticasDalc : Dalc, IService
    {
        protected PracticasDalc(NotConstructable dummy) : base(dummy) { }

        #region Practica
        public EntityCollection<Practica> PracticaReadAll()
        {
            return dalEngine.GetAll<Practica>(Practica.Properties.Name);
        }

        public EntityCollection<PracticaName> PracticaNameReadByServicioEspecialidad(int servicioEspecialidadId)
        {
            return dalEngine.GetManyByProperty<PracticaName>(PracticaName.Properties.ServicioEspecialidad.Id, servicioEspecialidadId);
        }

        public ReadAllCollection<Practica> PracticaReadAllOrderByCodeRegionName()
        {
            EntityCollection<Practica> practicas = dalEngine.GetAll<Practica>(
                        new IPropertyReference[] { Practica.Properties.Code, Practica.Properties.Region,
                                                        Practica.Properties.Name });
            return new ReadAllCollection<Practica>(practicas);
        }






        public ReadAllCollection<Practica> PracticaReadByServicio(int servicioID)
        {
            EntityCollection<Practica> practicas = new EntityCollection<Practica>();

            foreach (Practica practica in dalEngine.GetManyByProperty<Practica>(
                Practica.Properties.ServicioEspecialidad.Servicio.Id, servicioID, Practica.Properties.Name))
            {
                if (!practica.Deleted)
                {
                    practicas.Add(practica);
                }
            }

            return new ReadAllCollection<Practica>(practicas);
        }

        public EntityCollection<Practica> PracticaReadByServicioEspecialidad(int servicioEspecialidadID)
        {
            EntityCollection<Practica> practicas = new EntityCollection<Practica>();

            foreach (Practica practica in dalEngine.GetManyByProperty<Practica>(Practica.Properties.ServicioEspecialidad.Id, servicioEspecialidadID, Practica.Properties.Name))
            {
                if (!practica.Deleted)
                {
                    practicas.Add(practica);
                }
            }

            return practicas;
        }


        public EntityCollection<Practica> PracticaReadByServicioAndCodeAndName(int servicioID, string code, string name)
        {
            ReadManyCommand<Practica> practicas = new ReadManyCommand<Practica>(dalEngine);

            Filter filter = new Filter();
            filter.Add(Practica.Properties.ServicioEspecialidad.Servicio.Id, "=", servicioID);
            filter.Add(BooleanOp.And, Practica.Properties.Deleted, "=", false);

            if (!string.IsNullOrEmpty(code))
                filter.Add(BooleanOp.And, Practica.Properties.Code, "LIKE", code.Replace(' ', '%') + '%');

            if (!string.IsNullOrEmpty(name))
                filter.Add(BooleanOp.And, Practica.Properties.Name, "LIKE", name.Replace(' ', '%') + '%');

            practicas.Filter = filter;
            practicas.Sort = new Sort(new SortItem(Practica.Properties.Name, SortingDirection.Asc));

            return practicas.Execute();
        }

        public ReadAllCollection<Practica> PracticaReadByServicioOrderByCodeRegionName(int servicioID)
        {
            EntityCollection<Practica> practicas = dalEngine.GetManyByProperty<Practica>(
                                            Practica.Properties.ServicioEspecialidad.Servicio.Id, servicioID,
                        new IPropertyReference[] { Practica.Properties.Code, Practica.Properties.Region,
                                                        Practica.Properties.Name });
            return new ReadAllCollection<Practica>(practicas);
        }

        public int PracticaGetCantidadByCodigoRegion(string codigo, string region, int id)
        {
            string hql = "from Practica p where p.Code = '" + codigo + "' AND p.Region = '" + region + "' AND p.Id <> :id ";
            IQuery query = dalEngine.CreateQuery(hql);
            //query.SetString("codigo", codigo);
            //query.SetString("region", region);
            query.SetInt32("id", id);
            EntityCollection<Practica> practicas = dalEngine.GetManyByQuery<Practica>(query);
            return practicas != null ? practicas.Count : 0;
        }

        public void PracticaDelete(int practicaID)
        {
            PracticaDeleted(practicaID, true);
        }

        public void PracticaUndelete(int practicaID)
        {
            PracticaDeleted(practicaID, false);
        }

        private void PracticaDeleted(int practicaID, bool deleted)
        {
            Practica practica = dalEngine.GetById<Practica>(practicaID);
            if (deleted)
                dalEngine.Delete(practica);
            else
            {
                practica.Deleted = false;
                dalEngine.Update(practica);
            }
        }

        public void PreparacionUpdate(Preparacion preparacion)
        {
            LobUpdater.UpdateClob<Preparacion>(preparacion,
                    Preparacion.Properties.TextoAdultos, Preparacion.Properties.IndicacionAdultos,
                    Preparacion.Properties.TextoPediatrico, Preparacion.Properties.IndicacionPediatrico);
        }


        public List<int> PracticaReadByEquipoId(int equipoId)
        {
            string hql = "SELECT DISTINCT ep.Practica.Id " +
                         "FROM EquipoPractica ep " +
                         "WHERE ep.Equipo.Id = :idEquipo ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idEquipo", equipoId);

            return (List<int>)query.List<int>();
        }

        /// <summary>
        /// Obtengo practicas segun el Tipo
        /// </summary>
        [Private]
        public ReadAllCollection<Practica> PracticaReadByTipoPractica(List<TipoPracticaEnum> tipos)
        {
            // Creo la lista de tipos a buscar
            List<int> idsTipos = new List<int>();
            foreach (TipoPracticaEnum tpe in tipos)
                idsTipos.Add((int)tpe);

            // Filtra por los tipos solicitados
            Filter filter = new Filter();
            filter.Add(Practica.Properties.TipoPractica.Id, "IN", idsTipos.ToArray());

            Sort sort = new Sort(new SortItem(Practica.Properties.Name));

            return new ReadAllCollection<Practica>(dalEngine.GetManyByFilter<Practica>(filter, sort));
        }

        /// <summary>
        /// Obtengo practicas segun el Tipo
        /// </summary>
        public EntityCollection<PracticaName> PracticaNameReadByTipoPractica(List<TipoPracticaEnum> tipos)
        {
            // Creo la lista de tipos a buscar
            List<int> idsTipos = new List<int>();
            foreach (TipoPracticaEnum tpe in tipos)
                idsTipos.Add((int)tpe);

            // Filtra por los tipos solicitados
            Filter filter = new Filter();
            filter.Add(PracticaName.Properties.TipoPracticaId, "IN", idsTipos.ToArray());

            Sort sort = new Sort(new SortItem(PracticaName.Properties.Name));

            return dalEngine.GetManyByFilter<PracticaName>(filter, sort);
        }

        /// <summary>
        /// Retorno Practicas filtradas por Nombre, Codigo y marca de Eliminada
        /// </summary>
        /// <param name="name">Nombre de las practicas a buscar</param>
        /// <param name="code">Codigo de las practicas a buscar</param>
        /// <param name="deleted">Marca si traigo eliminadas o no [Null -> Todas | T/F -> Deleted = marca]</param>
        /// <returns>Lista de Practicas</returns>
        public ReadAllCollection<Practica> PracticaReadByNameCodeAndDeleted(string name, string code, bool? deleted)
        {
            name = name.Trim().Replace(" ", "%");

            Filter filter = new Filter();
            if (String.IsNullOrEmpty(name) == false)
                filter.Add(Practica.Properties.Name, "LIKE", "%" + name + "%");
            if (String.IsNullOrEmpty(code) == false)
                filter.Add(BooleanOp.And, Practica.Properties.Code, "LIKE", "%" + code + "%");
            if (deleted.HasValue)
                filter.Add(BooleanOp.And, Practica.Properties.Deleted, "=", deleted.Value);
            EntityCollection<Practica> practicas = dalEngine.GetManyByFilter<Practica>(filter);
            return new ReadAllCollection<Practica>(practicas);
        }

        public bool PracticaTieneConveniosVigentes(int practicaId)
        {
            string hql = "select count(plp.Plan) from PlanPracticaPrecio plp JOIN plp.Plan osp JOIN osp.ObraSocial os "
                         + " where plp.Deleted = false "
                         + "AND osp.Deleted = false AND osp.Activo = true "
                         + "AND os.Deleted = false AND os.EstadoId = :obraSocialActiva "
                         + "AND plp.Practica = :practicaID "
                         + "AND plp.FechaDesde < :desde "
                         + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaId);
            query.SetParameter("desde", enfoke.Time.Now.Date.AddDays(1));
            query.SetParameter("hasta", enfoke.Time.Now.Date.AddDays(1));
            query.SetParameter("obraSocialActiva", (int)ObraSocialEstadoEnum.Activa);

            object res = query.UniqueResult();
            if (res == null || res == DBNull.Value)
                return false;
            else
            {
                int cantidad = int.Parse(res.ToString());

                return (cantidad != 0);
            }
        }

        public EntityCollection<Practica> PracticasReadByPlan(int planId)
        {
            string hql = " select ptr " +
                         " from Practica ptr, PlanPracticaPrecio pptr, ObraSocialPlan osp " +
                         " where ptr.Id = pptr.Practica.Id " +
                         " and pptr.Id = osp.Id " +
                         " and pptr.Deleted = false " +
                         " and ptr.Deleted = false " +
                         " and osp.Deleted = false " +
                         " and osp.Id = :planId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planId", planId);
            return dalEngine.GetManyByQuery<Practica>(query);
        }

        public EntityCollection<Practica> PracticasReadByIds(List<int> ids)
        {
            return dalEngine.GetManyByPropertyList<Practica>(Practica.Properties.Id, ids);
        }

        public EntityCollection<Practica> PracticasReadVigentes()
        {
            string hql = " from Practica ptr " +
                    " where ptr.Deleted = false " +
                    " order by ptr.Name ASC ";
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<Practica>(query);
        }

        public EntityCollection<Practica> PracticasWithListaPrecioByListaPrecioId(int listaPrecioId)
        {
            EntityCollection<Practica> practicas = this.PracticasReadVigentes();

            if (practicas != null && practicas.Count > 0)
            {
                foreach (Practica practica in practicas)
                {
                    practica.PracticaListaPrecios = PracticaListaPreciosReadByPracticaListaVigente(practica.Id, listaPrecioId);
                }
            }

            return practicas;
        }

        #endregion

        #region PracticaForList

        public EntityCollection<PracticaForList> PracticaForListReadAll(bool ordeServicio, bool ordenCodigo)
        {
            String hql = "select pra " +
                         "from PracticaForList pra " +
                         "where pra.Delete = false ";

            if (!ordeServicio && !ordenCodigo)
                hql += " order by  pra.Name, pra.ServicioEspecialidad.Servicio.Name ";
            else if (ordeServicio)
                hql += " order by  pra.ServicioEspecialidad.Servicio.Name, pra.Name ";
            else
                hql += " order by  pra.Code , pra.Name ";

            IQuery query = dalEngine.CreateQuery(hql);

            return dalEngine.GetManyByQuery<PracticaForList>(query);
        }

        public EntityCollection<PracticaForList> PracticaForListReadAll()
        {
            String hql = "select pra " +
                       "from PracticaForList pra " +
                       "where pra.Delete = false ";

            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<PracticaForList>(query);
        }

        public EntityCollection<PracticaForList> PracticaForListReadByPlan(int planId)
        {
            string hql = " select distinct ptr " +
                         " from PracticaForList ptr, PlanPracticaPrecio pptr " +
                         " where ptr.Id = pptr.Practica.Id " +
                         " and pptr.Plan.Id = :planId " +
                         " and pptr.Deleted = false " +
                         " and ptr.Delete = false " +
                         " order by ptr.Name asc ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planId", planId);
            return dalEngine.GetManyByQuery<PracticaForList>(query);
        }

        public EntityCollection<PracticaForListWithPreparaciones> PracticaForListWithPreparacionesReadByServicioId(int? idServicio)
        {

            string hql = "FROM PracticaForListWithPreparaciones p " +
                         "WHERE (p.Preparacion.TextoAdultos IS NOT NULL OR p.Preparacion.TextoPediatrico IS NOT NULL) " +
                         (idServicio.HasValue ? "AND p.ServicioEspecialidad.Servicio.Id = :idServicio " : "") +
                         "ORDER BY p.Name ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            if (idServicio.HasValue)
                query.SetParameter("idServicio", idServicio.Value);

            return dalEngine.GetManyByQuery<PracticaForListWithPreparaciones>(query);
        }

        public EntityCollection<PracticaForList> PracticaForListReadByParameters(DateTime? fechaReferencia, int? equipoId, int? practicaAdicionalId, List<int> planId, List<int> listaPrecioIds, List<int> unidadArancelariaIds, List<int> insumoIds, List<int> documentacionRequeridaIds, List<int> tipoCoberturaIds, List<int> tipoCoseguroIds, bool? valorEquipo, bool? exijaAutorizacion, bool? exijaConfirmacion, bool? exijaPresupuesto)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            if (fechaReferencia.HasValue != false)
                fecha = fechaReferencia.Value.Date;

            StringBuilder hql = new StringBuilder();
            hql.Append("select distinct pra ");
            hql.Append("from PlanPracticaPrecio pplp, PracticaForList pra ");

            if (documentacionRequeridaIds != null && documentacionRequeridaIds.Count > 0)
                hql.Append(", PlanPracticaDocumentacion ppd ");

            if (unidadArancelariaIds != null && unidadArancelariaIds.Count > 0)
            {

                hql.Append(",UnidadArancelariaPlan uap  ");
            }

            if (insumoIds != null && insumoIds.Count > 0)
                hql.Append(", PlanPracticaInsumo ppi ");

            hql.Append("where pplp.Plan.Id in (:planId) ");
            hql.Append("and pra.Id = pplp.Practica.Id ");

            if (unidadArancelariaIds != null && unidadArancelariaIds.Count > 0)
            {
                hql.Append("and uap.ObraSocialPlan.Id = pplp.Plan.Id ");

                hql.Append("and ( pplp.UAGastos.Id = uap.UnidadArancelaria.Id  ");
                hql.Append("or  pplp.UAHonorarios.Id = uap.UnidadArancelaria.Id ");
                hql.Append("or  pplp.UAInsumos.Id = uap.UnidadArancelaria.Id ");
                hql.Append("or  pplp.UAModulo.Id = uap.UnidadArancelaria.Id) ");

                hql.Append("and uap.UnidadArancelaria.Id in (:unidadArancelariaIds) ");
                hql.Append("and uap.Deleted = false ");
                hql.Append("and uap.FechaDesde <= :fecha ");
                hql.Append("and (uap.FechaHasta >= :fecha or uap.FechaHasta is null) ");
            }

            if (practicaAdicionalId.HasValue == true && practicaAdicionalId.Value > 0)
                hql.Append("and pplp.PracticaAdicional.Adicional.Id = ").Append(practicaAdicionalId.Value).Append(" ");
            else
                hql.Append("and pplp.PracticaAdicional is null ");

            if (documentacionRequeridaIds != null && documentacionRequeridaIds.Count > 0)
            {
                hql.Append("and ppd.PlanPracticaId = pplp.Id ");
                hql.Append("and ppd.TipoDocumentacionRequerida.Id in (:documentacionRequeridaIds) ");
            }

            if (insumoIds != null && insumoIds.Count > 0)
            {
                hql.Append("and ppi.PlanPracticaPrecio.Id = pplp.Id ");
                hql.Append("and ppi.Insumo.Id in (:insumoIds) ");
            }

            if (listaPrecioIds != null && listaPrecioIds.Count > 0)
                hql.Append("and pplp.ListaPrecios.Id in (:listaPrecioIds) ");

            if (tipoCoberturaIds != null && tipoCoberturaIds.Count > 0)
                hql.Append("and pplp.TipoCobertura.Id in (:tipoCoberturaIds) ");

            if (tipoCoseguroIds != null && tipoCoseguroIds.Count > 0)
                hql.Append("and pplp.TipoCoseguroID in (:tipoCoseguroIds) ");

            if (exijaPresupuesto.HasValue == true)
                hql.Append("and pplp.ExigePresupuestoMarca = :exijaPresupuesto ");

            if (exijaConfirmacion.HasValue == true)
                hql.Append("and pplp.ExigeConfirmacionMarca = :exijaConfirmacion ");

            if (exijaAutorizacion.HasValue == true)
                hql.Append("and pplp.ExigeAutorizacionMarca = :exijaAutorizacion ");

            if (valorEquipo.HasValue == true)
            {
                if (valorEquipo.Value == true)
                    hql.Append("and pplp.Equipo is not null ");
                else
                    hql.Append("and pplp.Equipo is null ");
            }

            if (equipoId.HasValue && equipoId.Value > 0)
                hql.Append("and pplp.Equipo.Id = :equipoId ");

            hql.Append("and  pplp.Deleted = false ");
            hql.Append("and  pplp.FechaDesde <= :fecha ");
            hql.Append("and (pplp.FechaHasta >= :fecha or pplp.FechaHasta is null) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameterList("planId", planId);
            query.SetDateTime("fecha", fecha.Date);

            if (equipoId.HasValue && equipoId.Value > 0)
                query.SetInt32("equipoId", equipoId.Value);

            if (listaPrecioIds != null && listaPrecioIds.Count > 0)
                query.SetParameterList("listaPrecioIds", listaPrecioIds);

            if (insumoIds != null && insumoIds.Count > 0)
                query.SetParameterList("insumoIds", insumoIds);

            if (tipoCoberturaIds != null && tipoCoberturaIds.Count > 0)
                query.SetParameterList("tipoCoberturaIds", tipoCoberturaIds);

            if (unidadArancelariaIds != null && unidadArancelariaIds.Count > 0)
                query.SetParameterList("unidadArancelariaIds", unidadArancelariaIds);

            if (tipoCoseguroIds != null && tipoCoseguroIds.Count > 0)
                query.SetParameterList("tipoCoseguroIds", tipoCoseguroIds);

            if (exijaAutorizacion.HasValue == true)
                query.SetBoolean("exijaAutorizacion", exijaAutorizacion.Value);

            if (exijaConfirmacion.HasValue == true)
                query.SetBoolean("exijaConfirmacion", exijaConfirmacion.Value);

            if (exijaPresupuesto.HasValue == true)
                query.SetBoolean("exijaPresupuesto", exijaPresupuesto.Value);

            if (documentacionRequeridaIds != null && documentacionRequeridaIds.Count > 0)
                query.SetParameterList("documentacionRequeridaIds", documentacionRequeridaIds);

            return dalEngine.GetManyByQuery<PracticaForList>(query);

        }

        #endregion

        #region PracticaObraSocialView
        /// <summary>
        /// Devuelve todas las practicas (view).
        /// </summary>
        /// <returns>Practicas (view)</returns>
        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewReadAll()
        {
            return dalEngine.GetAll<PracticaObraSocialView>(PracticaObraSocialView.Properties.Name);
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByNameNotAdicional(string name, FilterFlag subSiguientes, int servicioId, int obraSocialPlanId)
        {
            return this.PracticaObraSocialViewSearchByNameNotAdicional(name, subSiguientes, servicioId, obraSocialPlanId, false);
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByNameNotAdicional(string name, FilterFlag subSiguientes, int servicioId, int obraSocialPlanId, bool soloServiciosEspontaneos)
        {
            EntityCollection<PracticaObraSocialView> practicas = this.PracticaObraSocialViewSearchByNameNotAdicional(name, servicioId, obraSocialPlanId, soloServiciosEspontaneos);

            if (subSiguientes == FilterFlag.NoFilter)
                return practicas;
            else
            {
                bool mustBeSubsiguiente = (subSiguientes == FilterFlag.FilterTrue);

                Predicate<PracticaObraSocialView> predicate = delegate(PracticaObraSocialView compare)
                {
                    return compare != null && compare.EsSubsiguiente == mustBeSubsiguiente;
                };

                EntityCollection<PracticaObraSocialView> filtered = new EntityCollection<PracticaObraSocialView>();
                filtered.AddRange(practicas.FindAll(predicate));
                return filtered;
            }
        }

        /// <summary>
        /// Devuelve todas las practicas con la descripción indicada SIN LAS QUE SON ADICIONALES.
        /// </summary>
        /// <returns>Practicas</returns>
        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByNameNotAdicional(string name, int servicioId, int obraSocialPlanId, bool soloServiciosEspontaneos)
        {
            name = name.Trim().Replace(" ", "%") + "%";

            string hql = "select pa "
                            + "from PracticaObraSocialView pa "
                            + "WHERE (pa.Name LIKE :name "
                            + "OR pa.Code LIKE :name " //pra_codigo
                            + "OR pa.CodigoInternoOS LIKE :name )" //plp_codigo_interno
                            + "AND pa.PlanId = :obraSocialPlanId "
                            + "AND pa.TipoPracticaID NOT IN (:tipoPracticasExcluidas) "
                            + "AND pa.Deleted = false ";

            if (servicioId != -1)
                hql += " AND pa.ServicioId = :servicioId ";

            if (soloServiciosEspontaneos)
                hql += " AND EXISTS(FROM Servicio s WHERE s.PermiteTurnoEspontaneo = true AND s.Id = pa.ServicioId) ";
            hql += "ORDER BY pa.Code, pa.Name";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("name", name);
            query.SetParameter("obraSocialPlanId", obraSocialPlanId);
            query.SetParameterList("tipoPracticasExcluidas", new int[] { (int)TipoPracticaEnum.Adicional, (int)TipoPracticaEnum.SetFarmacia });
            if (servicioId != -1)
                query.SetParameter("servicioId", servicioId);

            EntityCollection<PracticaObraSocialView> practicas = dalEngine.GetManyByQuery<PracticaObraSocialView>(query);
            EntityCollection<PracticaObraSocialView> response = ExcluirCodigosRepetidosPorEquipo(practicas);


            MergePracticaNoFacturable(name, servicioId, soloServiciosEspontaneos, response);


            return response;
        }

        private void MergePracticaNoFacturable(string name, int servicioId, bool soloServiciosEspontaneos, EntityCollection<PracticaObraSocialView> response)
        {
            EntityCollection<Practica> notInvoicePractices = PracticaSearchByNameNotAdicionalNoFacturable(name, servicioId, soloServiciosEspontaneos);

            //Se agregan las practicas que no se facturan al resultado.
            foreach (Practica practice in notInvoicePractices)
                if (!PracticaYaCargadaPorConvenio(response, practice))
                    response.Add(new PracticaObraSocialView(practice));
        }

        private EntityCollection<Practica> PracticaSearchByNameNotAdicionalNoFacturable(string name, int servicioId, bool soloServiciosEspontaneos)
        {
            //Se buscan las practicas que no son facturables para agregarlas.
            string hql = " Select ptr " +
                       " from Practica ptr WHERE ptr.EsFacturable = false AND ptr.Deleted = false AND ptr.TipoPractica.Id NOT IN (:tipoPracticasExcluidas) "
                       + " AND (ptr.Name LIKE :name "
                       + "OR ptr.Code LIKE :name) ";

            if (servicioId != -1)
                hql += " AND ptr.ServicioEspecialidad.Servicio.Id = :servicioId ";
            if (soloServiciosEspontaneos)
                hql += " AND ptr.ServicioEspecialidad.Servicio.PermiteTurnoEspontaneo = true ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("name", name);
            query.SetParameterList("tipoPracticasExcluidas", new int[] { (int)TipoPracticaEnum.Adicional, (int)TipoPracticaEnum.SetFarmacia });
            if (servicioId != -1)
                query.SetParameter("servicioId", servicioId);

            EntityCollection<Practica> notInvoicePractices = dalEngine.GetManyByQuery<Practica>(query);
            return notInvoicePractices;
        }

        private EntityCollection<PracticaObraSocialView> ExcluirCodigosRepetidosPorEquipo(EntityCollection<PracticaObraSocialView> practicas)
        {
            EntityCollection<PracticaObraSocialView> response = new EntityCollection<PracticaObraSocialView>();
            List<string> codigos = new List<string>();
            if (practicas != null && practicas.Count > 0)
                foreach (PracticaObraSocialView pra in practicas)
                    if (pra != null)
                        if (!codigos.Contains(pra.Code + pra.Name))
                        {
                            response.Add(pra);
                            codigos.Add(pra.Code + pra.Name);
                        }

            return response;
        }

        private bool PracticaYaCargadaPorConvenio(EntityCollection<PracticaObraSocialView> response, Practica practica)
        {
            foreach (PracticaObraSocialView practicaConvenio in response)
                if (practicaConvenio.Id == practica.Id)
                    return true;

            return false;
        }

        /// <summary>
        /// Devuelve todas las practicas con la descripción indicada.
        /// </summary>
        /// <returns>Practicas</returns>
        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name)
        {
            return PracticaObraSocialViewSearchByName(name, -1);
        }

        /// <summary>
        /// Devuelve todas las practicas con la descripción indicada.
        /// </summary>
        /// <returns>Practicas</returns>
        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name, int servicioId)
        {
            return PracticaObraSocialViewSearchByName(name, -1, -1, false);
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name, int servicioId, int obraSocialPlanId, bool excluirModulo)
        {
            // La busqueda se hace por partes como esta definido
            // en el punto 1.1.12. de la especificacion de turnos V3
            name = name.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<PracticaObraSocialView> readCmd = new ReadManyCommand<PracticaObraSocialView>(dalEngine);

            Filter filter = new Filter();

            // Trabajo solo con las practicas activas
            filter.Add(PracticaObraSocialView.Properties.Deleted, "=", false);

            // se filtra el nombre
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(PracticaObraSocialView.Properties.Name, "LIKE", name);
            filter.Add(BooleanOp.Or, PracticaObraSocialView.Properties.Code, "LIKE", name);
            filter.Add(BooleanOp.Or, PracticaObraSocialView.Properties.CodigoInternoOS, "LIKE", name);
            filter.Add(new CloseParenthesis());

            // filtra por servicio
            if (servicioId != -1)
            {
                filter.Add(BooleanOp.And, PracticaObraSocialView.Properties.ServicioId,
                    "=", servicioId);
            }

            //filtra por obraSocialPlan
            // el -1 tambien vale (Ver vwPracticaObraSocial)
            filter.Add(BooleanOp.And, PracticaObraSocialView.Properties.PlanId,
                "=", obraSocialPlanId);

            // Si la practica agregada es un modulo, entonces filtro a las que sean del tipo modulo.
            if (excluirModulo == true)
                filter.Add(BooleanOp.And, PracticaObraSocialView.Properties.TipoPracticaID,
                "!=", (int)TipoPracticaEnum.Modulo);

            readCmd.Filter = filter;

            EntityCollection<PracticaObraSocialView> response = readCmd.Execute();

            MergePracticaNoFacturable(name, servicioId, false, response);

            return response;
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name, FilterFlag subSiguientes)
        {
            return this.PracticaObraSocialViewSearchByName(name, subSiguientes, -1);
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name, FilterFlag subSiguientes, int servicioId)
        {
            return this.PracticaObraSocialViewSearchByName(name, subSiguientes, -1, -1, false);
        }

        public EntityCollection<PracticaObraSocialView> PracticaObraSocialViewSearchByName(string name, FilterFlag subSiguientes, int servicioId, int obraSocialPlanId, bool excluirModulo)
        {
            EntityCollection<PracticaObraSocialView> practicas = this.PracticaObraSocialViewSearchByName(name, servicioId, obraSocialPlanId, excluirModulo);

            if (subSiguientes == FilterFlag.NoFilter)
                return practicas;
            else
            {
                bool mustBeSubsiguiente = (subSiguientes == FilterFlag.FilterTrue);

                Predicate<PracticaObraSocialView> predicate = delegate(PracticaObraSocialView compare)
                {
                    return compare.EsSubsiguiente == mustBeSubsiguiente;
                };

                EntityCollection<PracticaObraSocialView> filtered = new EntityCollection<PracticaObraSocialView>();
                filtered.AddRange(practicas.FindAll(predicate));
                return filtered;
            }
        }
        #endregion

        #region PracticaAdicional
        /// <summary>
        /// Devuelve una PracticaAdicional en base a dos Practicas
        /// </summary>
        /// <param name="practicaID">La practica principal</param>
        /// <param name="adicionalId">La practica adicional</param>
        /// <returns>La PracticaAdicional correspondiente</returns>
        public PracticaAdicional PracticaAdicionalReadByPracticaAndAdicional(int practicaID, int adicionalID)
        {
            string hql = "from PracticaAdicional pa Where "
                + "pa.PracticaID = :practicaID and pa.Adicional = :adicionalID "
                + " and pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("adicionalID", adicionalID);

            EntityCollection<PracticaAdicional> ret = dalEngine.GetManyByQuery<PracticaAdicional>(query);

            return ret.Count > 0 ? ret[0] : null;
        }

        public EntityCollection<Practica> PracticasReadByCodesAndRegions(List<string> codes, List<string> regions)
        {
            if (codes == null || codes.Count == 0 || regions == null || regions.Count == 0)
                return new EntityCollection<Practica>();

            EntityCollection<Practica> response = new EntityCollection<Practica>();
            int registers_per_page = Math.Min(codes.Count, 1000);
            int pages = (int)Math.Ceiling((decimal)codes.Count / registers_per_page);
            for (int index = 0; index < pages; index++)
            {
                StringBuilder hqlBuilder = new StringBuilder("select pra from Practica pra where ");
                for (int paramIndex = 0; paramIndex < registers_per_page; paramIndex++)
                {
                    hqlBuilder.AppendFormat("(pra.Deleted = false and upper(ltrim(rtrim(pra.Code))) = :code{0} and upper(ltrim(rtrim(pra.Region))) = :region{0})", paramIndex);
                    if (paramIndex < registers_per_page - 1)
                        hqlBuilder.Append(" or ");
                }

                IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
                for (int paramIndex = 0; paramIndex < registers_per_page; paramIndex++)
                {
                    query.SetString("code" + paramIndex.ToString(), codes[paramIndex].ToUpper());
                    query.SetString("region" + paramIndex.ToString(), regions[paramIndex].ToUpper());
                }

                codes.RemoveRange(0, registers_per_page);
                regions.RemoveRange(0, registers_per_page);
                registers_per_page = Math.Min(codes.Count, 1000);
                response.AddRange(dalEngine.GetManyByQuery<Practica>(query));
            }

            return response;
        }

        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByAdicional(int adicionalID)
        {
            string hql = "from PracticaAdicional pa  "
                + "Where pa.Adicional = :adicionalID "
                + " and pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("adicionalID", adicionalID);

            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        public bool PracticaAdicionalPerteneceAPractica(int practicaID, int adicionalID)
        {
            string hql = "from PracticaAdicional pa Where "
                + "pa.PracticaID = :practicaID and pa.Adicional = :adicionalID "
                + " and pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("adicionalID", adicionalID);

            EntityCollection<PracticaAdicional> ret = dalEngine.GetManyByQuery<PracticaAdicional>(query);

            return ret.Count > 0 ? true : false;
        }

        public EntityCollection<PracticaAdicional> PracticaAdicionalReadAll()
        {
            string hql = "select distinct pa "
                + "from PracticaAdicional pa Where "
                + "pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        public EntityCollection<PracticaAdicionalForHC> PracticaAdicionalForHcReadAll()
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select distinct new enfoke.Eges.Entities.Results.PracticaAdicionalForHC( ");
            hql.Append("pad.Adicional.Name, pad.Adicional.Id, pad.PracticaID  )");
            hql.Append("from PracticaAdicional pad, ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<PracticaAdicionalForHC>(query);
        }

        public EntityCollection<Practica> PracticasFromPracticaAdicionalReadAll()
        {
            string hql = "select distinct pa "
                + "from PracticaAdicional pa Where "
                + "pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            return this.GetPracticasFromPracticasAdicionales(dalEngine.GetManyByQuery<PracticaAdicional>(query));
        }

        public EntityCollection<Practica> PracticasTipoAdicionaloSet()
        {
            string hql = "select distinct pa.Adicional "
                + "from PracticaAdicional pa Where "
                + "pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<Practica>(query);
        }

        /// <summary>
        /// Devuelve las practicas adicionales a una practica dada
        /// </summary>
        /// <param name="practicaID">La practica a la cual buscar adicionales</param>
        /// <returns>Las practicas adicionales a la practica dada</returns>
        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByPractica(int practicaID)
        {
            return PracticaAdicionalReadByPracticaIdPracticaNameAndPracticaCode(practicaID, string.Empty, string.Empty);
        }

        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByPracticas(IList<int> practicasIds)
        {
            if (practicasIds.Count == 0)
                return new EntityCollection<PracticaAdicional>();

            Filter filter = new Filter();
            filter.Add(PracticaAdicional.Properties.PracticaID, "IN", practicasIds);
            filter.Add(BooleanOp.And, PracticaAdicional.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<PracticaAdicional>(filter);
        }

        public EntityCollection<Practica> PracticaAdicionalPracticaReadByPracticasId(List<int> practicasIds)
        {
            if (practicasIds.Count == 0)
                return new EntityCollection<Practica>();
            EntityCollection<PracticaAdicional> adicionales =
                    dalEngine.GetManyByPropertyList<PracticaAdicional>(PracticaAdicional.Properties.PracticaID, practicasIds);
            return (from practicaAdicional in adicionales
                    where practicaAdicional.Deleted == false
                    orderby practicaAdicional.Adicional.Name
                    select practicaAdicional.Adicional).Distinct().
                        ToEntityCollection<Practica>();

        }
        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByPracticaIdPracticaNameAndPracticaCode(int practicaID, string practicaName, string practicaCode)
        {
            string hql = "SELECT pa " +
                         "FROM PracticaAdicional pa, Practica pr " +
                         "WHERE pr.Id = pa.PracticaID " +
                         "AND pa.PracticaID = :practicaID " +
                         "AND pa.Adicional.Deleted = false " +
                         "AND pa.Deleted = false ";

            if (!String.IsNullOrEmpty(practicaName))
                hql += "AND pr.Name LIKE :practicaName ";

            if (!String.IsNullOrEmpty(practicaCode))
                hql += "AND pr.Code LIKE :practicaCode ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            if (!String.IsNullOrEmpty(practicaName))
                query.SetParameter("practicaName", practicaName.Trim().Replace(' ', '%').ToUpper() + "%");
            if (!String.IsNullOrEmpty(practicaCode))
                query.SetParameter("practicaCode", practicaCode.Trim().Replace(' ', '%').ToUpper() + "%");

            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByPracticasIdsPracticaNameAndPracticaCode(List<int> practicasIDs, string practicaName, string practicaCode)
        {
            string hql = "SELECT pa " +
                         "FROM PracticaAdicional pa, Practica pr " +
                         "WHERE pr.Id = pa.Adicional.Id " +
                         "AND pa.PracticaID IN (:practicasIDs) " +
                         "AND pa.Deleted = false ";

            if (!String.IsNullOrEmpty(practicaName))
                hql += "AND pr.Name LIKE :practicaName ";

            if (!String.IsNullOrEmpty(practicaCode))
                hql += "AND pr.Code LIKE :practicaCode ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("practicasIDs", practicasIDs);
            if (!String.IsNullOrEmpty(practicaName))
                query.SetParameter("practicaName", practicaName.Trim().Replace(' ', '%').ToUpper() + "%");
            if (!String.IsNullOrEmpty(practicaCode))
                query.SetParameter("practicaCode", practicaCode.Trim().Replace(' ', '%').ToUpper() + "%");

            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        /// <summary>
        /// Devuelve las practicas de las practicas adicionales a una practica dada
        /// </summary>
        /// <param name="practicaID">La practica a la cual buscar adicionales</param>
        /// <returns>Las practicas de las practicas adicionales a la practica dada</returns>
        public EntityCollection<Practica> PracticasFromPracticaAdicionalReadByPractica(int practicaID)
        {
            string hql = "select pa " +
                         "from PracticaAdicional pa, Practica pra " +
                         "where pa.PracticaID = pra.Id " +
                         "and pa.PracticaID = :practicaID " +
                         "order by pra.Name asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            EntityCollection<PracticaAdicional> practicasAdicional = dalEngine.GetManyByQuery<PracticaAdicional>(query);
            return GetPracticasFromPracticasAdicionales(practicasAdicional);
        }
        private EntityCollection<Practica> GetPracticasFromPracticasAdicionales(EntityCollection<PracticaAdicional> practicasAdicional)
        {
            EntityCollection<Practica> practicas = new EntityCollection<Practica>();
            if (practicasAdicional != null && practicasAdicional.Count > 0)
            {
                foreach (PracticaAdicional pa in practicasAdicional)
                {
                    if (!practicas.Contains(pa.Adicional))
                        practicas.Add(pa.Adicional);
                }
            }

            return practicas;
        }

        /// <summary>
        /// Devuelve las practicas adicionales a una practica dada de un plan dado
        /// </summary>
        /// <param name="practicaID">La practica a la cual buscar adicionales con su respectivo plan</param>
        /// <returns>Las practicas adicionales a la practica dada de un plan dado</returns>
        public EntityCollection<PracticaAdicional> PracticaAdicionalReadByPracticaAndPlan(int practicaID, int planID)
        {
            string hql = "select distinct pa " +
            "from PracticaAdicional pa, PlanPracticaPrecio pp  " +
            "where (pa.Id = pp.PracticaAdicional.Id  or pa.Adicional.Id = pp.Practica.Id) " +
            "and pp.Plan.Id = :planID " +
            "and (pa.PracticaID = :practicaID)" +
            "and pa.Deleted = false " +
            "and pp.Deleted = false " +
            "and pp.FechaDesde <= :fecha " +
            "and (pp.FechaHasta >= :fecha OR pp.FechaHasta is null)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("planID", planID);
            query.SetParameter("fecha", enfoke.Time.Today);
            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        [RequiresTransaction]
        public virtual void PracticaAdicionalUpdate(EntityCollection<PracticaAdicional> nuevas, EntityCollection<PracticaAdicional> modificadas, EntityCollection<PracticaAdicional> eliminadas)
        {
            // Marca las eliminadas
            foreach (PracticaAdicional pa in eliminadas)
                pa.Deleted = true;
            // Graba...
            dalEngine.UpdateCollection(eliminadas);
            dalEngine.UpdateCollection(nuevas);
            dalEngine.UpdateCollection(modificadas);
        }

        public bool PracticaEsAdicional(int practicaID)
        {
            string hql = "from PracticaAdicional pa Where "
                    + "pa.Adicional = :adicionalID and pa.Deleted = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("adicionalID", practicaID);

            EntityCollection<PracticaAdicional> ret = dalEngine.GetManyByQuery<PracticaAdicional>(query);

            return ret.Count != 0;
        }
        #endregion

        #region PlanPracticaCoberturaInsumo
        [RequiresTransaction]
        [Private]
        public virtual void PlanPracticaCobInsumoUpdateManyForHerramientaCopia(EntityCollection<PlanPracticaCobInsumo> planPracticaCobInsumo)
        {
            dalEngine.UpdateCollection(planPracticaCobInsumo);
        }

        public PlanPracticaCobInsumo PlanPracticaCobInsumoReadById(int planPracticaCobInsumoId)
        {
            return dalEngine.GetById<PlanPracticaCobInsumo>(planPracticaCobInsumoId);
        }

        public PlanPracticaCobInsumo PlanPracticaCobInsumoReadByPlanPractica(int planPracticaId)
        {
            return dalEngine.GetByProperty<PlanPracticaCobInsumo>(
                        PlanPracticaCobInsumo.Properties.PlanPracticaId, planPracticaId);
        }

        public EntityCollection<PlanPracticaCobInsumo> PlanPracticaCobInsumoReadByPlan(int planId)
        {
            string hql = "select ppci from PlanPracticaCobInsumo ppci, PlanPracticaPrecio pp where "
                + " ppci.PlanPracticaId = pp.Id and pp.Plan.Id = :planId order by ppci.PlanPracticaId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planId", planId);
            return dalEngine.GetManyByQuery<PlanPracticaCobInsumo>(query);
        }

        public EntityCollection<PlanPracticaCobInsumo> PlanPracticaCobInsumoReadByPlanPracticaIds(List<int> planPracticaIds)
        {
            if (planPracticaIds != null && planPracticaIds.Count > 0)
            {
                SQLBlockBuilder<int> block = new SQLBlockBuilder<int>(planPracticaIds);
                StringBuilder hql = new StringBuilder();

                hql.Append("select ppci from PlanPracticaCobInsumo ppci, PlanPracticaPrecio pp where ");
                hql.Append(" ppci.PlanPracticaId = pp.Id and pp.Deleted = false ");
                hql.Append(" and " + block.BuildConstrainBlock("pp.Id"));

                hql.Append("order by ppci.PlanPracticaId ");
                IQuery query = dalEngine.CreateQuery(hql.ToString());
                EntityCollection<PlanPracticaCobInsumo> ppci = dalEngine.GetManyByQuery<PlanPracticaCobInsumo>(query);

                if (ppci != null)
                    return ppci;
                else
                    return new EntityCollection<PlanPracticaCobInsumo>();
            }

            return new EntityCollection<PlanPracticaCobInsumo>();
        }

        #endregion

        #region PlanPracticaInsumo

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPlanPracticaAndTipoCategoriaPractica(int planPracticaId, int tipoCategoriaInsumo)
        {
            string hql = "select ppi " +
                         "from Insumo ins , PlanPracticaInsumo ppi " +
                         "where ppi.Insumo.Id = ins.Id " +
                         "and ins.Deleted = false " +
                         "and ins.Categoria.TipoInsumoCategoriaInt = :tipoCategoriaInsumo " +
                         "and ppi.PlanPracticaPrecio.Id = :planPracticaId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planPracticaId", planPracticaId);
            query.SetParameter("tipoCategoriaInsumo", tipoCategoriaInsumo);
            return dalEngine.GetManyByQuery<PlanPracticaInsumo>(query);
        }

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPlanPractica(int planPracticaId)
        {
            return dalEngine.GetManyByProperty<PlanPracticaInsumo>
                        (PlanPracticaInsumo.Properties.PlanPracticaPrecio.Id, planPracticaId);
        }
        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPlan(int planId)
        {
            return dalEngine.GetManyByProperty<PlanPracticaInsumo>
                        (PlanPracticaInsumo.Properties.PlanPracticaPrecio.Plan.Id, planId,
                         PlanPracticaInsumo.Properties.PlanPracticaPrecio.Id);
        }

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPlanAndPractica(int planId, int practicaId)
        {
            Filter filter = new Filter();
            filter.Add(PlanPracticaInsumo.Properties.PlanPracticaPrecio.Plan.Id, "=", planId);
            filter.Add(PlanPracticaInsumo.Properties.PlanPracticaPrecio.Practica.Id, "=", practicaId);
            return dalEngine.GetManyByFilter<PlanPracticaInsumo>(filter);
        }

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPlanAndPracticas(int planId, List<int> practicasIds, DateTime vigencia)
        {
            Filter filter = new Filter();
            filter.Add(PlanPracticaInsumo.Properties.PlanPracticaPrecio.Plan.Id, "=", planId);
            filter.Add(BooleanOp.And, PlanPracticaInsumo.Properties.PlanPracticaPrecio.Practica.Id, "IN", practicasIds);
            filter.Add(BooleanOp.And, PlanPracticaInsumo.Properties.PlanPracticaPrecio.FechaDesde, ">=", vigencia.Date);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(PlanPracticaInsumo.Properties.PlanPracticaPrecio.FechaHasta, "IS", null);
            filter.Add(BooleanOp.Or, PlanPracticaInsumo.Properties.PlanPracticaPrecio.FechaHasta, "<", vigencia.Date.AddDays(1));
            filter.Add(new CloseParenthesis());
            return dalEngine.GetManyByFilter<PlanPracticaInsumo>(filter);
        }

        public EntityCollection<PlanPracticaInsumoLight> PlanPracticaInsumoLightReadByPlanPracticaIds(List<int> planPracticaIds)
        {
            if (planPracticaIds != null && planPracticaIds.Count > 0)
            {
                StringBuilder hql = new StringBuilder();
                hql.Append("select  ppi ");
                hql.Append("from PlanPracticaInsumoLight ppi, PlanPracticaPrecio plp ");
                hql.Append("where ppi.PlanPracticaPrecio = plp.Id and plp.Id in (:planPracticaIds) ");

                hql.Append("and plp.Deleted = false ");

                IQuery query = dalEngine.CreateQuery(hql.ToString());
                query.SetParameterList("planPracticaIds", planPracticaIds);
                return dalEngine.GetManyByQuery<PlanPracticaInsumoLight>(query);
            }

            return new EntityCollection<PlanPracticaInsumoLight>();
        }

        public EntityCollection<PlanPracticaInsumoHC> PlanPracticaInsumoReadByPlanPracticaIds(List<int> planPracticaIds)
        {
            if (planPracticaIds == null || planPracticaIds.Count == 0)
                return new EntityCollection<PlanPracticaInsumoHC>();

            EntityCollection<PlanPracticaInsumoHC> response = new EntityCollection<PlanPracticaInsumoHC>();
            int minRows = Math.Min(planPracticaIds.Count, 1000);
            while (planPracticaIds.Count > 0)
            {
                List<int> ids = planPracticaIds.GetRange(0, minRows);
                response.AddRange((from ins in dalEngine.Query<PlanPracticaInsumoHC>() where ids.Contains(ins.PlanPracticaId) select ins));
                planPracticaIds.RemoveRange(0, minRows);
                minRows = Math.Min(planPracticaIds.Count, 1000);
            }

            return response;
        }

        public EntityCollection<PracticaAdicional> AdicionalesReadByCodes(EntityCollection<PracticaAdicional> adicionales)
        {
            if (adicionales == null || adicionales.Count == 0)
                return new EntityCollection<PracticaAdicional>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select pad from PracticaAdicional pad where ");
            string constaint = SQLBlockBuilder<PracticaAdicional>.ConstruirBloqueTernario("pad", PracticaAdicional.Properties.PracticaID, PracticaAdicional.Properties.Adicional.Code, PracticaAdicional.Properties.Adicional.Region, adicionales);
            constaint = constaint + " and pad.Deleted = false ";
            hqlBuilder.Append(constaint);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            for (int index = 0; index < adicionales.Count; index++)
            {
                query.SetInt32("A" + index.ToString(), adicionales[index].Practica.PracticaId);
                query.SetString("B" + index.ToString(), adicionales[index].Adicional.Code.Trim().ToUpper());
                query.SetString("C" + index.ToString(), adicionales[index].Adicional.Region.Trim().ToUpper());
            }

            return dalEngine.GetManyByQuery<PracticaAdicional>(query);
        }

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoUpdate(EntityCollection<PlanPracticaInsumo> planPracticaInsumos)
        {
            return dalEngine.UpdateCollection<PlanPracticaInsumo>(planPracticaInsumos);
        }

        [RequiresTransaction]
        [Private]
        public virtual EntityCollection<PlanPracticaInsumoLight> PlanPracticaInsumoLightUpdateForHerramientaCopia(EntityCollection<PlanPracticaInsumoLight> planPracticaInsumos)
        {
            return dalEngine.UpdateCollection<PlanPracticaInsumoLight>(planPracticaInsumos);
        }

        public void PlanPracticaInsumoDelete(EntityCollection<PlanPracticaInsumo> planPracticaInsumos)
        {
            dalEngine.Delete(planPracticaInsumos);
        }

        #endregion

        #region PracticaInsumo

        public EntityCollection<PracticaInsumo> PracticaInsumoVigenteReadByPractica(int practicaId)
        {
            string hql = "select pi " +
                            "from Insumo ins , PracticaInsumo pi " +
                            "where pi.Insumo.Id = ins.Id " +
                            "and ins.Categoria.TipoInsumoCategoriaInt is not null " +
                            "and pi.PracticaID = :practicaId " +
                            "and pi.Deleted  = false " +
                            "and ins.Deleted = false ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaId", practicaId);
            return dalEngine.GetManyByQuery<PracticaInsumo>(query);
        }

        /// <summary>
        /// Retorna todos los insumos para una práctica
        /// </summary>
        /// <param name="practicaID">Práctica a buscar</param>
        /// <returns>Todos los insumos de la práctica</returns>
        [AnonymousMethod()]
        public EntityCollection<PracticaInsumo> PracticaInsumoReadByPractica(int practicaId)
        {
            bool eli = false;
            StringBuilder hql = new StringBuilder();
            hql.Append("select pt ");
            hql.Append("from PracticaInsumo pt ");
            hql.Append("where pt.Insumo.Deleted = :eli ");
            hql.Append("and pt.PracticaID = :practicaId ");
            hql.Append("and pt.Deleted = :eli ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("practicaId", practicaId);
            query.SetParameter("eli", eli);
            return dalEngine.GetManyByQuery<PracticaInsumo>(query);
        }

        [AnonymousMethod()]
        public EntityCollection<PracticaInsumo> PracticaInsumoReadByPracticas(IEnumerable<int> practicasIds)
        {
            bool contain = false;
            foreach (int id in practicasIds)
                contain = true;

            if (!contain)
                return new EntityCollection<PracticaInsumo>();
            Filter filter = new Filter();
            filter.Add(PracticaInsumo.Properties.PracticaID, "IN", practicasIds);
            filter.Add(BooleanOp.And, PracticaInsumo.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<PracticaInsumo>(filter);
        }

        /// <summary>
        /// Retorna todos los insumos para una práctica
        /// </summary>
        /// <param name="practicaID">Práctica a buscar</param>
        /// <param name="radioactivo">Si se retornan sólo materiales radioactivos</param>
        /// <returns>Todos los insumos de la práctica</returns>
        public EntityCollection<PracticaInsumo> PracticaInsumoReadByPractica(int practicaID, FilterFlag radioactivo)
        {
            EntityCollection<PracticaInsumo> insumos;
            insumos = this.PracticaInsumoReadByPractica(practicaID);

            if (radioactivo == FilterFlag.NoFilter)
                return insumos;
            else
            {
                bool mustBeRadioactive = (radioactivo == FilterFlag.FilterTrue);

                Predicate<PracticaInsumo> predicate = delegate(PracticaInsumo compare)
                {
                    return compare.Insumo.Radioactivo == mustBeRadioactive;
                };

                EntityCollection<PracticaInsumo> filtered = new EntityCollection<PracticaInsumo>();
                filtered.AddRange(insumos.FindAll(predicate));
                return filtered;
            }
        }

        /// <summary>
        /// Retorna la relacion de insumos y práctica
        /// </summary>
        /// <param name="practicaID">Práctica a buscar</param>
        /// <param name="insumoId">Insumo a buscar</param>
        /// <returns>La relacion de insumo y práctica. Null si no existe</returns>
        public PracticaInsumo PracticaInsumoReadByPracticaInsumo(int practicaID, int insumoID)
        {

            ReadManyCommand<PracticaInsumo> readCmd = new ReadManyCommand<PracticaInsumo>(dalEngine);
            Filter filter = new Filter();
            filter.Add(PracticaInsumo.Properties.PracticaID,
                "=", practicaID);

            filter.Add(BooleanOp.And, PracticaInsumo.Properties.Insumo,
                "=", insumoID);

            readCmd.Filter = filter;
            EntityCollection<PracticaInsumo> col = readCmd.Execute();
            if (col.Count > 0 && col[0] != null)
                return col[0];
            else
                return null;
        }
        [RequiresTransaction]
        public virtual void PracticaInsumoUpdate(EntityCollection<PracticaInsumo> nuevos, EntityCollection<PracticaInsumo> modificados, EntityCollection<PracticaInsumo> eliminados)
        {
            foreach (PracticaInsumo insumo in eliminados)
                dalEngine.Delete(insumo);
            dalEngine.UpdateCollection(nuevos);
            dalEngine.UpdateCollection(modificados);
        }
        #endregion

        #region PlanPracticaPrecio

        #endregion

        #region Practicas por Paciente
        [AnonymousMethod()]
        public EntityCollection<HistorialPracticaPaciente> HistorialPracticaPacienteReadByPaciente(int pacienteID)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT new enfoke.Eges.Entities.Results.HistorialPracticaPaciente(pt.Turno.Id, ");
            sb.Append("pt.Turno.Fecha, ");
            sb.Append("pt.Turno.Orden.MedicoSolicitante.Id, ");
            sb.Append("pt.Turno.Orden.MedicoSolicitante.FirstName, ");
            sb.Append("pt.Turno.Orden.MedicoSolicitante.LastName, ");
            sb.Append("pt.Turno.Orden.Paciente.Id, ");
            sb.Append("pt.Turno.Orden.Paciente.Nombre, ");
            sb.Append("pt.Turno.Orden.Paciente.Apellido, ");
            sb.Append("pt.Practica.Id, ");
            sb.Append("pt.Practica.Name, ");
            sb.Append("ti.Id, ");
            sb.Append("pt.Medico.Name, ");
            sb.Append("pt.Medico.Apellido, ");
            sb.Append("ti.TurnoInformePrincipalId, pt.Id) ");
            sb.Append("FROM PracticaTurnoHQL pt, ");
            sb.Append("TurnoInformeHQL ti ");
            sb.Append("WHERE ti.TurnoHQL = pt.Turno ");
            sb.Append("AND (pt.RegionInforme.Id = ti.RegionInforme.Id  OR (pt.RegionInforme is null AND ti.RegionInforme is null)) ");
            sb.Append("AND ti.EstadoInforme.InformeInformado = true ");
            sb.Append("AND pt.Turno.Orden.Paciente.Id = :pacienteID ");
            sb.Append("AND pt.Practica.InformeRequerido = true ");
            sb.Append("order by pt.Turno.Fecha desc ");

            IQuery query = dalEngine.CreateQuery(sb.ToString());
            query.SetParameter("pacienteID", pacienteID);
            EntityCollection<HistorialPracticaPaciente> colConDuplicados = dalEngine.GetManyByQuery<HistorialPracticaPaciente>(query);

            EntityCollection<HistorialPracticaPaciente> colResul = new EntityCollection<HistorialPracticaPaciente>();
            foreach (HistorialPracticaPaciente item in colConDuplicados)
            {
                if (HayDuplicadoQueReemplaza(colConDuplicados, item))
                    continue;

                colResul.Add(item);
            }

            return colResul;
        }

        private bool HayDuplicadoQueReemplaza(EntityCollection<HistorialPracticaPaciente> colConDuplicados, HistorialPracticaPaciente item)
        {
            foreach (HistorialPracticaPaciente duplicado in colConDuplicados)
                if (item.TurnoInformeId == duplicado.TurnoInformeId && item.TurnoPracticaId != duplicado.TurnoPracticaId && item.TurnoPracticaId > duplicado.TurnoPracticaId)
                    return true;

            return false;
        }

        #endregion

        #region TipoPractica
        public EntityCollection<TipoPractica> TipoPracticaReadAll()
        {
            return dalEngine.GetAll<TipoPractica>(TipoPractica.Properties.Name);
        }





        #endregion

        #region ListaPrecios

        public ListaPrecios ListaPreciosReadByName(string name)
        {
            string hql = "from ListaPrecios lp where upper(ltrim(rtrim(lp.Name))) = :name";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetString("name", name.ToUpper().Trim());
            return dalEngine.GetByQuery<ListaPrecios>(query);
        }

        #endregion

        #region PracticaListaPrecios

        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaListaReadAll()
        {
            return (from pra in dalEngine.Query<PracticaForList>()
                    where pra.Delete == false
                    orderby pra.Name, pra.ServicioEspecialidad.Servicio.Name ascending
                    select new DatosPracticaListaPrecioVigente
                            (pra.Id, pra.EsDetallado, pra.TipoPractica, pra.Name, pra.Code,
                             pra.Region, pra.ServicioEspecialidad, pra.ServicioEspecialidad.Servicio.Name, pra)).ToEntityCollection();
        }

        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaReadParaPlan(DateTime fecha, int planId, bool soloModulos, bool soloSets, bool soloPracticas)
        {
            EntityCollection<PlanPracticaPrecioSinNorma> convenios = PlanPracticaReadByPlanesAndFecha(new List<int>(1) { planId }, fecha, null, soloModulos, soloSets, soloPracticas, true);
            List<KeyValuePair<int, int>> practicasIds = ObtenerPracticasIds(convenios);
            EntityCollection<PracticaListaPrecios> listaPrecios = PracticaListaPreciosReadByPracticasAndFecha(practicasIds, fecha);
            EntityCollection<DatosPracticaListaPrecioVigente> response = ConstruirDatos(convenios, listaPrecios);
            return response;
        }

        private static EntityCollection<DatosPracticaListaPrecioVigente> ConstruirDatos(EntityCollection<PlanPracticaPrecioSinNorma> convenios, EntityCollection<PracticaListaPrecios> listaPrecios)
        {
            EntityCollection<ValidacionTipoOperacion> tiposOperaciones = Context.Session.Dalc.GetAll<ValidacionTipoOperacion>();
            EntityCollection<TipoCoseguro> tiposCoseguros = Context.Session.Dalc.GetAll<TipoCoseguro>();
            EntityCollection<DatosPracticaListaPrecioVigente> response = new EntityCollection<DatosPracticaListaPrecioVigente>();
            foreach (PlanPracticaPrecioSinNorma convenio in convenios)
            {
                DatosPracticaListaPrecioVigente dato = GetDato(listaPrecios, convenio, tiposOperaciones, tiposCoseguros);
                response.Add(dato);
            }
            return response;
        }

        private static DatosPracticaListaPrecioVigente GetDato(EntityCollection<PracticaListaPrecios> listaPrecios, PlanPracticaPrecioSinNorma convenio,
            EntityCollection<ValidacionTipoOperacion> tiposOperaciones, EntityCollection<TipoCoseguro> tiposCoseguros)
        {
            int practicaId = convenio.PracticaAdicional != null ? convenio.PracticaAdicional.Adicional.Id : convenio.Practica.Id;
            PracticaListaPrecios lista = listaPrecios.Find(delegate(PracticaListaPrecios listaPrecio) { return listaPrecio.PracticaID == practicaId && convenio.ListaPrecios != null && convenio.ListaPrecios.Id == listaPrecio.ListaPrecios.Id; });
            ValidacionTipoOperacion validacion = tiposOperaciones.FindByKey(convenio.ValidacionTipoOperacionId);
            TipoCoseguro coseguro = tiposCoseguros.FindByKey(convenio.TipoCoseguroID);
            return new DatosPracticaListaPrecioVigente(convenio, lista, validacion, coseguro);
        }

        private EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByPracticasAndFecha(List<KeyValuePair<int, int>> practicasListasIds, DateTime fecha)
        {
            const int MAX_RECORD = 500;
            if (practicasListasIds == null || practicasListasIds.Count == 0)
                return new EntityCollection<PracticaListaPrecios>();

            EntityCollection<PracticaListaPrecios> response = new EntityCollection<PracticaListaPrecios>();
            while (practicasListasIds.Count > 0)
            {
                StringBuilder hqlBuilder = new StringBuilder();
                hqlBuilder.Append("Select new enfoke.Eges.Entities.PracticaListaPrecios(plp.Id, plp.FechaDesde, plp.FechaHasta, plp.CantidadGastos, uaGastos, ");
                hqlBuilder.Append("plp.CantidadHonorarios, uaHonorarios, plp.CantidadInsumos, uaInsumos, plp.CantidadModulo, uaModulo, plp.ListaPrecios, plp.PracticaID) from PracticaListaPrecios plp ");
                hqlBuilder.Append("left join plp.UAGastos uaGastos left join plp.UAHonorarios uaHonorarios left join plp.UAInsumos uaInsumos left join plp.UAModulo uaModulo where ");
                Filter filter = new Filter();
                hqlBuilder.Append("(");
                for (int index = 0; index < Math.Min(practicasListasIds.Count, MAX_RECORD); index++)
                {
                    hqlBuilder.AppendFormat("(plp.PracticaID = :practica{0} and plp.ListaPrecios.Id = :lista{0}) ", index);
                    if (index < Math.Min(practicasListasIds.Count, MAX_RECORD) - 1)
                        hqlBuilder.Append(" or ");
                }

                hqlBuilder.Append(") and ");
                hqlBuilder.Append("plp.FechaDesde <= :fechaDesde and (plp.FechaHasta is null or plp.FechaHasta > :fechaHasta) and plp.Deleted = false ");
                IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
                for (int index = 0; index < Math.Min(practicasListasIds.Count, MAX_RECORD); index++)
                {
                    query.SetInt32("practica" + index.ToString(), practicasListasIds[index].Key);
                    query.SetInt32("lista" + index.ToString(), practicasListasIds[index].Value);
                }

                query.SetDateTime("fechaDesde", fecha.Date);
                query.SetDateTime("fechaHasta", fecha.Date.AddDays(1));
                response.AddRange(dalEngine.GetManyByQuery<PracticaListaPrecios>(query));
                practicasListasIds.RemoveRange(0, Math.Min(practicasListasIds.Count, MAX_RECORD));
            }

            return response;
        }

        private List<KeyValuePair<int, int>> ObtenerPracticasIds(EntityCollection<PlanPracticaPrecioSinNorma> convenios)
        {
            List<KeyValuePair<int, int>> result = new List<KeyValuePair<int, int>>();
            foreach (PlanPracticaPrecioSinNorma convenio in convenios)
            {
                if (convenio.PracticaAdicional == null && convenio.ListaPrecios != null)
                    result.Add(new KeyValuePair<int, int>(convenio.Practica.Id, convenio.ListaPrecios.Id));
                else if (convenio.ListaPrecios != null)
                    result.Add(new KeyValuePair<int, int>(convenio.PracticaAdicional.Adicional.Id, convenio.ListaPrecios.Id));
            }

            return result;
        }

        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaReadParaPlanes(DateTime fecha, List<int> planesIds, bool soloModulos, bool soloSets, bool soloPracticas)
        {
            EntityCollection<PlanPracticaPrecioSinNorma> convenios = PlanPracticaReadByPlanesAndFecha(planesIds, fecha, null, soloModulos, soloSets, soloPracticas, true);
            List<KeyValuePair<int, int>> practicasIds = ObtenerPracticasIds(convenios);
            EntityCollection<PracticaListaPrecios> listaPrecios = PracticaListaPreciosReadByPracticasAndFecha(practicasIds, fecha);
            EntityCollection<DatosPracticaListaPrecioVigente> response = ConstruirDatos(convenios, listaPrecios);
            return response;
        }

        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaReadParaPlanes(DateTime fecha, List<int> planesIds, int practicaId, bool soloModulos, bool soloSets, bool soloPracticas)
        {
            EntityCollection<PlanPracticaPrecioSinNorma> convenios = PlanPracticaReadByPlanesAndFecha(planesIds, fecha, new List<int>() { practicaId }, soloModulos, soloSets, soloPracticas, true);
            List<KeyValuePair<int, int>> practicasIds = ObtenerPracticasIds(convenios);
            EntityCollection<PracticaListaPrecios> listaPrecios = PracticaListaPreciosReadByPracticasAndFecha(practicasIds, fecha);
            EntityCollection<DatosPracticaListaPrecioVigente> response = ConstruirDatos(convenios, listaPrecios);
            return response;
        }

        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaReadParaPlanesAndPracticas(DateTime fecha, List<int> planesIds, List<int> practicasIds, bool soloModulos, bool soloSets, bool soloPracticas, bool actualizaNoConvenidas)
        {
            EntityCollection<PlanPracticaPrecioSinNorma> convenios = PlanPracticaReadByPlanesAndFecha(planesIds, fecha, practicasIds, soloModulos, soloSets, soloPracticas, actualizaNoConvenidas);
            List<KeyValuePair<int, int>> practicasListasIds = ObtenerPracticasIds(convenios);
            EntityCollection<PracticaListaPrecios> listaPrecios = PracticaListaPreciosReadByPracticasAndFecha(practicasListasIds, fecha);
            EntityCollection<DatosPracticaListaPrecioVigente> response = ConstruirDatos(convenios, listaPrecios);
            return response;
        }


        private EntityCollection<PlanPracticaPrecioSinNorma> PlanPracticaReadByPlanesAndFecha(List<int> planesIds, DateTime fecha, IList<int> practicasIds, bool filtraModulos, bool filtraSets, bool filtraPracticas, bool actualizaNoConvenidas)
        {
            if (planesIds == null || planesIds.Count == 0)
                return new EntityCollection<PlanPracticaPrecioSinNorma>();

            List<int> tiposFiltro = ObtenerTiposPracticas(filtraModulos, filtraSets, filtraPracticas);
            if (tiposFiltro.Count == 0)
                return new EntityCollection<PlanPracticaPrecioSinNorma>();

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(planesIds);
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select distinct new enfoke.Eges.Entities.PlanPracticaPrecioSinNorma(pra, adi, equ, ser, plan, ppr.Id, ppr.CantidadGastos, ppr.CantidadHonorarios, ");
            hqlBuilder.Append("ppr.CantidadInsumos, ppr.CantidadModulo, ppr.CoberturaGastos, ppr.CoberturaHonorarios, ppr.CoberturaInsumos, ppr.CoberturaModulo,  ");
            hqlBuilder.Append("ppr.CodigoInterno, ppr.CoeficienteLista, ppr.TipoCoseguroID, ppr.ImporteCoseguro, ppr.ExigeAutorizacionMarca, ppr.ExigeConfirmacionMarca, ");
            hqlBuilder.Append("ppr.ExigeInformeMarca, ppr.ExigePresupuestoMarca, ppr.FechaDesde, ppr.FechaHasta, ppr.MedicoCobraHonorarios, ppr.RequiereAuditoria, ppr.ValidacionTipoOperacionId, ");
            hqlBuilder.Append("tco, uaGastos, uaHonorarios, uaInsumos, uaModulo, lista, ppr.CreateDate, ppr.UpdateDate ) ");
            hqlBuilder.Append("from PlanPracticaPrecioSinNorma ppr join ppr.Practica pra join ppr.Plan plan left join ppr.PracticaAdicional adi left join ppr.Equipo equ ");
            hqlBuilder.Append("left join equ.Servicio ser left join ppr.TipoCobertura tco left join ppr.UAHonorarios uaHonorarios left join ppr.UAGastos uaGastos left join ppr.UAInsumos uaInsumos ");
            hqlBuilder.Append("left join ppr.UAModulo uaModulo left join ppr.ListaPrecios lista ");
            hqlBuilder.Append("where " + blockBuilder.BuildConstrainBlock("plan.Id"));
            hqlBuilder.Append(" and ppr.FechaDesde <= :fecha and (ppr.FechaHasta is null or ppr.FechaHasta >= :fecha)");
            hqlBuilder.Append(" and ppr.Deleted = false and pra.Deleted = false ");
            hqlBuilder.Append(" and (adi is null or (pra.TipoPractica.Id = " + ((int)TipoPracticaEnum.Modulo).ToString() + " and pra.EsDetallado = true and adi.Deleted = false)) ");
            hqlBuilder.Append(" and pra.TipoPractica.Id IN (:tipos) ");
            if (!actualizaNoConvenidas)
                hqlBuilder.Append(" and tco.Id <> :noConvenida and tco.Id <> :noCubre ");

            if (practicasIds != null && practicasIds.Count > 0)
            {
                SQLBlockBuilder<int> blockPracticas = new SQLBlockBuilder<int>(practicasIds);
                hqlBuilder.Append(" and ");
                hqlBuilder.Append(blockPracticas.BuildConstrainBlock("pra.Id"));
            }

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetDateTime("fecha", fecha.Date);
            query.SetParameterList("tipos", tiposFiltro);
            if (!actualizaNoConvenidas)
            {
                query.SetParameter("noConvenida", (int)TipoCoberturaEnum.NoConvenida);
                query.SetParameter("noCubre", (int)TipoCoberturaEnum.NoCubre);
            }
            return dalEngine.GetManyByQuery<PlanPracticaPrecioSinNorma>(query);
        }


        public List<int> ObtenerTiposPracticas(bool filtraModulos, bool filtraSets, bool filtraPracticas)
        {
            List<int> ids = new List<int>();
            if (filtraPracticas)
            {
                Array all = Enum.GetValues(typeof(TipoPracticaEnum));
                foreach (object value in all)
                {
                    int current = (int)value;
                    if (current != (int)TipoPracticaEnum.Modulo && current != (int)TipoPracticaEnum.SetFarmacia)
                        ids.Add(current);
                }
            }

            if (filtraModulos)
                ids.Add((int)TipoPracticaEnum.Modulo);
            if (filtraSets)
                ids.Add((int)TipoPracticaEnum.SetFarmacia);

            return ids;
        }
        public EntityCollection<DatosPracticaListaPrecioVigente> DatosPracticaListaPrecioVigenteReadAll(int listaPrecio, bool ordeServicio, bool ordenCodigo)
        {
            DateTime fecha = enfoke.Time.Now.Date;

            StringBuilder hql = new StringBuilder("Select distinct new enfoke.Eges.Entities.Results.DatosPracticaListaPrecioVigente(pra.Id, pra.EsDetallado, pra.TipoPractica, pra.Name, pra.Code, pra.Region, plp.CantidadGastos, plp.CantidadHonorarios, plp.CantidadModulo, plp.CantidadInsumos, plp.ListaPrecios.Id, lp.Name, pra.ServicioEspecialidad, pra.ServicioEspecialidad.Servicio.Name)");
            hql.Append(" from PracticaListaPrecios plp , Practica pra, ListaPrecios lp, Servicio srv");
            hql.Append(" where plp.PracticaID = pra.Id");
            hql.Append(" and plp.ListaPrecios.Id = lp.Id");
            hql.Append(" and lp.Id = :listaPrecio");
            hql.Append(" and plp.Deleted = false");
            hql.Append(" and pra.Deleted = false");
            hql.Append(" and plp.FechaDesde <= :fecha");
            hql.Append(" and (plp.FechaHasta >= :fecha OR plp.FechaHasta is null)");

            if (!ordeServicio && !ordenCodigo)
                hql.Append(" order by  pra.Name, pra.ServicioEspecialidad.Servicio.Name ");
            else if (ordeServicio)
                hql.Append(" order by  pra.ServicioEspecialidad.Servicio.Name, pra.Name ");
            else
                hql.Append(" order by  pra.Code , pra.Name ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fecha", fecha);
            query.SetParameter("listaPrecio", listaPrecio);

            return dalEngine.GetManyByQuery<DatosPracticaListaPrecioVigente>(query);

        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByPractica(int practicaID, bool soloVigentes)
        {
            string hql = "from PracticaListaPrecios plp where plp.PracticaID = :practicaID ";
            if (soloVigentes)
            {
                hql += "AND plp.Deleted = false "
                    + "AND plp.FechaDesde <= :desde "
                    + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";
            }
            hql += " order by plp.ListaPrecios.Id, plp.FechaDesde";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            if (soloVigentes)
            {
                query.SetParameter("desde", enfoke.Time.Today);
                query.SetParameter("hasta", enfoke.Time.Today);
            }

            return dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByPracticaAndListaNames(IList<PracticaListaPrecios> precios, DateTime fechaVigencia)
        {
            if (precios == null || precios.Count == 0)
                return new EntityCollection<PracticaListaPrecios>();

            Filter filter = new Filter();
            for (int index = 0; index < precios.Count; index++)
            {
                filter.Add(new OpenParenthesis(BooleanOp.Or));
                filter.Add(PracticaListaPrecios.Properties.Deleted, "=", false);
                filter.Add(BooleanOp.And, PracticaListaPrecios.Properties.PracticaID, "=", precios[index].PracticaID);
                filter.Add(BooleanOp.And, PracticaListaPrecios.Properties.FechaDesde, "<=", fechaVigencia.Date);
                filter.Add(new OpenParenthesis(BooleanOp.And));
                filter.Add(PracticaListaPrecios.Properties.FechaHasta, "IS", null);
                filter.Add(BooleanOp.Or, PracticaListaPrecios.Properties.FechaHasta, ">", fechaVigencia.Date.AddDays(1));
                filter.Add(new CloseParenthesis());
                filter.Add(new CloseParenthesis());
            }

            return dalEngine.GetManyByFilter<PracticaListaPrecios>(filter);
        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByListaAndPractica(int listaPrecioId, int practicaID, bool soloVigentes)
        {
            string hql = "from PracticaListaPrecios plp where plp.PracticaID = :practicaID " +
                         "and plp.ListaPrecios.Id = :listaPrecioId ";
            if (soloVigentes)
            {
                hql += "AND plp.Deleted = false "
                    + "AND plp.FechaDesde <= :desde "
                    + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";
            }
            hql += " order by plp.ListaPrecios.Id, plp.FechaDesde";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("listaPrecioId", listaPrecioId);
            if (soloVigentes)
            {
                query.SetParameter("desde", enfoke.Time.Today);
                query.SetParameter("hasta", enfoke.Time.Today);
                DateTime fecha = enfoke.Time.Today;
            }

            EntityCollection<PracticaListaPrecios> list = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            return list;
        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByLista(int listaPrecioId, DateTime fechaVigencia)
        {
            string hql = "from PracticaListaPrecios plp " +
                         "where  plp.ListaPrecios.Id = :listaPrecioId ";
            
            hql += "AND plp.Deleted = false "
                + "AND plp.FechaDesde <= :desde "
                + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";

            hql += " order by plp.ListaPrecios.Id, plp.FechaDesde";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("listaPrecioId", listaPrecioId);

            query.SetParameter("desde", fechaVigencia.Date);
            query.SetParameter("hasta", fechaVigencia.Date);
            EntityCollection<PracticaListaPrecios> list = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            return list;
        }


        public EntityCollection<PracticaFullListaPrecios> PracticaListaPreciosReadByEspecialidadListaAndPrecio(int listaPrecioId, int? especialidadId, int? servicioId, DateTime fechaVigencia)
        {
            IQueryable<PracticaFullListaPrecios> listas = null;
            if (especialidadId.HasValue)
                listas = from plp in dalEngine.Query<PracticaFullListaPrecios>()
                         where plp.ListaPrecios.Id == listaPrecioId
                         && plp.FechaDesde <= fechaVigencia.Date
                         && !plp.Deleted
                         && (plp.FechaHasta == null || fechaVigencia.Date <= plp.FechaHasta)
                         && (plp.Practica.ServicioEspecialidad.Id == especialidadId.Value)
                         && !plp.Practica.Deleted
                         select plp;
            else
            {
                if (!servicioId.HasValue)
                {
                    listas = from plp in dalEngine.Query<PracticaFullListaPrecios>()
                             where plp.ListaPrecios.Id == listaPrecioId
                             && plp.FechaDesde <= fechaVigencia.Date && !plp.Deleted
                             && (plp.FechaHasta == null || fechaVigencia.Date <= plp.FechaHasta)
                             && !plp.Practica.Deleted
                             select plp;
                }
                else
                    listas = from plp in dalEngine.Query<PracticaFullListaPrecios>()
                             where plp.ListaPrecios.Id == listaPrecioId
                             && plp.FechaDesde <= fechaVigencia.Date && !plp.Deleted
                             && (plp.FechaHasta == null || fechaVigencia.Date <= plp.FechaHasta)
                             && (plp.Practica.ServicioEspecialidad.Servicio.Id == servicioId.Value)
                             && !plp.Practica.Deleted
                             select plp;
            }

            return listas.ToEntityCollection();
        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByListaAndPracticasVigentes(int listaPrecioId, List<int> practicasIDs)
        {
            if (practicasIDs == null || practicasIDs.Count == 0)
                return new EntityCollection<PracticaListaPrecios>();

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(practicasIDs);
            string block = blockBuilder.BuildConstrainBlock("plp.PracticaID");
            string hql = "from PracticaListaPrecios plp where  " + block +
                         " and plp.ListaPrecios.Id = :listaPrecioId ";
            hql += "AND plp.Deleted = false "
                + "AND plp.FechaDesde <= :desde "
                + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("listaPrecioId", listaPrecioId);
            query.SetParameter("desde", enfoke.Time.Now.Date);
            query.SetParameter("hasta", enfoke.Time.Now.Date);

            EntityCollection<PracticaListaPrecios> list = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            return list;
        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByListaAndPracticasAndFecha(int listaPrecioId, List<int> practicasIDs, DateTime fechaVigencia)
        {
            if (practicasIDs == null || practicasIDs.Count == 0)
                return new EntityCollection<PracticaListaPrecios>();

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(practicasIDs);
            string block = blockBuilder.BuildConstrainBlock("plp.PracticaID");
            string hql = "from PracticaListaPrecios plp where  " + block +
                         " and plp.ListaPrecios.Id = :listaPrecioId ";
            hql += "AND plp.Deleted = false "
                + "AND plp.FechaDesde <= :desde "
                + "AND (plp.FechaHasta >= :hasta OR plp.FechaHasta is null) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("listaPrecioId", listaPrecioId);
            query.SetParameter("desde", fechaVigencia.Date);
            query.SetParameter("hasta", fechaVigencia.Date);

            EntityCollection<PracticaListaPrecios> list = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            return list;
        }

        public PracticaListaPrecios PracticaListaPreciosReadByPracticaListaVigente(int practicaID, int listaPrecionsID)
        {
            return this.PracticaListaPreciosReadByPracticaListaAndFecha(practicaID, listaPrecionsID, enfoke.Time.Now);
        }
        public PracticaListaPrecios PracticaListaPreciosReadByPracticaListaAndFecha(int practicaID, int listaPreciosID, DateTime fecha)
        {
            if (fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            string hql = "FROM PracticaListaPrecios plp "
                       + "WHERE plp.PracticaID = :practicaID "
                       + "AND plp.ListaPrecios.Id = :listaPreciosID "
                       + "AND plp.FechaDesde <= :fecha "
                       + "AND (plp.FechaHasta IS NULL OR plp.FechaHasta >= :fecha) "
                       + "AND plp.Deleted = false "
                       + "ORDER BY plp.FechaDesde desc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("listaPreciosID", listaPreciosID);
            query.SetParameter("fecha", fecha.Date);

            EntityCollection<PracticaListaPrecios> practicasListaPrecio = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            if (practicasListaPrecio != null && practicasListaPrecio.Count > 0)
                return practicasListaPrecio[0];
            else
                return null;
        }
        public PracticaListaPrecios PracticaListaPreciosReadByPracticasListaAndFecha(List<int> practicaID, int listaPreciosID, DateTime fecha)
        {
            if (fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            string hql = "FROM PracticaListaPrecios plp "
                       + "WHERE plp.PracticaID = :practicaID "
                       + "AND plp.ListaPrecios.Id = :listaPreciosID "
                       + "AND plp.FechaDesde <= :fecha "
                       + "AND (plp.FechaHasta IS NULL OR plp.FechaHasta >= :fecha) "
                       + "AND plp.Deleted = false "
                       + "ORDER BY plp.FechaDesde desc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("listaPreciosID", listaPreciosID);
            query.SetParameter("fecha", fecha.Date);

            EntityCollection<PracticaListaPrecios> practicasListaPrecio = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            if (practicasListaPrecio != null && practicasListaPrecio.Count > 0)
                return practicasListaPrecio[0];
            else
                return null;
        }

        public PracticaListaPrecios PracticaListaPreciosReadByPracticaListaAndFecha(int practicaID, int listaPreciosID)
        {
            DateTime fecha = enfoke.Time.Today.Date;

            string hql = "FROM PracticaListaPrecios plp "
                       + "WHERE plp.PracticaID = :practicaID "
                       + "AND plp.ListaPrecios.Id = :listaPreciosID "
                       + "AND plp.FechaDesde <= :fecha "
                       + "AND (plp.FechaHasta IS NULL OR plp.FechaHasta >= :fecha) "
                       + "AND plp.Deleted = false "
                       + "ORDER BY plp.FechaDesde desc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaID", practicaID);
            query.SetParameter("listaPreciosID", listaPreciosID);
            query.SetParameter("fecha", fecha);

            EntityCollection<PracticaListaPrecios> practicasListaPrecio = dalEngine.GetManyByQuery<PracticaListaPrecios>(query);
            if (practicasListaPrecio != null && practicasListaPrecio.Count > 0)
                return practicasListaPrecio[0];
            else
                return null;
        }

        public void PracticaListaPrecioUpdate(PracticaListaPrecios practicaListaPrecio, EntityCollection<PracticaListaPrecios> practicasListaPrecio)
        {
            PracticaListaPrecioUpdate(practicaListaPrecio, false, false, practicasListaPrecio);
        }

        public void PracticaListaPrecioFinalizar(PracticaListaPrecios practicaListaPrecio, DateTime fechaHasta)
        {
            // Seteo la Fecha Hasta
            practicaListaPrecio.FechaHasta = fechaHasta;

            // Actualizo
            PracticaListaPrecioUpdate(practicaListaPrecio, true, false, null);
        }

        public void PracticaListaPrecioEliminar(PracticaListaPrecios practicaListaPrecio)
        {
            // Marco como Eliminado
            practicaListaPrecio.Deleted = true;

            // Actualizo
            PracticaListaPrecioUpdate(practicaListaPrecio, false, true, null);
        }

        [RequiresTransaction]
        protected virtual void PracticaListaPrecioUpdate(PracticaListaPrecios practicaListaPrecio, bool finalizar, bool eliminar, EntityCollection<PracticaListaPrecios> practicasListaPrecio)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            EntityCollection<PracticaListaPrecios> practicasListaPrecioMod = new EntityCollection<PracticaListaPrecios>();
            if (practicasListaPrecio == null || practicasListaPrecio.Count == 0)
                practicasListaPrecio = PracticaListaPreciosReadByListaAndPractica(practicaListaPrecio.ListaPrecios.Id, practicaListaPrecio.PracticaID, false);

            if (!eliminar)
            {

                if (practicasListaPrecio == null && practicaListaPrecio.Id > 0)
                {
                    practicasListaPrecio = new EntityCollection<PracticaListaPrecios>();
                    //practicasListaPrecio.Add(practicaListaPrecio);
                }
                // Obtengo las modificaciones y las Agrego a la coleccion de modificados
                practicasListaPrecioMod.AddRange(VigenciaUtils<PracticaListaPrecios>.ObtenerModificaciones(practicasListaPrecio, practicaListaPrecio, finalizar, user));
            }
            else
            {
                // Audito
                Audit.AuditDelete(practicaListaPrecio, user.Id);

                // Agrego a la coleccion de modificados
                practicasListaPrecioMod.Add(practicaListaPrecio);
            }


            // Actualizo
            practicasListaPrecioMod = dalEngine.UpdateCollection<PracticaListaPrecios>(practicasListaPrecioMod);

        }

        public EntityCollection<PracticaListaPrecios> PracticaListaPreciosReadByUnidadArancelaria(UnidadArancelaria ua)
        {
            ReadManyCommand<PracticaListaPrecios> readCmd = new ReadManyCommand<PracticaListaPrecios>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(PracticaListaPrecios.Properties.UAGastos.Id, "=", ua.Id);
            filter.Add(BooleanOp.Or, PracticaListaPrecios.Properties.UAHonorarios.Id, "=", ua.Id);
            filter.Add(BooleanOp.Or, PracticaListaPrecios.Properties.UAInsumos.Id, "=", ua.Id);
            filter.Add(BooleanOp.Or, PracticaListaPrecios.Properties.UAModulo.Id, "=", ua.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        #endregion

        #region Practicas Por Servicio
        public EntityCollection<PracticasPorServicio> PracticasPorServicioReadAll()
        {
            ServiciosDalc ServiciosDalc = Context.Session.ServiciosDalc;

            EntityCollection<Servicio> servicios = ServiciosDalc.ServicioReadAllPuros();

            EntityCollection<PracticasPorServicio> practicasPorServicios = new EntityCollection<PracticasPorServicio>();

            foreach (Servicio servicio in servicios)
            {
                PracticasPorServicio pps = new PracticasPorServicio();
                pps.Servicio = servicio;
                pps.Practicas.AddRange(PracticaReadByServicio(servicio.Id).Collection);
                practicasPorServicios.Add(pps);
            }

            return practicasPorServicios;

        }
        #endregion

        #region PracticaForNomenclador

        public PracticaForNomencladorConRegiones PracticaForNomencladorReadByNameCodeServicioAndDeleted(string name, string code, string servicio, string especialidad, bool? deleted)
        {
            return PracticaForNomencladorReadByNameCodeServicioAndDeleted(name, code, servicio, especialidad, null, deleted);
        }

        public PracticaForNomencladorConRegiones PracticaForNomencladorReadByNameCodeServicioAndDeleted(string name, string code, string servicio, string especialidad, int? servicioId, bool? deleted)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;

            PracticaForNomencladorConRegiones ret = new PracticaForNomencladorConRegiones();
            name = name.Trim().Replace(" ", "%") + "%";
            servicio = servicio.Trim().Replace(" ", "%") + "%";
            especialidad = especialidad.Trim().Replace(" ", "%") + "%";
            code = code.Trim() + "%";

            string condiciones = this.GenenerarCondicionesWhereForNomenclador(name, code, servicio, especialidad, servicioId, deleted);

            string hql = "SELECT new enfoke.Eges.Entities.Results.PracticaForNomenclador( " +
                             "p.Id, p.Code, p.Region, p.Name, p.InternalName, p.Abreviacion, p.ServicioEspecialidad, " +
                             "p.TipoPractica.Id, p.TipoPractica.Name, p.InformeRequerido, p.RegionInformeId, p.Deleted) " +
                         "FROM Practica p " + condiciones;
            hql += " ORDER BY p.Name ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            if (!String.IsNullOrEmpty(name))
                query.SetParameter("name", name);
            if (!String.IsNullOrEmpty(code))
                query.SetParameter("code", code);
            if (!String.IsNullOrEmpty(servicio))
                query.SetParameter("servicio", servicio);
            if (!String.IsNullOrEmpty(especialidad))
                query.SetParameter("especialidad", especialidad);
            if (servicioId.HasValue)
                query.SetParameter("idServicio", servicioId.Value);
            if (deleted.HasValue)
                query.SetParameter("deleted", deleted);

            ret.Practicas = dalEngine.GetManyByQuery<PracticaForNomenclador>(query);

            // Recupero las regiones por separado
            Dictionary<int, string> regiones = new Dictionary<int, string>();

            EntityCollection<RegionInforme> ris = InformesDalc.RegionInformeReadAll();
            foreach (RegionInforme region in ris)
            {
                regiones.Add(region.Id, region.Name);
            }
            ret.Regiones = regiones;

            return ret;
        }

        private string GenenerarCondicionesWhereForNomenclador(string name, string code, string servicio, string especialidad, int? servicioId, bool? deleted)
        {
            string hql = "WHERE 1 = 1 ";

            if (!String.IsNullOrEmpty(name))
                hql += " AND p.Name LIKE :name";
            if (!String.IsNullOrEmpty(code))
                hql += " AND p.Code LIKE :code";
            if (!String.IsNullOrEmpty(servicio))
                hql += " AND p.ServicioEspecialidad.Servicio.Name LIKE :servicio";
            if (!String.IsNullOrEmpty(especialidad))
                hql += " AND p.ServicioEspecialidad.Nombre LIKE :especialidad";
            if (servicioId.HasValue)
                hql += " AND p.ServicioEspecialidad.Servicio.Id = :idServicio";
            if (deleted.HasValue)
                hql += " AND p.Deleted = :deleted";

            return hql;
        }

        public PracticaForNomencladorConRegiones PracticaForNomencladorReadByServicio(int servicioID)
        {
            return PracticaForNomencladorReadByNameCodeServicioAndDeleted(string.Empty, string.Empty, string.Empty, string.Empty, servicioID, false);
        }

        #endregion

        public ReadAllCollection<PracticaName> PracticaNameReadByServicios(List<int> servicioIDs)
        {
            EntityCollection<PracticaName> practicaNames = new EntityCollection<PracticaName>();

            Filter filter = new Filter
                                {
                                    {Practica.Properties.ServicioEspecialidad.Servicio.Id, " IN ", servicioIDs.ToArray()},
                                    {BooleanOp.And, Practica.Properties.Deleted, "=", false}
                                };

            Sort sort = new Sort { Practica.Properties.Code, Practica.Properties.Region };

            string lastCode = string.Empty;

            foreach (Practica practica in dalEngine.GetManyByFilter<Practica>(filter, sort))
            {
                if (lastCode != practica.Code)
                {
                    practicaNames.Add(new PracticaName(practica.Id, practica.Name, practica.Code,
                                                       practica.DuracionSeconds, practica.TipoPractica.Id));

                    lastCode = practica.Code;
                }
            }

            return new ReadAllCollection<PracticaName>(practicaNames);
        }

        public EntityCollection<PracticaName> PracticaNameReadByServicioId(int idServicio)
        {
            Filter filter = new Filter
                                {
                                    {PracticaName.Properties.ServicioEspecialidad.Servicio.Id, " = ", idServicio},
                                    {BooleanOp.And, PracticaName.Properties.Deleted, "=", false}
                                };

            Sort sort = new Sort { PracticaName.Properties.Code, PracticaName.Properties.Region };
            return dalEngine.GetManyByFilter<PracticaName>(filter, sort);
        }

        public EntityCollection<CondicionPractica> CondicionPracticaReadByCondicionId(int condicionId)
        {
            return Context.Session.Dalc.GetManyByProperty<CondicionPractica>(CondicionPractica.Properties.Condicion.Id, condicionId);
        }

        [RequiresTransaction]
        public virtual void CondicionPracticaUpdate(Condicion condicion, EntityCollection<PracticaName> updatePracticas)
        {
            EntityCollection<CondicionPractica> condicionesPracticaAEliminar = CondicionPracticaReadByCondicionId(condicion.Id);
            dalEngine.Delete(condicionesPracticaAEliminar); ///-->elimino toda las condiciones practicas ya existente

            EntityCollection<CondicionPracticaLight> condicionesPracticaAAgregar = new EntityCollection<CondicionPracticaLight>();
            foreach (PracticaName practica in updatePracticas)
            {
                CondicionPracticaLight condicionPractica = new CondicionPracticaLight();
                condicionPractica.CondicionId = condicion.Id;
                condicionPractica.PracticaId = practica.Id;
                condicionesPracticaAAgregar.Add(condicionPractica);
            }

            dalEngine.UpdateCollection<CondicionPracticaLight>(condicionesPracticaAAgregar);
        }

        public EntityCollection<PlanPracticaRequisito> GetPlanesPracticasRequisitosByIds(Dictionary<int, List<int>> diccObraSocialPracticas)
        {
            DateTime now = enfoke.Time.Today;
            return GetPlanesPracticasRequisitosByIds(diccObraSocialPracticas, now);
        }

        public EntityCollection<PlanPracticaRequisito> GetPlanesPracticasRequisitosByIds(Dictionary<int, List<int>> diccObraSocialPracticas, DateTime fechaBusqueda)
        {
            DateTime fechaBusquedaDesde = fechaBusqueda.Date.AddDays(1.0).AddSeconds(-1.0);
            DateTime fechaBusquedaHasta = fechaBusqueda.Date.AddSeconds(-1.0);
            EntityCollection<PlanPracticaRequisito> planesPracticas = new EntityCollection<PlanPracticaRequisito>();
            foreach (int key in diccObraSocialPracticas.Keys)
            {
                EntityCollection<PlanPracticaRequisito> pprs = new EntityCollection<PlanPracticaRequisito>();
                if (diccObraSocialPracticas[key].Count > 0)
                {
                    pprs = (from pprQuery in dalEngine.Query<PlanPracticaRequisito>()
                            where
                              !pprQuery.Deleted &&
                              pprQuery.Plan.Id == key &&
                              diccObraSocialPracticas[key].Contains(pprQuery.Practica.Id) &&
                              (
                                    pprQuery.FechaDesde < fechaBusquedaDesde &&
                                   ((pprQuery.FechaHasta == null) || pprQuery.FechaHasta > fechaBusquedaHasta)
                              )
                            select pprQuery).ToEntityCollection();
                    planesPracticas.AddRange(pprs);

                    if (pprs.Count > 0)
                    {
                        EntityCollection<PlanPracticaDocumentacion> docs = (from documentacion in this.dalEngine.Query<PlanPracticaDocumentacion>()
                                                                            where pprs.GetIds().Contains(documentacion.PlanPracticaRequisitoId)
                                                                            orderby documentacion.PlanPracticaRequisitoId
                                                                            select documentacion
                                                                ).ToEntityCollection<PlanPracticaDocumentacion>();

                        foreach (PlanPracticaRequisito docPPR in pprs)
                        {
                            docPPR.DocumentacionRequerida.AddRange(docs.FindAll(delegate(PlanPracticaDocumentacion ppDoc) { return ppDoc.RequisitosId == docPPR.Id; }));
                        }
                    }
                }
            }

            return planesPracticas;
        }

        #region Preparaciones

        public EntityCollection<Preparacion> PreparacionReadByParameters(string descripcion)
        {
            ReadManyCommand<Preparacion> preparacion = new ReadManyCommand<Preparacion>(dalEngine);

            Filter filter = new Filter();
            if (!string.IsNullOrEmpty(descripcion))
                filter.Add(Preparacion.Properties.Descripcion, "LIKE", '%' + descripcion.Replace(' ', '%') + '%');

            preparacion.Filter = filter;
            preparacion.Sort = new Sort(new SortItem(Preparacion.Properties.Descripcion, SortingDirection.Asc));

            return preparacion.Execute();
        }


        public int PreparacionCantidadPracticasRelacionadas(int preparacionId)
        {
            return (from practica in dalEngine.Query<Practica>()
                    where practica.PreparacionId == preparacionId
                    select practica).Count();


        }

        public EntityCollection<Practica> PracticaReadByPreparacion(int preparacionID)
        {
            List<Practica> practicas = (from practica in dalEngine.Query<Practica>()
                                        where practica.PreparacionId == preparacionID
                                        select practica).ToList();

            return new EntityCollection<Practica>(practicas);

        }

        [RequiresTransaction]
        public virtual void PreparacionRelacionarPracticasUpdate(int preparacionId, EntityCollection<Practica> nuevos, EntityCollection<Practica> eliminados)
        {
            foreach (Practica pi in eliminados)
                pi.PreparacionId = pi.PreparacionId == preparacionId ? null : pi.PreparacionId;
            foreach (Practica pi in nuevos)
                pi.PreparacionId = preparacionId;

            dalEngine.UpdateCollection(eliminados);
            dalEngine.UpdateCollection(nuevos);
        }

        #endregion//Preparaciones

        #region Consentimiento

        public Consentimiento ConsentimientoReadByName(string nombre, bool soloHabilitados)
        {
            var query = from con in dalEngine.Query<Consentimiento>() where con.Name == nombre && (!soloHabilitados || !con.Deleted) select con;
            EntityCollection<Consentimiento> cons = query.ToEntityCollection();

            return cons.Count > 0 ? cons[0] : null;
        }

        public Dictionary<int, EntityCollection<Consentimiento>> ConsentimientoReadByPracticasIds(List<int> practicasIds)
        {
            if (practicasIds == null || practicasIds.Count <= 0)
                return new Dictionary<int, EntityCollection<Consentimiento>>();

            var query = from pco in dalEngine.Query<PracticaConsentimiento>() join con in dalEngine.Query<Consentimiento>() on pco.ConsentimientoId equals con.Id where practicasIds.Contains(pco.PracticaId) select new { PracticaId = pco.PracticaId, Consentimiento = con };

            Dictionary<int, EntityCollection<Consentimiento>> result = new Dictionary<int, EntityCollection<Consentimiento>>();
            foreach (var par in query)
            {
                if (!result.ContainsKey(par.PracticaId))
                    result.Add(par.PracticaId, new EntityCollection<Consentimiento>());

                result[par.PracticaId].Add(par.Consentimiento);
            }

            return result;
        }

        public List<int> ConsentimientoIdsReadByPracticaId(int practicaId)
        {
            var query = from pco in dalEngine.Query<PracticaConsentimiento>() where pco.PracticaId == practicaId select pco.ConsentimientoId;

            return query.ToList();
        }

        #endregion

        public Practica GetPracticaPrincipalByTurnoId(int turId)
        {
            Practica practica = (from pt in dalEngine.Query<PracticaTurno>()
                                 where
                                    pt.TurnoId == turId &&
                                    pt.Tipo == (int)PracticaTurnoTipoEnum.Principal
                                 select pt.Practica).FirstOrDefault<Practica>();

            return practica;
        }

        public EntityCollection<T> GetPracticasByTurnoId<T>(int turId, PracticaTurnoTipoEnum tipoPracticaTurno) where T : IIdentificable
        {
            EntityCollection<T> practicas = (from pt in dalEngine.Query<PracticaTurno>()
                                             join pad in dalEngine.Query<T>()
                                                on pt.Practica.Id equals pad.Id
                                             where
                                                pt.TurnoId == turId &&
                                                pt.Tipo == (int)PracticaTurnoTipoEnum.Adicional
                                             select pad).ToEntityCollection();

            return practicas;
        }

        public Medico GetMedicoByTurnoId(int turId)
        {
            Medico medico = (from pt in dalEngine.Query<PracticaTurno>()
                             where
                                pt.TurnoId == turId &&
                                pt.Tipo == (int)PracticaTurnoTipoEnum.Principal
                             select pt.Medico).FirstOrDefault<Medico>();
            return medico;
        }

        public void CrearRelacionMedicoPractica(Medico medico, Practica practica)
        {
            MedicoPractica medicoPractica = new MedicoPractica();
            medicoPractica.MedicoId = medico.Id;
            medicoPractica.Practica = practica;
            Context.Session.Dalc.Update<MedicoPractica>(medicoPractica);
        }
    }
}

