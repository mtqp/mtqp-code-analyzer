using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using enfoke.AOP;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;
using System.Linq;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Eges.Persistance;
using System.Linq.Expressions;
using enfoke.Eges.Valorizacion;

namespace enfoke.Eges.Data
{
    /// <summary>
    /// Maneja los datos concernientes a las obras sociales
    /// </summary>
    public class ObrasSocialesDalc : Dalc, IService
    {
        protected ObrasSocialesDalc(NotConstructable dummy) : base(dummy) { }


        #region CategoriaObraSocial





        #endregion

        #region PlanPracticaPrecio

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanesAndDate(IList<int> planesIds, DateTime fechaVigencia)
        {
            Filter filter = new Filter();
            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "IN", planesIds);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.FechaDesde, "<=", fechaVigencia.Date);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(PlanPracticaPrecio.Properties.FechaHasta, "IS", null);
            filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.FechaHasta, ">", fechaVigencia.Date.AddDays(1));
            filter.Add(new CloseParenthesis());
            return dalEngine.GetManyByFilter<PlanPracticaPrecio>(filter);
        }

        public EntityCollection<PlanPracticaRequisito> PlanPracticaRequisitoReadByPlanesAndDate(IList<int> planesIds, DateTime fechaVigencia)
        {
            return PlanPracticaRequisitoVigenteReadByPlanPracticasAndDate(planesIds, null, fechaVigencia);
        }

        public PlanPracticaRequisito PlanPracticaRequisitoVigenteReadByPlanPractica(int planId, int practicaId)
        {
            EntityCollection<PlanPracticaRequisito> pprs = PlanPracticaRequisitoVigenteReadByPlanPracticasAndDate(new List<int>() { planId }, new List<int>() { practicaId }, enfoke.Time.Now);

            if (pprs.Count < 1)
                return new PlanPracticaRequisito();
            else
                return pprs[0];
        }

        public EntityCollection<PlanPracticaRequisito> PlanPracticaRequisitoVigenteReadByPlanPracticasAndDate(IList<int> planesIds, IList<int> practicasIds, DateTime fechaVigencia)
        {
            Filter filter = new Filter();
            if (practicasIds != null && practicasIds.Count > 0)
                filter.Add(PlanPracticaRequisito.Properties.Practica.Id, "IN", practicasIds);
            if (planesIds != null && planesIds.Count > 0)
                filter.Add(BooleanOp.And, PlanPracticaRequisito.Properties.Plan.Id, "IN", planesIds);

            filter.Add(BooleanOp.And, PlanPracticaRequisito.Properties.FechaDesde, "<=", fechaVigencia.Date);
            filter.Add(BooleanOp.And, PlanPracticaRequisito.Properties.Deleted, "=", false);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(PlanPracticaRequisito.Properties.FechaHasta, "IS", null);
            filter.Add(BooleanOp.Or, PlanPracticaRequisito.Properties.FechaHasta, ">=", fechaVigencia.Date);
            filter.Add(new CloseParenthesis());
            return dalEngine.GetManyByFilter<PlanPracticaRequisito>(filter);
        }


        public EntityCollection<PlanPracticaPrecioLight> PlanPracticaLightReadByIds(List<int> ids)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from PlanPracticaPrecioLight ppl ");
            hql.Append("where ppl.Deleted = false ");

            if (ids.Count > 0)
            {
                hql.Append(" and ppl.PlanId in (:ids)");
            }

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("ids", ids);

            return dalEngine.GetManyByQuery<PlanPracticaPrecioLight>(query);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByIds(List<int> ids)
        {
            SQLBlockBuilder<int> block = new SQLBlockBuilder<int>(ids);
            String hql = "from PlanPracticaPrecio plp " +
                         "where " + block.BuildConstrainBlock("plp.Id");
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaPrecioLight> PlanPracticaPrecioLightReadByPP(PlanPracticaPrecio pp)
        {
            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecioLight.Properties.PlanId, "=", pp.Plan.Id);
            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.PracticaId, "=", pp.Practica.Id);

            if (pp.PracticaAdicional != null)
                filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.PracticaAdicionalId, "=", pp.PracticaAdicional.Id);
            else
                filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.PracticaAdicionalId, "is", null);

            return dalEngine.GetManyByFilter<PlanPracticaPrecioLight>(filter);

        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaReadByPP(PlanPracticaPrecio pp)
        {
            return PlanPracticaPrecioReadByPlanPracticaAndEquipo(pp.Plan.Id, pp.Practica.Id, pp.PracticaAdicional != null ? pp.PracticaAdicional.Id : (int?)null, null);
        }

        private EntityCollection<PlanPracticaPrecio> PlanPracticaReadByPlanPracticaAndEquipo(int planId, int? practicaId, int? equipoId)
        {
            return PlanPracticaPrecioReadByPlanPracticaAndEquipo(planId, practicaId, null, equipoId);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioByPracticaAdicionalAndPlanId(int practicaAdicionalId, int planId, DateTime fecha)
        {
            if (fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;
            string hql = "FROM PlanPracticaPrecio pl " +
                         "WHERE pl.Plan.Id = :planId " +
                         "AND pl.PracticaAdicional.Adicional.Id  = :practicaAdicional " +
                         "AND pl.Deleted = false " +
                         "AND pl.FechaDesde <= :fecha " +
                         "AND (pl.FechaHasta >= :fecha " +
                         "OR pl.FechaHasta IS NULL) ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("planId", planId);
            query.SetParameter("practicaAdicional", practicaAdicionalId);
            query.SetParameter("fecha", fecha);

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaInsumo> PlanPracticaInsumoReadByPracticaAndFechaVigenciaDesde(int practicaId, DateTime vigenteDesde)
        {
            // me traigo cuando la practica esta convenida por si sola
            var query1 = from
                            ppi in dalEngine.Query<PlanPracticaInsumo>()
                         where
                             ppi.PlanPracticaPrecio.Practica.Id == practicaId && ppi.PlanPracticaPrecio.PracticaAdicional == null && ppi.PlanPracticaPrecio.Deleted == false &&
                             (ppi.PlanPracticaPrecio.FechaHasta == null || ppi.PlanPracticaPrecio.FechaHasta > vigenteDesde)
                         select
                             ppi;

            // me traigo cuando la practica esta convenida combinada con otra
            var query2 = from
                             ppi in dalEngine.Query<PlanPracticaInsumo>()
                         where
                             ppi.PlanPracticaPrecio.PracticaAdicional.PracticaID == practicaId && ppi.PlanPracticaPrecio.Deleted == false &&
                             (ppi.PlanPracticaPrecio.FechaHasta == null || ppi.PlanPracticaPrecio.FechaHasta > vigenteDesde)
                         select ppi;

            List<PlanPracticaInsumo> ppis = query1.ToList();
            ppis.AddRange(query2.ToList());
            return ppis.OrderBy(ppi => ppi.PlanPracticaPrecio.Id).ToEntityCollection();
        }

        public TipoCobertura TipoCoberturaGetCurrentByObraSocialPlanAndPracticaAndEquipo(int? plan, int? practica, int? adicional, int? equipo, DateTime? fecha, bool esModuloDetallado)
        {
            PlanPracticaPrecio planPractica =
                PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(
                plan, practica, adicional, equipo, fecha, esModuloDetallado);

            if (planPractica != null)
                return planPractica.TipoCobertura;

            return null;

        }

        public PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(int? plan, int? practica, int? adicional, int? equipo, DateTime? fecha, bool esModuloDetallado)
        {
            if (!fecha.HasValue || fecha.Value == DateTime.MaxValue || fecha.Value == DateTime.MinValue)
                fecha = enfoke.Time.Now;

            int _plan = plan.HasValue ? plan.Value : 0;
            int _practica = practica.HasValue ? practica.Value : 0;

            return this.PlanPracticaPrecioReadByPlanPracticaAndEquipo(_plan, _practica, adicional, fecha.Value, equipo, esModuloDetallado);
        }



        public PlanPracticaPrecio PlanPracticaPrecioReadByPlanPracticaAndEquipo(int planID, int practicaID, int? adicionalID, DateTime fecha, int? equipoId, bool esModuloDetallado)
        {
            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            fecha = fecha.Date;

            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.FechaDesde, "<=", fecha);
            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);
            filter.Add(PlanPracticaPrecio.Properties.FechaHasta, ">=", fecha);
            filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.FechaHasta, "IS", null);
            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Practica.Id, "=", practicaID);

            if (adicionalID.HasValue)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.PracticaAdicional.Id, "=", adicionalID.Value);
            else if (!esModuloDetallado)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.PracticaAdicional, "is", null);

            if (equipoId.HasValue)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(PlanPracticaPrecio.Properties.Equipo.Id, "=", equipoId.Value);
                filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.Equipo, "IS", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            readCmd.Filter = filter;

            EntityCollection<PlanPracticaPrecio> planPracticas = readCmd.Execute();

            return VigenciaDesdeUtils<PlanPracticaPrecio>.ObtenerVigente(planPracticas, fecha);
        }

        public EntityCollection<PlanPracticaPrecioLight> PlanPracticaPreciosReadCurrentByPlan(int planID)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            //if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
            //  fecha = enfoke.Time.Now.Date;

            ReadManyCommand<PlanPracticaPrecioLight> readCmd = new ReadManyCommand<PlanPracticaPrecioLight>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecioLight.Properties.PlanId, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.FechaDesde, "<=", fecha);
            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);
            filter.Add(PlanPracticaPrecioLight.Properties.FechaHasta, ">=", fecha);
            filter.Add(BooleanOp.Or, PlanPracticaPrecioLight.Properties.FechaHasta, "IS", null);
            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            readCmd.Sort = new Sort(new SortItem(PlanPracticaPrecioLight.Properties.PracticaId));
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<PlanPracticaPrecioLight> PlanPracticaPreciosReadByPlanAndDate(int planID, DateTime fecha)
        {

            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            ReadManyCommand<PlanPracticaPrecioLight> readCmd = new ReadManyCommand<PlanPracticaPrecioLight>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecioLight.Properties.PlanId, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.Deleted, "=", false);
            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);
            filter.Add(BooleanOp.And, PlanPracticaPrecioLight.Properties.FechaHasta, ">=", fecha);
            filter.Add(BooleanOp.Or, PlanPracticaPrecioLight.Properties.FechaHasta, "IS", null);
            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            readCmd.Sort = new Sort(new SortItem(PlanPracticaPrecioLight.Properties.PracticaId));
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        private EntityCollection<EntityCollection<PlanPracticaImpactoHerramientaCopia>> PlanPracticaImpactoHerramientaCopiaLimited(EntityCollection<PlanPracticaImpactoHerramientaCopia> referenciaImpacto)
        {
            EntityCollection<EntityCollection<PlanPracticaImpactoHerramientaCopia>> aux =
                new EntityCollection<EntityCollection<PlanPracticaImpactoHerramientaCopia>>();

            if (referenciaImpacto == null)
                referenciaImpacto = new EntityCollection<PlanPracticaImpactoHerramientaCopia>();

            if (referenciaImpacto.Count > 300)
            {
                EntityCollection<PlanPracticaImpactoHerramientaCopia> auxItem =
                    new EntityCollection<PlanPracticaImpactoHerramientaCopia>();

                foreach (PlanPracticaImpactoHerramientaCopia item in referenciaImpacto)
                {
                    auxItem.Add(item);

                    if (auxItem.Count == 300)
                    {
                        aux.Add(auxItem);
                        auxItem = new EntityCollection<PlanPracticaImpactoHerramientaCopia>();
                    }
                }

                if (auxItem.Count > 0)
                    aux.Add(auxItem);
            }
            else
            {
                aux.Add(referenciaImpacto);
            }

            return aux;
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioVigenteReadByPlanPracticaImpactoForHerramientaCopia(List<int> planesIds, EntityCollection<PlanPracticaImpactoHerramientaCopia> referenciaImpacto, bool? noConvenidas)
        {
            EntityCollection<PlanPracticaPrecio> planPracticasAux = new EntityCollection<PlanPracticaPrecio>();
            EntityCollection<EntityCollection<PlanPracticaImpactoHerramientaCopia>> ppIHC =
                PlanPracticaImpactoHerramientaCopiaLimited(referenciaImpacto);
            if (ppIHC != null)
            {
                foreach (EntityCollection<PlanPracticaImpactoHerramientaCopia> item in ppIHC)
                {
                    EntityCollection<PlanPracticaPrecio> planPracticas = PlanPracticaPrecioVigenteReadByPlanPracticaImpactoHerramientaCopiaBis(planesIds, item, noConvenidas);
                    if (planPracticas != null && planPracticas.Count > 0)
                        planPracticasAux.AddRange(planPracticas);
                }
            }

            return planPracticasAux;
        }

        private EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioVigenteReadByPlanPracticaImpactoHerramientaCopiaBis(List<int> planesIds, EntityCollection<PlanPracticaImpactoHerramientaCopia> referenciaImpacto, bool? noConvenidas)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            StringBuilder hql = new StringBuilder();

            hql.Append("select  plp ");
            hql.Append("from PlanPracticaPrecio plp ");
            hql.Append("left join plp.PracticaAdicional pad ");
            hql.Append("left join plp.Equipo equi ");
            hql.Append("where plp.Deleted = false ");

            if (noConvenidas.HasValue == true)
            {
                if (noConvenidas == true)
                    hql.Append("and plp.TipoCobertura.Id = :NoConvenida ");
                else
                    hql.Append("and plp.TipoCobertura.Id != :NoConvenida ");
            }

            hql.Append("and  plp.FechaDesde <= :fecha ");
            hql.Append("and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) and ( ");

            for (int i = 0; i < planesIds.Count; i++)
                hql.Append(" ( plp.Plan.Id =  ").Append(planesIds[i]).Append(" ) ").Append(planesIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(") and ( ");

            for (int i = 0; i < referenciaImpacto.Count; i++)
            {
                PlanPracticaImpactoHerramientaCopia refe = referenciaImpacto[i];
                hql.Append(" ( plp.Practica.Id =").Append(refe.PracticaId);

                if (refe.PracticaAdicionalId.HasValue == true)
                    hql.Append(" and pad.Adicional.Id = ").Append(refe.PracticaAdicionalId.Value);
                else
                    hql.Append(" and pad is null ");

                if (refe.EquipoId.HasValue == true)
                    hql.Append(" and equi.Id = ").Append(refe.EquipoId.Value);
                else
                    hql.Append(" and equi is null ");

                hql.Append(" ) ").Append(referenciaImpacto.Count - 1 != i ? " or " : String.Empty);
            }
            hql.Append(" ) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (noConvenidas.HasValue == true)
                query.SetInt32("NoConvenida", (int)TipoCoberturaEnum.NoConvenida);

            query.SetDateTime("fecha", fecha.Date);

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioVigenteReadByPlanIdsAndPracticasIdsAdicionalIdAndEquipo(List<int> planesIds, List<int> practicasIds, int? adicional, int? equipo, bool? noConvenidas)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            StringBuilder hql = new StringBuilder();
            hql.Append("from PlanPracticaPrecio plp ");
            hql.Append("where plp.Deleted = false ");

            if (noConvenidas.HasValue == true)
            {
                if (noConvenidas == true)
                    hql.Append("and plp.TipoCobertura.Id = :NoConvenida ");
                else
                    hql.Append("and plp.TipoCobertura.Id != :NoConvenida ");
            }

            hql.Append("and  plp.FechaDesde <= :fecha ");
            hql.Append("and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) and ( ");

            for (int i = 0; i < planesIds.Count; i++)
                hql.Append(" ( plp.Plan.Id =  ").Append(planesIds[i]).Append(" ) ").Append(planesIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(") and ( ");

            for (int i = 0; i < practicasIds.Count; i++)
                hql.Append(" ( plp.Practica.Id =  ").Append(practicasIds[i]).Append(" ) ").Append(practicasIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(") ");

            if (adicional.HasValue == true)
                hql.Append("and plp.PracticaAdicional.Id = ").Append(adicional.Value).Append(" ");
            else
                hql.Append("and plp.PracticaAdicional is null  ");

            if (equipo.HasValue == true)
                hql.Append("and plp.Equipo.Id = ").Append(equipo.Value).Append(" ");
            else
                hql.Append("and plp.Equipo is null ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (noConvenidas.HasValue == true)
                query.SetInt32("NoConvenida", (int)TipoCoberturaEnum.NoConvenida);

            query.SetDateTime("fecha", fecha.Date);

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioVigenteReadByPlanIdsAndPracticasIdsAndAdicionalId(List<int> planesIds, List<int> practicasIds, int? adicional, int? equipoId, bool? noConvenidas)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            StringBuilder hql = new StringBuilder();
            hql.Append("from PlanPracticaPrecio plp ");
            hql.Append("where plp.Deleted = false ");

            if (equipoId.HasValue)
                hql.Append("and plp.Equipo.Id = :equipo ");
            else
                hql.Append("and plp.Equipo is null ");

            if (noConvenidas.HasValue == true)
                if (noConvenidas == true)
                    hql.Append("and plp.TipoCobertura.Id = :NoConvenida ");
                else
                    hql.Append("and plp.TipoCobertura.Id != :NoConvenida ");

            hql.Append("and  plp.FechaDesde <= :fecha ");
            hql.Append("and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) and ( ");

            for (int i = 0; i < planesIds.Count; i++)
                hql.Append(" ( plp.Plan.Id =  ").Append(planesIds[i]).Append(" ) ").Append(planesIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(") and ( ");

            for (int i = 0; i < practicasIds.Count; i++)
                hql.Append(" ( plp.Practica.Id =  ").Append(practicasIds[i]).Append(" ) ").Append(practicasIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(" ) ");

            if (adicional.HasValue == true)
                hql.Append("and plp.PracticaAdicional.Id = ").Append(adicional.Value).Append(" ");
            else
                hql.Append("and plp.PracticaAdicional is null  ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (noConvenidas.HasValue == true)
                query.SetInt32("NoConvenida", (int)TipoCoberturaEnum.NoConvenida);

            if (equipoId.HasValue)
                query.SetInt32("equipo", equipoId.Value);

            query.SetDateTime("fecha", fecha.Date);
            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioVigenteReadByPlanIdsAndPracticasIdsAndAdicionalId(List<int> planesIds, List<KeyValuePair<int, int?>> practicasIds, bool? noConvenidas)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            StringBuilder hql = new StringBuilder();
            hql.Append("from PlanPracticaPrecio plp ");
            hql.Append("where plp.Deleted = false ");

            if (noConvenidas.HasValue == true)
                if (noConvenidas == true)
                    hql.Append("and plp.TipoCobertura.Id = :NoConvenida ");
                else
                    hql.Append("and plp.TipoCobertura.Id != :NoConvenida ");

            hql.Append("and  plp.FechaDesde <= :fecha ");
            hql.Append("and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) and ( ");

            for (int i = 0; i < planesIds.Count; i++)
                hql.Append(" ( plp.Plan.Id =  ").Append(planesIds[i]).Append(" ) ").Append(planesIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(") and ( ");

            for (int i = 0; i < practicasIds.Count; i++)
            {
                hql.Append(" ( plp.Practica.Id =  " + practicasIds[i].Key);

                if (practicasIds[i].Value.HasValue)
                {
                    hql.Append(" and plp.PracticaAdicional.Adicional.Id = " + practicasIds[i].Value);
                }
                else
                {
                    hql.Append(" and plp.PracticaAdicional is null ");
                }

                hql.Append(" ) ");

                if (i == practicasIds.Count - 1)
                {
                    hql.Append(" ) ");
                }
                else
                {
                    hql.Append(" or ");
                }
            }

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (noConvenidas.HasValue == true)
                query.SetInt32("NoConvenida", (int)TipoCoberturaEnum.NoConvenida);
            query.SetDateTime("fecha", fecha.Date);
            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaListaPrecioVw> PlanPracticaListaPrecioVwReadByParameters(DateTime? fechaReferencia, int? practicaAdicionalId, List<int> planId, List<int> listaPrecioIds, List<int> unidadArancelariaIds, List<int> insumoIds, List<int> documentacionRequeridaIds, List<int> tipoCoberturaIds, List<int> tipoCoseguroIds, int? equipoId, bool? exijaAutorizacion, bool? exijaConfirmacion, bool? exijaPresupuesto)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            if (fechaReferencia.HasValue != false)
                fecha = fechaReferencia.Value.Date;

            StringBuilder hql = new StringBuilder();
            hql.Append("select distinct pplp ");
            hql.Append("from PlanPracticaListaPrecioVw pplp ");

            if (documentacionRequeridaIds != null && documentacionRequeridaIds.Count > 0)
                hql.Append(", PlanPracticaDocumentacion ppd ");

            if (insumoIds != null && insumoIds.Count > 0)
                hql.Append(", PlanPracticaInsumo ppi ");

            hql.Append("where pplp.ObraSocialPlanId in (:planId) ");

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
                hql.Append("and pplp.ListaPrecioId in (:listaPrecioIds) ");

            if (tipoCoberturaIds != null && tipoCoberturaIds.Count > 0)
                hql.Append("and pplp.TipoCoberturaId in (:tipoCoberturaIds) ");

            if (tipoCoseguroIds != null && tipoCoseguroIds.Count > 0)
                hql.Append("and pplp.CoseguroId in (:tipoCoseguroIds) ");

            if (exijaPresupuesto.HasValue == true)
                hql.Append("and pplp.ExigePresupuesto = :exijaPresupuesto ");

            if (exijaConfirmacion.HasValue == true)
                hql.Append("and pplp.ExigeConfirmacion = :exijaConfirmacion ");

            if (exijaAutorizacion.HasValue == true)
                hql.Append("and pplp.ExigeAutorizacion = :exijaAutorizacion ");

            hql.Append("and  pplp.DeleteFlag = false ");
            hql.Append("and  pplp.FechaDesde <= :fecha ");
            hql.Append("and (pplp.FechaHasta >= :fecha or pplp.FechaHasta is null) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameterList("planId", planId);
            query.SetDateTime("fecha", fecha.Date);

            if (listaPrecioIds != null && listaPrecioIds.Count > 0)
                query.SetParameterList("listaPrecioIds", listaPrecioIds);

            if (insumoIds != null && insumoIds.Count > 0)
                query.SetParameterList("insumoIds", insumoIds);

            if (tipoCoberturaIds != null && tipoCoberturaIds.Count > 0)
                query.SetParameterList("tipoCoberturaIds", tipoCoberturaIds);

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

            return dalEngine.GetManyByQuery<PlanPracticaListaPrecioVw>(query);

        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByParameters(DateTime? fechaReferencia, int? equipoId, int? practicaAdicionalId, List<int> planId, List<int> listaPrecioIds, List<int> unidadArancelariaIds, List<int> insumoIds, List<int> documentacionRequeridaIds, List<int> tipoCoberturaIds, List<int> tipoCoseguroIds, bool? valorEquipo, bool? exijaAutorizacion, bool? exijaConfirmacion, bool? exijaPresupuesto)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            if (fechaReferencia.HasValue != false)
                fecha = fechaReferencia.Value.Date;

            StringBuilder hql = new StringBuilder();
            hql.Append("select pplp ");
            hql.Append("from PlanPracticaPrecio pplp ");

            if (documentacionRequeridaIds != null && documentacionRequeridaIds.Count > 0)
                hql.Append(", PlanPracticaDocumentacion ppd ");

            if (unidadArancelariaIds != null && unidadArancelariaIds.Count > 0)
            {

                hql.Append(",UnidadArancelariaPlan uap  ");
            }

            if (insumoIds != null && insumoIds.Count > 0)
                hql.Append(", PlanPracticaInsumo ppi ");

            hql.Append("where pplp.Plan.Id in (:planId) ");

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

            EntityCollection<PlanPracticaPrecio> planesPractica = dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
            EntityCollection<PlanPracticaPrecio> planesPracticaAux = new EntityCollection<PlanPracticaPrecio>();

            List<int> ids = new List<int>();
            if (planesPractica != null && planesPractica.Count > 0)
                foreach (PlanPracticaPrecio item in planesPractica)
                {
                    if (ids.Contains(item.Id) == false)
                    {
                        ids.Add(item.Id);
                        planesPracticaAux.Add(item);
                    }
                }

            return planesPracticaAux;

        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanIds(List<int> planIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from PlanPracticaPrecio plp ");
            hql.Append("where plp.Deleted = false ");
            hql.Append("and plp.Plan.Id IN (:planIds) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("planIds", planIds);

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        public EntityCollection<PlanPracticaListaPrecioVw> DatosPlanPracticaByPlanEquipoAndFecha(int planID, int? equipoId, DateTime fecha, bool orderSercicio, bool practicaCode)
        {
            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            String hql = "select distinct plp " +
            "from PlanPracticaListaPrecioVw plp " +
            "where plp.ObraSocialPlanId = :planID " +
            "and  plp.DeleteFlag = false " +
            "and  plp.FechaDesde <= :fecha " +
            "and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) ";

            if (!orderSercicio && !practicaCode)
                hql += "order by plp.PracticaDescripcion ,plp.ServicioName,plp.PracticaCode ";
            else if (orderSercicio)
                hql += "order by plp.ServicioName,plp.PracticaDescripcion ,plp.PracticaCode ";
            else
                hql += "order by plp.PracticaCode,plp.PracticaDescripcion,plp.ServicioName ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("planID", planID);
            query.SetParameter("fecha", fecha);

            return dalEngine.GetManyByQuery<PlanPracticaListaPrecioVw>(query);

        }

        public EntityCollection<PlanPracticaListaPrecioVw> DatosPlanPracticaByPlanEquipoAndFecha(bool? noConvenidas, int planID, int? equipoId, DateTime fecha, bool orderSercicio, bool practicaCode)
        {
            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            String hql = "select distinct plp " +
            "from PlanPracticaListaPrecioVw plp " +
            "where plp.ObraSocialPlanId = :planID " +
            "and  plp.DeleteFlag = false " +
            "and  plp.FechaDesde <= :fecha " +
            "and (plp.FechaHasta >= :fecha or plp.FechaHasta is null) ";

            if (noConvenidas.HasValue == true)
                if (noConvenidas == true)
                    hql += "and plp.TipoCoberturaId = :NoConvenida ";
                else
                    hql += "and plp.TipoCoberturaId != :NoConvenida ";

            if (!orderSercicio && !practicaCode)
                hql += "order by plp.PracticaDescripcion ,plp.ServicioName,plp.PracticaCode ";
            else if (orderSercicio)
                hql += "order by plp.ServicioName, plp.PracticaDescripcion ,plp.PracticaCode ";
            else
                hql += "order by plp.PracticaCode,plp.PracticaDescripcion,plp.ServicioName ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (noConvenidas.HasValue == true)
                query.SetParameter("NoConvenida", (int)TipoCoberturaEnum.NoConvenida);

            query.SetParameter("planID", planID);
            query.SetParameter("fecha", fecha);

            return dalEngine.GetManyByQuery<PlanPracticaListaPrecioVw>(query);

        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanEquipoAndFecha(int planID, int? equipoId, DateTime fecha)
        {
            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.FechaDesde, "<=", fecha);
            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);
            filter.Add(PlanPracticaPrecio.Properties.FechaHasta, ">=", fecha);
            filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.FechaHasta, "IS", null);
            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            if (equipoId.HasValue)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(PlanPracticaPrecio.Properties.Equipo.Id, "=", equipoId.Value);
                filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.Equipo, "IS", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            Sort sort = new Sort();
            sort.Add(PlanPracticaPrecio.Properties.Practica.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        [RequiresTransaction]
        public virtual void UnidadArancelariaPlanUpdateConExitente(UnidadArancelariaPlan nuevo, DateTime fechaVigencia, EntityCollection<UnidadArancelariaPlan> existentes)
        {
            EntityCollection<UnidadArancelariaPlan> all = VigenciaUtils<UnidadArancelariaPlan>.ObtenerModificaciones(existentes, nuevo, false, null).ToEntityCollection();
            dalEngine.UpdateCollection(all);
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanEquipoAndFecha(int planID, int? equipoId, DateTime fecha, bool ordenarPorServicio, bool ordenarPorCodigo)
        {
            if (fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.FechaDesde, "<=", fecha);
            OpenParenthesis openPar = new OpenParenthesis(BooleanOp.And);
            filter.Add(openPar);
            filter.Add(PlanPracticaPrecio.Properties.FechaHasta, ">=", fecha);
            filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.FechaHasta, "IS", null);
            CloseParenthesis closePar = new CloseParenthesis();
            filter.Add(closePar);

            if (equipoId.HasValue)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(PlanPracticaPrecio.Properties.Equipo.Id, "=", equipoId.Value);
                filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.Equipo, "IS", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            Sort sort = new Sort();

            sort.Add(PlanPracticaPrecio.Properties.Practica.Name, SortingDirection.Asc);
            sort.Add(PlanPracticaPrecio.Properties.Practica.ServicioEspecialidad.Servicio.Name, SortingDirection.Asc);
            sort.Add(PlanPracticaPrecio.Properties.Practica.Code, SortingDirection.Asc);

            //if (ordenarPorServicio)
            //{
            //    sort.Add(PlanPracticaPrecio.Properties.Practica.Servicio.Name, SortingDirection.Asc);
            //    sort.Add(PlanPracticaPrecio.Properties.Practica.Name, SortingDirection.Asc);
            //}
            //else if (ordenarPorCodigo)
            //{
            //    sort.Add(PlanPracticaPrecio.Properties.Practica.Code.Name, SortingDirection.Asc);
            //    sort.Add(PlanPracticaPrecio.Properties.Practica.Name, SortingDirection.Asc);
            //}
            //else
            //    sort.Add(PlanPracticaPrecio.Properties.Practica.Name, SortingDirection.Asc);

            readCmd.Sort = sort;
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        //********************//
        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanPracticaAndEquipo(int planID, int? practicaID, int? adicionalID, int? equipoId)
        {
            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            if (practicaID.HasValue)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Practica.Id, "=", practicaID.Value);

            if (adicionalID.HasValue)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.PracticaAdicional.Id, "=", adicionalID.Value);
            else
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.PracticaAdicional, "is", null);

            if (equipoId.HasValue)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(PlanPracticaPrecio.Properties.Equipo.Id, "=", equipoId.Value);
                filter.Add(BooleanOp.Or, PlanPracticaPrecio.Properties.Equipo, "IS", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanPracticasAdicionales(int planID, IList<int> practicaID)
        {
            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            if (practicaID != null)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.PracticaAdicional.Id, "IN", practicaID);

            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlanPractica(int planID, int practicaID)
        {
            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.Plan.Id, "=", planID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Practica.Id, "=", practicaID);
            filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            EntityCollection<PlanPracticaPrecio> planPracticas = readCmd.Execute();

            VigenciaUtils<PlanPracticaPrecio>.SetearVigentes(planPracticas, enfoke.Time.Now.Date);

            return planPracticas;
        }

        [AnonymousMethod()]
        public PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipoMandatory(int plan, int practica, int? equipo, DateTime? fecha)
        {
            return this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(plan, practica, equipo, false, fecha);
        }

        [AnonymousMethod()]
        public EntityCollection<PlanPracticaPrecio> PlanPracticasPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipoAndAdicionalMandatory(int plan, int practica, int? equipo, List<int> adicionales, DateTime fecha)
        {
            EntityCollection<PlanPracticaPrecio> response = new EntityCollection<PlanPracticaPrecio>();
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select plp from PlanPracticaPrecio plp ");
            hqlBuilder.Append("where plp.Deleted = false and ");
            hqlBuilder.Append("plp.Plan.Id = :plan and ");
            hqlBuilder.Append("plp.Practica.Id = :practica and ");
            hqlBuilder.Append("plp.FechaDesde <= :fecha and ");
            hqlBuilder.Append("(plp.FechaHasta >= :fecha or plp.FechaHasta is null) and ");
            hqlBuilder.Append("plp.PracticaAdicional is null ");
            if (equipo.HasValue)
                hqlBuilder.Append("and plp.Equipo.Id = :equipo ");
            else
                hqlBuilder.Append("and plp.Equipo is null ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("practica", practica);
            query.SetInt32("plan", plan);
            query.SetDateTime("fecha", fecha);
            if (equipo.HasValue)
                query.SetInt32("equipo", equipo.Value);

            response = dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
            if (adicionales != null && adicionales.Count > 0)
            {
                hqlBuilder.Clear();
                hqlBuilder.Append("select plp from PlanPracticaPrecio plp ");
                hqlBuilder.Append("where plp.Deleted = false and ");
                hqlBuilder.Append("plp.Plan.Id = :plan and ");
                hqlBuilder.Append("plp.PracticaAdicional.PracticaID = :practica and ");
                hqlBuilder.Append("plp.FechaDesde <= :fecha and ");
                hqlBuilder.Append("(plp.FechaHasta >= :fecha or plp.FechaHasta is null) ");
                hqlBuilder.Append("and plp.PracticaAdicional.Adicional.Id in (:adicionales) ");
                if (equipo.HasValue)
                    hqlBuilder.Append("and plp.Equipo.Id = :equipo ");
                else
                    hqlBuilder.Append("and plp.Equipo is null ");

                query = dalEngine.CreateQuery(hqlBuilder.ToString());
                query.SetInt32("practica", practica);
                query.SetInt32("plan", plan);
                query.SetDateTime("fecha", fecha);
                query.SetParameterList("adicionales", adicionales);
                if (equipo.HasValue)
                    query.SetInt32("equipo", equipo.Value);
                response.AddRange(dalEngine.GetManyByQuery<PlanPracticaPrecio>(query));
            }

            return response;
        }


        public PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPracticaAdicionalAndEquipoMandatory(int plan, int practica, int? equipo, int adicional, bool buscarTodos, DateTime fecha)
        {
            EntityCollection<PlanPracticaPrecio> PPs = PlanPracticaPrecioReadByPlanPracticaAndEquipo(plan, practica, adicional, equipo);

            PlanPracticaPrecio winner = VigenciaDesdeUtils<PlanPracticaPrecio>.ObtenerVigente(PPs, fecha);

            if (buscarTodos && (winner == null && equipo.HasValue))
                return this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(plan, practica, null, true, fecha);

            return winner;
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(int? plan, int?[] practicas, int? equipo, DateTime? fecha)
        {
            EntityCollection<PlanPracticaPrecio> col = new EntityCollection<PlanPracticaPrecio>();

            foreach (int? idPractica in practicas)
                col.Add(this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(plan, idPractica, equipo, fecha));

            return col;

        }

        [AnonymousMethod()]
        public PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(int? plan, int? practica, int? equipo, DateTime? fecha)
        {
            if (plan.GetValueOrDefault(0) == 0 || practica.GetValueOrDefault(0) == 0)
                return null;

            return this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(plan.Value, practica, equipo, true, fecha);
        }

        public PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaEquipoAndAdicional(int plan, int? practica, int? adicional, int? equipo, bool buscarTodos, DateTime? fecha)
        {
            if (!fecha.HasValue || fecha.Value == DateTime.MaxValue || fecha.Value == DateTime.MinValue)
                fecha = enfoke.Time.Now;

            PlanPracticaPrecio winner = PlanPracticaPrecioReadByPlanPracticaAndEquipo(plan, (practica.HasValue ? practica.Value : 0), adicional, fecha.Value, equipo, false);

            if (buscarTodos && (winner == null && equipo.HasValue))
                return this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaEquipoAndAdicional(plan, practica, adicional, null, true, fecha);

            return winner;
        }

        [Private]
        public EntityCollection<Equipo> PlanPracticaPrecioEspecialParaEquipo(int plan, int practica, int? adicional)
        {
            EntityCollection<PlanPracticaPrecio> PPs = PlanPracticaPrecioReadByPlanPracticaAndEquipo(plan, practica, adicional, null);

            EntityCollection<Equipo> equipos = new EntityCollection<Equipo>();
            foreach (PlanPracticaPrecio plp in PPs)
            {
                // Me quedo solo con los vigentes
                if (plp.FechaDesde > DateTime.Now || (plp.FechaHasta != null && plp.FechaHasta < DateTime.Now))
                    continue;

                // Puede ser un equipo como tambien NULL
                if (!equipos.Contains(plp.Equipo))
                    equipos.Add(plp.Equipo);
            }

            return equipos;
        }

        private PlanPracticaPrecio PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(int plan, int? practica, int? equipo, bool buscarTodos, DateTime? fecha)
        {
            if (!fecha.HasValue || fecha.Value == DateTime.MaxValue || fecha.Value == DateTime.MinValue)
                fecha = enfoke.Time.Now;

            EntityCollection<PlanPracticaPrecio> PPs = PlanPracticaPrecioReadByPlanPracticaAndEquipo(plan, practica, null, equipo);

            PlanPracticaPrecio winner = VigenciaDesdeUtils<PlanPracticaPrecio>.ObtenerVigente(PPs, fecha.Value);

            if (buscarTodos && (winner == null && equipo.HasValue))
                return this.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(plan, practica, null, true, fecha);

            return winner;
        }

        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByPlan(int planId)
        {
            return PlanPracticaReadByPlanPracticaAndEquipo(planId, null, null);
        }

        /// <summary>
        /// Devuelve todas las relaciones "plan / práctica" vigentes.
        /// </summary>
        /// <param name="fecha">Fecha a evaluar</param>
        /// <returns></returns>
        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByFecha(DateTime fecha)
        {
            return PlanPracticaPrecioReadByFecha(fecha, null);
        }

        /// <summary>
        /// Devuelve todas las relaciones "plan / práctica" vigentes.
        /// </summary>
        /// <param name="fecha">Fecha a evaluar</param>
        /// <param name="practica"></param>
        /// <returns></returns>
        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByFecha(DateTime fecha, Practica practica)
        {
            return PlanPracticaPrecioReadByFecha(fecha, practica, null);
        }

        /// <summary>
        /// Devuelve todas las relaciones "plan / práctica" vigentes.
        /// </summary>
        /// <param name="fecha">Fecha a evaluar</param>
        /// <param name="practica"></param>
        /// <param name="equipo"></param>
        /// <returns></returns>
        internal EntityCollection<PlanPracticaArbol> PlanPracticaPrecioArbolReadByFecha(int osId, DateTime fecha, bool mostrarSetsFarmacia)
        {
            DateTime T1 = DateTime.Now;

            EntityCollection<PlanPracticaArbol> ret = (from planPracticaPrecio in dalEngine.Query<PlanPracticaPrecio>()
                                                       join practica in dalEngine.Query<Practica>()
                                                          on planPracticaPrecio.Practica.Id equals practica.Id
                                                       join plan in dalEngine.Query<ObraSocialPlan>()
                                                          on planPracticaPrecio.Plan.Id equals plan.Id
                                                       join obraSocial in dalEngine.Query<ObraSocial>()
                                                          on plan.ObraSocial.Id equals obraSocial.Id
                                                       join tipoCobertura in dalEngine.Query<TipoCobertura>()
                                                          on planPracticaPrecio.TipoCobertura.Id equals tipoCobertura.Id
                                                       join tipoPractica in dalEngine.Query<TipoPractica>()
                                                          on practica.TipoPractica.Id equals tipoPractica.Id
                                                       where planPracticaPrecio.FechaDesde <= fecha &&
                                                             (planPracticaPrecio.FechaHasta == null || planPracticaPrecio.FechaHasta >= fecha)
                                                             && obraSocial.Id == osId
                                                             && (mostrarSetsFarmacia || tipoPractica.Id != (int)TipoPracticaEnum.SetFarmacia)
                                                             && (tipoCobertura.Id != (int)TipoCoberturaEnum.NoCubre)
                                                             && !planPracticaPrecio.Deleted
                                                             && plan.Activo
                                                             && !practica.Deleted
                                                       select new PlanPracticaArbol()
                                                       {
                                                           PracticaId = practica.Id,
                                                           PlanId = plan.Id,
                                                           EsNoConvenida = (tipoCobertura.Id == (int)TipoCoberturaEnum.NoConvenida)
                                                       }).ToEntityCollection<PlanPracticaArbol>();

            // Le anexa datos de especialidad y servicio (esto es más rápido en memoria que en la base)
            IDictionary<int, int> practicas = (from practica in dalEngine.Query<Practica>()
                                               select new Tuple<int, int>(practica.Id, practica.ServicioEspecialidad.Id)).ToDictionary(practica => practica.Item1, practica => practica.Item2);
            IDictionary<int, int> especialidades = (from servicioEspecialidad in dalEngine.Query<ServicioEspecialidad>()
                                                    select new Tuple<int, int>(servicioEspecialidad.Id, servicioEspecialidad.Servicio.Id)).ToDictionary(servicioEspecialidad => servicioEspecialidad.Item1, servicioEspecialidad => servicioEspecialidad.Item2);
            foreach (PlanPracticaArbol ppa in ret)
            {
                int esp = practicas[ppa.PracticaId];
                ppa.EspecialidadId = esp;
                ppa.ServicioId = especialidades[esp];
            }
            ret = (from ppa in ret orderby ppa.PlanId, ppa.ServicioId, ppa.EspecialidadId, ppa.PracticaId select ppa).ToEntityCollection<PlanPracticaArbol>();

            DateTime T5 = DateTime.Now;
            double i4 = (T5 - T1).TotalMilliseconds;
            return ret;
        }

        /// <summary>
        /// Devuelve todas las relaciones "plan / práctica" vigentes.
        /// </summary>
        /// <param name="fecha">Fecha a evaluar</param>
        /// <param name="practica"></param>
        /// <param name="equipo"></param>
        /// <returns></returns>
        public EntityCollection<PlanPracticaPrecio> PlanPracticaPrecioReadByFecha(DateTime fecha, Practica practica, Equipo equipo)
        {
            ReadManyCommand<PlanPracticaPrecio> readCmd = new ReadManyCommand<PlanPracticaPrecio>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PlanPracticaPrecio.Properties.FechaDesde, "<=", fecha);

            if (practica != null)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Practica.Id, "=", practica.Id);

            if (equipo != null)
                filter.Add(BooleanOp.And, PlanPracticaPrecio.Properties.Equipo.Id, "=", equipo.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public PlanPracticaPrecio PlanPracticaPrecioReadLastByPracticaAndPlan(int practicaId, int planId)
        {
            string hql = "from PlanPracticaPrecio plp " +
                         "where plp.Deleted  = false " +
                         "and plp.Practica.Id = :practicaId " +
                         "and plp.Plan.Id = :planId " +
                         "order by plp.Id desc";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaId", practicaId);
            query.SetParameter("planId", planId);
            EntityCollection<PlanPracticaPrecio> result = dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
            if (result != null && result.Count > 0)
                return result[0];
            else
                return null;
        }

        public void PlanPracticaPrecioUpdate(PlanPracticaPrecio pp)
        {
            PlanPracticaPrecioUpdateAndRefresh(pp);
        }
        [RequiresTransaction]
        public virtual PlanPracticaPrecio PlanPracticaUpdateAndRefresh(PlanPracticaPrecio pp, PlanPracticaRequisito requisitos)
        {
            // Graba requisitos
            if (requisitos != null)
            {
                requisitos.Plan = pp.Plan;
                requisitos.Practica = pp.Practica;
                Context.Session.RequisitosDalc.PlanPracticaRequisitoUpdate(requisitos);
            }
            // Graba precio
            return PlanPracticaPrecioUpdateAndRefresh(pp);
        }


        public PlanPracticaPrecio PlanPracticaPrecioUpdateAndRefresh(PlanPracticaPrecio pp)
        {
            return doPlanPracticaPrecioUpdate(pp, false, false, null, null, true);
        }

        private PlanPracticaRequisitoHC GenerarRequisitosPorConvenio(EntityCollection<PlanPracticaRequisitoHC> requisitosExistentesOFuturos, PlanPracticaRequisitoHC requisitoOriginal, PlanPracticaHC convenio, HCNorma<PlanPracticaDocumentacion> nuevaNorma)
        {
            if (nuevaNorma == null)
                return null;

            EntityCollection<PlanPracticaRequisitoHC> requisitos = new EntityCollection<PlanPracticaRequisitoHC>();
            if (requisitosExistentesOFuturos != null)
                requisitos = (from req in requisitosExistentesOFuturos where req.PracticaId == convenio.PracticaId && req.PlanId == convenio.PlanId select req).ToEntityCollection();

            PlanPracticaRequisitoHC nuevo = new PlanPracticaRequisitoHC();
            NormaBuilder<PlanPracticaDocumentacion> norma = new NormaBuilder<PlanPracticaDocumentacion>(nuevaNorma);
            nuevo.Norma = norma.ActualizarNorma(requisitoOriginal);
            nuevo.MezclarConConvenio(convenio);
            return Context.Session.RequisitosDalc.ActualizarRequisito(requisitos, nuevo);
        }

        private void GenerarDocumentacion(PlanPracticaRequisitoHC requisito, HCNorma<PlanPracticaDocumentacion> nuevaNorma)
        {
            if (nuevaNorma == null)
                return;

            EntityCollection<PlanPracticaDocumentacion> docPlan = nuevaNorma.ObtenerDocumentacion();
            EntityCollection<PlanPracticaDocumentacion> nuevos = new EntityCollection<PlanPracticaDocumentacion>();
            foreach (PlanPracticaDocumentacion documento in docPlan)
            {
                PlanPracticaDocumentacion nuevo = TypeUtils.Clone<PlanPracticaDocumentacion>(documento);
                nuevo.Id = 0;
                nuevo.PlanPracticaRequisitoId = requisito.Id;
                nuevos.Add(nuevo);
            }

            dalEngine.UpdateCollection(nuevos);
        }

        public virtual void PlanPracticasUpdateFinalizandoExistentes(EntityCollection<PlanPracticaHC> convenios, EntityCollection<PlanPracticaRequisitoHC> requisitosOriginales, EntityCollection<PlanPracticaRequisitoHC> requisitosVigentes, HCDatosValorizacion valorizacion, HCOtrosDatos otros, HCNorma<PlanPracticaDocumentacion> norma, EntityCollection<PlanPracticaHC> existentes)
        {
            PlanPracticasUpdateFinalizandoExistentes(convenios, requisitosOriginales, requisitosVigentes, valorizacion, otros, norma, existentes, false);
        }

        [Private]
        [RequiresTransaction]
        public virtual void PlanPracticasUpdateFinalizandoExistentes(EntityCollection<PlanPracticaHC> convenios, EntityCollection<PlanPracticaRequisitoHC> requisitosOriginales, EntityCollection<PlanPracticaRequisitoHC> requisitosVigentes, HCDatosValorizacion valorizacion, HCOtrosDatos otros, HCNorma<PlanPracticaDocumentacion> norma, EntityCollection<PlanPracticaHC> existentes, bool forzarUsoPrimerRequisito)
        {
            foreach (PlanPracticaHC convenio in convenios)
            {
                if ((valorizacion != null && valorizacion.ActualizaValorizacion) || (otros != null && otros.ActualizaOtros))
                    this.GenerarPlanPracticaPrecio(valorizacion, existentes, convenio);

                // las adicionales no llevan requisitos solo la cabecera
                if (norma != null && (norma.CopiarNorma || norma.CopiarDocumentacion) && !convenio.PracticaAdicionalId.HasValue)
                    this.GenerarNormaYDocumentacionPorConvenio(requisitosOriginales, requisitosVigentes, norma, convenio, forzarUsoPrimerRequisito);
            }
        }

        private void GenerarPlanPracticaPrecio(HCDatosValorizacion valorizacion, EntityCollection<PlanPracticaHC> existentes, PlanPracticaHC convenio)
        {
            EntityCollection<PlanPracticaHC> existenteAActualizar = new EntityCollection<PlanPracticaHC>();
            if (existentes != null)
                existenteAActualizar.AddRange(existentes.FindAll(delegate(PlanPracticaHC plan) { return plan.PracticaId == convenio.PracticaId && convenio.PracticaAdicionalId == plan.PracticaAdicionalId; }));

            EntityCollection<PlanPracticaHC> actualizados = VigenciaUtils<PlanPracticaHC>.ObtenerModificaciones(existenteAActualizar, convenio, false, null).ToEntityCollection();
            dalEngine.UpdateCollection(actualizados);
            this.CrearPlanPracticaInsumoNuevos(convenio, valorizacion);
            this.CrearCoberturaInsumos(convenio, valorizacion);
        }

        [RequiresTransaction]
        public virtual void FinalizarConvenios(EntityCollection<PlanPracticaHC> existentes, PlanPracticaHC convenio)
        {
            EntityCollection<PlanPracticaHC> existenteAActualizar = new EntityCollection<PlanPracticaHC>();
            if (existentes != null)
                existenteAActualizar.AddRange(existentes.FindAll(delegate(PlanPracticaHC plan) { return plan.PracticaId == convenio.PracticaId && convenio.PracticaAdicionalId == plan.PracticaAdicionalId; }));

            EntityCollection<PlanPracticaHC> actualizados = VigenciaUtils<PlanPracticaHC>.ActualizarExistentes(existenteAActualizar, null, convenio).ToEntityCollection();
            actualizados.Add(convenio);
            dalEngine.UpdateCollection(actualizados);
        }

        [RequiresTransaction]
        protected internal virtual void GenerarNormaYDocumentacionPorConvenio(EntityCollection<PlanPracticaRequisitoHC> requisitosOriginales, EntityCollection<PlanPracticaRequisitoHC> requisitosVigentes, HCNorma<PlanPracticaDocumentacion> norma, PlanPracticaHC convenio, bool forzarUsoPrimerRequisito)
        {
            if (norma.EstadoNorma == NormaYDocumentacionEnum.Mantener && norma.EstadoDocumentacion == NormaYDocumentacionEnum.Mantener)
                return;

            PlanPracticaRequisitoHC requisitoOriginal = ObtenerVigente(requisitosOriginales, convenio, forzarUsoPrimerRequisito);
            PlanPracticaRequisitoHC requisitoVigente = ObtenerVigente(requisitosVigentes, convenio, true);
            norma.DocumentosOriginales = new EntityCollection<PlanPracticaDocumentacion>();
            if (requisitoOriginal != null)
                norma.DocumentosOriginales = requisitoOriginal.Documentacion;
            if (requisitoVigente != null)
                norma.DocumentosExistentes = requisitoVigente.Documentacion;

            //PlanPracticaRequisitoHC requisito = (norma.EstadoNorma == NormaYDocumentacionEnum.Mantener) ? requisitoVigente : requisitoOriginal;
            //PlanPracticaRequisitoHC requisito = this.GenerarRequisitosPorConvenio(requisitosVigentes, pprVigente, convenio, norma);
            //if (requisito != null)
            //    this.GenerarDocumentacion(requisito, norma);
            PlanPracticaRequisitoHC requisito = this.GenerarRequisitosPorConvenio(requisitosVigentes, (norma.EstadoNorma == NormaYDocumentacionEnum.Mantener) ? requisitoVigente : requisitoOriginal, convenio, norma);
            if (requisito != null)
                this.GenerarDocumentacion(requisito, norma);
        }

        private static PlanPracticaRequisitoHC ObtenerVigente(EntityCollection<PlanPracticaRequisitoHC> requisitos, PlanPracticaHC convenio, bool forzarUsoPrimerRequisito)
        {
            if (requisitos == null)
                return null;
            if (forzarUsoPrimerRequisito)
                return (from req in requisitos where req.Deleted == false orderby req.FechaDesde ascending select req).FirstOrDefault();

            return (from req in requisitos where req.PracticaId == convenio.PracticaId && req.Deleted == false orderby req.FechaDesde ascending select req).FirstOrDefault();
        }

        private void CrearCoberturaInsumos(PlanPracticaHC planPractica, HCDatosValorizacion valorizacion)
        {
            if ((TipoPracticaEnum)planPractica.TipoPracticaId != TipoPracticaEnum.SetFarmacia)
                return;

            PlanPracticaCobInsumo cober = null;
            if (planPractica.Cobertura != null)
                cober = TypeUtils.Clone<PlanPracticaCobInsumo>(planPractica.Cobertura);

            if (cober == null)
                if (valorizacion == null)
                    return;

            if (valorizacion.EstadosBonificacionSet == ThreeStatesEnum.Reemplazar || cober == null)
                cober = new PlanPracticaCobInsumo(valorizacion);

            cober.Id = 0;
            cober.PlanPracticaId = planPractica.Id;
            Context.Session.Dalc.Update<PlanPracticaCobInsumo>(cober);
        }

        private void CrearPlanPracticaInsumoNuevos(PlanPracticaHC planPractica, HCDatosValorizacion valorizacion)
        {
            if ((TipoPracticaEnum)planPractica.TipoPracticaId != TipoPracticaEnum.SetFarmacia)
                return;


            if (planPractica.Insumos == null)
                return;

            EntityCollection<PlanPracticaInsumoHC> nuevos = new EntityCollection<PlanPracticaInsumoHC>();
            foreach (PracticaInsumo insumo in planPractica.Insumos)
            {
                if (valorizacion == null)
                    return;

                PlanPracticaInsumoHC nuevo = null;
                if (insumo.PlanPracticaInsumo != null)
                    nuevo = new PlanPracticaInsumoHC(insumo.PlanPracticaInsumo, valorizacion);
                else
                    nuevo = new PlanPracticaInsumoHC(insumo, valorizacion);

                nuevo.PlanPracticaId = planPractica.Id;
                nuevos.Add(nuevo);
            }

            dalEngine.UpdateCollection<PlanPracticaInsumoHC>(nuevos);
        }

        [RequiresTransaction]
        [Private]
        public virtual PlanPracticaPrecio PlanPracticaPrecioUpdateForHerramientaCopia(PlanPracticaPrecio pp, bool finalizar, EntityCollection<PlanPracticaPrecioLight> existentes)
        {
            return doPlanPracticaPrecioUpdate(pp, finalizar, false, null, existentes, false);
        }

        public PlanPracticaPrecio PlanPracticaPrecioFinalizar(PlanPracticaPrecio pp, DateTime fechaHasta, EntityCollection<TurnoUpdateHuerfano> huerfanos)
        {
            // Seteo la Fecha Hasta
            pp.FechaHasta = fechaHasta;

            // Actualizo
            return doPlanPracticaPrecioUpdate(pp, true, false, huerfanos, null, true);
        }

        public void PlanPracticaPrecioEliminar(PlanPracticaPrecio pp, EntityCollection<TurnoUpdateHuerfano> huerfanos)
        {
            // Marco como Eliminado
            pp.Deleted = true;

            // Actualizo
            doPlanPracticaPrecioUpdate(pp, false, true, huerfanos, null, false);
        }


        public virtual EntityCollection<PlanPracticaPrecio> ObternerPorEquipoDelPlanPracticaPrecio(EntityCollection<PlanPracticaPrecio> planPracticaPracticaPrecios, PlanPracticaPrecio planPracticaPrecio)
        {
            EntityCollection<PlanPracticaPrecio> ppsExistentesExcluyendoEquipos = new EntityCollection<PlanPracticaPrecio>();
            int equipoId = planPracticaPrecio.Equipo != null ? planPracticaPrecio.Equipo.Id : 0;
            if (planPracticaPracticaPrecios != null && planPracticaPracticaPrecios.Count > 0)
                foreach (PlanPracticaPrecio item in planPracticaPracticaPrecios)
                {
                    int equipoItem = item.Equipo != null ? item.Equipo.Id : 0;
                    if (equipoItem == equipoId)
                        ppsExistentesExcluyendoEquipos.Add(item);
                }

            return ppsExistentesExcluyendoEquipos;
        }

        public virtual EntityCollection<PlanPracticaPrecioLight> ObternerPorEquipoDelPlanPracticaPrecio(EntityCollection<PlanPracticaPrecioLight> planPracticaPracticaPrecios, PlanPracticaPrecio planPracticaPrecio)
        {
            EntityCollection<PlanPracticaPrecioLight> ppsExistentesExcluyendoEquipos = new EntityCollection<PlanPracticaPrecioLight>();
            int equipoId = planPracticaPrecio.Equipo != null ? planPracticaPrecio.Equipo.Id : 0;
            if (planPracticaPracticaPrecios != null && planPracticaPracticaPrecios.Count > 0)
                foreach (PlanPracticaPrecioLight item in planPracticaPracticaPrecios)
                {
                    int equipoItem = item.EquipoId.GetValueOrDefault(0);
                    if (equipoItem == equipoId)
                        ppsExistentesExcluyendoEquipos.Add(item);
                }

            return ppsExistentesExcluyendoEquipos;
        }

        /// <summary>
        /// Crea o modifica una un plan practica (convenio) y deja turnosIds huerfanos
        /// </summary>
        /// <param name="pp">El PP Modificado</param>
        /// <param name="finalizar">Marca si el registro esta Finalizado</param>
        /// <param name="eliminar">Marca si el registro esta Eliminado</param>
        /// <param name="huerfanos">Los Turnos a dejar Huerfanos</param>
        [RequiresTransaction]
        protected virtual PlanPracticaPrecio doPlanPracticaPrecioUpdate(PlanPracticaPrecio pp,
                  bool finalizar, bool eliminar,
                  EntityCollection<TurnoUpdateHuerfano> huerfanos,
                  EntityCollection<PlanPracticaPrecioLight> existentes,
                  bool returnRefreshValue)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            EntityCollection<IVigencia> PPs = new EntityCollection<IVigencia>();

            // Obtengo las modificaciones y las Agrego a la coleccion de modificados
            if (!eliminar)
            {
                if (existentes == null)
                    existentes = ObternerPorEquipoDelPlanPracticaPrecio(PlanPracticaPrecioLightReadByPP(pp), pp); ;

                IList<IVigencia> existentesRecast = new List<IVigencia>();
                foreach (PlanPracticaPrecioLight ppl in existentes)
                    existentesRecast.Add(ppl);

                PPs.AddRange(VigenciaUtils<IVigencia>.ObtenerModificaciones(existentesRecast, pp, finalizar, user));
            }
            else
            {
                // Audito
                Audit.AuditDelete(pp, user.Id);

                // Agrego a la coleccion de modificados
                PPs.Add(pp);
            }

            // Actualizo
            foreach (IVigencia item in PPs)
                dalEngine.Update(item);

            // Marco los turnosIds como Huerfanos
            if (huerfanos != null && huerfanos.Count > 0)
                Context.Session.TurnosDalc.TurnoUpdateMany(huerfanos);

            if (returnRefreshValue)
                return PlanPracticaPrecioReadLastByPracticaAndPlan(pp.Practica.Id, pp.Plan.Id);
            else
            {   // Trata de devolverlo de la sesión...
                IVigencia max = null;
                foreach (IVigencia v in PPs)
                    if (max == null || max.Id < v.Id)
                        max = v;

                if (max is PlanPracticaPrecio)
                    return max as PlanPracticaPrecio;
                else
                    return dalEngine.GetById<PlanPracticaPrecio>(max.Id);
            }
        }

        #endregion

        #region PlanPracticaPrecioView




        /// <summary>
        /// Devuelve el viw para todas las relaciones "plan / práctica" cubiertas por el plan indicado, para la prática indicada.
        /// </summary>
        /// <param name="planId">ObraSocialPlan utilizado</param>
        /// <returns>Las relaciones de "convenios con prácticas" encontradas</returns>
        public EntityCollection<PlanPracticaPrecioView> PlanPracticaPrecioViewReadByPlan(int planId)
        {
            return PlanPracticaPrecioViewReadByParameters(planId, null, null, null, null, false);
        }

        /// <summary>
        /// [CB] Devuelve el view para todas las relaciones "plan / práctica" cubiertas por el plan indicado, para el codigo/descripcion/servicioId indicado
        /// </summary>
        /// <param name="planId">Plan del cual se quieren traer las practicas</param>
        /// <param name="codigo">Código de la práctica (utiliza LIKE)</param>
        /// <param name="descripcion">Descripción de la práctica (utiliza LIKE)</param>
        /// <param name="servicioId">Servicio de la práctica (utiliza LIKE)</param>
        /// <returns>Relaciones de "convenios con prácticas" encontradas</returns>
        public EntityCollection<PlanPracticaPrecioView> PlanPracticaPrecioViewReadByParameters(int planId, string txtCodigo, string txtDescripcion, int? servicioId, int? servicioEspecialidadId, bool vigente)
        {
            string codigoPractica = txtCodigo.Trim();
            string descripcion = "%" + txtDescripcion.Trim().Replace(" ", "%") + "%";

            string hql = "from PlanPracticaPrecioView plp " +
             "where plp.Deleted = false " +
             "and plp.PlanId = :planId ";

            if (!String.IsNullOrEmpty(txtCodigo))
                hql += " and plp.PracticaCode = :codigoPractica ";

            if (!String.IsNullOrEmpty(txtDescripcion))
                hql += "and plp.Practica like :descripcion ";


            if (servicioId.HasValue)
                hql += " and plp.ServicioEspecialidad.Servicio.Id = " + servicioId.Value.ToString() + " ";

            if (servicioEspecialidadId.HasValue)
                hql += " and plp.ServicioEspecialidad.Id = " + servicioEspecialidadId.Value.ToString() + " ";



            if (vigente)
                hql += " and plp.Fecha = :fechaActual ";
            else
                hql += " and plp.Fecha = plp.FechaHastaFiltro ";

            hql += "order by plp.Practica desc";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planId", planId);

            if (!String.IsNullOrEmpty(txtCodigo))
                query.SetParameter("codigoPractica", codigoPractica);

            if (vigente)
                query.SetParameter("fechaActual", DateTime.Now.Date);

            if (!String.IsNullOrEmpty(txtDescripcion))
                query.SetParameter("descripcion", descripcion);

            EntityCollection<PlanPracticaPrecioView> result = dalEngine.GetManyByQuery<PlanPracticaPrecioView>(query);

            return result;
        }

        public EntityCollection<PlanPracticaPrecioView> PlanPracticaPrecioViewReadByDatosValorizacionMasiva(CondicionConsultaConvenioMasivaCollection condicionCollection)
        {
            EntityCollection<PlanPracticaPrecioView> retorno = new EntityCollection<PlanPracticaPrecioView>();
            foreach (CondicionConsultaConvenioMasiva condicion in condicionCollection.CondicionConsultaConvenioMasiva)
            {
                EntityCollection<PlanPracticaPrecioView> ret = PlanPracticaPrecioViewRead(condicion);
                if (ret != null)
                    retorno.AddRange(ret);
            }

            return retorno;
        }

        private EntityCollection<PlanPracticaPrecioView> PlanPracticaPrecioViewRead(CondicionConsultaConvenioMasiva condicion)
        {
            EntityCollection<PlanPracticaPrecioView> ret = new EntityCollection<PlanPracticaPrecioView>();
            SQLBlockBuilder<int> practIds = new SQLBlockBuilder<int>(condicion.PracticaIds);
            string practIdsAux = practIds.BuildConstrainBlock("plp.PracticaId");

            SQLBlockBuilder<int> planIds = new SQLBlockBuilder<int>(condicion.PlanIds);
            string planIdsAux = planIds.BuildConstrainBlock("plp.PlanId");

            StringBuilder hql = new StringBuilder();
            hql.Append(" select plp from PlanPracticaPrecioView plp ");
            hql.Append(" where plp.Fecha = :fecha ");
            hql.AppendFormat(" and  {0} ", practIdsAux);
            hql.AppendFormat(" and  {0} ", planIdsAux);

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fecha", condicion.Fecha);
            ret = dalEngine.GetManyByQuery<PlanPracticaPrecioView>(query);
            return ret;
        }

        #endregion

        [RequiresTransaction]
        public virtual HcAvances HcAvancesUpdate(HcAvances avances)
        {
            return dalEngine.Update<HcAvances>(avances);
        }

        public HcAvances HcAvancesReadById(int id)
        {
            return dalEngine.GetById<HcAvances>(id);
        }

        #region ObraSocial
        /// <summary>
        /// Devuelve todas las Obras Sociales con Descripcion o Código como el Envíado
        /// </summary>
        /// <returns>Obras Sociales con Descripcion o Código indicado</returns>
        public EntityCollection<ObraSocial> ObraSocialSearchByDescripcionAndCodigo(string name)
        {
            string search = name.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<ObraSocial> readCmd = new ReadManyCommand<ObraSocial>(dalEngine);

            Filter filter = new Filter();

            filter.Add(ObraSocial.Properties.Deleted, "=", false);

            OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
            filter.Add(open);

            filter.Add(ObraSocial.Properties.Name, "LIKE", search);
            filter.Add(BooleanOp.Or, ObraSocial.Properties.Code, "LIKE", search);

            CloseParenthesis close = new CloseParenthesis();
            filter.Add(close);

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(ObraSocial.Properties.Name);
            sort.Add(sortItem);

            readCmd.Sort = sort;
            readCmd.Filter = filter;

            EntityCollection<ObraSocial> OSs = readCmd.Execute();
            return OSs;
        }

        /// <summary>
        /// [RQ] Trae todas las obras sociales que exportan con el mismo formato de archivo
        /// </summary>
        /// <param name="id">El formato de exportación</param>
        /// <returns>Las obras sociales que exportan a dicho formato</returns>
        public EntityCollection<ObraSocial> ObraSocialReadByTipoExportacionId(int id)
        {
            EntityCollection<ObraSocial> OSs = dalEngine.GetManyByProperty<ObraSocial>(ObraSocial.Properties.TipoExportacionId, id);
            return OSs;
        }

        /// <summary>
        /// Trae todas las obras sociales que tienen la misma generenciadora
        /// </summary>
        /// <param name="gerenciadoraId">La gerenciadora</param>
        /// <returns>Las obras sociales que tiene esa gerenciadora</returns>
        public EntityCollection<ObraSocial> ObraSocialReadByGerenciadoraId(int gerenciadoraId)
        {
            return dalEngine.GetManyByProperty<ObraSocial>(ObraSocial.Properties.GerenciadoraID, gerenciadoraId);
        }











        /// <summary>
        /// Devuelve la obra social pedida
        /// </summary>
        /// <param name="obraSocialId">Id de la obra social a buscar</param>
        /// <returns>la obras social con el id indicado</returns>
        public EntityCollection<ObraSocial> ObrasSocialesReadByIds(IEnumerable<int> obrasSocialesIds)
        {
            Filter filter = new Filter();
            filter.Add(ObraSocial.Properties.Id, "IN", obrasSocialesIds);
            filter.Add(BooleanOp.And, ObraSocial.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<ObraSocial>(filter);
        }

        public EntityCollection<ObraSocialForList> ObraSocialForListReadAll()
        {
            //EntityCollection<ObraSocialForList> OSs = dalEngine.GetAll<ObraSocialForList>(ObraSocialForList.Properties.Name);
            //return new ReadAllCollection<ObraSocialForList>(OSs);

            ReadManyCommand<ObraSocialForList> readCmd = new ReadManyCommand<ObraSocialForList>(dalEngine);

            Filter filter = new Filter();
            filter.Add(ObraSocialForList.Properties.Deleted, "=", false);
            filter.Add(BooleanOp.And, ObraSocialForList.Properties.EstadoId, "=", (int)ObraSocialEstadoEnum.Activa);

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(ObraSocialForList.Properties.Name);
            sort.Add(sortItem);

            readCmd.Sort = sort;
            readCmd.Filter = filter;

            EntityCollection<ObraSocialForList> OSs = readCmd.Execute();
            return OSs;

        }

        public ObraSocialForList ObraSocialForListReadById(int osId)
        {
            ObraSocialForList OSs = dalEngine.GetByProperty<ObraSocialForList>(ObraSocialForList.Properties.Id, osId);
            return OSs;
        }

        public EntityCollection<ObraSocial> ObraSocialReadAll()
        {
            return dalEngine.GetAll<ObraSocial>(ObraSocial.Properties.Name);
        }

        /// <summary>
        /// Devuelve todas las obras sociales
        /// </summary>
        /// <returns>Obras Sociales</returns>
        public ReadAllCollection<ObraSocial> ObraSocialReadAll(ObraSocialEstadoEnum estado, FilterFlag gerenciadoras)
        {
            return ObraSocialReadAll(estado, gerenciadoras, FilterFlag.NoFilter);
        }

        /// <summary>
        /// Devuelve todas las obras sociales
        /// </summary>
        /// <returns>Obras Sociales</returns>
        public ReadAllCollection<ObraSocial> ObraSocialReadAll(ObraSocialEstadoEnum estado, FilterFlag gerenciadoras, FilterFlag deleted)
        {
            EntityCollection<ObraSocial> obras = this.ObraSocialReadAll();

            EntityCollection<ObraSocial> filteredDeleted = new EntityCollection<ObraSocial>();
            EntityCollection<ObraSocial> filtered = new EntityCollection<ObraSocial>();

            if (deleted == FilterFlag.NoFilter)
                filteredDeleted = obras;
            else
            {
                bool mustBeDeleted = (deleted == FilterFlag.FilterTrue);

                foreach (ObraSocial o in obras)
                    if (o.Deleted == mustBeDeleted)
                        filteredDeleted.Add(o);
            }

            // se filtran de acuerdo al criterio
            if (estado == ObraSocialEstadoEnum.Todos && gerenciadoras == FilterFlag.NoFilter)
                filtered = filteredDeleted;	// no se filtran por gerenciadoras
            else
            {
                bool mustBeGerenciadora = (gerenciadoras == FilterFlag.FilterTrue);

                foreach (ObraSocial o in filteredDeleted)
                    if ((estado == ObraSocialEstadoEnum.Todos || o.EstadoId == (int)estado) && (gerenciadoras == FilterFlag.NoFilter || o.EsGerenciadora == mustBeGerenciadora))
                        filtered.Add(o);
            }

            return new ReadAllCollection<ObraSocial>(filtered);
        }

        public ObraSocial ObraSocialReadByCodigo(string codigo)
        {
            ReadManyCommand<ObraSocial> readCmd = new ReadManyCommand<ObraSocial>(dalEngine);

            Filter filter = new Filter();
            filter.Add(ObraSocial.Properties.Code, "=", codigo);

            readCmd.Filter = filter;

            EntityCollection<ObraSocial> OSs = readCmd.Execute();

            if (OSs.Count == 0)
                return null;
            return OSs[0];
        }

        /// <summary>
        /// Devuelve todas las obras sociales con la descripción indicada.
        /// </summary>
        /// <returns>Obras Sociales con la descripción indicada</returns>
        public EntityCollection<ObraSocial> ObraSocialSearchByName(string name)
        {
            ReadManyCommand<ObraSocial> readCmd = new ReadManyCommand<ObraSocial>(dalEngine);

            Filter filter = new Filter();

            // se filtra el nombre
            if (!String.IsNullOrEmpty(name))
            {
                if (name.Trim().Contains(" "))
                {
                    string name1 = name.Split(' ')[0];
                    string name2 = name.Split(' ')[1];
                    filter.Add(ObraSocial.Properties.Name, "LIKE", name1 + "% " + name2 + "%");
                }
                else
                {
                    filter.Add(ObraSocial.Properties.Name, "LIKE", name + "%");
                }
            }

            // se filtra el código
            filter.Add(BooleanOp.Or, ObraSocial.Properties.Code, "LIKE", name.Trim().Replace(" ", "%") + "%");

            filter.Add(BooleanOp.And, ObraSocial.Properties.Deleted, "=", 0);

            filter.Add(BooleanOp.And, ObraSocial.Properties.EsGerenciadora, "=", 0);

            readCmd.Filter = filter;

            //ordeno
            Sort sort = new Sort();
            sort.Add(ObraSocial.Properties.Name, SortingDirection.Asc);
            readCmd.Sort = sort;

            EntityCollection<ObraSocial> OSs = readCmd.Execute();
            return OSs;
        }

        /// <summary>
        /// Devuelve todas las obras sociales con la descripción indicada, devolviendo también las hijas de las gerenciadoras que cumplan con el criterio.
        /// </summary>
        /// <returns>Obras sociales con la descripción indicada</returns>
        public EntityCollection<ObraSocial> ObraSocialSearchByNameForTurnoSelection(string name)
        {
            EntityCollection<ObraSocial> obrasSociales = new EntityCollection<ObraSocial>();

            string search = string.Empty;
            string searchTag = string.Empty;
            DateTime fecha = enfoke.Time.Now.Date;

            if (!String.IsNullOrEmpty(name))
            {
                if (name.Trim().Contains(" "))
                {
                    string name1 = name.Split(' ')[0];
                    string name2 = name.Split(' ')[1];
                    search = name1 + "% " + name2 + "%";
                    searchTag = "%" + name1 + "% " + name2 + "%";
                }
                else
                {
                    search = name + "%";
                    searchTag = "%" + name + "%";
                }
            }

            // Trae las que no son gerenciadoras, y cumplen el criterio
            IQuery query = dalEngine.CreateQuery("from ObraSocial os where "
                                + " os.EsGerenciadora = false AND os.EstadoId <> :inactiva "
                                 + " AND (os.Name like :name OR os.Tag like :nameTag)"
                                + " AND (os.FinVigencia is null OR os.FinVigencia > :fecha)"
                                + " AND (os.TipoConvenio = :tipoConvenioDirecto OR (os.TipoConvenio = :tipoConvenioMedico AND os.PermiteBusquedaReserva = true))");

            query.SetParameter("inactiva", (int)ObraSocialEstadoEnum.Inactiva);
            query.SetParameter("name", search);
            query.SetParameter("nameTag", searchTag);
            query.SetParameter("fecha", fecha);
            query.SetInt32("tipoConvenioDirecto", (int)ObraSocialTipoConvenioEnum.Directo);
            query.SetInt32("tipoConvenioMedico", (int)ObraSocialTipoConvenioEnum.PorMedico);
            obrasSociales.AddRange(dalEngine.GetManyByQuery<ObraSocial>(query));

            // Trae las hijas que no cumplen el criterio de las gerenciadoras que cumplen el criterio
            IQuery query2 = dalEngine.CreateQuery("select osg from ObraSocial os, ObraSocial osg where "
                                + " os.EsGerenciadora = true AND os.Id = osg.GerenciadoraID "
                                + " AND osg.EstadoId <> :inactiva "
                                + " AND (os.Name like :name OR os.Tag like :name)"
                                + " AND (osg.TipoConvenio = :tipoConvenioDirecto OR (osg.TipoConvenio = :tipoConvenioMedico AND osg.PermiteBusquedaReserva = true))"
                                + " AND NOT (osg.Name like :name)"
                                + " AND (os.FinVigencia is null OR os.FinVigencia > :fecha)");
            query2.SetParameter("inactiva", (int)ObraSocialEstadoEnum.Inactiva);
            query2.SetParameter("name", search);
            query2.SetInt32("tipoConvenioDirecto", (int)ObraSocialTipoConvenioEnum.Directo);
            query2.SetInt32("tipoConvenioMedico", (int)ObraSocialTipoConvenioEnum.PorMedico);
            query2.SetParameter("fecha", fecha);

            obrasSociales.AddRange(dalEngine.GetManyByQuery<ObraSocial>(query2));

            // Ordena
            obrasSociales.Sort(new Comparison<ObraSocial>(delegate(ObraSocial left, ObraSocial right)
            {
                return left.Name.CompareTo(right.Name);
            }));

            return obrasSociales;
        }

        public EntityCollection<ObraSocial> ObraSocialSearchByNameAndCode(string name, string code, string etiqueta, ObraSocialEstadoEnum estado)
        {
            ReadManyCommand<ObraSocial> readCmd = new ReadManyCommand<ObraSocial>(dalEngine);

            Filter filter = new Filter();

            // se filtra el nombre
            if (!String.IsNullOrEmpty(name))
                filter.Add(ObraSocial.Properties.Name, "LIKE", name.TrimEnd().Replace(" ", "% ") + "%");

            // se filtra el etiqueta
            if (!String.IsNullOrEmpty(etiqueta))
                filter.Add(ObraSocial.Properties.Tag, "LIKE", etiqueta.TrimEnd().Replace(" ", "% ") + "%");

            // se filtra el código
            if (!String.IsNullOrEmpty(code))
                filter.Add(BooleanOp.And, ObraSocial.Properties.Code, "LIKE", code.Trim().Replace(" ", "%") + "%");

            if (estado != ObraSocialEstadoEnum.Todos)
                filter.Add(BooleanOp.And, ObraSocial.Properties.EstadoId, "=", (int)estado);

            readCmd.Filter = filter;

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocial.Properties.Name, SortingDirection.Asc);

            EntityCollection<ObraSocial> OSs = readCmd.Execute();

            return OSs;
        }

        [RequiresTransaction]
        public virtual ObraSocial ObraSocialUpdate(ObraSocial oso)
        {
            oso = dalEngine.Update<ObraSocial>(oso);

            if (!string.IsNullOrEmpty(oso.Tag) && oso.Tag.Length > 100)
                throw new NotLoggeableException("La etiqueta no puede superar los 100 caracteres");

            // Si tiene tiposExportacion, actualiza...
            if (oso.TiposExportacion != null)
                ObraSocialTipoExportacionUpdateObraSocial(oso);
            // Graba los blobs...
            return LobUpdater.UpdateClob<ObraSocial>(oso,
                            ObraSocial.Properties.Norma,
                            ObraSocial.Properties.NormaCorta,
                            ObraSocial.Properties.Observaciones);
        }

        [RequiresTransaction]
        protected virtual void ObraSocialTipoExportacionUpdateObraSocial(ObraSocial oso)
        {
            // Trae los preexistentes y los borra...
            EntityCollection<ObraSocialTipoExportacion> preexistentes = ObraSocialTipoExportacionReadByObraSocial(oso.Id);
            if (preexistentes.Count > 0)
                dalEngine.Delete(preexistentes);
            EntityCollection<ObraSocialTipoExportacion> actuales = new EntityCollection<ObraSocialTipoExportacion>();
            foreach (TipoExportacion te in oso.TiposExportacion)
            {
                ObraSocialTipoExportacion osTe = new ObraSocialTipoExportacion();
                osTe.ObraSocialId = oso.Id;
                osTe.TipoExportacion = te;
                actuales.Add(osTe);
            }
            dalEngine.UpdateCollection(actuales);
        }

        #endregion

        public TipoPlan TipoPlanReadById(int tipoPlanId)
        {
            return dalEngine.GetById<TipoPlan>(tipoPlanId);
        }

        public EntityCollection<ObraSocialTipoConvenio> ObraSocialTipoConvenioReadAll()
        {
            return dalEngine.GetAll<ObraSocialTipoConvenio>(ObraSocialTipoConvenio.Properties.Tipo);
        }

        public EntityCollection<ObraSocialPlanRelacionado> ObraSocialPlanRelacionadoGetByObraSocialPlanAsociacion(int ospId)
        {
            return dalEngine.GetManyByProperty<ObraSocialPlanRelacionado>(ObraSocialPlanRelacionado.Properties.ObraSocialPlanAsociado.Id, ospId);
        }

        public EntityCollection<ObraSocialPlanRelacionado> ObraSocialPlanRelacionadoGetByObraSocialPlan(int ospId)
        {
            return dalEngine.GetManyByProperty<ObraSocialPlanRelacionado>(ObraSocialPlanRelacionado.Properties.ObraSocialPlan.Id, ospId);
        }

        public EntityCollection<ObraSocialPlanRelacionado> ObraSocialPlanRelacionadoPorMedicoGetByObraSocialPlanRelacionadoAndMedico(int ospId, int medicoId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select ospr from ObraSocialPlanRelacionado ospr where ospr.ObraSocialPlanAsociado.Id = :ospId ");
            hqlBuilder.Append("AND ospr.ObraSocialPlan.ObraSocial.TipoConvenio = :convenioMedico ");
            hqlBuilder.Append("AND ospr.ObraSocialPlan.ObraSocial.MedicoConvenio.Id = :medicoId ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("ospId", ospId);
            query.SetInt32("convenioMedico", (int)ObraSocialTipoConvenioEnum.PorMedico);
            query.SetInt32("medicoId", medicoId);

            return dalEngine.GetManyByQuery<ObraSocialPlanRelacionado>(query);
        }

        public EntityCollection<ObraSocialTipoExportacion> ObraSocialTipoExportacionReadByObraSocial(int obraSocialId)
        {
            return dalEngine.GetManyByProperty<ObraSocialTipoExportacion>(ObraSocialTipoExportacion.Properties.ObraSocialId,
                                    obraSocialId, ObraSocialTipoExportacion.Properties.TipoExportacion.Nombre);
        }

        public EntityCollection<ObraSocialTipoExportacion> ObraSocialTipoExportacionReadByTipoExportacion(int tipoExportacionId)
        {
            return dalEngine.GetManyByProperty<ObraSocialTipoExportacion>(ObraSocialTipoExportacion.Properties.TipoExportacion.Id,
                                    tipoExportacionId, ObraSocialTipoExportacion.Properties.TipoExportacion.Id);
        }

        #region ObraSocialLight





        public ReadAllCollection<ObraSocialName> ObraSocialNameReadAll()
        {
            return new ReadAllCollection<ObraSocialName>(dalEngine.GetManyByProperty<ObraSocialName>(ObraSocialName.Properties.Deleted, false, ObraSocialName.Properties.Name));
        }

        #endregion

        #region ObraSocialNivel
        /// <summary>
        /// Devuelve todos los niveles de Obra Social
        /// </summary>
        /// <returns>Todos los tipos de cobertura</returns>
        public EntityCollection<ObraSocialNivel> ObraSocialNivelReadAll()
        {
            return dalEngine.GetAll<ObraSocialNivel>(ObraSocialNivel.Properties.Descripcion);
        }

        /// <summary>
        /// Devuelve el nivel de obra social con un nombre determinado
        /// </summary>
        /// <param name="name">El nombre de del nivel de obra social</param>
        /// <returns>El nivel de obra social con dicho nombre</returns>
        public ObraSocialNivel ObraSocialNivelRead(string name)
        {
            return dalEngine.GetByProperty<ObraSocialNivel>(ObraSocialNivel.Properties.Descripcion, name);
        }










        #endregion

        #region ObraSocialPlan

        public EntityCollection<ObraSocialPlanForList> ObraSocialPlanForListReadAll()
        {
            ReadManyCommand<ObraSocialPlanForList> readCmd = new ReadManyCommand<ObraSocialPlanForList>(dalEngine);

            Filter filter = new Filter();

            // se filtra el id
            filter.Add(ObraSocialPlanForList.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, ObraSocialPlanForList.Properties.Deleted, "=", 0);

            readCmd.Filter = filter;

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocialPlanForList.Properties.Name, SortingDirection.Asc);

            return readCmd.Execute();
        }

        public EntityCollection<ObraSocialPlan> ObraSocialPlanReadAll()
        {
            return dalEngine.GetManyByProperty<ObraSocialPlan>(ObraSocialPlan.Properties.Deleted, 0);
        }

        public EntityCollection<ObraSocialPlan> ObraSocialPlanReadByObraSocial(int obraSocialId)
        {
            return this.ObraSocialPlanReadByObraSocial(obraSocialId, true);
        }

        public EntityCollection<ObraSocialPlanForList> ObraSocialPlanForListReadByObraSocial(int obraSocialId)
        {
            ReadManyCommand<ObraSocialPlanForList> readCmd = new ReadManyCommand<ObraSocialPlanForList>(dalEngine);

            Filter filter = new Filter();

            // se filtra el id
            filter.Add(ObraSocialPlanForList.Properties.ObraSocialId, "=", obraSocialId);

            filter.Add(BooleanOp.And, ObraSocialPlanForList.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, ObraSocialPlanForList.Properties.Deleted, "=", 0);

            readCmd.Filter = filter;

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocialPlanForList.Properties.Name, SortingDirection.Asc);

            return readCmd.Execute();
        }

        public EntityCollection<ObraSocialPlanForList> ObraSocialPlanAllForListReadByObraSocial(int obraSocialId)
        {
            ReadManyCommand<ObraSocialPlanForList> readCmd = new ReadManyCommand<ObraSocialPlanForList>(dalEngine);
            Filter filter = new Filter();

            // se filtra el id
            filter.Add(ObraSocialPlanForList.Properties.ObraSocialId, "=", obraSocialId);
            filter.Add(BooleanOp.And, ObraSocialPlanForList.Properties.Deleted, "=", 0);
            readCmd.Filter = filter;
            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocialPlanForList.Properties.Name, SortingDirection.Asc);
            return readCmd.Execute();
        }

        public EntityCollection<ObraSocialPlan> ObraSocialPlanReadByObraSocial(int obraSocialId, bool soloActivos)
        {
            ReadManyCommand<ObraSocialPlan> readCmd = new ReadManyCommand<ObraSocialPlan>(dalEngine);

            Filter filter = new Filter();

            // se filtra el id
            filter.Add(ObraSocialPlan.Properties.ObraSocial.Id, "=", obraSocialId);

            if (soloActivos)
                filter.Add(BooleanOp.And, ObraSocialPlan.Properties.Activo, "=", true);

            filter.Add(BooleanOp.And, ObraSocialPlan.Properties.Deleted, "=", 0);

            readCmd.Filter = filter;

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocialPlan.Properties.Name, SortingDirection.Asc);

            return readCmd.Execute();
        }

        public EntityCollection<ObraSocialPlanLight> ObraSocialPlanLightReadByObraSocial(int obraSocialId)
        {
            ReadManyCommand<ObraSocialPlanLight> readCmd = new ReadManyCommand<ObraSocialPlanLight>(dalEngine);

            Filter filter = new Filter();

            // se filtra el id
            filter.Add(ObraSocialPlanLight.Properties.ObraSocialId, "=", obraSocialId);

            filter.Add(BooleanOp.And, ObraSocialPlanLight.Properties.Deleted, "=", 0);

            readCmd.Filter = filter;

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(ObraSocialPlanLight.Properties.Name, SortingDirection.Asc);

            return readCmd.Execute();
        }






        public ObraSocialPlanReserva ObraSocialPlanReservaReadById(int obraSocialPlanId)
        {
            return dalEngine.GetById<ObraSocialPlanReserva>(obraSocialPlanId);
        }






        [Private]
        public int ObraSocialVencimientoOrden(int ospID)
        {
            string hql = "SELECT osp.ObraSocial.VencimientoOrden FROM ObraSocialPlan osp " +
               "WHERE osp.Id = :idObraSocialPlan ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idObraSocialPlan", ospID);
            query.SetMaxResults(1);

            object ret = query.UniqueResult();

            if (ret != null)
                return int.Parse(ret.ToString());

            throw new Exception("Se ha suminstrado un obra social plan inválido.");
        }

        public ObraSocialLight ObraSocialLightReadByObraSocialPlanId(int idOSP)
        {
            string hql = "SELECT os FROM ObraSocialLight os, ObraSocialPlan osp " +
                         "WHERE os.Id = osp.ObraSocial.Id " +
                         "AND osp.Id = :idObraSocialPlan ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idObraSocialPlan", idOSP);
            query.SetMaxResults(1);

            return dalEngine.GetByQuery<ObraSocialLight>(query);
        }






        public int LapsoDiasFacturacionReadByObraSocialPlanId(int idOSP)
        {
            string hql = "SELECT osp.ObraSocial.LapsoDiasFacturacion FROM ObraSocialPlan osp " +
               "WHERE osp.Id = :idObraSocialPlan ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idObraSocialPlan", idOSP);
            query.SetMaxResults(1);

            object ret = query.UniqueResult();

            if (ret != null)
                return int.Parse(ret.ToString());

            throw new Exception("La parametrización respecto al lapso de días para facturar es incorrecta.");
        }

        public bool ObraSocialEsParticularReadByTurnoId(int idTurno)
        {
            string hql = "SELECT t.Orden.ObraSocialPlan.ObraSocial.EsParticular FROM TurnoHQL t " +
               "WHERE t.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);
            query.SetMaxResults(1);

            object ret = query.UniqueResult();

            if (ret != null)
                return bool.Parse(ret.ToString());

            throw new Exception("La parametrización respecto a la obra social es incorrecta.");
        }

        internal bool ObraSocialEsParticularReadByObraSocialPlanId(int obraSocialPlanID)
        {
            string hql = "SELECT osp.ObraSocial.EsParticular FROM ObraSocialPlan osp " +
               "WHERE osp.Id = :idObraSocialPlan ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idObraSocialPlan", obraSocialPlanID);
            query.SetMaxResults(1);

            object ret = query.UniqueResult();

            if (ret != null)
                return bool.Parse(ret.ToString());

            throw new Exception("La parametrización respecto a la obra social es incorrecta.");
        }


        [RequiresTransaction]
        public virtual ObraSocialPlan ObraSocialPlanUpdate(ObraSocialPlan osp, EntityCollection<TipoPlan> tiposPlan,
                        ObraSocialPlanRequisito requisitos)
        {
            osp = dalEngine.Update(osp);

            if (osp.ConveniosRelacionados == null)
                osp.ConveniosRelacionados = new EntityCollection<ObraSocialPlanRelacionado>();

            //Agrego los tipos de plan a la obra social plan
            this.ObraSocialPlanTipoUpdate(tiposPlan, osp.Id);
            this.RemoverRelacionesViejas(osp);
            PonerRelacionesComoNuevas(osp);
            this.CrearRelacionesNuevas(osp);
            if (requisitos != null)
                requisitos.Plan = osp;
            Context.Session.RequisitosDalc.ObraSocialPlanRequisitoUpdate(requisitos);

            return osp;
        }

        private void CrearRelacionesNuevas(ObraSocialPlan osp)
        {

            EntityCollection<ObraSocialPlanRelacionado> relacionesNuevas = new EntityCollection<ObraSocialPlanRelacionado>(osp.ConveniosRelacionados);
            dalEngine.UpdateCollection(relacionesNuevas);
        }

        private static void PonerRelacionesComoNuevas(ObraSocialPlan osp)
        {
            foreach (ObraSocialPlanRelacionado relacion in osp.ConveniosRelacionados)
                relacion.Id = 0;
        }

        private void RemoverRelacionesViejas(ObraSocialPlan osp)
        {
            EntityCollection<ObraSocialPlanRelacionado> relacionesViejas = ObraSocialPlanRelacionadoGetByObraSocialPlan(osp.Id);
            dalEngine.Delete(relacionesViejas);
        }

        /// <summary>
        /// Actualiza ObraSocialPlan.
        /// </summary>
        /// <param name="osp">La ObraSocialPlan</param>
        public void ObraSocialPlanUpdate(EntityCollection<ObraSocialPlan> osp)
        {
            foreach (ObraSocialPlan o in osp)
                dalEngine.Update(o);
        }

        /// <summary>
        /// Borra ObraSocialPlan.
        /// </summary>
        /// <param name="osp">La ObraSocialPlan</param>
        public void ObraSocialPlanDelete(ObraSocialPlan osp)
        {
            osp.Deleted = true;
            dalEngine.Update(osp);
        }

        public void ObraSocialPlanActivate(ObraSocialPlan osp, bool activo)
        {
            osp.Activo = activo;
            dalEngine.Update(osp);
        }

        public EntityCollection<ObraSocialPlanLight> PlanLightReadByIds(IList<int> ids)
        {
            return dalEngine.GetManyByIds<ObraSocialPlanLight>(ids);
        }

        /// <summary>
        /// Borra ObraSocialPlan.
        /// </summary>
        /// <param name="osp">La ObraSocialPlan</param>
        public void ObraSocialPlanDelete(EntityCollection<ObraSocialPlan> osp)
        {
            foreach (ObraSocialPlan o in osp)
                ObraSocialPlanDelete(o);
        }

        /// <summary>
        /// Devuelve el plan indicado
        /// </summary>
        /// <param name="planId">Obra social</param>
        /// <returns>ObraSocialPlan con el id indicado</returns>
        [AnonymousMethod()]
        public ObraSocialPlan ObraSocialPlanReadById(int planId)
        {
            return dalEngine.GetById<ObraSocialPlan>(planId);
        }

        [Private]
        public EntityCollection<ObraSocialPlan> ObraSocialPlanReadByIds(IEnumerable<int> ids)
        {
            Filter filter = new Filter();
            filter.Add(ObraSocialPlan.Properties.Id, "IN", ids);
            return dalEngine.GetManyByFilter<ObraSocialPlan>(filter);
        }

        /// <summary>
        /// Chequeo si el codigo en cuestion ya existe en otro plan de la OS
        /// </summary>
        /// <param name="osp">Plan con los datos</param>
        /// <returns>True/False si ya existe el codigo</returns>
        public bool ObraSocialPlanExisteCodigo(int osID, string code, int ospID)
        {
            ReadManyCommand<ObraSocialPlan> readCmd = new ReadManyCommand<ObraSocialPlan>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(ObraSocialPlan.Properties.ObraSocial.Id, "=", osID);

            readCmd.Filter.Add(BooleanOp.And, ObraSocialPlan.Properties.Code, "=", code);

            readCmd.Filter.Add(BooleanOp.And, ObraSocialPlan.Properties.Deleted, "=", false);

            readCmd.Filter.Add(BooleanOp.And, ObraSocialPlan.Properties.Id, "!=", ospID);

            return readCmd.Execute().Count > 0;
        }
        #endregion

        #region ObraSocialEstado

















        #endregion

        #region UnidadArancelariaCategoria
        /// <summary>
        /// Devuelve los valores de ua por categoría de una unidad arancelaria
        /// </summary>
        /// <param name="id">El id del unidad arancelaria</param>
        /// <returns>Los valores de unidades arancelarias por categoría</returns>
        public EntityCollection<UnidadArancelariaCategoria> UnidadArancelariaCategoriaReadByUAPlan(int uaPlanId)
        {
            return dalEngine.GetManyByProperty<UnidadArancelariaCategoria>(UnidadArancelariaCategoria.Properties.UnidadArancelariaPlan.Id, uaPlanId);
        }

        public EntityCollection<UnidadArancelariaCategoria> UnidadArancelariaCategoriaReadByUAPlanes(List<int> uaPlanIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from UnidadArancelariaCategoria uac ");
            hql.Append("where ( ");

            for (int i = 0; i < uaPlanIds.Count; i++)
                hql.Append(" ( uac.UnidadArancelariaPlan.Id =  ").Append(uaPlanIds[i]).Append(" ) ").Append(uaPlanIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(" ) ");

            if (uaPlanIds != null && uaPlanIds.Count > 0)
            {
                IQuery query = dalEngine.CreateQuery(hql.ToString());
                return dalEngine.GetManyByQuery<UnidadArancelariaCategoria>(query);
            }
            return new EntityCollection<UnidadArancelariaCategoria>();
        }

        public UnidadArancelariaCategoria UnidadArancelariaCategoriaReadByUAPlanAndCategoriaMedico(UnidadArancelariaPlan uaPlan, CategoriaMedico categoriaMedico)
        {
            if (uaPlan == null || categoriaMedico == null)
                return null;

            ReadManyCommand<UnidadArancelariaCategoria> readCmd = new ReadManyCommand<UnidadArancelariaCategoria>(dalEngine);

            Filter filter = new Filter();

            filter.Add(UnidadArancelariaCategoria.Properties.CategoriaMedico, "=", categoriaMedico.Id);

            filter.Add(BooleanOp.And, UnidadArancelariaCategoria.Properties.UnidadArancelariaPlan, "=", uaPlan.Id);

            readCmd.Filter = filter;

            EntityCollection<UnidadArancelariaCategoria> col = readCmd.Execute();

            if (col.Count > 0)
                return col[0];
            else
                return null;
        }

        public void UnidadArancelariaCategoriaUpdate(UnidadArancelariaCategoria uac)
        {
            dalEngine.Update<UnidadArancelariaCategoria>(uac);
        }

        [RequiresTransaction]
        [Private]
        public virtual void UnidadArancelariaCategoriaUpdateManyForHerramientaCopia(EntityCollection<UnidadArancelariaCategoria> uacs)
        {
            dalEngine.UpdateCollection(uacs);
        }

        #endregion

        #region UnidadArancelariaPlan

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanGetCurrentByOSPlan(int osPlanID, DateTime fecha)
        {
            if (fecha == null || fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now.Date;

            string hql = "from UnidadArancelariaPlan uap " +
                         "where uap.Deleted = false " +
                         "and  uap.ObraSocialPlan.Id = :osPlanID " +
                         "and  uap.FechaDesde <= :fecha " +
                         "and  (:fecha <= uap.FechaHasta or uap.FechaHasta is null) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetDateTime("fecha", fecha.Date);
            query.SetInt32("osPlanID", osPlanID);
            return dalEngine.GetManyByQuery<UnidadArancelariaPlan>(query);
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanGetActualOFuturasByOSPlan(int osPlanID, List<int> unidadesArancelarias, DateTime fecha)
        {
            if (unidadesArancelarias == null || unidadesArancelarias.Count == 0)
                return new EntityCollection<UnidadArancelariaPlan>();

            return (from uap in dalEngine.Query<UnidadArancelariaPlan>()
                    where unidadesArancelarias.Contains(uap.UnidadArancelaria.Id) &&
                    uap.ObraSocialPlan.Id == osPlanID &&
                    uap.FechaDesde <= fecha.Date &&
                    (uap.FechaHasta == null || uap.FechaHasta >= fecha.Date) &&
                    uap.Deleted == false
                    select uap).ToEntityCollection();
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanWithObjectGetCurrentByOSPlan(int osPlanID, DateTime fecha)
        {
            EntityCollection<UnidadArancelariaPlan> unidadArancelariaPlan =
                UnidadArancelariaPlanGetCurrentByOSPlan(osPlanID, fecha);

            if (unidadArancelariaPlan != null)
                foreach (UnidadArancelariaPlan uap in unidadArancelariaPlan)
                {
                    uap.UnidadArancelariaCategoria = UnidadArancelariaCategoriaReadByUAPlan(uap.Id);
                }

            return unidadArancelariaPlan;
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanCurrentReadByPlanes(IList<int> planIds)
        {
            DateTime fecha = enfoke.Time.Now.Date;

            StringBuilder hql = new StringBuilder();
            hql.Append("from UnidadArancelariaPlan uap ");
            hql.Append("where uap.Deleted = false and ( ");

            for (int i = 0; i < planIds.Count; i++)
                hql.Append(" ( uap.ObraSocialPlan.Id =  ").Append(planIds[i]).Append(" ) ").Append(planIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(" ) ");

            hql.Append("and uap.FechaDesde <= :fecha ");
            hql.Append("and (uap.FechaHasta >= :fecha or uap.FechaHasta is null) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fecha", fecha);
            return dalEngine.GetManyByQuery<UnidadArancelariaPlan>(query);
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByPlanesYFecha(IList<int> planIds, DateTime fecha)
        {
            if (planIds == null || planIds.Count == 0)
                return new EntityCollection<UnidadArancelariaPlan>();

            StringBuilder hql = new StringBuilder();
            hql.Append("from UnidadArancelariaPlan uap ");
            hql.Append("where uap.Deleted = false and ( ");
            List<int> distinctsIds = new List<int>(planIds.Distinct());
            for (int i = 0; i < distinctsIds.Count; i++)
                hql.Append(" ( uap.ObraSocialPlan.Id =  ").Append(distinctsIds[i]).Append(" ) ").Append(distinctsIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(" ) ");

            hql.Append("and uap.FechaDesde <= :fecha ");
            hql.Append("and (uap.FechaHasta >= :fecha or uap.FechaHasta is null) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fecha", fecha.Date);
            return dalEngine.GetManyByQuery<UnidadArancelariaPlan>(query);
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanNotDeletedReadByPlanes(List<int> planIds)
        {
            DateTime fecha = enfoke.Time.Now.Date;
            if (planIds.Count == 0)
                return new EntityCollection<UnidadArancelariaPlan>();

            StringBuilder hql = new StringBuilder();
            hql.Append("from UnidadArancelariaPlan uap ");
            hql.Append("where uap.Deleted = false and ( ");

            for (int i = 0; i < planIds.Count; i++)
                hql.Append(" ( uap.ObraSocialPlan.Id =  ").Append(planIds[i]).Append(" ) ").Append(planIds.Count - 1 != i ? " or " : String.Empty);

            hql.Append(" ) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<UnidadArancelariaPlan>(query);
        }

        public UnidadArancelariaPlan UnidadArancelariaPlanGetCurrentByOSPlan(int osPlanID, UnidadArancelaria UA, DateTime fecha)
        {
            if (UA == null)
                return null;

            if (fecha == null || fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now;

            ReadManyCommand<UnidadArancelariaPlan> readCmd = new ReadManyCommand<UnidadArancelariaPlan>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(UnidadArancelariaPlan.Properties.ObraSocialPlan.Id, "=", osPlanID);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.UnidadArancelaria.Id, "=", UA.Id);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            EntityCollection<UnidadArancelariaPlan> UAPs = readCmd.Execute();

            return VigenciaDesdeUtils<UnidadArancelariaPlan>.ObtenerVigente(UAPs, fecha);
        }

        public UnidadArancelariaPlan UnidadArancelariaPlanGetCurrentByOSPlan(int osPlanID, int uaId, DateTime fecha)
        {
            if (fecha == null || fecha == DateTime.MaxValue || fecha == DateTime.MinValue)
                fecha = enfoke.Time.Now;

            ReadManyCommand<UnidadArancelariaPlan> readCmd = new ReadManyCommand<UnidadArancelariaPlan>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(UnidadArancelariaPlan.Properties.ObraSocialPlan.Id, "=", osPlanID);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.UnidadArancelaria.Id, "=", uaId);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.Deleted, "=", false);

            readCmd.Filter = filter;

            EntityCollection<UnidadArancelariaPlan> UAPs = readCmd.Execute();

            return VigenciaDesdeUtils<UnidadArancelariaPlan>.ObtenerVigente(UAPs, fecha);
        }






        /// <summary>
        /// Devuelve las unidades arancelarias de un plan de una obra social.
        /// </summary>
        /// <param name="ospID">El id del plan</param>
        /// <returns>Las unidades arancelarias</returns>
        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByOSPlan(int ospID)
        {
            return UnidadArancelariaPlanReadByOSPlanAndUA(ospID, null);
        }

        [Private]
        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByUAP(UnidadArancelariaPlan uap)
        {
            return UnidadArancelariaPlanReadByOSPlanAndUA(uap.ObraSocialPlan.Id, uap.UnidadArancelaria.Id);
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByUnidadArancelaria(UnidadArancelaria ua)
        {
            ReadManyCommand<UnidadArancelariaPlan> readCmd = new ReadManyCommand<UnidadArancelariaPlan>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(UnidadArancelariaPlan.Properties.UnidadArancelaria.Id, "=", ua.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByOSPlanAndUA(int ospID, int? uaID)
        {
            ReadManyCommand<UnidadArancelariaPlan> readCmd = new ReadManyCommand<UnidadArancelariaPlan>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(UnidadArancelariaPlan.Properties.ObraSocialPlan.Id, "=", ospID);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.Deleted, " = ", false);
            if (uaID.HasValue)
                filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.UnidadArancelaria.Id, "=", uaID.Value);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<UnidadArancelariaPlan> UnidadArancelariaPlanReadByOSPlanesAndUA(List<int> ospID, int? uaID)
        {
            ReadManyCommand<UnidadArancelariaPlan> readCmd = new ReadManyCommand<UnidadArancelariaPlan>(dalEngine);

            // se crea el filtro por plan
            Filter filter = new Filter();
            filter.Add(UnidadArancelariaPlan.Properties.ObraSocialPlan.Id, "IN", ospID);
            filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.Deleted, " = ", false);
            if (uaID.HasValue)
                filter.Add(BooleanOp.And, UnidadArancelariaPlan.Properties.UnidadArancelaria.Id, "=", uaID.Value);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public void UnidadArancelariaPlanUpdate(UnidadArancelariaPlan uap, EntityCollection<UnidadArancelariaCategoria> UACs)
        {
            UnidadArancelariaPlanUpdate(uap, false, false, UACs);
        }

        public void UnidadArancelariaPlanFinalizar(UnidadArancelariaPlan uap, DateTime fechaHasta)
        {
            // Seteo la Fecha Hasta
            uap.FechaHasta = fechaHasta;

            // Actualizo
            UnidadArancelariaPlanUpdate(uap, true, false, null);
        }

        public void UnidadArancelariaPlanEliminar(UnidadArancelariaPlan uap)
        {
            // Marco como Eliminado
            uap.Deleted = true;

            // Actualizo
            UnidadArancelariaPlanUpdate(uap, false, true, null);
        }

        [RequiresTransaction]
        [Private]
        public virtual UnidadArancelariaPlan UnidadArancelariaPlanUpdateForHerramientaCopia(UnidadArancelariaPlan uap, EntityCollection<UnidadArancelariaPlan> uaps)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            EntityCollection<UnidadArancelariaPlan> UAPs = new EntityCollection<UnidadArancelariaPlan>();
            UAPs.AddRange(VigenciaUtils<UnidadArancelariaPlan>.ObtenerModificaciones(uaps, uap, false, user));


            UAPs = dalEngine.UpdateCollection<UnidadArancelariaPlan>(UAPs);

            UnidadArancelariaPlan uapUpdated = new UnidadArancelariaPlan();
            foreach (UnidadArancelariaPlan uapItem in UAPs)
                if (uapItem.UnidadArancelaria.Id == uap.UnidadArancelaria.Id && uapItem.Id > uapUpdated.Id)
                    uapUpdated = uapItem;

            return uapUpdated;
        }

        /// <summary>
        /// Crea o modifica una unidad arancelaria plan y sus valores por categoria.
        /// </summary>
        /// <param name="uap">La UAP Modificada</param>
        /// <param name="finalizar">Marca si el registro esta Finalizado</param>
        /// <param name="eliminar">Marca si el registro esta Eliminado</param>
        /// <param name="UACs">Las UACs de la UAP Nueva</param>
        [RequiresTransaction]
        protected virtual void UnidadArancelariaPlanUpdate(UnidadArancelariaPlan uap, bool finalizar, bool eliminar, EntityCollection<UnidadArancelariaCategoria> UACs)
        {
            UnidadArancelariaPlan uapUpdated = new UnidadArancelariaPlan();
            SecurityUser user = Security.Current.UserInfo.User;
            EntityCollection<UnidadArancelariaPlan> UAPs = new EntityCollection<UnidadArancelariaPlan>();

            if (eliminar)
            {
                // Audito
                Audit.AuditDelete(uap, user.Id);
            }

            if (uap.Id == 0 && UACs.Count > 0)
            {
                UAPs.AddRange(VigenciaUtils<UnidadArancelariaPlan>.ObtenerModificaciones(UAPs, uap, finalizar, user));
            }
            else
                UAPs.Add(uap);

            // Actualizo
            UAPs = dalEngine.UpdateCollection<UnidadArancelariaPlan>(UAPs);
            foreach (UnidadArancelariaPlan uapItem in UAPs)
            {
                if (uapItem.UnidadArancelaria.Id == uap.UnidadArancelaria.Id)
                    if (uapItem.Id > uapUpdated.Id)
                        uapUpdated = uapItem;
            }


            // Busco la UAP nueva para adherirle los UACs
            if (UACs != null && UACs.Count > 0)
            {
                //UnidadArancelariaPlan uapNueva = VigenciaUtils<UnidadArancelariaPlan>.ObtenerUltimo(UAPs);

                EntityCollection<UnidadArancelariaCategoria> uacOriginales = UnidadArancelariaCategoriaReadByUAPlan(uapUpdated.Id);


                dalEngine.Delete(uacOriginales);

                // Seteo el UAP a las UACs
                foreach (UnidadArancelariaCategoria uac in UACs)
                    uac.UnidadArancelariaPlan = uapUpdated;


                UACs = dalEngine.UpdateCollection<UnidadArancelariaCategoria>(UACs);
            }
        }

        /// <summary>
        /// Elimina una unidad arancelaria.
        /// </summary>
        /// <param name="uap">Una unidad arancelaria</param>
        public void UnidadArancelariaPlanDelete(UnidadArancelariaPlan uap)
        {
            // Pongo fecha hasta ayer
            uap.FechaHasta = enfoke.Time.Today.AddDays(-1);


            uap = dalEngine.Update<UnidadArancelariaPlan>(uap);
        }

        public Dictionary<UnidadArancelariaPlan, bool> UnidadArancelariaPlanReadByOSPlanWithCategorias(int id)
        {
            Dictionary<UnidadArancelariaPlan, bool> retorno = new Dictionary<UnidadArancelariaPlan, bool>();

            // Obtengo las UAPs
            EntityCollection<UnidadArancelariaPlan> UAPs = Context.Session.ObrasSocialesDalc.UnidadArancelariaPlanReadByOSPlan(id);

            // Obtengo a ver si tiene Categorias
            foreach (UnidadArancelariaPlan uap in UAPs)
                retorno.Add(uap, Context.Session.ObrasSocialesDalc.UnidadArancelariaCategoriaReadByUAPlan(uap.Id).Count > 0);

            return retorno;
        }
        #endregion

        #region TipoCobertura
        /// <summary>
        /// Devuelve todos los tipos de cobertura
        /// </summary>
        /// <returns>Todos los tipos de cobertura</returns>
        public EntityCollection<TipoCobertura> TipoCoberturaReadAll()
        {
            ReadManyCommand<TipoCobertura> readCmd = new ReadManyCommand<TipoCobertura>(dalEngine);

            return readCmd.Execute();
        }

        /// <summary>
        /// Devuelve el tipo de cobertura con un nombre determinado
        /// </summary>
        /// <param name="name">El nombre del tipo de cobertura</param>
        /// <returns>El tipo de cobertura con dicho nombre</returns>
        public TipoCobertura TipoCoberturaRead(string name)
        {
            return dalEngine.GetByProperty<TipoCobertura>(TipoCobertura.Properties.Name, name);
        }











        public TipoCobertura TipoCoberturaGetCurrentByObraSocialPlanAndPracticaAdicionalAndEquipo(int? plan, PracticaAdicional practicaAdicional, DateTime? fecha)
        {
            PracticasDalc PracticasDalc = Context.Session.PracticasDalc;
            Practica practica = dalEngine.GetById<Practica>(practicaAdicional.PracticaID);

            PlanPracticaPrecio planPractica = null;

            if (practica.TipoPractica.Id == (int)TipoPracticaEnum.Modulo)
            {
                if (practica.EsDetallado)
                    planPractica = Context.Session.ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(
                  plan, practicaAdicional.PracticaID, practicaAdicional.Id, null, fecha, true);
            }
            else
            {
                planPractica = Context.Session.ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(
            plan, practicaAdicional.PracticaID, practicaAdicional.Id, null, fecha, true);

                if (planPractica == null)
                    planPractica = Context.Session.ObrasSocialesDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(
            plan, practicaAdicional.Adicional.Id, null, null, fecha, false);
            }

            if (planPractica != null)
                return planPractica.TipoCobertura;

            return null;
        }
        #endregion

        #region TipoCoseguro










        /// <summary>
        /// Retorno todos los TipoCoseguro
        /// </summary>
        /// <returns>Todos los TipoCoseguro</returns>
        public EntityCollection<TipoCoseguro> TipoCoseguroReadAll()
        {
            return dalEngine.GetAll<TipoCoseguro>(TipoCoseguro.Properties.Name);
        }
        #endregion

        #region FormatoValidacion
        //[Private]
        public FormatoValidacion FormatoValidacionReadByTipoAndObraSocialPlan(TipoFormatoValidacion tipo, int ospID)
        {
            ReadManyCommand<FormatoValidacion> readCmd = new ReadManyCommand<FormatoValidacion>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(FormatoValidacion.Properties.Tipo, "=", (int)tipo);

            readCmd.Filter.Add(BooleanOp.And, FormatoValidacion.Properties.ObraSocialPlanID, "=", ospID);

            EntityCollection<FormatoValidacion> formatos = readCmd.Execute();

            if (formatos.Count > 0)
                return formatos[0];
            else
                return null;
        }

        [RequiresTransaction]
        public virtual FormatoValidacion FormatoValidacionUpdate(FormatoValidacion formato)
        {
            bool nuevo = formato.Id == 0;

            // Elimino los items Existentes
            if (!nuevo)
            {

                EntityCollection<FormatoValidacionItem> itemsToDelete = FormatoValidacionItemReadByFormato(formato.Id);
                dalEngine.Delete(itemsToDelete);

                foreach (FormatoValidacionItem item in formato.Items)
                {
                    item.Id = 0;
                }
            }
            else
            {
                FormatoValidacion formatoExistente = FormatoValidacionReadByTipoAndObraSocialPlan(formato.Tipo, formato.ObraSocialPlanID);

                if (formatoExistente != null)
                {

                    EntityCollection<FormatoValidacionItem> itemsToDelete = FormatoValidacionItemReadByFormato(formatoExistente.Id);
                    dalEngine.Delete(itemsToDelete);


                    dalEngine.Delete(formatoExistente);
                }
            }


            // Guardo el formato
            formato = dalEngine.Update<FormatoValidacion>(formato);

            // Guardo los Items Nuevos
            foreach (FormatoValidacionItem item in formato.Items)
                item.FormatoValidacionAfiliadoID = formato.Id;

            formato.Items = dalEngine.UpdateCollection<FormatoValidacionItem>(formato.Items);

            return formato;
        }
        #endregion

        #region FormatoValidacionItem
        [Private]
        public EntityCollection<FormatoValidacionItem> FormatoValidacionItemReadByFormato(int formatoID)
        {
            return dalEngine.GetManyByProperty<FormatoValidacionItem>(FormatoValidacionItem.Properties.FormatoValidacionAfiliadoID, formatoID, FormatoValidacionItem.Properties.Desde);
        }
        #endregion

        #region PadronValidacionAfiliado
        [Private]
        public bool PadronValidacionAfiliadoExiste(int ospID, string afiliado)
        {
            ReadManyCommand<PadronValidacionAfiliado> readCmd = new ReadManyCommand<PadronValidacionAfiliado>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(PadronValidacionAfiliado.Properties.ObraSocialPlanID, "=", ospID);

            readCmd.Filter.Add(BooleanOp.And, PadronValidacionAfiliado.Properties.NumeroAfiliado, "=", afiliado);

            // Si retorna algo, es porque existe
            return readCmd.Execute().Count > 0;
        }
        #endregion

        #region Tipo Plan

        /// <summary>
        /// Retorna todos los tipos de planes
        /// </summary>
        /// <returns>Tipos de planes</returns>
        public EntityCollection<TipoPlan> TipoPlanGetAll()
        {
            return dalEngine.GetAll<TipoPlan>();
        }

        public TipoPlan TipoPlanReadByTag(string tag)
        {
            return dalEngine.GetByProperty<TipoPlan>(TipoPlan.Properties.Tag, tag);
        }

        /// <summary>
        /// Retorna todos los tipos de planes correspondiente a la obra social plan
        /// </summary>
        /// <returns>Tipos de planes</returns>
        public EntityCollection<TipoPlan> GetTipoPlanByObraSocialPlan(int obraSocialPlanId)
        {
            string hql = "select tpl " +
                       "from TipoPlan tpl, " +
                       "ObraSocialPlanTipo ospt, " +
                       "ObraSocialPlan osp " +
                       "where ospt.TipoPlanId = tpl.Id " +
                       "and   ospt.ObraSocialPlanId = osp.Id " +
                       "and   osp.Id = :obraSocialPlanId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("obraSocialPlanId", obraSocialPlanId);
            return dalEngine.GetManyByQuery<TipoPlan>(query);
        }

        /// <summary>
        /// Retorna todos los obra social tipos de planes correspondiente a la obra social plan
        /// </summary>
        /// <returns>Obra social tipos de planes</returns>
        public TipoPlan GeTipoPlanByObraSocialPlanAndTipoPlanId(int obraSocialPlanId, int tipoPlanId)
        {
            string hql = "select tpl " +
                      "from TipoPlan tpl, " +
                      "ObraSocialPlanTipo ospt, " +
                      "ObraSocialPlan osp " +
                      "where ospt.TipoPlanId = tpl.Id " +
                      "and   ospt.ObraSocialPlanId = osp.Id " +
                      "and   osp.Id = :obraSocialPlanId " +
                      "and   tpl.Id = :tipoPlanId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("obraSocialPlanId", obraSocialPlanId);
            query.SetParameter("tipoPlanId", tipoPlanId);
            return dalEngine.GetByQuery<TipoPlan>(query);
        }

        /// <summary>
        /// Retorna todos los obra social tipos de planes correspondiente a la obra social plan
        /// </summary>
        /// <returns>Obra social tipos de planes</returns>
        public EntityCollection<ObraSocialPlanTipo> GetObraSocialPlanTipoByObraSocialPlan(int obraSocialPlanId)
        {
            string hql = "select ospt " +
                         "from ObraSocialPlanTipo ospt, " +
                         "ObraSocialPlan osp " +
                         "where   ospt.ObraSocialPlanId = osp.Id " +
                         "and   osp.Id = :obraSocialPlanId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("obraSocialPlanId", obraSocialPlanId);
            return dalEngine.GetManyByQuery<ObraSocialPlanTipo>(query);
        }

        /// <summary>
        /// Retorna el tipo de plan con mayor IVA, correspondiente a la obra social plan
        /// </summary>
        /// <returns>Obra social tipo de plan</returns>
        public TipoPlan TipoPlanConMayorIVAReadByObraSocialPlan(int obraSocialPlanId)
        {
            string hql = "SELECT tp " +
                         "FROM ObraSocialPlanTipo ospt, TipoPlan tp " +
                         "WHERE ospt.TipoPlanId = tp.Id " +
                         "AND ospt.ObraSocialPlanId = :obraSocialPlanId " +
                         "ORDER BY tp.Iva DESC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("obraSocialPlanId", obraSocialPlanId);
            query.SetMaxResults(1);

            return dalEngine.GetByQuery<TipoPlan>(query);
        }

        /// <summary>
        /// Retorna el valor de mayor IVA, correspondiente a la obra social plan
        /// </summary>
        /// <returns>Valor del mayor Iva (Decimal)</returns>
        public Decimal TipoPlanMayorIVAReadByObraSocialPlan(int obraSocialPlanId)
        {
            TipoPlan tipoPlan = TipoPlanConMayorIVAReadByObraSocialPlan(obraSocialPlanId);

            if (tipoPlan == null)
                throw new NullReferenceException("No existe un tipo de plan asociado a la obra social plan");

            return tipoPlan.Iva;
        }

        /// <summary>
        /// Inserta o modifica los tipos de planes por plan
        /// </summary>
        public void ObraSocialPlanTipoUpdate(EntityCollection<TipoPlan> tiposPlan, int obraSocialPlanId)
        {
            //Obtengo todos los obra social tipo plan relacionados 
            EntityCollection<ObraSocialPlanTipo> ospts = this.GetObraSocialPlanTipoByObraSocialPlan(obraSocialPlanId);

            //Si existe alguno lo borro
            if (ospts != null && ospts.Count > 0)
            {

                // Elimino
                dalEngine.Delete(ospts);
            }

            //Si existen tipo planes a agregar se insertan uno por uno
            if (tiposPlan != null && tiposPlan.Count > 0)
            {
                EntityCollection<ObraSocialPlanTipo> newsOspt = new EntityCollection<ObraSocialPlanTipo>();
                foreach (TipoPlan tPlan in tiposPlan)
                {
                    ObraSocialPlanTipo newOspt = new ObraSocialPlanTipo();
                    newOspt.ObraSocialPlanId = obraSocialPlanId;
                    newOspt.TipoPlanId = tPlan.Id;
                    newsOspt.Add(newOspt);
                }


                // Guardo
                dalEngine.UpdateCollection<ObraSocialPlanTipo>(newsOspt);
            }
        }

        #endregion

        #region Agrupamiento Factura Por Plan
        public EntityCollection<ObraSocialFacturaPlan> ObraSocialFacturaPlanReadByOs(int obraSocialId)
        {
            return dalEngine.GetManyByProperty<ObraSocialFacturaPlan>(ObraSocialFacturaPlan.Properties.ObraSocial.Id, obraSocialId, ObraSocialFacturaPlan.Properties.NumeroGrupo);
        }

        public void ObraSocialFacturaPlanDelete(EntityCollection<ObraSocialFacturaPlan> planes)
        {
            dalEngine.Delete(planes);
        }





        #endregion


        #region PlanPracticaCobInsumo

        /// <summary>
        /// Obtiene los porcentejes de cobertura y bonificación para los insumos de un convenio
        /// </summary>
        /// <param name="planPracticaId">Id del convenio a filtrar</param>
        /// <returns>PlanPracticaInsumo</returns>
        public PlanPracticaCobInsumo PlanPracticaCobInsumoReadByPlanPracticaId(int planPracticaId)
        {
            Filter filter = new Filter();
            filter.Add(PlanPracticaCobInsumo.Properties.PlanPracticaId, "=", planPracticaId);
            EntityCollection<PlanPracticaCobInsumo> planPracticaCobInsumos = dalEngine.GetManyByFilter<PlanPracticaCobInsumo>(filter);
            if (planPracticaCobInsumos != null && planPracticaCobInsumos.Count > 0)
                return planPracticaCobInsumos[0];
            else
                return null;
        }

        #endregion

        public ObraSocialName ObraSocialNameReadByLoteTrasladoId(int idLoteTraslado)
        {
            EntityCollection<OrdenHQL> orden = dalEngine.GetManyByProperty<OrdenHQL>(OrdenHQL.Properties.LoteTraslado.Id, idLoteTraslado);
            if (orden != null && orden.Count > 0)
                return dalEngine.GetById<ObraSocialName>(orden[0].ObraSocialPlan.ObraSocial.Id);

            return null;
        }

        public ObraSocialPlanLight ObraSocialPlanLightReadByLoteTrasladoId(int idLoteTraslado)
        {
            string hql = "SELECT ospl FROM TurnoHQL t, ObraSocialPlanLight ospl " +
                         "WHERE t.Orden.LoteTraslado.Id = :idLoteTraslado AND t.Orden.ObraSocialPlan.Id = ospl.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idLoteTraslado", idLoteTraslado);

            EntityCollection<ObraSocialPlanLight> osps = dalEngine.GetManyByQuery<ObraSocialPlanLight>(query);
            if (osps != null && osps.Count > 0)
                return osps[0];
            else
                return null;
        }

        public ObraSocialPlanLight ObraSocialPlanLightReadByOrdenId(int idOrden)
        {
            string hql = "SELECT ospl FROM Orden o, ObraSocialPlanLight ospl " +
                         "WHERE o.Id = :idOrden AND o.ObraSocialPlanId = ospl.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idOrden", idOrden);

            return dalEngine.GetByQuery<ObraSocialPlanLight>(query);
        }

        public ObraSocial ObraSocialReadByTurnoId(int turnoId)
        {
            string hql = "SELECT osp.ObraSocial FROM Turno tur, ObraSocialPlan osp " +
                         "WHERE tur.Id = :turnoId AND tur.Orden.ObraSocialPlanId = osp.Id ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("turnoId", turnoId);

            return dalEngine.GetByQuery<ObraSocial>(query);
        }


        public PlanPracticaHC PlanPracticaHCVigenteReadByPlanPracticaAndFecha(int practicaId, int planId, DateTime fechaVigencia)
        {
            return (from convenio in dalEngine.Query<PlanPracticaHC>()
                    where convenio.PlanId == planId && convenio.PracticaId == practicaId
                        && convenio.FechaDesde <= fechaVigencia.Date && (convenio.FechaHasta == null || fechaVigencia.Date <= convenio.FechaHasta)
                        && convenio.Deleted == false
                    select convenio).FirstOrDefault();
        }

        public EntityCollection<PlanPracticaHC> PlanPracticaHCIncluyendoExcedentesReadByPracticasAndPlanAndFecha(List<KeyValuePair<int, int?>> practicasEquiposIds, int planId, DateTime fechaVigencia)
        {
            if (practicasEquiposIds == null || practicasEquiposIds.Count == 0)
                return new EntityCollection<PlanPracticaHC>();

            List<int?> equiposIds = new List<int?>();
            foreach (KeyValuePair<int, int?> equipo in practicasEquiposIds)
            {
                if (!equiposIds.Contains(equipo.Value))
                    equiposIds.Add(equipo.Value);
            }

            EntityCollection<PlanPracticaHC> response = new EntityCollection<PlanPracticaHC>();
            EntityCollection<PlanPracticaHC> adicionales = new EntityCollection<PlanPracticaHC>();
            int rows_per_current_page = Math.Min(1000, practicasEquiposIds.Count);
            while (rows_per_current_page > 0)
            {
                StringBuilder parentHqlWhere = new StringBuilder("select convenio from PlanPracticaHC convenio where " +
                                    "convenio.PracticaAdicionalId is null and " +
                                    "convenio.Deleted = false and " +
                                    "convenio.PlanId = " + planId.ToString() + " and " +
                                    "(convenio.FechaHasta is null or convenio.FechaHasta >= :fecha) ");

                StringBuilder aditionalHqlBuilder = new StringBuilder("select convenio from PlanPracticaHC convenio, PracticaAdicional adic where " +
                                        "convenio.PracticaAdicionalId = adic.Id and " +
                                        "convenio.Deleted = false and " +
                                        "convenio.PlanId = " + planId.ToString() + " and " +
                                        "(convenio.FechaHasta is null or convenio.FechaHasta >= :fecha) ");

                parentHqlWhere.Append(" and ( convenio.PracticaId IN (:practicas) ");
                aditionalHqlBuilder.Append(" and ( convenio.PracticaId IN (:practicas) ");
                List<KeyValuePair<int, int?>> ids = practicasEquiposIds.GetRange(0, rows_per_current_page);
                List<int> practicas = new List<int>();
                for (int index = 0; index < ids.Count; index++)
                    practicas.Add(practicasEquiposIds[index].Key);

                aditionalHqlBuilder.Append(") ");
                parentHqlWhere.Append(") ");
                IQuery query = dalEngine.CreateQuery(parentHqlWhere.ToString());
                query.SetDateTime("fecha", fechaVigencia.Date);
                query.SetParameterList("practicas", practicas);
                response.AddRange(dalEngine.GetManyByQuery<PlanPracticaHC>(query));
                query = dalEngine.CreateQuery(aditionalHqlBuilder.ToString());
                query.SetDateTime("fecha", fechaVigencia.Date);
                query.SetParameterList("practicas", practicas);
                adicionales.AddRange(dalEngine.GetManyByQuery<PlanPracticaHC>(query));
                practicasEquiposIds.RemoveRange(0, rows_per_current_page);
                rows_per_current_page = Math.Min(1000, practicasEquiposIds.Count);
            }

            response.AddRange(adicionales);
            EntityCollection<PlanPracticaHC> toRemove = new EntityCollection<PlanPracticaHC>();
            foreach (PlanPracticaHC convenio in response)
            {
                if (!equiposIds.Contains(convenio.EquipoId))
                    toRemove.Add(convenio);
            }

            response.RemoveRange(toRemove);
            return response;
        }

        public EntityCollection<PlanPracticaHC> PlanPracticaHCVigenteReadByPracticasAndPlanAndFecha(List<KeyValuePair<int, int?>> practicasEquiposIds, int planId, DateTime fechaVigencia)
        {
            if (practicasEquiposIds == null || practicasEquiposIds.Count == 0)
                return new EntityCollection<PlanPracticaHC>();

            List<int?> equiposIds = new List<int?>();
            foreach (KeyValuePair<int, int?> equipo in practicasEquiposIds)
            {
                if (!equiposIds.Contains(equipo.Value))
                    equiposIds.Add(equipo.Value);
            }

            EntityCollection<PlanPracticaHC> response = new EntityCollection<PlanPracticaHC>();
            EntityCollection<PlanPracticaHC> adicionales = new EntityCollection<PlanPracticaHC>();
            int rows_per_current_page = Math.Min(1000, practicasEquiposIds.Count);
            while (rows_per_current_page > 0)
            {
                StringBuilder parentHqlWhere = new StringBuilder("select convenio from PlanPracticaHC convenio where " +
                                    "convenio.FechaDesde <= :fecha and " +
                                    "convenio.PracticaAdicionalId is null and " +
                                    "convenio.Deleted = false and " +
                                    "convenio.PlanId = " + planId.ToString() + " and " +
                                    "(convenio.FechaHasta is null or convenio.FechaHasta >= :fecha) ");

                StringBuilder aditionalHqlBuilder = new StringBuilder("select convenio from PlanPracticaHC convenio where " +
                                        "convenio.FechaDesde <= :fecha and " +
                                        "convenio.Deleted = false and " +
                                        "convenio.PlanId = " + planId.ToString() + " and " +
                                        "(convenio.FechaHasta is null or convenio.FechaHasta >= :fecha) ");

                parentHqlWhere.Append(" and ( ");
                aditionalHqlBuilder.Append(" and ( ");
                List<KeyValuePair<int, int?>> ids = practicasEquiposIds.GetRange(0, rows_per_current_page);
                for (int index = 0; index < ids.Count; index++)
                {
                    KeyValuePair<int, int?> current = practicasEquiposIds[index];
                    parentHqlWhere.Append(" (convenio.PracticaId = " + current.Key.ToString() + (current.Value != null ? " and convenio.EquipoId = " + current.Value.ToString() : " and convenio.EquipoId is null ") + ")");
                    aditionalHqlBuilder.Append(" (convenio.PracticaAdicionalId = " + current.Key.ToString() + (current.Value != null ? " and convenio.EquipoId = " + current.Value.ToString() : " and convenio.EquipoId is null ") + ")");
                    if (index < ids.Count - 1)
                    {
                        parentHqlWhere.Append(" or ");
                        aditionalHqlBuilder.Append(" or ");
                    }
                }

                aditionalHqlBuilder.Append(") ");
                parentHqlWhere.Append(") ");
                IQuery query = dalEngine.CreateQuery(parentHqlWhere.ToString());
                query.SetDateTime("fecha", fechaVigencia.Date);
                response.AddRange(dalEngine.GetManyByQuery<PlanPracticaHC>(query));
                query = dalEngine.CreateQuery(aditionalHqlBuilder.ToString());
                query.SetDateTime("fecha", fechaVigencia.Date);
                adicionales.AddRange(dalEngine.GetManyByQuery<PlanPracticaHC>(query));
                practicasEquiposIds.RemoveRange(0, rows_per_current_page);
                rows_per_current_page = Math.Min(1000, practicasEquiposIds.Count);
            }

            response.AddRange(adicionales);
            EntityCollection<PlanPracticaHC> toRemove = new EntityCollection<PlanPracticaHC>();
            foreach (PlanPracticaHC convenio in response)
            {
                if (!equiposIds.Contains(convenio.EquipoId))
                    toRemove.Add(convenio);
            }

            response.RemoveRange(toRemove);
            return response;
        }

        public EntityCollection<PlanPracticaRequisitoHC> PlanPracticaRequisitoReadByPracticasAndPlanAndDate(List<int> practicasIds, int planId, DateTime fechaVigencia, bool traerFuturos)
        {
            if (practicasIds == null || practicasIds.Count == 0)
                return new EntityCollection<PlanPracticaRequisitoHC>();

            EntityCollection<PlanPracticaRequisitoHC> requisitos = new EntityCollection<PlanPracticaRequisitoHC>();
            int rows_per_page = Math.Min(1000, practicasIds.Count);
            while (rows_per_page > 0)
            {
                List<int> ids = practicasIds.GetRange(0, rows_per_page);
                if (!traerFuturos)
                {
                    requisitos.AddRange((from requisito in dalEngine.Query<PlanPracticaRequisitoHC>()
                                         where requisito.PlanId == planId
                                         && ids.Contains(requisito.PracticaId)
                                         && requisito.Deleted == false
                                         && requisito.FechaDesde <= fechaVigencia.Date
                                         && (requisito.FechaHasta == null || requisito.FechaHasta >= fechaVigencia.Date)
                                         select requisito).ToEntityCollection());
                    practicasIds.RemoveRange(0, rows_per_page);
                }
                else
                {
                    requisitos.AddRange((from requisito in dalEngine.Query<PlanPracticaRequisitoHC>()
                                         where requisito.PlanId == planId
                                         && ids.Contains(requisito.PracticaId)
                                         && requisito.Deleted == false
                                         && (requisito.FechaHasta == null || requisito.FechaHasta >= fechaVigencia.Date)
                                         select requisito).ToEntityCollection());
                    practicasIds.RemoveRange(0, rows_per_page);
                }

                rows_per_page = Math.Min(1000, practicasIds.Count);
            }

            CargarDocumentacion(requisitos);
            return requisitos;
        }


        private static void CargarDocumentacion(EntityCollection<PlanPracticaRequisitoHC> response)
        {
            EntityCollection<PlanPracticaDocumentacion> documentacion = Context.Session.RequisitosDalc.PlanPracticaDocumentacionReadByPlanPracticaIds(response.GetIds());
            foreach (PlanPracticaRequisitoHC requisito in response)
                requisito.Documentacion = (from doc in documentacion where doc.RequisitosId == requisito.Id select doc).ToEntityCollection();
        }

        public PlanPracticaRequisitoHC PlanPracticaRequisitoReadByPracticaPlanAndFecha(int practicaId, int planId, DateTime fechaVigencia, bool traerFuturos)
        {
            EntityCollection<PlanPracticaRequisitoHC> response = PlanPracticaRequisitoReadByPracticasAndPlanAndDate(new List<int>() { practicaId }, planId, fechaVigencia, traerFuturos);
            if (response.Count > 0)
                return response[0];

            return null;
        }

        public EntityCollection<Empresa> GetEmpresasFaltantes(List<int> empresasAFiltrar)
        {
            EntityCollection<Empresa> empresasFaltantes;
            int count = empresasAFiltrar.Count;
            if (empresasAFiltrar != null && count > 0)
                empresasFaltantes = (from emp in dalEngine.Query<Empresa>() where !empresasAFiltrar.Contains(emp.Id) select emp).ToEntityCollection<Empresa>();
            else
                empresasFaltantes = Context.Session.Dalc.GetAll<Empresa>();
            return empresasFaltantes;
        }

        public ObraSocialReserva GetObraSocialReservaTurnoId(int turId)
        {
            ObraSocialReserva osr = (from tur in dalEngine.Query<Turno>()
                                     join osp in dalEngine.Query<ObraSocialPlan>()
                                        on tur.Orden.ObraSocialPlanId equals osp.Id
                                     join os in dalEngine.Query<ObraSocialReserva>()
                                        on osp.ObraSocial.Id equals os.Id
                                     where tur.Id == turId
                                     select os).FirstOrDefault<ObraSocialReserva>();
            return osr;
        }

        public ObraSocialPlanReserva GetObraSocialPlanReservaTurnoId(int turId)
        {
            ObraSocialPlanReserva osr = (from tur in dalEngine.Query<Turno>()
                                         join osp in dalEngine.Query<ObraSocialPlanReserva>()
                                            on tur.Orden.ObraSocialPlanId equals osp.Id
                                         where tur.Id == turId
                                         select osp).FirstOrDefault<ObraSocialPlanReserva>();
            return osr;

        }

        public ObraSocialPlanRequisito GetObraSocialPlanRequisitoVigente(int planId)
        {
            DateTime now = enfoke.IO.Time.Now.Date;
            ObraSocialPlanRequisito ospr = (from requisito in dalEngine.Query<ObraSocialPlanRequisito>()
                                            where
                                                requisito.Plan.Id == planId &&
                                                requisito.FechaDesde <= now &&
                                                (requisito.FechaHasta == null || requisito.FechaHasta >= now)
                                            select requisito).FirstOrDefault<ObraSocialPlanRequisito>();
            return ospr;
        }

        [Private]
        public EntityCollection<PlanPracticaPrecio> GetPlanPracticaPreciosVigenteAFechaTurno(int turnoId)
        {
            EntityCollection<PracticaTurno> practicaTurnos = Context.Session.Dalc.GetManyByProperty<PracticaTurno>(PracticaTurno.Properties.TurnoId, turnoId);
            List<int> practicasIds = (from pt in practicaTurnos select pt.Practica.Id).ToList<int>();

            EntityCollection<PlanPracticaPrecio> planPracticaPrecios = new EntityCollection<PlanPracticaPrecio>();
            if (practicasIds.Count > 0)
            {
                Turno turno = Context.Session.Dalc.GetById<Turno>(turnoId);
                DateTime fechaVigencia = (turno.Fecha.HasValue) ? turno.Fecha.Value.Date : DateTime.Now.Date;
                DateTime fechaDesde = fechaVigencia.AddDays((double)1).AddMinutes((double)-1);
                DateTime fechaHasta = fechaVigencia;
                planPracticaPrecios = (from ppp in dalEngine.Query<PlanPracticaPrecio>()
                                       where
                                           ppp.Plan.Id == turno.Orden.ObraSocialPlanId &&
                                           practicasIds.Contains(ppp.Practica.Id) &&
                                           ppp.FechaDesde <= fechaDesde &&
                                           ((ppp.FechaHasta == null) || (ppp.FechaHasta.Value >= fechaHasta)) &&
                                           !ppp.Deleted
                                       select ppp).ToEntityCollection<PlanPracticaPrecio>();
            }

            return planPracticaPrecios;
        }

        [Private]
        public EntityCollection<PlanPracticaPrecio> GetPlanPracticaPreciosByValorizacionInfo(ValorizacionInfo valorizacionInfo, EntityCollection<PlanPracticaPrecio> planPracticaPrecios)
        {
            List<int> planPracticaPrecioIds = new List<int>();
            foreach (ValorizacionItem valItem in valorizacionInfo.Items)
                if (valItem.PlanPracticaUsadoId.HasValue)
                    planPracticaPrecioIds.Add(valItem.PlanPracticaUsadoId.Value);
            if (planPracticaPrecioIds.Count > 0)
                planPracticaPrecios = Context.Session.Dalc.GetManyByIds<PlanPracticaPrecio>(planPracticaPrecioIds);
            return planPracticaPrecios;
        }
    }
}
