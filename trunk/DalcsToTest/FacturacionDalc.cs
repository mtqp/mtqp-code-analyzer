using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using NHibernate;
using System.Linq;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Eges.Auditoria;
using enfoke.Data.Reference;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Entities.Configuracion;
using enfoke.Eges.Persistance;
using enfoke.Eges.Valorizacion;
using enfoke.Eges.Utils;
using enfoke.Data.Filters;
using System.Globalization;
using enfoke.AOP;
using IFilter = enfoke.AOP.IFilter;
using enfoke.Eges.Persistence;

namespace enfoke.Eges.Data
{
    public class FacturacionDalc : Dalc, IService
    {
        protected FacturacionDalc(NotConstructable dummy) : base(dummy) { }

        #region TipoControlFacturacion

        public TipoControlFacturacion TipoControlFacturacionReadById(int id)
        {
            // los cachea a nivel thread...
            TipoControlFacturacion ret = EntityThreadCache<TipoControlFacturacion>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<TipoControlFacturacion>(id);
                EntityThreadCache<TipoControlFacturacion>.SetItem(id, ret);
            }
            return ret;
        }
        #endregion

        #region TipoDebito
        /// <summary>
        /// Retorno un TipoDebito para un ID
        /// </summary>
        /// <param name="id">ID del TipoDebito</param>
        /// <returns>El TipoDebito correspondiente</returns>
        public TipoDebito TipoDebitoReadByID(int id)
        {
            // los cachea a nivel thread...
            TipoDebito ret = EntityThreadCache<TipoDebito>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<TipoDebito>(id);
                EntityThreadCache<TipoDebito>.SetItem(id, ret);
            }
            return ret;
        }

        public TipoDebito TipoDebitoReadByCodigo(string codigo)
        {
            // Lo hago asi medio raro porque en Oracle se esta guardando el codigo con espacios porq es un CHAR(10 CHAR)
            StringBuilder hql = new StringBuilder(" from TipoDebito td where td.Codigo LIKE :codigo");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetString("codigo", codigo + "%");

            EntityCollection<TipoDebito> tds = dalEngine.GetManyByQuery<TipoDebito>(query);
            foreach (TipoDebito tipoDebito in tds)
            {
                if (tipoDebito.Codigo.Trim() == codigo)
                    return tipoDebito;
            }

            return null;
        }

        public TipoDebito TipoDebitoReadByName(string name)
        {
            return dalEngine.GetByProperty<TipoDebito>(TipoDebito.Properties.Name, name);
        }

        public EntityCollection<TipoDebito> TipoDebitoReadByFilters(string codigo, string descripcion)
        {
            bool hayFiltros = !String.IsNullOrEmpty(codigo) || !String.IsNullOrEmpty(descripcion);

            StringBuilder hql = new StringBuilder(" from TipoDebito td ");

            if (hayFiltros)
            {
                hql.Append(" where ");

                if (!String.IsNullOrEmpty(codigo))
                    hql.Append(" td.Codigo LIKE :codigo");

                if (!String.IsNullOrEmpty(descripcion))
                {
                    if (!String.IsNullOrEmpty(codigo))
                        hql.Append(" and ");

                    hql.Append("td.Name LIKE :descripcion");
                }
            }

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            if (!String.IsNullOrEmpty(codigo))
                query.SetString("codigo", codigo + "%");
            if (!String.IsNullOrEmpty(descripcion))
                query.SetString("descripcion", descripcion + "%");

            return dalEngine.GetManyByQuery<TipoDebito>(query);
        }

        public void TipoDebitoDelete(int tipoDebitoId)
        {
            TipoDebito tipoDebito = TipoDebitoReadByID(tipoDebitoId);
            dalEngine.Delete(tipoDebito);
        }

        /// <summary>
        /// Retorno todos los TipoDebito
        /// </summary>
        /// <returns>Todos los TipoDebito</returns>
        public EntityCollection<TipoDebito> TipoDebitoReadAll()
        {
            return dalEngine.GetAll<TipoDebito>(TipoDebito.Properties.Name);
        }

        /// <summary>
        /// Actualizo los TipoDebito
        /// </summary>
        /// <param name="tipos">Los TipoDebito a Actualizar</param>
        /// <param name="user">Usuario de la Modificación</param>
        public void TipoDebitoUpdateMany(EntityCollection<TipoDebito> tipos)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            foreach (TipoDebito tipo in tipos)
            {
                tipo.UpdateUser = user.Id;
                tipo.UpdateDate = enfoke.Time.Now;
            }


            tipos = dalEngine.UpdateCollection<TipoDebito>(tipos);
        }

        public void TipoDebitoUpdate(TipoDebito tipo)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            tipo.UpdateUser = user.Id;
            tipo.UpdateDate = enfoke.Time.Now;


            tipo = dalEngine.Update<TipoDebito>(tipo);
        }


        public EntityCollection<TipoDebito> TipoDebitoReadByTipo(int tipo)
        {
            return dalEngine.GetManyByProperty<TipoDebito>(TipoDebito.Properties.TipoRegistroDebito, tipo);
        }

        #endregion

        #region Comprobante

        /// <summary>
        /// [GG] Recupero los cortes por plan para una obra social
        /// </summary>
        /// <param name="obraSocialId"></param>
        /// </returns>
        public EntityCollection<ObraSocialFacturaPlan> ObraSocialFacturaPlanReadByObraSocial(int obraSocialId)
        {
            return dalEngine.GetManyByProperty<ObraSocialFacturaPlan>(ObraSocialFacturaPlan.Properties.ObraSocial.Id, obraSocialId, ObraSocialFacturaPlan.Properties.NumeroGrupo, enfoke.Data.SortOrder.Ascending);
        }

        public EntityCollection<ObraSocialFacturaPlan> ObraSocialFacturaPlanReadByObraSocial(List<int> obraSocialIds)
        {
            return dalEngine.GetManyByPropertyList<ObraSocialFacturaPlan>(ObraSocialFacturaPlan.Properties.ObraSocial.Id, obraSocialIds);
        }

        /// <summary>
        /// [GG] Recupero el corte por plan para una obra social y un plan determinados
        /// </summary>
        /// <param name="obraSocialId"></param>
        /// <returns></returns>
        public ObraSocialFacturaPlan ObraSocialFacturaPlanReadByObraSocialAndPlan(int obraSocialId, int planId)
        {
            StringBuilder hql = new StringBuilder(" from ObraSocialFacturaPlan osfp ");
            hql.Append(" where osfp.ObraSocial.Id = :obraSocialId");
            hql.Append(" AND osfp.ObraSocialPlan.Id = :planId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("obraSocialId", obraSocialId);
            query.SetParameter("planId", planId);

            return dalEngine.GetByQuery<ObraSocialFacturaPlan>(query);
        }

        /// <summary>
        /// Retorno la Cantidad de Comprobantes que hay Basados en otro (Refacturaciones)
        /// </summary>
        /// <param name="id">ID del Comprobante Original</param>
        /// <returns>Cantidad de Comprobantes Generados</returns>
        public int ComprobanteReadByOriginal(int id)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT COUNT(1) FROM comprobante WHERE com_fecha_anulacion is null and com_comprobante_padre_id = ").Append(id);

            object ret = dalEngine.Connection.ExecuteScalar(sb.ToString());
            if (ret == null)
                throw new Exception("No se pudo obtener la cantidad de comprobantes.");

            int cantComprobantes = int.Parse(ret.ToString());
            return cantComprobantes;
        }

        public EntityCollection<ControlDiarioView> ControlDiarioViewReadByComprobante(int comprobanteId)
        {
            const string hql = "SELECT cdv FROM ControlDiarioView cdv " + "WHERE cdv.ComprobanteId = :comprobanteId and cdv.ValorizacionItemId > 0 " +
                               "ORDER BY cdv.Protocolo ";

            IQuery exQuery = dalEngine.CreateQuery(hql);
            exQuery.SetInt32("comprobanteId", comprobanteId);
            EntityCollection<ControlDiarioView> cdw = dalEngine.GetManyByQuery<ControlDiarioView>(exQuery);

            if (cdw != null)
                return cdw;

            return new EntityCollection<ControlDiarioView>();
        }

        public List<DetalleComprobante> DetalleComprobanteReadByComprobante(int comprobanteId)
        {
            var query = from coi in dalEngine.Query<ComprobanteItem>()
                        join vli in dalEngine.Query<ValorizacionItem>() on coi.ValorizacionItemID equals vli.Id
                        join pt in dalEngine.Query<PracticaTurno>() on vli.PracticaTurno.Id equals pt.Id
                        join tur in dalEngine.Query<Turno>() on pt.TurnoId equals tur.Id
                        join pac in dalEngine.Query<Paciente>() on tur.Orden.PacienteId equals pac.Id
                        join equ in dalEngine.Query<Equipo>() on tur.EquipoId equals equ.Id
                        where coi.ComprobanteID == comprobanteId
                              && tur.Deleted == false
                              && vli.Valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Prefacturacion
                            //&& vli.Valorizacion.Deleted == false
                              && vli.Cantidad > 0
                        orderby tur.Orden.Protocolo.ProtocoloFull
                        select new DetalleComprobante(tur.Id, tur.FechaControlDiario, tur.Orden.Protocolo.ProtocoloFull, pac.ApellidoNombre, pt.Practica, vli.Cantidad, equ.Servicio, vli.ImporteDerechos + vli.ImporteModulo, vli.ImporteHonorarios, vli.ImporteInsumos, vli.ImporteCoseguro, tur.Orden.ObraSocialPlanId, tur.Orden.NumeroAfiliado, tur.Orden.NumeroAutorizacion, tur.Orden.LoteTraslado, tur.Orden.PosicionEnLote, tur.MotivoID);

            List<DetalleComprobante> detalles = query.ToList();

            DetalleComprobanteCargarTipoDebito(detalles);
            DetalleComprobanteCargarObraSocialPlanes(detalles);

            return detalles;
        }

        private void DetalleComprobanteCargarObraSocialPlanes(List<DetalleComprobante> detalles)
        {
            List<int> obraSocialePlanesIds = DetalleComprobante.DetalleComprobanteIdentificarObraSocialPlanIdsABuscar(detalles);

            if (obraSocialePlanesIds.Count <= 0)
                return;

            var osPlans = from osp in dalEngine.Query<ObraSocialPlan>() where obraSocialePlanesIds.Contains(osp.Id) select osp;
            Dictionary<int, ObraSocialPlan> obrasSocialesPlanes = osPlans.ToDictionary(osp => osp.Id, osp => osp);
            foreach (DetalleComprobante dc in detalles)
                dc.ObraSocialPlan = obrasSocialesPlanes[dc.OsPlanId];
        }

        private void DetalleComprobanteCargarTipoDebito(List<DetalleComprobante> detalles)
        {
            List<int> motivoNoFacturarIds = DetalleComprobante.DetalleComprobanteIdentificarIdsABuscar(detalles);
            if (motivoNoFacturarIds.Count <= 0)
                return;

            var tiposDebitos = from td in dalEngine.Query<TipoDebito>() where motivoNoFacturarIds.Contains(td.Id) select td;
            Dictionary<int, string> tipoDictionary = tiposDebitos.ToDictionary(td => td.Id, td => td.Name);
            foreach (DetalleComprobante dc in detalles)
            {
                if (dc.MotivoNoFacturarId.HasValue)
                    dc.MotivoNoFacturarName = tipoDictionary[dc.MotivoNoFacturarId.Value];
            }
        }

        public EntityCollection<ControlDiarioView> ControlDiarioViewReadByTipoEntregaAndTCF(int tipoControlFacturacion, List<int> tipoEntregaOrden)
        {
            string strTipoEntregaOrden = "";
            foreach (int teo in tipoEntregaOrden)
            {
                strTipoEntregaOrden += teo.ToString() + ",";
            }
            strTipoEntregaOrden.Remove(strTipoEntregaOrden.Length - 1);

            StringBuilder hql = new StringBuilder(" select cdv from ControlDiarioView cdv ");
            hql.Append(" where cdv.EntregaOrden in (" + strTipoEntregaOrden + ")");
            hql.Append("   AND cdv.TipoControlFacturacion = :tipoControlFacturacion ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("tipoControlFacturacion", tipoControlFacturacion);

            return dalEngine.GetManyByQuery<ControlDiarioView>(query);
        }

        public ControlDiarioView ControlDiarioViewReadByTurnoId(int turnoId)
        {
            StringBuilder hql = new StringBuilder(" from ControlDiarioView cdv ");
            hql.Append(" where cdv.Id = :turnoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("turnoId", turnoId);

            query.SetMaxResults(1);

            return dalEngine.GetByQuery<ControlDiarioView>(query);
        }

        public EntityCollection<ControlDiarioView> ControlDiarioViewReadByTurnoId(List<int> turnosId, int? comprobanteId)
        {
            StringBuilder hql = new StringBuilder(" from ControlDiarioView cdv ");
            hql.Append(" where cdv.Id IN (:turnosId)");

            if (comprobanteId.HasValue)
                hql.Append(" and cdv.ComprobanteId = :comprobanteId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnosId", turnosId);
            if (comprobanteId.HasValue)
                query.SetParameter("comprobanteId", comprobanteId);

            return dalEngine.GetManyByQuery<ControlDiarioView>(query);
        }

        public EntityCollection<ControlDiarioView> ControlDiarioViewReadByTurnoIds(List<int> turnoIds)
        {
            if (turnoIds.Count == 0)
                return new EntityCollection<ControlDiarioView>();

            StringBuilder hql = new StringBuilder(" from ControlDiarioView cdv ");
            hql.Append(" where cdv.Id in (:turnoIds) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnoIds", turnoIds);

            return dalEngine.GetManyByQuery<ControlDiarioView>(query);
        }

        [MinuteTimeout]
        public virtual EntityCollection<ControlDiarioView> ControlDiarioViewRead(string obraSocial, int plan, int? centro, string paciente, string protocolo, string lote, int? medico, string equipo, int? servicio, string practica, DateTime fechaDesde, DateTime fechaHasta, bool noControlados, bool aFacturar, bool aNoFacturar, bool facturado, bool prefacturado, bool particular, bool debitadas, bool bajaDefinitiva, bool muestraParticulares, bool? ordenFacturable)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            string strPlan = "";
            if (plan > 0)
                strPlan = ObrasSocialesDalc.ObraSocialPlanReadById(plan).Name;

            return ControlDiarioViewRead(obraSocial, strPlan, centro, paciente, protocolo, lote, medico, equipo, servicio, practica, fechaDesde, fechaHasta, noControlados, aFacturar, aNoFacturar, facturado, prefacturado, particular, debitadas, bajaDefinitiva, muestraParticulares, ordenFacturable);
        }

        [MinuteTimeout]
        public virtual EntityCollection<ControlDiarioView> ControlDiarioViewRead(string obraSocial, string plan, int? centro, string paciente, string protocolo, string lote, int? medico, string equipo, int? servicio, string practica, DateTime fechaDesde, DateTime fechaHasta, bool noControlados, bool aFacturar, bool aNoFacturar, bool facturado, bool prefacturado, bool particular, bool debitadas, bool bajaDefinitiva, bool muestraParticulares, bool? ordenFacturable)
        {
            return ControlDiarioViewRead(obraSocial, plan, centro, paciente, protocolo, lote, medico, equipo, servicio, practica, fechaDesde, fechaHasta, noControlados, aFacturar, aNoFacturar, facturado, prefacturado, particular, debitadas, bajaDefinitiva, null, null, muestraParticulares, false, false, ordenFacturable, 0);
        }

        [Timeout(600)]
        public virtual EntityCollection<ControlDiarioView> ControlDiarioWithMultilineViewReadHQL(int? obraSocialId, string plan, List<int> centros, string paciente, string protocolo, string lote, int? medico, string equipo, int? servicio, string practica, string nroAfiliado, DateTime? fechaDesde, DateTime? fechaHasta, bool noControlados, bool aFacturar, bool aNoFacturar, bool facturado, bool prefacturado, bool particular, bool debitadas, bool bajaDefinitva, int? tipoPlan, int? tipoEntregaOrden, bool muestraParticulares, bool soloAmbulatorio, bool soloInternacion, bool? ordenFacturable, int maxRows)
        {
            List<int> tipos = ObtenerTiposSeleccionados(noControlados, aFacturar, aNoFacturar, facturado, prefacturado, particular, debitadas, bajaDefinitva);
            if (tipos.Count == 0)
                return new EntityCollection<ControlDiarioView>();

            IList<int> protocolosIds = new List<int>();
            if (!string.IsNullOrEmpty(protocolo))
                protocolosIds = Context.Session.TurnosDalc.ProtocolosIdsReadByCodigo(protocolo);

            EntityCollection<ControlDiarioView> informes = GetControlDiarioViews(obraSocialId, plan, centros, paciente, lote, medico, equipo, servicio, practica, nroAfiliado, fechaDesde, fechaHasta, tipoPlan, tipoEntregaOrden, soloAmbulatorio, soloInternacion, ordenFacturable, maxRows, tipos, protocolosIds);
            AgregarEPO(informes);
            AgregarCentroControlDiario(informes);
            SortedDictionary<int, List<ControlDiarioView>> informesPorTurnoIds;
            SortedDictionary<int, ControlDiarioView> informesPorValorzacionItemIds;
            SepararPorTurnosYParPracticaOSPlanYValorizacionItem(informes, out informesPorTurnoIds, out informesPorValorzacionItemIds);
            AgregarComprobantes(informesPorValorzacionItemIds);
            EntityCollection<LoteValidacion> lotes = ObtenerLotes(informesPorTurnoIds);
            EntityCollection<ControlDiarioView> controlDiario = ContruirInformesMultiLine(informes, lotes);
            return controlDiario;
        }

        private static int? GetLoteIdFilter(string lote)
        {
            int loteId;
            LoteTraslado loteTraslado = null;
            if (!String.IsNullOrEmpty(lote) && int.TryParse(lote, out loteId))
            {
                loteTraslado = Context.Session.Dalc.GetById<LoteTraslado>(loteId);
            }
            int? loteIdFilter = (loteTraslado != null) ? loteTraslado.Id : new Nullable<int>();
            return loteIdFilter;
        }

        private EntityCollection<ControlDiarioView> GetControlDiarioViews(int? obraSocialId, string plan, List<int> centros, string paciente, string lote, int? medico, string equipo, int? servicio, string practica, string nroAfiliado, DateTime? fechaDesde, DateTime? fechaHasta, int? tipoPlan, int? tipoEntregaOrden, bool soloAmbulatorio, bool soloInternacion, bool? ordenFacturable, int maxRows, List<int> tipos, IList<int> protocolosIds)
        {
            int? loteId = GetLoteIdFilter(lote);
            IQuery query = CrearQueryGenerico(plan, centros, paciente, loteId, medico, equipo, servicio, practica, fechaDesde, fechaHasta, tipoPlan, tipoEntregaOrden, soloAmbulatorio, soloInternacion, ordenFacturable, maxRows, tipos, obraSocialId, protocolosIds, nroAfiliado/*, PLAN_PRACTICA_NULL*/);
            EntityCollection<ControlDiarioView> informes = dalEngine.GetManyByQuery<ControlDiarioView>(query);

            if (loteId.HasValue)
                SetLoteIdInInformes(loteId.Value, informes);
            else
                AgregarLoteTrasladoId(informes);

            return informes;
        }


        private void AgregarLoteTrasladoId(EntityCollection<ControlDiarioView> informes)
        {
            List<int> ordenesId = (from informe in informes select informe.OrdenId).Distinct().ToList<int>();
            if (ordenesId.Count <= 0)
                return;
            EntityCollection<KeyValuePair<int, int>> ordenesIdsLotesIds = GetOrdenesIdLoteIds(ordenesId);
            SetLoteIdsInInformesFilterByOrdenId(ordenesIdsLotesIds, informes);

            //throw new enfokeNotSupportedException();
            //int dummy;
            //if (!String.IsNullOrEmpty(lote) && int.TryParse(lote, out dummy))
            //hqlBuilder.Append("and ord.LoteTraslado.Id = :lote ");
        }

        private static void SetLoteIdInInformes(int loteId, EntityCollection<ControlDiarioView> informes)
        {
            foreach (ControlDiarioView cdv in informes)
                cdv.LoteTrasladoId = loteId;
        }

        private static void SetLoteIdsInInformesFilterByOrdenId(EntityCollection<KeyValuePair<int, int>> ordenesIdsLotesIds, EntityCollection<ControlDiarioView> informes)
        {
            foreach (KeyValuePair<int, int> ordenIdLoteId in ordenesIdsLotesIds)
            {
                EntityCollection<ControlDiarioView> informesByOrdenId = (from informe in informes
                                                                         where informe.OrdenId == ordenIdLoteId.Key
                                                                         select informe).ToEntityCollection();
                SetLoteIdInInformes(ordenIdLoteId.Value, informesByOrdenId);
            }
        }

        private EntityCollection<KeyValuePair<int, int>> GetOrdenesIdLoteIds(List<int> ordenesId)
        {
            //se hace el split por el in de mas de mil elementos de oracle
            List<List<int>> splittedOrdenes = LinqInClause.SplitIntoBucketsForOracle(ordenesId);
            EntityCollection<KeyValuePair<int, int>> ordenesIdsLotesIds = new EntityCollection<KeyValuePair<int, int>>();
            foreach (List<int> bucketOrdenes in splittedOrdenes)
            {
                EntityCollection<KeyValuePair<int, int>> bucketOrdenesIdLotesId = (from orden in dalEngine.Query<Orden>()
                                                                                   where
                                                                                        orden.LoteTraslado != null &&
                                                                                        bucketOrdenes.Contains(orden.Id)
                                                                                   select new KeyValuePair<int, int>(orden.Id, orden.LoteTraslado.Id)).ToEntityCollection();
                ordenesIdsLotesIds.AddRange(bucketOrdenesIdLotesId);
            }
            return ordenesIdsLotesIds;
        }

        private string ControlDiarioViewConstructor(bool withCodigoInterno)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append(" new enfoke.Eges.Entities.ControlDiarioView(vitem.Id, vitem.Valorizacion.Id, vitem.PracticaTurno.Practica.Name, vitem.PracticaTurno.Practica.Code, vitem.PracticaTurno.Practica.Region, ");

            if (withCodigoInterno)
            {
                hqlBuilder.Append("ppHC.CodigoInterno, ");
            }
            else
            {
                hqlBuilder.Append("'', ");
            }

            hqlBuilder.Append("vitem.PracticaTurno.Practica.Id, vitem.PracticaTurno.Practica.EsFacturable, vitem.Cantidad, vitem.ImporteDerechos, vitem.ImporteModulo, vitem.ImporteHonorarios, ");
            hqlBuilder.Append("vitem.ImporteInsumos, vitem.ImporteCoseguro, vitem.Modificado, vitem.Valorizacion.TipoPlan.Descripcion, vitem.Valorizacion.TipoPlan.Id, ");
            hqlBuilder.Append("vitem.PracticaTurno.MedicoInformante.Apellido, vitem.PracticaTurno.MedicoInformante.Name, vitem.Liquidado, vitem.PracticaTurno.Id, tur.Id, tur.CobranzaVigenteID, tur.ImporteOrdenMedica, tur.TipoControlFacturacion.Id, mot, tur.FechaControlDiario, ");
            hqlBuilder.Append("ord.NumeroAfiliado, ord.NumeroAutorizacion, ord.Protocolo.ProtocoloFull, ord.Id, ord.DebeOrdenMedicaId, lote, ord.PosicionEnLote, ord.InfoInternacion, ttur, pac.ApellidoNombre, pac.Importancia, pac.PartidoID, ");
            hqlBuilder.Append("osp.ObraSocial.Name, osp.Name, osp.ObraSocial.Id, osp.Id, osp.ObraSocial.LapsoDiasFacturacion, osp.ObraSocial.EsParticular, equ, teo) ");

            return hqlBuilder.ToString();
        }

        public virtual EntityCollection<Orden> OrdenEntregaOrdenRead(List<int> ordenIds)
        {
            if (ordenIds.Count <= 0)
                return new EntityCollection<Orden>();
            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(ordenIds);
            string ordenes = blockBuilder.BuildConstrainBlock("ord.Id");
            StringBuilder hql = new StringBuilder();
            hql.Append("Select distinct new enfoke.Eges.Entities.Orden(ord.Id, ord.dbEntregaOrdenId, ltr, ord.PosicionEnLote) from Orden as ord left join ord.LoteTraslado ltr ");
            hql.AppendFormat(" where {0} ", ordenes);
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<Orden>(query);
        }


        private IQuery CrearQueryGenerico(string plan, List<int> centros, string paciente, int? loteId, int? medico, string equipo, int? servicio, string practica, DateTime? fechaDesde, DateTime? fechaHasta, int? tipoPlan, int? tipoEntregaOrden, bool soloAmbulatorio, bool soloInternacion, bool? ordenFacturable, int maxRows, List<int> tipos, int? osId, IList<int> protocolosIds, string nroAfiliado/*, bool isPlanPracticaNull*/)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            // tur.Id, tur.CobranzaVigenteID, tur.ImporteOrdenMedica, tur.TipoControlFacturacion.Id, mot, tur.FechaControlDiario, ord
            //ord.NumeroAfiliado, ord.NumeroAutorizacion, ord.Protocolo.ProtocoloFull, ord.Id, ord.DebeOrdenMedicaId, lote, ord.PosicionEnLote, ord.InfoInternacion

            hqlBuilder.Append("select new enfoke.Eges.Entities.ControlDiarioView(vitem.Id, vitem.Valorizacion.Id, prt.Practica.Name, prt.Practica.Code, prt.Practica.Region, ");
            //if (!isPlanPracticaNull)
            //hqlBuilder.Append("ppHC.CodigoInterno, ");
            hqlBuilder.Append("vitem.CodigoFicticio, ");
            hqlBuilder.Append("prt.Practica.Id, prt.Practica.EsFacturable, vitem.Cantidad, vitem.ImporteDerechos, vitem.ImporteModulo, vitem.ImporteHonorarios, ");
            hqlBuilder.Append("vitem.ImporteInsumos, vitem.ImporteCoseguro, vitem.Modificado, vitem.Valorizacion.TipoPlan.Descripcion, vitem.Valorizacion.TipoPlan.Id, ");
            hqlBuilder.Append("prt.MedicoInformante.Apellido, prt.MedicoInformante.Name, vitem.Liquidado, prt.Id, tur.Id, tur.CobranzaVigenteID, tur.ImporteOrdenMedica, tur.TipoControlFacturacion.Id, mot, tur.FechaControlDiario, tur.CentroControlDiario, ");
            //hqlBuilder.Append("ord.NumeroAfiliado, ord.NumeroAutorizacion, ord.Protocolo.ProtocoloFull, ord.Id, ord.DebeOrdenMedicaId, lote, ord.PosicionEnLote, ord.InfoInternacion, ttur, pac.ApellidoNombre, pac.Importancia, pac.PartidoID, ");
            hqlBuilder.Append("ord.NumeroAfiliado, ord.NumeroAutorizacion, ord.Protocolo.ProtocoloFull, ord.Id, ord.DebeOrdenMedicaId, ord.PosicionEnLote, ord.InfoInternacion, ttur, pac.ApellidoNombre, pac.Importancia, pac.PartidoID, ");
            hqlBuilder.Append("osp.ObraSocial.Name, osp.Name, osp.ObraSocial.Id, osp.Id, osp.ObraSocial.LapsoDiasFacturacion, osp.ObraSocial.EsParticular, equ, teo, vitem.PlanPracticaUsadoId) ");
            hqlBuilder.Append("from ValorizacionItem vitem join vitem.Valorizacion.Turno tur join vitem.PracticaTurno prt join tur.Orden ord left join tur.MotivoNoFacturar mot, Paciente pac, ObraSocialPlan osp, Equipo equ, EstadoTurno est, TipoTurno ttur, TipoEntregaOrden teo ");
            //            if (!isPlanPracticaNull)
            //                hqlBuilder.Append(", PlanPracticaHC ppHC ");
            hqlBuilder.Append("where pac.Id = ord.PacienteId and ");
            hqlBuilder.Append("ord.ObraSocialPlanId = osp.Id and ");
            hqlBuilder.Append("tur.EquipoId = equ.Id and ");
            hqlBuilder.Append("tur.EstadoTurnoID = est.Id and ");
            hqlBuilder.Append("ord.dbEntregaOrdenId = teo.Id and ");
            hqlBuilder.Append("ttur.Id = tur.TipoTurnoId and ");
            hqlBuilder.Append("tur.Deleted = false and ");
            hqlBuilder.Append("est.Facturable = true and ");
            hqlBuilder.Append("vitem.Valorizacion.Tipo.Id = :prefacturacion and ");
            hqlBuilder.Append("vitem.Valorizacion.Deleted = false and ");
            hqlBuilder.Append("vitem.Cantidad > 0 and ");
            hqlBuilder.Append("tur.TipoControlFacturacion.Id in (:tipos) ");
            //            if (!isPlanPracticaNull)
            //                hqlBuilder.Append("and ppHC.Id = vitem.PlanPracticaUsadoId ");
            //            else
            //                hqlBuilder.Append("and vitem.PlanPracticaUsadoId is null ");

            if (loteId.HasValue)
                hqlBuilder.Append("and ord.LoteTraslado.Id = :loteId ");

            if (osId.HasValue)
                hqlBuilder.Append("and osp.ObraSocial.Id = :obraSocial ");

            if (!String.IsNullOrEmpty(plan))
                hqlBuilder.Append("and osp.Name like :plan ");

            if (!String.IsNullOrEmpty(paciente))
                hqlBuilder.Append("and pac.ApellidoNombre like :paciente ");

            if (protocolosIds.Count > 0)
                hqlBuilder.Append("and ord.Protocolo.Id IN (:protocolo) ");

            if (!String.IsNullOrEmpty(nroAfiliado))
                hqlBuilder.Append("and ord.NumeroAfiliado = :nroAfiliado ");

            if (medico.HasValue)
                hqlBuilder.Append("and prt.MedicoInformante.Id = :medico ");

            if (!String.IsNullOrEmpty(equipo))
                hqlBuilder.Append("and equ.Descripcion like :equipo ");

            if (servicio.HasValue)
                hqlBuilder.Append("and equ.Servicio.Id = :servicio ");

            if (!String.IsNullOrEmpty(practica))
                hqlBuilder.Append("and prt.Practica.Name like :practica ");

            if (centros != null && centros.Count > 0)
                hqlBuilder.Append("and equ.Sucursal.Id IN (:centros) ");

            if (tipoPlan.HasValue)
                hqlBuilder.Append("and vitem.Valorizacion.TipoPlan.Id = :tipoPlan ");

            if (tipoEntregaOrden.HasValue)
                hqlBuilder.Append("and teo.Id = :tipoEntregaOrden ");

            if (ordenFacturable.HasValue)
                hqlBuilder.Append("and teo.EsFacturable = :ordenFacturable ");

            if (fechaDesde.HasValue)
                hqlBuilder.Append("and tur.FechaControlDiario >= :fechaDesde ");

            if (fechaHasta.HasValue)
                hqlBuilder.Append("and tur.FechaControlDiario < :fechaHasta ");

            if (soloInternacion)
                hqlBuilder.Append("and vitem.Valorizacion.Turno.Orden.InfoInternacion is not null ");

            if (soloInternacion)
                hqlBuilder.Append("and ord.InfoInternacion is not null ");
            else if (soloAmbulatorio)
                hqlBuilder.Append("and ord.InfoInternacion is null ");

            //FEDEhqlBuilder.Append("order by ord.Protocolo.ProtocoloFull asc, tur.Id asc");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("tipos", tipos);
            query.SetInt32("prefacturacion", (int)ValorizacionTiposEnum.Prefacturacion);
            if (osId.HasValue)
                query.SetInt32("obraSocial", osId.Value);

            if (!String.IsNullOrEmpty(plan))
                query.SetString("plan", plan.Trim().Replace(" ", "%") + "%");

            if (!String.IsNullOrEmpty(paciente))
                query.SetString("paciente", paciente.Trim().Replace(" ", "%") + "%");

            if (protocolosIds.Count > 0)
                query.SetParameterList("protocolo", new EntityCollection<int>(protocolosIds));

            if (loteId.HasValue)
                query.SetInt32("loteId", loteId.Value);

            if (medico.HasValue)
                query.SetInt32("medico", medico.Value);

            if (!String.IsNullOrEmpty(equipo))
                query.SetString("equipo", equipo.Trim().Replace(" ", "%") + "%");

            if (!String.IsNullOrEmpty(nroAfiliado))
                query.SetString("nroAfiliado", nroAfiliado.Trim());

            if (servicio.HasValue)
                query.SetInt32("servicio", servicio.Value);

            if (!String.IsNullOrEmpty(practica))
                query.SetString("practica", practica.Trim().Replace(" ", "%") + "%");

            if (centros != null && centros.Count > 0)
                query.SetParameterList("centros", centros);

            if (tipoPlan.HasValue)
                query.SetInt32("tipoPlan", tipoPlan.Value);

            if (tipoEntregaOrden.HasValue)
                query.SetInt32("tipoEntregaOrden", tipoEntregaOrden.Value);

            if (ordenFacturable.HasValue)
                query.SetBoolean("ordenFacturable", ordenFacturable.Value);

            if (fechaDesde.HasValue)
                query.SetDateTime("fechaDesde", fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                query.SetDateTime("fechaHasta", fechaHasta.Value.Date.AddDays(1));
            query.SetMaxResults(maxRows);

            query.SetLockMode("vitem", LockMode.Read);
            query.SetLockMode("tur", LockMode.Read);
            query.SetLockMode("ord", LockMode.Read);
            query.SetLockMode("lote", LockMode.Read);
            query.SetLockMode("pac", LockMode.Read);
            query.SetLockMode("osp", LockMode.Read);
            //            query.SetLockMode("ppHC", LockMode.Read);
            query.SetLockMode("ptr", LockMode.Read);

            return query;
        }

        private static void AgregarCentroControlDiario(EntityCollection<ControlDiarioView> informes)
        {
            EntityCollection<SucursalName> sucursales = Context.Session.TurnosDalc.SucursalNameReadAll().Collection;

            foreach (ControlDiarioView item in informes)
                if (item.CentroControlDiarioId.HasValue)
                {
                    SucursalName centro = sucursales.FindByKey(item.CentroControlDiarioId.Value);
                    item.CentroControlDiarioName = centro;
                }
        }

        private static void AgregarEPO(EntityCollection<ControlDiarioView> informes)
        {
            List<int> partidoIds = new List<int>();
            foreach (ControlDiarioView item in informes)
                if (item.NumeroPartido.HasValue)
                    if (!partidoIds.Contains(item.NumeroPartido.Value))
                        partidoIds.Add(item.NumeroPartido.Value);

            EntityCollection<NumeroEpo> epos = Context.Session.TurnosDalc.NumeroEpoReadByPartidoIds(partidoIds);

            foreach (ControlDiarioView item in informes)
                if (item.NumeroPartido.HasValue)
                {
                    NumeroEpo epo = epos.FindByKey(item.NumeroPartido.Value);
                    if (epo != null)
                        item.NumeroEPO = epo.Epo;
                }
        }

        private static EntityCollection<LoteValidacion> ObtenerLotes(SortedDictionary<int, List<ControlDiarioView>> informesPorTurnoIds)
        {
            List<int> informeIds = new List<int>();
            foreach (int i in informesPorTurnoIds.Keys)
                informeIds.Add(i);

            return Context.Session.ValidadoresDalc.LoteValidacionReadLastByTurnosIds(informeIds);
        }

        private void AgregarComprobantes(SortedDictionary<int, ControlDiarioView> informesPorValorzacionItemIds)
        {
            IList<ComprobanteItem> comprobanteItems = ObtenerComprobantesPorValorizacionItems(informesPorValorzacionItemIds.Keys, informesPorValorzacionItemIds.Keys.Count);
            foreach (ComprobanteItem comprobanteItem in comprobanteItems)
                informesPorValorzacionItemIds[comprobanteItem.ValorizacionItemID].MezclarConComprobanteItem(comprobanteItem);
        }

        private IList<ComprobanteItem> ObtenerComprobantesPorValorizacionItems(IEnumerable<int> itemsIds, int totalItems)
        {
            if (totalItems == 0)
                return new List<ComprobanteItem>();

            List<int> idsVli = new List<int>();
            foreach (int i in itemsIds)
                idsVli.Add(i);

            StringBuilder hqlBuilder = new StringBuilder();
            SQLBlockBuilder<int> itemsVli = new SQLBlockBuilder<int>(idsVli);
            string vliIds = itemsVli.BuildConstrainBlock("com.ValorizacionItem.Id");
            hqlBuilder.AppendFormat("Select new enfoke.Eges.Entities.ComprobanteItem(com.Comprobante.Ano, com.Comprobante.Mes, com.Comprobante.Id, com.ValorizacionItem.Id) from ComprobanteItemHQL com where {0} ", vliIds);
            hqlBuilder.Append(" and com.Comprobante.FechaAnulacion is null ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());

            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        private static void SepararPorTurnosYParPracticaOSPlanYValorizacionItem(EntityCollection<ControlDiarioView> informes, out SortedDictionary<int, List<ControlDiarioView>> informesPorTurnoIds, out SortedDictionary<int, ControlDiarioView> informesPorValorzacionItemIds)
        {
            informesPorTurnoIds = new SortedDictionary<int, List<ControlDiarioView>>();
            informesPorValorzacionItemIds = new SortedDictionary<int, ControlDiarioView>();
            foreach (ControlDiarioView informe in informes)
            {
                List<ControlDiarioView> infs;
                if (!informesPorTurnoIds.TryGetValue(informe.Id, out infs))
                {
                    infs = new List<ControlDiarioView>();
                    informesPorTurnoIds.Add(informe.Id, infs);
                }

                infs.Add(informe);
                if (!informesPorValorzacionItemIds.ContainsKey(informe.ValorizacionItemId))
                    informesPorValorzacionItemIds.Add(informe.ValorizacionItemId, informe);
            }
        }

        private IList<PlanPracticaPrecio> PlanPracticaExigenInformes(IEnumerable<KeyValuePair<int, int>> paresPracticaPlan, int totalItems)
        {
            if (paresPracticaPlan == null || totalItems == 0)
                return new List<PlanPracticaPrecio>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.PlanPracticaPrecio(plp.Practica.Id, plp.Plan.Id, plp.ExigeInformeMarca) from PlanPracticaPrecio plp where ( ");
            int index = 0;
            foreach (KeyValuePair<int, int> clave in paresPracticaPlan)
            {
                hqlBuilder.AppendFormat(" (plp.Practica.Id = :practica{0} and plp.Plan.Id = :osp{0}) or ", index);
                index++;
            }

            //remuevo el ultimo or
            hqlBuilder.Remove(hqlBuilder.Length - 4, 4);
            hqlBuilder.Append(") and plp.Equipo is null and plp.PracticaAdicional is null and plp.FechaHasta is null and plp.Deleted = false and plp.ExigeInformeMarca = true");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            index = 0;
            foreach (KeyValuePair<int, int> clave in paresPracticaPlan)
            {
                query.SetInt32(string.Format("practica{0}", index), clave.Key);
                query.SetInt32(string.Format("osp{0}", index), clave.Value);
                index++;
            }

            return dalEngine.GetManyByQuery<PlanPracticaPrecio>(query);
        }

        private static EntityCollection<ControlDiarioView> ContruirInformesMultiLine(EntityCollection<ControlDiarioView> informes, EntityCollection<LoteValidacion> lotes)
        {
            /*reordena por turno id para poder unirlos en multiline*/
            informes.SortByProperty(ControlDiarioView.Properties.Id);

            EntityCollection<ControlDiarioView> informesMultiline = new EntityCollection<ControlDiarioView>();
            if (informes.Count == 0)
                return informesMultiline;

            EntityCollection<PracticaValidacion> validaciones = Context.Session.ValidadoresDalc.PracticaValidacionReadByLotes(lotes);
            bool exigeInforme = false;
            string practicaMultiline = string.Empty;
            string practicaCodigo = string.Empty;
            string practicaCodigoInterno = string.Empty;
            string practicaCantidad = string.Empty;
            string importeDerechos = string.Empty;
            string importeHonorarios = string.Empty;
            string importeInsumos = string.Empty;
            string importeCoseguro = string.Empty;
            string importeTotal = string.Empty;
            string importeTotalPaciente = string.Empty;
            decimal importeTotalValue = 0;
            string equi = string.Empty;
            string estado = string.Empty;
            string autorizacion = string.Empty;
            foreach (ControlDiarioView i in informes)
            {
                LoteValidacion lote = lotes.Find(delegate(LoteValidacion lval) { return lval.TurnoId == i.Id; });
                if (lote != null && lote.Aprobada)
                    i.CompletarDatosDeValidacion(lote);
            }

            for (int n = 0; n < informes.Count; n++)
            {
                ControlDiarioView i = informes[n];
                // Toma los valores de la fila....
                practicaMultiline += i.Practica.ToString() + Environment.NewLine;
                practicaCodigo += i.PracticaCodigo.ToString() + Environment.NewLine;
                if (!string.IsNullOrEmpty(i.PracticaCodigoInterno))
                    practicaCodigoInterno += i.PracticaCodigoInterno.ToString();
                practicaCodigoInterno += Environment.NewLine;
                practicaCantidad += i.PracticaCantidad.ToString() + Environment.NewLine;
                equi += i.DescripcionEquipo + Environment.NewLine;
                // Con que alguna practica exija informe, entonces el turno lo exige
                exigeInforme = exigeInforme ? exigeInforme : i.ExigeInforme;

                importeDerechos += TypeUtils.FormatValue(i.ImporteDerechos * i.PracticaCantidad, true) + Environment.NewLine;
                importeHonorarios += TypeUtils.FormatValue(i.ImporteHonorarios * i.PracticaCantidad, true) + Environment.NewLine;
                importeInsumos += TypeUtils.FormatValue(i.ImporteInsumos * i.PracticaCantidad) + Environment.NewLine;
                importeCoseguro += TypeUtils.FormatValue(i.ImporteCoseguro, true) + Environment.NewLine;
                importeTotal += TypeUtils.FormatValue(((i.ImporteDerechos + i.ImporteHonorarios + i.ImporteInsumos) * i.PracticaCantidad) - i.ImporteCoseguro, true) + Environment.NewLine;
                importeTotalValue = GetImporteTotalValue(i);

                importeTotalPaciente += TypeUtils.FormatValue(i.ImporteOrdenMedica, true) + Environment.NewLine;
                estado += (validaciones.Find(delegate(PracticaValidacion pv) { return pv.CodigoPractica.Trim() == i.PracticaCodigo.Trim() && pv.TurnoId == i.Id; }) != null ? i.Validacion : string.Empty) + Environment.NewLine;


                bool debeInsertarFila = (n == informes.Count - 1) || (informes[n].Id != informes[n + 1].Id);

                if (debeInsertarFila)
                {
                    i.Practica = practicaMultiline.Substring(0, practicaMultiline.Length - 1);
                    i.PracticaCodigo = practicaCodigo.Substring(0, practicaCodigo.Length - 1);
                    i.PracticaCodigoInterno = practicaCodigoInterno.Substring(0, practicaCodigoInterno.Length - 1);
                    i.PracticaCantidadMultiline = practicaCantidad.Substring(0, practicaCantidad.Length - 1);
                    i.DescripcionEquipo = equi.Substring(0, equi.Length - 1);
                    i.ExigeInforme = exigeInforme;
                    i.ImporteTotalValue = importeTotalValue;
                    i.ImporteDerechosMultiline = importeDerechos.Substring(0, importeDerechos.Length - 1);
                    i.ImporteHonorariosMultiline = importeHonorarios.Substring(0, importeHonorarios.Length - 1);
                    i.ImporteInsumosMultiline = importeInsumos.Substring(0, importeInsumos.Length - 1);
                    i.ImporteCoseguroMultiline = importeCoseguro.Substring(0, importeCoseguro.Length - 1);
                    i.ImporteTotalMultiline = importeTotal.Substring(0, importeTotal.Length - 1);
                    i.ImporteTotalPacienteMultiline = importeTotalPaciente.Substring(0, importeTotalPaciente.Length - 1);
                    if (!string.IsNullOrEmpty(estado))
                        i.Validacion = estado.Substring(0, estado.Length - 1);
                    if (!string.IsNullOrEmpty(autorizacion))
                        i.NroAutorizacion = autorizacion.Substring(0, autorizacion.Length - 1);
                    informesMultiline.Add(i);

                    // Limpio variables de sumarización
                    practicaMultiline = string.Empty;
                    practicaCodigo = string.Empty;
                    practicaCodigoInterno = string.Empty;
                    practicaCantidad = string.Empty;
                    importeDerechos = string.Empty;
                    importeHonorarios = string.Empty;
                    importeInsumos = string.Empty;
                    importeCoseguro = string.Empty;
                    importeTotal = string.Empty;
                    importeTotalPaciente = string.Empty;
                    estado = string.Empty;
                    equi = string.Empty;
                    autorizacion = string.Empty;
                    exigeInforme = false;
                }
            }

            informesMultiline.SortByProperty(ControlDiarioView.Properties.Protocolo);
            return informesMultiline;
        }

        private static decimal GetImporteTotalValue(ControlDiarioView cdv)
        {
            decimal importeTotal = 0;
            if (cdv.ImporteDerechos.HasValue)
                importeTotal += cdv.ImporteDerechos.Value;
            importeTotal += cdv.ImporteHonorarios;
            importeTotal += cdv.ImporteInsumos;
            importeTotal *= cdv.PracticaCantidad;
            importeTotal -= cdv.ImporteCoseguro;
            return importeTotal;
        }

        private EntityCollection<ControlDiarioView> ControlDiarioViewRead(string obraSocial, string plan, int? centroId, string paciente, string protocolo, string lote,
          int? medicoId, string equipo, int? servicioId, string practica, DateTime fechaDesde, DateTime fechaHasta, bool noControlados, bool aFacturar,
          bool aNoFacturar, bool facturado, bool prefacturado, bool particular, bool debitadas, bool bajaDefinitiva, int? tipoPlan, int? tipoEntregaOrden, bool muestraParticulares, bool soloAmbulatorio, bool soloInternacion, bool? ordenFacturable, int maxRows)
        {
            if (!noControlados && !aFacturar && !aNoFacturar &&
                !facturado && !prefacturado && !particular && !debitadas && !bajaDefinitiva)
                return new EntityCollection<ControlDiarioView>();

            ReadManyCommand<ControlDiarioView> readCmd = new ReadManyCommand<ControlDiarioView>(dalEngine);

            Filter filter = new Filter();

            if (!String.IsNullOrEmpty(obraSocial))
            {
                string OSSearch = obraSocial.Trim().Replace(" ", "%") + "%";
                filter.Add(new OpenParenthesis());
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.ObraSocial, "LIKE", OSSearch);
                filter.Add(new CloseParenthesis());
            }

            if (!String.IsNullOrEmpty(plan))
            {
                string planSearch = plan.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.ObraSocialPlan, "LIKE", planSearch);
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                string pacienteSearch = paciente.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.Paciente, "LIKE", pacienteSearch);
            }

            if (!String.IsNullOrEmpty(protocolo))
            {
                string protocoloSearch = protocolo.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.Protocolo, "LIKE", protocoloSearch);
            }

            if (!String.IsNullOrEmpty(lote))
            {
                string loteSearch = lote.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.LoteTrasladoId, "LIKE", loteSearch);
            }

            if (medicoId.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.InformanteId, "=", medicoId.Value);

            if (!String.IsNullOrEmpty(equipo))
            {
                string equipoSearch = equipo.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.DescripcionEquipo, "LIKE", equipoSearch);
            }

            if (servicioId.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.ServicioId, "=", servicioId.Value);

            if (!String.IsNullOrEmpty(practica))
            {
                string practicaSearch = practica.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.Practica, "LIKE", practicaSearch);
            }

            if (centroId.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.CentroId, " = ", centroId.Value);

            /*if (!muestraParticulares)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.EsParticular, " <> ", true);*/

            if (tipoPlan.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.TipoPlanId, " = ", tipoPlan.Value);

            if (tipoEntregaOrden.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.EntregaOrden, " = ", tipoEntregaOrden.Value);

            if (ordenFacturable.HasValue)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.EsOrdenFacturable, " = ", ordenFacturable.Value);

            filter.Add(BooleanOp.And, ControlDiarioView.Properties.FechaTurno, " >= ", fechaDesde.Date);

            filter.Add(BooleanOp.And, ControlDiarioView.Properties.FechaTurno, " < ", fechaHasta.Date.AddDays(1));

            if (soloInternacion)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.InfoInternacion, " not is ", null);
            else if (soloAmbulatorio)
                filter.Add(BooleanOp.And, ControlDiarioView.Properties.InfoInternacion, "is ", null);

            List<int> tipos = ObtenerTiposSeleccionados(noControlados, aFacturar, aNoFacturar, facturado, prefacturado, particular, debitadas, bajaDefinitiva);
            filter.Add(BooleanOp.And, ControlDiarioView.Properties.TipoControlFacturacion, " IN ", tipos);
            // filter.Add(BooleanOp.And, ControlDiarioView.Properties.EsFacturable, " = ", true);
            readCmd.Filter = filter;

            Sort sort = new Sort();

            sort.Add(ControlDiarioView.Properties.Protocolo, SortingDirection.Asc);
            sort.Add(ControlDiarioView.Properties.Id, SortingDirection.Asc);

            readCmd.Sort = sort;
            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            return readCmd.Execute();
        }

        private static List<int> ObtenerTiposSeleccionados(bool noControlados, bool aFacturar, bool aNoFacturar, bool facturado, bool prefacturado, bool particular, bool debitadas, bool bajaDefinitiva)
        {
            List<int> tipos = new List<int>();
            if (noControlados)
                tipos.Add((int)TipoControlFacturacionEnum.NoControlado);
            if (aFacturar)
                tipos.Add((int)TipoControlFacturacionEnum.AFacturar);
            if (aNoFacturar)
                tipos.Add((int)TipoControlFacturacionEnum.ANoFacturar);
            if (facturado)
            {
                tipos.Add((int)TipoControlFacturacionEnum.Facturado);
                tipos.Add((int)TipoControlFacturacionEnum.ReFacturado);
            }
            if (prefacturado)
            {
                tipos.Add((int)TipoControlFacturacionEnum.PreFacturado);
                tipos.Add((int)TipoControlFacturacionEnum.AReFacturar);
            }
            if (particular)
                tipos.Add((int)TipoControlFacturacionEnum.Particular);
            if (debitadas)
                tipos.Add((int)TipoControlFacturacionEnum.Debitado);
            if (bajaDefinitiva)
                tipos.Add((int)TipoControlFacturacionEnum.BajaDefinitiva);

            return tipos;
        }

        public string ExcluirUnProtocoloDeComprobante(ControlDiarioView cdv)
        {
            return ExcluirUnProtocoloDeComprobante(cdv.Id);
        }

        public string ExcluirUnProtocoloDeComprobante(int turnoId)
        {
            string ret = "";
            EntityCollection<Comprobante> comprobantes = ComprobantesReadByTurno(turnoId);
            if (comprobantes.Count > 0)
            {
                foreach (Comprobante comprobante in comprobantes)
                {
                    if (!comprobante.FechaAnulacion.HasValue)
                    {
                        EntityCollection<ComprobanteItemView> ItmesComprobante = ComprobanteItemViewReadByComprobanteAndTurno(comprobante.Id, turnoId);
                        ComprobanteEliminarItems(comprobante.Id, ItmesComprobante);
                        ret = comprobante.Numero.ToString();
                    }
                }
            }
            return ret;
        }

        /// <summary>
        /// Genero los Comprobantes
        /// </summary>
        /// <param name="practica">Practica para ver como crear el comprobante</param>
        /// <param name="agrupamiento">Agrupamiento a Utilizar</param>
        /// <param name="obraSocialID">Obra Social de los Comprobantes a Generar</param>
        /// <param name="gerenciadoraID">Obra Social Gerenciadora [0 = Uso obraSocialID | != 0 = Uso este ID]</param>
        /// <param name="obraSocialPlanID">Plan de la Obra Social de los Comprobantes a Generar (0 = Todos)</param>
        /// <param name="ano">Año de los Comprobantes a Generar</param>
        /// <param name="mes">Mes de los Comprobantes a Generar</param>
        /// <returns>Comprobantes Generados</returns>
        [Private]
        public virtual Comprobante ComprobantesGenerar(IPracticaLiquidableView practica, AgrupamientoEnum agrupamiento, int obraSocialID, int gerenciadoraID, int obraSocialPlanID,
                              int ano, int mes, int? facturaFormatoId, int? osFacturaPlanGrupo, int? original, int tipoPlanId)
        {
            return ComprobanteInsert(obraSocialID, obraSocialPlanID, ano, mes, practica.PorcentajeIVA, original, facturaFormatoId, osFacturaPlanGrupo, gerenciadoraID, tipoPlanId);
        }

        private Comprobante ComprobanteCrearObjeto(int obraSocialID, int obraSocialPlanID, decimal porcentajeIva, int gerenciadoraID, int? tipoPlanId, int ano, int mes, int? original, int? facturaFormatoId, int? osFacturaPlanGrupo)
        {
            Comprobante comprobante = new Comprobante();
            // Creo el Objeto Comprobante
            comprobante.TipoPlanId = tipoPlanId;
            //comprobante.Numero = numero;
            comprobante.Ano = ano;
            comprobante.Mes = mes;
            comprobante.Fecha = enfoke.Time.Now;
            comprobante.ObraSocialID = obraSocialID;
            if (gerenciadoraID > 0)
                comprobante.OsGerenciadoraId = gerenciadoraID;
            if (obraSocialPlanID > 0)
                comprobante.ObraSocialPlanID = obraSocialPlanID;
            else
                comprobante.ObraSocialPlanID = null;
            //comprobante.Monto = montoComprobante;
            comprobante.PorcentajeIVA = porcentajeIva;
            ObraSocial os = Context.Session.Dalc.GetById<ObraSocial>(obraSocialID);
            if (os.IvaPorItem)
                comprobante.TipoIVAID = (int)TipoIVAEnum.PorItem;
            else
                comprobante.TipoIVAID = (int)TipoIVAEnum.NoAplica;
            comprobante.ComprobantePadreID = original;
            if (facturaFormatoId.HasValue)
                comprobante.Faf = FacturaFormatoReadById(facturaFormatoId.Value);

            if (osFacturaPlanGrupo != null)
                comprobante.GrupoPlanId = osFacturaPlanGrupo.Value;

            return comprobante;
        }

        /// <summary>
        /// Inserto un Comprobante con sus Items
        /// </summary>
        [Private]
        public virtual Comprobante ComprobanteInsert(int obraSocial, int obraSocialPlanID, int ano, int mes, decimal porcentajeIVA, int? original, int? facturaFormatoId, int? osFacturaPlanGrupo, int? gerenciadoraID, int? tipoPlanId)
        {
            Comprobante comprobante = ComprobanteCrearObjeto(obraSocial, obraSocialPlanID, porcentajeIVA, gerenciadoraID.GetValueOrDefault(0), tipoPlanId, ano, mes, original, facturaFormatoId, osFacturaPlanGrupo);
            comprobante.Numero = ComprobanteObtenerNuevoNumero();
            
            // Ingreso el Comprobante
            return dalEngine.Update(comprobante);
        }

        [Private]
        public int ComprobanteObtenerNuevoNumero()
        {
            StringBuilder hql = new StringBuilder(" select max(comp.Numero) from Comprobante comp ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            object comNroObject = (object)query.UniqueResult();

            int numero = 0;
            if (comNroObject != null)
                numero = Convert.ToInt32(comNroObject) + 1;
            return numero;
        }

        public Comprobante UpdateMontoComprobante(int comprobanteId)
        {
            decimal monto = ComprobanteItemsImporteTotalReadByComprobante(comprobanteId);

            // Actualizo el monto del comprobante con la suma de los items
            Comprobante comprobante = dalEngine.GetById<Comprobante>(comprobanteId);
            comprobante.Monto = monto;

            return dalEngine.Update<Comprobante>(comprobante);
        }

        /// <summary>
        /// Actualizo el comprobante [ComprobanteUpdatePadre]
        /// </summary>
        /// <param name="comprobante">El ComprobanteUpdatePadre a actualizar</param>
        /// <returns>El ComprobanteUpdatePadre actualizado</returns>
        [RequiresTransaction]
        public virtual ComprobanteUpdatePadre ComprobanteUpdate(ComprobanteUpdatePadre comprobante)
        {
            // Actualizo
            comprobante = dalEngine.Update<ComprobanteUpdatePadre>(comprobante);

            // Actualizo los Items del nuevo Padre
            foreach (ComprobanteItem item in comprobante.ItemsActualizar)
                dalEngine.Update(item);

            return comprobante;
        }


        /// <summary>
        /// Busco el primer comprobanteItemDebito del comprobante de la practica del turno. Si libera => Debitado sino Facturado 
        /// </summary>
        public Dictionary<int, bool> ProcolosDebitadosEstanLiberados(List<int> turnosIdsRefacturar)
        {
            if (turnosIdsRefacturar.Count <= 0)
                return new Dictionary<int, bool>();

            string hql = " select pt from ComprobanteItemDebito cod, ComprobanteItem coi, PracticaTurno pt where cod.ComprobanteItemId = coi.Id " +
                         " and cod.TipoDebito.LiberaLiquidacion = true and pt.TurnoId IN (:turnosIdsRefacturar) " +
                         " and pt.Id = coi.PracticaTurnoID ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnosIdsRefacturar", turnosIdsRefacturar);

            EntityCollection<PracticaTurno> items = dalEngine.GetManyByQuery<PracticaTurno>(query);
            Dictionary<int, bool> result = new Dictionary<int, bool>();
            foreach (int turnoId in turnosIdsRefacturar)
            {
                if (!result.ContainsKey(turnoId))
                    result.Add(turnoId, false);

                bool encontro = false;
                foreach (PracticaTurno pt in items)
                {
                    if (turnoId == pt.TurnoId)
                    {
                        encontro = true;
                        break;
                    }

                }

                result[turnoId] = encontro;
            }

            return result;
        }

        /// <summary>
        /// Obtengo el Total Respaldado de un Comprobante
        /// [GG] Lo cambie de sql a hql
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <returns>El Total Respaldado</returns>
        public decimal ComprobanteObtenerTotalRespaldado(int comprobanteId)
        {
            var query = from nc in dalEngine.Query<Factura>() where nc.FechaAnulacion == null && nc.FacturaPadre.ComprobanteId == comprobanteId && nc.TipoTalonarioId == (int)TipoTalonarioEnum.NotaDeCredito select nc.TotalNeto;
            decimal? importe = query.Sum();
            return importe.HasValue ? importe.Value : 0;
        }

        /// <summary>
        /// Obtengo la Cantidad de Items que fueron Liquidados por Honorarios Médicos
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <returns>Cantidad de Items</returns>
        public int ComprobanteObtenerItemsLiquidadosDeHonorariosMedicos(int id)
        {
            StringBuilder hql = new StringBuilder(" from ComprobanteItem coi ");
            hql.Append(" where coi.ComprobanteID = :id");
            hql.Append("   and coi.LiquidacionHonorariosID IS NOT NULL");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("id", id);
            EntityCollection<ComprobanteItem> items = dalEngine.GetManyByQuery<ComprobanteItem>(query);

            return items.Count;

            /*
            string sql = "SELECT ISNULL(COUNT(1), 0) FROM comprobante_item WHERE coi_comprobante_id = " + id.ToString() + " AND coi_liq_id IS NOT NULL";
            return (int)dalEngine.Connection.ExecuteScalar(sql);
            */
        }

        /// <summary>
        /// Elimino Items de un Comprobante
        /// </summary>
        /// <param name="idComprobante">ID del Comprobante</param>
        /// <param name="items">Items a Eliminar</param>
        /// <param name="user">Usuario de la Operación</param>
        [RequiresTransaction]
        public virtual void ComprobanteEliminarItems(int idComprobante, EntityCollection<ComprobanteItemView> items)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            // Obtengo el Comprobante para Actualizar el Monto
            Comprobante comprobante = dalEngine.GetById<Comprobante>(idComprobante);

            // Acumulo el Monto Eliminado para Actualizar el Comprobante
            // Marco los Items de Valorizacion como No Liquidados
            // Guardo los Items a Eliminar del Comprobante
            decimal montoEliminado = 0;
            ValorizacionItemUpdateLiquidado valorizacionItem;
            EntityCollection<ComprobanteItem> itemsEliminar = new EntityCollection<ComprobanteItem>();
            foreach (ComprobanteItemView item in items)
            {

                valorizacionItem = dalEngine.GetById<ValorizacionItemUpdateLiquidado>(item.ValorizacionItemID);

                // Marco el Item como No Liquidado
                valorizacionItem.Liquidado = (int)LiquidadoEnum.NoLiquidado;

                // Actualizo
                valorizacionItem = ValorizacionesDalc.ValorizacionItemUpdate(valorizacionItem);

                // Acumulo
                montoEliminado += item.Total;

                // Guardo el Item del Comprobante para Eliminarlo
                itemsEliminar.Add(dalEngine.GetById<ComprobanteItem>(item.Id));
            }


            // Elimino los Items
            dalEngine.Delete(itemsEliminar);

            // Actualizo el monto del comprobante con la suma de sus items
            UpdateMontoComprobante(comprobante.Id);
        }

        [Private]
        public void ValorizacionItemLiquidar(EntityCollection<ComprobanteItem> comprobantesItem, LiquidadoEnum liquidado)
        {
            List<int> valorizacionItemIds = new List<int>();
            foreach (ComprobanteItem coi in comprobantesItem)
                valorizacionItemIds.Add(coi.ValorizacionItemID);

            dalEngine.UpdatePropertyBatchByIds<ValorizacionItem>(valorizacionItemIds, ValorizacionItem.Properties.Liquidado, (int)liquidado);
        }

        [Private]
        public void TurnoUpdateMotivoNoFacturar(List<int> turnosIds, int motivoNoFacturar)
        {
            dalEngine.UpdatePropertyBatchByIds<Turno>(turnosIds, Turno.Properties.MotivoNoFacturar, motivoNoFacturar);
        }

        /// <summary>
        /// Genera el ComprobanteItem de un Comprobante.
        /// </summary>
        [RequiresTransaction]
        [Private]
        public virtual ComprobanteItem ComprobanteGenerarComprobanteItem(Comprobante comprobante, IPracticaLiquidableView practica)
        {
            // Genero el nuevo Items del Comprobante
            ComprobanteItem item = new ComprobanteItem();
            if (practica.ItemLiquidado == (int)LiquidadoEnum.Liquidado && practica.Cantidad > 0)
                throw new NotLoggeableException("Un Item de Valorización ya se encuentra Liquidado.");

            item.ValorizacionItemID = practica.Id;
            item.PracticaTurnoID = practica.PracticaTurnoID;
            item.Cantidad = practica.Cantidad;
            item.Honorarios = practica.Honorarios;
            item.Derechos = practica.Derechos;
            item.Modulo = practica.Modulo;
            item.Insumos = practica.Insumos;
            item.Coseguro = practica.Coseguro;
            item.LiquidacionAplicaDescuento = practica.LiquidacionAplicaDescuento;

            // Seteo el Comprobante
            item.ComprobanteID = comprobante.Id;
            item.Comprobante = comprobante;
            // Lo Guardo en la Colección de Items a Agregar
            if (item.Cantidad > 0 && practica.PracticaFacturable.GetValueOrDefault(true))
                return item;

            return null;
        }

        /// <summary>
        /// Retorno los Comprobantes Originales Facturados para una OS [Comprobantes]
        /// </summary>
        /// <param name="idOS">Obra Social</param>
        /// <param name="periodo">Periodo de Facturacion a Buscar</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        public EntityCollection<ComprobanteView> ComprobanteReadOriginalesFacturadosByOS(PeriodoFacturacionComprobanteView periodo)
        {
            Filter filter = new Filter();

            // Comprobantes Originales
            filter.Add(ComprobanteView.Properties.ComprobantePadreID,
                " IS ",
                null);

            // Comprobantes Facturados
            filter.Add(BooleanOp.And, ComprobanteView.Properties.NumeroFactura,
                " IS NOT ",
                  null);

            // De la OS
            filter.Add(BooleanOp.And, ComprobanteView.Properties.ObraSocialID,
                "=", periodo.Os);

            // Del Periodo
            filter.Add(BooleanOp.And, ComprobanteView.Properties.Ano,
                "=", periodo.Ano.ToString());

            filter.Add(BooleanOp.And, ComprobanteView.Properties.Mes,
                "=", periodo.Mes.ToString());

            return dalEngine.GetManyByFilter<ComprobanteView>(filter);
        }

        [Private]
        public EntityCollection<Factura> FacturasReadBySendToERP(bool enviadasToERP)
        {
            return dalEngine.GetManyByProperty<Factura>(Factura.Properties.EnvioERP, enviadasToERP);
        }

        [Private]
        public void FacturasUpdateBatchEnviadasToErp(List<int> facturasIds)
        {
            dalEngine.UpdatePropertyBatchByIds<Factura>(facturasIds, Factura.Properties.EnvioERP, true);
        }

        [Private]
        public void TurnosNuevoTipoControlFacturacion(List<int> turnosIDs, TipoControlFacturacionEnum tipo)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoHQL>(turnosIDs, TurnoHQL.Properties.TipoControlFacturacionId, (int)tipo);
        }

        #endregion

        #region ComprobanteView
        /// <summary>
        /// Retorno los Comprobantes para una OS con filtros [Comprobantes] [Vista]
        /// </summary>
        /// <param name="periodo">Periodo de Facturacion a Buscar</param>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <param name="facturados">Filtro si Obtengo los Facturados o NO</param>
        /// <param name="noFacturados">Filtro si Obtengo los No Facturados o NO</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        [MinuteTimeout]
        public virtual EntityCollection<IComprobanteView> ComprobanteViewRead(PeriodoFacturacionComprobanteView periodo, string txtBusqueda, ComprobanteSearchTypeEnum tipoBusqueda, bool facturados, bool noFacturados, bool anulados, bool mostrarFacturasAnuladas, bool incluyeCalculoDebitado, bool incluyeRefacturados)
        {
            // Si no selecciono ningun Check no traigo nada
            if (facturados == false && noFacturados == false && anulados == false)
                return new EntityCollection<IComprobanteView>();

            Filter filter = new Filter();
            string search = txtBusqueda.Trim().Replace(" ", "%") + "%";

            if (!String.IsNullOrEmpty(txtBusqueda) &&
                tipoBusqueda == ComprobanteSearchTypeEnum.Comprobante)
            {
                filter.Add(BooleanOp.And, ComprobanteView.Properties.Numero,
                    "LIKE", search);
            }

            if (!incluyeRefacturados)
                filter.Add(BooleanOp.And, ComprobanteView.Properties.ComprobantePadreID, "IS", null);

            ComprobanteViewReadAddFilters(periodo.Ano, periodo.Mes, periodo.Os, filter);

            Sort sort = new Sort(new SortItem(ComprobanteView.Properties.Numero, SortingDirection.Desc));

            filter.Add(new OpenParenthesis(BooleanOp.And));

            if (anulados)
            {
                FilterItem filterAnulados = new FilterItem(BooleanOp.And, ComprobanteView.Properties.FechaAnulacion, "IS NOT", null);
                filter.Add(filterAnulados);
            }
            if (facturados)
            {
                IFilterItem parenthesis = anulados ? new OpenParenthesis(BooleanOp.Or) : new OpenParenthesis(BooleanOp.And);
                filter.Add(parenthesis);
                filter.Add(BooleanOp.And, ComprobanteView.Properties.NumeroFactura, "IS NOT", null);
                filter.Add(BooleanOp.And, ComprobanteView.Properties.FechaAnulacion, "IS", null);
                filter.Add(new CloseParenthesis());
            }
            if (noFacturados)
            {
                IFilterItem parenthesis = anulados || facturados ? new OpenParenthesis(BooleanOp.Or) : new OpenParenthesis(BooleanOp.And);
                filter.Add(parenthesis);
                filter.Add(BooleanOp.And, ComprobanteView.Properties.NumeroFactura, "IS", null);
                filter.Add(BooleanOp.And, ComprobanteView.Properties.FechaAnulacion, "IS", null);
                filter.Add(new CloseParenthesis());
            }

            filter.Add(new CloseParenthesis());
            EntityCollection<ComprobanteView> resultado = dalEngine.GetManyByFilter<ComprobanteView>(filter, sort);

            Dictionary<int, decimal> debitadoPorComprobanteId = new Dictionary<int, decimal>();
            if (incluyeCalculoDebitado)
            {
                // Me traigo las Nota de Credito de los comprobantes
                EntityCollection<Factura> notasDeCredito = NotaCreditoReadByComprobantes(resultado.GetIds());
                foreach (Factura factura in notasDeCredito)
                {
                    if (!debitadoPorComprobanteId.ContainsKey(factura.FacturaPadre.ComprobanteId.Value))
                        debitadoPorComprobanteId.Add(factura.FacturaPadre.ComprobanteId.Value, 0);
                    debitadoPorComprobanteId[factura.FacturaPadre.ComprobanteId.Value] += factura.TotalNeto.Value;
                }
            }

            EntityCollection<IComprobanteView> response = new EntityCollection<IComprobanteView>();
            foreach (ComprobanteView view in resultado)
            {
                if (!response.Contains(view))
                {
                    if (debitadoPorComprobanteId.ContainsKey(view.Id))
                        view.TotalDebitado = debitadoPorComprobanteId[view.Id];

                    response.Add(view);
                }
            }

            return response;
        }


        public EntityCollection<ComprobanteView> ComprobanteViewReadByComprobantePadre(int comprobanteId)
        {
            string hql = "select com from ComprobanteView com where com.ComprobantePadreID = :comprobanteId AND com.FechaAnulacion is null";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("comprobanteId", comprobanteId);

            return dalEngine.GetManyByQuery<ComprobanteView>(query);
        }

        private EntityCollection<Factura> NotaCreditoReadByComprobantes(List<int> comprobantesIds)
        {
            if (comprobantesIds.Count <= 0)
                return new EntityCollection<Factura>();

            StringBuilder hql = new StringBuilder(" select nc from Factura nc ");
            hql.Append(" where nc.FacturaPadre.ComprobanteId IN (:comprobantesIds) ");
            hql.Append(" and nc.FacturaPadre.FechaAnulacion IS NULL ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("comprobantesIds", comprobantesIds);

            return dalEngine.GetManyByQuery<Factura>(query);

        }

        private void ComprobanteViewReadAddFilters(int ano, int mes, int obraSocialID, Filter filter)
        {
            // Filtro por OS
            filter.Add(BooleanOp.And, ComprobanteView.Properties.ObraSocialID,
                "=", obraSocialID);

            // Armo el Filtro del Periodo
            if (ano != 0 && mes != 0)
            {
                filter.Add(BooleanOp.And, ComprobanteView.Properties.Ano,
                    "=", ano);
                filter.Add(BooleanOp.And, ComprobanteView.Properties.Mes,
                    "=", mes);
            }
        }

        /// <summary>
        /// Retorno los Comprobantes para una OS con filtros [Facturas] [Vista]
        /// </summary>
        /// <param name="periodo">Periodo de Facturacion a Buscar</param>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        [MinuteTimeout]
        public virtual EntityCollection<ComprobanteView> ComprobanteViewRead(PeriodoFacturacionFacturaView periodo, string txtBusqueda, FacturaSearchTypeEnum tipoBusqueda)
        {
            string search = txtBusqueda.Trim().Replace(" ", "%") + "%";
            Filter filter = new Filter();
            if (!String.IsNullOrEmpty(txtBusqueda))
            {
                IPropertyReference searchColumn;
                switch (tipoBusqueda)
                {
                    case FacturaSearchTypeEnum.Factura:
                        searchColumn = ComprobanteView.Properties.NumeroFactura;
                        break;
                    case FacturaSearchTypeEnum.Comprobante:
                        searchColumn = ComprobanteView.Properties.Numero;
                        break;
                    case FacturaSearchTypeEnum.MesComprobante:
                        searchColumn = ComprobanteView.Properties.Mes;
                        break;
                    case FacturaSearchTypeEnum.AnoComprobante:
                        searchColumn = ComprobanteView.Properties.Ano;
                        break;
                    case FacturaSearchTypeEnum.FacturaOriginal:
                        searchColumn = ComprobanteView.Properties.NumeroFacturaPadre;
                        break;
                    case FacturaSearchTypeEnum.ComprobanteOriginal:
                        searchColumn = ComprobanteView.Properties.NumeroPadre;
                        break;
                    default:
                        throw new Exception("Tipo de búsqueda no reconocida.");
                }
                filter.Add(BooleanOp.And, searchColumn,
                    "LIKE", search);
            }
            ComprobanteViewReadAddFilters(periodo.Ano, periodo.Mes, periodo.ObraSocialID, filter);

            filter.Add(BooleanOp.And, ComprobanteView.Properties.NumeroFactura, "IS NOT", null);

            Sort sort = new Sort(new SortItem(ComprobanteView.Properties.Numero, SortingDirection.Desc));
            return dalEngine.GetManyByFilter<ComprobanteView>(filter, sort);
        }

        public ComprobanteView ComprobanteViewReadById(int comprobanteId)
        {
            return dalEngine.GetById<ComprobanteView>(comprobanteId);
        }

        #endregion

        #region NotaCreditoView
        /// <summary>
        /// [RQ] Trae los débitos (NotaCreditoView) realizados entre rango de fechas de débito y agrupados por servicio
        /// </summary>
        /// <param name="idSer">El id del servicio para filtrar (0 para todos)</param>
        /// <param name="fechaDesde">La fecha de débito incial</param>
        /// <param name="fechaHasta">La fecha de débito final</param>
        /// <returns>Las Notas de crédito (NotaCreditoView) que apliquen según el filtro</returns>
        [MinuteTimeout]
        public virtual EntityCollection<NotaCreditoView> NotaCreditoViewRead(int idSerervicio, DateTime fechaDesde, DateTime fechaHasta)
        {
            ReadManyCommand<NotaCreditoView> readCmd = new ReadManyCommand<NotaCreditoView>(dalEngine);

            Filter filter = new Filter();

            // se crea el filtro por la fecha, quitando la hora
            filter.Add(NotaCreditoView.Properties.DebitoDate,
                ">=", fechaDesde.Date);

            filter.Add(BooleanOp.And, NotaCreditoView.Properties.DebitoDate,
                "<", fechaHasta.Date.AddDays(1));

            if (idSerervicio > 0)
            {
                filter.Add(BooleanOp.And, NotaCreditoView.Properties.ServicioID,
                    "=", idSerervicio);
            }

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(NotaCreditoView.Properties.ObraSocialID, SortingDirection.Asc);
            sort.Add(NotaCreditoView.Properties.ServicioID, SortingDirection.Asc);
            sort.Add(NotaCreditoView.Properties.TurnoDate, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }
        #endregion

        #region ComprobanteItem
        /// <summary>
        /// Retorno los Items de un Comprobante
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <returns>Los Items del Comprobante correspondiente</returns>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadByComprobante(int id)
        {
            return this.ComprobanteItemReadByComprobante(id, false);
        }

        /// <summary>
        /// [GG] Obtengo el total del comprobante sumando sus items. Esto es para poder verificar la consistencia de
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public decimal ComprobanteItemsImporteTotalReadByComprobante(int comprobanteId)
        {
            return dalEngine.Query<ComprobanteItem>().Where(coi => coi.ComprobanteID == comprobanteId).Select(coi => new { ImporteItem = (coi.Derechos + coi.Honorarios + coi.Insumos + coi.Modulo) }).ToList().Sum(item => item.ImporteItem);//.Sum(coi => (decimal?)(coi.Derechos + coi.Honorarios + coi.Insumos + coi.Modulo)) ?? 0);
        }

        /// <summary>
        /// Retorno los Items de un Comprobante
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <param name="aRefacturar">Si obtengo Solo los Items a Refacturar</param>
        /// <returns>Los Items del Comprobante correspondiente</returns>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadByComprobante(int id, bool aRefacturar)
        {
            ReadManyCommand<ComprobanteItem> readCmd = new ReadManyCommand<ComprobanteItem>(dalEngine);

            Filter filter = new Filter();

            filter.Add(ComprobanteItem.Properties.ComprobanteID, "=", id);

            readCmd.Filter = filter;

            EntityCollection<ComprobanteItem> todos = readCmd.Execute();
            EntityCollection<ComprobanteItem> result = null;

            if (aRefacturar)
            {
                // Tengo que devolver los que tienen al menos un ComprobanteItemDebito 
                EntityCollection<ComprobanteItemDebito> debitos = ComprobanteItemDebitoReadByComprobante(id, true);
                result = new EntityCollection<ComprobanteItem>();
                foreach (ComprobanteItem item in todos)
                {
                    bool encontro = false;
                    foreach (ComprobanteItemDebito itemDebito in debitos)
                    {
                        if (itemDebito.ComprobanteItemId == item.Id && itemDebito.TipoDebito.EsRefacturable /*&& !itemDebito.TipoDebito.LiberaLiquidacion*/)
                        {
                            encontro = true;
                            break;
                        }
                    }

                    if (encontro)
                        result.Add(item);
                }


            }
            else
                result = todos;

            return result;
        }

        public EntityCollection<Comprobante> ComprobanteReadByComprobantes(List<int> comprobanteIds)
        {
            string hql = "from Comprobante co " +
              " where co.Id in (:comprobanteIds) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("comprobanteIds", comprobanteIds);
            return dalEngine.GetManyByQuery<Comprobante>(query);
        }

        /// <summary>
        /// Retorno los Items de un Comprobante
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <returns>Los Items del Comprobante correspondiente</returns>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadByComprobanteRefacturar(int id)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            EntityCollection<ComprobanteItem> items = ComprobanteItemReadByComprobante(id, true);
            EntityCollection<ComprobanteItem> itemsResult = new EntityCollection<ComprobanteItem>();
            Dictionary<int, ComprobanteItemRefacturacion> cirByCoi = ObtenerRefacturacionesEnDiccionarioPorComprobanteItemId(id);
            Dictionary<int, string> servicioByEquipoId = new Dictionary<int, string>();

            for (int i = 0; i < items.Count; i++)
            {
                ComprobanteItem item = items[i];

                item.PracticaTurno = dalEngine.GetById<PracticaTurno>(item.PracticaTurnoID);

                Turno turno = TurnosDalc.TurnoReadById(item.PracticaTurno.TurnoId);
                turno.Orden.Paciente = TurnosDalc.PacienteReadById(turno.Orden.PacienteId);
                item.FechaTurno = turno.Fecha;
                item.ProtocoloFull = turno.Orden.Protocolo.ProtocoloFull;
                item.PacienteApellidoNombre = turno.Orden.Paciente.ApellidoNombre;
                item.NroAfiliado = turno.Orden.NumeroAfiliado;
                if (item.ValorizacionItem == null)
                    item.ValorizacionItem = dalEngine.GetById<ValorizacionItem>(item.ValorizacionItemID);

                if (turno.EquipoId.HasValue)
                {
                    if (!servicioByEquipoId.ContainsKey(turno.EquipoId.Value))
                        servicioByEquipoId.Add(turno.EquipoId.Value, Context.Session.EquiposDalc.ServicioReadByEquipoId(turno.EquipoId.Value).Name);
                    item.Servicio = servicioByEquipoId[turno.EquipoId.Value];
                }
                else
                    item.Servicio = String.Empty;

                // Meto el detalle de la refacturacion si ya hay)
                if (cirByCoi.ContainsKey(item.Id))
                    item.ComprobanteItemRefacturacion = cirByCoi[item.Id];

                // Me fijo que el turno es Facturado o Debitado
                if (turno.TipoControlFacturacion.Id == (int)TipoControlFacturacionEnum.Debitado)
                    itemsResult.Add(item);
            }

            // Para agregar los montos ya refacturados, traigo los comprobanteItem de comprobantes no anulados de refacturacion y sumo por practicaTurno.
            EntityCollection<ComprobanteItem> itemsRefacturados = ComprobanteItemReadRefacturadosByComprobantePadre(id);
            Dictionary<int, decimal> montoRefacturadoByPracticaTurno = new Dictionary<int, decimal>();
            foreach (ComprobanteItem item in itemsRefacturados)
            {
                if (!montoRefacturadoByPracticaTurno.ContainsKey(item.PracticaTurnoID))
                    montoRefacturadoByPracticaTurno.Add(item.PracticaTurnoID, 0);

                montoRefacturadoByPracticaTurno[item.PracticaTurnoID] += item.Total;
            }


            // Para agregar los montos debitados originalmente, traigo los comprobanteItemDebito de comprobantes no anulados y sumo montos de refacturables por ComprobanteItem.
            EntityCollection<ComprobanteItemDebito> itemsDebitados = ComprobanteItemDebitoReadByComprobante(id, true);
            Dictionary<int, decimal> montoDebitadoByComprobanteItem = new Dictionary<int, decimal>();
            foreach (ComprobanteItemDebito item in itemsDebitados)
            {
                if (!item.TipoDebito.EsRefacturable)
                    continue;

                if (!montoDebitadoByComprobanteItem.ContainsKey(item.ComprobanteItemId))
                    montoDebitadoByComprobanteItem.Add(item.ComprobanteItemId, 0);

                montoDebitadoByComprobanteItem[item.ComprobanteItemId] += item.ImporteDebito;
            }


            foreach (ComprobanteItem comprobanteItem in itemsResult)
            {
                if (montoRefacturadoByPracticaTurno.ContainsKey(comprobanteItem.PracticaTurnoID))
                    comprobanteItem.TotalRefacturado = montoRefacturadoByPracticaTurno[comprobanteItem.PracticaTurnoID];

                if (montoDebitadoByComprobanteItem.ContainsKey(comprobanteItem.Id))
                    comprobanteItem.SaldoDebitado = montoDebitadoByComprobanteItem[comprobanteItem.Id];
            }

            return itemsResult;
        }

        private Dictionary<int, ComprobanteItemRefacturacion> ObtenerRefacturacionesEnDiccionarioPorComprobanteItemId(int id)
        {
            EntityCollection<ComprobanteItemRefacturacion> cirs = ComprobanteItemRefacturacionesReadByComprobanteId(id);
            Dictionary<int, ComprobanteItemRefacturacion> cirsByCoiId = new Dictionary<int, ComprobanteItemRefacturacion>();
            foreach (ComprobanteItemRefacturacion cir in cirs)
                cirsByCoiId.Add(cir.ComprobanteItemId, cir);

            return cirsByCoiId;
        }

        private EntityCollection<ComprobanteItem> ComprobanteItemReadRefacturadosByComprobantePadre(int comprobanteId)
        {
            string hql = "select ci from ComprobanteItem ci, Comprobante com "
                        + "where com.FechaAnulacion is null and ci.ComprobanteID = com.Id "
                        + "AND com.ComprobantePadreID = :comprobanteId ";

            IQuery query = dalEngine.CreateQuery(hql);
            // Agrego el Filtro por Comprobante
            query.SetInt32("comprobanteId", comprobanteId);

            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        /// <summary>
        /// Retorno los Items de un Comprobante de un Servicio
        /// </summary>
        /// <param name="comprobante">ID del Comprobante</param>
        /// <param name="servicio">ID del Servicio</param>
        /// <param name="listado">Reporte (Para ordenamiento)</param>
        /// <returns>Los Items del Comprobante y Servicio correspondiente</returns>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadByComprobanteAndServicio(int comprobante, int servicio, ListadosEnum listado)
        {
            string hql = "select ci from ComprobanteItem ci, PracticaTurno pt, Turno t, Equipo e,  Paciente p "
                        + "where ci.PracticaTurnoID = pt.Id AND pt.TurnoId = t.Id AND t.EquipoId = e.Id AND t.Orden.PacienteId = p.Id "
                        + "AND ci.ComprobanteID = :comprobante ";
            if (servicio > 0)
                hql += "AND e.Servicio.Id = :servicio ";

            // Agrego Orden según Listado
            switch (listado)
            {
                case ListadosEnum.General:
                case ListadosEnum.ConCoseguro:
                case ListadosEnum.ConSubtotalesServicio:
                case ListadosEnum.FormatoProforma:
                case ListadosEnum.OSPersonalEscribania:
                    hql += "ORDER BY e.Servicio.Tag ASC, t.Orden.Protocolo.Numero ASC, ";
                    break;
                case ListadosEnum.Alfabetico:
                    hql += "ORDER BY p.Apellido ASC, p.Nombre ASC, ";
                    break;
                case ListadosEnum.ConDiagnosticoYOrden:
                    hql += "ORDER BY t.Orden.Protocolo.Numero ASC, ";
                    break;
                default:
                    hql += "ORDER BY ";
                    break;
            }
            hql += "pt.Id ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            // Agrego el Filtro por Comprobante
            query.SetParameter("comprobante", comprobante);
            // Agrego el Filtro por Servicio
            if (servicio > 0)
                query.SetParameter("servicio", servicio);

            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        /// <summary>
        /// [GG] Devuelvo los items de un comprobante para un tipo de practica determinado
        /// </summary>
        /// <param name="comprobanteId"></param>
        /// <param name="practicaTipo"></param>
        /// <returns></returns>
        public EntityCollection<DetalleItemComprobante> ComprobanteItemReadByComprobanteAndTipoPractica(int comprobanteId, IList<int> practicaTipo)
        {
            StringBuilder hql = new StringBuilder("Select new enfoke.Eges.Entities.Results.DetalleItemComprobante(coi.Id, coi.ComprobanteID, coi.ValorizacionItemID, coi.Derechos, coi.Honorarios, coi.Insumos, coi.Modulo, coi.PracticaTurnoID, pra.Id ,pra.Name)");
            hql.Append(" from ComprobanteItem coi, PracticaTurno pt, Practica pra ");
            hql.Append(" where coi.PracticaTurnoID = pt.Id AND pt.Practica.Id = pra.Id ");
            hql.Append(" AND pra.TipoPractica IN (");
            foreach (int tipo in practicaTipo)
            {
                hql.Append(tipo.ToString() + ",");
            }
            hql.Remove(hql.Length - 1, 1);
            hql.Append(") AND coi.ComprobanteID = :comprobanteId");
            hql.Append(" ORDER BY pra.Id");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);

            return dalEngine.GetManyByQuery<DetalleItemComprobante>(query);
        }


        [RequiresTransaction]
        public virtual void ComprobanteItemDebitoUpdateMany(EntityCollection<ComprobanteItemDebito> items)
        {
            dalEngine.UpdateCollection(items);
        }

        public virtual void ComprobanteItemDebitoUpdate(ComprobanteItemDebito item)
        {
            dalEngine.Update(item);
        }

        /// <summary>
        /// Obtengo los Items de los Comprobantes donde esta el Item de una Valorización
        /// </summary>
        /// <param name="id">Id del Item de la Valorización</param>
        /// <returns>Items de Comprobantes asociados al Item de la Valorización</returns>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadByValorizacionItem(int id)
        {
            return dalEngine.GetManyByProperty<ComprobanteItem>(ComprobanteItem.Properties.ValorizacionItemID, id);
        }

        /// <summary>
        /// Genera items lógicos para la generacion de reportes
        /// [RQ] Agregado filtro por servicio
        /// </summary>
        public EntityCollection<ComprobanteItem> ComprobanteItemReadForReport(int obraSocial, int obraSocialPlan, DateTime fechaDesde, DateTime fechaHasta, int servicioID)
        {
            //crea una coleccion
            EntityCollection<ComprobanteItem> cis = new EntityCollection<ComprobanteItem>();

            //busca los items, segun los parametros
            EntityCollection<IPracticaLiquidableView> plvs = this.PracticaLiquidableViewRead(obraSocial, false, fechaDesde, fechaHasta, obraSocialPlan, false, null, servicioID);

            //contador para ids virtuales
            int cont = 0;

            //por cada item encontrado, arma los items logicos 
            foreach (PracticaLiquidableView plv in plvs)
            {
                if (plv.Total > 0)
                {
                    //crea el item
                    ComprobanteItem ci = new ComprobanteItem();

                    ci.Id = ++cont;

                    ci.ValorizacionItemID = plv.ValorizacionItemID;
                    ci.PracticaTurnoID = plv.PracticaTurnoID;
                    ci.Cantidad = plv.Cantidad;
                    ci.Honorarios = plv.Honorarios;
                    ci.Derechos = plv.Derechos;
                    ci.Modulo = plv.Modulo;
                    ci.Insumos = plv.Insumos;
                    ci.Coseguro = plv.Coseguro;
                    ci.PorcentajeIva = plv.PorcentajeIVA;

                    cis.Add(ci);
                }
            }

            return cis;
        }

        [Private]
        public ComprobanteItem ComprobanteItemReadByComprobantePracticaTurno(int comprobante, int practicaTurno)
        {
            ReadManyCommand<ComprobanteItem> readCmd = new ReadManyCommand<ComprobanteItem>(dalEngine);

            Filter filter = new Filter();

            filter.Add(ComprobanteItem.Properties.ComprobanteID,
                "=", comprobante);

            filter.Add(BooleanOp.And, ComprobanteItem.Properties.PracticaTurnoID,
                "=", practicaTurno);

            readCmd.Filter = filter;

            EntityCollection<ComprobanteItem> items = readCmd.Execute();

            if (items.Count == 1)
                return items[0];
            else
                return null;
        }

        /// <summary>
        /// Cantidad de ComprobanteItem que tienen el campo coi_liq_aplica_descuento en true.
        /// </summary>
        /// <param name="hasta">Fecha hasta la cual se quieren buscar ComprobanteItems no liquidados.</param>
        /// <returns>Cantidad de ComprobanteItems que aplican descuento en la liquidacion de honorarios Obra Social.</returns>
        [Private]
        public int CantidadComprobanteItemQueAplicaDescuento(DateTime hasta, TipoLiquidacionHonorariosEnum tipoLiquidacionHonorario)
        {
            DateTime? fechaFiltro = null;
            if ((int)tipoLiquidacionHonorario == (int)TipoLiquidacionHonorariosEnum.ObrasSociales)
                fechaFiltro = (Context.Session.LiquidacionDalc).TraerFechaReferencia(tipoLiquidacionHonorario);

            string hql = "SELECT COUNT(DISTINCT ci.Id) " +
                         "FROM ComprobanteItemHQL ci  " +
                         "JOIN ci.Comprobante c " +
                         "JOIN ci.ValorizacionItem vi " +
                         "JOIN c.Facturas f " +
                         "WHERE c.ComprobantePadreID IS NULL " +
                         "AND f.FechaAnulacion IS NULL " +
                         "AND f.Fecha < :fecha ";

            if (fechaFiltro.HasValue)
                hql += "AND f.Fecha >= :fechaFiltro ";

            hql += "AND ci.LiquidacionHonorarios.Id IS NULL " +
            "AND ci.LiquidacionAplicaDescuento = true " +
            "AND (CASE WHEN vi.Modulado = 1 THEN vi.Cantidad * (vi.PorcentajeModulo/100) * vi.ImporteHonorarioInterno " +
            "ELSE vi.Cantidad * (vi.PorcentajeHonorarios/100) * vi.ImporteHonorarioInterno END ) > 0 ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));

            if (fechaFiltro.HasValue)
                query.SetParameter("fechaFiltro", fechaFiltro.Value.Date);


            query.SetMaxResults(1);

            Object result = query.UniqueResult<Object>();

            if (result == null)
                return 0;
            else
                return Convert.ToInt32(result);
        }

        #endregion

        #region ComprobanteItemView

        public EntityCollection<ComprobanteItemView> ComprobanteItemViewReadByParameters(int comprobanteId, DateTime fechaDesde, DateTime fechaHasta, string paciente, string nroAfiliado, string servicio, string practica, string codigoPractica, string codigoHomologado, string protocolo)
        {
            Filter filter = new Filter();
            filter.Add(ComprobanteItemView.Properties.ComprobanteID, "=", comprobanteId);
            filter.Add(BooleanOp.And, ComprobanteItemView.Properties.FechaTurno, ">=", fechaDesde.Date);
            filter.Add(BooleanOp.And, ComprobanteItemView.Properties.FechaTurno, "<", fechaHasta.Date.AddDays(1));
            if (!String.IsNullOrEmpty(paciente))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.Paciente, "LIKE", paciente + "%");
            if (!String.IsNullOrEmpty(nroAfiliado))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.NumeroAfiliado, "=", nroAfiliado);
            if (!String.IsNullOrEmpty(practica))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.Practica, "LIKE", practica + "%");
            if (!String.IsNullOrEmpty(codigoHomologado))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.CodigoFicticio, "=", codigoHomologado);
            if (!String.IsNullOrEmpty(protocolo))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.Protocolo, "=", protocolo);
            if (!String.IsNullOrEmpty(servicio))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.ServicioName, "LIKE", servicio + "%");
            if (!String.IsNullOrEmpty(codigoPractica))
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.PracticaCodigo, "=", codigoPractica);


            return dalEngine.GetManyByFilter<ComprobanteItemView>(filter);
        }

        /// <summary>
        /// Retorno los Items de un Comprobante [Vista]
        /// </summary>
        /// <param name="id">ID del Comprobante</param>
        /// <returns>Los Items del Comprobante correspondiente</returns>
        public EntityCollection<ComprobanteItemView> ComprobanteItemViewReadByComprobante(int id)
        {
            return dalEngine.GetManyByProperty<ComprobanteItemView>(ComprobanteItemView.Properties.ComprobanteID, id);
        }

        public EntityCollection<ComprobanteItemView> ComprobanteItemViewReadByComprobanteAndTurno(int Comprobanteid, int turnoId)
        {
            StringBuilder hql = new StringBuilder(" from ComprobanteItemView com ");
            hql.Append(" where com.TurnoID = :turnoId ");
            hql.Append(" AND com.ComprobanteID = :id");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("turnoId", turnoId);
            query.SetParameter("id", Comprobanteid);
            return dalEngine.GetManyByQuery<ComprobanteItemView>(query);
        }

        public EntityCollection<ComprobanteItemView> ComprobanteItemViewReadByTurno(int id)
        {
            return dalEngine.GetManyByProperty<ComprobanteItemView>(ComprobanteItemView.Properties.TurnoID, id);
        }

        public EntityCollection<Comprobante> ComprobantesReadByTurno(int turnoId)
        {
            StringBuilder hql = new StringBuilder(" select distinct com From Comprobante com, ComprobanteItemView coi ");
            hql.Append(" where com.Id = coi.ComprobanteID ");
            hql.Append(" AND coi.TurnoID = :turnoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("turnoId", turnoId);
            return dalEngine.GetManyByQuery<Comprobante>(query);
        }

        // Trae los comprobantes NO FACTURADOS
        public EntityCollection<Comprobante> ComprobantesReadByPerido(int? anio, int? mes, List<int> osIds, bool soloSinFacturar)
        {
            StringBuilder hql = new StringBuilder(" select com From Comprobante com, ObraSocial os");
            hql.Append(" where com.ObraSocialID = os.Id ");
            hql.Append("   and com.FechaAnulacion IS NULL ");

            if (anio.HasValue && mes.HasValue)
            {
                hql.Append(" and com.Ano = :anio ");
                hql.Append(" and com.Mes = :mes ");
            }

            if (osIds.Count > 0)
                hql.Append(" AND com.ObraSocialID IN (:osIds) ");

            if (soloSinFacturar)
                hql.Append(" AND com.Id NOT IN (SELECT fac.ComprobanteId FROM Factura fac WHERE fac.FechaAnulacion is   null AND fac.ComprobanteId = com.Id) ");

            hql.Append(" order by os.Name ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (anio.HasValue && mes.HasValue)
            {
                query.SetParameter("anio", anio);
                query.SetParameter("mes", mes);
            }

            if (osIds.Count > 0)
                query.SetParameterList("osIds", osIds);

            return dalEngine.GetManyByQuery<Comprobante>(query);
        }

        /// <summary>
        /// Retorno los Items de un Comprobante de un Servicio [Vista]
        /// </summary>
        /// <param name="comprobante">ID del Comprobante</param>
        /// <param name="servicio">ID del Servicio</param>
        /// <param name="listado">Reporte (Para ordenamiento)</param>
        /// <returns>Los Items del Comprobante y Servicio correspondiente</returns>
        public EntityCollection<ComprobanteItemView> ComprobanteItemViewReadByComprobanteAndServicio(int comprobante, int servicio, ListadosEnum listado)
        {
            // Agrego el Filtro por Comprobante
            Filter filter = new Filter();
            filter.Add(ComprobanteItemView.Properties.ComprobanteID,
                "=", comprobante);

            // Agrego el Filtro por Servicio
            if (servicio > 0)
            {
                filter.Add(BooleanOp.And, ComprobanteItemView.Properties.ServicioID,
                "=", servicio);
            }
            Sort sort = new Sort();
            // Agrego Orden según Listado
            switch (listado)
            {
                case ListadosEnum.General:
                case ListadosEnum.ConCoseguro:
                case ListadosEnum.ConSubtotalesServicio:
                case ListadosEnum.FormatoProforma:
                case ListadosEnum.OSPersonalEscribania:
                    sort.Add(ComprobanteItemView.Properties.Protocolo);
                    break;
                case ListadosEnum.Alfabetico:
                    sort.Add(ComprobanteItemView.Properties.Paciente);
                    sort.Add(ComprobanteItemView.Properties.ProtocoloNumero);
                    break;
                case ListadosEnum.ConDiagnosticoYOrden:
                    sort.Add(ComprobanteItemView.Properties.ProtocoloNumero);
                    break;
            }
            sort.Add(ComprobanteItemView.Properties.PracticaTurnoID);

            return dalEngine.GetManyByFilter<ComprobanteItemView>(filter, sort);
        }
        #endregion

        #region PracticaLiquidable
        /// <summary>
        /// Retorno todas las Practicas Liquidables con los Filtros Dados
        /// [RQ] Se agregó un filtro por servicio
        /// </summary>
        /// <param name="obraSocial">Obra Social de las Practicas</param>
        /// <param name="esGerenciadora">Marca si el ID es de una Gerenciadora [Busco las Practicas de todas las Gerenciadas]</param>
        /// <param name="fechaDesde">Fecha Desde para buscar Practicas</param>
        /// <param name="fechaHasta">Fecha Hasta para buscar Practicas</param>
        /// <param name="obraSocialPlan">Plan de la Obra Social de las Practicas [Puede ser nulo]</param>
        /// <param name="soloAFacturar">Solo las que son a facturar</param>
        /// <param name="porcentajeIVA">Si es Distinto de Nulo, traigo solo las Prácticas con ese Porcentaje de IVA</param>
        /// <param name="servicioID">Id de servicio por el cual filtrar las Prácticas</param>
        /// <returns>Las Practicas Liquidables que apliquen al filtro</returns>
        private EntityCollection<IPracticaLiquidableView> PracticaLiquidableViewRead(int obraSocial, bool esGerenciadora, DateTime fechaDesde, DateTime fechaHasta, int obraSocialPlan, bool soloAFacturar, decimal? porcentajeIVA, int? servicioID)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            // Esta vista funciona bien si recibe el ObraSocialPlan. Para poder aprovechar eso,
            // si recibe sólo la obra social o la gerenciadora-filtro trae todos los obrasocialplan
            // para poder ir recuperando uno a uno las grupos de prácticas a devolver..
            List<int> planes = new List<int>();
            if (obraSocialPlan > 0)
                planes.Add(obraSocialPlan);
            else
            {
                // Los trae....
                // Si es gerenciadora, tiene que resolver eso antes...
                List<int> obrasSociales = new List<int>();
                if (!esGerenciadora)
                    obrasSociales.Add(obraSocial);
                else
                    foreach (ObraSocial os in ObrasSocialesDalc.ObraSocialReadByGerenciadoraId(obraSocial))
                        obrasSociales.Add(os.Id);

                // Ahora trae los planes de todas...
                foreach (int obraSocialId in obrasSociales)
                    foreach (ObraSocialPlan osp in ObrasSocialesDalc.ObraSocialPlanReadByObraSocial(obraSocialId, false))
                        planes.Add(osp.Id);
            }

            EntityCollection<PracticaLiquidableView> PLVs = new EntityCollection<PracticaLiquidableView>();

            Filter filter = new Filter();
            // Armo el Filtro del Periodo de Fechas
            filter.Add(BooleanOp.And,
                PracticaLiquidableView.Properties.FechaTurno,
                ">=", fechaDesde.Date);
            filter.Add(BooleanOp.And,
                PracticaLiquidableView.Properties.FechaTurno,
                "<", fechaHasta.AddDays(1).Date);

            // Solo trae las que son "a facturar"
            if (soloAFacturar)
            {
                filter.Add(BooleanOp.And,
                    PracticaLiquidableView.Properties.TipoControlFacturacionID,
                    "=", (int)TipoControlFacturacionEnum.AFacturar);
            }

            // Armo el Filtro del Porcentaje de IVA
            if (porcentajeIVA.GetValueOrDefault(-1) > -1)
            {
                filter.Add(BooleanOp.And,
                    PracticaLiquidableView.Properties.PorcentajeIVA,
                    "=", porcentajeIVA.Value);
            }

            // [RQ] Armo el Filtro por servicio
            if (servicioID.HasValue)
            {
                filter.Add(BooleanOp.And,
                    PracticaLiquidableView.Properties.ServicioID,
                    "=", servicioID.Value);
            }

            // Armo el Filtro del Plan de la Obra Social
            FilterItem filterObraSocialPlan = filter.Add(BooleanOp.And,
                PracticaLiquidableView.Properties.ObraSocialPlan,
                "=", 0);

            // Usa la vista con filtro fijo
            ReadManyCommand<PracticaLiquidablePlFechaView> viewCmd = new ReadManyCommand<PracticaLiquidablePlFechaView>(dalEngine);
            viewCmd.Filter = filter;
            viewCmd.UseLoopJoins = true;
            EntityCollection<IPracticaLiquidableView> retorno = new EntityCollection<IPracticaLiquidableView>();
            foreach (int plan in planes)
            {
                filterObraSocialPlan.Value = plan;
                // Traigo todas las PLVs
                EntityCollection<PracticaLiquidablePlFechaView> resultados = viewCmd.Execute();
                // Me quedo solo con las que tiene Algo a Pagar por la OS
                // Estos filtros se hacen desde código porque sino la vista se vuelve
                // demasiado compleja y Sql Server optimiza mal la consulta.
                foreach (PracticaLiquidablePlFechaView plv in resultados)
                    if (plv.Total > 0 &&
                            plv.Liquidado == (int)LiquidadoEnum.NoLiquidado)
                        retorno.Add((IPracticaLiquidableView)plv);
            }

            // Ordeno por Protocolo
            retorno.Sort(new Comparison<IPracticaLiquidableView>(delegate(IPracticaLiquidableView left, IPracticaLiquidableView right)
            {
                int res = left.Protocolo.CompareTo(right.Protocolo);
                if (res != 0)
                    return res;
                else
                    return left.PracticaTurnoID.CompareTo(right.PracticaTurnoID);
            }));

            return retorno;
        }
        /*
        public bool CumpleCentro(int formatoFacturaId, int centroId)
        {
            // Me fijo si existe            
            if (FacturaFormatoSucursalReadByFacturaFormatoAndCentro(formatoFacturaId, centroId).Count > 0)
                return true;

            // Si no existe, verifico que no sea justo para este centro que no existe
            string hql = "SELECT max(ffs.Id) FROM FacturaFormatoSucursal ffs WHERE ffs.Deleted = false and ffs.FafId = :formatoFacturaId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("formatoFacturaId", formatoFacturaId);
            
            return (query.UniqueResult() == null);
            
        }*/

        public EntityCollection<PracticaLiquidableView> PracticaLiquidableViewReadByTurno(int turnoId)
        {
            return dalEngine.GetManyByProperty<PracticaLiquidableView>(PracticaLiquidableView.Properties.TurId, turnoId);
        }

        public EntityCollection<PracticaLiquidableView> PracticaLiquidableViewReadByTurno(List<int> turnoId)
        {
            EntityCollection<PracticaLiquidableView> ret = new EntityCollection<PracticaLiquidableView>();

            StringBuilder hql = new StringBuilder(" select plv from PracticaLiquidableView plv, PracticaTurno prt ");
            hql.Append(" where plv.PracticaTurnoID = prt.Id ");
            hql.Append(" and plv.TurId IN (:turnosId) ");
            hql.Append(" order by plv.TurId, prt.Tipo");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnosId", turnoId);

            ret = dalEngine.GetManyByQuery<PracticaLiquidableView>(query);

            return ret;
        }

        [MinuteTimeout]
        public virtual EntityCollection<IPracticaLiquidableView> PracticaLiquidableViewRead(int obraSocial, bool esGerenciadora, DateTime fechaDesde, DateTime fechaHasta, int obraSocialPlan, bool soloAFacturar, decimal? porcentajeIVA)
        {
            return this.PracticaLiquidableViewRead(obraSocial, esGerenciadora, fechaDesde, fechaHasta, obraSocialPlan, soloAFacturar, porcentajeIVA, null);
        }

        #endregion

        #region TipoRegistroDebito


















        #endregion

        #region MotivoRegistroDebito



















        /// <summary>
        /// Retorno todos los MotivoRegistroDebito de un Tipo
        /// </summary>
        /// <param name="tipo">ID del MotivoRegistroDebito</param>
        /// <returns>Todos los MotivoRegistroDebito de un Tipo</returns>
        public EntityCollection<MotivoRegistroDebito> MotivoRegistroDebitoReadByTipo(int tipo)
        {
            return dalEngine.GetManyByProperty<MotivoRegistroDebito>(MotivoRegistroDebito.Properties.TipoRegistroDebito, tipo);
        }
        #endregion

        #region RegistroDebito
        /// <summary>
        /// Inserto un Registro del Débito
        /// </summary>
        /// <param name="registro">RegistroDebito</param>
        /// <param name="operacion">Marca de 'A', 'M' o 'B'</param>
        /// <param name="user">Usuario de la Operación</param>
        [RequiresTransaction]
        public virtual void RegistroDebitoUpdate(RegistroDebito registro, char operacion)
        {
            // Audito
            if (operacion == 'B')
                Audit.AuditDelete(registro, Security.Current.UserInfo.User.Id);

            // Ingreso el RegistroDebito
            dalEngine.Update<RegistroDebito>(registro);
        }











        /// <summary>
        /// Retorno todos los RegistroDebito
        /// </summary>
        /// <returns>Todos los RegistroDebito</returns>
        public EntityCollection<RegistroDebito> RegistroDebitoReadAll()
        {
            return dalEngine.GetManyByProperty<RegistroDebito>(RegistroDebito.Properties.Deleted, 0);
        }

        /// <summary>
        /// Retorno los RegistroDebito con filtros
        /// </summary>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <returns>Los RegistroDebito que apliquen</returns>
        public EntityCollection<RegistroDebitoView> RegistroDebitoRead(string txtBusqueda, RegistroDebitoSearchTypeEnum tipoBusqueda)
        {
            string search = txtBusqueda.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<RegistroDebitoView> readCmd = new ReadManyCommand<RegistroDebitoView>(dalEngine);

            if (!String.IsNullOrEmpty(txtBusqueda.Trim()))
            {
                Filter filter = new Filter();

                switch (tipoBusqueda)
                {
                    case RegistroDebitoSearchTypeEnum.Protocolo:
                        filter.Add(BooleanOp.And, RegistroDebitoView.Properties.Protocolo,
                            "LIKE", search);

                        break;
                    case RegistroDebitoSearchTypeEnum.Motivo:
                        filter.Add(BooleanOp.And, RegistroDebitoView.Properties.MotivoRegistroDebito,
                            "LIKE", search);

                        break;
                    case RegistroDebitoSearchTypeEnum.UsuarioError:
                        filter.Add(BooleanOp.And, RegistroDebitoView.Properties.UsuarioError,
                            "LIKE", search);

                        break;
                    case RegistroDebitoSearchTypeEnum.UsuarioCorreccion:
                        filter.Add(BooleanOp.And, RegistroDebitoView.Properties.UsuarioCorreccion,
                            "LIKE", search);

                        break;
                }

                readCmd.Filter = filter;
            }

            Sort sort = new Sort();
            sort.Add(RegistroDebitoView.Properties.Id, SortingDirection.Asc);

            return readCmd.Execute();
        }
        #endregion

        #region Factura

        [AnonymousMethod]
        public EntityCollection<FacturaPaginaEscaneo> FacturaPaginaEscaneoReadByFacturaId(int facturaId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" from FacturaPaginaEscaneo fes where fes.FacturaId = :facturaId ORDER BY fes.PaginaEscaneo.NroEscaneo, fes.PaginaEscaneo.LadoEscaneo, fes.Id ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaId", facturaId);
            return dalEngine.GetManyByQuery<FacturaPaginaEscaneo>(query);
        }

        public FacturaPaginaEscaneo FacturaPaginaEscaneoInsertar(FacturaPaginaEscaneo facturaPaginaEscaneo)
        {
            if (facturaPaginaEscaneo.PaginaEscaneo != null && facturaPaginaEscaneo.PaginaEscaneo.Id <= 0)
                facturaPaginaEscaneo.PaginaEscaneo = dalEngine.Update(facturaPaginaEscaneo.PaginaEscaneo);

            return dalEngine.Update<FacturaPaginaEscaneo>(facturaPaginaEscaneo);
        }


        [AnonymousMethod]
        public EntityCollection<FacturaPaginaEscaneo> FacturaPaginaEscaneoReadByFacturaIds(List<int> facturaIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select oes ");
            hql.Append(" from FacturaPaginaEscaneo fpe ");
            hql.Append(" where fpe.FacturaId IN (:facturaIds) ORDER BY fpe.PaginaEscaneo.NroEscaneo, fpe.PaginaEscaneo.LadoEscaneo, fpe.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("facturaIds", facturaIds);
            return dalEngine.GetManyByQuery<FacturaPaginaEscaneo>(query);
        }



        public int FacturaPaginaEscaneoReadMaxNroEscaneoByFactura(int facturaID)
        {
            int resultado = 0;

            StringBuilder hql = new StringBuilder(" Select max(fpe.PaginaEscaneo.NroEscaneo) from FacturaPaginaEscaneo fpe ");
            hql.Append(" where fpe.FacturaId = :facturaID ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaID", facturaID);

            object maxNroEscaneo = query.UniqueResult();

            if (maxNroEscaneo != null)
                resultado = (int)maxNroEscaneo;

            return resultado;
        }


        public int FacturaReadMaximoNumeroInterno()
        {
            StringBuilder hql = new StringBuilder(" select max(fac.NroInterno) from Factura fac ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetLockMode("lock", global::NHibernate.LockMode.Read);
            object maxId = query.UniqueResult();

            if (maxId != null)
                return (int)maxId;
            else
                return 0;
        }

        public EntityCollection<ComprobanteItemDebito> ComprobanteItemDebitoReadByNotaCreditoId(int notaCreditoId)
        {
            // Obtengo los ComprobanteItemDebito creados para el comprobante
            StringBuilder hql = new StringBuilder();
            hql.Append("select cid from ComprobanteItemDebito cid, Factura nc WHERE cid.NotaCreditoId = nc.Id AND nc.Deleted = false AND nc.FechaAnulacion is NULL AND nc.Id = :notaCreditoId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("notaCreditoId", notaCreditoId);
            return dalEngine.GetManyByQuery<ComprobanteItemDebito>(query);
        }

        public EntityCollection<ComprobanteItemDebito> ComprobanteItemDebitoReadByComprobante(int comprobanteId, bool withNC)
        {
            // Obtengo los ComprobanteItemDebito creados para el comprobante
            StringBuilder hql = new StringBuilder();
            if (withNC)
                hql.Append("select cid from ComprobanteItemDebito cid, ComprobanteItem coi, Factura nc WHERE cid.ComprobanteItemId = coi.Id AND cid.NotaCreditoId = nc.Id AND nc.Deleted = false AND nc.FechaAnulacion is NULL AND coi.ComprobanteID = :comprobanteID ");
            else
                hql.Append("select cid from ComprobanteItemDebito cid, ComprobanteItem coi WHERE cid.ComprobanteItemId = coi.Id AND coi.ComprobanteID = :comprobanteID ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteID", comprobanteId);
            return dalEngine.GetManyByQuery<ComprobanteItemDebito>(query);
        }

        public EntityCollection<ComprobanteItemDebito> ComprobanteItemDebitoReadByTurno(int turnoId)
        {
            // Obtengo los ComprobanteItemDebito creados para el comprobante
            StringBuilder hql = new StringBuilder();
            hql.Append("select cid from ComprobanteItemDebito cid, ComprobanteItem coi, PracticaTurno prt, Factura nc ");
            hql.Append("WHERE cid.ComprobanteItemId = coi.Id AND cid.NotaCreditoId = nc.Id and prt.Id = coi.PracticaTurnoID and prt.TurnoId = :turnoId ");
            hql.Append("AND nc.Deleted = false AND nc.FechaAnulacion is NULL  ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("turnoId", turnoId);
            return dalEngine.GetManyByQuery<ComprobanteItemDebito>(query);
        }


        /// <summary>
        /// Devuelve los turnos (Protocolos) que se van a facturar. Si el segundo parametro es true se buscaran aquellos turnos que no requieren informe para facturar
        /// y aquellos que requieren informe y lo tiene. Si es false se devolveran aquellos turnos que requieran informe para facturar y no lo tengan.
        /// </summary>
        /// <param name="comprobanteId"></param>
        /// <param name="verificaProtocoloNoInformado"></param>
        /// <returns></returns>
        public EntityCollection<DatosProtocolosEnFacturacion> ProtocolosANoFacturarReadByComprobante(int comprobanteId)
        {
            InformesDalc InformesDalc = Context.Session.InformesDalc;
            
            // Me traigo solo los turnos que tienen informes NO informados. Hago el query 
            // partido porque aca me trae pocos registros y asi el 2do query con acceso a plp tarda menos
            var query = from coi in dalEngine.Query<ComprobanteItem>()
                        join prt in dalEngine.Query<PracticaTurno>() on coi.PracticaTurnoID equals prt.Id
                        join tui in dalEngine.Query<TurnoInforme>() on prt.TurnoId equals tui.TurnoID
                        where coi.ComprobanteID == comprobanteId
                              && !tui.EstadoInforme.InformeInformado
                        select prt.Id;
            
            List<int> practicaTurnoNoInformados = query.ToList();

            if(practicaTurnoNoInformados.Count <= 0)
                return new EntityCollection<DatosProtocolosEnFacturacion>();

            //se hace el split por el in de mas de mil elementos de oracle
            List<List<int>> prtIdsMenor1000 = LinqInClause.SplitIntoBucketsForOracle(practicaTurnoNoInformados);
            List<DatosProtocolosEnFacturacion> respuestas = new List<DatosProtocolosEnFacturacion>();
            foreach (List<int> prtIds in prtIdsMenor1000)
            {
                List<DatosProtocolosEnFacturacion> items = (from vli in dalEngine.Query<ValorizacionItem>()
                        join plp in dalEngine.Query<PlanPracticaPrecio>() on vli.PlanPracticaUsadoId equals plp.Id
                        join tur in dalEngine.Query<Turno>() on vli.PracticaTurno.TurnoId equals tur.Id
                        where plp.ExigeInformeMarca && prtIds.Contains(vli.PracticaTurno.Id)
                                                            select new DatosProtocolosEnFacturacion(vli.PracticaTurno.TurnoId, tur.Orden.Protocolo.ProtocoloFull, tur.Orden.Id)).ToList();
                respuestas.AddRange(items);
            }

            return EliminarItemsDuplicados(respuestas);
        }

        private static EntityCollection<DatosProtocolosEnFacturacion> EliminarItemsDuplicados(List<DatosProtocolosEnFacturacion> respuestas)
        {
            EntityCollection<DatosProtocolosEnFacturacion> ret = new EntityCollection<DatosProtocolosEnFacturacion>();
            foreach (DatosProtocolosEnFacturacion datosProtocolosEnFacturacion in respuestas)
            {
                if(!ret.Contains(datosProtocolosEnFacturacion))
                    ret.Add(datosProtocolosEnFacturacion);
            }
                
            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="comprobanteId"></param>
        /// <param name="verificaProtocoloNoInformado"></param>
        /// <param name="turnosAExcluir">Lista de turnos que no se van a tener en cuenta al momento de realizar la factura</param>
        /// <returns></returns>
        public List<int> TurnosIdAFacturarReadByComprobante(int comprobanteId)
        {
            return (from coi in dalEngine.Query<ComprobanteItem>()
                    join prt in dalEngine.Query<PracticaTurno>() on coi.PracticaTurnoID equals prt.Id
                    where coi.ComprobanteID == comprobanteId
                    select prt.TurnoId).Distinct().ToList();
        }

        public Factura FacturaReadByComprobante(int comprobanteId)
        {
            // Obtengo la factura vigente en base al comprobante
            StringBuilder hql = new StringBuilder();
            hql.Append("from Factura fac ");
            hql.Append("where fac.ComprobanteId = :comprobanteId and fac.FechaAnulacion is null");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            query.SetMaxResults(1);
            return dalEngine.GetByQuery<Factura>(query);
        }

        public Factura FacturaReadByComprobanteRemito(int comprobanteId, int remitoId)
        {
            // Obtengo la factura vigente en base al comprobante
            StringBuilder hql = new StringBuilder();

            hql.Append("from Factura fac ");
            hql.Append("where fac.ComprobanteId = :comprobanteId and fac.FechaAnulacion is null and (fac.Deleted = 0 Or fac.Deleted is null)");

            if (remitoId > 0)
                hql.Append(" and fac.RemitoId = :remitoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("comprobanteId", comprobanteId);

            if (remitoId > 0)
                query.SetParameter("remitoId", remitoId);

            EntityCollection<Factura> facturas = dalEngine.GetManyByQuery<Factura>(query);

            return facturas[0];

        }

        public EntityCollection<ErpConcepto> ErpConceptosReadTipoDebitoClaseERP()
        {
            // Obtengo la factura vigente en base al comprobante
            StringBuilder hql = new StringBuilder();

            hql.Append("from ErpConcepto erc where erc.Clase = :claseERP and erc.TipoConcepto = :tipoDebito ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetString("claseERP", ErpConcepto.Erp);
            query.SetInt32("tipoDebito", (int)TipoErpConceptoEnum.Debito);

            return dalEngine.GetManyByQuery<ErpConcepto>(query);
        }


        [RequiresTransaction]
        public virtual Factura FacturaUpdate(Factura factura)
        {
            if (String.IsNullOrEmpty(factura.Domicilio))
                throw new NotLoggeableException("El domicilio es inválido. Verifique los datos del cliente.");

            if (String.IsNullOrEmpty(factura.Cuit))
                throw new NotLoggeableException("El CUIT es inválido. Verifique los datos del cliente.");

            Factura facturaNueva = dalEngine.Update(factura);

            if (factura.FacturaDetalle.Count > 0)
            {
                foreach (FacturaDetalle fd in factura.FacturaDetalle)
                {
                    fd.Factura = factura;
                    dalEngine.Update<FacturaDetalle>(fd);
                }
            }

            return facturaNueva;
        }

        public EntityCollection<Factura> FacturaReadByFilters(DateTime? fechaDesde, DateTime? fechaHasta, int? empresaEmisora, string razonSocial, string cuit, int tipoTalonario, string numeroFactura, bool mostrarAnuladas)
        {
            int numeroTalonario = 0;
            int numeroFiscal = 0;
            string tipoFactura = String.Empty;

            if (!String.IsNullOrEmpty(numeroFactura))
            {
                bool numeroFacturaInvalido = false;

                numeroFactura = numeroFactura.Replace("-", "");

                if (numeroFactura.Length < 8)
                {
                    if (!Int32.TryParse(numeroFactura, out numeroTalonario))
                        numeroFacturaInvalido = true;
                }
                else
                {
                    if (!Int32.TryParse(numeroFactura.Substring(numeroFactura.Length - 8, 8), out numeroTalonario))
                        numeroFacturaInvalido = true;

                    if (numeroFactura.Length <= 13)
                    {
                        if (!Int32.TryParse(numeroFactura.PadLeft(12, '0').Substring(numeroFactura.Length < 13 ? 0 : 1, 4), out numeroFiscal))
                            numeroFacturaInvalido = true;

                        if (numeroFactura.Length == 13)
                            tipoFactura = numeroFactura.Substring(0, 1);
                    }
                    else
                    {
                        if (numeroFactura.Length > 13)
                            numeroFacturaInvalido = true;
                    }
                }

                if (numeroFacturaInvalido)
                    throw new NotLoggeableException("El número de factura ingresado es inválido");
            }

            string hql = "from Factura fact where fact.TipoTalonarioId = :tipoTalonarioId ";

            if (fechaDesde.HasValue)
                hql += " and fact.Fecha >= :fechaDesde";
            if (fechaHasta.HasValue)
                hql += " and fact.Fecha <= :fechaHasta ";
            if (empresaEmisora.HasValue)
                hql += " and fact.EmpresaEmisora = :empresaEmisora ";
            if (!String.IsNullOrEmpty(razonSocial))
                hql += " and fact.RazonSocial = :razonSocial ";
            if (!String.IsNullOrEmpty(cuit))
                hql += " and fact.Cuit = :cuit ";
            if (numeroTalonario != 0)
                hql += " and fact.NumeroTalonario = :numeroTalonario ";
            if (numeroFiscal != 0)
                hql += " and fact.NumeroFiscal = :numeroFiscal ";
            if (!String.IsNullOrEmpty(tipoFactura))
                hql += " and fact.TipoFactura = :tipoFactura ";
            if (!mostrarAnuladas)
                hql += " and (fact.Deleted is null or fact.Deleted = 0)";

            IQuery query = dalEngine.CreateQuery(hql);


            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value);
            query.SetParameter("tipoTalonarioId", tipoTalonario);
            if (empresaEmisora.HasValue)
                query.SetParameter("empresaEmisora", empresaEmisora);
            if (!String.IsNullOrEmpty(razonSocial))
                query.SetParameter("razonSocial", razonSocial);
            if (!String.IsNullOrEmpty(cuit))
                query.SetParameter("cuit", cuit);
            if (numeroTalonario != 0)
                query.SetParameter("numeroTalonario", numeroTalonario);
            if (numeroFiscal != 0)
                query.SetParameter("numeroFiscal", numeroFiscal);
            if (!String.IsNullOrEmpty(tipoFactura))
                query.SetParameter("tipoFactura", tipoFactura);

            return dalEngine.GetManyByQuery<Factura>(query);

        }

        public EntityCollection<Factura> NotaDeCreditoReadByComprobantesId(List<int> comprobantesIds, bool soloNoAnuladas)
        {
            if (comprobantesIds.Count <= 0)
                return new EntityCollection<Factura>();

            var query = from nc in dalEngine.Query<Factura>()
                        join fac in dalEngine.Query<Factura>() on nc.FacturaPadre.Id equals fac.Id
                        where comprobantesIds.Contains(fac.ComprobanteId.Value) && nc.Deleted == false && fac.Deleted == false
                        && (!soloNoAnuladas || nc.FechaAnulacion == null)
                        select nc;

            return query.ToEntityCollection();
        }

        public EntityCollection<Factura> NotaDeCreditoReadByFacturaOriginal(int facturaOriginalID)
        {
            IQuery query = dalEngine.CreateQuery("FROM Factura f WHERE f.FacturaPadre = :facturaPadre AND f.FechaAnulacion is null AND (f.Deleted is null or f.Deleted = 0)");
            query.SetInt32("facturaPadre", facturaOriginalID);
            return dalEngine.GetManyByQuery<Factura>(query);
        }

        public decimal NotaCreditoCSubTotalByFacturaOriginalId(int facturaOriginalID)
        {
            decimal subtotal = 0;
            EntityCollection<Factura> NCs = this.NotaDeCreditoReadByFacturaOriginal(facturaOriginalID);
            foreach (Factura nc in NCs)
                subtotal += FacturaSubTotalById(nc.Id);

            return subtotal;
        }

        public decimal FacturaSubTotalById(int facturaId)
        {
            EntityCollection<FacturaDetalle> detalles = FacturaDetalleReadByFactura(facturaId);
            decimal subtotal = 0;
            foreach (FacturaDetalle fd in detalles)
                subtotal += fd.PrecioUnitario * fd.Cantidad;

            return subtotal;
        }

        public EntityCollection<Factura> FacturaReadByNumero(int tipoTalonarioId, string tipoFactura, int numeroFiscal, int numeroTalonario)
        {
            IQuery query = dalEngine.CreateQuery("FROM Factura f WHERE f.TipoFactura = :tipoFactura AND f.NumeroFiscal = :numeroFiscal AND f.NumeroTalonario = :numeroTalonario AND f.TipoTalonarioId = :TipoTalonarioId ");
            query.SetParameter("tipoFactura", tipoFactura);
            query.SetInt32("numeroFiscal", numeroFiscal);
            query.SetInt32("numeroTalonario", numeroTalonario);
            query.SetInt32("TipoTalonarioId", tipoTalonarioId);
            return dalEngine.GetManyByQuery<Factura>(query);
        }

        public Factura FacturaUnicaReadByNumero(string tipoFactura, int numeroFiscal, int numeroTalonario)
        {
            IQuery query = dalEngine.CreateQuery("FROM Factura f WHERE f.TipoFactura = :tipoFactura AND f.NumeroFiscal = :numeroFiscal AND f.NumeroTalonario = :numeroTalonario");
            query.SetParameter("tipoFactura", tipoFactura);
            query.SetInt32("numeroFiscal", numeroFiscal);
            query.SetInt32("numeroTalonario", numeroTalonario);
            return dalEngine.GetByQuery<Factura>(query);
        }

        /// <summary>
        /// [RQ] Retorno los Comprobantes para un formato de exportación y un Periodo [Facturas]
        /// </summary>
        /// <param name="idFormato">Formato</param>
        /// <param name="periodo">Mes y año del Período</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        public EntityCollection<Comprobante> FacturaReadByFormato(int idFormato, DateTime periodo)
        {
            return this.FacturaReadByFormato(idFormato, periodo.Month, periodo.Year);
        }

        public EntityCollection<Comprobante> ComprobanteYFacturasReadByFormato(int idFormato, DateTime periodo)
        {
            return ComprobanteYFacturasReadByFormato(idFormato, periodo.Month, periodo.Year);
        }


        public EntityCollection<Comprobante> ComprobanteYFacturasReadByFormato(int idFormato, int mes, int ano)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            EntityCollection<Comprobante> comprobantes = new EntityCollection<Comprobante>();

            comprobantes.AddRange(FacturaReadByFormato(idFormato, mes, ano));

            String hql = "Select c FROM Comprobante c, ObraSocialTipoExportacion ot " +
            "WHERE c.ObraSocialID = ot.ObraSocialId " +
            "AND ot.TipoExportacion.Id = :idFormato " +
            "AND c.FechaAnulacion is null " +
            "AND c.DeleteDate is null " +
            "AND c.Mes = :mes " +
            "AND c.Ano = :ano ";

            if (comprobantes.Count > 0)
                hql += "AND c.Id not in (:cids) ";

            hql += "ORDER BY c.Numero DESC";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetInt32("idFormato", idFormato);
            query.SetInt32("mes", mes);
            query.SetInt32("ano", ano);

            if (comprobantes.Count > 0)
                query.SetParameterList("cids", comprobantes.GetIds());

            comprobantes.AddRange(dalEngine.GetManyByQuery<Comprobante>(query));

            foreach (Comprobante comprobante in comprobantes)
            {
                if (comprobante.ObraSocial == null)
                    comprobante.ObraSocial = dalEngine.GetById<ObraSocial>(comprobante.ObraSocialID);
            }

            return comprobantes;
        }


        /// <summary>
        /// [RQ] Retorno los Comprobantes para un formato de exportación y un Periodo [Facturas]
        /// </summary>
        /// <param name="idFormato">Formato</param>
        /// <param name="mes">Mes del Período</param>
        /// <param name="ano">Año del Período</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        public EntityCollection<Comprobante> FacturaReadByFormato(int idFormato, int mes, int ano)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            IQuery query = dalEngine.CreateQuery(
                "Select c FROM Comprobante c, ObraSocialTipoExportacion ot, Factura fac " +
                "WHERE c.ObraSocialID = ot.ObraSocialId AND c.Id = fac.ComprobanteId AND fac.FechaAnulacion is NULL " +
                "AND ot.TipoExportacion.Id = :idFormato " +
                "AND c.FechaAnulacion is null " +
                "AND c.DeleteDate is null " +
                "AND c.Mes = :mes " +
                "AND c.Ano = :ano " +

                "ORDER BY c.Numero DESC");

            query.SetInt32("idFormato", idFormato);
            query.SetInt32("mes", mes);
            query.SetInt32("ano", ano);

            EntityCollection<Comprobante> comprobantes = dalEngine.GetManyByQuery<Comprobante>(query);
            foreach (Comprobante comprobante in comprobantes)
            {
                if (comprobante.ObraSocial == null)
                    comprobante.ObraSocial = dalEngine.GetById<ObraSocial>(comprobante.ObraSocialID);

                comprobante.FacturaAsociada = FacturaReadByComprobante(comprobante.Id);
            }

            return comprobantes;
        }

        public Comprobante ComprobanteRead(int mes, int ano, int? gerenciadoraId, int? obraSocialId, int? planId, int? numeroGrupoOsfp, int? comprobanteOriginal, int tipoPlan, int? facturaFormatoId)
        {

            StringBuilder hql = new StringBuilder(" select com from Comprobante com ");
            hql.Append(" where com.FechaAnulacion IS NULL ");
            hql.Append("   AND com.Id NOT in (SELECT fac.ComprobanteId FROM Factura fac WHERE fac.FechaAnulacion is null and fac.ComprobanteId = com.Id) ");
            hql.Append("   and com.Mes = :mes");
            hql.Append("   and com.Ano = :ano");
            hql.Append("   and com.TipoPlanId  = :tipoPlan");

            if (gerenciadoraId.HasValue)
                hql.Append(" and com.OsGerenciadoraId = :gerenciadora");

            if (obraSocialId.HasValue)
                hql.Append(" and com.ObraSocialID = :obraSocial");
            else
                hql.Append(" and com.ObraSocialPlanID IS NULL");

            if (planId.HasValue)
                hql.Append(" and com.ObraSocialPlanID = :obraSocialPlan");
            else
                hql.Append(" and com.ObraSocialPlanID IS NULL");

            //MARCELO. Cambiar el nombre del campo para poner el grupo de planes
            if (numeroGrupoOsfp.HasValue)
                hql.Append(" and com.GrupoPlanId  = :osGrupoPlanId");

            if (comprobanteOriginal.HasValue)
                hql.Append(" and com.ComprobantePadreID = :comprobanteOriginal");

            if (facturaFormatoId.HasValue)
                hql.Append(" and com.Faf = :facturaFormatoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("mes", mes);
            query.SetParameter("ano", ano);
            query.SetParameter("tipoPlan", tipoPlan);

            if (gerenciadoraId.HasValue)
                query.SetParameter("gerenciadora", gerenciadoraId.Value);
            if (obraSocialId.HasValue)
                query.SetParameter("obraSocial", obraSocialId.Value);
            if (planId.HasValue)
                query.SetParameter("obraSocialPlan", planId.Value);
            if (numeroGrupoOsfp.HasValue)
                query.SetParameter("osGrupoPlanId", numeroGrupoOsfp.Value);
            if (comprobanteOriginal.HasValue)
                query.SetParameter("comprobanteOriginal", comprobanteOriginal.Value);
            if (facturaFormatoId.HasValue)
                query.SetParameter("facturaFormatoId", facturaFormatoId.Value);
            
            query.SetMaxResults(1);

            return dalEngine.GetByQuery<Comprobante>(query);
        }

        public Comprobante ComprobanteReadByTurnoDebitado(int turnoId)
        {
            StringBuilder hql = new StringBuilder("Select distinct com from Comprobante com, ComprobanteItem coi, ComprobanteItemDebito cod, PracticaTurno pt ");
            hql.Append(" where com.Id = coi.ComprobanteID ");
            hql.Append(" AND coi.Id = cod.ComprobanteItemId ");
            hql.Append(" AND com.FechaAnulacion IS NULL");
            hql.Append(" AND pt.Id = coi.PracticaTurnoID ");
            hql.Append(" AND pt.TurnoId = :turnoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("turnoId", turnoId);

            EntityCollection<Comprobante> comprobantes = dalEngine.GetManyByQuery<Comprobante>(query);

            if (comprobantes.Count > 1)
                throw new Exception("Solo se debería encontrar un comprobante.");
            else if (comprobantes.Count == 1)
                return comprobantes[0];
            else
                return null;
        }

        public Comprobante ComprobanteNoAnuladoReadByValorizacionItem(int valorizacionItemId)
        {
            StringBuilder hql = new StringBuilder("Select com from Comprobante com, ComprobanteItem coi");
            hql.Append(" where com.Id = coi.ComprobanteID ");
            hql.Append(" AND coi.ValorizacionItemID = :valorizacionItemId");
            hql.Append(" AND com.FechaAnulacion IS NULL");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("valorizacionItemId", valorizacionItemId);

            EntityCollection<Comprobante> comprobantes = dalEngine.GetManyByQuery<Comprobante>(query);

            if (comprobantes.Count > 0)
                return comprobantes[0];

            return new Comprobante();
        }

        /// <summary>
        /// Retorno los Comprobantes para una OS y un Periodo [Facturas]
        /// </summary>
        /// <param name="idOS">Obra Social </param>
        /// <param name="mes">Mes del Período</param>
        /// <param name="ano">Año del Período</param>
        /// <returns>Los Comprobantes que apliquen</returns>
        public EntityCollection<Comprobante> ComprobanteRead(int idOS, int mes, int ano)
        {
            StringBuilder hql = new StringBuilder("Select com from Comprobante com, Factura fac ");
            hql.Append(" where com.ObraSocialID = :idOS ");
            hql.Append(" AND com.Id = fac.ComprobanteId AND fac.FechaAnulacion is null ");
            hql.Append(" AND com.Mes = :mes and com.Ano = :ano ");
            hql.Append(" ORDER BY com.Numero ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("idOS", idOS);
            query.SetParameter("mes", mes);
            query.SetParameter("ano", ano);

            return dalEngine.GetManyByQuery<Comprobante>(query);
        }

        #endregion

        #region ItemFactura
        /// <summary>
        /// Retorno los Items de Factura para una factura
        /// </summary>
        /// <param name="id">ID de la factura</param>
        /// <returns>Los Items de la Factura</returns>
        [Private]
        public EntityCollection<ItemFacturaView> ItemFacturaViewReadByFacturaId(int idFactura)
        {
            return dalEngine.GetManyByProperty<ItemFacturaView>(ItemFacturaView.Properties.Id, idFactura);
        }

        #endregion

        #region CondicionIVA










        public EntityCollection<CondicionIVA> CondicionIVAParaCajaReadAll()
        {
            return dalEngine.GetManyByProperty<CondicionIVA>(CondicionIVA.Properties.SoloFacturacion, false);
        }

        /// <summary>
        /// Retorno todas las CondicionIVA
        /// </summary>
        /// <returns>Todas las CondicionIVA</returns>
        public EntityCollection<CondicionIVA> CondicionIVAReadAll()
        {
            return dalEngine.GetAll<CondicionIVA>(CondicionIVA.Properties.Name);
        }
        #endregion

        #region Remito
        /// <summary>
        /// Retorno un Remito de Factura
        /// </summary>
        /// <param name="id">ID del Remito</param>
        /// <returns>El Remitocorrespondiente</returns>
        public Remito RemitoReadByID(int id)
        {
            return dalEngine.GetById<Remito>(id);
        }

        public EntityCollection<Remito> RemitoReadByFilters(string usuario, string nroFactura, DateTime? fecha)
        {
            StringBuilder hql = new StringBuilder();

            hql.Append("SELECT distinct rem ");
            hql.Append("FROM Remito rem, Factura fact ");
            hql.Append("WHERE fact.RemitoId is not null and rem.Id = fact.RemitoId ");

            if (!string.IsNullOrEmpty(usuario))
            {
                string nameComparison = SQLPortable.StringConcat("rem.UsuarioCreador.LastName", " ");
                nameComparison = SQLPortable.StringConcat(nameComparison, "rem.UsuarioCreador.FirstName");
                hql.Append("and " + nameComparison + " like :usuario ");
            }
            string tipoFactura = string.Empty;
            int numeroFiscal = 0; int numeroTalonario = 0;

            if (!string.IsNullOrEmpty(nroFactura))
            {
                tipoFactura = Factura.GetTipoFactura(nroFactura);
                numeroFiscal = Factura.GetNumeroFiscal(nroFactura);
                numeroTalonario = Factura.GetNumeroTalonario(nroFactura);
                hql.Append("and fact.NumeroFiscal is not null and fact.NumeroFiscal = :numeroFiscal ");
                hql.Append("and fact.NumeroTalonario is not null and fact.NumeroTalonario = :numeroTalonario ");
                hql.Append("and fact.TipoFactura = :tipoFactura ");
            }

            if (fecha.HasValue)
                hql.Append("and rem.FechaEntregaFacturas = :fechaEntrega ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (!string.IsNullOrEmpty(usuario))
                query.SetParameter("usuario", usuario + "%");

            if (!string.IsNullOrEmpty(nroFactura))
            {
                query.SetString("tipoFactura", tipoFactura);
                query.SetInt32("numeroFiscal", numeroFiscal);
                query.SetInt32("numeroTalonario", numeroTalonario);
            }

            if (fecha.HasValue)
                query.SetDateTime("fechaEntrega", fecha.Value.Date);

            EntityCollection<Remito> remitos = dalEngine.GetManyByQuery<Remito>(query);
            remitos.SortByProperty(Remito.Properties.Numero);
            return remitos;
        }


        /// <summary>
        /// Retorno todos los remitos
        /// </summary>
        /// <returns>Todos los remitos</returns>
        public EntityCollection<Remito> RemitoReadAll()
        {
            ReadManyCommand<Remito> readCmd = new ReadManyCommand<Remito>(dalEngine);
            return readCmd.Execute();
        }

        /// <summary>
        /// Retorno los Comprobantes para un remito dado
        /// </summary>
        /// <param name="idRemito">Remito</param>
        /// <returns>Los comprobantes encontrados</returns>
        public EntityCollection<Comprobante> ComprobantesReadByRemito(int idRemito, bool loadOS)
        {
            string hql = "select com from Comprobante com, Factura fac where fac.FechaAnulacion is null AND fac.ComprobanteId = com.Id and fac.RemitoId = :remitoId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("remitoId", idRemito);

            EntityCollection<Comprobante> cptes = dalEngine.GetManyByQuery<Comprobante>(query);

            if (loadOS)
            {
                foreach (Comprobante cpte in cptes)
                {
                    cpte.Remito = new Remito { Id = idRemito };
                }
                CargarObjetos(cptes);
            }

            return cptes;
        }

        private void CargarObjetos(EntityCollection<Comprobante> cptes)
        {
            ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;

            foreach (Comprobante cpte in cptes)
            {
                cpte.ObraSocial = Context.Session.Dalc.GetById<ObraSocial>(cpte.ObraSocialID);
                cpte.FacturaAsociada = FacturaReadByComprobanteRemito(cpte.Id, (cpte.Remito != null) ? cpte.Remito.Id : 0);
            }
        }

        /// <summary>
        /// Retorno todos comprobantes no remitidas, para el rango dado - Aplica sobre la Fecha de la Factura
        /// </summary>
        /// <param name="fechaDesde">Fecha Desde para buscar Practicas</param>
        /// <param name="fechaHasta">Fecha Hasta para buscar Practicas</param>
        /// <returns>Los comprobantes que apliquen al filtro</returns>
        public EntityCollection<Comprobante> ComprobantesARemitirReadByFecha(DateTime fechaDesde, DateTime fechaHasta)
        {
            IQuery query = dalEngine.CreateQuery("select c from Comprobante c, Factura f where f.ComprobanteId = c.Id and f.FechaAnulacion is null "
                // No remitidas
                        + " AND f.RemitoId IS NULL "
                // Armo el Filtro del Periodo de Fechas
                        + " AND f.Fecha >= :fechaDesde "
                        + " AND f.Fecha < :fechaHasta "
                        + " ORDER BY c.Numero ASC");
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));

            EntityCollection<Comprobante> cptes = dalEngine.GetManyByQuery<Comprobante>(query);
            // Obtengo la OS de cada Comprobante
            CargarObjetos(cptes);

            return cptes;
        }

        /// <summary>
        /// Genera el remito
        /// </summary>
        /// <param name="seleccionados">Colección con los comprobantes seleccionados</param>
        /// <returns>Remito generado</returns>
        [RequiresTransaction]
        public virtual Remito RemitoGenerar(EntityCollection<Comprobante> cptesSeleccionados, SecurityUser usuario, DateTime fechaEntregaFacturas)
        {
            Remito rem = new Remito();

            //obtiene el siguiente numero
            string sql = "SELECT MAX(rem_numero) FROM remito";
            object ret = dalEngine.Connection.ExecuteScalar(sql);

            int numero;
            if (ret == null || ret == DBNull.Value)
                numero = 1;
            else
                numero = 1 + Convert.ToInt32(ret);

            //crea el remito
            rem.Numero = numero;
            rem.FechaCreacion = enfoke.Time.Now;
            rem.Importe = GetRemitoImporte(cptesSeleccionados);
            rem.UsuarioCreador = usuario;
            rem.FechaEntregaFacturas = fechaEntregaFacturas;

            dalEngine.Update<Remito>(rem);
            UpdateFacturasRemitoId(rem.Id, cptesSeleccionados.GetIds());

            return rem;
        }

        private void UpdateFacturasRemitoId(int remitoId, IEnumerable<int> comprobantesId)
        {
            Filter filter = new Filter();
            filter.Add(Factura.Properties.ComprobanteId, " in ", comprobantesId);
            EntityCollection<Factura> facturas = dalEngine.GetManyByFilter<Factura>(filter);

            foreach (Factura factura in facturas)
                factura.RemitoId = remitoId;

            dalEngine.UpdateCollection(facturas);
        }

        private decimal GetRemitoImporte(EntityCollection<Comprobante> cptesSeleccionados)
        {
            List<int> cptesIds = cptesSeleccionados.GetIds();
            //[MDS]
            //--> la implementacion anterior, por alguna razon, buscaba los comprobantes de memoria (en vez de utilizar los que tiene por parámetro
            //--> la mantengo ya que no se el por qué de este comportamiento
            EntityCollection<Comprobante> comprobantesRefrescados = dalEngine.GetManyByIds<Comprobante>(cptesIds);

            decimal importe = 0;
            foreach (Comprobante cpte in comprobantesRefrescados)
            {
                Comprobante comprobante = Context.Session.Dalc.GetById<Comprobante>(cpte.Id);
                //suma el importe con iva incluido
                importe += comprobante.Monto + comprobante.Monto * (comprobante.PorcentajeIVA / 100);
            }
            return importe;
        }

        /// <summary>
        /// Anula el remito
        /// </summary>
        /// <param name="remitoId">Remito a anular</param>
        /// <returns></returns>
        [RequiresTransaction]
        public virtual void RemitoAnular(int remitoId)
        {
            // Limpio los remitos de los comprobantes
            EntityCollection<Factura> colFacturas = FacturaReadByRemito(remitoId);

            for (int i = 0; i < colFacturas.Count; i++)
            {
                Factura factura = colFacturas[i];

                // Blanqueo el Remito
                factura.RemitoId = null;

                // Actualizo
                Context.Session.Dalc.Update<Factura>(factura);
            }

            // Anulo el Remito
            Remito remito = RemitoReadByID(remitoId);
            remito.FechaAnulacion = enfoke.Time.Now;

            // Actualizo
            Context.Session.Dalc.Update<Remito>(remito);
        }

        private EntityCollection<Factura> FacturaReadByRemito(int remitoId)
        {
            return dalEngine.GetManyByProperty<Factura>(Factura.Properties.RemitoId, remitoId);
        }
        #endregion
        #region ComprobantesVenta

        #region ErpITEM

        public EntityCollection<ErpItem> ErpItemReadByFilters(int erpConcepto, int? servicioId, int? especialidadId, string cuentaContable)
        {
            var erpItems = dalEngine.Query<ErpItem>().Where(erpItem => erpItem.Erc.Id == erpConcepto);

            if (servicioId.HasValue)
                erpItems = erpItems.Where(erpItem => erpItem.Ser.Id == servicioId.Value);

            if (especialidadId.HasValue)
                erpItems = erpItems.Where(erpItem => erpItem.ServicioEspecialidad.Id == especialidadId.Value);

            if (!String.IsNullOrWhiteSpace(cuentaContable))
                erpItems = erpItems.Where(erpItem => erpItem.CuentaContable.Contains(cuentaContable));

            EntityCollection<ErpItem> items = erpItems.Select(erpItem => erpItem).ToEntityCollection();

            // Si me viene con servicio, le sumo los erp de especialdiad del servicio (no lo hago todo de una porque no anda el LEFT JOIN)
            if (servicioId.HasValue)
            {
                var erpItems2 = dalEngine.Query<ErpItem>().Where(erpItem => erpItem.Erc.Id == erpConcepto);
                erpItems2 = erpItems2.Where(erpItem => erpItem.ServicioEspecialidad.Servicio.Id == servicioId.Value);

                if (!String.IsNullOrWhiteSpace(cuentaContable))
                    erpItems = erpItems.Where(erpItem => erpItem.CuentaContable.Contains(cuentaContable));

                EntityCollection<ErpItem> items2 = erpItems2.Select(erpItem => erpItem).ToEntityCollection();
                items.AddRange(items2);
            }

            return items;
        }

        public EntityCollection<ErpItem> ErpItemBuscarPorParametrosExactos(int erpConcepto, int? sucursalId, int? servicioId, int? especialidadId)
        {
            var erpItems = dalEngine.Query<ErpItem>().Where(erpItem => erpItem.Erc.Id == erpConcepto);

            if (servicioId.HasValue)
                erpItems = erpItems.Where(erpItem => erpItem.Ser.Id == servicioId.Value && erpItem.ServicioEspecialidad == null);
            else if (especialidadId.HasValue)
                erpItems = erpItems.Where(erpItem => erpItem.Ser == null && erpItem.ServicioEspecialidad.Id == especialidadId.Value);
            else
                erpItems = erpItems.Where(erpItem => erpItem.ServicioEspecialidad == null && erpItem.Ser == null);

            if (sucursalId.HasValue)
                erpItems = erpItems.Where(erpItem => erpItem.SucId == sucursalId);
            else
                erpItems = erpItems.Where(erpItem => erpItem.SucId == null);

            return erpItems.Select(erpItem => erpItem).ToEntityCollection();
        }


        /// <summary>
        /// [GG] Devueve un Item de ERP_ITEM por ID
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        [Private]
        public ErpItem ErpItemReadById(int id)
        {
            // Se toma la libertad de cachearlo por thread
            ErpItem ret = EntityThreadCache<ErpItem>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<ErpItem>(id);
                if (ret != null)
                    EntityThreadCache<ErpItem>.SetItem(id, ret);
            }
            return ret;
        }

        #endregion

        #region ErpConcepto

        public EntityCollection<ErpConcepto> ErpConceptoReadByFilters(string descripcion, string clase)
        {
            string hql = "select erp from ErpConcepto erp ";
            string condition = String.Empty;

            if (!String.IsNullOrEmpty(descripcion))
                condition += " AND erp.Descripcion LIKE :descripcion";
            if (!String.IsNullOrEmpty(clase))
                condition += " AND erp.Clase LIKE :clase";

            if (!String.IsNullOrEmpty(condition))
                hql += "WHERE " + condition.Substring(4);

            IQuery query = dalEngine.CreateQuery(hql);
            if (!String.IsNullOrEmpty(descripcion))
                query.SetParameter("descripcion", descripcion.Insert(descripcion.Length - 1, "%"));
            if (!String.IsNullOrEmpty(clase))
                query.SetParameter("clase", clase);

            EntityCollection<ErpConcepto> erps = dalEngine.GetManyByQuery<ErpConcepto>(query);
            List<int> ids = new List<int>();
            foreach (ErpConcepto erpConcepto in erps)
                ids.Add(erpConcepto.Id);

            // Busco por cada item cuandl es su ERP Codigo DEFAULT (sin sucursal ni servicio)
            if (ids.Count > 0)
            {
                hql = "from ErpItem erp WHERE erp.Erc.Id IN (:ids) AND erp.SucId is null and erp.Ser is null";
                query = dalEngine.CreateQuery(hql);
                query.SetParameterList("ids", ids);
                EntityCollection<ErpItem> items = dalEngine.GetManyByQuery<ErpItem>(query);

                Predicate<ErpConcepto> predicate = null;

                foreach (ErpItem item in items)
                {
                    // Predicate para buscar por Id
                    predicate = delegate(ErpConcepto compare)
                    {
                        return (compare.Id == item.Erc.Id);
                    };

                    erps.Find(predicate).CodigoERPDefault = item.CodProducto;
                    erps.Find(predicate).CuentaContable = item.CuentaContable;
                }
            }
            return erps;
        }

        public void ErpConceptoDelete(int erpConceptoId)
        {
            ErpConcepto concepto = dalEngine.GetById<ErpConcepto>(erpConceptoId);
            concepto.Deleted = true;
            concepto.DeleteDate = enfoke.Time.Now;
            concepto.DeleteUser = Security.Current.UserInfo.User.Id;
            dalEngine.Update<ErpConcepto>(concepto);
        }

        public EntityCollection<ErpConcepto> ErpConceptoReadByTipo(string clase)
        {
            Filter filterConceptoNoEliminado = new Filter();

            filterConceptoNoEliminado.Add(new OpenParenthesis());
            filterConceptoNoEliminado.Add(ErpConcepto.Properties.Deleted, "=", false);
            filterConceptoNoEliminado.Add(BooleanOp.Or, ErpConcepto.Properties.Deleted, "is", null);
            filterConceptoNoEliminado.Add(new CloseParenthesis());

            filterConceptoNoEliminado.Add(BooleanOp.And, ErpConcepto.Properties.Clase, "=", clase);

            return dalEngine.GetManyByFilter<ErpConcepto>(filterConceptoNoEliminado);
        }

        public ErpItem ErpConceptoManualUpdate(ErpItem item)
        {
            item.Erc.Clase = ErpConcepto.Manual;
            item.Erc = dalEngine.Update(item.Erc);

            item.Ser = null;
            item.SucId = null;
            item = dalEngine.Update(item);

            return item;
        }

        private ErpItem ErpItemReadByErpConceptoManual(int erpId)
        {
            string hql = " from ErpItem erp where erp.Erc.Id = :erpId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("erpId", erpId);
            return dalEngine.GetByQuery<ErpItem>(query);
        }

        public ErpItem ErpItemReadByErpConcepto(int erpConceptoId)
        {
            var query = from erp in dalEngine.Query<ErpItem>()
                        where erp.Erc.Id == erpConceptoId
                        select erp;

            return query.First();
        }

        /// <summary>
        /// Devuelve el primer ERPItem que encuentre relacionado con el Concepto.
        /// </summary>
        /// <param name="idERPConcepto">Id del concepto sobre el cual se quiere traer UN solo ERPItem.</param>
        /// <returns>ERPItem asociado al Concepto.</returns>
        [Private]
        public ErpItem ErpItemReadByERPConceptoId(int idERPConcepto)
        {
            ReadManyCommand<ErpItem> readCmd = new ReadManyCommand<ErpItem>(dalEngine);

            readCmd.Filter = new Filter(new FilterItem(ErpItem.Properties.Erc.Id, " = ", idERPConcepto));

            readCmd.MaxResults = 1;

            EntityCollection<ErpItem> col = readCmd.Execute();
            if (col.Count > 0)
                return col[0];

            return null;
        }

        #endregion

        [RequiresTransaction]
        [Private]
        public virtual FacturasContadoSend2ERPResult FormularioMarcarEnviadoErp(EntityCollection<Formulario> formularios, IList<int> formulariosId, Caja cajaAjustarSaldo, FacturasContadoSend2ERPResult result)
        {
            CajaDalc CajaDalc = Context.Session.CajaDalc;

            EntityCollection<Formulario> formus = new EntityCollection<Formulario>();

            if (formularios != null && formularios.Count > 0)
            {

                foreach (Formulario form in formularios)
                {
                    if (formulariosId.Contains(form.Id))
                    {
                        form.EnvioERP = true;
                        formus.Add(form);

                        //TOTAL DE FACTURAS Y AJUSTES
                        result.CantFacturas++;

                        decimal imp = Decimal.Round(form.ImporteNeto + form.ImporteIVA, 2, MidpointRounding.AwayFromZero);
                        //si es NC invierte
                        if (form.TipoFormularioID == (int)TipoFormularioEnum.NotaCredito)
                            imp = imp * decimal.MinusOne;
                        //acumula
                        result.TotFacturas += imp;
                        //TODO: si se corresponde a la caja de ajuste (ver como obtener la caja de cierre del mov. item)
                        //ahora se hace provisoriamente segun el tipo de formulario
                        if (cajaAjustarSaldo != null)
                        {
                            if (form.TipoFormularioID == (int)TipoFormularioEnum.Factura ||
                                (form.TipoFormularioID == (int)TipoFormularioEnum.NotaCredito && (form.FormularioOriginal != null && form.FormularioOriginal.TipoFormularioID == (int)TipoFormularioEnum.Factura)))
                            {
                                result.CantFacturasAjust++;
                                result.TotFacturasAjust += imp;
                            }
                        }

                        if (formus != null && formus.Count > 0)
                        {
                            dalEngine.UpdateCollection<Formulario>(formus);
                        }

                        //ajusta la caja si corresponde
                        if (cajaAjustarSaldo != null)
                            CajaDalc.CajaUsuarioAjustar(CajaDalc.CajaUsuarioReadByCaja(cajaAjustarSaldo.Id), result.TotFacturasAjust * decimal.MinusOne);

                        result.Envio = true;
                    }
                }
            }
            return result;
        }

        private EntityCollection<VwComprobanteVenta> GetVwComprobanteVenta(DateTime fechaDesde, DateTime fechaHasta)
        {
            string hql = " select distinct cv from VwComprobanteVenta cv "
                       + " where cv.Fecha >= :desde "
                       + " and cv.Fecha < :hasta "
                       + " order by cv.FormularioId , cv.TipoMedioPago DESC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", fechaDesde);
            query.SetParameter("hasta", fechaHasta.AddDays(1));
            EntityCollection<VwComprobanteVenta> test = dalEngine.GetManyByQuery<VwComprobanteVenta>(query);
            return test;
        }

        private Decimal ImporteBalanceadoCoseguro(Decimal? importeCoseguro, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro, ErpConceptoEnum concepto)
        {
            Decimal ret = 0;

            if (ConceptoAplicaCoseguro(concepto, modalidadCoseguro, importeCoseguro))
            {
                int divididoPor = 1; // ModalidadDerecho

                if (modalidadCoseguro == ValorizacionItemModalidadCoseguroEnum.ModalidadDerechoHonorario)
                    divididoPor = 2;
                else if (modalidadCoseguro == ValorizacionItemModalidadCoseguroEnum.ModalidadDerechoHonorarioInsumo)
                    divididoPor = 3;

                if (importeCoseguro.HasValue == true)
                    ret = importeCoseguro.Value / divididoPor;
            }

            return ret;
        }

        private bool ConceptoAplicaCoseguro(ErpConceptoEnum concepto, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro, Decimal? importeCoseguro)
        {
            if (importeCoseguro.HasValue == false || importeCoseguro.Value == 0)
                return false;

            if (modalidadCoseguro == ValorizacionItemModalidadCoseguroEnum.ModalidadDerecho)
            {
                if (concepto == ErpConceptoEnum.Gastos)
                    return true;
            }
            else if (modalidadCoseguro == ValorizacionItemModalidadCoseguroEnum.ModalidadDerechoHonorario)
            {
                if (concepto == ErpConceptoEnum.Gastos || concepto == ErpConceptoEnum.Honorarios)
                    return true;
            }
            else if (modalidadCoseguro == ValorizacionItemModalidadCoseguroEnum.ModalidadDerechoHonorarioInsumo)
            {
                return true;
            }

            return false;
        }

        private EntityCollection<ComprobanteVenta> CompletarComprobanteVenta(EntityCollection<VwComprobanteVenta> comprobantesHelp, IList<int> formulariosId, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            try
            {
                EntityCollection<ComprobanteVenta> comprobantes = new EntityCollection<ComprobanteVenta>();


                if (comprobantesHelp != null && comprobantesHelp.Count > 0)
                {

                    foreach (VwComprobanteVenta compHelp in comprobantesHelp)
                    {
                        if (!String.IsNullOrEmpty(compHelp.NuemroDoc))
                        {
                            bool insertar = false;

                            formulariosId.Add(compHelp.FormularioId);
                            ComprobanteVenta comprobante = new ComprobanteVenta();
                            ErpItem erpItem = ErpItemReadById(compHelp.ErpItemId);
                            comprobante.CodigoEmpresaEmisora = compHelp.CodigoEmpresaEmisora;
                            comprobante.FormularioId = compHelp.FormularioId;
                            comprobante.Tipo = compHelp.Tipo;
                            comprobante.Fecha = compHelp.Fecha;
                            comprobante.AsNumeroDoc = compHelp.NuemroDoc;
                            comprobante.NroTarjeta = compHelp.NroTarjeta;
                            comprobante.CodigoCliente = compHelp.CodigoCliente;
                            comprobante.CodigoCondicionPago = compHelp.CodigoCondicionPago;
                            comprobante.Anulada = compHelp.Anulada;
                            comprobante.Cai = compHelp.Cai;
                            comprobante.CaiVto = compHelp.CaiVto;
                            comprobante.Observaciones = compHelp.Observaciones;
                            comprobante.NroDocPadre = compHelp.NumeroDocPadre;
                            comprobante.FechaPadre = compHelp.FechaPadre;
                            comprobante.IaDescrip = compHelp.IaDescripcion;
                            comprobante.IaCantidad = compHelp.IaCantidad;
                            comprobante.IaPorcIvaRI = compHelp.ProcentajeIva;
                            comprobante.ImporteIVA = compHelp.ImporteIva;
                            comprobante.ImporteTotal = compHelp.ImporteTotal;
                            comprobante.CodigoProducto = erpItem.CodProducto;
                            comprobante.TipoFacturaFormulario = compHelp.TipoFacturaFormulario;
                            comprobante.CodigoEmpresaEmisora = compHelp.CodigoEmpresaEmisora;

                            Decimal importeBonificaciones = compHelp.ImporteBonificaciones.HasValue ? compHelp.ImporteBonificaciones.Value : 0;
                            Decimal importeHonorarioDerecho = (compHelp.ImporteHonorario.HasValue ? compHelp.ImporteHonorario.Value : 0) +
                                    (compHelp.ImporteDerecho.HasValue ? compHelp.ImporteDerecho.Value : 0);

                            if ((compHelp.ImporteDerecho.HasValue && compHelp.ImporteDerecho.Value > 0)
                                || (compHelp.ImporteHonorario.HasValue && compHelp.ImporteHonorario.Value > 0)
                                || (compHelp.ImporteInsumo.HasValue && compHelp.ImporteInsumo.Value > 0)
                                || (compHelp.ImporteCoseguro.HasValue && compHelp.ImporteCoseguro.Value > 0))
                            {
                                // Si el concepto del ERP es honorario
                                if (erpItem.Erc.Id == (int)ErpConceptoEnum.Honorarios)
                                {
                                    if ((compHelp.ImporteHonorario.HasValue && compHelp.ImporteHonorario.Value > 0) || ConceptoAplicaCoseguro(ErpConceptoEnum.Honorarios, modalidadCoseguro, compHelp.ImporteCoseguro))
                                    {
                                        Decimal importeHonorario = 0;
                                        insertar = true;
                                        if (compHelp.ImporteHonorario.HasValue)
                                        {
                                            Decimal porcentajeHonorarioSobreHonorarioDerecho = importeHonorarioDerecho > 0 ? ((compHelp.ImporteHonorario.Value * 100) / importeHonorarioDerecho) : 0;

                                            Decimal importeBonificacionHonorario = ((importeBonificaciones *
                                                                                     porcentajeHonorarioSobreHonorarioDerecho) /
                                                                                    100);

                                            importeHonorario = compHelp.ImporteHonorario.Value -
                                                               importeBonificacionHonorario;
                                        }

                                        Decimal importeHonorarioTotal = importeHonorario +
                                                                        ImporteBalanceadoCoseguro(
                                                                            compHelp.ImporteCoseguro, modalidadCoseguro, ErpConceptoEnum.Honorarios);

                                        if (importeHonorarioTotal > 0)
                                        {
                                            Decimal porcentajeHonorarioSobreTotal = (((importeHonorarioTotal) * 100) /
                                                                                     comprobante.ImporteTotal);
                                            // Si el honorario tiene valor
                                            comprobante.IaPrecio = ((porcentajeHonorarioSobreTotal *
                                                                     (compHelp.ImportePago /
                                                                      (1 + (comprobante.IaPorcIvaRI / 100)))) / 100);

                                            comprobante.IaImporteIVA = (comprobante.IaPrecio.Value *
                                                                        (comprobante.IaPorcIvaRI.Value / 100));
                                            comprobante.IaPrecio = comprobante.IaPrecio;
                                        }
                                    }
                                }
                                // Si el concepto del ERP es Gastos
                                else if (erpItem.Erc.Id == (int)ErpConceptoEnum.Gastos)
                                {
                                    Decimal? importeDerecho = 0;

                                    if (compHelp.ImporteDerecho.HasValue && compHelp.ImporteDerecho.Value > 0)
                                    {
                                        Decimal porcentajeDerechoSobreHonorarioDerecho = importeHonorarioDerecho > 0 ? ((compHelp.ImporteDerecho.Value * 100) / importeHonorarioDerecho) : 0;
                                        Decimal importeBonificacionDerecho = ((importeBonificaciones * porcentajeDerechoSobreHonorarioDerecho) / 100);
                                        importeDerecho = (compHelp.ImporteDerecho.Value - importeBonificacionDerecho);
                                    }

                                    Decimal? importeGastos = ((importeDerecho.HasValue ? importeDerecho.Value : 0) + (ImporteBalanceadoCoseguro(compHelp.ImporteCoseguro, modalidadCoseguro, ErpConceptoEnum.Gastos)));

                                    if (importeGastos.HasValue && importeGastos.Value > 0)
                                    {
                                        insertar = true;
                                        Decimal porcentajeGastosSobreTotal = ((importeGastos.Value * 100) / comprobante.ImporteTotal);

                                        // Si el honorario tiene valor
                                        comprobante.IaPrecio = ((porcentajeGastosSobreTotal * (compHelp.ImportePago / (1 + (comprobante.IaPorcIvaRI / 100)))) / 100);
                                        //comprobante.IaPrecio = ((porcentajeGastosSobreTotal * compHelp.ImportePago ) / 100);
                                        comprobante.IaImporteIVA = (comprobante.IaPrecio.Value * (comprobante.IaPorcIvaRI.Value / 100));
                                        comprobante.IaPrecio = comprobante.IaPrecio;// -comprobante.IaImporteIVA;
                                    }
                                }
                                // Si el concepto del ERP es Insumos
                                else if (erpItem.Erc.Id == (int)ErpConceptoEnum.Insumos)
                                {
                                    if ((compHelp.ImporteInsumo.HasValue && compHelp.ImporteInsumo.Value > 0) || ConceptoAplicaCoseguro(ErpConceptoEnum.Insumos, modalidadCoseguro, compHelp.ImporteCoseguro))
                                    {
                                        Decimal importeInsumo = 0;
                                        //if (compHelp.ImporteBonificaciones.HasValue)
                                        //{
                                        //    Decimal porcentajeBonificacionesInsumo = ((compHelp.ImporteInsumo.Value * compHelp.ImporteBonificaciones.Value) / 100);
                                        //    Decimal importeBonificacionInsumo = ((importeBonificaciones * porcentajeBonificacionesInsumo) / 100);
                                        //}
                                        //else
                                        importeInsumo = (compHelp.ImporteInsumo.HasValue
                                                             ? compHelp.ImporteInsumo.Value
                                                             : 0) +
                                                        ImporteBalanceadoCoseguro(compHelp.ImporteCoseguro,
                                                                                  modalidadCoseguro, ErpConceptoEnum.Insumos);
                                        ;

                                        insertar = true;
                                        Decimal porcentajeInsumosSobreTotal = ((importeInsumo * 100) / comprobante.ImporteTotal);
                                        // Si el honorario tiene valor
                                        comprobante.IaPrecio = ((porcentajeInsumosSobreTotal * (compHelp.ImportePago / (1 + (comprobante.IaPorcIvaRI / 100)))) / 100);
                                        //comprobante.IaPrecio = ((porcentajeInsumosSobreTotal * compHelp.ImportePago ) / 100);
                                        comprobante.IaImporteIVA = (comprobante.IaPrecio.Value * (comprobante.IaPorcIvaRI.Value / 100));
                                        comprobante.IaPrecio = comprobante.IaPrecio;//6 -comprobante.IaImporteIVA;
                                    }
                                }
                            }
                            else
                            {
                                // 
                                if (erpItem.Erc.Id == (int)ErpConceptoEnum.Gastos)
                                {
                                    if (compHelp.ImporteTotal > 0)
                                    {
                                        insertar = true;
                                        Decimal porcentajeTotalSobreTotal = ((compHelp.ImporteTotal * 100) / comprobante.ImporteTotal);

                                        // Si el honorario tiene valor
                                        comprobante.IaPrecio = ((porcentajeTotalSobreTotal * (compHelp.ImportePago / (1 + (comprobante.IaPorcIvaRI / 100)))) / 100);
                                        //comprobante.IaPrecio = ((porcentajeTotalSobreTotal * compHelp.ImportePago ) / 100);
                                        //comprobante.IaImporteIVA = (comprobante.IaPrecio.Value * comprobante.IaPorcIvaRI.Value) / 100;
                                        comprobante.IaImporteIVA = (comprobante.IaPrecio.Value * (comprobante.IaPorcIvaRI.Value / 100));
                                        comprobante.IaPrecio = comprobante.IaPrecio;// -comprobante.IaImporteIVA;
                                    }
                                }
                            }

                            //if (compHelp.TipoMedioPago == (int)TipoMedioPagoEnum.Efectivo)
                            comprobante.TipoMedioPago = compHelp.TipoMedioPago;
                            comprobante.ImporteEfectivo = compHelp.ImportePago;
                            comprobante.CodigoCuenta = compHelp.CodigoCuenta;
                            comprobante.CodigoBanco = compHelp.CodigoBanco;
                            comprobante.NroCheque = compHelp.NroCheque;
                            comprobante.FechaVtoCheque = compHelp.FechaVtoCheque;
                            comprobante.CodigoTarjeta = compHelp.CodigoTarjeta;
                            comprobante.NroLote = compHelp.NroLote;
                            comprobante.FechaVtoTarjeta = compHelp.FechaVtoTarjeta;
                            comprobante.NroAutorizacion = compHelp.NroAutorizacion;
                            comprobante.Titular = compHelp.Titular;

                            if (insertar)
                                comprobantes.Add(comprobante);
                        }
                    }
                }

                EntityCollection<ComprobanteVenta> comprobantesAgrupados = ComprobanteVentaAgruparPorErpProductoPorMedioPago(comprobantes);
                ComprobanteVentaValidarImportes(comprobantesAgrupados);

                return comprobantesAgrupados;

            }
            catch (Exception j)
            {
                throw new Exception("No se ha podido realizar la exportación :" + j.Message);
            }
        }

        private void ComprobanteVentaValidarImportes(EntityCollection<ComprobanteVenta> comprobantesVenta)
        {
            List<String> key = new List<String>();

            // Si existen comprobantes de venta a agrupar
            if (comprobantesVenta != null && comprobantesVenta.Count > 0)
            {
                foreach (ComprobanteVenta compVenta in comprobantesVenta)
                {
                    if (!key.Contains(compVenta.KeyFactura))
                    {
                        key.Add(compVenta.KeyFactura);
                        ComprobanteVentaValidarImportePorFactura(comprobantesVenta, compVenta.KeyFactura);
                    }
                }
            }
        }

        private void ComprobanteVentaValidarImportePorFactura(EntityCollection<ComprobanteVenta> comprobantesVenta, String Key)
        {
            // Si existen comprobantes de venta a agrupar
            if (comprobantesVenta != null && comprobantesVenta.Count > 0)
            {
                ComprobanteVenta compRedondeo = null;

                // Recorro todos los comprobantes buscando los que tienen igual
                // nro de factura, tipo y medio de pago
                Decimal importeFactura = 0;
                Decimal importeIva = 0;
                Decimal importesParciales = 0;
                Decimal importesParcialesIva = 0;

                bool compRedondeoFlag = true;

                foreach (ComprobanteVenta compVenta in comprobantesVenta)
                {
                    if (Key == compVenta.KeyFactura)
                    {
                        decimal difItem = Decimal.Round(compVenta.ImporteEfectivo - (compVenta.IaPrecio.GetValueOrDefault(0) + compVenta.IaImporteIVA.GetValueOrDefault(0)), 2);
                        if (Math.Abs(Decimal.Round(difItem, 2)) == Convert.ToDecimal(0.01))
                            compVenta.IaPrecio += difItem;

                        importesParciales += (compVenta.IaPrecio.HasValue ? compVenta.IaPrecio.Value : 0);
                        importesParcialesIva += (compVenta.IaImporteIVA.HasValue ? compVenta.IaImporteIVA.Value : 0);
                        importeFactura = compVenta.ImporteTotal;
                        importeIva = compVenta.ImporteIVA;
                    }
                }

                Decimal dif = importeFactura - importesParciales;
                Decimal difIva = importeIva - importesParcialesIva;

                Decimal totalDif = Math.Abs(Decimal.Round(dif, 2));
                Decimal totalDifIva = Math.Abs(Decimal.Round(difIva, 2));
                Decimal compareDif = Convert.ToDecimal(0.01);

                if (Decimal.Round(dif, 2) != 0 || Decimal.Round(difIva, 2) != 0)
                    if (totalDif == compareDif || totalDifIva == compareDif)
                    {
                        foreach (ComprobanteVenta compVenta in comprobantesVenta)
                        {
                            if (Key == compVenta.KeyFactura)
                            {
                                compVenta.ImporteTotal = importesParciales;
                                compVenta.ImporteIVA = importesParcialesIva;
                            }
                        }
                    }
                    else
                        throw new Exception("Hay totales de facturas que no coinciden con el total de los items");
            }
        }

        /// <summary>
        /// Agrupa por ERP producto y Medios de pago
        /// </summary>
        /// <param name="comprobantesVenta">Colección de comprobantes de vanta a exportar</param>
        /// <returns>Colección de comprobantes de venta agrupados</returns>
        private EntityCollection<ComprobanteVenta> ComprobanteVentaAgruparPorErpProductoPorMedioPago(EntityCollection<ComprobanteVenta> comprobantesVenta)
        {
            List<String> key = new List<String>();
            EntityCollection<ComprobanteVenta> compVentas = new EntityCollection<ComprobanteVenta>();

            // Si existen comprobantes de venta a agrupar
            if (comprobantesVenta != null && comprobantesVenta.Count > 0)
            {
                foreach (ComprobanteVenta compVenta in comprobantesVenta)
                {
                    if (!key.Contains(compVenta.Key))
                    {
                        key.Add(compVenta.Key);
                        ComprobanteVenta aux = ComprobanteVentaAgruparPorErpProductoPorMedioPago(comprobantesVenta, compVenta.Key);
                        if (aux != null)
                            compVentas.Add(aux);
                    }
                }
            }

            return compVentas;
        }

        /// <summary>
        /// Agrupa por ERP producto y Medios de pago
        /// </summary>
        /// <param name="comprobantesVenta">Colección de comprobantes de vanta a exportar</param>
        /// <param name="key"></param>
        /// <returns>Comprobantes de Venta agrupado</returns>
        private ComprobanteVenta ComprobanteVentaAgruparPorErpProductoPorMedioPago(EntityCollection<ComprobanteVenta> comprobantesVenta, String key)
        {
            ComprobanteVenta comprobanteAgrupado = null;

            // Si existen comprobantes de venta a agrupar
            if (comprobantesVenta != null && comprobantesVenta.Count > 0)
            {
                bool primerItem = true;

                // Recorro todos los comprobantes buscando los que tienen igual
                // nro de factura, tipo y medio de pago
                foreach (ComprobanteVenta compVenta in comprobantesVenta)
                {
                    if (key == compVenta.Key)
                    {
                        if (primerItem == true)
                        {
                            comprobanteAgrupado = new ComprobanteVenta(compVenta);
                            comprobanteAgrupado.IaPrecio = comprobanteAgrupado.IaPrecio.HasValue == false
                                                               ? 0
                                                               : comprobanteAgrupado.IaPrecio.Value;

                            comprobanteAgrupado.IaImporteIVA = comprobanteAgrupado.IaImporteIVA.HasValue == false
                                                               ? 0
                                                               : comprobanteAgrupado.IaImporteIVA.Value;

                            primerItem = false;
                        }
                        else
                        {
                            comprobanteAgrupado.IaImporteIVA += (compVenta.IaImporteIVA == null) ? 0 : compVenta.IaImporteIVA;
                            comprobanteAgrupado.IaPrecio += (compVenta.IaPrecio == null) ? 0 : compVenta.IaPrecio;
                        }
                    }
                }
            }

            return comprobanteAgrupado;
        }

        #endregion

        #region FacturasContadoERP

        /// <summary>
        /// Busca todas las facturas a enviar al ERP
        /// </summary>
        /// <param name="fechaDesde">Fecha desde a considerar</param>
        /// <param name="fechaHasta">Fecha hasta a considerar</param>
        /// <returns>Colección de facturas</returns>
        [Private]
        public EntityCollection<Formulario> FacturasContadoAEnviarReadAll(DateTime fechaDesde, DateTime fechaHasta)
        {
            string hql = "select distinct f from Formulario f, TipoFormulario t, MovimientoCajaItem m "
                        + " where f.TipoFormularioID = t.Id and f.Id = m.FormularioID "
                        + " and t.EnviarERP = true "
                        + " and f.EnvioERP = false AND f.CreateDate >= :desde "
                        + " and f.CreateDate < :hasta AND t.EnviarERP = true "
                        + " order by f.Id";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", fechaDesde);
            query.SetParameter("hasta", fechaHasta.AddDays(1));
            return dalEngine.GetManyByQuery<Formulario>(query);
        }

        #endregion

        #region Formulario
        /// <summary>
        /// Inserto un Nuevo Formulario
        /// </summary>
        /// <param name="formulario">Formulario a Insertar</param>
        /// <param name="user">Usuario de la Operacion</param>
        [RequiresTransaction]
        protected internal virtual Formulario FormularioInsert(Formulario formulario, Turno turno)
        {
            formulario.EnvioERP = false;

            // Inserto o modifico el Formulario
            formulario = dalEngine.Update<Formulario>(formulario);

            // Codigo para poder tener siempre actualizados los campos ImportePagado en el Turno.
            if (turno != null)
            {
                turno.AsignarImportesPagados(formulario);
                turno = dalEngine.Update<Turno>(turno);
            }

            return formulario;
        }

        /// <summary>
        /// [RQ] Verifica si existe una factura en la tabla Formulario
        /// </summary>
        /// <param name="facturaNumeracion">La factura con los datos de sucursal, tipo y número</param>
        /// <returns>Si existe la factura o no</returns>
        public bool FormularioExisteFactura(FacturaNumeracion facturaNumeracion)
        {
            ReadManyCommand<Formulario> readCmd = new ReadManyCommand<Formulario>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Formulario.Properties.Numero,
                   "=", facturaNumeracion.Numero);

            filter.Add(BooleanOp.And, Formulario.Properties.Sucursal,
                   "=", facturaNumeracion.PuntoVenta.NumeroFiscal);

            filter.Add(BooleanOp.And, Formulario.Properties.Clase,
                   "=", facturaNumeracion.Tipo);

            filter.Add(BooleanOp.And, Formulario.Properties.Empresa.Id,
                   "=", facturaNumeracion.PuntoVenta.Empresa.Id);

            readCmd.Filter = filter;

            EntityCollection<Formulario> fc = readCmd.Execute();

            // Si encontre, retorno True. Sino, false.
            if (fc.Count > 0)
                return true;
            else
                return false;
        }

        /// <summary>
        /// Busco todos los Formularios Asociados a un MovimientoCaja
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Buscar sus Formularios</param>
        /// <returns>Colección de Formularios Asociados a un MovimientoCaja</returns>
        internal EntityCollection<Formulario> FormularioReadByMovimientoCaja(int movimientoID)
        {
            string hql = "select distinct f from Formulario f, MovimientoCajaItem m "
                        + " where f.Id = m.FormularioID "
                        + " and m.MovimientoCaja.Id = :movimientoCajaId "
                        + " order by f.Id";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("movimientoCajaId", movimientoID);

            return dalEngine.GetManyByQuery<Formulario>(query);
        }











        /// <summary>
        /// Retorno todas las Facturas de Depósito sin una NC Asociada
        /// </summary>
        /// <returns>Facturas de Depósito sin NC Asociada</returns>
        public EntityCollection<Formulario> FormularioReadDepositosPendientes()
        {
            //string hql = "from Formulario f where f.TipoFormularioID = :facturaDeposito "
            //            + " and f.FechaAnulacion is null and f.Id not in "
            //                + "(select nc.FormularioOriginalID from Formulario nc "
            //                + " where nc.TipoFormularioID = :notaCredito and nc.FechaAnulacion is null)"
            //            + " order by f.Id";

            string hql = "select new enfoke.Eges.Entities.Results.FormularioDepositoPendiente(f, tf, cond) "
               + " from Formulario f, TipoFormulario tf, CondicionIVA cond where f.TipoFormularioID = tf.Id and f.CondicionIVAID = cond.Id and f.TipoFormularioID = :facturaDeposito "
            + " and f.FechaAnulacion is null and not exists "
                + "(select nc from Formulario nc "
                + " where nc.FormularioOriginalID = f.Id and nc.TipoFormularioID = :notaCredito and nc.FechaAnulacion is null)"
            + " order by f.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("facturaDeposito", (int)TipoFormularioEnum.FacturaDeposito);
            query.SetParameter("notaCredito", (int)TipoFormularioEnum.NotaCredito);

            EntityCollection<FormularioDepositoPendiente> retorno = dalEngine.GetManyByQuery<FormularioDepositoPendiente>(query);
            EntityCollection<Formulario> ret = new EntityCollection<Formulario>();

            if (retorno != null && retorno.Count > 0)
                foreach (FormularioDepositoPendiente deposito in retorno)
                    ret.Add(deposito.Formulario);

            return ret;
        }

        public void FormularioRevertByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            CajaDalc CajaDalc = Context.Session.CajaDalc;

            EntityCollection<MovimientoCajaItem> movimientoCajaItems = CajaDalc.MovimientoCajaItemReadByMovimiento(movimientoCaja.Id, true);
            Formulario formulario;

            CajaDalc.PagoMovimientoCajaDeleteByMovimientoCaja(movimientoCaja);
            CajaDalc.PagosDeleteByMovimientoCaja(movimientoCaja);
            CajaDalc.MovimientoCajaItemDeleteByMovimientoCaja(movimientoCaja);
            dalEngine.Delete(movimientoCaja);

            if (movimientoCajaItems != null)
            {
                foreach (MovimientoCajaItem movItem in movimientoCajaItems)
                {
                    if (movItem.FormularioID.HasValue)
                    {
                        formulario = dalEngine.GetById<Formulario>(movItem.FormularioID.Value);
                        if (formulario != null)
                            dalEngine.Delete(formulario);
                    }
                }
            }
        }






        [Private]
        public void FormulariosUpdateBatchEnviadosToErp(List<int> formulariosIds)
        {
            dalEngine.UpdatePropertyBatchByIds<Formulario>(formulariosIds, Formulario.Properties.EnvioERP, true);
        }

        #endregion

        #region FormularioView

        /// <summary>
        /// Retorno todos los Formularios con los Filtros dados [Vista]
        /// </summary>
        /// <param name="txtBusqueda">Texto del Filtro Seleccionado</param>
        /// <param name="tipoBusqueda">Filtro Seleccionado</param>
        /// <param name="fechaDesde">Filtro por Fecha del Formulario</param>
        /// <param name="fechaHasta">Filtro por Fecha del Formulario</param>
        /// <returns>Los Formularios que apliquen a los filtros</returns>
        [MinuteTimeout]
        public virtual EntityCollection<FormularioView> FormularioViewRead(string txtBusqueda, FormularioSearchTypeEnum tipoBusqueda, DateTime fechaDesde, DateTime fechaHasta, int empresaId)
        {
            CajaDalc CajaDalc = Context.Session.CajaDalc;

            string search = "'%" + txtBusqueda + "%'";

            Filter filter = new Filter();
            // Armo el Filtro del Periodo de Fechas
            filter.Add(BooleanOp.And, FormularioView.Properties.Fecha,
                ">=", fechaDesde.Date);
            filter.Add(BooleanOp.And, FormularioView.Properties.Fecha,
                "<", fechaHasta.AddDays(1));
            if (empresaId > 0)
                filter.Add(BooleanOp.And, FormularioView.Properties.EmpresaID, "=", empresaId);

            EntityCollection<TipoFormulario> tfs = dalEngine.GetAll<TipoFormulario>();
            List<int> tipos = new List<int>();
            for (int i = 0; i < tfs.Count; i++)
                if (!tfs[i].EnviarERP)
                    tipos.Add(tfs[i].Id);
            filter.Add(BooleanOp.And, FormularioView.Properties.TipoFormularioID,
                "NOT IN", tipos.ToArray());

            // Armo el Filtro del Combo
            if (!String.IsNullOrEmpty(txtBusqueda))
            {
                IPropertyReference searchProperty = null;
                switch (tipoBusqueda)
                {
                    case FormularioSearchTypeEnum.NumeroFormulario:
                        searchProperty = FormularioView.Properties.Descripcion;
                        break;
                    case FormularioSearchTypeEnum.Protocolo:
                        searchProperty = FormularioView.Properties.Protocolo;
                        break;
                    case FormularioSearchTypeEnum.RazonSocial:
                        searchProperty = FormularioView.Properties.RazonSocial;
                        break;
                }
                if (searchProperty != null)
                {
                    filter.Add(BooleanOp.And, searchProperty,
                        "LIKE", search);
                }
            }

            Sort sort = new Sort(new SortItem(FormularioView.Properties.Descripcion));
            return dalEngine.GetManyByFilter<FormularioView>(filter, sort);
        }
        [MinuteTimeout]
        public virtual EntityCollection<FormularioView> FormularioViewRead(string txtBusqueda, FormularioSearchTypeEnum tipoBusqueda, DateTime fechaDesde, DateTime fechaHasta, int sucursalId, int empresaId)
        {
            CajaDalc CajaDalc = Context.Session.CajaDalc;

            if (sucursalId > 0)
            {
                string search = "'%" + txtBusqueda + "%'";

                EntityCollection<TipoFormulario> tfs = dalEngine.GetAll<TipoFormulario>();
                List<int> tipos = new List<int>();
                for (int i = 0; i < tfs.Count; i++)
                    if (!tfs[i].EnviarERP)
                        tipos.Add(tfs[i].Id);

                String hql = "select distinct fw from FormularioView fw, PuntoVenta pv " +
                             "where fw.SucursalID = pv.NumeroFiscal " +
                             "and pv.Sucursal.Id = :sucursalId " +
                             "and fw.Fecha >= :fechaDesde " +
                             "and fw.Fecha < :fechaHasta " +
                             "and fw.TipoFormularioID not in (:tipos) ";

                if (empresaId > 0)
                    hql += "and fw.EmpresaID = :empresaId";

                // Armo el Filtro del Combo
                if (!String.IsNullOrEmpty(txtBusqueda))
                {
                    IPropertyReference searchProperty = null;
                    switch (tipoBusqueda)
                    {
                        case FormularioSearchTypeEnum.NumeroFormulario:
                            searchProperty = FormularioView.Properties.Descripcion;
                            break;
                        case FormularioSearchTypeEnum.Protocolo:
                            searchProperty = FormularioView.Properties.Protocolo;
                            break;
                        case FormularioSearchTypeEnum.RazonSocial:
                            searchProperty = FormularioView.Properties.RazonSocial;
                            break;
                    }
                    if (searchProperty != null)
                    {
                        hql += "and fw." + searchProperty.Name + " like " + search;
                    }
                }

                hql += " order by fw.Descripcion ";

                IQuery query = dalEngine.CreateQuery(hql);
                query.SetParameterList("tipos", tipos.ToArray());
                query.SetParameter("fechaDesde", fechaDesde.Date);
                query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1));
                query.SetParameter("sucursalId", sucursalId);
                if (empresaId > 0)
                    query.SetParameter("empresaId", empresaId);

                return dalEngine.GetManyByQuery<FormularioView>(query);
            }
            else
            {
                return FormularioViewRead(txtBusqueda, tipoBusqueda, fechaDesde, fechaHasta, empresaId);
            }
        }

        #endregion

        #region Diskettes

        [Private]
        public IList EjecutarHQLDinamico(string hql)
        {
            IQuery query = dalEngine.CreateQuery(hql);
            return query.List();
        }

        [RequiresTransaction]
        public virtual void TipoExportacionDelete(int tipoExportacionId)
        {
            EntityCollection<ITipoExportacionItem> cabecera =
                ITipoExportacionItemReadByTipoExportacionId(tipoExportacionId, true);
            EntityCollection<ITipoExportacionItem> detalle =
                ITipoExportacionItemReadByTipoExportacionId(tipoExportacionId, false);
            TipoExportacion tipoExportacion = dalEngine.GetById<TipoExportacion>(tipoExportacionId);

            dalEngine.Delete(cabecera);
            dalEngine.Delete(detalle);

            EntityCollection<ObraSocialTipoExportacion> obraSocialTipoExportaciones = Context.Session.ObrasSocialesDalc.ObraSocialTipoExportacionReadByTipoExportacion(tipoExportacion.Id);
            if (obraSocialTipoExportaciones != null && obraSocialTipoExportaciones.Count > 0)
                throw new NotLoggeableException("El formato de exportación que desea eliminar se encuentra asociado a una obra social.");

            dalEngine.Delete(tipoExportacion);
        }






        [RequiresTransaction]
        public virtual void TipoExportacionSave(TipoExportacion te)
        {
            TipoExportacionSave(te, null, null);
        }
        [RequiresTransaction]
        public virtual void TipoExportacionSave(TipoExportacion te, EntityCollection<ITipoExportacionItem> cabecera, EntityCollection<ITipoExportacionItem> detalle)
        {
            //Estos campos y sus harcodeos desaparecen cuando se deje de utilizar el SP.
            te.Origen = "sp_Generar_DKT";
            te.TipoOrigen = TipoOrigenExportacionEnum.MigracionSP;
            TipoExportacion savedTe = dalEngine.Update<TipoExportacion>(te);
            TipoExportacionCabeceraSave(cabecera, savedTe);
            TipoExportcionDetalleSave(detalle, savedTe);
        }

        [RequiresTransaction]
        protected virtual void TipoExportcionDetalleSave(EntityCollection<ITipoExportacionItem> detalle,
            TipoExportacion te)
        {
            if (detalle == null || detalle.Count == 0)
            {
                EntityCollection<ITipoExportacionItem> detalleOld =
                  ITipoExportacionItemReadByTipoExportacionId(te.Id, false);

                dalEngine.Delete(detalleOld);
            }
            else
            {
                //actualiza y agrega items
                foreach (ITipoExportacionItem item in detalle)
                {
                    TipoExportacionDetalleProp d = (TipoExportacionDetalleProp)item;
                    d.TipoExportacion = te;
                    dalEngine.Update<TipoExportacionDetalleProp>(d);
                }

                //elimina si se borró alguno
                foreach (ITipoExportacionItem item in detalle.DeletedItems)
                {
                    TipoExportacionDetalleProp d = (TipoExportacionDetalleProp)item;
                    dalEngine.Delete(d);
                }
            }
        }

        [RequiresTransaction]
        protected virtual void TipoExportacionCabeceraSave(EntityCollection<ITipoExportacionItem> cabecera,
            TipoExportacion te)
        {

            if (cabecera == null || cabecera.Count == 0)
            {
                EntityCollection<ITipoExportacionItem> cabeceraOld =
                    ITipoExportacionItemReadByTipoExportacionId(te.Id, true);

                dalEngine.Delete(cabeceraOld);
            }
            else
            {
                //actualiza y agrega items
                foreach (ITipoExportacionItem item in cabecera)
                {
                    TipoExportacionCabeceraProp c = (TipoExportacionCabeceraProp)item;
                    c.TipoExportacion = te;
                    dalEngine.Update<TipoExportacionCabeceraProp>(c);
                }

                //elimina si se borró alguno
                foreach (ITipoExportacionItem item in cabecera.DeletedItems)
                {
                    TipoExportacionCabeceraProp c = (TipoExportacionCabeceraProp)item;
                    dalEngine.Delete(c);
                }
            }
        }

        public EntityCollection<TipoExportacionExcepcion> TipoExportacionExcepcionReadAll()
        {
            return dalEngine.GetAll<TipoExportacionExcepcion>(TipoExportacionExcepcion.Properties.Nombre);
        }

        public EntityCollection<TipoExportacionColumna> TipoExportacionColumnaReadAll()
        {
            return dalEngine.GetAll<TipoExportacionColumna>(TipoExportacionColumna.Properties.Nombre);
        }

        public EntityCollection<TipoExportacion> TipoExportacionReadAll()
        {
            return dalEngine.GetAll<TipoExportacion>(TipoExportacion.Properties.Nombre);
        }

        public EntityCollection<ITipoExportacionItem>
              ITipoExportacionItemReadByTipoExportacionId(int tipoExportacionId, bool esCabecera)
        {

            string entidad;
            if (esCabecera)
                entidad = "TipoExportacionCabeceraProp";
            else
                entidad = "TipoExportacionDetalleProp";

            string hql = "from " + entidad + " tec " +
                  "where tec.TipoExportacion.Id = :tipoExportacionId " +
                  "order by tec.Orden";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("tipoExportacionId", tipoExportacionId);
            return dalEngine.GetManyByQuery<ITipoExportacionItem>(query);
        }

        public EntityCollection<TipoExportacion> TipoExportacionReadByObraSocial(int obraSocialId)
        {
            string hql = "select oste.TipoExportacion from ObraSocialTipoExportacion oste " +
                    "where oste.ObraSocialId = :obraSocialId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("obraSocialId", obraSocialId);
            return dalEngine.GetManyByQuery<TipoExportacion>(query);
        }

        /// <summary>
        /// [RQ] Obtengo los datos para generar el archivo DBF de IOMA 
        /// Primero ejecuta el stored procedure que crea la tabla que tendrá 
        /// los datos que devuelve este método.
        /// </summary>
        /// <param name="comprobantes">Comprobantes a Obtener las Líneas para el archivo DBF</param>
        /// <returns>Colección de LineaIOMA</returns>
        public EntityCollection<LineaIOMA> DisketteGenerarDatosDBF(List<int> comprobantes)
        {
            StringBuilder sbComprobantes = new StringBuilder();
            for (int i = 0; i < comprobantes.Count; i++)
            {
                sbComprobantes.Append(comprobantes[i]);
                if (i < comprobantes.Count - 1)
                    sbComprobantes.Append(",");
            }

            ExecuteSP("SP_Generar_DKT_IOMA", new KeyValuePair<string, object>("@comprobantes", sbComprobantes.ToString()));


            return dalEngine.GetAll<LineaIOMA>();
        }
        [Private]
        public DataSet ExportacionResolverOrigenStoredProcedure(TipoExportacion te, List<int> comprobantes)
        {
            // Arma un string con los ids
            string comprobantesString = comprobantesToString(comprobantes);
            // Se prepara para ejecutar el SP
            List<IDbDataParameter> args = new List<IDbDataParameter>();
            args.Add(CreateParameter("v_nombre_exp", te.Nombre));
            //ESTE PARAMETRO NO SE USA EN EL SP
            args.Add(CreateParameter("v_expo_seccion", 1));
            args.Add(CreateParameter("v_comp_list", comprobantesString));
            // Si es oracle, le agrega el parámetro para el output
            if (enfoke.Context.Data.Session.DatabaseType == DatabaseTypeEnum.Oracle)
            {
                IDbDataParameter p_return;
                p_return = new System.Data.OracleClient.OracleParameter("p_return", System.Data.OracleClient.OracleType.Cursor);
                p_return.Direction = ParameterDirection.Output;
                args.Add(p_return);
            }
            return enfoke.Context.Data.Session.ExecuteDataSet(CommandType.StoredProcedure, te.Origen,
                                args.ToArray());
        }

        private IDbDataParameter CreateParameter(string name, object value)
        {
            IDbDataParameter parameter = enfoke.Context.Data.Session.ExecuterCreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }

        private static string comprobantesToString(List<int> comprobantes)
        {
            if (comprobantes == null)
                return "";
            string ret = "";
            foreach (int comprobante in comprobantes)
            {
                if (ret != "")
                    ret += ",";
                ret += comprobante.ToString();
            }
            return ret;
        }
        [Private]
        public List<string> DisketteGenerarLegacy(TipoExportacion te, List<int> comprobantes)
        {
            List<string> lineasDiskette = new List<string>();
            switch (te.Id)
            {
                case (int)FormatosExportacionEnum.CASA:
                    {
                        ReadManyCommand<ExportCASAView> readCmd = new ReadManyCommand<ExportCASAView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportCASAView.Properties.ComprobanteID,
                            "IN", comprobantes.ToArray());

                        EntityCollection<ExportCASAView> lineas = readCmd.Execute();

                        foreach (ExportCASAView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }
                    break;
                case (int)FormatosExportacionEnum.OSDE:
                    {
                        ReadManyCommand<ExportOSDEView> readCmd = new ReadManyCommand<ExportOSDEView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportOSDEView.Properties.ComprobanteID,
                            "IN", comprobantes.ToArray());

                        if (te.Id == (int)FormatosExportacionEnum.OSDE)
                        {
                            readCmd.Sort = new Sort();
                            readCmd.Sort.Add(ExportOSDEView.Properties.ServicioID, SortingDirection.Asc);
                            readCmd.Sort.Add(ExportOSDEView.Properties.FechaTurno, SortingDirection.Asc);
                            readCmd.Sort.Add(ExportOSDEView.Properties.TurnoID, SortingDirection.Asc);
                            readCmd.Sort.Add(ExportOSDEView.Properties.TipoPracticaTurnoID, SortingDirection.Asc);
                        }

                        EntityCollection<ExportOSDEView> lineas = readCmd.Execute();

                        foreach (ExportOSDEView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }

                    break;
                case (int)FormatosExportacionEnum.LuisPasteur:
                    {
                        ReadManyCommand<ExportLuisPasteurView> readCmd = new ReadManyCommand<ExportLuisPasteurView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportLuisPasteurView.Properties.ComprobanteID,
                            "IN", comprobantes.ToArray());

                        EntityCollection<ExportLuisPasteurView> lineas = readCmd.Execute();

                        foreach (ExportLuisPasteurView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }

                    break;
                case (int)FormatosExportacionEnum.OSAP:
                    {
                        ReadManyCommand<ExportOSAPView> readCmd = new ReadManyCommand<ExportOSAPView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportOSAPView.Properties.ComprobanteID,
                            "IN", comprobantes.ToArray());

                        EntityCollection<ExportOSAPView> lineas = readCmd.Execute();

                        foreach (ExportOSAPView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }

                    break;
                case (int)FormatosExportacionEnum.Galeno:
                    {
                        // Si es para Galeno, debo generar las lineas via un SP
                        if (te.Id == (int)FormatosExportacionEnum.Galeno)
                            ExecuteSP("SP_Generar_DKT_Galeno", new KeyValuePair<string, object>("@comprobante", comprobantes[0]));

                        ReadManyCommand<ExportGalenoView> readCmd = new ReadManyCommand<ExportGalenoView>(dalEngine);

                        readCmd.Sort = new Sort();
                        readCmd.Sort.Add(ExportGalenoView.Properties.Protocolo, SortingDirection.Asc);
                        readCmd.Sort.Add(ExportGalenoView.Properties.LineaID, SortingDirection.Asc);

                        EntityCollection<ExportGalenoView> lineas = readCmd.Execute();

                        foreach (ExportGalenoView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }
                    break;
                case (int)FormatosExportacionEnum.SM:
                    {
                        ReadManyCommand<ExportSMView> readCmd = new ReadManyCommand<ExportSMView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportSMView.Properties.ComprobanteID,
                            "IN", comprobantes.ToArray());

                        EntityCollection<ExportSMView> lineas = readCmd.Execute();

                        foreach (ExportSMView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }
                    break;
                case (int)FormatosExportacionEnum.OMINT:
                    {
                        ReadManyCommand<ExportOmintView> readCmd = new ReadManyCommand<ExportOmintView>(dalEngine);

                        readCmd.Filter = new Filter();
                        readCmd.Filter.Add(ExportOmintView.Properties.Id,
                            "IN", comprobantes.ToArray());

                        EntityCollection<ExportOmintView> lineas = readCmd.Execute();

                        foreach (ExportOmintView linea in lineas)
                            lineasDiskette.Add(linea.Linea);
                    }
                    break;
            }
            return lineasDiskette;
        }

        private void ExecuteSP(string spName, params KeyValuePair<string, object>[] parameters)
        {
            IDbDataParameter[] dbParameters = new IDbDataParameter[parameters.Length];
            for (int n = 0; n < parameters.Length; n++)
            {
                IDbDataParameter parameter = dalEngine.Connection.ExecuterCreateParameter();
                parameter.ParameterName = parameters[n].Key;
                parameter.Value = parameters[n].Value;

                dbParameters[n] = parameter;
            }
            dalEngine.Connection.ExecuteNonQuery(spName, dbParameters);
        }
        #endregion

        #region FacturacionMedicoServicio
        /// <summary>
        /// [RQ] Obtengo la facturación de médicos agrupada por servicio en un rango de fechas.
        /// </summary>
        /// <param name="desde">Fecha Inicial del filtro</param>
        /// <param name="hasta">Fecha Final del filtro </param>
        /// <param name="medico">Id del Médico para el cual traer la facturación (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer la facturación (0 = Todos)</param>
        /// <returns>Colección de Recibos Médicos Filtrados</returns>
        public EntityCollection<FacturacionServicioMedicoView> FacturacionMedicoServicioViewReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio)
        {
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, FacturacionServicioMedicoView.Properties.FechaFiltro, ">=", desde.Date);
            filter.Add(BooleanOp.And, FacturacionServicioMedicoView.Properties.FechaFiltro, "<", hasta.Date.AddDays(1));
            bool tieneMedico = medico > 0;
            if (tieneMedico)
                filter.Add(BooleanOp.And, FacturacionServicioMedicoView.Properties.MedicoID, "=", medico);

            bool tieneServicio = servicio > 0;
            if (tieneServicio)
                filter.Add(BooleanOp.And, FacturacionServicioMedicoView.Properties.ServicioID, "=", servicio);

            EntityCollection<FacturacionServicioMedicoView> items = dalEngine.GetManyByFilter<FacturacionServicioMedicoView>(filter);
            EntityCollection<FacturacionServicioMedicoView> itemsAgrupados = new EntityCollection<FacturacionServicioMedicoView>();
            FacturacionServicioMedicoView itemAgrupado = null;
            int servicioId = 0;
            int medicoId = 0;
            foreach (FacturacionServicioMedicoView item in items)
            {
                if (servicioId != item.ServicioID || medicoId != item.MedicoID)
                {
                    // Chequeo que sea efectivamente uno nuevo y no el primero
                    if (servicioId != 0)
                        itemsAgrupados.Add(itemAgrupado);

                    itemAgrupado = new FacturacionServicioMedicoView();
                    itemAgrupado.FechaFiltro = item.FechaFiltro;
                    itemAgrupado.Matricula = item.Matricula;
                    itemAgrupado.Medico = item.Medico;
                    itemAgrupado.MedicoID = item.MedicoID;
                    itemAgrupado.Servicio = item.Servicio;
                    itemAgrupado.ServicioID = item.ServicioID;

                    itemAgrupado.Derechos = 0;
                    itemAgrupado.Honorarios = 0;
                    itemAgrupado.HonorariosInterno = 0;
                    itemAgrupado.Insumos = 0;
                    itemAgrupado.Iva = 0;
                    itemAgrupado.Modulo = 0;
                    itemAgrupado.Pacientes = 0;
                    itemAgrupado.Practicas = 0;
                    itemAgrupado.RestoInterno = 0;

                    servicioId = item.ServicioID;
                    medicoId = item.MedicoID;
                }

                itemAgrupado.Derechos += item.Derechos;
                itemAgrupado.Honorarios += item.Honorarios;
                itemAgrupado.HonorariosInterno += item.HonorariosInterno;
                itemAgrupado.Insumos += item.Insumos;
                itemAgrupado.Iva += item.Iva;
                itemAgrupado.Modulo += item.Modulo;
                itemAgrupado.Pacientes += item.Pacientes;
                itemAgrupado.Practicas += item.Practicas;
                itemAgrupado.RestoInterno += item.RestoInterno;
            }

            // Agrego el ultimo
            itemsAgrupados.Add(itemAgrupado);

            return itemsAgrupados;
        }
        #endregion

        #region PeriodoFacturacion
        /// <summary>
        /// Retorno los distintos Periodos de Facturacion de una OS - Comprobantes
        /// </summary>
        /// <param name="os">Id de la Obra Social</param>
        /// <returns>Colección de Periodos donde hay Comprobantes para la OS</returns>
        public EntityCollection<PeriodoFacturacionComprobanteView> PeriodoFacturacionComprobanteReadByOS(int os)
        {
            ReadManyCommand<PeriodoFacturacionComprobanteView> readCmd = new ReadManyCommand<PeriodoFacturacionComprobanteView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PeriodoFacturacionComprobanteView.Properties.Os,
                "=", os);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(PeriodoFacturacionComprobanteView.Properties.Ano, SortingDirection.Asc);
            sort.Add(PeriodoFacturacionComprobanteView.Properties.Mes, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public PeriodoFacturacionComprobanteView PeriodoFacturacionComprobanteReadFirst()
        {
            StringBuilder hql = new StringBuilder(" Select comp from PeriodoFacturacionComprobanteView comp ");
            hql.Append(" order by comp.Ano, comp.Mes asc ");

            // Obtengo el nro de orden consecutivo para el lote generado
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetMaxResults(1);
            PeriodoFacturacionComprobanteView resutl = dalEngine.GetByQuery<PeriodoFacturacionComprobanteView>(query);

            return resutl;

        }

        /// <summary>
        /// Retorno los distintos Periodos de Facturacion de una OS - Facturas
        /// </summary>
        /// <param name="os">Id de la Obra Social</param>
        /// <returns>Colección de Periodos donde hay Facturas para la OS</returns>
        public EntityCollection<PeriodoFacturacionFacturaView> PeriodoFacturacionFacturaReadByOS(int os)
        {
            ReadManyCommand<PeriodoFacturacionFacturaView> readCmd = new ReadManyCommand<PeriodoFacturacionFacturaView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PeriodoFacturacionFacturaView.Properties.ObraSocialID,
                "=", os);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(PeriodoFacturacionFacturaView.Properties.Ano, SortingDirection.Asc);
            sort.Add(PeriodoFacturacionFacturaView.Properties.Mes, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }
        #endregion

        #region CobranzaView
        /// <summary>
        /// Obtengo los Turnos para la Pantalla de Cobranza
        /// </summary>
        /// <param name="fecha">Fecha de los Turnos</param>
        /// <param name="traerSoloPendientes">Marca si Obtengo solo los Pendientes</param>
        /// <param name="sector">Sector del usuario conectado</param>
        /// <returns>Los Turnos que Requieren Cobranza con los Parametros Recibidos</returns>
        [MinuteTimeout]
        public virtual EntityCollection<CobranzaView> CobranzaViewRead(DateTime fecha, bool traerSoloPendientes, Sector sector, bool mostrarOtrosSectores, bool mostrarSoloSucursal, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            ReadManyCommand<CobranzaView> readCmd = new ReadManyCommand<CobranzaView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(CobranzaView.Properties.Fecha,
                ">=", fecha.Date);

            filter.Add(BooleanOp.And, CobranzaView.Properties.Fecha,
                "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, CobranzaView.Properties.EstadoId,
                "<>", (int)EstadoTurnoEnum.Cancelado);

            if (traerSoloPendientes)
            {
                filter.Add(BooleanOp.And, CobranzaView.Properties.Cobranza,
                    " IS ", null);
            }

            if (!mostrarOtrosSectores && sector != null)
            {
                filter.Add(BooleanOp.And, CobranzaView.Properties.SectorRecepcionId,
                    "=", sector.Id);
            }

            if (mostrarSoloSucursal && sector != null)
            {
                // Arma la lista de sectoresId
                EntityCollection<Sector> sectores = TurnosDalc.SectoresReadBySucursal(sector.Sucursal.Id);
                List<int> sectoresSucursal = new List<int>();
                foreach (Sector sec in sectores)
                    sectoresSucursal.Add(sec.Id);
                // Agrega el filtro
                if (sectoresSucursal.Count > 0)
                    filter.Add(BooleanOp.And,
                        CobranzaView.Properties.SectorRecepcionId,
                        "IN", sectoresSucursal.ToArray());
            }

            // filtro equipos por sector
            if (sector != null)
            {
                // equipos de la sucursal del sector                
                // y de servicios del sector
                List<int> equiposDelSector = EquiposDalc.EquiposIdsPorSector(sector, false);
                if (equiposDelSector.Count > 0)
                    filter.Add(BooleanOp.And,
                        CobranzaView.Properties.EquipoId,
                        "IN", equiposDelSector);
            }

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(CobranzaView.Properties.Fecha, SortingDirection.Asc);
            sort.Add(CobranzaView.Properties.Cobranza, SortingDirection.Asc);

            readCmd.Sort = sort;

            EntityCollection<CobranzaView> CobranzaViewItemsAux = readCmd.Execute();
            EntityCollection<CobranzaView> CobranzaViewItems = new EntityCollection<CobranzaView>();

            if (CobranzaViewItemsAux != null && CobranzaViewItemsAux.Count > 0)
            {
                foreach (CobranzaView cob in CobranzaViewItemsAux)
                {
                    FullValorizacion fullValorizacion = null;

                    Turno turno = TurnosDalc.TurnoReadById(cob.Id);
                    Entities.Valorizacion valorizacion = ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turno.Id, (int)ValorizacionTiposEnum.Admision);
                    EntityCollection<ValorizacionItem> items = valorizacion.Items;
                    turno.Valorizacion = new ValorizacionInfo(valorizacion, items);
                    fullValorizacion = new FullValorizacion(turno.Valorizacion, modalidadCoseguro);
                    bool cobro = false;

                    if (cob.Cobranza.HasValue)
                    {
                        cobro = true;
                    }
                    else
                    {
                        switch (turno.Orden.DebeOrdenMedica)
                        {
                            case DebeOrdenEnum.DepositoParcial:
                                cobro = true;
                                break;
                            case DebeOrdenEnum.DepositoTotal:
                                cobro = true;
                                break;
                            case DebeOrdenEnum.NoDebe:
                            case DebeOrdenEnum.SiDebe:
                            case DebeOrdenEnum.SiDebeAutorizacion:
                            case DebeOrdenEnum.OrdenPendiente:
                                if (turno.Valorizacion != null && turno.Valorizacion.ImporteTotalPaciente > 0)
                                    cobro = true;
                                break;
                        }
                    }
                    if (cobro)
                        CobranzaViewItems.Add(cob);
                }
            }

            return CobranzaViewItems;
        }

        public EntityCollection<DatosCobranza> DatosCobranzaReadByFechaPendientesAndSectores(string protocolo, bool traerSoloPendientes, EntityCollection<Sector> sectores, bool usuarioConCajaAsignada, int? cajaUsuarioId)
        {
            return DatosCobranzaReadByFechaPendientesAndSectores(null, null, traerSoloPendientes, sectores, usuarioConCajaAsignada, cajaUsuarioId, protocolo);
        }

        public EntityCollection<DatosCobranza> DatosCobranzaReadByFechaPendientesAndSectores(DateTime? fecha, string apellidoYNombre, bool traerSoloPendientes, EntityCollection<Sector> sectores, bool usuarioConCajaAsignada, int? cajaUsuarioId)
        {
            return DatosCobranzaReadByFechaPendientesAndSectores(fecha, apellidoYNombre, traerSoloPendientes, sectores, usuarioConCajaAsignada, cajaUsuarioId, null);
        }

        private EntityCollection<DatosCobranza> DatosCobranzaReadByFechaPendientesAndSectores(DateTime? fecha, string apellidoYNombre, bool traerSoloPendientes, EntityCollection<Sector> sectores, bool usuarioConCajaAsignada, int? cajaUsuarioId, string protocolo)
        {
            int[] tiposFormulario = new int[] { (int)TipoFormularioEnum.FacturaAnticipo, (int)TipoFormularioEnum.Factura, (int)TipoFormularioEnum.FacturaDeposito, (int)TipoFormularioEnum.ReciboMedico };

            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.DatosCobranza( ");
            hql.Append("tur.Id ");
            hql.Append(", tur.Fecha ");
            hql.Append(", tur.Orden.Paciente.ApellidoNombre ");
            hql.Append(", pt.Practica.Name ");
            hql.Append(", tur.Orden.ObraSocialPlan.ObraSocial.Name ");
            hql.Append(", pt.Medico.Name ");
            hql.Append(", pt.Medico.Apellido ");
            hql.Append(", mov.Id ");
            hql.Append(", tur.Orden.SecRecepcion.Name ");
            hql.Append(", tur.RequiereCobranza ");
            hql.Append(", tfr.Abreviatura ");
            hql.Append(", frm.Id ");
            hql.Append(", frm.Clase ");
            hql.Append(", frm.Numero ");
            hql.Append(", frm.Sucursal ");
            hql.Append(", frmPadre.Id ");
            hql.Append(", frmPadre.Clase ");
            hql.Append(", frmPadre.Numero ");
            hql.Append(", frmPadre.Sucursal ");
            hql.Append(", tur.Equipo.id ");
            hql.Append(", tur.Orden.ObraSocialPlan.ObraSocial.EsParticular ");
            hql.Append(") ");
            hql.Append("from TurnoHQL tur ");
            hql.Append("inner join tur.PracticaTurno pt ");
            hql.Append("left join tur.FormularioHQL frm ");
            hql.Append("left join frm.Formulario frmPadre ");
            hql.Append("left join tur.MovimientoCaja mov ");
            hql.Append("left join frm.TipoFormulario tfr ");
            bool sub = false;
            bool reqCobranza = true;
            hql.Append("where pt.Practica.EsSubsiguiente = :sub ");

            if (sectores != null && sectores.Count > 0)
                hql.Append("and tur.Orden.SecRecepcion in (:sectores) ");

            if (fecha.HasValue)
            {
                hql.Append("and tur.Fecha >= :fechaDesde ");
                hql.Append("and tur.Fecha < :fechaHasta ");
            }
            if (!string.IsNullOrEmpty(protocolo))               //chequear que esta linea no se rompa todoo...
                hql.Append("and tur.Orden.Protocolo.ProtocoloFull = :protocolo ");

            if (!string.IsNullOrEmpty(apellidoYNombre))
                hql.Append("and tur.Orden.Paciente.ApellidoNombre like :apellidoYNombre ");

            hql.Append("and tur.RequiereCobranza = :reqCobranza ");
            hql.Append("and tur.Estado.Id != :estado ");
            hql.Append("and (frm is null) ");

            hql.Append("and (tur.Orden.PagoDiferidoCliente is null or ");
            hql.Append("(tur.Orden.PagoDiferidoCliente is not null and tur.ImporteOrdenMedica = tur.ImportePago)) ");

            if (traerSoloPendientes == true)
                hql.Append("and mov is null ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            if (sectores != null && sectores.Count > 0)
                query.SetParameterList("sectores", sectores);

            if (fecha.HasValue)
            {
                query.SetParameter("fechaDesde", fecha.Value.Date);
                query.SetParameter("fechaHasta", fecha.Value.Date.AddDays(1));
            }

            if (!string.IsNullOrEmpty(protocolo))
                query.SetString("protocolo", protocolo);

            if (!string.IsNullOrEmpty(apellidoYNombre))
                query.SetString("apellidoYNombre", "%" + apellidoYNombre + "%");

            query.SetParameter("sub", sub);
            query.SetParameter("reqCobranza", reqCobranza);
            query.SetParameter("estado", (int)EstadoTurnoEnum.Cancelado);

            StringBuilder hql2 = new StringBuilder();
            hql2.Append("select new  enfoke.Eges.Entities.Results.DatosCobranza( ");
            hql2.Append("tur.Id ");
            hql2.Append(", tur.Fecha ");
            hql2.Append(", tur.Orden.Paciente.ApellidoNombre ");
            hql2.Append(", pt.Practica.Name ");
            hql2.Append(", tur.Orden.ObraSocialPlan.ObraSocial.Name ");
            hql2.Append(", pt.Medico.Name ");
            hql2.Append(", pt.Medico.Apellido ");
            hql2.Append(", mov.Id ");
            hql2.Append(", tur.Orden.SecRecepcion.Name ");
            hql2.Append(", tur.RequiereCobranza ");
            hql2.Append(", tfr.Abreviatura ");
            hql2.Append(", frm.Id ");
            hql2.Append(", frm.Clase ");
            hql2.Append(", frm.Numero ");
            hql2.Append(", frm.Sucursal ");
            hql2.Append(", frmPadre.Id ");
            hql2.Append(", frmPadre.Clase ");
            hql2.Append(", frmPadre.Numero ");
            hql2.Append(", frmPadre.Sucursal ");
            hql2.Append(", tur.Equipo.id ");
            hql2.Append(", tur.Orden.ObraSocialPlan.ObraSocial.EsParticular ");
            hql2.Append(") ");
            hql2.Append("from TurnoHQL tur ");
            hql2.Append("inner join tur.PracticaTurno pt ");
            hql2.Append("left join tur.FormularioHQL frm ");
            hql2.Append("left join frm.Formulario frmPadre ");
            hql2.Append("left join tur.MovimientoCaja mov ");
            hql2.Append("left join frm.TipoFormulario tfr ");
            hql2.Append("where pt.Practica.EsSubsiguiente = false ");

            if (sectores != null && sectores.Count > 0)
                hql2.Append("and tur.Orden.SecRecepcion in (:sectores) ");

            if (fecha.HasValue)
            {
                hql2.Append("and tur.Fecha >= :fechaDesde ");
                hql2.Append("and tur.Fecha < :fechaHasta ");
            }

            if (!string.IsNullOrEmpty(protocolo))
                hql2.Append("and tur.Orden.Protocolo.ProtocoloFull = :protocolo ");

            if (!string.IsNullOrEmpty(apellidoYNombre))
                hql2.Append("and tur.Orden.Paciente.ApellidoNombre like :apellidoYNombre ");

            hql2.Append("and tur.RequiereCobranza = true ");
            hql2.Append("and tur.Estado.Id != :estado ");
            hql2.Append("and ((tfr.Id = :reciboDiferencial) or ((frm.Numero is not null and tfr.Id in (:tiposFormulario))");
            hql2.Append("and (tur.Orden.PagoDiferidoCliente is null or ");
            hql2.Append("(tur.Orden.PagoDiferidoCliente is not null and tur.ImporteOrdenMedica = tur.ImportePago)))) ");

            if (traerSoloPendientes == true)
                hql2.Append("and mov is null ");

            IQuery query2 = dalEngine.CreateQuery(hql2.ToString());
            if (sectores != null && sectores.Count > 0)
                query2.SetParameterList("sectores", sectores);
            if (fecha.HasValue)
            {
                query2.SetParameter("fechaDesde", fecha.Value.Date);
                query2.SetParameter("fechaHasta", fecha.Value.Date.AddDays(1));
            }

            if (!string.IsNullOrEmpty(protocolo))
                query2.SetString("protocolo", protocolo);

            if (!string.IsNullOrEmpty(apellidoYNombre))
                query2.SetString("apellidoYNombre", "%" + apellidoYNombre + "%");

            query2.SetParameter("estado", (int)EstadoTurnoEnum.Cancelado);
            query2.SetParameterList("tiposFormulario", tiposFormulario);
            query2.SetParameter("reciboDiferencial", (int)TipoFormularioEnum.ReciboDiferencial);
            EntityCollection<DatosCobranza> aux = new EntityCollection<DatosCobranza>();
            EntityCollection<DatosCobranza> datosCob = DatosCobranzaReadByFechaPendientesAndSectoresFromMov(fecha, apellidoYNombre,
                                                                                                          traerSoloPendientes,
                                                                                                            sectores, protocolo);
            if (datosCob != null)
                aux.AddRange(datosCob);

            EntityCollection<DatosCobranza> cob1 = dalEngine.GetManyByQuery<DatosCobranza>(query);
            EntityCollection<DatosCobranza> cob2 = dalEngine.GetManyByQuery<DatosCobranza>(query2);
            cob2.AddRange(cob1);

            EntityCollection<DatosCobranza> datos = QuitarCobranzaDuplicada(cob2);
            datos = AgregaRazonSocialEmpresa(datos);

            if (datos != null)
                aux.AddRange(datos);

            if (usuarioConCajaAsignada && cajaUsuarioId.HasValue)
            {
                EntityCollection<EquipoPuntoVenta> equiposPVentaEcluidos = Context.Session.EquiposDalc.EquiposExcluidosPorCaja(cajaUsuarioId.Value);
                List<DatosCobranza> excluidos = new List<DatosCobranza>();
                foreach (DatosCobranza datoCobranza in aux)
                {
                    EquipoPuntoVenta equipoPVta = equiposPVentaEcluidos.Find(delegate(EquipoPuntoVenta ep) { return ep.Equipo.Id == datoCobranza.EquipoId; });
                    if (equipoPVta != null &&
                        ((equipoPVta.PuntoVenta.Particular && datoCobranza.ObraSocialParticular)
                        || (equipoPVta.PuntoVenta.NoParticular && !datoCobranza.ObraSocialParticular)))
                        excluidos.Add(datoCobranza);
                }

                aux.RemoveRange(excluidos);
            }

            return aux;
        }

        private EntityCollection<DatosCobranza> AgregaRazonSocialEmpresa(EntityCollection<DatosCobranza> datos)
        {
            List<int> formulariosIds = new List<int>();
            foreach (DatosCobranza datosCobranza in datos)
            {
                if (datosCobranza.Formulario.HasValue)
                    formulariosIds.Add(datosCobranza.Formulario.Value);
            }

            if (formulariosIds.Count > 0)
            {
                EntityCollection<Formulario> formularios = dalEngine.GetManyByIds<Formulario>(formulariosIds);
                foreach (DatosCobranza datosCobranza in datos)
                {
                    if (datosCobranza.Formulario.HasValue)
                    {
                        Formulario form = formularios.FindByKey(datosCobranza.Formulario.Value);
                        datosCobranza.Empresa = form.Empresa != null ? form.Empresa.RazonSocial : null;
                    }
                }
            }

            return datos;
        }

        public EntityCollection<DatosCobranza> DatosCobranzaReadByFechaPendientesAndSectoresFromMov(DateTime? fecha, string apellidoYNombre, bool traerSoloPendientes, EntityCollection<Sector> sectores, string protocolo)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.DatosCobranza( ");
            hql.Append("frm.CreateDate ");
            hql.Append(", frm.RazonSocial ");
            hql.Append(", mci.MovimientoCajaHQL.FacturaNumeracion.PuntoVenta.Sucursal.Name ");
            hql.Append(", mci.MovimientoCajaHQL.Id ");
            hql.Append(", frm.TipoFormulario.Abreviatura ");
            hql.Append(", frm.Id ");
            hql.Append(", frm.Clase ");
            hql.Append(", frm.Numero ");
            hql.Append(", frm.Sucursal ");
            hql.Append(", frmPadre.Id ");
            hql.Append(", frmPadre.Clase ");
            hql.Append(", frmPadre.Numero ");
            hql.Append(", frmPadre.Sucursal ");
            hql.Append(", frm.Empresa.RazonSocial ");
            hql.Append(") ");
            hql.Append("from MovimientoCajaItemHQL mci, FormularioHQL frm ");
            hql.Append("left join frm.Formulario frmPadre ");
            if (sectores != null && sectores.Count > 0)
                hql.Append(" , PuntoVentaSector pvs ");
            hql.Append("where frm.Id = mci.FormularioHQL.Id ");
            if (sectores != null && sectores.Count > 0)
                hql.Append("and mci.MovimientoCajaHQL.FacturaNumeracion.PuntoVenta.Id = pvs.PuntoVenta.Id and pvs.Sector.Id in (:sectores) ");
            hql.Append("and frm.Sucursal is not null ");
            hql.Append("and frm.Turno is null ");
            hql.Append("and frm.Numero is not null and frm.FechaAnulacion is null ");
            if (fecha.HasValue)
            {
                hql.Append("and frm.CreateDate >= :fechaDesde ");
                hql.Append("and frm.CreateDate < :fechaHasta ");
            }
            if (!string.IsNullOrEmpty(protocolo))
                hql.Append("and frm.Turno.Orden.Protocolo.ProtocoloFull = :protocolo ");

            if (!string.IsNullOrEmpty(apellidoYNombre))
                hql.Append("and frm.Turno.Orden.Paciente.ApellidoNombre like :apellidoYNombre ");

            hql.Append("and frmPadre is null ");
            hql.Append("and not exists(select fr.Id from Formulario fr where fr.FormularioOriginalID = frm.Id) ");
            if (traerSoloPendientes == true)
                hql.Append("and mci is null ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (sectores != null && sectores.Count > 0)
                query.SetParameterList("sectores", sectores);

            if (fecha.HasValue)
            {
                query.SetParameter("fechaDesde", fecha.Value.Date);
                query.SetParameter("fechaHasta", fecha.Value.Date.AddDays(1));
            }
            if (!string.IsNullOrEmpty(protocolo))
                query.SetString("protocolo", protocolo);

            if (!string.IsNullOrEmpty(apellidoYNombre))
                query.SetString("apellidoYNombre", "%" + apellidoYNombre + "%");

            return QuitarCobranzaDuplicada(dalEngine.GetManyByQuery<DatosCobranza>(query));
        }

        public EntityCollection<CobranzaView> CobranzaViewReadByParametros(PuntoVenta puntoVenta, DateTime fecha, bool traerSoloPendientes, EntityCollection<Sector> sectores)
        {

            EntityCollection<DatosCobranza> datoCobranzas = DatosCobranzaReadByFechaPendientesAndSectores(fecha, null, traerSoloPendientes, sectores, false, null);//(fecha,null, traerSoloPendientes, sectores, false, null);

            ReadManyCommand<CobranzaView> readCmd = new ReadManyCommand<CobranzaView>(dalEngine);
            List<int> sectoresId = new List<int>();
            if (sectores != null && sectores.Count > 0)
            {
                foreach (Sector sect in sectores)
                    sectoresId.Add(sect.Id);
            }

            Filter filter = new Filter();

            filter.Add(CobranzaView.Properties.Fecha,
                ">=", fecha.Date);

            filter.Add(BooleanOp.And, CobranzaView.Properties.Fecha,
                "<", fecha.Date.AddDays(1));

            filter.Add(BooleanOp.And, CobranzaView.Properties.EstadoId,
                "<>", (int)EstadoTurnoEnum.Cancelado);

            if (traerSoloPendientes)
            {
                filter.Add(BooleanOp.And, CobranzaView.Properties.Cobranza,
                    " IS ", null);
            }

            if (sectoresId != null && sectoresId.Count > 0)
                filter.Add(BooleanOp.And, CobranzaView.Properties.SectorRecepcionId,
                           "IN", sectoresId.ToArray());
            readCmd.Filter = filter;

            //Sort sort = new Sort();
            //sort.Add(CobranzaView.Properties.Fecha, SortingDirection.Asc);
            //sort.Add(CobranzaView.Properties.Cobranza, SortingDirection.Asc);

            //readCmd.Sort = sort;

            EntityCollection<CobranzaView> CobranzaViewItemsAux = readCmd.Execute();
            CobranzaViewItemsAux = this.QuitarCobranzaDuplicada(CobranzaViewItemsAux);
            EntityCollection<CobranzaView> CobranzaViewItems = new EntityCollection<CobranzaView>();

            if (CobranzaViewItemsAux != null && CobranzaViewItemsAux.Count > 0)
            {
                foreach (CobranzaView cob in CobranzaViewItemsAux)
                {
                    if (cob.Id > 0)
                    {
                        bool cobro = false;
                        if (cob.Cobranza.HasValue)
                            cobro = true;
                        else
                        {
                            if (cob.RequiereCobranza == true)
                                cobro = true;
                        }

                        if (cobro)
                            CobranzaViewItems.Add(cob);
                    }
                    else
                        CobranzaViewItems.Add(cob);
                }
            }

            return CobranzaViewItems;
        }


        private DatosCobranza ObtenerUltimaCobranza(EntityCollection<DatosCobranza> cobranzas, int turnoId)
        {
            DatosCobranza cobranza = null;
            foreach (DatosCobranza cob in cobranzas)
                if (turnoId == cob.Id.GetValueOrDefault(0))
                    cobranza = cob;

            return cobranza;
        }

        private EntityCollection<DatosCobranza> QuitarCobranzaDuplicada(EntityCollection<DatosCobranza> cobranzas)
        {
            IList<int> ids = new List<int>();
            IList<int> idsFormularios = new List<int>();
            EntityCollection<DatosCobranza> cabranzaAux = new EntityCollection<DatosCobranza>();
            if (cobranzas != null && cobranzas.Count > 0)
            {
                //cabranzaAux.Add(cobranzas[0]);

                //if (cobranzas[0].Id.HasValue == true && cobranzas[0].Id.Value > 0)
                //    ids.Add(cobranzas[0].Id.Value);
                //else
                //    idsFormularios.Add(cobranzas[0].Formulario.Value);

                foreach (DatosCobranza cob in cobranzas)
                {
                    if (cob.Id.HasValue == true && cob.Id.Value > 0)
                    {
                        if (!ids.Contains(cob.Id.Value))
                        {
                            cabranzaAux.Add(ObtenerUltimaCobranza(cobranzas, cob.Id.Value));
                            ids.Add(cob.Id.Value);
                        }
                    }
                    else
                    {
                        if (!idsFormularios.Contains(cob.Formulario.Value))
                        {
                            cabranzaAux.Add(cob);
                            idsFormularios.Add(cob.Formulario.Value);
                        }
                    }
                }
            }

            return cabranzaAux;
        }

        private EntityCollection<CobranzaView> QuitarCobranzaDuplicada(EntityCollection<CobranzaView> cobranzas)
        {
            IList<int> ids = new List<int>();
            IList<int> idsFormularios = new List<int>();
            EntityCollection<CobranzaView> cabranzaAux = new EntityCollection<CobranzaView>();
            if (cobranzas != null && cobranzas.Count > 0)
            {
                cabranzaAux.Add(cobranzas[0]);

                if (cobranzas[0].Id > 0)
                    ids.Add(cobranzas[0].Id);
                else
                    idsFormularios.Add(cobranzas[0].FormularioId);

                foreach (CobranzaView cob in cobranzas)
                {
                    if (cob.Id > 0)
                    {
                        if (!ids.Contains(cob.Id))
                        {
                            cabranzaAux.Add(cob);
                            ids.Add(cob.Id);
                        }
                    }
                    else
                    {
                        if (!idsFormularios.Contains(cob.FormularioId))
                        {
                            cabranzaAux.Add(cob);
                            idsFormularios.Add(cob.FormularioId);
                        }
                    }
                }
            }

            return cabranzaAux;
        }

        #endregion

        #region Formato Factura

        public EntityCollection<TipoAgrupamientoImpresion> TipoAgrupamientoImpresionReadAll()
        {
            return dalEngine.GetAll<TipoAgrupamientoImpresion>(TipoAgrupamientoImpresion.Properties.Id);
        }

        public EntityCollection<FacturaImpresion> FacturaImpresionReadAll()
        {
            return dalEngine.GetAll<FacturaImpresion>(FacturaImpresion.Properties.Id);
        }






        public EntityCollection<FacturaImpresion> FacturaImpresionReadNoEliminados()
        {
            return dalEngine.GetManyByProperty<FacturaImpresion>(FacturaImpresion.Properties.Deleted, false, FacturaImpresion.Properties.Descripcion, enfoke.Data.SortOrder.Ascending);
        }

        public EntityCollection<FacturaGrupoFormato> FacturaGrupoFormatoReadNoEliminados()
        {
            return dalEngine.GetManyByProperty<FacturaGrupoFormato>(FacturaGrupoFormato.Properties.Deleted, false, FacturaGrupoFormato.Properties.Descripcion, enfoke.Data.SortOrder.Ascending);
        }

        public FacturaFormato FacturaFormatoReadById(int facturaFormatoId)
        {
            return dalEngine.GetById<FacturaFormato>(facturaFormatoId);
        }

        public EntityCollection<FacturaFormato> FacturaFormatoReadByFacturaGrupoFormato(int facturaGrupoFormatoId)
        {
            return dalEngine.GetManyByProperty<FacturaFormato>(FacturaFormato.Properties.Fgf.Id, facturaGrupoFormatoId, FacturaFormato.Properties.Orden, enfoke.Data.SortOrder.Ascending);
        }

        public EntityCollection<FacturaFormato> FacturaFormatoReadByFacturaGrupoFormato(List<int> facturaGrupoFormatoIds)
        {
            return dalEngine.GetManyByPropertyList<FacturaFormato>(FacturaFormato.Properties.Fgf.Id, facturaGrupoFormatoIds);
        }

        public EntityCollection<FacturaFormato> FacturaFormatoNoDeletedReadByFacturaGrupoFormato(List<int> facturaGrupoFormatoId)
        {
            if (facturaGrupoFormatoId == null || facturaGrupoFormatoId.Count == 0)
                return new EntityCollection<FacturaFormato>();

            StringBuilder hql = new StringBuilder(" from FacturaFormato formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.Fgf.Id IN (:fgf) ");
            hql.Append(" order by formato.Orden ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("fgf", facturaGrupoFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormato>(query);
        }

        public EntityCollection<FacturaFormato> FacturaFormatoNoDeletedReadByFacturaGrupoFormato(int facturaGrupoFormatoId)
        {
            StringBuilder hql = new StringBuilder(" from FacturaFormato formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.Fgf.Id = :fgf");
            hql.Append(" order by formato.Orden ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fgf", facturaGrupoFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormato>(query);
        }

        public EntityCollection<FacturaFormatoSucursal> FacturaFormatoSucursalReadByFacturaFormato(int facturaFormatoId)
        {
            StringBuilder hql = new StringBuilder(" from FacturaFormatoSucursal formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.FafId = :facturaFormatoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormatoSucursal>(query);
        }

        public EntityCollection<FacturaFormatoSucursal> FacturaFormatoSucursalReadByFacturaFormatoAndCentro(int facturaFormatoId, int centro)
        {
            StringBuilder hql = new StringBuilder("from FacturaFormatoSucursal formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.FafId = :facturaFormatoId");
            hql.Append(" and formato.Sucursal.Id = :centro");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);
            query.SetParameter("centro", centro);

            return dalEngine.GetManyByQuery<FacturaFormatoSucursal>(query);
        }

        public bool HayFacturaFormatoSucursalReadByFacturaFormatoNotCentro(int facturaFormatoId, int centro)
        {
            StringBuilder hql = new StringBuilder("from FacturaFormatoSucursal formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.FafId = :facturaFormatoId");
            hql.Append(" and formato.Sucursal.Id <> :centro");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);
            query.SetParameter("centro", centro);
            query.SetMaxResults(1);
            object obj = query.UniqueResult();
            return (obj != null);
        }

        public EntityCollection<FacturaFormatoServicio> FacturaFormatoServicioReadByFacturaFormato(int facturaFormatoId)
        {
            StringBuilder hql = new StringBuilder(" from FacturaFormatoServicio formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.FafId = :facturaFormatoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormatoServicio>(query);
        }

        public EntityCollection<FacturaFormatoServicio> FacturaFormatoServicioReadByFacturaFormato(List<int> facturaFormatoIds)
        {
            if (facturaFormatoIds == null || facturaFormatoIds.Count == 0)
                return new EntityCollection<FacturaFormatoServicio>();
            
            var query = from ffs in dalEngine.Query<FacturaFormatoServicio>() where !ffs.Deleted && facturaFormatoIds.Contains(ffs.FafId) select ffs;

            return query.ToEntityCollection();
        }

        public EntityCollection<FacturaFormatoPractica> FacturaFormatoPracticaReadByFacturaFormato(int facturaFormatoId)
        {
            StringBuilder hql = new StringBuilder(" from FacturaFormatoPractica formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.FafId = :facturaFormatoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormatoPractica>(query);
        }

        public EntityCollection<FacturaFormatoEspecialidad> FacturaFormatoEspecialidadReadByFacturaFormato(List<int> facturaFormatoIds)
        {
            var query = from ffe in dalEngine.Query<FacturaFormatoEspecialidad>() where !ffe.Deleted && facturaFormatoIds.Contains(ffe.Faf.Id) select ffe;

            return query.ToEntityCollection();
        }

        public EntityCollection<FacturaFormatoEspecialidad> FacturaFormatoEspecialidadReadByFacturaFormato(int facturaFormatoId)
        {
            StringBuilder hql = new StringBuilder(" from FacturaFormatoEspecialidad formato ");
            hql.Append(" where formato.Deleted = false");
            hql.Append(" and formato.Faf.Id = :facturaFormatoId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("facturaFormatoId", facturaFormatoId);

            return dalEngine.GetManyByQuery<FacturaFormatoEspecialidad>(query);
        }

        public void FacturaFormatoPracticaUpdateMany(EntityCollection<FacturaFormatoPractica> colFacturaFormatoPractica)
        {
            dalEngine.UpdateCollection(colFacturaFormatoPractica);
        }

        public void FacturaFormatoSucursalUpdateMany(EntityCollection<FacturaFormatoSucursal> colFacturaFormatoSucursal)
        {
            dalEngine.UpdateCollection(colFacturaFormatoSucursal);
        }

        public void FacturaFormatoServicioUpdateMany(EntityCollection<FacturaFormatoServicio> colFacturaFormatoServicio)
        {
            dalEngine.UpdateCollection(colFacturaFormatoServicio);
        }

        public void FacturaFormatoEspecialidadUpdateMany(EntityCollection<FacturaFormatoEspecialidad> colFacturaFormatoEspecialidad)
        {
            dalEngine.UpdateCollection(colFacturaFormatoEspecialidad);
        }




        public FacturaImpresion FacturaImpresionUpdate(FacturaImpresion fai)
        {
            return dalEngine.Update(fai);
        }






        #region VwOsResumenEstadosAnterior

        //public EntityCollection<vwOsResumenEstados> ObraSocialEmisionFacturaReadByPeriodo(int? mesPeriodo, int? anioPeriodo)
        //{
        //    Filter filter = new Filter();
        //    if (mesPeriodo.HasValue && anioPeriodo.HasValue)
        //    {
        //        filter.Add(BooleanOp.And, vwOsResumenEstados.Properties.Mes, " = ", mesPeriodo.Value);
        //        filter.Add(BooleanOp.And, vwOsResumenEstados.Properties.Ano, " = ", anioPeriodo.Value);
        //    }

        //    ReadManyCommand<vwOsResumenEstados> readCmd = new ReadManyCommand<vwOsResumenEstados>(dalEngine);
        //    Sort sortDirection = new Sort();
        //    sortDirection.Add(vwOsResumenEstados.Properties.Nombre, SortingDirection.Asc);
        //    readCmd.Filter = filter;
        //    readCmd.Sort = sortDirection;

        //    return this.AgruparPorOS(readCmd.Execute());
        //}

        #endregion

        #region VwOsResumenEstados

        private EntityCollection<vwOsResumenEstados> ObraSocialEmisionFacturaReadByPeriodo(int? mesPeriodo, int? anioPeriodo, int? usrId, List<int> osIds, bool gerenciadora)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("SELECT new enfoke.Eges.Entities.vwOsResumenEstados( ");
            hql.Append("ose.AFacturar ");
            hql.Append(", ose.Ano ");
            hql.Append(", ose.Facturadas ");
            hql.Append(", ose.Id ");
            hql.Append(", ose.Ingresadas ");
            hql.Append(", ose.Mes ");
            hql.Append(", ose.NoControladas ");
            hql.Append(", ose.NoFacturadas ");
            hql.Append(gerenciadora ? ", osoGeren.Name " : ", oso.Name ");
            hql.Append(gerenciadora ? ", osoGeren.Id " : ", ose.Os ");
            hql.Append(", ose.PreFacturadas, ose.Debitadas, ose.AReFacturar, ose.ReFacturadas, ose.BajaDefinitiva ");
            hql.Append(" ) ");
            hql.Append("from ObraSocialEmisionFactura ose ");

            if (usrId.HasValue)
            {
                hql.Append(" , UserProfileFactObraSocial userOs ");
                hql.Append(" , UserProfileFacturacion perfil ");
            }

            hql.Append(",ObraSocial oso ");

            if (gerenciadora)
                hql.Append(",ObraSocial osoGeren ");

            hql.Append("where oso.Id = ose.Os ");

            if (gerenciadora)
                hql.Append(" and osoGeren.Id = oso.GerenciadoraID ");

            if (osIds != null && osIds.Count > 0)
                hql.Append(" and oso.Id in (:osIds) ");

            if (usrId.HasValue)
            {
                hql.Append(" and userOs.ObraSocialId = oso.Id ");
                hql.Append(" and userOs.UserProfileFactId = perfil.Id ");
                hql.Append(" and perfil.SecurityUserId = :usrId ");
            }

            if (gerenciadora)
                hql.Append("and oso.GerenciadoraID is not null ");
            else
                hql.Append("and oso.EsGerenciadora = false ");

            if (mesPeriodo.HasValue && anioPeriodo.HasValue)
            {
                hql.Append("and  ose.Mes = :mesPeriodo ");
                hql.Append("and  ose.Ano = :anioPeriodo ");
            }

            hql.Append("order by  oso.Name asc");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (usrId.HasValue)
                query.SetParameter("usrId", usrId.Value);

            if (osIds != null && osIds.Count > 0)
                query.SetParameterList("osIds", osIds);

            if (mesPeriodo.HasValue && anioPeriodo.HasValue)
            {
                query.SetParameter("mesPeriodo", mesPeriodo.Value);
                query.SetParameter("anioPeriodo", anioPeriodo.Value);
            }

            EntityCollection<vwOsResumenEstados> result = new EntityCollection<vwOsResumenEstados>();
            result = dalEngine.GetManyByQuery<vwOsResumenEstados>(query);

            if (gerenciadora)
                result = AgruparGerenciadora(result);

            result.SortByProperty(vwOsResumenEstados.Properties.Nombre);

            return result;
        }

        private EntityCollection<vwOsResumenEstados> AgruparGerenciadora(EntityCollection<vwOsResumenEstados> obraSocialEmisionFactura)
        {
            if (obraSocialEmisionFactura == null || obraSocialEmisionFactura.Count == 0)
                return new EntityCollection<vwOsResumenEstados>();

            EntityCollection<vwOsResumenEstados> itemsAgrupados = new EntityCollection<vwOsResumenEstados>();
            Dictionary<int, vwOsResumenEstados> agrupadas = new Dictionary<int, vwOsResumenEstados>();
            foreach (vwOsResumenEstados item in obraSocialEmisionFactura)
            {
                vwOsResumenEstados ore;
                if (agrupadas.TryGetValue(item.KeyAgrupamientoGerenciadora, out ore))
                {
                    ore.Ingresadas += item.Ingresadas;
                    ore.NoControladas += item.NoControladas;
                    ore.NoFacturadas += item.NoFacturadas;
                    ore.AFacturar += item.AFacturar;
                    ore.PreFacturadas += item.PreFacturadas;
                    ore.Facturadas += item.Facturadas;

                }
                else
                {
                    ore = new vwOsResumenEstados(item.AFacturar, item.Ano, item.Facturadas, item.Id, item.Ingresadas, item.Mes, item.NoControladas, item.NoFacturadas, item.Nombre, item.OsId, item.PreFacturadas, item.Debitadas, item.AReFacturar, item.ReFacturadas, item.BajaDefinitiva);
                    agrupadas.Add(item.KeyAgrupamientoGerenciadora, ore);
                }
            }

            foreach (KeyValuePair<int, vwOsResumenEstados> agrupado in agrupadas)
                itemsAgrupados.Add(agrupado.Value);

            return itemsAgrupados;
        }

        public EntityCollection<vwOsResumenEstados> ObraSocialEmisionFacturaReadByPeriodo(int? mesPeriodo, int? anioPeriodo, bool agrupaGerenciadora)
        {
            EntityCollection<vwOsResumenEstados> resumen = new EntityCollection<vwOsResumenEstados>();
            EntityCollection<vwOsResumenEstados> resumenOs = ObraSocialEmisionFacturaReadByPeriodo(mesPeriodo, anioPeriodo, null, null, false);
            resumen.AddRange(resumenOs);

            if (agrupaGerenciadora)
            {
                EntityCollection<vwOsResumenEstados> resumenGer = ObraSocialEmisionFacturaReadByPeriodo(mesPeriodo, anioPeriodo, null, null, true);
                resumen.AddRange(resumenGer);
            }

            return this.AgruparPorOS(resumen);
        }

        public EntityCollection<vwOsResumenEstados> AgruparPorOS(EntityCollection<vwOsResumenEstados> colOsEmisionFactura)
        {
            if (colOsEmisionFactura.Count == 0)
                return colOsEmisionFactura;

            int AFacturar = 0;
            int Facturado = 0;
            int Ingresados = 0;
            int NoControladas = 0;
            int NoFacturar = 0;
            int PreFacturado = 0;
            int Debitado = 0;
            int AReFacturar = 0;
            int ReFacturado = 0;
            int BajaDefinitiva = 0;
            string osAnterior = "";
            int osIdAnterior = 0;
            vwOsResumenEstados datosOsEmisionFacturaAgrupado;
            colOsEmisionFactura.SortByProperty(vwOsResumenEstados.Properties.Nombre);
            osAnterior = colOsEmisionFactura[0].Nombre;
            osIdAnterior = colOsEmisionFactura[0].OsId;
            EntityCollection<vwOsResumenEstados> colOsEmisionFacturaAgrupado = new EntityCollection<vwOsResumenEstados>();
            foreach (vwOsResumenEstados os in colOsEmisionFactura)
            {
                if (os.Nombre.Equals(osAnterior))
                {
                    osIdAnterior = os.OsId;
                    osAnterior = os.Nombre;
                    AFacturar += os.AFacturar;
                    Facturado += os.Facturadas;
                    Ingresados += os.Ingresadas;
                    NoControladas += os.NoControladas;
                    NoFacturar += os.NoFacturadas;
                    PreFacturado += os.PreFacturadas;
                    Debitado += os.Debitadas;
                    AReFacturar += os.AReFacturar;
                    ReFacturado += os.ReFacturadas;
                    BajaDefinitiva += os.BajaDefinitiva;
                }
                else
                {
                    datosOsEmisionFacturaAgrupado = new vwOsResumenEstados();
                    datosOsEmisionFacturaAgrupado.Nombre = osAnterior;
                    datosOsEmisionFacturaAgrupado.OsId = osIdAnterior;
                    datosOsEmisionFacturaAgrupado.AFacturar = AFacturar;
                    datosOsEmisionFacturaAgrupado.Facturadas = Facturado;
                    datosOsEmisionFacturaAgrupado.Ingresadas = Ingresados;
                    datosOsEmisionFacturaAgrupado.NoControladas = NoControladas;
                    datosOsEmisionFacturaAgrupado.NoFacturadas = NoFacturar;
                    datosOsEmisionFacturaAgrupado.PreFacturadas = PreFacturado;
                    datosOsEmisionFacturaAgrupado.Debitadas = Debitado;
                    datosOsEmisionFacturaAgrupado.AReFacturar = AReFacturar;
                    datosOsEmisionFacturaAgrupado.ReFacturadas = ReFacturado;
                    datosOsEmisionFacturaAgrupado.BajaDefinitiva = BajaDefinitiva;
                    colOsEmisionFacturaAgrupado.Add(datosOsEmisionFacturaAgrupado);

                    osIdAnterior = os.OsId;
                    osAnterior = os.Nombre;
                    AFacturar = os.AFacturar;
                    Facturado = os.Facturadas;
                    Ingresados = os.Ingresadas;
                    NoControladas = os.NoControladas;
                    NoFacturar = os.NoFacturadas;
                    PreFacturado = os.PreFacturadas;
                    Debitado = os.Debitadas;
                    AReFacturar = os.AReFacturar;
                    ReFacturado = os.ReFacturadas;
                    BajaDefinitiva = os.BajaDefinitiva;
                }
            }

            datosOsEmisionFacturaAgrupado = new vwOsResumenEstados();
            datosOsEmisionFacturaAgrupado.Nombre = osAnterior;
            datosOsEmisionFacturaAgrupado.OsId = osIdAnterior;
            datosOsEmisionFacturaAgrupado.AFacturar = AFacturar;
            datosOsEmisionFacturaAgrupado.Facturadas = Facturado;
            datosOsEmisionFacturaAgrupado.Ingresadas = Ingresados;
            datosOsEmisionFacturaAgrupado.NoControladas = NoControladas;
            datosOsEmisionFacturaAgrupado.NoFacturadas = NoFacturar;
            datosOsEmisionFacturaAgrupado.PreFacturadas = PreFacturado;
            datosOsEmisionFacturaAgrupado.Debitadas = Debitado;
            datosOsEmisionFacturaAgrupado.AReFacturar = AReFacturar;
            datosOsEmisionFacturaAgrupado.ReFacturadas = ReFacturado;
            datosOsEmisionFacturaAgrupado.BajaDefinitiva = BajaDefinitiva;
            colOsEmisionFacturaAgrupado.Add(datosOsEmisionFacturaAgrupado);

            return colOsEmisionFacturaAgrupado;
        }

        public EntityCollection<vwOsResumenEstados> ObraSocialEmisionFacturaReadAll(bool agrupaGerenciadora)
        {
            EntityCollection<vwOsResumenEstados> resumen = new EntityCollection<vwOsResumenEstados>();
            EntityCollection<vwOsResumenEstados> resumenOs = ObraSocialEmisionFacturaReadByPeriodo(null, null, null, null, false);
            resumen.AddRange(resumenOs);

            if (agrupaGerenciadora)
            {
                EntityCollection<vwOsResumenEstados> resumenGer = ObraSocialEmisionFacturaReadByPeriodo(null, null, null, null, true);
                resumen.AddRange(resumenGer);
            }

            return resumen;
        }

        public EntityCollection<vwOsResumenEstados> ObraSocialEmisionFacturaReadByPerfilUsuarioAndPeriodo(int usrId, int? mesPeriodo, int? anioPeriodo, bool agrupaGerenciadora)
        {
            EntityCollection<vwOsResumenEstados> resumen = new EntityCollection<vwOsResumenEstados>();
            EntityCollection<vwOsResumenEstados> resumenOs = ObraSocialEmisionFacturaReadByPeriodo(mesPeriodo, anioPeriodo, usrId, null, false);
            resumen.AddRange(resumenOs);

            if (agrupaGerenciadora)
            {
                EntityCollection<vwOsResumenEstados> resumenGer = ObraSocialEmisionFacturaReadByPeriodo(mesPeriodo, anioPeriodo, usrId, null, true);
                resumen.AddRange(resumenGer);
            }

            return resumen;
        }

        public EntityCollection<vwOsResumenEstados> OsEmisionFacturaViewReadObrasSociales(List<int> osIds, bool agrupaGerenciadora)
        {
            if (osIds == null || osIds.Count == 0)
                return new EntityCollection<vwOsResumenEstados>();

            EntityCollection<vwOsResumenEstados> resumen = new EntityCollection<vwOsResumenEstados>();
            EntityCollection<vwOsResumenEstados> resumenOs = ObraSocialEmisionFacturaReadByPeriodo(null, null, null, osIds, false);
            resumen.AddRange(resumenOs);

            if (agrupaGerenciadora)
            {
                EntityCollection<vwOsResumenEstados> resumenGer = ObraSocialEmisionFacturaReadByPeriodo(null, null, null, osIds, true);
                resumen.AddRange(resumenGer);
            }

            return resumen;
        }


        #endregion


        public EntityCollection<ObraSocialComprobante> ObraSocialComprobanteReadByObraSocialIdsAndPeriodo(
            List<int> idsObrasSociales, PeriodoFacturacionComprobanteView periodo,
            string txtBusqueda, ComprobanteSearchTypeEnum tipoBusqueda,
            bool facturados, bool noFacturados, bool anulados)
        {
            EntityCollection<ObraSocialComprobante> result = new EntityCollection<ObraSocialComprobante>();

            // Si no selecciono ningun Check no traigo nada
            if ((facturados || noFacturados || anulados) == false)
            {
                foreach (int idObraSocial in idsObrasSociales)
                    result.Add(new ObraSocialComprobante(idObraSocial));

                return result;
            }

            string numeroComprobante = txtBusqueda.Trim().Replace(" ", "%") + "%";

            string hql = "SELECT new enfoke.Eges.Entities.Results.ObraSocialComprobante(cv.ObraSocialID, " +
                                                                                       "cv.Id, " +
                                                                                       "cv.FacturaId) " +
                         "FROM ComprobanteView cv " +
                         "WHERE cv.ObraSocialID IN (:idsObrasSociales) ";

            if (String.IsNullOrEmpty(txtBusqueda) == false &&
                tipoBusqueda == ComprobanteSearchTypeEnum.Comprobante)
                hql += "AND cv.Numero LIKE :numeroComprobante ";

            // Armo el Filtro del Periodo
            if (periodo.Ano != 0 && periodo.Mes != 0)
                hql += "AND cv.Ano = :ano " +
                       "AND cv.Mes = :mes ";

            // Agrego los filtros referentes a los "estados" de los comprobantes
            //anulados	facturados	noFacturados
            //    0		    0	    	0       -> no filtro por nada (situación considerada al principio)
            //    0		    0	    	1       -> filtro por NumeroFactura IS null AND FechaAnulacion IS null
            //    0		    1	    	0       -> filtro por NumeroFactura IS NOT null AND FechaAnulacion IS null
            //    0		    1	    	1       -> filtro por FechaAnulacion IS null
            //    1		    0	    	0       -> filtro por FechaAnulacion IS NOT null
            //    1		    0	    	1       -> filtro por (NumeroFactura IS null AND FechaAnulacion IS null) OR FechaAnulacion IS NOT null
            //    1	    	1	    	0       -> filtro por (NumeroFactura IS NOT null AND FechaAnulacion IS null) OR FechaAnulacion IS NOT null
            //    1	    	1	    	1       -> no filtro por nada

            if ((anulados && facturados && noFacturados) == false)
            {
                if (anulados && facturados)
                    hql += "AND ((cv.NumeroFactura IS NOT null AND cv.FechaAnulacion IS null) OR cv.FechaAnulacion IS NOT null) ";
                else if (anulados && noFacturados)
                    hql += "AND ((cv.NumeroFactura IS null AND cv.FechaAnulacion IS null) OR cv.FechaAnulacion IS NOT null) ";
                else if (anulados)
                    hql += "AND cv.FechaAnulacion IS NOT null ";
                else if (facturados && noFacturados)
                    hql += "AND cv.FechaAnulacion IS null ";
                else if (facturados)
                    hql += "AND cv.NumeroFactura IS NOT null AND cv.FechaAnulacion IS null ";
                else
                    hql += "AND cv.NumeroFactura IS null AND cv.FechaAnulacion IS null ";
            }

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("idsObrasSociales", idsObrasSociales.ToArray());

            if (String.IsNullOrEmpty(txtBusqueda) == false &&
                tipoBusqueda == ComprobanteSearchTypeEnum.Comprobante)
                query.SetParameter("numeroComprobante", numeroComprobante);

            if (periodo.Ano != 0 && periodo.Mes != 0)
            {
                query.SetParameter("ano", periodo.Ano);
                query.SetParameter("mes", periodo.Mes);
            }

            result = dalEngine.GetManyByQuery<ObraSocialComprobante>(query);

            return AgruparObraSocialComprobante(result, idsObrasSociales);
        }

        private EntityCollection<ObraSocialComprobante> AgruparObraSocialComprobante(
            EntityCollection<ObraSocialComprobante> obrasSociales, List<int> idsObrasSociales)
        {
            EntityCollection<ObraSocialComprobante> result = new EntityCollection<ObraSocialComprobante>();

            foreach (ObraSocialComprobante osc in obrasSociales)
            {
                #region predicate
                Predicate<ObraSocialComprobante> predicate = delegate(ObraSocialComprobante compare)
                 {
                     return compare.ObraSocialID == osc.ObraSocialID;
                 };
                #endregion

                ObraSocialComprobante aux = result.Find(predicate);

                if (aux == null)
                    result.Add(osc);
                else
                    aux.UnirObraSocial(osc);
            }

            // Agrego las obras sociales que no hayan sido retornadas por la consulta
            foreach (int idObraSocial in idsObrasSociales)
            {
                #region predicate
                Predicate<ObraSocialComprobante> predicate = delegate(ObraSocialComprobante compare)
                {
                    return compare.ObraSocialID == idObraSocial;
                };
                #endregion

                ObraSocialComprobante aux = result.Find(predicate);

                if (aux == null)
                    result.Add(new ObraSocialComprobante(idObraSocial));
            }

            return result;
        }

        #endregion

        #region Perfil Usuario Facturacion

        public EntityCollection<UserProfileFactObraSocial> UserProfileFactObraSocialReadByUserProfileFacturacion(int upfId)
        {
            return dalEngine.GetManyByProperty<UserProfileFactObraSocial>(UserProfileFactObraSocial.Properties.UserProfileFactId, upfId);
        }






        public UserProfileFacturacion UserProfileFacturacionReadById(int id)
        {
            return dalEngine.GetById<UserProfileFacturacion>(id);
        }

        public UserProfileFacturacion UserProfileFacturacionReadByUser(int usrId)
        {
            return dalEngine.GetByProperty<UserProfileFacturacion>(UserProfileFacturacion.Properties.SecurityUserId, usrId);
        }

        public void UserProfileFactObraSocialDeleteByUserProfileFacturacion(UserProfileFactObraSocial upo)
        {
            dalEngine.Delete(upo);
        }

        public int UserProfileFacturacionReadMaxUsrProFacOS()
        {
            StringBuilder hql = new StringBuilder(" select max(perfil.UserProfileFactOsId) from UserProfileFacturacion perfil ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetLockMode("lock", global::NHibernate.LockMode.Read);
            object maxId = query.UniqueResult();

            if (maxId != null)
                return (int)maxId;
            else
                return 0;

        }
        #endregion

        #region Listados de Rendicion

        [Private]
        public EntityCollection<ComprobanteItemRefacturacion> ComprobanteItemRefacturacionesReadByComprobanteId(int comprobanteId)
        {
            var query = from cir in dalEngine.Query<ComprobanteItemRefacturacion>()
                        join coi in dalEngine.Query<ComprobanteItem>() on cir.ComprobanteItemId equals coi.Id
                        where coi.ComprobanteID == comprobanteId && coi.DeleteDate == null
                        select new ComprobanteItemRefacturacion(coi.PracticaTurnoID, cir);

            return query.ToEntityCollection();
        }

        [Private]
        public EntityCollection<RendicionDebitosItem> RendicionDebitosSinAgruparReadByComprobante(int comprobanteId)
        {
            var query = from cid in dalEngine.Query<ComprobanteItemDebito>()
                        join coi in dalEngine.Query<ComprobanteItem>() on cid.ComprobanteItemId equals coi.Id
                        join prt in dalEngine.Query<PracticaTurno>() on coi.PracticaTurnoID equals prt.Id
                        join tur in dalEngine.Query<Turno>() on prt.TurnoId equals tur.Id
                        join pac in dalEngine.Query<Paciente>() on tur.Orden.PacienteId equals pac.Id
                        join nc in dalEngine.Query<Factura>() on cid.NotaCreditoId equals nc.Id
                        where
                            coi.ComprobanteID == comprobanteId
                            && nc.FechaAnulacion == null
                            && coi.DeleteDate == null
                        select new RendicionDebitosItem(prt.Id, tur.Orden.NumeroAfiliado, pac.ApellidoNombre, tur.Fecha, prt.Practica.Code, coi.Cantidad, coi.Honorarios + +coi.Derechos + coi.Modulo + coi.Insumos, coi.Cantidad, cid.ImporteDebito, prt.Practica.ServicioEspecialidad.Servicio.Name, nc.NroTramite, nc.TipoFactura, nc.NumeroFiscal, nc.NumeroTalonario);

            return query.ToEntityCollection();
        }

        public ObraSocialName ObraSocialReadByComprobanteId(int comprobanteId)
        {
            var query = from osn in dalEngine.Query<ObraSocialName>()
                        join com in dalEngine.Query<Comprobante>() on osn.Id equals com.ObraSocialID
                        select osn;
            return query.First();
        }

        [Private]
        public EntityCollection<ComprobanteItem> ComprobantesItemsRefacturados(int comprobantePadreId)
        {
            var query = from comRefact in dalEngine.Query<Comprobante>()
                        join coiRefact in dalEngine.Query<ComprobanteItem>() on comRefact.Id equals coiRefact.ComprobanteID
                        where
                            comRefact.ComprobantePadreID == comprobantePadreId
                            && comRefact.FechaAnulacion == null
                            && coiRefact.DeleteDate == null
                        select coiRefact;

            return query.ToEntityCollection();
        }

        public EntityCollection<RendicionConsPorServModuloView> RendicionConsPorServicioModuloViewReadByComprobanteAndModulo(int comprobanteId, int? tipoPlan, int? servicioId, int? centroId, int? obraSocialId, int? planId, bool soloPracticasImagen, bool soloPracticasConsulta, TipoParametroOrdenEnum tipoOrden, int practicaModuloId, int turnoId)
        {
            //StringBuilder hql = new StringBuilder(" Select rcp from RendicionConsPorServModuloView rcp ");
            //hql.Append(" where rcp.ComprobanteId = :comprobanteId");
            //hql.Append(" and  rcp.PracticaModulo = :practicaModuloId ");
            //hql.Append(" and  rcp.Id = :turnoId ");
            //if (soloPracticasImagen ^ soloPracticasConsulta)
            //    hql.Append("   and rcp.PracticaEsConsulta = :soloPracticasConsulta");

            //    if (tipoPlan.HasValue && tipoPlan.Value > 0)
            //        hql.Append(" and rcp.ServicioId = :tipoPlan");
            //    if (servicioId.HasValue && servicioId.Value > 0)
            //        hql.Append(" and rcp.ServicioId = :servicioId");
            //    if (centroId.HasValue && centroId.Value > 0)
            //        hql.Append(" and rcp.CentroId = :centroId");
            //    if (obraSocialId.HasValue && obraSocialId.Value > 0)
            //        hql.Append(" and rcp.ObraSocialId = :obraSocialId");
            //    if (planId.HasValue && planId.Value > 0)
            //        hql.Append(" and rcp.PlanId = :planId");

            //    hql.Append(" order by rcp.Id ASC ");

            //    IQuery query = dalEngine.CreateQuery(hql.ToString());
            //    query.SetParameter("comprobanteId", comprobanteId);
            //    if (soloPracticasImagen ^ soloPracticasConsulta)
            //        query.SetParameter("soloPracticasConsulta", soloPracticasConsulta);

            //    query.SetParameter("turnoId", turnoId);
            //    query.SetParameter("practicaModuloId", practicaModuloId);
            //    if (tipoPlan.HasValue && tipoPlan.Value > 0)
            //        query.SetParameter("tipoPlan", tipoPlan);
            //    if (servicioId.HasValue && servicioId.Value > 0)
            //        query.SetParameter("servicioId", servicioId);
            //    if (centroId.HasValue && centroId.Value > 0)
            //        query.SetParameter("centroId", centroId);
            //    if (obraSocialId.HasValue && obraSocialId.Value > 0)
            //        query.SetParameter("obraSocialId", obraSocialId);
            //    if (planId.HasValue && planId.Value > 0)
            //        query.SetParameter("planId", planId);

            //EntityCollection<RendicionConsPorServModuloView> rendicion = dalEngine.GetManyByQuery<RendicionConsPorServModuloView>(query);

            //switch (tipoOrden)
            //{
            //    case TipoParametroOrdenEnum.Plan:
            //        rendicion.Sort(new Comparison<RendicionConsPorServModuloView>(
            //      delegate(RendicionConsPorServModuloView left, RendicionConsPorServModuloView right)
            //      {
            //          int retorno = left.ObraSocialId.CompareTo(right.ObraSocialId);

            //              if (retorno == 0)
            //                  retorno = left.PlanId.CompareTo(right.PlanId);

            //              if (retorno == 0)
            //                  retorno = left.Servicio.CompareTo(right.Servicio);

            //              if (retorno == 0)
            //                  retorno = left.Protocolo.CompareTo(right.Protocolo);

            //      return retorno;
            //  }));
            //    break;
            //case TipoParametroOrdenEnum.Convenio:
            //    rendicion.Sort(new Comparison<RendicionConsPorServModuloView>(
            //  delegate(RendicionConsPorServModuloView left, RendicionConsPorServModuloView right)
            //  {
            //      // Comparo los Servicios
            //      int retorno = left.ObraSocialId.CompareTo(right.ObraSocialId);

            //              // Comparo las Centros
            //              if (retorno == 0)
            //                  retorno = left.PlanId.CompareTo(right.PlanId);

            //              // Si son iguales comparo los Protocolos
            //              if (retorno == 0)
            //                  retorno = left.Protocolo.CompareTo(right.Protocolo);

            //              // Si son iguales comparo el TipoPlan
            //              if (retorno == 0)
            //                  retorno = left.TipoPlan.CompareTo(right.TipoPlan);

            //              // Si son iguales comparo el orden de la ValorizacionItem
            //              if (retorno == 0)
            //                  retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

            //              return retorno;
            //          }));
            //            break;
            //        case TipoParametroOrdenEnum.Servicio:

            //rendicion.Sort(new Comparison<RendicionConsPorServModuloView>(
            //delegate(RendicionConsPorServModuloView left, RendicionConsPorServModuloView right)
            //{
            //    // Comparo los Servicios
            //    int retorno = left.Servicio.CompareTo(right.Servicio);

            //                // Comparo las Centros
            //                if (retorno == 0)
            //                    retorno = left.Centro.CompareTo(right.Centro);

            //                // Si son iguales comparo los Protocolos
            //                if (retorno == 0)
            //                    retorno = left.Protocolo.CompareTo(right.Protocolo);

            //                // Si son iguales comparo el TipoPlan
            //                if (retorno == 0)
            //                    retorno = left.TipoPlan.CompareTo(right.TipoPlan);

            //                // Si son iguales comparo el orden de la ValorizacionItem
            //                if (retorno == 0)
            //                    retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

            //                return retorno;
            //            }));

            //            break;
            //        case TipoParametroOrdenEnum.Protocolo:

            //            rendicion.SortByProperty(RendicionConsPorServicioView.Properties.Protocolo);

            //            break;
            //        case TipoParametroOrdenEnum.Lote:

            //rendicion.Sort(new Comparison<RendicionConsPorServModuloView>(
            //                   delegate(RendicionConsPorServModuloView left, RendicionConsPorServModuloView right)
            //                   {
            //                       int loteTrasladoIdleft = (left.LoteTrasladoId.HasValue)
            //                                                    ? left.LoteTrasladoId.Value
            //                                                    : 0;

            //                                   int loteTrasladoIdright = (right.LoteTrasladoId.HasValue)
            //                                                                 ? right.LoteTrasladoId.Value
            //                                                                 : 0;

            //                                   int posicionEnLoteleft = (left.PosicionEnLote.HasValue)
            //                                                                ? left.PosicionEnLote.Value
            //                                                                : 0;

            //                                   int posicionEnLoteright = (right.PosicionEnLote.HasValue)
            //                                                                 ? right.PosicionEnLote.Value
            //                                                                 : 0;

            //                                   int retorno = loteTrasladoIdleft.CompareTo(loteTrasladoIdright);

            //                                   if (retorno == 0)
            //                                       retorno = posicionEnLoteleft.CompareTo(posicionEnLoteright);

            //                                   return retorno;
            //                       }));

            //            break;
            // }

            return new EntityCollection<RendicionConsPorServModuloView>();
        }

        public EntityCollection<RendicionConsPorServicioView> RendicionConsPorServicioViewReadByFecha(DateTime fechaDesde, DateTime fechaHasta)
        {
            EntityCollection<RendicionConsPorServicioView> rendicion = new EntityCollection<RendicionConsPorServicioView>();
            StringBuilder hql = new StringBuilder(" from RendicionConsPorSevicioView rendicion ");
            hql.Append(" where rendicion.FechaTurno >= :fechaDesde");
            hql.Append(" AND rendicion.FechaTurno <=  :fechaHasta");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1).Date);

            rendicion = dalEngine.GetManyByQuery<RendicionConsPorServicioView>(query);

            return rendicion;
        }

        public EntityCollection<RendicionAprodia> RendicionAprodiaReadByComprobante(int comprobanteId, int? servicioId, int? obraSocialId, int? planId, bool soloPracticasImagen, bool soloPracticasConsulta)
        {
            StringBuilder hql =
                new StringBuilder(
                    "select new  enfoke.Eges.Entities.Results.RendicionAprodia(pac.ApellidoNombre, tur.NumeroAfiliado, tur.FechaControlDiario, plp.CodigoInterno, pra.Name, pro.ProtocoloFull, vi ) ");
            hql.Append(" from ComprobanteItemHQL ci ");
            hql.Append(" join ci.Comprobante c ");
            hql.Append(" join ci.ValorizacionItem vi ");
            hql.Append(" join vi.Valorizacion vlr ");
            hql.Append(" join vlr.ObraSocialPlan osp ");
            hql.Append(" join osp.ObraSocial oso ");
            hql.Append(" join ci.PracticaTurnoHQL pt ");
            hql.Append(" join pt.Practica pra ");
            hql.Append(" join pt.Turno tur ");
            hql.Append(" join tur.Equipo eq ");
            hql.Append(" join eq.Servicio ser ");
            hql.Append(" join eq.Sucursal suc ");
            hql.Append(" join tur.Paciente pac ");
            hql.Append(" join tur.Protocolo pro, PlanPracticaPrecio plp ");
            hql.Append(" WHERE plp.Id = vi.PlanPracticaUsadoId AND c.Id = :comprobanteId  ");

            if (soloPracticasImagen ^ soloPracticasConsulta)
                hql.Append(" and pra.EsConsulta = :soloPracticasConsulta");
            if (servicioId.HasValue)
                hql.Append(" and ser.Id = :servicioId");
            if (obraSocialId.HasValue)
                hql.Append(" and oso.Id = :obraSocialId");
            if (planId.HasValue)
                hql.Append(" and osp.Id = :planId");

            hql.Append(" order by pro.ProtocoloFull ASC, pra.Name ASC");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("comprobanteId", comprobanteId);

            if (soloPracticasImagen ^ soloPracticasConsulta)
                query.SetParameter("soloPracticasConsulta", soloPracticasConsulta);
            if (servicioId.HasValue)
                query.SetParameter("servicioId", servicioId);
            if (obraSocialId.HasValue)
                query.SetParameter("obraSocialId", obraSocialId);
            if (planId.HasValue)
                query.SetParameter("planId", planId);

            return dalEngine.GetManyByQuery<RendicionAprodia>(query);
        }

        public EntityCollection<RendicionConsPorServicioView> RendicionConsPorServicioViewReadByComprobante(int comprobanteId, int? tipoPlan, int? servicioId, int? centroId, int? obraSocialId, int? planId, bool soloPracticasImagen, bool soloPracticasConsulta, TipoParametroOrdenEnum tipoOrden)
        {
            StringBuilder hql = new StringBuilder(" Select rcp from RendicionConsPorServicioView rcp");
            hql.Append(" where rcp.ComprobanteId = :comprobanteId");
            if (soloPracticasImagen ^ soloPracticasConsulta)
                hql.Append("   and rcp.PracticaEsConsulta = :soloPracticasConsulta");

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                hql.Append(" and rcp.ServicioId = :tipoPlan");
            if (servicioId.HasValue && servicioId.Value > 0)
                hql.Append(" and rcp.ServicioId = :servicioId");
            if (centroId.HasValue && centroId.Value > 0)
                hql.Append(" and rcp.CentroId = :centroId");
            if (obraSocialId.HasValue && obraSocialId.Value > 0)
                hql.Append(" and rcp.ObraSocialId = :obraSocialId");
            if (planId.HasValue && planId.Value > 0)
                hql.Append(" and rcp.PlanId = :planId");

            hql.Append(" order by rcp.Id ASC ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            if (soloPracticasImagen ^ soloPracticasConsulta)
                query.SetParameter("soloPracticasConsulta", soloPracticasConsulta);

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                query.SetParameter("tipoPlan", tipoPlan);
            if (servicioId.HasValue && servicioId.Value > 0)
                query.SetParameter("servicioId", servicioId);
            if (centroId.HasValue && centroId.Value > 0)
                query.SetParameter("centroId", centroId);
            if (obraSocialId.HasValue && obraSocialId.Value > 0)
                query.SetParameter("obraSocialId", obraSocialId);
            if (planId.HasValue && planId.Value > 0)
                query.SetParameter("planId", planId);

            EntityCollection<RendicionConsPorServicioView> rendicion = dalEngine.GetManyByQuery<RendicionConsPorServicioView>(query);

            rendicion = QuitarTurnosPagosCompletamentePorPaciente(rendicion);

            // Ordeno los items según el tipoOrden del parametro.

            switch (tipoOrden)
            {
                case TipoParametroOrdenEnum.Convenio:
                    rendicion.Sort(new Comparison<RendicionConsPorServicioView>(
                  delegate(RendicionConsPorServicioView left, RendicionConsPorServicioView right)
                  {
                      // Comparo los Servicios
                      int retorno = left.ObraSocialId.CompareTo(right.ObraSocialId);

                      // Comparo las Centros
                      if (retorno == 0)
                          retorno = left.PlanId.CompareTo(right.PlanId);

                      // Si son iguales comparo los Protocolos
                      if (retorno == 0)
                          retorno = left.Protocolo.CompareTo(right.Protocolo);

                      // Si son iguales comparo el TipoPlan
                      if (retorno == 0)
                          retorno = left.TipoPlan.CompareTo(right.TipoPlan);

                      // Si son iguales comparo el orden de la ValorizacionItem
                      if (retorno == 0)
                          retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

                      return retorno;
                  }));
                    break;
                case TipoParametroOrdenEnum.Servicio:

                    rendicion.Sort(new Comparison<RendicionConsPorServicioView>(
                    delegate(RendicionConsPorServicioView left, RendicionConsPorServicioView right)
                    {
                        // Comparo los Servicios
                        int retorno = left.Servicio.CompareTo(right.Servicio);

                        // Comparo las Centros
                        if (retorno == 0)
                            retorno = left.Centro.CompareTo(right.Centro);

                        // Si son iguales comparo los Protocolos
                        if (retorno == 0)
                            retorno = left.Protocolo.CompareTo(right.Protocolo);

                        // Si son iguales comparo el TipoPlan
                        if (retorno == 0)
                            retorno = left.TipoPlan.CompareTo(right.TipoPlan);

                        // Si son iguales comparo el orden de la ValorizacionItem
                        if (retorno == 0)
                            retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

                        return retorno;
                    }));

                    break;
                case TipoParametroOrdenEnum.Protocolo:

                    rendicion.SortByProperty(RendicionConsPorServicioView.Properties.Protocolo);

                    break;
                case TipoParametroOrdenEnum.FechaProtocolo:
                    rendicion.Sort(new Comparison<RendicionConsPorServicioView>(
                delegate(RendicionConsPorServicioView left, RendicionConsPorServicioView right)
                {
                    // Comparo los Servicios
                    int retorno = left.FechaControlDiario.Date.CompareTo(right.FechaControlDiario.Date);

                    // Comparo las Centros
                    if (retorno == 0)
                        retorno = left.Protocolo.CompareTo(right.Protocolo);

                    if (retorno == 0)
                        retorno = left.TipoPracticaTurnoId.CompareTo(right.TipoPracticaTurnoId);

                    return retorno;
                }));

                    break;
                case TipoParametroOrdenEnum.Lote:

                    rendicion.Sort(new Comparison<RendicionConsPorServicioView>(
                                       delegate(RendicionConsPorServicioView left, RendicionConsPorServicioView right)
                                       {
                                           int loteTrasladoIdleft = (left.LoteTrasladoId.HasValue)
                                                                        ? left.LoteTrasladoId.Value
                                                                        : 0;

                                           int loteTrasladoIdright = (right.LoteTrasladoId.HasValue)
                                                                         ? right.LoteTrasladoId.Value
                                                                         : 0;

                                           int posicionEnLoteleft = (left.PosicionEnLote.HasValue)
                                                                        ? left.PosicionEnLote.Value
                                                                        : 0;

                                           int posicionEnLoteright = (right.PosicionEnLote.HasValue)
                                                                         ? right.PosicionEnLote.Value
                                                                         : 0;

                                           int retorno = loteTrasladoIdleft.CompareTo(loteTrasladoIdright);

                                           if (retorno == 0)
                                               retorno = posicionEnLoteleft.CompareTo(posicionEnLoteright);

                                           return retorno;
                                       }));

                    break;
            }

            return rendicion;
        }

        public EntityCollection<RendicionConsPorServicioInsumosView> RendicionConsPorServicioInsumosViewReadByComprobante(int comprobanteId, int? tipoPlan, int? servicioId, int? centroId, int? obraSocialId, int? planId, bool soloPracticasImagen, bool soloPracticasConsulta, TipoParametroOrdenEnum tipoOrden)
        {
            StringBuilder hql = new StringBuilder(" Select rcp from RendicionConsPorServicioInsumosView rcp ");
            hql.Append(" where rcp.ComprobanteId = :comprobanteId");
            if (soloPracticasImagen ^ soloPracticasConsulta)
                hql.Append("   and rcp.PracticaEsConsulta = :soloPracticasConsulta");

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                hql.Append(" and rcp.ServicioId = :tipoPlan");
            if (servicioId.HasValue && servicioId.Value > 0)
                hql.Append(" and rcp.ServicioId = :servicioId");
            if (centroId.HasValue && centroId.Value > 0)
                hql.Append(" and rcp.CentroId = :centroId");
            if (obraSocialId.HasValue && obraSocialId.Value > 0)
                hql.Append(" and rcp.ObraSocialId = :obraSocialId");
            if (planId.HasValue && planId.Value > 0)
                hql.Append(" and rcp.PlanId = :planId");

            hql.Append(" order by rcp.Id ASC ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            if (soloPracticasImagen ^ soloPracticasConsulta)
                query.SetParameter("soloPracticasConsulta", soloPracticasConsulta);

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                query.SetParameter("tipoPlan", tipoPlan);
            if (servicioId.HasValue && servicioId.Value > 0)
                query.SetParameter("servicioId", servicioId);
            if (centroId.HasValue && centroId.Value > 0)
                query.SetParameter("centroId", centroId);
            if (obraSocialId.HasValue && obraSocialId.Value > 0)
                query.SetParameter("obraSocialId", obraSocialId);
            if (planId.HasValue && planId.Value > 0)
                query.SetParameter("planId", planId);

            EntityCollection<RendicionConsPorServicioInsumosView> rendicion = dalEngine.GetManyByQuery<RendicionConsPorServicioInsumosView>(query);

            rendicion = QuitarTurnosPagosCompletamentePorPaciente(rendicion);

            // Ordeno los items según el tipoOrden del parametro.

            switch (tipoOrden)
            {
                case TipoParametroOrdenEnum.Convenio:
                    rendicion.Sort(new Comparison<RendicionConsPorServicioInsumosView>(
                  delegate(RendicionConsPorServicioInsumosView left, RendicionConsPorServicioInsumosView right)
                  {
                      // Comparo los Servicios
                      int retorno = left.ObraSocialId.CompareTo(right.ObraSocialId);

                      // Comparo las Centros
                      if (retorno == 0)
                          retorno = left.PlanId.CompareTo(right.PlanId);

                      // Si son iguales comparo los Protocolos
                      if (retorno == 0)
                          retorno = left.Protocolo.CompareTo(right.Protocolo);

                      // Si son iguales comparo el TipoPlan
                      if (retorno == 0)
                          retorno = left.TipoPlan.CompareTo(right.TipoPlan);

                      // Si son iguales comparo el orden de la ValorizacionItem
                      if (retorno == 0)
                          retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

                      return retorno;
                  }));
                    break;
                case TipoParametroOrdenEnum.Servicio:

                    rendicion.Sort(new Comparison<RendicionConsPorServicioInsumosView>(
                    delegate(RendicionConsPorServicioInsumosView left, RendicionConsPorServicioInsumosView right)
                    {
                        // Comparo los Servicios
                        int retorno = left.Servicio.CompareTo(right.Servicio);

                        // Comparo las Centros
                        if (retorno == 0)
                            retorno = left.Centro.CompareTo(right.Centro);

                        // Si son iguales comparo los Protocolos
                        if (retorno == 0)
                            retorno = left.Protocolo.CompareTo(right.Protocolo);

                        // Si son iguales comparo el TipoPlan
                        if (retorno == 0)
                            retorno = left.TipoPlan.CompareTo(right.TipoPlan);

                        // Si son iguales comparo el orden de la ValorizacionItem
                        if (retorno == 0)
                            retorno = left.ValorizacionItemOrden.CompareTo(right.ValorizacionItemOrden);

                        return retorno;
                    }));

                    break;
                case TipoParametroOrdenEnum.Protocolo:

                    rendicion.SortByProperty(RendicionConsPorServicioInsumosView.Properties.Protocolo);

                    break;
                case TipoParametroOrdenEnum.Lote:

                    rendicion.Sort(new Comparison<RendicionConsPorServicioInsumosView>(
                                       delegate(RendicionConsPorServicioInsumosView left, RendicionConsPorServicioInsumosView right)
                                       {
                                           int loteTrasladoIdleft = (left.LoteTrasladoId.HasValue)
                                                                        ? left.LoteTrasladoId.Value
                                                                        : 0;

                                           int loteTrasladoIdright = (right.LoteTrasladoId.HasValue)
                                                                         ? right.LoteTrasladoId.Value
                                                                         : 0;

                                           int posicionEnLoteleft = (left.PosicionEnLote.HasValue)
                                                                        ? left.PosicionEnLote.Value
                                                                        : 0;

                                           int posicionEnLoteright = (right.PosicionEnLote.HasValue)
                                                                         ? right.PosicionEnLote.Value
                                                                         : 0;

                                           int retorno = loteTrasladoIdleft.CompareTo(loteTrasladoIdright);

                                           if (retorno == 0)
                                               retorno = posicionEnLoteleft.CompareTo(posicionEnLoteright);

                                           return retorno;
                                       }));

                    break;
            }

            return rendicion;
        }

        private EntityCollection<RendicionConsPorServicioView> QuitarTurnosPagosCompletamentePorPaciente(EntityCollection<RendicionConsPorServicioView> rendicion)
        {
            EntityCollection<RendicionConsPorServicioView> rendicionOk = new EntityCollection<RendicionConsPorServicioView>();

            List<int> turnoIdsProcesados = new List<int>();

            foreach (RendicionConsPorServicioView item in rendicion)
            {
                // Si no existe en la lista, entonces lo agrego porque lo voy a procesar
                if (item == null)
                    continue;

                if (turnoIdsProcesados.BinarySearch(item.Id) < 0)
                {
                    rendicionOk.AddRange(PracticasFromRendicionQueTienenImportesAPagarPorObraSocialReadByTurnoID(rendicion, item.Id));

                    turnoIdsProcesados.Add(item.Id);
                }
            }

            // Asigno a la lista de rendición solo los turnos que tienen alguna de sus practicas con algo a pagar por la ObraSocial
            return rendicionOk;
        }

        private EntityCollection<RendicionConsPorServicioInsumosView> QuitarTurnosPagosCompletamentePorPaciente(EntityCollection<RendicionConsPorServicioInsumosView> rendicion)
        {
            EntityCollection<RendicionConsPorServicioInsumosView> rendicionOk = new EntityCollection<RendicionConsPorServicioInsumosView>();

            List<int> turnoIdsProcesados = new List<int>();

            foreach (RendicionConsPorServicioInsumosView item in rendicion)
            {
                // Si no existe en la lista, entonces lo agrego porque lo voy a procesar
                if (turnoIdsProcesados.BinarySearch(item.Id) < 0)
                {
                    rendicionOk.AddRange(PracticasFromRendicionQueTienenImportesAPagarPorObraSocialReadByTurnoID(rendicion, item.Id));

                    turnoIdsProcesados.Add(item.Id);
                }
            }

            // Asigno a la lista de rendición solo los turnos que tienen alguna de sus practicas con algo a pagar por la ObraSocial
            return rendicionOk;
        }

        private EntityCollection<RendicionConsPorServicioView> PracticasFromRendicionQueTienenImportesAPagarPorObraSocialReadByTurnoID(EntityCollection<RendicionConsPorServicioView> rendicion, int turnoId)
        {

            EntityCollection<RendicionConsPorServicioView> rendicionTurnoId = new EntityCollection<RendicionConsPorServicioView>();
            EntityCollection<RendicionConsPorServicioView> rendicionOk = new EntityCollection<RendicionConsPorServicioView>();

            bool agregarToRendicionOk = false;

            // Predicate para buscar por id de turno
            Predicate<RendicionConsPorServicioView> predicate = delegate(RendicionConsPorServicioView compare)
            {
                return compare.Id == turnoId;
            };

            rendicionTurnoId.AddRange(rendicion.FindAll(predicate));

            foreach (RendicionConsPorServicioView itemTurnoId in rendicionTurnoId)
            {
                // Si tiene un importe a pagar por la ObraSocial,
                agregarToRendicionOk = itemTurnoId.calculoTotalInstitucion > 0;

                if (agregarToRendicionOk == true)
                    break;
            }

            if (agregarToRendicionOk == true)
                rendicionOk.AddRange(rendicionTurnoId);

            return rendicionOk;
        }

        private EntityCollection<RendicionConsPorServicioInsumosView> PracticasFromRendicionQueTienenImportesAPagarPorObraSocialReadByTurnoID(EntityCollection<RendicionConsPorServicioInsumosView> rendicion, int turnoId)
        {

            EntityCollection<RendicionConsPorServicioInsumosView> rendicionTurnoId = new EntityCollection<RendicionConsPorServicioInsumosView>();
            EntityCollection<RendicionConsPorServicioInsumosView> rendicionOk = new EntityCollection<RendicionConsPorServicioInsumosView>();

            bool agregarToRendicionOk = false;

            // Predicate para buscar por id de turno
            Predicate<RendicionConsPorServicioInsumosView> predicate = delegate(RendicionConsPorServicioInsumosView compare)
            {
                return compare.Id == turnoId;
            };

            rendicionTurnoId.AddRange(rendicion.FindAll(predicate));

            foreach (RendicionConsPorServicioInsumosView itemTurnoId in rendicionTurnoId)
            {
                // Si tiene un importe a pagar por la ObraSocial,
                agregarToRendicionOk = itemTurnoId.calculoTotalInstitucion > 0;

                if (agregarToRendicionOk == true)
                    break;
            }

            if (agregarToRendicionOk == true)
                rendicionOk.AddRange(rendicionTurnoId);

            return rendicionOk;
        }

        public EntityCollection<RendicionConsPorServicioView> RendicionConsPorServicioViewReadByServicio(List<int> comprobanteId, int? tipoPlan, int servicioId, int? centroId, int? planId, bool soloPracticasImagen, bool soloPracticasConsulta)
        {
            //RendicionConsPorServicioView.Properties.ObraSocial
            StringBuilder hql = new StringBuilder(" Select rcp from RendicionConsPorServicioView rcp ");
            hql.Append(" where rcp.ServicioId = :servicioId");
            if (soloPracticasImagen ^ soloPracticasConsulta)
                hql.Append("   and rcp.PracticaEsConsulta = :soloPracticasConsulta");
            hql.Append("   and rcp.ComprobanteId IN (:comprobanteId) ");

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                hql.Append(" and rcp.TipoPlanId = :tipoPlan");
            if (centroId.HasValue && centroId.Value > 0)
                hql.Append(" and rcp.CentroId = :centroId");
            if (planId.HasValue && planId.Value > 0)
                hql.Append(" and rcp.PlanId = :planId");

            hql.Append(" order by rcp.ObraSocial, rcp.Servicio, rcp.Centro ASC, rcp.Protocolo, rcp.TipoPlan, rcp.ValorizacionItemOrden ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("servicioId", servicioId);
            if (soloPracticasImagen ^ soloPracticasConsulta)
                query.SetParameter("soloPracticasConsulta", soloPracticasConsulta);
            query.SetParameterList("comprobanteId", comprobanteId);

            if (tipoPlan.HasValue && tipoPlan.Value > 0)
                query.SetParameter("tipoPlan", tipoPlan);
            if (centroId.HasValue && centroId.Value > 0)
                query.SetParameter("centroId", centroId);
            if (planId.HasValue && planId.Value > 0)
                query.SetParameter("planId", planId);

            EntityCollection<RendicionConsPorServicioView> rendicion = dalEngine.GetManyByQuery<RendicionConsPorServicioView>(query);
            return rendicion;
        }

        [MinuteTimeout]
        public virtual EntityCollection<SetFarmaciaComposicionView> SetFarmaciaComposicionViewRead(int comprobanteId, int? servicioId)
        {
            EntityCollection<SetFarmaciaComposicionView> datos = new EntityCollection<SetFarmaciaComposicionView>();

            StringBuilder hql = new StringBuilder(" from SetFarmaciaComposicionView sfc ");
            hql.Append(" where sfc.ComprobanteId = :comprobanteId");
            //   hql.Append(" and  sfc.TurnoId = 1934836 ");
            if (servicioId.GetValueOrDefault(0) > 0)
                hql.Append("   and sfc.ServicioId = :servicioId");

            hql.Append(" order by sfc.PracticaDesc, sfc.ImporteOs");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            if (servicioId.GetValueOrDefault(0) > 0)
                query.SetParameter("servicioId", servicioId);



            datos = dalEngine.GetManyByQuery<SetFarmaciaComposicionView>(query);

            return datos;
        }

        public EntityCollection<SetFarmaciaComposicionView> SetFarmaciaComposicionViewReadByTurnoPractica(int turnoId, int practicaId)
        {
            ReadManyCommand<SetFarmaciaComposicionView> readCmd = new ReadManyCommand<SetFarmaciaComposicionView>(dalEngine);

            Filter filter = new Filter();
            filter.Add(SetFarmaciaComposicionView.Properties.TurnoId, "=", turnoId);
            filter.Add(BooleanOp.And, SetFarmaciaComposicionView.Properties.PracticaId, "=", practicaId);
            readCmd.Filter = filter;

            EntityCollection<SetFarmaciaComposicionView> setsFarmacia = readCmd.Execute();
            return setsFarmacia;
        }

        [MinuteTimeout]
        public virtual EntityCollection<SetFarmaciaProtocolosView> SetFarmaciaProtocolosViewRead(int comprobanteId, decimal importeSetFcia, string practica)
        {
            EntityCollection<SetFarmaciaProtocolosView> datos = new EntityCollection<SetFarmaciaProtocolosView>();

            StringBuilder hql = new StringBuilder(" from SetFarmaciaProtocolosView sfp ");
            hql.Append(" where sfp.ComprobanteId = :comprobanteId");
            hql.Append("   and sfp.CompInsumos = :importeSetFcia");
            hql.Append("   and sfp.Practica = :practica");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            query.SetParameter("importeSetFcia", importeSetFcia);
            query.SetParameter("practica", practica);

            datos = dalEngine.GetManyByQuery<SetFarmaciaProtocolosView>(query);

            return datos;
        }

        public EntityCollection<DatosRptSetFarmaciaSet> DatosRptSetFarmaciaSetRead(int comprobanteId, int? centroId, int? servicioId, int? obraSocialId, int? planId, TipoPracticaEnum tipoPractica)
        {
            StringBuilder hql = new StringBuilder(" select distinct new enfoke.Eges.Entities.Results.DatosRptSetFarmaciaSet("
                    + "vli.PracticaTurno.Practica.Name, vli.PracticaTurno.Practica.Id, "
                    + "vli.CodigoFicticio , "
                    + "vii.PlanPracticaInsumo.Insumo.Id, "
                    + "vii.PlanPracticaInsumo.Insumo.Code, "
                    + "vii.PlanPracticaInsumo.Insumo.Name, "
                    + "vii.PlanPracticaInsumo.Cantidad, "
                    + "vii.PlanPracticaInsumo.CoeficienteLista, "
                    + "vii.PlanPracticaInsumo.ValorUnitario, vii.ImporteOs, vli.ImporteInsumos, "
                    + "vii.PlanPracticaInsumo.PlanPracticaPrecio.Id, coi.ComprobanteID, "
                    + "equ.Servicio.Id, equ.Servicio.Name, vli.Valorizacion.TipoPlan.Descripcion, "
                    + "vli.Valorizacion.TipoPlan.Id, vli.ImporteCoseguro)");

            hql.Append(" from ValorizacionItem vli, ");
            hql.Append(" ValorizacionItemInsumo vii, ");
            hql.Append(" Turno tur, ");
            hql.Append(" Equipo equ, ");
            hql.Append(" ComprobanteItem coi, ");
            hql.Append(" ");
            // Hace los joins
            hql.Append(" where vii.ValorizacionItemId = vli.Id ");
            hql.Append(" and coi.ValorizacionItemID = vli.Id ");
            hql.Append(" and tur.Id = vli.PracticaTurno.TurnoId ");
            hql.Append(" and tur.EquipoId = equ.Id ");
            // Agrega condiciones
            hql.Append(" and vli.Valorizacion.Deleted = false ");
            hql.Append(" and vli.Valorizacion.Tipo.Id = 3 ");
            hql.Append(" and vli.PracticaTurno.Practica.TipoPractica = :tipoPractica");
            hql.Append(" and coi.ComprobanteID = :comprobanteId");
            // Agrega filtros condicionales
            if (centroId.HasValue)
                hql.Append(" and equ.Sucursal.Id = :centroId");
            if (servicioId.HasValue)
                hql.Append(" and equ.Servicio.Id = :servicioId");
            if (obraSocialId.HasValue)
            {
                hql.Append(" and vli.Valorizacion.ObraSocialPlan.ObraSocial.Id = :obraSocialId");
                if (planId.HasValue)
                    hql.Append(" and vli.Valorizacion.ObraSocialPlan.Id = :planId");
            }
            hql.Append(" order by vii.PlanPracticaInsumo.PlanPracticaPrecio.Id, "
                            + "vli.PracticaTurno.Practica.Name, vii.PlanPracticaInsumo.Insumo.Id ");
            // Pone valores a los filtros
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            query.SetParameter("tipoPractica", (int)tipoPractica);
            if (centroId.HasValue)
                query.SetParameter("centroId", centroId);
            if (servicioId.HasValue)
                query.SetParameter("servicioId", servicioId);
            if (obraSocialId.HasValue)
            {
                query.SetParameter("obraSocialId", obraSocialId);
                if (planId.HasValue)
                    query.SetParameter("planId", planId);
            }
            EntityCollection<DatosRptSetFarmaciaSet> resultado = dalEngine.GetManyByQuery<DatosRptSetFarmaciaSet>(query);
            return resultado;
        }

        public EntityCollection<DatosRptSetFarmaciaProtocolo> DatosRptSetFarmaciaSetReadProtocolos(int comprobanteId, int practicaId, int planPracticaId, TipoPracticaEnum tipoPractica)
        {
            StringBuilder hql = new StringBuilder();
            if (tipoPractica == TipoPracticaEnum.SetFarmacia)
                hql.Append(" select distinct new enfoke.Eges.Entities.Results.DatosRptSetFarmaciaProtocolo(suc.Name, pro.ProtocoloFull, pac.ApellidoNombre, pra.Code, pra.Name, vli.PorcentajeInsumos, ppi.PlanPracticaPrecio.Id, coi.Id, pra.Id, tur.Id)");
            else
                hql.Append(" select distinct new enfoke.Eges.Entities.Results.DatosRptSetFarmaciaProtocolo(suc.Name, pro.ProtocoloFull, pac.ApellidoNombre, pra.Code, pra.Name, vli.PorcentajeInsumos, plp.Id, coi.Id, pra.Id, tur.Id)");
            hql.Append(" from ValorizacionItem vli, ");
            hql.Append(" enfoke.Eges.Entities.Valorizacion vlr, ");
            if (tipoPractica == TipoPracticaEnum.SetFarmacia)
                hql.Append(" PlanPracticaInsumo ppi, ");
            else
                hql.Append(" PlanPracticaPrecio plp, ");

            hql.Append(" PracticaTurno prt, ");
            hql.Append(" Practica pra, ");
            hql.Append(" Turno tur, ");
            hql.Append(" Protocolo pro, ");
            hql.Append(" Equipo equ, ");
            hql.Append(" Sucursal suc, ");
            hql.Append(" Paciente pac, ");
            hql.Append(" ComprobanteItem coi, ");
            hql.Append(" where vlr.Id = vli.Valorizacion.Id ");

            if (tipoPractica == TipoPracticaEnum.SetFarmacia)
                hql.Append("   and vli.PlanPracticaUsadoId = ppi.PlanPracticaPrecio.Id ");
            else
                hql.Append("   and vli.PlanPracticaUsadoId = plp.Id ");

            hql.Append("   and vli.PracticaTurno.Id = prt.Id ");
            hql.Append("   and pra.Id = prt.Practica.Id ");
            hql.Append("   and prt.TurnoId = tur.Id ");
            hql.Append("   and pro.Id = tur.Orden.Protocolo.Id ");
            hql.Append("   and equ.Id = tur.EquipoId ");
            hql.Append("   and equ.Sucursal.Id = suc.Id ");
            hql.Append("   and tur.Orden.PacienteId = pac.Id ");
            hql.Append("   and coi.ValorizacionItemID = vli.Id ");
            hql.Append("   and vlr.Tipo.Id = 3 ");
            hql.Append("   and vlr.Deleted = false ");
            hql.Append("   and coi.ComprobanteID = :comprobanteId");
            hql.Append("   and pra.Id = :practicaId");
            if (tipoPractica == TipoPracticaEnum.SetFarmacia)
                hql.Append("   and ppi.PlanPracticaPrecio.Id = :planPracticaId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteId", comprobanteId);
            query.SetParameter("practicaId", practicaId);
            if (tipoPractica == TipoPracticaEnum.SetFarmacia)
                query.SetParameter("planPracticaId", planPracticaId);

            EntityCollection<DatosRptSetFarmaciaProtocolo> protocolos = dalEngine.GetManyByQuery<DatosRptSetFarmaciaProtocolo>(query);
            return protocolos;
        }

        #endregion

        public EntityCollection<ObraSocialParaFacturacion> ObraSocialParaFacturacionReadById(List<int> ids)
        {
            return dalEngine.GetManyByIds<ObraSocialParaFacturacion>(ids);
        }

        # region ERP

        [Private]
        public EntityCollection<ComprobanteVenta> GetComprobanteVenta(DateTime fechaDesde, DateTime fechaHasta, IList<int> formulariosId, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<VwComprobanteVenta> comprobantesHelp = this.GetVwComprobanteVenta(fechaDesde, fechaHasta);
            return this.CompletarComprobanteVenta(comprobantesHelp, formulariosId, modalidadCoseguro);
        }

        #endregion

        public bool TieneFacturaFormatoServiciosAsociados(int facturaFormatoId)
        {
            return (dalEngine.GetManyByProperty<FacturaFormatoServicio>(FacturaFormatoServicio.Properties.FafId, facturaFormatoId).Count > 0);
        }

        public bool TieneFacturaFormatoSucursalesAsociados(int facturaFormatoId)
        {
            return (dalEngine.GetManyByProperty<FacturaFormatoSucursal>(FacturaFormatoSucursal.Properties.FafId, facturaFormatoId).Count > 0);
        }

        public Dictionary<int, bool> TieneFacturaFormatoSucursalesAsociados(List<int> facturaFormatoIds)
        {
            // Inicializo el dic en FALSE
            Dictionary<int, bool> ret = new Dictionary<int, bool>();
            foreach (int id in facturaFormatoIds)
                ret.Add(id, false);

            var query = from ffs in dalEngine.Query<FacturaFormatoSucursal>() where facturaFormatoIds.Contains(ffs.FafId) select new { FacturaFormatoId = ffs.FafId, FacturaFormatoSucursalId = ffs.Id };
            foreach (var relacion in query)
                ret[relacion.FacturaFormatoId] = true;

            return ret;
        }

        public TipoControlFacturacionEnum TipoControlFacturacionReadByTurnoId(int idTurno)
        {
            string hql = "SELECT t.TipoControlFacturacionId FROM TurnoHQL t " +
               "WHERE t.Id = :idTurno ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);
            query.SetMaxResults(1);

            object ret = query.UniqueResult();

            if (ret != null)
                return (TipoControlFacturacionEnum)int.Parse(ret.ToString());

            throw new Exception("El turno no tiene establecido un tipo de control de facturación.");
        }

        public EntityCollection<FacturaFormatoPractica> FacturaFormatoPracticaReadByFacturaFormatoAndPracticas(int facturaFormatoId)
        {
            EntityCollection<FacturaFormatoPractica> ret = new EntityCollection<FacturaFormatoPractica>();
            string hql = "from FacturaFormatoPractica ffp where ffp.FafId = :ffpId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("ffpId", facturaFormatoId);
            ret.AddRange(dalEngine.GetManyByQuery<FacturaFormatoPractica>(query));

            return ret;
        }

        public EntityCollection<FacturaFormatoPractica> FacturaFormatoPracticaReadByFacturaFormatoAndPracticas(List<int> facturaFormatoIds)
        {
            var query = from ffp in dalEngine.Query<FacturaFormatoPractica>() where facturaFormatoIds.Contains(ffp.FafId) select ffp;
            return query.ToEntityCollection();
        }

        #region Cliente

        public EntityCollection<Cliente> ClienteReadByFilters(string razonSocial, string CUIT, string codigoErp, bool mostrarEliminados)
        {
            string hql = "FROM Cliente cli ";

            string condiciones = "";
            if (!String.IsNullOrEmpty(razonSocial))
                condiciones += " and cli.RazonSocial LIKE :razonSocial";

            if (!String.IsNullOrEmpty(CUIT))
                condiciones += " and cli.Cuit LIKE :CUIT";

            if (!String.IsNullOrEmpty(codigoErp))
                condiciones += " and cli.CodigoErp LIKE :codigoErp";

            if (!mostrarEliminados)
                condiciones += " and cli.DeleteFlag = false";

            if (!String.IsNullOrEmpty(condiciones))
                hql += "WHERE" + condiciones.Substring(4);

            IQuery query = dalEngine.CreateQuery(hql);
            if (!String.IsNullOrEmpty(razonSocial))
                query.SetParameter("razonSocial", "%" + razonSocial + "%");
            if (!String.IsNullOrEmpty(CUIT))
                query.SetParameter("CUIT", "%" + CUIT + "%");
            if (!String.IsNullOrEmpty(codigoErp))
                query.SetParameter("codigoErp", "%" + codigoErp + "%");

            return dalEngine.GetManyByQuery<Cliente>(query);
        }

        #endregion

        public EntityCollection<PuntoVenta> PuntoVentaReadByTipo(TipoPuntoVentaEnum _tipo)
        {
            switch (_tipo)
            {
                case TipoPuntoVentaEnum.Facturacion:
                    return PuntoVentaCajaoFacturacion(_tipo);
                case TipoPuntoVentaEnum.Caja:
                    return PuntoVentaCajaoFacturacion(_tipo);
                case TipoPuntoVentaEnum.FacturacionYCaja:
                    return dalEngine.GetManyByProperty<PuntoVenta>(PuntoVenta.Properties.dbTipoPuntoVenta, (int)_tipo);
                default:
                    throw new Exception("Enum error");
            }
        }

        private EntityCollection<PuntoVenta> PuntoVentaCajaoFacturacion(TipoPuntoVentaEnum _tipo)
        {
            if (_tipo != TipoPuntoVentaEnum.Caja && _tipo != TipoPuntoVentaEnum.Facturacion)
                throw new Exception("No se debe llamar a este metodo con un tipo que que no es facturacion ni caja");

            // Agrego Tipo (facturacion o Caja)
            EntityCollection<PuntoVenta> resul = dalEngine.GetManyByProperty<PuntoVenta>(PuntoVenta.Properties.dbTipoPuntoVenta, (int)_tipo);
            // Agrego FacturacionYCaja
            resul.AddRange(PuntoVentaReadByTipo(TipoPuntoVentaEnum.FacturacionYCaja));

            return resul;
        }

        #region FacturaNumeracion


        public DateTime? MaximaFechaFacturaReadByNumeroTalonario(int empresa, int tipoTalonario, string tipoFactura, int sucursal)
        {
            StringBuilder hql = new StringBuilder(" select max(fac.Fecha) from Factura fac ");
            hql.Append("where fac.EmpresaEmisora.Id  = :empresa ");
            hql.Append("and fac.TipoTalonarioId  = :tipoTalonario ");
            hql.Append("and fac.NumeroFiscal  = :sucursal ");
            hql.Append("and fac.TipoFactura  = :tipoFactura ");
            hql.Append("and fac.FechaAnulacion is null ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("empresa", empresa);
            query.SetInt32("tipoTalonario", tipoTalonario);
            query.SetInt32("sucursal", sucursal);
            query.SetString("tipoFactura", tipoFactura);

            object fechaObject = (object)query.UniqueResult();

            DateTime? fecha = null;
            if (fechaObject != null)
                fecha = Convert.ToDateTime(fechaObject).Date;

            return fecha;
        }

        /// <summary>
        /// [PC] Obtengo la existencia de FacturaNumeracion para un Talonario-Tipo-Sucursal-Numero
        /// </summary>
        /// <param name="facturaNumeracion">FacturaNumeracion a chequear</param>
        /// <returns>verdadero o falso dependiendo de la existencia</returns>
        public bool FacturaNumeracionExisteByTalonarioTipoSucursalNumero(FacturaNumeracion facturaNumeracion)
        {
            ReadManyCommand<FacturaNumeracion> readCmd = new ReadManyCommand<FacturaNumeracion>(dalEngine);

            Filter filter = new Filter();

            filter.Add(FacturaNumeracion.Properties.TipoTalonarioID, "=", facturaNumeracion.TipoTalonarioID);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.Tipo, "=", facturaNumeracion.Tipo);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.PuntoVenta, "=", facturaNumeracion.PuntoVenta.Id);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.Deleted, "=", false);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.Id, "!=", facturaNumeracion.Id);

            readCmd.Filter = filter;

            EntityCollection<FacturaNumeracion> facNum = readCmd.Execute();

            if (facNum != null && facNum.Count > 0)
                return true;
            else
                return false;
        }

        public PuntoVenta PuntoVentaReadBySucursalAndEmpresa(int sucursalId, int empresaId)
        {
            Filter filter = new Filter();
            filter.Add(PuntoVenta.Properties.Sucursal.Id, " = ", sucursalId);
            filter.Add(BooleanOp.And, PuntoVenta.Properties.Empresa.Id, " = ", empresaId);
            return dalEngine.GetManyByFilter<PuntoVenta>(filter)[0];
        }

        public PuntoVenta PuntoVentaReadByNumeroFiscalAndEmpresa(int numeroFiscal, int empresaId)
        {
            Filter filter = new Filter();
            filter.Add(PuntoVenta.Properties.NumeroFiscal, " = ", numeroFiscal);
            filter.Add(BooleanOp.And, PuntoVenta.Properties.Empresa.Id, " = ", empresaId);
            return dalEngine.GetManyByFilter<PuntoVenta>(filter)[0];
        }

        public EntityCollection<PuntoVentaSector> GetPuntoVentaSectoresBySectores(EntityCollection<Sector> sectores) 
        {
            EntityCollection<PuntoVentaSector> puntosVentaSector = new EntityCollection<PuntoVentaSector>();

            if(sectores.Count>0)
            {
                List<int> sectoresIds = sectores.GetIds();
                puntosVentaSector = (from pvs in dalEngine.Query<PuntoVentaSector>()
                               where 
                                    sectoresIds.Contains(pvs.Sector.Id) &&
                                    !pvs.Deleted
                               select pvs).ToEntityCollection<PuntoVentaSector>();
            }

            return puntosVentaSector;
        }

        public PuntoVenta PuntoVentaReadById(int puntoVenta)
        {
            Filter filter = new Filter();
            filter.Add(PuntoVenta.Properties.Id, " = ", puntoVenta);
            return dalEngine.GetManyByFilter<PuntoVenta>(filter)[0];

        }

        /// <summary>
        /// Obtengo un FacturaNumeracion para un Talonario-Tipo-Sucursal
        /// </summary>
        /// <param name="tipoTalonario">ID del Talonario</param>
        /// <param name="tipo">Tipo de Factura</param>
        /// <param name="sucursal">Sucursal</param>
        /// <returns>El FacturaNumeracion Correspondiente</returns>
        [Private]
        public FacturaNumeracion FacturaNumeracionReadByTalonarioTipoAndPuntoVenta(int tipoTalonario, string tipo, int puntoVenta)
        {
            bool delete = false;
            String hql = "from FacturaNumeracion fn " +
                         "where fn.TipoTalonarioID = :tipoTalonario " +
                         "and fn.Tipo = :tipo " +
                         "and fn.PuntoVenta.Id = :puntoVenta " +
                         "and fn.Deleted = :delete ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("tipoTalonario", tipoTalonario);
            query.SetParameter("tipo", tipo);
            query.SetParameter("delete", delete);
            query.SetParameter("puntoVenta", puntoVenta);

            EntityCollection<FacturaNumeracion> facNum = dalEngine.GetManyByQuery<FacturaNumeracion>(query);

            if (facNum != null && facNum.Count > 0)
            {
                FacturaNumeracion fac = facNum[0];
                return AnalizarSiNCUtilizaTalonarioFacturacion(fac);
            }
            else
                throw new Exception("No se ha podido obtener el número para la factura. Consulte con el administrador del sitema");
        }


        private FacturaNumeracion AnalizarSiNCUtilizaTalonarioFacturacion(FacturaNumeracion fac)
        {
            if ((fac.TipoTalonarioID == (int)TipoTalonarioEnum.NotaDeCredito || fac.TipoTalonarioID == (int)TipoTalonarioEnum.NotaDeDebito) && fac.TalonarioFactura)
                return FacturaNumeracionReadByTalonarioTipoAndPuntoVenta((int)TipoTalonarioEnum.Factura, fac.Tipo, fac.PuntoVenta.Id);

            return fac;
        }

        /// <summary>
        /// Incremento la Númeracion de una FacturaNumeracion
        /// </summary>
        /// <param name="numeracion">FacturaNumeracion a Incrementar</param>
        public void FacturaNumeracionIncrementar(FacturaNumeracion numeracion)
        {
            // Incremento la Numeración
            numeracion.Numero += 1;


            // Actualizo
            numeracion = dalEngine.Update<FacturaNumeracion>(numeracion);
        }




















        public EntityCollection<FacturaNumeracion> FacturaNumeracionReadByPuntoVenta(PuntoVenta puntoVenta)
        {
            ReadManyCommand<FacturaNumeracion> readCmd = new ReadManyCommand<FacturaNumeracion>(dalEngine);

            Filter filter = new Filter();

            filter.Add(FacturaNumeracion.Properties.PuntoVenta,
                       "=", puntoVenta.Id);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.Deleted,
                       "=", false);

            readCmd.Filter = filter;

            return (readCmd.Execute());
        }

        public EntityCollection<FacturaNumeracion> FacturaNumeracionReadByPuntoVentaAndTipo(PuntoVenta puntoVenta,
                                                                                            int tipoTalonario)
        {
            ReadManyCommand<FacturaNumeracion> readCmd = new ReadManyCommand<FacturaNumeracion>(dalEngine);

            Filter filter = new Filter();

            filter.Add(FacturaNumeracion.Properties.TipoTalonarioID,
                       "=", tipoTalonario);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.PuntoVenta,
                       "=", puntoVenta.Id);

            filter.Add(BooleanOp.And, FacturaNumeracion.Properties.Deleted,
                       "=", false);

            OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
            filter.Add(open);

            //FEDEfilter.Add(FacturaNumeracion.Properties.FechaVencimiento, ">", enfoke.Time.Now.Date);
            filter.Add(BooleanOp.Or, FacturaNumeracion.Properties.Vence, "=", false);

            CloseParenthesis close = new CloseParenthesis();
            filter.Add(close);

            readCmd.Filter = filter;

            return (readCmd.Execute());
        }











        #endregion

        public EntityCollection<FacturaDetalle> FacturaDetalleReadByFactura(int facturaId)
        {
            return dalEngine.GetManyByProperty<FacturaDetalle>(FacturaDetalle.Properties.Factura, facturaId);
        }

        public Cliente ClienteReadById(int clienteId)
        {
            return dalEngine.GetById<Cliente>(clienteId);
        }

        public bool OrdenEnComprobanteNoAnulado(List<int> ordenesId)
        {
            StringBuilder hql = new StringBuilder(" select distinct com From Comprobante com, ComprobanteItemView coi, Turno tur ");
            hql.Append(" where com.Id = coi.ComprobanteID ");
            hql.Append(" AND coi.TurnoID = tur.Id ");
            hql.Append(" AND com.FechaAnulacion is null ");
            hql.Append(" AND tur.Orden.Id IN (:ordenesId) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("ordenesId", ordenesId);
            EntityCollection<Comprobante> comprobantes = dalEngine.GetManyByQuery<Comprobante>(query);

            return (comprobantes.Count > 0);
        }

        #region Empresa

        public EntityCollection<Empresa> EmpresaReadAllByRazonSocialCodigo(string razonSocial, string codigo)
        {
            string razonSocialABuscar = razonSocial.Trim().Replace(" ", "%") + "%";
            string codigoABuscar = codigo.Trim().Replace(" ", "%") + "%";

            Filter filter = new Filter();
            filter.Add(Empresa.Properties.RazonSocial, "like", razonSocialABuscar);
            filter.Add(BooleanOp.And, Empresa.Properties.Codigo, "like", codigoABuscar);

            Sort sort = new Sort();
            sort.Add(Empresa.Properties.RazonSocial, SortingDirection.Asc);

            return dalEngine.GetManyByFilter<Empresa>(filter, sort);
        }

        public void UpdateAllEmpresaPredeterminada(bool viejoValor, bool nuevoValor)
        {
            dalEngine.UpdatePropertyBatchByProperty<Empresa>(Empresa.Properties.Predeterminada, viejoValor, Empresa.Properties.Predeterminada, nuevoValor);
        }
        public EntityCollection<Empresa> EmpresaReadAll()
        {
            return dalEngine.GetAll<Empresa>(Empresa.Properties.RazonSocial, enfoke.Data.SortOrder.Ascending);
        }

        public Empresa EmpresaReadById(int idEmpresa)
        {
            return dalEngine.GetByProperty<Empresa>(Empresa.Properties.Id, idEmpresa);
        }

        public EntityCollection<Cliente> ClienteCuentaCorrienteReadByCodigoAndRazonSocial(string codigo, string razonSocial, string cuit, bool conImportes)
        {
            string codigoSearch = codigo.Trim().Replace(" ", "%") + "%";
            string razonSocialSearch = razonSocial.Trim().Replace(" ", "%") + "%";
            string cuitSearch = cuit.Trim().Replace(" ", "%") + "%";

            ReadManyCommand<Cliente> readCmd = new ReadManyCommand<Cliente>(dalEngine);

            readCmd.Filter = new Filter();
            readCmd.Filter.Add(Cliente.Properties.PagoDiferidoCaja, "=", true);
            if (codigoSearch != "%")
                readCmd.Filter.Add(BooleanOp.And, Cliente.Properties.CodigoErp, "LIKE", codigoSearch);
            if (razonSocialSearch != "%")
                readCmd.Filter.Add(BooleanOp.And, Cliente.Properties.RazonSocial, "LIKE", razonSocialSearch);
            if (cuitSearch != "%")
                readCmd.Filter.Add(BooleanOp.And, Cliente.Properties.Cuit, "LIKE", cuitSearch);

            readCmd.Sort = new Sort();
            readCmd.Sort.Add(Cliente.Properties.RazonSocial, SortingDirection.Asc);
            readCmd.Sort.Add(Cliente.Properties.CodigoErp, SortingDirection.Asc);

            EntityCollection<Cliente> result = readCmd.Execute();

            if (conImportes)
            {
                // Me traigo todos los que ya fueron atendidos
                string hqlAtendido = "SELECT distinct new Cliente(e, SUM(t.ImporteAPagar), SUM(t.ImportePagado) ) " +
                                     "FROM Turno t, Cliente e " +
                                     "WHERE e.PagoDiferidoCaja = true and t.Id IN (SELECT tu.Id FROM Turno tu WHERE tu.Orden.PagoDiferidoClienteId IS NOT NULL) " +
                                     "AND t.Orden.PagoDiferidoClienteId = e.Id ";

                if (codigoSearch != "%")
                    hqlAtendido += "AND e.CodigoErp LIKE :codigo ";
                if (razonSocialSearch != "%")
                    hqlAtendido += "AND e.RazonSocial LIKE :razonSocial ";

                hqlAtendido += "GROUP BY e.Id, e.SituacionIIBB, e.CodigoErp, e.Cuit, e.DireccionDomicilio, e.DireccionCodigoPostal, " +
                    "e.DireccionProvincia, e.DireccionLocalidad, e.EmailContacto, " +
                    "e.RazonSocial, e.Contacto, e.Telefono, e.CreateDate, e.Cuit, e.DeleteDate, e.DeleteFlag, e.LimiteDescubierto, e.PagoDiferidoCaja, e.PresentacionDomicilio, e.PresentacionCodigoPostal, " +
                    "e.PresentacionProvincia, e.PresentacionLocalidad, e.Telefono, e.UpdateUser, e.UpdateDate, e.DeleteUser, e.CreateUser, e.CondicionIVA";

                IQuery queryAtendido = dalEngine.CreateQuery(hqlAtendido);
                if (codigoSearch != "%")
                    queryAtendido.SetParameter("codigo", codigoSearch);
                if (razonSocialSearch != "%")
                    queryAtendido.SetParameter("razonSocial", razonSocialSearch);

                EntityCollection<Cliente> atendidos = dalEngine.GetManyByQuery<Cliente>(queryAtendido);

                AgruparClientes(atendidos, result);
            }

            return result;
        }

        private void AgruparClientes(EntityCollection<Cliente> atendidos, EntityCollection<Cliente> result)
        {
            foreach (Cliente cli in atendidos)
            {
                int i = result.IndexOf(cli);
                if (i >= 0)
                {
                    // Total Atendido
                    if (result[i].TotalAtendido == null)
                        result[i].TotalAtendido = cli.TotalAtendido;
                    else
                        result[i].TotalAtendido += cli.TotalAtendido;

                    // Total Abonado
                    if (result[i].TotalAbonado == null)
                        result[i].TotalAbonado = cli.TotalAbonado;
                    else
                        result[i].TotalAbonado += cli.TotalAbonado;
                }
                else
                    result.Add(cli);
            }
        }






        public EntityCollection<ClienteImporteDetalleTurno> ClienteImporteDetalleTurnoReadByClienteIDAndEstadoAndFecha(int idCliente, int idEstado, DateTime? fechaDesde, DateTime? fechaHasta)
        {
            // Me traigo todos los que ya fueron atendidos
            string hql = "SELECT new enfoke.Eges.Entities.Results.ClienteImporteDetalleTurno(c.RazonSocial, c.Id, t.Fecha, " +
                         "pa.Nombre, pa.Apellido, pt.Practica.Name, p, t.ImporteAPagar, t.ImportePagado, t.Id) " +
                         "FROM Turno t JOIN t.Orden.Protocolo p, " +
                         "PracticaTurno pt, Paciente pa, Cliente c " +
                         "WHERE t.Id = pt.TurnoId " +
                         "AND t.Orden.PacienteId = pa.Id " +
                         "AND t.Orden.PagoDiferidoClienteId = c.Id " +
                         "AND c.Id = :idCliente " +
                         "AND pt.Tipo = :idTipoPracticaPrincipal ";
            if (fechaDesde.HasValue)
                hql += "AND t.Fecha >= :fechaDesde ";
            if (fechaHasta.HasValue)
                hql += "AND t.Fecha < :fechaHasta ";

            if (idEstado == (int)ClienteImporteEstadoEnum.Impagos)
            {
                //hql += "AND ISNULL(t.ImporteAPagar,0) > ISNULL(t.ImportePagado,0) ";
                hql += " AND ( " +
                            " (t.ImporteAPagar is null AND t.ImportePagado is not null AND 0 > t.ImportePagado ) " +
                            " OR (t.ImporteAPagar is not null AND t.ImportePagado is null AND t.ImporteAPagar > 0 ) " +
                            " OR (t.ImporteAPagar is not null AND t.ImportePagado is not null AND t.ImporteAPagar >  t.ImportePagado) " +
                            ")";

            }
            if (idEstado == (int)ClienteImporteEstadoEnum.Pagos)
            {
                //hql += "AND ISNULL(t.ImporteAPagar,0) <= ISNULL(t.ImportePagado,0) ";
                hql += " AND ( " +
                            " (t.ImporteAPagar is null AND t.ImportePagado is null) " +
                            " OR (t.ImporteAPagar is null AND t.ImportePagado is not null AND 0 <= t.ImportePagado ) " +
                            " OR (t.ImporteAPagar is not null AND t.ImportePagado is null AND t.ImporteAPagar <= 0 ) " +
                            " OR (t.ImporteAPagar is not null AND t.ImportePagado is not null AND t.ImporteAPagar <=  t.ImportePagado) " +
                            ")";
            }

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idCliente", idCliente);
            query.SetParameter("idTipoPracticaPrincipal", (int)PracticaTurnoTipoEnum.Principal);

            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value.AddDays(1));

            EntityCollection<ClienteImporteDetalleTurno> datos = dalEngine.GetManyByQuery<ClienteImporteDetalleTurno>(query);

            return datos;
        }

        public EntityCollection<Empresa> EmpresaReadByPuntoVentaTipo(TipoPuntoVentaEnum tipoPuntoVentaEnum)
        {
            string hql = "SELECT DISTINCT pv.Empresa FROM PuntoVenta pv INNER JOIN pv.Empresa WHERE pv.dbTipoPuntoVenta = :FacturacionYCaja ";

            if (tipoPuntoVentaEnum != TipoPuntoVentaEnum.FacturacionYCaja)
                hql += " OR pv.dbTipoPuntoVenta = :puntoVentaTipo";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("FacturacionYCaja", (int)TipoPuntoVentaEnum.FacturacionYCaja);

            if (tipoPuntoVentaEnum != TipoPuntoVentaEnum.FacturacionYCaja)
                query.SetParameter("puntoVentaTipo", (int)tipoPuntoVentaEnum);

            return dalEngine.GetManyByQuery<Empresa>(query);
        }
        #endregion

        public Comprobante ComprobanteReadByFactura(int facturaId)
        {
            string hql = "select com From Factura fac, Comprobante com where fac.ComprobanteId = com.Id and fac.Id = :facturaId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("facturaId", facturaId);
            return dalEngine.GetByQuery<Comprobante>(query);

        }

        public Comprobante ComprobanteReadByNotaDeCreditoId(int ncId)
        {
            string hql = "select distinct com From Factura nc, Factura fac, Comprobante com where nc.Id = :ncId AND nc.FacturaPadre.Id = fac.Id AND fac.ComprobanteId = com.Id ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("ncId", ncId);
            return dalEngine.GetByQuery<Comprobante>(query);
        }

        [Private]
        public Factura NotaCreditoReadByFactura(int facturaId)
        {
            bool deleted = false;
            StringBuilder hql = new StringBuilder();
            hql.Append("select fac from Factura as fac ");
            hql.Append("where fac.FacturaPadre.Id = :facturaId ");
            hql.Append("and fac.Deleted = :deleted ");
            hql.Append("and fac.FechaAnulacion is null ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("deleted", deleted);
            query.SetParameter("facturaId", facturaId);
            return dalEngine.GetByQuery<Factura>(query);
        }

        [Private]
        public Factura GenerarNCFromERP(decimal importe, string NCNumero, Factura facturaPadre)
        {
            Factura nc = new Factura();
            nc.CondicionIVA = facturaPadre.CondicionIVA;
            nc.ClienteId = facturaPadre.ClienteId;
            nc.ComprobanteId = null;
            nc.Cuit = facturaPadre.Cuit;
            nc.Deleted = false;
            nc.Domicilio = facturaPadre.Domicilio;
            nc.EmpresaEmisora = facturaPadre.EmpresaEmisora;
            nc.FacturaNumeracion = null;
            nc.FacturaPadre = facturaPadre;
            nc.Fecha = enfoke.Time.Now;
            nc.FechaVto = enfoke.Time.Now;
            nc.Ffp = facturaPadre.Ffp;
            nc.Localidad = facturaPadre.Localidad;
            nc.NumeroFiscal = Convert.ToInt32(NCNumero.Substring(1, 4));
            nc.NumeroTalonario = Convert.ToInt32(NCNumero.Substring(5, 8));
            ;
            nc.Origen = (byte)FacturaOrigenEnum.Comprobante;
            nc.PorcentajeIva = facturaPadre.PorcentajeIva;
            nc.RazonSocial = facturaPadre.RazonSocial;
            nc.Reimpresion = facturaPadre.Reimpresion;
            nc.TipoFactura = NCNumero.Substring(0, 1);
            ;
            nc.TipoTalonarioId = (int)TipoTalonarioEnum.NotaDeCredito;
            nc.TotalIva = decimal.Round(facturaPadre.PorcentajeIva * importe / 100, 2);
            nc.TotalNeto = importe;

            return FacturaUpdate(nc);
        }

        public int NumeroComprobanteReadById(int idComprobante)
        {
            string hql = "SELECT c.Numero FROM Comprobante c WHERE c.Id = :idComprobante ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("idComprobante", idComprobante);

            object result = query.UniqueResult();

            if (result == null)
                throw new Exception("Error en búsqueda de número de comprobante.");

            return Convert.ToInt32(result);
        }

        public Comprobante ComprobanteReadByComprobanteItem(int comprobanteItemId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select com from Comprobante com, ComprobanteItem as coi ");
            hql.Append("where com.Id = coi.ComprobanteID ");
            hql.Append("and coi.Id = :comprobanteItemId ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("comprobanteItemId", comprobanteItemId);
            return dalEngine.GetByQuery<Comprobante>(query);
        }

        public void ComprobanteItemDebitoDelete(EntityCollection<ComprobanteItemDebito> comprobanteItemDebitos)
        {
            dalEngine.Delete(comprobanteItemDebitos);
        }

        public EntityCollection<ComprobanteItemDebito> ComprobanteItemDebitoReadByComprobanteItemId(int comprobanteItemId)
        {
            return dalEngine.GetManyByProperty<ComprobanteItemDebito>(ComprobanteItemDebito.Properties.ComprobanteItemId, comprobanteItemId);
        }

        public IComprobanteView ComprobanteViewReadByNumero(int numero)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select com from ComprobanteView com WHERE com.Numero = :numero ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("numero", numero);
            return (IComprobanteView)dalEngine.GetByQuery<ComprobanteView>(query);
        }

        public Factura NotaCreditoViewReadByNroTramite(string numero)
        {
            return dalEngine.GetByProperty<Factura>(Factura.Properties.NroTramite, numero);
        }

        public Factura NotaCreditoViewReadByNroInterno(string numero)
        {
            return dalEngine.GetByProperty<Factura>(Factura.Properties.NroInterno, numero);
        }

        public EntityCollection<ComprobanteItem> ComprobanteItemReadByTurno(int turnoId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select coi from ComprobanteItem coi, PracticaTurno prt, Comprobante com WHERE ");
            hql.Append(" coi.ComprobanteID = com.Id AND prt.Id = coi.PracticaTurnoID AND prt.TurnoId = :turnoId ");
            hql.Append(" AND com.FechaAnulacion is null ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("turnoId", turnoId);
            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        public EntityCollection<ComprobanteItem> ComprobanteItemReadByTurno(List<int> turnosId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select coi from ComprobanteItem coi, PracticaTurno prt, Comprobante com WHERE ");
            hql.Append(" coi.ComprobanteID = com.Id AND prt.Id = coi.PracticaTurnoID AND prt.TurnoId IN (:turnoId) ");
            hql.Append(" AND com.FechaAnulacion is null ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("turnoId", turnosId);
            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        public void ValorizacionItemsUpdate(EntityCollection<ValorizacionItem> items)
        {
            dalEngine.UpdateCollection(items);
        }

        public EntityCollection<ComprobanteItem> ComprobanteItemReadByIds(List<int> comprobanteItemIds)
        {
            return dalEngine.GetManyByIds<ComprobanteItem>(comprobanteItemIds);
        }

        /// <summary>
        /// Me traigo las Valorizaciones no anuladas del tipo de valorizacion al que apunta el comprobanteItem 
        /// (que si se trata de un item debito (con libera liquidacion) debería estar anulado.
        /// </summary>
        /// <param name="comprobanteItemIds"></param>
        /// <returns></returns>
        public EntityCollection<Entities.Valorizacion> ValorizacionVigenteReadByComprobanteItemIds(List<int> comprobanteItemIds)
        {
            if (comprobanteItemIds.Count <= 0)
                return new EntityCollection<Entities.Valorizacion>();

            StringBuilder hql = new StringBuilder();
            hql.Append(" select vliACT.Valorizacion from ComprobanteItem coi, ValorizacionItem vliNOACT, ValorizacionItem vliACT WHERE ");
            hql.Append(" coi.Id IN (:comprobanteItemIds) AND  coi.ValorizacionItemID = vliNOACT.Id ");
            // Misma practica turno // Mismo tipo de valorizacion
            hql.Append(" AND vliNOACT.PracticaTurno.Id = vliACT.PracticaTurno.Id AND vliNOACT.Valorizacion.Tipo.Id = vliACT.Valorizacion.Tipo.Id ");
            // La activada debe estar activadda
            hql.Append(" AND vliACT.Valorizacion.Deleted = false ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("comprobanteItemIds", comprobanteItemIds);
            return dalEngine.GetManyByQuery<Entities.Valorizacion>(query);
        }


        /// <summary>
        /// Me traigo los VLI no anuladas del tipo de valorizacion al que apunta el comprobanteItem 
        /// (que si se trata de un item debito (con libera liquidacion) debería estar anulado.
        /// </summary>
        /// <param name="comprobanteItemIds"></param>
        /// <returns></returns>
        public EntityCollection<ValorizacionItem> ValorizacionItemVigenteReadByComprobanteItemIds(List<int> comprobanteItemIds)
        {
            if (comprobanteItemIds.Count <= 0)
                return new EntityCollection<ValorizacionItem>();

            StringBuilder hql = new StringBuilder();
            hql.Append(" select vliACT from ComprobanteItem coi, ValorizacionItem vliNOACT, ValorizacionItem vliACT WHERE ");
            hql.Append(" coi.Id IN (:comprobanteItemIds) AND  coi.ValorizacionItemID = vliNOACT.Id ");
            // Misma practica turno // Mismo tipo de valorizacion
            hql.Append(" AND vliNOACT.PracticaTurno.Id = vliACT.PracticaTurno.Id AND vliNOACT.Valorizacion.Tipo.Id = vliACT.Valorizacion.Tipo.Id ");
            // La activada debe estar activadda
            hql.Append(" AND vliACT.Valorizacion.Deleted = false ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("comprobanteItemIds", comprobanteItemIds);
            return dalEngine.GetManyByQuery<ValorizacionItem>(query);
        }

        [Private]
        public virtual void ValorizacionUpdateProperty(List<int> ids, IPropertyReference propiedad, object valor)
        {
            dalEngine.UpdatePropertyBatchByIds<Entities.Valorizacion>(ids, propiedad, valor);
        }

        public EntityCollection<Entities.Valorizacion> ValorizacionReadByComprobanteItemIds(List<int> comprobanteItemIds)
        {
            if (comprobanteItemIds.Count <= 0)
                return new EntityCollection<Entities.Valorizacion>();

            StringBuilder hql = new StringBuilder();
            hql.Append(" select vli.Valorizacion from ComprobanteItem coi, ValorizacionItem vli WHERE ");
            hql.Append(" coi.Id IN (:comprobanteItemIds) AND  coi.ValorizacionItemID = vli.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("comprobanteItemIds", comprobanteItemIds);
            return dalEngine.GetManyByQuery<Entities.Valorizacion>(query);
        }

        public EntityCollection<ValorizacionItemUpdateLiquidado> ValorizacionItemUpdateLiquidadoReadByIds(List<int> vliUpdateLiquidados)
        {
            return dalEngine.GetManyByIds<ValorizacionItemUpdateLiquidado>(vliUpdateLiquidados);
        }

        public TipoTalonario TipoTalonarioReadById(int tipoTalonarioId)
        {
            return dalEngine.GetById<TipoTalonario>(tipoTalonarioId);
        }

        public EntityCollection<ComprobanteItem> ComprobanteItemReadDebitadosByTurnoId(List<int> turnoId)
        {
            var query = from ci in dalEngine.Query<ComprobanteItem>()
                        join cid in dalEngine.Query<ComprobanteItemDebito>() on ci.Id equals cid.ComprobanteItemId
                        join pt in dalEngine.Query<PracticaTurno>() on ci.PracticaTurnoID equals pt.Id
                        where turnoId.Contains(pt.TurnoId) && pt.Cantidad > 0
                        select ci;

            return query.ToEntityCollection();
        }

        public EntityCollection<ComprobanteItem> ComprobanteItemReadDebitadosByTurnoId(int turnoId)
        {
            var query = from ci in dalEngine.Query<ComprobanteItem>()
                        join cid in dalEngine.Query<ComprobanteItemDebito>() on ci.Id equals cid.ComprobanteItemId
                        join pt in dalEngine.Query<PracticaTurno>() on ci.PracticaTurnoID equals pt.Id
                        where pt.TurnoId == turnoId && pt.Cantidad > 0
                        select ci;

            return query.ToEntityCollection();
        }

        public EntityCollection<FacturaNumeracionCai> FacturaNumeracionCaiReadByFacturaNumeracionId(int facturaNumeracionId)
        {
            var query = from fnc in dalEngine.Query<FacturaNumeracionCai>() where fnc.FacturaNumeracionId == facturaNumeracionId select fnc;
            return query.ToEntityCollection();
        }

        [Private]
        public bool NotaCreditoTieneDebitosRefacturados(int notaCreditoId)
        {
            var query = from cid in dalEngine.Query<ComprobanteItemDebito>()
                        join ci in dalEngine.Query<ComprobanteItem>() on cid.ComprobanteItemId equals ci.Id
                        join comRefacturado in dalEngine.Query<Comprobante>() on ci.ComprobanteID equals comRefacturado.ComprobantePadreID
                        join ciRefacturado in dalEngine.Query<ComprobanteItem>() on comRefacturado.Id equals ciRefacturado.ComprobanteID
                        join vli in dalEngine.Query<ValorizacionItem>() on ci.ValorizacionItemID equals vli.Id
                        join vliRefa in dalEngine.Query<ValorizacionItem>() on ciRefacturado.ValorizacionItemID equals vliRefa.Id
                        where vli.PracticaTurno.Id == vliRefa.PracticaTurno.Id
                        && cid.NotaCreditoId == notaCreditoId
                        && comRefacturado.FechaAnulacion == null
                        && ciRefacturado.DeleteDate == null
                        select 1;

            return query.Count() > 0;
        }

        public Comprobante ComprobanteReadLastByTurno(int turnoId)
        {
            var query = from coi in dalEngine.Query<ComprobanteItem>()
                        join prt in dalEngine.Query<PracticaTurno>() on coi.PracticaTurnoID equals prt.Id
                        join com in dalEngine.Query<Comprobante>() on coi.ComprobanteID equals com.Id
                        where prt.TurnoId == turnoId
                              && com.FechaAnulacion == null
                        orderby coi.Id descending
                        select com;







            EntityCollection<Comprobante> coms = query.ToEntityCollection();
            return (coms == null || coms.Count <= 0) ? null : coms[0];
        }


        public EntityCollection<Protocolo> ProtocoloDebitadosYRevalorizadosReadByNotaDeCredito(int notaCreditoId)
        {
            // Voy a comparar el vlr.osp == ord.osp. Si es distinto, lo devuelvo
            var query = from cid in dalEngine.Query<ComprobanteItemDebito>()
                        join ci in dalEngine.Query<ComprobanteItem>() on cid.ComprobanteItemId equals ci.Id
                        join vli in dalEngine.Query<ValorizacionItem>() on ci.ValorizacionItemID equals vli.Id
                        join tur in dalEngine.Query<Turno>() on vli.PracticaTurno.TurnoId equals tur.Id
                        where
                        cid.NotaCreditoId == notaCreditoId
                        && vli.Valorizacion.ObraSocialPlan.Id != tur.Orden.ObraSocialPlanId
                        select tur.Orden.Protocolo;

            return query.ToEntityCollection();
        }


        #region Modalidades IIBB

        [Private]
        public void DetallePadronIIBBInsertRaw(EntityCollection<DetallePadronIIBB> detalles)
        {
            string sqlPrefix = "";
            string sqlSuffix = "";
            string insert = "INSERT INTO detalle_padron_IIBB (dpi_cabecera_id, dpi_cuit, dpi_alicuota_percepcion) VALUES ({0},'{1}', {2})";
            StringBuilder sb = new StringBuilder();
            if (enfoke.Context.Data.Session.DatabaseType == DatabaseTypeEnum.Oracle)
            {
                sqlPrefix = "begin ";
                sqlSuffix = "end;";
                insert += ";";
            }

            string sql = String.Empty;
            for (int i = 0; i < detalles.Count; i++)
                sb.Append(String.Format(insert, detalles[i].Cabecera.Id, detalles[i].Cuit, TypeUtils.FormatValue(detalles[i].AlicuotaPercepcion, true).Replace(',', '.')));

            enfoke.Context.Data.Session.ExecuteNonQuery(CommandType.Text, String.Format("{0} {1} {2}", sqlPrefix, sb, sqlSuffix), true);
        }

        public EntityCollection<ModalidadIIBB> ModalidadIIBBReadAllWithDetails()
        {
            var query = from mibd in dalEngine.Query<ModalidadIIBBDetalle>()
                        where mibd.FechaDesde < enfoke.Time.Now && (mibd.FechaHasta == null || mibd.FechaHasta > enfoke.Time.Now)
                        select new ModalidadIIBB(mibd.ModalidadIIBB.Id, mibd.ModalidadIIBB.Descripcion, mibd.ModalidadIIBB.Enabled, mibd.MontoMinimo, mibd.AlicuotaDefault);

            return query.ToEntityCollection();
        }

        public ModalidadIIBB ModalidadIIBBReadWithDetailsById(int modalidadId)
        {
            var query = from mibd in dalEngine.Query<ModalidadIIBBDetalle>()
                        where mibd.FechaDesde < enfoke.Time.Now && (mibd.FechaHasta == null || mibd.FechaHasta > enfoke.Time.Now) && mibd.ModalidadIIBB.Id == modalidadId
                        select new ModalidadIIBB(mibd.ModalidadIIBB.Id, mibd.ModalidadIIBB.Descripcion, mibd.ModalidadIIBB.Enabled, mibd.MontoMinimo, mibd.AlicuotaDefault);

            return query.FirstOrDefault();
        }

        [Private]
        public EntityCollection<CabeceraPadronIIBB> CabeceraPadronIIBBParaEliminar()
        {
            DateTime fechaUmbralDeleteTemporales = enfoke.IO.Time.Now.AddDays(-2);
            var query = from cabe in dalEngine.Query<CabeceraPadronIIBB>()
                        where cabe.Deleted == true || (cabe.EsTemporal == true && cabe.FechaCarga < fechaUmbralDeleteTemporales)
                        select cabe;

            return query.ToEntityCollection();

        }

        [Private]
        public void PadronIIBBEliminarFisicamente(EntityCollection<CabeceraPadronIIBB> padrones)
        {
            foreach (CabeceraPadronIIBB padron in padrones)
            {
                EntityCollection<DetallePadronIIBB> detalles = Context.Session.Dalc.GetManyByProperty<DetallePadronIIBB>(DetallePadronIIBB.Properties.Cabecera.Id, padron.Id);
                Context.Session.Dalc.Delete(detalles);
                Context.Session.Dalc.Delete(padron);
            }
        }

        public ModalidadIIBB ModalidadIIBBReadByIdWithDetails(ModalidadIIBBEnum modalidad)
        {
            var query = from mibd in dalEngine.Query<ModalidadIIBBDetalle>()
                        where mibd.FechaDesde < enfoke.Time.Now && (mibd.FechaHasta == null || mibd.FechaHasta > enfoke.Time.Now)
                              && mibd.ModalidadIIBB.Id == (int)modalidad
                        select new ModalidadIIBB(mibd.ModalidadIIBB.Id, mibd.ModalidadIIBB.Descripcion, mibd.ModalidadIIBB.Enabled, mibd.MontoMinimo, mibd.AlicuotaDefault);

            EntityCollection<ModalidadIIBB> mods = query.ToEntityCollection();

            return (mods != null && mods.Count > 0) ? mods[0] : null;
        }


        public EntityCollection<CabeceraPadronIIBB> CabeceraPadronIIBBActivosYOrdenados()
        {
            var query = from cabe in dalEngine.Query<CabeceraPadronIIBB>() where cabe.Deleted == false && cabe.EsTemporal == false orderby cabe.Fecha descending select cabe;
            return query.ToEntityCollection();
        }

        public EntityCollection<ModalidadIIBBDetalle> ModalidadIIBBDetalleReadByModalidadIIBB(int modalidadIIBB)
        {
            var query = from mibd in dalEngine.Query<ModalidadIIBBDetalle>()
                        where mibd.ModalidadIIBB.Id == modalidadIIBB
                        select mibd;

            return query.ToEntityCollection();
        }

        public EntityCollection<ModalidadIIBB> ModalidadIIBBReadAll(bool soloEnables)
        {
            var query = from mi in dalEngine.Query<ModalidadIIBB>()
                        where (!soloEnables || mi.Enabled)
                        select mi;

            return query.ToEntityCollection();
        }

        public decimal ModalidadIIBBGetMontoMinimoDefault()
        {
            ModalidadIIBB mod = ModalidadIIBBReadByIdWithDetails(ModalidadIIBBEnum.JuridiccionCABA);
            return (mod != null) ? mod.MontoMinimo : 0;
        }

        [RequiresTransaction]
        public virtual void ModalidadIIBBUpdateDetalle(int modalidadIIBB, EntityCollection<ModalidadIIBBDetalle> nuevos)
        {
            EntityCollection<ModalidadIIBBDetalle> paraBorrar = Context.Session.Dalc.GetManyByProperty<ModalidadIIBBDetalle>(ModalidadIIBBDetalle.Properties.ModalidadIIBB.Id, modalidadIIBB);
            if (paraBorrar.Count > 0)
                Context.Session.Dalc.Delete(paraBorrar);

            ModalidadIIBB mod = Context.Session.Dalc.GetById<ModalidadIIBB>(modalidadIIBB);
            foreach (ModalidadIIBBDetalle det in nuevos)
                det.ModalidadIIBB = mod;

            Context.Session.Dalc.UpdateCollection(nuevos);
        }

        public CabeceraPadronIIBB CabeceraPadronIIBBVigente()
        {
            // No anda en Oracle
            //var query = from cabe in dalEngine.Query<CabeceraPadronIIBB>() where cabe.Deleted == false && cabe.EsTemporal == false && enfoke.IO.Time.Now > cabe.FechaDesde && enfoke.IO.Time.Now < cabe.FechaHasta select cabe;
            //return query.FirstOrDefault();

            EntityCollection<CabeceraPadronIIBB> cabes = dalEngine.GetAll<CabeceraPadronIIBB>();
            foreach (CabeceraPadronIIBB cabe in cabes)
            {
                if (cabe.Deleted == false && cabe.EsTemporal == false && enfoke.IO.Time.Now > cabe.FechaDesde && enfoke.IO.Time.Now < cabe.FechaHasta)
                    return cabe;
            }

            return null;
        }

        public DetallePadronIIBB DetallePadronIIBBReadByPadronYCuit(CabeceraPadronIIBB padron, string cuit)
        {
            var query = from deta in dalEngine.Query<DetallePadronIIBB>() where deta.Cabecera.Id == padron.Id && deta.Cuit == cuit select deta;
            return query.FirstOrDefault();
        }

        #endregion


        public FacturaMaipuInfo GetDatosFacturaMaipuByComprobanteId(int comprobanteId)
        {
            FacturaMaipuInfo maipu = new FacturaMaipuInfo();

            var especialidadesValores = (from comp in dalEngine.Query<Comprobante>()
                                         join compItem in dalEngine.Query<ComprobanteItem>()
                                             on comp.Id equals compItem.ComprobanteID
                                         join pt in dalEngine.Query<PracticaTurno>()
                                             on compItem.PracticaTurnoID equals pt.Id
                                         //join servEsp in dalEngine.Query<ServicioEspecialidad>()
                                         //    on pt.Practica
                                         where comp.Id == comprobanteId
                                         select new { Especialidad = new Especialidad(pt.Practica.ServicioEspecialidad.Id, pt.Practica.ServicioEspecialidad.Nombre), Total = decimal.Round(compItem.Derechos + compItem.Honorarios + compItem.Modulo + compItem.Insumos, 2, MidpointRounding.AwayFromZero) });

            foreach (var esp in especialidadesValores)
                maipu.AddEspecialidadValor(esp.Especialidad, esp.Total);

            return maipu;
        }

        public EntityCollection<LeyendaFacturacion> GetLeyendaFacturacionByTipoPlanId(int tipoPlanId)
        {
            return Context.Session.Dalc.GetManyByProperty<LeyendaFacturacion>(LeyendaFacturacion.Properties.TipoPlanId, tipoPlanId);
        }

        public EntityCollection<LeyendaFacturacion> LeyendaFacturacionReadByFactura(int facturaId)
        {
            var query = from flf in dalEngine.Query<FacturaLeyendaFacturacion>()
                        join ley in dalEngine.Query<LeyendaFacturacion>() on flf.LeyendaId equals ley.Id
                        where flf.FacturaId == facturaId
                        select ley;
            return query.ToEntityCollection();

            //return new EntityCollection<LeyendaFacturacion>();
        }

        public EntityCollection<Empresa> GetAllEmpresasHabilitadas()
        {
            EntityCollection<Empresa> empresasHabilitadas = (from empresa in dalEngine.Query<Empresa>()
                                                             where !empresa.Deleted
                                                             select empresa).ToEntityCollection<Empresa>();

            return empresasHabilitadas;
        }


        public EntityCollection<VwAsientoCondor> GetAsientosCondorParaExportar()
        {
            var query = from cbc in dalEngine.Query<VwAsientoCondor>() where cbc.FechaRegistracion < enfoke.Time.Now.Date select cbc;
            return query.ToEntityCollection();
        }

        public List<TurnoTipoPlan> TurnoTipoPlanReadForTurnos(List<int> turnosIds, ValorizacionTiposEnum tipoValorizacion)
        {
            if (turnosIds == null || turnosIds.Count <= 0)
                return new List<TurnoTipoPlan>();
            else if (turnosIds.Count > 1000)
                throw new Exception("No se pueden procesar tantos turnos a la vez. El batch debe ser menor a 1000");

            var query = from val in dalEngine.Query<Entities.Valorizacion>()
                        where val.Tipo.Id == (int)tipoValorizacion
                              && turnosIds.Contains(val.Turno.Id)
                              && !val.Deleted
                        select new TurnoTipoPlan(val.Turno.Id, val.TipoPlan.Id);

            return query.ToList();
        }
    }
}




