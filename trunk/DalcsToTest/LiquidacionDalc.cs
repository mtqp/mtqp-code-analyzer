using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Text;

using enfoke.Connector;
using enfoke.Eges;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;

using enfoke.Eges.Persistence.DAL;

using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using enfoke.Eges.Valorizacion;

using enfoke.Eges.Persistance;
using NHibernate;
using enfoke.Data;
using enfoke.Data.DisconnectedSupport;
using enfoke.Eges.Auditoria;
using enfoke.Data.Filters;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class LiquidacionDalc : Dalc, IService
    {
        protected LiquidacionDalc(NotConstructable dummy) : base(dummy) { }

        #region TipoLiquidacionHonorarios


















        #endregion

        #region LiquidacionHonorarios

        /// <summary>
        /// Genero una Liquidación de Honorarios
        /// </summary>
        /// <param name="tipo">Tipo de Liquidación a Generar</param>
        /// <param name="hasta">Fecha Máxima a Buscar para la Liquidación</param>
        /// <returns>La LiquidacionHonorarios Generada</returns>
        public int LiquidacionHonorariosGenerate(TipoLiquidacionHonorariosEnum tipo, DateTime hasta)
        {
            return LiquidacionHonorariosGenerate(tipo, hasta, 0);
        }

        /// <summary>
        /// Genero una Liquidación de Honorarios
        /// </summary>
        /// <param name="tipo">Tipo de Liquidación a Generar</param>
        /// <param name="hasta">Fecha Máxima a Buscar para la Liquidación</param>
        /// <param name="porcentajeDescuento">Valor del porcentaje de descuento que se aplica a la liquidación</param>
        /// <returns>La LiquidacionHonorarios Generada</returns>
        public int LiquidacionHonorariosGenerate(TipoLiquidacionHonorariosEnum tipo, DateTime hasta, decimal porcentajeDescuento)
        {
            LiquidacionHonorarios lh = null;

            switch (tipo)
            {
                case TipoLiquidacionHonorariosEnum.Caja:
                    lh = CrearLiquidacionHonorariosCaja(hasta, porcentajeDescuento);
                    break;
                case TipoLiquidacionHonorariosEnum.ObrasSociales:
                    lh = CrearLiquidacionHonorariosObrasSociales(hasta, porcentajeDescuento);
                    break;
                //case TipoLiquidacionHonorariosEnum.Externos:
                //    lh = CrearLiquidacionHonorariosExternos(hasta, porcentajeDescuento);
                //    break;
            }

            return lh.Numero;
        }

        public int LiquidacionHonorariosExternosGenerate(DateTime hasta, decimal porcentajeDescuento, List<int> valorizacionItemIds)
        {
            LiquidacionHonorarios lh;
            // Obtengo los Items a Liquidar

            EntityCollection<LiquidacionHonorariosCancelacion> itemsCancelados = TraerCanceladosParaLiquidarExternos(hasta);

            if (valorizacionItemIds.Count == 0 && itemsCancelados.Count == 0)
                throw new NotLoggeableException("No se Encontraron Registros para Liquidar.");

            lh = CrearLiquidacionHonorarios(hasta, TipoLiquidacionHonorariosEnum.Externos, porcentajeDescuento);

            //EntityCollection<ValorizacionItemLight> items = TraerItemsParaLiquidarExternosLimitado(valorizacionItemIds);

            // Guardo los porcentajes utilizados
            CrearPorcentajesLiquidacionHonorariosExternos(lh, valorizacionItemIds, hasta);

            List<int> aux = new List<int>();
            for (int i = 0; i < valorizacionItemIds.Count; i++)
            {
                aux.Add(valorizacionItemIds[i]);
                if (aux.Count == 999)
                {
                    dalEngine.UpdatePropertyBatchByIds<ValorizacionItem>(aux, ValorizacionItem.Properties.LiquidacionHonorariosID, lh.Id);
                    aux = new List<int>();
                }
            }

            if (aux.Count > 0)
                dalEngine.UpdatePropertyBatchByIds<ValorizacionItem>(aux, ValorizacionItem.Properties.LiquidacionHonorariosID, lh.Id);

            // Seteo la Liquidación [Cancelaciones]
            ActualizarItemsCancelados(itemsCancelados, lh);

            return lh.Numero;

        }

        [RequiresTransaction]
		protected virtual LiquidacionHonorarios CrearLiquidacionHonorarios(DateTime hasta, TipoLiquidacionHonorariosEnum tipo, decimal porcentajeDescuento)
        {
            // Obtengo el Siguiente Número
            string sqlNumero = "SELECT MAX(liq_numero) FROM liquidacion_honorarios";
            object ret = dalEngine.Connection.ExecuteScalar(sqlNumero);
            int numero;
            if (ret == null || ret == DBNull.Value)
                numero = 1;
            else
                numero = 1 + Convert.ToInt32(ret);

            // Creo la Liquidación
            LiquidacionHonorarios lh = new LiquidacionHonorarios();
            lh.Fecha = enfoke.Time.Now;
            lh.Numero = numero;
            lh.TipoLiquidacionHonorariosID = (int)tipo;
            lh.FechaHasta = hasta;
            lh.PorcentajeDescuento = porcentajeDescuento;


            // Inserto la Liquidación
            lh = dalEngine.Update<LiquidacionHonorarios>(lh);

            return lh;
        }

        private void ActualizarItemsCancelados(EntityCollection<LiquidacionHonorariosCancelacion> itemsCancelados, LiquidacionHonorarios lh)
        {
            if (itemsCancelados.Count > 0)
            {
                foreach (LiquidacionHonorariosCancelacion itemCancelado in itemsCancelados)
                    itemCancelado.LiquidacionHonorariosID = lh.Id;


                dalEngine.UpdateCollection<LiquidacionHonorariosCancelacion>(itemsCancelados);
            }
        }

        #region Caja
        [RequiresTransaction]
		  protected virtual LiquidacionHonorarios CrearLiquidacionHonorariosCaja(DateTime hasta, decimal porcentajeDescuento)
        {
            LiquidacionHonorarios lh = null;

            // Obtengo los Items a Liquidar
            EntityCollection<ReciboMedico> items = TraerItemsParaLiquidarCaja(hasta);
            EntityCollection<LiquidacionHonorariosCancelacion> itemsCancelados = TraerCanceladosParaLiquidarCaja(hasta);

            if (items.Count == 0 && itemsCancelados.Count == 0)
                throw new NotLoggeableException("No se encontraron registros para liquidar.");

            lh = CrearLiquidacionHonorarios(hasta, TipoLiquidacionHonorariosEnum.Caja, porcentajeDescuento);

            // Guardo los porcentajes utilizados
            CrearPorcentajesLiquidacionHonorariosCaja(lh, hasta);

            // Seteo la Liquidación
            foreach (ReciboMedico item in items)
                item.LiquidacionHonorariosID = lh.Id;


            // Actualizo los Items Liquidados
            dalEngine.UpdateCollection<ReciboMedico>(items);

            // Seteo la Liquidación [Cancelaciones]
            ActualizarItemsCancelados(itemsCancelados, lh);

            return lh;
        }

        private EntityCollection<ReciboMedico> TraerItemsParaLiquidarCaja(DateTime hasta)
        {
            ReadManyCommand<ReciboMedico> readCmd = new ReadManyCommand<ReciboMedico>(dalEngine);

            readCmd.Filter = new Filter();
            // Armo el Filtro de Fecha Hasta
            readCmd.Filter.Add(ReciboMedico.Properties.FechaCreacion,
                "<", hasta.AddDays(1));

            // Recibos No Liquidados
            readCmd.Filter.Add(BooleanOp.And, ReciboMedico.Properties.LiquidacionHonorariosID,
                "IS",
                null);

            // Recibos No Anulados
            readCmd.Filter.Add(BooleanOp.And, ReciboMedico.Properties.FechaAnulacion,
                "IS",
                null);

            return readCmd.Execute();
        }

        private EntityCollection<LiquidacionHonorariosCancelacion> TraerCanceladosParaLiquidarCaja(DateTime hasta)
        {
            string hql = "SELECT lhc FROM LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh, ReciboMedico rm " +
                "WHERE lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id AND lhc.ItemRelacionadoID = rm.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("fecha", hasta.AddDays(1));
            query.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Caja);

            return dalEngine.GetManyByQuery<LiquidacionHonorariosCancelacion>(query);
        }

        private void CrearPorcentajesLiquidacionHonorariosCaja(LiquidacionHonorarios lh, DateTime hasta)
        {
            // Obtengo los Medicos para Insertar los Porcentajes Utilizados en la Liquidacion
            EntityCollection<Medico> medicos = TraerMedicosParaLiquidarCaja(hasta);

            EntityCollection<LiquidacionHonorariosPorcentaje> lhps = new EntityCollection<LiquidacionHonorariosPorcentaje>();
            List<int> medicosAgregados = new List<int>();

            // Inserto los Porcentajes Utilizados en esta Liquidacion
            foreach (Medico medico in medicos)
            {
                if (!medicosAgregados.Contains(medico.Id))
                {
                    LiquidacionHonorariosPorcentaje lhp = new LiquidacionHonorariosPorcentaje(lh.Id, medico);
                    lhp.PorcentajeRecuperoHonorariosCaja = medico.PorcentajeRecuperoHonorariosCaja;

                    lhps.Add(lhp);

                    medicosAgregados.Add(medico.Id);
                }
            }

            // Guardo los Porcentajes Utilizados en esta Liquidacion
            if (lhps.Count > 0)

            {
                dalEngine.UpdateCollection<LiquidacionHonorariosPorcentaje>(lhps);
            }
        }

        private EntityCollection<Medico> TraerMedicosParaLiquidarCaja(DateTime hasta)
        {
            string hql = "SELECT m FROM Medico m, ReciboMedico rm " +
                "WHERE rm.MedicoID = m.Id " +
                "AND rm.FechaCreacion < :fecha " +
                "AND rm.LiquidacionHonorariosID IS NULL " +
                "AND rm.FechaAnulacion IS NULL ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));

            string hql2 = "SELECT m FROM Medico m, ReciboMedico rm, LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh " +
                "WHERE rm.MedicoID = m.Id AND lhc.ItemRelacionadoID = rm.Id AND lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query2 = dalEngine.CreateQuery(hql2);
            query2.SetParameter("fecha", hasta.AddDays(1));
            query2.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Caja);

            EntityCollection<Medico> medicosItems = dalEngine.GetManyByQuery<Medico>(query);
            EntityCollection<Medico> medicosCancelados = dalEngine.GetManyByQuery<Medico>(query2);

            EntityCollection<Medico> medicos = new EntityCollection<Medico>();
            medicos.AddRange(medicosItems);
            medicos.AddRange(medicosCancelados);

            return medicos;
        }
        #endregion

        #region ObrasSociales

        private int? TraerIdReferencia(TipoLiquidacionHonorariosEnum tipoLiquidacion)
        {
            int? idReferencia;
            idReferencia = TraerMinimoCancelado(tipoLiquidacion);
            if (!idReferencia.HasValue)
                idReferencia = TraerMaximaLiquidacionHonorario(tipoLiquidacion);

            return idReferencia;
        }


        private int? TraerMaximaLiquidacionHonorario(TipoLiquidacionHonorariosEnum tipoLiquidacion)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select max(liq.Id) ");
            hql.Append("from LiquidacionHonorarios liq  ");
            hql.Append("where liq.TipoLiquidacionHonorariosID = :tipoLiquidacion  ");

            // Obtengo el nro de orden consecutivo para el lote generado
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("tipoLiquidacion", (int)tipoLiquidacion);
            object ordenObject = query.UniqueResult();

            if (ordenObject != null)
                return Convert.ToInt32(ordenObject);
            else
                return (int?)null;
        }

        private int? TraerMinimoCancelado(TipoLiquidacionHonorariosEnum tipoLiquidacion)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select min(lhc.LiquidacionHonorariosItemRelacionadoID) ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc,LiquidacionHonorarios liq  ");
            hql.Append("where lhc.LiquidacionHonorariosItemRelacionadoID = liq.Id  ");
            hql.Append("and liq.TipoLiquidacionHonorariosID = :tipoLiquidacion  ");
            hql.Append("and lhc.LiquidacionHonorariosID is null ");

            // Obtengo el nro de orden consecutivo para el lote generado
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("tipoLiquidacion", (int)tipoLiquidacion);
            object ordenObject = query.UniqueResult();


            if (ordenObject != null)
                return Convert.ToInt32(ordenObject);
            else
                return (int?)null;
        }


        public DateTime? TraerFechaReferencia(TipoLiquidacionHonorariosEnum tipoLiquidacion)
        {
            int? idReferencia = TraerIdReferencia(tipoLiquidacion);
            StringBuilder hql = new StringBuilder();
            hql.Append("select max(liq.FechaHasta) ");
            hql.Append("from LiquidacionHonorarios liq  ");
            hql.Append("where liq.TipoLiquidacionHonorariosID = :tipoLiquidacion  ");
            hql.Append("and liq.Id <= ").Append(idReferencia.GetValueOrDefault(0));

            // Obtengo el nro de orden consecutivo para el lote generado
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("tipoLiquidacion", (int)tipoLiquidacion);
            object ordenObject = query.UniqueResult();
            if (ordenObject == null)
                return (DateTime?)null;

            return DateTime.Parse(ordenObject.ToString());

        }

        [RequiresTransaction]
		  protected virtual LiquidacionHonorarios CrearLiquidacionHonorariosObrasSociales(DateTime hasta, decimal porcentajeDescuento)
        {
            LiquidacionHonorarios lh = null;

            // Obtengo los Items a Liquidar
            EntityCollection<ComprobanteItem> items = TraerItemsParaLiquidarObrasSociales(hasta);
            EntityCollection<LiquidacionHonorariosCancelacion> itemsCancelados = TraerCanceladosParaLiquidarObrasSociales(hasta);

            if (items.Count == 0 && itemsCancelados.Count == 0)
                throw new NotLoggeableException("No se encontraron registros para liquidar.");

            lh = CrearLiquidacionHonorarios(hasta, TipoLiquidacionHonorariosEnum.ObrasSociales, porcentajeDescuento);

            // Guardo los porcentajes utilizados
            CrearPorcentajesLiquidacionHonorariosObrasSociales(lh, hasta, items);

            // Seteo la Liquidación
            foreach (ComprobanteItem item in items)
                item.LiquidacionHonorariosID = lh.Id;


            // Actualizo los Items Liquidados
            dalEngine.UpdateCollection<ComprobanteItem>(items);

            // Seteo la Liquidación [Cancelaciones]
            ActualizarItemsCancelados(itemsCancelados, lh);

            return lh;
        }

        [Timeout(3600)]
        public virtual EntityCollection<ComprobanteItem> TraerItemsParaLiquidarObrasSociales(DateTime hasta)
        {
            string hql = "SELECT ci FROM ComprobanteItem ci, Comprobante c, ValorizacionItem vi, Factura f " +
                "WHERE ci.ComprobanteID = c.Id AND ci.ValorizacionItemID = vi.Id AND f.ComprobanteId = c.Id " +
                "AND f.FechaAnulacion IS NULL " +
                "AND c.ComprobantePadreID IS NULL " +
                "AND c.FechaAnulacion IS NULL " +
                "AND f.Fecha <:fecha " +
                "AND ci.LiquidacionHonorariosID IS NULL " +
                "AND vi.Cantidad > 0 " +
                "AND vi.ImporteHonorarioInterno > 0 " +
                "AND ((vi.Modulado = 1 AND vi.PorcentajeModulo > 0) OR (vi.Modulado <> 1 AND vi.PorcentajeHonorarios > 0))";
                
                /*"AND (CASE WHEN vi.Modulado = 1 THEN vi.Cantidad * (vi.PorcentajeModulo/100)     * vi.ImporteHonorarioInterno " +
                                               "ELSE vi.Cantidad * (vi.PorcentajeHonorarios/100) * vi.ImporteHonorarioInterno END ) > 0 ";*/

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));

            return dalEngine.GetManyByQuery<ComprobanteItem>(query);
        }

        private EntityCollection<LiquidacionHonorariosCancelacion> TraerCanceladosParaLiquidarObrasSociales(DateTime hasta)
        {
            string hql = "SELECT lhc FROM LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh, " +
                "ComprobanteItem ci, Comprobante c, ValorizacionItem vi " +
                "WHERE lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id " +
                "AND lhc.ItemRelacionadoID = ci.Id " +
                "AND ci.ComprobanteID = c.Id " +
                "AND ci.ValorizacionItemID = vi.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND (CASE WHEN vi.Modulado = 1 THEN vi.Cantidad * (vi.PorcentajeModulo/100) * vi.ImporteHonorarioInterno " +
                "ELSE vi.Cantidad * (vi.PorcentajeHonorarios/100) * vi.ImporteHonorarioInterno END ) > 0 " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));
            query.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);

            return dalEngine.GetManyByQuery<LiquidacionHonorariosCancelacion>(query);
        }

        [RequiresTransaction]
		  protected virtual void CrearPorcentajesLiquidacionHonorariosObrasSociales(LiquidacionHonorarios lh, DateTime hasta, EntityCollection<ComprobanteItem> itemComprobante)
        {
            ServiciosDalc ServiciosDalc = Context.Session.ServiciosDalc;
            MedicosDalc MedicosDalc = Context.Session.MedicosDalc;

            // Obtengo los Medicos para Insertar los Porcentajes Utilizados en la Liquidacion
            EntityCollection<Medico> medicos = TraerMedicosParaLiquidarObrasSociales(hasta, itemComprobante);

            EntityCollection<LiquidacionHonorariosPorcentaje> lhps = new EntityCollection<LiquidacionHonorariosPorcentaje>();
            List<int> medicosAgregados = new List<int>();

            // Inserto los Porcentajes Utilizados en esta Liquidacion
            EntityCollection<Servicio> servicios = ServiciosDalc.ServicioReadAll();
            foreach (Servicio servicio in servicios)
            {
                LiquidacionHonorariosPorcentaje lhp = new LiquidacionHonorariosPorcentaje(lh.Id, servicio);
                lhp.PorcentajeRecuperoHonorariosOS = servicio.PorcentajeRecuperoHonorariosOS;

                lhps.Add(lhp);
            }

            foreach (Medico medico in medicos)
            {
                if (!medicosAgregados.Contains(medico.Id))
                {
                    EntityCollection<MedicoServicio> serviciosMedico = MedicosDalc.MedicoServicioReadByMedico(medico.Id, false);

                    foreach (MedicoServicio medicoServicio in serviciosMedico)
                    {
                        LiquidacionHonorariosPorcentaje lhp = new LiquidacionHonorariosPorcentaje(lh.Id, medicoServicio);
                        lhp.PorcentajeRecuperoHonorariosOS = medicoServicio.PorcentajeRecuperoHonorariosOS;

                        lhps.Add(lhp);
                    }

                    medicosAgregados.Add(medico.Id);
                }
            }

            // Guardo los Porcentajes Utilizados en esta Liquidacion
            if (lhps.Count > 0)

            {
                dalEngine.UpdateCollection<LiquidacionHonorariosPorcentaje>(lhps);
            }
        }

        private EntityCollection<Medico> TraerMedicosParaLiquidarObrasSociales(DateTime hasta, EntityCollection<ComprobanteItem> itemComprobante)
        {
            DateTime? fechaReferencia = TraerFechaReferencia(TipoLiquidacionHonorariosEnum.ObrasSociales);
            EntityCollection<Medico> medicos = new EntityCollection<Medico>();
            List<int> ids = new List<int>();
            foreach (ComprobanteItem item in itemComprobante)
            {
                ids.Add(item.Id);
                if (ids.Count == 999)
                {
                    medicos.AddRange(TraerMedicosParaLiquidarObrasSocialesLimitado(hasta, fechaReferencia, ids));
                    ids = new List<int>();
                }
            }

            if (ids.Count > 0)
                medicos.AddRange(TraerMedicosParaLiquidarObrasSocialesLimitado(hasta, fechaReferencia, ids));

            string hql2 = "SELECT m FROM PracticaTurno pt Join pt.MedicoInformante m, ComprobanteItem ci, Comprobante c, ValorizacionItem vi, " +
                "LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh " +
                "WHERE ci.PracticaTurnoID = pt.Id AND ci.ComprobanteID = c.Id AND ci.ValorizacionItemID = vi.Id " +
                "AND lhc.ItemRelacionadoID = ci.Id AND lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND (CASE WHEN vi.Modulado = 1 THEN vi.Cantidad * (vi.PorcentajeModulo/100) * vi.ImporteHonorarioInterno " +
                "ELSE vi.Cantidad * (vi.PorcentajeHonorarios/100) * vi.ImporteHonorarioInterno END ) > 0 " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query2 = dalEngine.CreateQuery(hql2);
            query2.SetParameter("fecha", hasta.AddDays(1));
            query2.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);
            
            EntityCollection<Medico> medicosCancelados = dalEngine.GetManyByQuery<Medico>(query2);
            medicos.AddRange(medicosCancelados);

            return medicos;

        }

        private EntityCollection<Medico> TraerMedicosParaLiquidarObrasSocialesLimitado(DateTime hasta, DateTime? fechaFiltro, List<int> itemComprobanteId)
        {
            string hql =
                "SELECT m FROM PracticaTurno pt Join pt.MedicoInformante m, ComprobanteItem ci, Comprobante c, ValorizacionItem vi, Factura f " +
                "WHERE ci.PracticaTurnoID = pt.Id AND ci.ComprobanteID = c.Id AND ci.ValorizacionItemID = vi.Id " +
                "AND ci.LiquidacionHonorariosID IS NULL " +
                "AND f.ComprobanteId = c.Id AND f.FechaAnulacion IS NULL AND f.Deleted = false " +
                "AND f.Fecha < :fecha ";
            if (fechaFiltro.HasValue)
                hql += "AND f.Fecha >= :fechaFiltro ";
                
            hql += "AND c.ComprobantePadreID IS NULL " +
                   "AND ci.Id in (:itemComprobanteId) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));
            if(fechaFiltro.HasValue)
                query.SetParameter("fechaFiltro", fechaFiltro.Value);

            query.SetParameterList("itemComprobanteId", itemComprobanteId);
            EntityCollection<Medico> medicosItems = dalEngine.GetManyByQuery<Medico>(query);

            return medicosItems;
        }

        #endregion

        #region Externos
        
        private EntityCollection<LiquidacionHonorariosCancelacion> TraerCanceladosParaLiquidarExternos(DateTime hasta)
        {
            string hql = "SELECT lhc FROM LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh, ValorizacionItem vi Join vi.Valorizacion v " +
                "WHERE lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id AND lhc.ItemRelacionadoID = vi.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND (vi.Cantidad * ((vi.PorcentajeDerechos/100) * vi.ImporteDerechosExt + " +
                "(vi.PorcentajeHonorarios/100) * vi.ImporteHonorariosExt + " +
                "(vi.PorcentajeInsumos/100) * vi.ImporteInsumosExt + " +
                "(vi.PorcentajeModulo/100) * vi.ImporteModuloExt)) > 0 " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));
            query.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);

            return dalEngine.GetManyByQuery<LiquidacionHonorariosCancelacion>(query);
        }

        [RequiresTransaction]
		  protected virtual void CrearPorcentajesLiquidacionHonorariosExternos(LiquidacionHonorarios lh, List<int> valorizacionItemIds, DateTime hasta)
        {
            ServiciosDalc ServiciosDalc = Context.Session.ServiciosDalc;
            MedicosDalc MedicosDalc = Context.Session.MedicosDalc;

            // Obtengo los Medicos para Insertar los Porcentajes Utilizados en la Liquidacion
            EntityCollection<Medico> medicos = TraerMedicosParaLiquidarExternosLimitado(valorizacionItemIds, hasta);

            EntityCollection<LiquidacionHonorariosPorcentaje> lhps = new EntityCollection<LiquidacionHonorariosPorcentaje>();
            List<int> medicosAgregados = new List<int>();

            // Inserto los Porcentajes Utilizados en esta Liquidacion
            EntityCollection<Servicio> servicios = ServiciosDalc.ServicioReadAll();
            foreach (Servicio servicio in servicios)
            {
                LiquidacionHonorariosPorcentaje lhp = new LiquidacionHonorariosPorcentaje(lh.Id, servicio);
                lhp.PorcentajeRecuperoHonorariosExternos = servicio.PorcentajeRecuperoHonorariosExternos;

                lhps.Add(lhp);
            }

            foreach (Medico medico in medicos)
            {
                if (!medicosAgregados.Contains(medico.Id))
                {
                    EntityCollection<MedicoServicio> serviciosMedico = MedicosDalc.MedicoServicioReadByMedico(medico.Id, false);

                    foreach (MedicoServicio medicoServicio in serviciosMedico)
                    {
                        LiquidacionHonorariosPorcentaje lhp = new LiquidacionHonorariosPorcentaje(lh.Id, medicoServicio);
                        lhp.PorcentajeRecuperoHonorariosExternos = medicoServicio.PorcentajeRecuperoHonorariosExternos;

                        lhps.Add(lhp);
                    }

                    medicosAgregados.Add(medico.Id);
                }
            }

            // Guardo los Porcentajes Utilizados en esta Liquidacion
            if (lhps.Count > 0)

            {
                dalEngine.UpdateCollection<LiquidacionHonorariosPorcentaje>(lhps);
            }
        }

        private EntityCollection<Medico> TraerMedicosParaLiquidarExternos(List<int> valorizacionItemIds)
        {
            string hql = "SELECT m FROM ValorizacionItem vi Join vi.PracticaTurno pt Join pt.MedicoInformante m Join vi.Valorizacion v " +
                "WHERE vi.Id in (:valorizacionItemIds) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("valorizacionItemIds", valorizacionItemIds);

            EntityCollection<Medico> medicos = dalEngine.GetManyByQuery<Medico>(query);

            return medicos;
        }

        private EntityCollection<Medico> TraerMedicosParaLiquidarExternosLimitado(List<int> valorizacionItemIds, DateTime hasta)
        {
            EntityCollection<Medico> result = new EntityCollection<Medico>();
            List<int> aux = new List<int>();
            for (int i = 0; i < valorizacionItemIds.Count; i++)
            {
                aux.Add(valorizacionItemIds[i]);
                if (aux.Count == 999)
                {
                    result.AddRange(TraerMedicosParaLiquidarExternos(aux));
                    aux = new List<int>();
                }
            }

            if (aux.Count > 0)
                result.AddRange(TraerMedicosParaLiquidarExternos(aux));


            //Agrego los cancelados.
            string hql = "SELECT m FROM ValorizacionItem vi Join vi.PracticaTurno pt Join pt.MedicoInformante m Join vi.Valorizacion v, LiquidacionHonorariosCancelacion lhc, LiquidacionHonorarios lh " +
                "WHERE lhc.ItemRelacionadoID = vi.Id AND lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id " +
                "AND lhc.LiquidacionHonorariosID IS NULL " +
                "AND lhc.Fecha < :fecha " +
                "AND (vi.Cantidad * ((vi.PorcentajeDerechos/100) * vi.ImporteDerechosExt + " +
                "(vi.PorcentajeHonorarios/100) * vi.ImporteHonorariosExt + " +
                "(vi.PorcentajeInsumos/100) * vi.ImporteInsumosExt + " +
                "(vi.PorcentajeModulo/100) * vi.ImporteModuloExt)) > 0 " +
                "AND lh.TipoLiquidacionHonorariosID = :tipoLiquidacion ";

            IQuery query2 = dalEngine.CreateQuery(hql);
            query2.SetParameter("fecha", hasta.AddDays(1));
            query2.SetParameter("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);

            EntityCollection<Medico> medicosCancelados = dalEngine.GetManyByQuery<Medico>(query2);

            result.AddRange(medicosCancelados);

            return result;
        }

        #endregion

        /// <summary>
        /// Obtengo los Recibos Medicos para la Liquidación de Honorarios de Caja
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Recibos (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Recibos (0 = Todos)</param>
        /// <returns>Colección de Recibos Médicos Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosCajaView> LiquidacionHonorariosCajaReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosCajaView> readCmd = new ReadManyCommand<LiquidacionHonorariosCajaView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Comprobantes para la Liquidación de Honorarios de Obras Sociales
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Comprobantes (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Comprobantes (0 = Todos)</param>
        /// <returns>Colección de Items de Comprobantes Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosObrasSocialesView> LiquidacionHonorariosObrasSocialesReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosObrasSocialesView> readCmd = new ReadManyCommand<LiquidacionHonorariosObrasSocialesView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Valorización para la Liquidación de Honorarios Externos
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <returns>Colección de Items de las Valorizaciones Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosExternosView> LiquidacionHonorariosExternosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            string hql = "SELECT v.Id FROM Valorizacion v " +
                         "WHERE v.CreateDate between :fechaDesde AND :fechaHasta ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", desde.Date);
            query.SetParameter("fechaHasta", hasta.Date.AddDays(1));

            IList<int> valorizacionIds = (List<int>)query.List<int>();

            ReadManyCommand<LiquidacionHonorariosExternosView> readCmd = new ReadManyCommand<LiquidacionHonorariosExternosView>(dalEngine);
            EntityCollection<LiquidacionHonorariosExternosView> result = new EntityCollection<LiquidacionHonorariosExternosView>();
            List<int> aux = new List<int>();

            for (int i = 0; i < valorizacionIds.Count; i++)
            {
                aux.Add(valorizacionIds[i]);
                if (aux.Count == 4000)
                {
                    readCmd = new ReadManyCommand<LiquidacionHonorariosExternosView>(dalEngine);
                    readCmd.Filter = LiquidacionHonorariosReadByFechaMedicoAndServicioGetFilter(DateTime.MinValue, DateTime.MinValue, medico, servicio, obraSocial, sucursal);
                    readCmd.Sort = LiquidacionHonorariosReadByFechaMedicoAndServicioGetSort();
                    readCmd.Filter.Add(BooleanOp.And, LiquidacionHonorariosExternosView.Properties.ValorizacionID,
                                       " in ", aux);
                    result.AddRange(readCmd.Execute());
                    aux = new List<int>();
                }
            }

            if (aux.Count > 0)
            {
                readCmd = new ReadManyCommand<LiquidacionHonorariosExternosView>(dalEngine);
                readCmd.Filter = LiquidacionHonorariosReadByFechaMedicoAndServicioGetFilter(DateTime.MinValue, DateTime.MinValue, medico, servicio, obraSocial, sucursal);
                readCmd.Sort = LiquidacionHonorariosReadByFechaMedicoAndServicioGetSort();
                readCmd.Filter.Add(BooleanOp.And, LiquidacionHonorariosExternosView.Properties.ValorizacionID,
                                   " in ", aux);
                result.AddRange(readCmd.Execute());
            }

            return result;
        }

        private static Filter LiquidacionHonorariosReadByFechaMedicoAndServicioGetFilter(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            Filter filter = new Filter();

            if (desde.Date != DateTime.MinValue)
            {
                filter.Add(BooleanOp.And, "FechaFiltro",
                    ">=", desde.Date);
            }

            if (hasta.Date != DateTime.MinValue)
            {
                filter.Add(BooleanOp.And, "FechaFiltro",
                           "<", hasta.Date.AddDays(1));
            }

            if (medico > 0)
            {
                filter.Add(BooleanOp.And, "MedicoID",
                    "=", medico);
            }

            if (servicio > 0)
            {
                filter.Add(BooleanOp.And, "ServicioID",
                    "=", servicio);
            }

            if (obraSocial > 0)
            {
                filter.Add(BooleanOp.And, "ObraSocialID",
                    "=", obraSocial);
            }

            if (sucursal > 0)
            {
                filter.Add(BooleanOp.And, "CentroID",
                    "=", sucursal);
            }

            return filter;
        }

        private static Sort LiquidacionHonorariosReadByFechaMedicoAndServicioGetSort()
        {
            Sort sort = new Sort();
            sort.Add("Fecha", SortingDirection.Asc);
            sort.Add("Id", SortingDirection.Asc);

            return sort;
        }

        /// <summary>
        /// Obtengo las Fechas Máximas de Todos los Tipos de Liquidación de Honorarios
        /// </summary>
        /// <returns>Fechas Máximas</returns>
        public LiquidacionHonorariosFechasMaximasView LiquidacionHonorariosGetFechasMaximas()
        {
            return dalEngine.GetById<LiquidacionHonorariosFechasMaximasView>(1);
        }

        /// <summary>
        /// Obtengo Items de Liquidacion de Honorarios
        /// </summary>
        /// <param name="tipoLiquidacion">Id del Tipo de Liquidacion</param>
        /// <param name="desde">Fecha Desde a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <param name="protocolo">Protocolo para el cual traer las Valorizaciones</param>
        /// <param name="paciente">Paciente para el cual traer las Valorizaciones</param>
        /// <returns>Colección de Items de las Valorizaciones Filtrados</returns>
        public EntityCollection<ILiquidacionHonorariosView> LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPaciente(TipoLiquidacionHonorariosEnum tipoLiquidacion, DateTime? desde, DateTime? hasta, int medico, int servicio, string protocolo, string paciente, int? idCentro)
        {
            EntityCollection<ILiquidacionHonorariosView> retorno = new EntityCollection<ILiquidacionHonorariosView>();
            IEntityCollection items;
            protocolo = protocolo.Trim().Replace(" ", "%") + "%";
            paciente = paciente.Trim().Replace(" ", "%") + "%";

            switch (tipoLiquidacion)
            {
                case TipoLiquidacionHonorariosEnum.Caja:
                    ReadManyCommand<LiquidacionHonorariosCajaView> readCmdCaja = new ReadManyCommand<LiquidacionHonorariosCajaView>(dalEngine);
                    readCmdCaja.Filter = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetFilter(desde, hasta, medico, servicio, protocolo, paciente, idCentro);
                    readCmdCaja.Sort = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetSort();
                    items = readCmdCaja.Execute();
                    break;
                case TipoLiquidacionHonorariosEnum.ObrasSociales:
                    ReadManyCommand<LiquidacionHonorariosObrasSocialesView> readCmdObrasSociales = new ReadManyCommand<LiquidacionHonorariosObrasSocialesView>(dalEngine);
                    readCmdObrasSociales.Filter = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetFilter(desde, hasta, medico, servicio, protocolo, paciente, idCentro);
                    readCmdObrasSociales.Sort = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetSort();
                    items = readCmdObrasSociales.Execute();
                    break;
                case TipoLiquidacionHonorariosEnum.Externos:
                    ReadManyCommand<LiquidacionHonorariosExternosView> readCmdExternos = new ReadManyCommand<LiquidacionHonorariosExternosView>(dalEngine);
                    readCmdExternos.Filter = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetFilter(desde, hasta, medico, servicio, protocolo, paciente, idCentro);
                    readCmdExternos.Sort = LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetSort();
                    items = readCmdExternos.Execute();
                    break;
                default:
                    throw new Exception("Tipo de Liquidación no reconocido.");
            }

            foreach (ILiquidacionHonorariosView item in items)
                retorno.Add(item);

            return retorno;
        }

        private static Filter LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetFilter(DateTime? desde, DateTime? hasta, int medico, int servicio, string protocolo, string paciente, int? idCentro)
        {
            Filter filter = new Filter();

            if (desde.HasValue)
            {
                filter.Add(BooleanOp.And, "FechaFiltro",
                    ">=", desde.Value.Date);
            }

            if (hasta.HasValue)
            {
                filter.Add(BooleanOp.And, "FechaFiltro",
                    "<", hasta.Value.Date.AddDays(1));
            }

            if (medico > 0)
            {
                filter.Add(BooleanOp.And, "MedicoID",
                    "=", medico);
            }

            if (servicio > 0)
            {
                filter.Add(BooleanOp.And, "ServicioID",
                    "=", servicio);
            }

            if (!String.IsNullOrEmpty(protocolo))
            {
                filter.Add(BooleanOp.And, "Protocolo",
                    "LIKE", protocolo.Trim().Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                filter.Add(BooleanOp.And, "Paciente",
                    "LIKE", paciente.Trim().Replace(" ", "%") + "%");
            }

            if (idCentro.HasValue)
            {
                filter.Add(BooleanOp.And, "CentroID",
                    "=", idCentro.Value);
            }

            return filter;
        }

        private static Sort LiquidacionHonorariosViewReadByFechaMedicoServicioProtocoloAndPacienteGetSort()
        {
            Sort sort = new Sort();
            sort.Add("Fecha", SortingDirection.Asc);
            sort.Add("Id", SortingDirection.Asc);

            return sort;
        }
        #endregion

        #region LiquidacionHonorariosCancelados
        /// <summary>
        /// Cancelo la Liquidacion de Honorarios de los items
        /// </summary>
        /// <param name="tipoLiquidacion">Id del Tipo de Liquidacion</param>
        /// <param name="items">Items a Levantar la Liquidacion de Honorarios</param>
        /// <param name="user">Usuario de la Operación</param>
        /// <returns>Coleccion de LiquidacionHonorariosCancelacion generados</returns>
        [RequiresTransaction]
		  public virtual EntityCollection<LiquidacionHonorariosCancelacion> LiquidacionHonorariosCancelar(TipoLiquidacionHonorariosEnum tipoLiquidacion, EntityCollection<ILiquidacionHonorariosView> items)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            EntityCollection<LiquidacionHonorariosCancelacion> retorno = new EntityCollection<LiquidacionHonorariosCancelacion>();
            foreach (ILiquidacionHonorariosView item in items)
            {
                // Creo el item de cancelación
                LiquidacionHonorariosCancelacion cancelacion = new LiquidacionHonorariosCancelacion();
                cancelacion.ItemRelacionadoID = item.Id;
                cancelacion.LiquidacionHonorariosItemRelacionadoID = item.LiquidacionID;
                cancelacion.ImporteNeto = item.ImporteNeto;
                cancelacion.Fecha = enfoke.Time.Now;
                cancelacion.UserID = user.Id;


                // Inserto el item de cancelación
                cancelacion = dalEngine.Update<LiquidacionHonorariosCancelacion>(cancelacion);

                // Agrego el item a la colección de retorno
                retorno.Add(cancelacion);
                // Levanto la liquidación y le seteo la liquidación en null
                IHonorariosLiquidable entidadLiquidable = EntidadLiquidableGetByTipoAndId(tipoLiquidacion, item);
                entidadLiquidable.LiquidacionHonorariosID = null;
                dalEngine.Update(entidadLiquidable);
            }

            return retorno;
        }

        private IHonorariosLiquidable EntidadLiquidableGetByTipoAndId(TipoLiquidacionHonorariosEnum tipoLiquidacion, ILiquidacionHonorariosView item)
        {
            IHonorariosLiquidable entidadLiquidable;
            switch (tipoLiquidacion)
            {
                case TipoLiquidacionHonorariosEnum.Caja:
                    entidadLiquidable = dalEngine.GetById<ReciboMedico>(item.Id);
                    break;
                case TipoLiquidacionHonorariosEnum.ObrasSociales:
                    entidadLiquidable = dalEngine.GetById<ComprobanteItem>(item.Id);
                    break;
                case TipoLiquidacionHonorariosEnum.Externos:
                    entidadLiquidable = dalEngine.GetById<ValorizacionItem>(item.Id);
                    break;
                default:
                    throw new Exception("Tipo de Liquidación no reconocido.");
            }
            return entidadLiquidable;
        }

        /// <summary>
        /// Obtengo los Recibos Medicos para la Liquidación de Honorarios de Caja [Cancelados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Recibos (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Recibos (0 = Todos)</param>
        /// <returns>Colección de Recibos Médicos Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosCajaCanceladosView> LiquidacionHonorariosCajaCanceladosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosCajaCanceladosView> readCmd = new ReadManyCommand<LiquidacionHonorariosCajaCanceladosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Comprobantes para la Liquidación de Honorarios de Obras Sociales [Cancelados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Comprobantes (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Comprobantes (0 = Todos)</param>
        /// <returns>Colección de Items de Comprobantes Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosObrasSocialesCanceladosView> LiquidacionHonorariosObrasSocialesCanceladosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosObrasSocialesCanceladosView> readCmd = new ReadManyCommand<LiquidacionHonorariosObrasSocialesCanceladosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Valorización para la Liquidación de Honorarios Externos [Cancelados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <returns>Colección de Items de las Valorizaciones Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosExternosCanceladosView> LiquidacionHonorariosExternosCanceladosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosExternosCanceladosView> readCmd = new ReadManyCommand<LiquidacionHonorariosExternosCanceladosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        private static Filter LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetFilter(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, "FechaFiltro",
                ">=", desde.Date);

            filter.Add(BooleanOp.And, "FechaFiltro",
                "<", hasta.Date.AddDays(1));

            if (medico > 0)
            {
                filter.Add(BooleanOp.And, "MedicoID",
                    "=", medico);
            }

            if (servicio > 0)
            {
                filter.Add(BooleanOp.And,  "ServicioID",
                    "=", servicio);
            }

            if (obraSocial > 0)
            {
                filter.Add(BooleanOp.And,  "ObraSocialID",
                    "=", obraSocial);
            }

            if (sucursal > 0)
            {
                filter.Add(BooleanOp.And, "CentroID",
                    "=", sucursal);
            }

            return filter;
        }

        private static Sort LiquidacionHonorariosCanceladosReadByFechaMedicoAndServicioGetSort()
        {
            Sort sort = new Sort();
            sort.Add("Fecha", SortingDirection.Asc);
            sort.Add("Id", SortingDirection.Asc);

            return sort;
        }
        #endregion

        #region LiquidacionHonorariosReLiquidados
        /// <summary>
        /// Obtengo los Recibos Medicos para la Liquidación de Honorarios de Caja [ReLiquidados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Recibos Médicos Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Recibos (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Recibos (0 = Todos)</param>
        /// <returns>Colección de Recibos Médicos Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosCajaReLiquidadosView> LiquidacionHonorariosCajaReLiquidadosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosCajaReLiquidadosView> readCmd = new ReadManyCommand<LiquidacionHonorariosCajaReLiquidadosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Comprobantes para la Liquidación de Honorarios de Obras Sociales [ReLiquidados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Comprobantes Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer los Comprobantes (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer los Comprobantes (0 = Todos)</param>
        /// <returns>Colección de Items de Comprobantes Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosObrasSocialesReLiquidadosView> LiquidacionHonorariosObrasSocialesReLiquidadosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosObrasSocialesReLiquidadosView> readCmd = new ReadManyCommand<LiquidacionHonorariosObrasSocialesReLiquidadosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        /// <summary>
        /// Obtengo los Items de Valorización para la Liquidación de Honorarios Externos [ReLiquidados]
        /// </summary>
        /// <param name="desde">Fecha Desde a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="hasta">Fecha Hasta a Obtener los Items de Valorizacion Liquidados</param>
        /// <param name="medico">Id del Médico para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <param name="servicio">Id del Servicio para el cual traer las Valorizaciones (0 = Todos)</param>
        /// <returns>Colección de Items de las Valorizaciones Filtrados</returns>
        [Private]
        public EntityCollection<LiquidacionHonorariosExternosReLiquidadosView> LiquidacionHonorariosExternosReLiquidadosReadByFechaMedicoAndServicio(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            ReadManyCommand<LiquidacionHonorariosExternosReLiquidadosView> readCmd = new ReadManyCommand<LiquidacionHonorariosExternosReLiquidadosView>(dalEngine);

            readCmd.Filter = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetFilter(desde, hasta, medico, servicio, obraSocial, sucursal);
            readCmd.Sort = LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetSort();

            return readCmd.Execute();
        }

        private static Filter LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetFilter(DateTime desde, DateTime hasta, int medico, int servicio, int obraSocial, int sucursal)
        {
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, "FechaFiltro",
                ">=", desde.Date);

            filter.Add(BooleanOp.And,  "FechaFiltro",
                "<", hasta.Date.AddDays(1));

            if (medico > 0)
            {
                filter.Add(BooleanOp.And,  "MedicoID",
                    "=", medico);
            }

            if (servicio > 0)
            {
                filter.Add(BooleanOp.And, "ServicioID",
                    "=", servicio);
            }

            if (obraSocial > 0)
            {
                filter.Add(BooleanOp.And, "ObraSocialID",
                    "=", obraSocial);
            }

            if (sucursal > 0)
            {
                filter.Add(BooleanOp.And, "CentroID",
                    "=", sucursal);
            }

            return filter;
        }

        private static Sort LiquidacionHonorariosReLiquidadosReadByFechaMedicoAndServicioGetSort()
        {
            Sort sort = new Sort();
            sort.Add("Fecha", SortingDirection.Asc);
            sort.Add("Id", SortingDirection.Asc);
            return sort;
        }
        #endregion

        #region LiquidacionCuadroResumen

        #region Obra social

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", vli.Modulado ");
            hql.Append(", vli.PorcentajeModulo ");
            hql.Append(", vli.PorcentajeHonorarios ");
            hql.Append(", vli.ImporteHonorarioInterno ");
            hql.Append(", vli.Cantidad ");
            hql.Append(") ");
            hql.Append("from ComprobanteItemHQL com JOIN com.Comprobante.Facturas f JOIN com.ValorizacionItem vli JOIN  com.LiquidacionHonorarios.LiquidacionHonorariosPorcentaje lhp ");
            hql.Append("where f.Fecha >= :desde ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId = com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append("and f.Fecha < :hasta ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", vli.Modulado ");
            hql.Append(", vli.PorcentajeModulo ");
            hql.Append(", vli.PorcentajeHonorarios ");
            hql.Append(", vli.ImporteHonorarioInterno ");
            hql.Append(", vli.Cantidad ");
            hql.Append(") ");
            hql.Append("from ComprobanteItemHQL com JOIN com.Comprobante.Facturas f JOIN com.ValorizacionItem vli JOIN  com.LiquidacionHonorarios.LiquidacionHonorariosPorcentaje lhp ");
            hql.Append("where f.Fecha >= :desde ");
            hql.Append("and f.Fecha < :hasta ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and com.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsCanceladosReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemCanceladoOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId = com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsCanceladosReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemCanceladoOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and com.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsReLiquidadoReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemReLiquidadoOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosID = lh.Id ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId = com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemOsReLiquidadoReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            //LiquidacionHonorariosCancelacion lhc;

            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemReLiquidadoOs( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosID = lh.Id ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and com.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.ObrasSociales);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        #endregion

        #region Externo

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemExterno( ");
            hql.Append("  vli.Id ");
            hql.Append(", pt.MedicoInformante.Id ");
            hql.Append(", pt.Turno.Equipo.Servicio.Id ");
            hql.Append(", pt.Turno.Equipo.Servicio.Name ");
            hql.Append(", pt.MedicoInformante.Name ");
            hql.Append(", pt.MedicoInformante.Apellido ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosExternos ");
            hql.Append(", vli ");
            hql.Append(") ");
            hql.Append("from ValorizacionItem vli, PracticaTurnoHQL pt, LiquidacionHonorariosHQL liq, LiquidacionHonorariosPorcentaje lhp ");
            hql.Append("where vli.LiquidacionHonorariosID = liq.Id ");
            hql.Append("and vli.PracticaTurno.Id = pt.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = liq.Id ");
            hql.Append("and lhp.ServicioId = pt.Turno.Equipo.Servicio.Id and lhp.MedicoId = pt.MedicoInformante.Id ");
            hql.Append("and vli.Valorizacion.CreateDate >= :desde ");
            hql.Append("and vli.Valorizacion.CreateDate < :hasta ");

            if (medico > 0)
                hql.Append("and pt.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and pt.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and pt.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by pt.MedicoInformante.Id, pt.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            //ValorizacionItem vli;

            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemExterno( ");
            hql.Append("  vli.Id ");
            hql.Append(", pt.MedicoInformante.Id ");
            hql.Append(", pt.Turno.Equipo.Servicio.Id ");
            hql.Append(", pt.Turno.Equipo.Servicio.Name ");
            hql.Append(", pt.MedicoInformante.Name ");
            hql.Append(", pt.MedicoInformante.Apellido ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosExternos ");
            hql.Append(", vli ");
            hql.Append(") ");
            hql.Append("from ValorizacionItem vli, PracticaTurnoHQL pt, LiquidacionHonorariosHQL liq, LiquidacionHonorariosPorcentaje lhp ");
            hql.Append("where vli.LiquidacionHonorariosID = liq.Id ");
            hql.Append("and vli.PracticaTurno.Id = pt.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = liq.Id ");
            hql.Append("and lhp.ServicioId = pt.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");
            hql.Append("and vli.Valorizacion.CreateDate >= :desde ");
            hql.Append("and vli.Valorizacion.CreateDate < :hasta ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and vli.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and pt.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and pt.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and pt.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by pt.MedicoInformante.Id, pt.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoCanceladosReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemCanceladoExterno( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId = com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");
            
            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoCanceladosReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemCanceladoExterno( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosItemRelacionadoID = lh.Id ");

            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and com.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoReLiquidadoReadByFechaMedicoServicioLiquidacionMedicoNotNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemReLiquidadoExterno( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosID = lh.Id ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId = com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        [Private]
        public EntityCollection<CuadroHonorarioItem> CuadroHonorarioItemExternoReLiquidadoReadByFechaMedicoServicioLiquidacionMedicoNull(DateTime desde, DateTime hasta, int medico, int servicio, int sucursal, List<int> comprobanteItemIds)
        {
            //LiquidacionHonorariosCancelacion lhc;

            StringBuilder hql = new StringBuilder();
            hql.Append("select new  enfoke.Eges.Entities.Results.CuadroHonorarioItemReLiquidadoExterno( ");
            hql.Append("  com.Id ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");
            hql.Append(", com.PracticaTurnoHQL.Turno.Equipo.Servicio.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Name ");
            hql.Append(", com.PracticaTurnoHQL.MedicoInformante.Apellido ");
            hql.Append(", com.Comprobante.PorcentajeIVA ");
            hql.Append(", lhp.PorcentajeRecuperoHonorariosOS ");
            hql.Append(", lhc.ImporteNeto ");
            hql.Append(") ");
            hql.Append("from LiquidacionHonorariosCancelacion lhc, LiquidacionHonorariosHQL lh, ComprobanteItemHQL com,ValorizacionItem vli,  ");
            hql.Append("LiquidacionHonorariosPorcentaje lhp  ");
            hql.Append("where lh.TipoLiquidacionHonorarios.Id = :tipoLiquidacion ");
            hql.Append("and lhc.LiquidacionHonorariosID = lh.Id ");
            hql.Append("and lhc.ItemRelacionadoID = com.Id ");
            hql.Append("and com.ValorizacionItem.Id = vli.Id ");
            hql.Append("and lhp.LiquidacionHonorariosID = lhc.LiquidacionHonorariosID ");
            hql.Append("and lhp.ServicioId = com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id and lhp.MedicoId is null ");
            hql.Append("and lhc.Fecha >= :desde ");
            hql.Append("and lhc.Fecha < :hasta ");

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                hql.Append("and com.Id not in (:comprobanteItemIds) ");

            if (medico > 0)
                hql.Append("and com.PracticaTurnoHQL.MedicoInformante.Id = :medico ");

            if (servicio > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id = :servicio ");

            if (sucursal > 0)
                hql.Append("and com.PracticaTurnoHQL.Turno.Equipo.Sucursal.Id = :sucursal ");

            hql.Append("order by com.PracticaTurnoHQL.MedicoInformante.Id, com.PracticaTurnoHQL.Turno.Equipo.Servicio.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (comprobanteItemIds != null && comprobanteItemIds.Count > 0)
                query.SetParameterList("comprobanteItemIds", comprobanteItemIds);

            query.SetInt32("tipoLiquidacion", (int)TipoLiquidacionHonorariosEnum.Externos);
            query.SetDateTime("desde", desde.Date);
            query.SetDateTime("hasta", hasta.Date.AddDays(1));

            if (medico > 0)
                query.SetInt32("medico", medico);

            if (servicio > 0)
                query.SetInt32("servicio", servicio);

            if (sucursal > 0)
                query.SetInt32("sucursal", sucursal);

            return dalEngine.GetManyByQuery<CuadroHonorarioItem>(query);
        }

        #endregion

        #endregion
    }
}

