using enfoke.AOP;
using System.Collections.Generic;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;

using NHibernate;
using System.Text;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class PuntoVentaDalc : Dalc, IService
    {
        protected PuntoVentaDalc(NotConstructable dummy) : base(dummy) { }

        #region PuntoVenta





        public virtual EntityCollection<PuntoVenta> PuntoVentaReadBySucursal(int sucursalId)
        {
            ReadManyCommand<PuntoVenta> readCmd = new ReadManyCommand<PuntoVenta>(dalEngine);
            Filter filter = new Filter();

            if (sucursalId != 0)
            {
                filter.Add(BooleanOp.And, PuntoVenta.Properties.Sucursal.Id,
                    "=", sucursalId);
            }

            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public virtual void CrearPuntosDeVentaParaCaja(EntityCollection<CajaPuntoVenta> posCollection)
        {
            dalEngine.UpdateCollection(posCollection);
        }

        public virtual void EliminarPuntosDeVentaParaCaja(int cajaId)
        {
            dalEngine.DeleteBatch(ObtenerCajaPuntoVentaPorCaja(cajaId));
        }

        public EntityCollection<CajaPuntoVenta> ObtenerCajaPuntoVentaPorCaja(int cajaId)
        {
            return dalEngine.GetManyByProperty<CajaPuntoVenta>(CajaPuntoVenta.Properties.CajaId, cajaId);
        }

        public EntityCollection<CajaPuntoVenta> ObtenerCajaPuntoVentaPorPuntoVenta(int posId)
        {
            return dalEngine.GetManyByProperty<CajaPuntoVenta>(CajaPuntoVenta.Properties.PuntoVenta.Id, posId);
        }











        public EntityCollection<PuntoVentaEquipo> PuntoVentaEquipoReadByPuntoVenta(int posId)
        {
            return dalEngine.GetManyByProperty<PuntoVentaEquipo>(PuntoVentaEquipo.Properties.PuntoVentaId, posId);
        }

        public EntityCollection<PuntoVenta> PuntoVentaReadByEmpresa(Empresa empresa) {
            return dalEngine.GetManyByProperty<PuntoVenta>(PuntoVenta.Properties.Empresa, empresa);
        }

        /// <summary>
        /// Obtiene el primer punto de venta de la caja.
        /// </summary>
        /// <param name="cajaId">El identificador de la caja.</param>
        /// <returns>El primer punto de venta de la caja.</returns>
        /// <remarks>Se toma el primero porque se usa para una validacón donde todos tienen los mismos valores a validar para la caja.</remarks>
        public PuntoVenta PuntoVentaReadFirstByCajaAndEmpresaId(int cajaId, int numeroFiscal, int empresaId)
        {
            string stringQuery = "Select pv from CajaPuntoVenta cpv join cpv.PuntoVenta pv where cpv.CajaId = :cajaId and pv.NumeroFiscal = :numeroFiscal and pv.Empresa.Id = :empresaId ";
            IQuery query = dalEngine.CreateQuery(stringQuery);
            query.SetInt32("cajaId", cajaId);
            query.SetInt32("numeroFiscal", numeroFiscal);
            query.SetInt32("empresaId", empresaId);
            query.SetMaxResults(1);

            return dalEngine.GetByQuery<PuntoVenta>(query);
        }

        public PuntoVenta PuntoVentaReadFirstByCaja(int cajaId)
        {
            string stringQuery = "Select pv from CajaPuntoVenta cpv join cpv.PuntoVenta pv where cpv.CajaId = :cajaId ";
            IQuery query = dalEngine.CreateQuery(stringQuery);
            query.SetInt32("cajaId", cajaId);
            query.SetMaxResults(1);
            return dalEngine.GetByQuery<PuntoVenta>(query);
        }

        [RequiresTransaction]
		  public virtual void CrearPuntoVentaConExclusiones(PuntoVenta pos, EntityCollection<Equipo> exclusiones)
        {

            bool isNewPos = pos.Id == 0;
            pos = dalEngine.Update<PuntoVenta>(pos);
            EntityCollection<PuntoVentaEquipo> exclusions = new EntityCollection<PuntoVentaEquipo>();
            foreach (Equipo equipment in exclusiones)
            {
                PuntoVentaEquipo pve = new PuntoVentaEquipo();
                pve.Equipo = equipment;
                pve.PuntoVentaId = pos.Id;
                exclusions.Add(pve);
            }

            if (!isNewPos)
                this.RemoverExclusionesAntiguas(pos);
            
            dalEngine.UpdateCollection(exclusions);
        }

        private void RemoverExclusionesAntiguas(PuntoVenta pos)
        {
            Filter filter = new Filter();
            filter.Add(PuntoVentaEquipo.Properties.PuntoVentaId, " = ", pos.Id);
            dalEngine.DeleteBatch(dalEngine.GetManyByFilter<PuntoVentaEquipo>(filter));
        }

        #endregion

        #region PuntoVentaSector

        public virtual EntityCollection<PuntoVentaSector> PuntoVentaSectorReadByPuntoVenta(PuntoVenta puntoVenta)
        {
            ReadManyCommand<PuntoVentaSector> readCmd = new ReadManyCommand<PuntoVentaSector>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, PuntoVentaSector.Properties.PuntoVenta,
                "=", puntoVenta.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public virtual EntityCollection<PuntoVentaSector> PuntoVentaSectorReadByCaja(int cajaId)
        {
            string hql = "Select pvs from CajaPuntoVenta cpv, PuntoVentaSector pvs where cpv.PuntoVenta.Id = pvs.PuntoVenta.Id and cpv.CajaId = :caja";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("caja", cajaId);
            return dalEngine.GetManyByQuery <PuntoVentaSector>(query);
        }

        public virtual EntityCollection<PuntoVentaSector> PuntoVentaSectorReadByPuntoVentaNoDelete(PuntoVenta puntoVenta)
        {
            ReadManyCommand<PuntoVentaSector> readCmd = new ReadManyCommand<PuntoVentaSector>(dalEngine);

            Filter filter = new Filter();

            filter.Add(PuntoVentaSector.Properties.PuntoVenta,
                "=", puntoVenta.Id);

            filter.Add(BooleanOp.And, PuntoVentaSector.Properties.Deleted,
                "=", false);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public virtual EntityCollection<PuntoVentaSector> PuntoVentaSectorReadByPuntoVentaAndSectorId(PuntoVenta puntoVenta, int sectorId)
        {
            ReadManyCommand<PuntoVentaSector> readCmd = new ReadManyCommand<PuntoVentaSector>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, PuntoVentaSector.Properties.PuntoVenta,
                "=", puntoVenta.Id);

            filter.Add(BooleanOp.And, PuntoVentaSector.Properties.Sector,
                "=", sectorId);

            readCmd.Filter = filter;

            EntityCollection<PuntoVentaSector> puntoVentaSector = readCmd.Execute();

            if (puntoVentaSector != null && puntoVentaSector.Count == 0)
                puntoVentaSector = null;

            return puntoVentaSector;
        }
        public virtual void PuntoVentaSectorDelete(PuntoVentaSector puntoVentaSector)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            puntoVentaSector.Deleted = true;
            puntoVentaSector.DeleteUser = user.Id;
            puntoVentaSector.DeleteDate = enfoke.Time.Now;


            dalEngine.Update<PuntoVentaSector>(puntoVentaSector);
        }
        public virtual void PuntoVentaSectorDeleteByPuntoVenta(PuntoVenta puntoVenta)
        {
            EntityCollection<PuntoVentaSector> puntoVentaSectores = this.PuntoVentaSectorReadByPuntoVenta(puntoVenta);

            if (puntoVentaSectores != null)
            {
                foreach (PuntoVentaSector puntoVentaSector in puntoVentaSectores)
                    this.PuntoVentaSectorDelete(puntoVentaSector);
            }
        }
        public virtual PuntoVentaSector PuntoVentaSectorInsert(PuntoVentaSector puntoVentaSector)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            puntoVentaSector.CreateDate = enfoke.Time.Now;
            puntoVentaSector.CreateUser = user.Id;

            return dalEngine.Update<PuntoVentaSector>(puntoVentaSector);
        }

        #endregion

        #region PuntoVentaConfiguracion






        #endregion

        #region LoteCierreFiscal






        #endregion

        #region Sector

        public virtual EntityCollection<Sector> SectorReadByEsRecepcion(bool esRecepcion)
        {
            return this.SectorReadByEsRecepcionAndSucursal(esRecepcion, null);
        }

        public virtual EntityCollection<Sector> SectorReadByEsRecepcionOFacturacionAndSucursal(bool esRecepcionOFacturacion, int? sucursalId)
        {
            string hql = "FROM Sector s " +
                        "WHERE " + (esRecepcionOFacturacion ? "" : "NOT") + "EXISTS ( SELECT sts.Id " +
                                                                        "FROM SectorTipoSector sts " +
                                                                        "WHERE sts.Sector.Id = s.Id " +
                                                                        "AND sts.TipoSector.Id IN (:esRecepcionOFacturacion)) " +
                        (sucursalId.HasValue ? "AND s.Sucursal.Id = :idSucursal" : "");

            IQuery query = dalEngine.CreateQuery(hql);
            List<int> ids = new List<int>();
            ids.Add((int)TipoSectorEnum.Recepcion);
            ids.Add((int)TipoSectorEnum.Facturacion);
            query.SetParameterList("esRecepcionOFacturacion", ids);
            if (sucursalId.HasValue)
                query.SetParameter("idSucursal", sucursalId.Value);

            return dalEngine.GetManyByQuery<Sector>(query);
        }

        public virtual EntityCollection<Sector> SectorReadByEsRecepcionAndSucursal(bool esRecepcion, int? sucursalId)
        {
            string hql = "FROM Sector s " +
                        "WHERE " + (esRecepcion ? "" : "NOT") + "EXISTS ( SELECT sts.Id " +
                                                                        "FROM SectorTipoSector sts " +
                                                                        "WHERE sts.Sector.Id = s.Id " +
                                                                        "AND sts.TipoSector.Id = :idTipoSector) " +
                        (sucursalId.HasValue ? "AND s.Sucursal.Id = :idSucursal" : "");

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTipoSector", (int)TipoSectorEnum.Recepcion);
            if (sucursalId.HasValue)
                query.SetParameter("idSucursal", sucursalId.Value);

            return dalEngine.GetManyByQuery<Sector>(query);
        }

        #endregion
    }
}

