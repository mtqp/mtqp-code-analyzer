using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Valorizacion;
using enfoke.Eges.Persistence;
using enfoke.Eges.Entities.Results;
using NHibernate;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.AOP;
using enfoke.Eges.Persistance;
using System.Linq;
using enfoke.Eges.Entities.No_Mapeadas;

namespace enfoke.Eges.Data
{
    public class CajaDalc : Dalc, IService
    {
        protected CajaDalc(NotConstructable dummy) : base(dummy) { }

        #region Caja

        /// <summary>
        /// [PC] Retorno una todas las cajas posibles para un usuario
        /// </summary>
        /// <param name="usuarioId">ID del usuario</param>
        /// <returns>Las cajas correspondientes</returns>
        public EntityCollection<Caja> CajaReadAllBySecurityUserId(int usuarioId)
        {
            int operativa = (int)CajaEnum.Operativa;

            string hql = "select distinct c from Caja c , SecurityUserSector su  where c.Sucursal = su.Sector.Sucursal or c.Sucursal is null)"
                         +
                         " and c.Operativa = :operativa and su.SecurityUser.Id = :usuarioId and not exists (select cu from CajaUsuario cu where c = cu.Caja and cu.FechaCierre is null)  order by c.Name ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("usuarioId", usuarioId);
            query.SetParameter("operativa", operativa);

            return dalEngine.GetManyByQuery<Caja>(query);
        }

        /// <summary>
        /// [PC] Retorno una todas las cajas posibles para un usuario
        /// </summary>
        /// <param name="usuarioId">ID del usuario</param>
        /// <returns>Colección de cajas</returns>
        public EntityCollection<Caja> CajaReadBySecurityUserIdInCajaHabilitada(int usuarioId)
        {
            int operativa = (int)CajaEnum.Operativa;

            string hql = "select distinct c from Caja c , SecurityUserSector su ,CajaUsuarioHabilitado cuh where cuh.CajaId = c.Id and cuh.Deleted = false and cuh.UsuarioId = :usuarioId and su.SecurityUser.Id = cuh.UsuarioId"
                         + " and (c.Sucursal = su.Sector.Sucursal or c.Sucursal is null)"
                         +
                         " and c.Operativa = :operativa and not exists (select cu from CajaUsuario cu where c = cu.Caja and cu.FechaCierre is null)  order by c.Name ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("usuarioId", usuarioId);
            query.SetParameter("operativa", operativa);

            return dalEngine.GetManyByQuery<Caja>(query);
        }

        /// <summary>
        /// [PC] Retorno una todas las cajas posibles para un usuario logueado en un determinado sector
        /// </summary>
        /// <param name="usuarioId">ID del usuario</param>
        /// /// <param name="sectorId">ID del sector</param>
        /// <returns>Colección de cajas</returns>
        public EntityCollection<Caja> CajaReadBySecurityUserIdAndSector(int usuarioId, int? sectorId)
        {
            int operativa = (int)CajaEnum.Operativa;
            string hql = "select distinct caj " +
                         "from Caja caj " +
                         ", PuntoVentaSector pvs " +
                         ", CajaUsuarioHabilitado cuh " +
                         ", CajaPuntoVenta cpv " +
                         "where cuh.CajaId = caj.Id " +
                         "and caj.Id = cpv.CajaId " +
                         "and cpv.PuntoVenta.Id = pvs.PuntoVenta.Id " +
                         "and cuh.UsuarioId = :usuarioId " +
                         "and caj.Operativa = :operativa " +
                         "and cuh.Deleted = false " +
                         "and pvs.Deleted = false ";

            if (sectorId.HasValue)
                hql += "and pvs.Sector.Id = :sectorId ";

            hql +=
                "and not exists (select cu from CajaUsuario cu where caj = cu.Caja and cu.FechaCierre is null)  order by caj.Name ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("usuarioId", usuarioId);
            query.SetParameter("operativa", operativa);

            if (sectorId.HasValue)
                query.SetParameter("sectorId", sectorId.Value);

            return dalEngine.GetManyByQuery<Caja>(query);
        }











        public Caja CajaReadByTag(string tag)
        {
            return dalEngine.GetByProperty<Caja>(Caja.Properties.Tag, tag);
        }

        /// <summary>
        /// Retorno todas las Cajas
        /// </summary>
        /// <returns>Todas las Cajas</returns>
        public EntityCollection<Caja> CajaReadAll(bool traerCajaFondeo)
        {
            ReadManyCommand<Caja> readCmd = new ReadManyCommand<Caja>(dalEngine);

            if (!traerCajaFondeo)
            {
                Filter filter = new Filter();
                filter.Add(Caja.Properties.Operativa,
                           "=", (int)CajaEnum.Fondeo);
                readCmd.Filter = filter;
            }

            Sort sort = new Sort();
            sort.Add(Caja.Properties.Operativa, SortingDirection.Asc);
            sort.Add(Caja.Properties.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<Caja> CajaReadOperativaBySucursal(int? sucId)
        {
            int operativa = (int)CajaEnum.Operativa;
            StringBuilder hql = new StringBuilder();
            hql.Append("FROM Caja AS caj").Append(" ");
            hql.Append("WHERE caj.Operativa = :operativa ");
            hql.Append("AND caj.Deleted = false ");
            if (sucId.HasValue)
                hql.Append("AND caj.Sucursal.Id = :sucId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("operativa", operativa);

            if (sucId.HasValue)
                query.SetParameter("sucId", sucId.Value);

            return dalEngine.GetManyByQuery<Caja>(query);
        }

        public EntityCollection<Caja> CajaReadAll()
        {
            return this.CajaReadAll(true);
        }

        public EntityCollection<Caja> CajaReadFondeoBySucursal(int sucursalId)
        {
            ReadManyCommand<Caja> readCmd = new ReadManyCommand<Caja>(dalEngine);

            Filter filter = new Filter();
            filter.Add(Caja.Properties.Operativa,
                       "!=", (int)CajaEnum.Operativa);

            filter.Add(BooleanOp.And,
                       Caja.Properties.Operativa,
                       "!=",
                       (int)CajaEnum.Logica);

            if (sucursalId != 0)
            {
                filter.Add(BooleanOp.And,
                           Caja.Properties.Sucursal.Id,
                           "=",
                           sucursalId);
            }
            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Caja.Properties.Operativa, SortingDirection.Asc);
            sort.Add(Caja.Properties.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<Caja> CajaReadLogicaBySucursalId(int? sucursalId)
        {
            ReadManyCommand<Caja> readCmd = new ReadManyCommand<Caja>(dalEngine);

            Filter filter = new Filter();
            filter.Add(Caja.Properties.Operativa,
                       "=", (int)CajaEnum.Logica);

            if (sucursalId.HasValue)
            {
                filter.Add(BooleanOp.And,
                           Caja.Properties.Sucursal.Id,
                           "=",
                           sucursalId.Value);
            }
            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Caja.Properties.Operativa, SortingDirection.Asc);
            sort.Add(Caja.Properties.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<Caja> CajaReadLogicaAndOperativaBySucursalId(int? sucursalId)
        {
            int[] tiposCaja = new int[] { (int)CajaEnum.Fondeo, (int)CajaEnum.Logica };

            StringBuilder hql = new StringBuilder("from Caja caj ");
            hql.Append("where caj.Operativa in ").Append(Utils.EnumerableConvert.ToString(tiposCaja)).Append(" ");

            if (sucursalId.HasValue == true)
                hql.Append("and caj.Sucursal.Id = :sucursalId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (sucursalId.HasValue == true)
                query.SetParameter("sucursalId", sucursalId.Value);

            return dalEngine.GetManyByQuery<Caja>(query);
        }

        /// <summary>
        /// Retorno una Colección con Todas las Cajas aún no Asignadas
        /// </summary>
        /// <returns>Colección de Cajas no Asignadas</returns>
        public EntityCollection<Caja> CajaReadNoAsignadas()
        {
            ReadManyCommand<Caja> readCmd = new ReadManyCommand<Caja>(dalEngine);

            int operativa = (int)CajaEnum.Operativa;

            Filter filter = new Filter();

            filter.Add(Caja.Properties.Id,
                       "NOT IN", "(select cu.Caja.Id from CajaUsuario cu where cu.FechaCierre IS NULL)");

            filter.Add(BooleanOp.And,
                       Caja.Properties.Activa,
                       "=", true);
            filter.Add(BooleanOp.And,
                       Caja.Properties.Operativa,
                       "=", operativa);

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(Caja.Properties.Operativa, SortingDirection.Asc);
            sort.Add(sortItem);
            SortItem sortItem2 = new SortItem(Caja.Properties.Name, SortingDirection.Asc);
            sort.Add(sortItem2);

            readCmd.Sort = sort;
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<Caja> CajaReadNoAsignadas(int sucursalId)
        {
            ReadManyCommand<Caja> readCmd = new ReadManyCommand<Caja>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Caja.Properties.Id,
                       "NOT IN", "(select cu.Caja.Id from CajaUsuario cu where cu.FechaCierre IS NULL)");

            filter.Add(BooleanOp.And,
                       Caja.Properties.Activa,
                       "=", true);
            filter.Add(BooleanOp.And,
                       Caja.Properties.Operativa,
                       "=", true);

            if (sucursalId != 0)
            {
                filter.Add(BooleanOp.And,
                           Caja.Properties.Sucursal.Id,
                           "=", sucursalId);
            }

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(Caja.Properties.Operativa, SortingDirection.Asc);
            sort.Add(sortItem);
            SortItem sortItem2 = new SortItem(Caja.Properties.Name, SortingDirection.Asc);
            sort.Add(sortItem2);

            readCmd.Sort = sort;
            readCmd.Filter = filter;
            readCmd.Sort = sort;
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

















        #endregion

        #region CajaUsuario

        /// <summary>
        /// [PC] Retorno todos los  SecurityUser con algun movimiento en caja usuario
        /// </summary>
        /// <param name="fecha">Fecha de movimiento</param>
        /// /// <param name="cajaId">Id de la caja a buscar</param>
        /// <returns>Colección de SecurityUser</returns>
        public EntityCollection<SecurityUser> CajaUsuarioReadSecurityUserByFecha(DateTime fecha, int? cajaId)
        {
            // [PC]
            //***************************//
            //  preguntar este metodo    //
            //  en donde se puede ubicar //
            //***************************//

            String hql = "select distinct cu.Usuario " +
                         "from CajaUsuario cu " +
                         "where cu.FechaAlta <= :fecha " +
                         "and cu.FechaCierre >= :fecha ";
            if (cajaId.HasValue)
                hql += "and cu.Caja.Id = :cajaId ";
            hql += "order by cu.Usuario.LastName asc, cu.Usuario.Name asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", fecha.Date);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        public EntityCollection<SecurityUser> CajaUsuarioReadSecurityUserByFecha(DateTime fechaDesde, DateTime fechaHasta, int? cajaId, bool cajaAbierta, bool cajaCerrada)
        {
            String hql = "select distinct cu.Usuario " +
                         "from CajaUsuario cu " +
                         "where cu.FechaAlta >= :fechaDesde " +
            "and  cu.FechaAlta < :fechaHasta ";


            if (cajaAbierta != cajaCerrada)
            {
                if (cajaAbierta)
                    hql += "and cu.FechaCierre is null ";
                else if (cajaCerrada)
                    hql += "and cu.FechaCierre is not null ";
            }


            if (cajaId.HasValue)
                hql += "and cu.Caja.Id = :cajaId ";
            hql += "order by cu.Usuario.LastName asc, cu.Usuario.Name asc ";


            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1));


            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);


            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }


        /// <summary>
        /// [PC] Retorno todos los  SecurityUser con algun movimiento en caja usuario
        /// </summary>
        /// /// <param name="cajaId">Id de la caja a buscar</param>
        /// <returns>Colección de SecurityUser</returns>
        public EntityCollection<SecurityUser> CajaUsuarioReadSecurityUserByCaja(int cajaId)
        {
            // [PC]
            //***************************//
            //  preguntar este metodo    //
            //  en donde se puede ubicar //
            //***************************//

            String hql = "select distinct cu.Usuario " +
                         "from CajaUsuario cu " +
                         "where cu.Caja.Id = :cajaId " +
                         "order by cu.Usuario.LastName asc, cu.Usuario.Name asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaId", cajaId);
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }











        public CajaUsuario CajaUsuarioReadByUser(SecurityUser user)
        {
            EntityCollection<CajaUsuario> cajas;
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);

            Filter filter = new Filter();
            filter.Add(CajaUsuario.Properties.Usuario.Id,
                       "=", user.Id);

            readCmd.Filter = filter;

            cajas = readCmd.Execute();

            if (cajas == null || cajas.Count == 0)
                return null;
            else
                return cajas[0];
        }

        public bool CajaUsuarioReadCheckNoVigenteByUser(int user)
        {
            string hql = "select cu.Id " +
                         "from CajaUsuario cu " +
                         "where cu.Usuario.Id = :user " +
                         "and cu.FechaCierre is null ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("user", user);
            query.SetMaxResults(1);

            Object retObj = query.UniqueResult<Object>();

            if (retObj != null)
                return true;
            else
                return false;
        }

        /// <summary>
        /// Retorno una Colección con Todas las Asignaciones de Cajas Vigentes
        /// </summary>
        /// <returns>Colección de CajaUsuario</returns>
        public EntityCollection<CajaUsuario> CajaUsuarioReadVigentes()
        {
            return this.CajaUsuarioReadVigentesBySucursal(0);
        }

        public EntityCollection<CajaUsuario> CajaUsuarioReadVigentesBySucursal(int sucursalId)
        {
            int logica = (int)CajaEnum.Logica;
            StringBuilder hql = new StringBuilder();
            hql.Append("SELECT DISTINCT cau ");
            hql.Append("FROM CajaUsuario cau ");
            hql.Append("WHERE cau.FechaCierre IS NULL ");
            hql.Append("AND cau.Caja.Operativa != :logica ");
            if (sucursalId != 0)
                hql.Append("AND cau.Caja.Sucursal.Id = :sucursalId ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (sucursalId != 0)
                query.SetParameter("sucursalId", sucursalId);

            query.SetParameter("logica", logica);
            return dalEngine.GetManyByQuery<CajaUsuario>(query);
        }

        /// <summary>
        /// Retorno una Colección con Todas las Asignaciones de Cajas No Operativas Vigentes
        /// </summary>
        /// <returns>Colección de CajaUsuario</returns>
        public EntityCollection<CajaUsuario> CajaUsuarioReadVigentesNoOperativas()
        {
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);

            Filter filter = new Filter();

            filter.Add(CajaUsuario.Properties.FechaCierre,
                       "IS",
                       null);

            filter.Add(BooleanOp.And,
                       CajaUsuario.Properties.Caja.Operativa,
                       "!=", (int)CajaEnum.Operativa);

            Sort sort = new Sort();
            SortItem sortItem = new SortItem(CajaUsuario.Properties.FechaAlta, SortingDirection.Asc);
            sort.Add(sortItem);

            readCmd.Sort = sort;
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Retorno una Colección con Todas las Asignaciones de Cajas de un Usuario
        /// </summary>
        /// <param name="user">Usuario Conectado al Sistema</param>
        /// <returns>Colección de CajaUsuario</returns>
        public EntityCollection<CajaUsuario> CajaUsuarioReadVigentesUsuario(SecurityUser user)
        {
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, CajaUsuario.Properties.FechaCierre,
                       " IS ",
                       null);

            filter.Add(BooleanOp.And, CajaUsuario.Properties.Usuario.Id,
                       "=", user.Id);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(CajaUsuario.Properties.FechaAlta, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Retorno las Asignaciones de Caja para una Fecha
        /// </summary>
        /// <param name="fecha">Fecha de las Asignaciones</param>
        /// <returns>Colección de CajaUsuario</returns>
        public EntityCollection<CajaUsuario> CajaUsuarioReadByFecha(DateTime fecha)
        {
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);
            Filter filter = new Filter();

            filter.Add(BooleanOp.And, CajaUsuario.Properties.FechaAlta,
                       ">=", fecha);

            filter.Add(BooleanOp.And, CajaUsuario.Properties.FechaAlta,
                       "<", fecha.AddDays(1));

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(CajaUsuario.Properties.Caja.Operativa, SortingDirection.Asc);
            sort.Add(CajaUsuario.Properties.Caja.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Crea un Registro en la CajaUsuario
        /// </summary>
        /// <param name="caja">Caja a Asignar</param>
        /// <param name="user">Usuario Asignado</param>
        /// <param name="saldo">Saldo Inicial</param>
        /// <param name="cajaApertura">Caja de la cual se pasa el Saldo de Apertura</param>
        [RequiresTransaction]
        public virtual void CajaUsuarioCreate(Caja caja, SecurityUser userAsignado, decimal saldo, CajaUsuario cajaApertura)
        {
            // Valido que la caja no se encuentre asignada a otro usuario
            if (this.CajaUsuarioReadEstaAsignadaByCaja(caja.Id))
                throw new Exception("La caja '" + caja.Name + "' se encuentra abierta por otro usuario");

            // Formo la CajaUsuario
            CajaUsuario cajaUsuario = new CajaUsuario();
            cajaUsuario.Caja = caja;
            cajaUsuario.FechaAlta = enfoke.IO.Time.Now;
            cajaUsuario.SaldoEfectivo = saldo;
            if (caja.Operativa == (int)CajaEnum.Operativa)
                cajaUsuario.Usuario = userAsignado;


            // Inserto
            cajaUsuario = dalEngine.Update<CajaUsuario>(cajaUsuario);

            if ((caja.Operativa == (int)CajaEnum.Operativa) && saldo > 0)
            {
                // Chequeo que la Caja tenga Saldo
                this.CajaUsuarioChequearSaldoEgreso(cajaApertura, saldo, true);

                // Creo el Movimiento Inicial (Egreso de Caja Origen)
                MovimientoCaja movimientoEgreso = new MovimientoCaja();

                // Asigno los Datos
                movimientoEgreso.CajaUsuario = cajaApertura;
                movimientoEgreso.CajaUsuarioID = cajaApertura.Id;
                movimientoEgreso.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
                movimientoEgreso.ImporteIngreso = 0;
                movimientoEgreso.ImporteEgreso = saldo;
                movimientoEgreso.Descripcion = "Transferencia por Apertura de Caja";

                // Creo el Item
                MovimientoCajaItem itemEgreso = new MovimientoCajaItem();

                itemEgreso.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Transferencia;
                itemEgreso.Descripcion = "Transferencia por Apertura de Caja " + caja.Name;
                itemEgreso.ImporteIngreso = 0;
                itemEgreso.ImporteEgreso = saldo;
                itemEgreso.CajaImpacto = caja;
                itemEgreso.CajaImpactoID = caja.Id;

                // Inserto el Movimiento con el Item
                this.MovimientoCajaInsert(movimientoEgreso, itemEgreso, true);

                // Creo el Movimiento Inicial (Ingreso a Caja Apertura)
                MovimientoCaja movimientoIngreso = new MovimientoCaja();

                // Asigno los Datos
                movimientoIngreso.CajaUsuario = cajaUsuario;
                movimientoIngreso.CajaUsuarioID = cajaUsuario.Id;
                movimientoIngreso.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Apertura;
                movimientoIngreso.ImporteIngreso = saldo;
                movimientoIngreso.ImporteEgreso = 0;
                movimientoIngreso.Descripcion = "Apertura de Caja";

                // Creo el Item
                MovimientoCajaItem itemIngreso = new MovimientoCajaItem();

                itemIngreso.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Apertura;
                itemIngreso.Descripcion = "Apertura de Caja";
                itemIngreso.ImporteIngreso = saldo;
                itemIngreso.ImporteEgreso = 0;
                itemIngreso.CajaImpacto = cajaApertura.Caja;
                itemIngreso.CajaImpactoID = cajaApertura.Caja.Id;

                // Inserto el Movimiento con el Item
                this.MovimientoCajaInsert(movimientoIngreso, itemIngreso, false);
            }
        }

        /// <summary>
        /// [PC] Crea un Registro en la CajaUsuario sin movimiento de transferencia
        /// </summary>
        /// <param name="caja">Caja a Asignar</param>
        /// <param name="user">Usuario Asignado</param>
        /// <param name="saldo">Saldo Inicial</param>
        /// <param name="cajaApertura">Caja de la cual se pasa el Saldo de Apertura</param>
        /// <param name="userSistema">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual void CajaUsuarioCreateForCierreConSaldo(Caja caja, SecurityUser userAsignado, decimal saldo,
                                                     CajaUsuario cajaApertura)
        {
            // Valido que la caja no se encuentre asignada a otro usuario
            if (this.CajaUsuarioReadEstaAsignadaByCaja(caja.Id))
                throw new Exception("La caja '" + caja.Name + "' se encuentra abierta por otro usuario");

            // Formo la CajaUsuario
            CajaUsuario cajaUsuario = new CajaUsuario();
            cajaUsuario.Caja = caja;
            cajaUsuario.FechaAlta = enfoke.IO.Time.Now;
            cajaUsuario.SaldoEfectivo = saldo;
            if (caja.Operativa == (int)CajaEnum.Operativa)
                cajaUsuario.Usuario = userAsignado;


            // Inserto
            cajaUsuario = dalEngine.Update<CajaUsuario>(cajaUsuario);

            // Creo el Movimiento Inicial (Ingreso a Caja Apertura)
            MovimientoCaja movimientoIngreso = new MovimientoCaja();

            // Asigno los Datos
            movimientoIngreso.CajaUsuario = cajaUsuario;
            movimientoIngreso.CajaUsuarioID = cajaUsuario.Id;
            movimientoIngreso.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Apertura;
            movimientoIngreso.ImporteIngreso = saldo;
            movimientoIngreso.ImporteEgreso = 0;
            movimientoIngreso.Descripcion = "Apertura de Caja";

            // Creo el Item
            MovimientoCajaItem itemIngreso = new MovimientoCajaItem();

            itemIngreso.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Apertura;
            itemIngreso.Descripcion = "Apertura de Caja";
            itemIngreso.ImporteIngreso = saldo;
            itemIngreso.ImporteEgreso = 0;
            //itemIngreso.CajaImpacto = cajaApertura.Caja;
            //itemIngreso.CajaImpactoID = cajaApertura.Caja.Id;

            // Inserto el Movimiento con el Item
            this.MovimientoCajaInsert(movimientoIngreso, itemIngreso, false);
        }

        /// <summary>
        /// Retorno una Asignación de una Caja Determinada
        /// </summary>
        /// <param name="id ">ID de la Caja que se Busca la Asignación</param>
        /// <returns>La CajaUsuario (Asignación)</returns>
        public CajaUsuario CajaUsuarioReadByCaja(int id)
        {
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);

            Filter filter = new Filter();

            filter.Add(CajaUsuario.Properties.Caja.Id,
                       "=", id);

            filter.Add(BooleanOp.And, CajaUsuario.Properties.FechaCierre,
                       " IS ",
                       null);

            readCmd.Filter = filter;

            EntityCollection<CajaUsuario> cajas = readCmd.Execute();
            if (cajas.Count > 1)
                throw new Exception("La caja '" + dalEngine.GetById<Caja>(id).Name +
                                    "' se encuentra abierta por más de un usuario.");
            else if (cajas.Count == 1)
                return cajas[0];
            else
                return null;
        }

        /// <summary>
        /// [PC] Retorno verdadero o falso dependiendo si no se encuentra ya asignada
        /// </summary>
        /// <param name="id ">ID de la Caja que se Busca la Asignación</param>
        /// <returns>Verdadero o Falso</returns>
        public bool CajaUsuarioReadEstaAsignadaByCaja(int id)
        {
            ReadManyCommand<CajaUsuario> readCmd = new ReadManyCommand<CajaUsuario>(dalEngine);

            Filter filter = new Filter();

            filter.Add(CajaUsuario.Properties.Caja.Id,
                       "=", id);

            filter.Add(BooleanOp.And, CajaUsuario.Properties.FechaCierre,
                       "IS",
                       null);

            readCmd.Filter = filter;

            EntityCollection<CajaUsuario> cajas = readCmd.Execute();

            if (cajas.Count > 0)
                return true;
            else
                return false;
        }

        #region CajasConSaldo

        /// <summary>
        /// [PC] Cierro una Caja Realizando todos los Movimientos Correspondientes cuando el se permite el cierre con saldo
        /// </summary>
        /// <param name="caja">CajaUsuario a Cerrar</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual EntityCollection<MovimientoCajaTransferencia> CajaUsuarioCerrarConSaldo(EntityCollection<Pago> pagos,
                                                                                     Decimal importeAtransferir,
                                                                                     Decimal saldoRemanente,
                                                                                     CajaUsuario caja)
        {
            // Busco si la caja tiene movimientos de transferencia pendientes ya sea de origen como de destino
            EntityCollection<MovimientoCajaTransferencia> movimientosTransferenciaPendientes =
                this.MovimientoCajaTransferenciaReadByCajaUsuarioIdAndEstadoIdWithObjects(caja.Id,
                                                                                          (int)
                                                                                          EstadoTransferenciaEnum.
                                                                                              Pendiente);

            // Si existen los retorno y no permito cerrar la caja
            if (movimientosTransferenciaPendientes != null && movimientosTransferenciaPendientes.Count > 0)
                return movimientosTransferenciaPendientes;

            Decimal importeTran = importeAtransferir;
            // Creo la Colección de Items de Cierre
            EntityCollection<MovimientoCajaItem> itemsCierre = new EntityCollection<MovimientoCajaItem>();

            // Obtengo los Movimientos de Cierre
            EntityCollection<MovimientoCajaCierreView> movimientos = this.MovimientoCajaCierreReadByCajaUsuario(caja.Id);

            // Inicio el Saldo Restante con el Actual de la Caja
            decimal saldoRestante = caja.SaldoNoEfectivo + caja.SaldoEfectivo;

            decimal totalDeTransferenciasRealizadas = 0;

            // Inicio la Colección de Movimientos a Generar
            List<Movimiento> movimientosImpacto = new List<Movimiento>();

            // Recorro la Coleccion de Movimientos
            foreach (MovimientoCajaCierreView i in movimientos)
            {
                // Chequeo si la Caja de Impacto es NULL
                if (i.CajaImpactoID.HasValue)
                {
                    //Chequeo si la Caja es del tipo Lógica
                    if (i.CajaImpacto.Operativa == (int)CajaEnum.Logica)
                    {
                        // Chequeo que la Caja de Impacto este Asignada
                        if (i.CajaUsuarioImpacto == null)
                            throw new NotLoggeableException("La Caja de Impacto no se Encuentra Asignada");

                        // Chequeo que las Cajas de Origen y Destino No sean las Mismas
                        if (i.CajaUsuarioImpacto.Id != caja.Id)
                        {
                            /**
                             * El Saldo es del Movimiento Original, es decir, en este debe ser Inverso
                             * Si es > 0 (Fue un Ingreso que ahora es Egreso) -> Caja de Cierre es Origen
                             * Si es < 0 (Fue un Egreso que ahora es Ingreso) -> Caja de Cierre es Destino
                             * */
                            // Acumulo para Cancelar
                            saldoRestante += (i.Saldo * -1);

                            decimal saldo = Math.Abs(i.Saldo);
                            totalDeTransferenciasRealizadas += saldo;

                            /**
                             * Movimiento en Caja de Ciere
                             * */
                            // Creo el Item del Movimiento de Cierre
                            MovimientoCajaItem itemCierre = new MovimientoCajaItem();
                            itemCierre.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                            itemCierre.CajaImpacto = i.CajaImpacto;
                            itemCierre.CajaImpactoID = i.CajaImpactoID;
                            if (i.Saldo > 0)
                            {
                                itemCierre.ImporteIngreso = 0;
                                itemCierre.ImporteEgreso = saldo;
                                itemCierre.Descripcion = "Transferencia a Caja " + i.CajaImpacto.Name;
                            }
                            else
                            {
                                itemCierre.ImporteIngreso = saldo;
                                itemCierre.ImporteEgreso = 0;
                                itemCierre.Descripcion = "Transferencia desde Caja " + i.CajaImpacto.Name;
                            }

                            // Lo Agrego a la Colección
                            itemsCierre.Add(itemCierre);

                            /**
                             * Movimiento en Caja de Impacto
                             * */
                            // Creo el Movimiento
                            Movimiento movimiento = new Movimiento();

                            // Creo el Movimiento en la Caja de Impacto
                            MovimientoCaja movimientoImpacto = new MovimientoCaja();

                            // Creo el Item
                            MovimientoCajaItem itemImpacto = new MovimientoCajaItem();

                            // Asigno los Datos del Movimiento
                            movimientoImpacto.CajaUsuario = i.CajaUsuarioImpacto;
                            movimientoImpacto.CajaUsuarioID = i.CajaUsuarioImpacto.Id;
                            movimientoImpacto.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
                            movimientoImpacto.Descripcion = "Transferencia por Cierre";

                            // Asigno los Datos del Item
                            itemImpacto.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                            itemImpacto.CajaImpacto = caja.Caja;
                            itemImpacto.CajaImpactoID = caja.Caja.Id;

                            if (i.Saldo > 0)
                            {
                                movimientoImpacto.ImporteIngreso = saldo;
                                movimientoImpacto.ImporteEgreso = 0;

                                itemImpacto.ImporteIngreso = saldo;
                                itemImpacto.ImporteEgreso = 0;
                                itemImpacto.Descripcion = "Transferencia desde Caja " + caja.NameCaja;
                            }
                            else if (i.Saldo <= 0)
                            {
                                // Chequeo que la Caja tenga Saldo
                                //this.CajaUsuarioChequearSaldoEgreso(movimientoImpacto.CajaUsuario, saldo, false);

                                movimientoImpacto.ImporteIngreso = 0;
                                movimientoImpacto.ImporteEgreso = saldo;

                                itemImpacto.ImporteIngreso = 0;
                                itemImpacto.ImporteEgreso = saldo;
                                itemImpacto.Descripcion = "Transferencia a Caja " + caja.NameCaja;
                            }

                            // Inserto el Movimiento en la Colección
                            movimiento.MovimientoCaja = movimientoImpacto;
                            movimiento.Items.Add(itemImpacto);
                            movimientosImpacto.Add(movimiento);
                        }
                    }
                }
            }

            // Chequeo si Quedo Saldo restante para hacer la Cancelacion
            if ((totalDeTransferenciasRealizadas - importeAtransferir) != 0)
            {
                Decimal imp = totalDeTransferenciasRealizadas - importeAtransferir;
                // Creo el Item del Movimiento de Cierre
                MovimientoCajaItem itemSaldoRestante = new MovimientoCajaItem();

                itemSaldoRestante.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.SaldoRestante;
                itemSaldoRestante.Descripcion = "Cancelación de Saldo Restante";
                itemSaldoRestante.ImporteIngreso = imp > 0 ? imp : 0;
                itemSaldoRestante.ImporteEgreso = imp > 0 ? 0 : imp;

                //itemSaldoRestante.ImporteIngreso = 0;
                //itemSaldoRestante.ImporteEgreso = imp;

                itemSaldoRestante.CajaImpactoID = null;

                // Lo Agrego a la Colección
                itemsCierre.Add(itemSaldoRestante);
            }

            // Si no tengo Items, genero uno en Cero
            if (itemsCierre.Count == 0)
            {
                // Creo el Item del Movimiento de Cierre
                MovimientoCajaItem itemCierre = new MovimientoCajaItem();

                itemCierre.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                itemCierre.CajaImpactoID = null;
                itemCierre.ImporteIngreso = 0;
                itemCierre.ImporteEgreso = 0;
                itemCierre.Descripcion = "Cierre de Caja Sin Saldo";

                // Lo Agrego a la Colección
                itemsCierre.Add(itemCierre);
            }

            // Creo el Movimiento de Cierre (Caja a Cerrar)
            MovimientoCaja movimientoCierre = this.MovimientoCajaCreate(importeTran, caja, (int)TipoMovimientoCajaEnum.Cierre, "Cierre de Caja");

            // Inserto el Movimiento de Cierre con los Items
            this.MovimientoCajaInsert(movimientoCierre, itemsCierre, false);

            // Inserto los Movimientos en las Cajas Impactadas
            foreach (Movimiento mov in movimientosImpacto)
                this.MovimientoCajaInsert(mov.MovimientoCaja, mov.Items, true);

            Decimal importeTotalPagos = 0;
            if (pagos != null && pagos.Count > 0)
            {
                foreach (Pago pag in pagos)
                {
                    if (pag.MedioPagoID != (int)MedioPagoEnum.Efectivo)
                    {
                        importeTotalPagos += pag.Importe;
                        PagoMovimientoCaja pagMovCaja = new PagoMovimientoCaja();
                        pagMovCaja.MovimientoCaja = movimientoCierre;
                        pagMovCaja.Pago = this.PagoInsert(pag);
                        dalEngine.Update(pagMovCaja);
                    }
                }

                Pago pagoEfectivoRestante = new Pago();
                pagoEfectivoRestante.Importe = importeAtransferir - importeTotalPagos;
                pagoEfectivoRestante.MedioPagoID = (int)MedioPagoEnum.Efectivo;

                PagoMovimientoCaja pagMovCajaRestante = new PagoMovimientoCaja();
                pagMovCajaRestante.MovimientoCaja = movimientoCierre;
                pagMovCajaRestante.Pago = this.PagoInsert(pagoEfectivoRestante);
                dalEngine.Update(pagMovCajaRestante);
            }

            // Cierro la Caja
            caja.FechaCierre = enfoke.IO.Time.Now;
            caja.SaldoEfectivo = 0;
            caja.SaldoNoEfectivo = 0;


            // Actualizo
            caja = dalEngine.Update<CajaUsuario>(caja);

            //modifico el saldo remanente de la caja
            caja.Caja.SaldoRemanente = saldoRemanente;


            //Actualizo 
            dalEngine.Update<Caja>(caja.Caja);

            return null;
        }

        #endregion

        /// <summary>
        /// Cierro una Caja Realizando todos los Movimientos Correspondientes
        /// </summary>
        /// <param name="caja">CajaUsuario a Cerrar</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual EntityCollection<MovimientoCajaTransferencia> CajaUsuarioCerrar(CajaUsuario caja)
        {
            // Busco si la caja tiene movimientos de transferencia pendientes ya sea de origen como de destino
            EntityCollection<MovimientoCajaTransferencia> movimientosTransferenciaPendientes =
                this.MovimientoCajaTransferenciaReadByCajaUsuarioIdAndEstadoIdWithObjects(caja.Id,
                                                                                          (int)
                                                                                          EstadoTransferenciaEnum.
                                                                                              Pendiente);

            // Si existen los retorno y no permito cerrar la caja
            if (movimientosTransferenciaPendientes != null && movimientosTransferenciaPendientes.Count > 0)
                return movimientosTransferenciaPendientes;

            // Creo el Movimiento de Cierre (Caja a Cerrar)
            MovimientoCaja movimientoCierre = new MovimientoCaja();

            // Asigno los Datos
            movimientoCierre.CajaUsuario = caja;
            movimientoCierre.CajaUsuarioID = caja.Id;
            movimientoCierre.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Cierre;
            if ((caja.SaldoNoEfectivo + caja.SaldoEfectivo) > 0)
            {
                movimientoCierre.ImporteIngreso = 0;
                movimientoCierre.ImporteEgreso = caja.SaldoNoEfectivo + caja.SaldoEfectivo;
            }
            else
            {
                movimientoCierre.ImporteIngreso = (caja.SaldoNoEfectivo + caja.SaldoEfectivo) * -1;
                movimientoCierre.ImporteEgreso = 0;
            }
            movimientoCierre.Descripcion = "Cierre de Caja";

            // Creo la Colección de Items de Cierre
            EntityCollection<MovimientoCajaItem> itemsCierre = new EntityCollection<MovimientoCajaItem>();

            // Obtengo los Movimientos de Cierre
            EntityCollection<MovimientoCajaCierreView> movimientos = this.MovimientoCajaCierreReadByCajaUsuario(caja.Id);

            // Inicio el Saldo Restante con el Actual de la Caja
            decimal saldoRestante = caja.SaldoNoEfectivo + caja.SaldoEfectivo;

            // Inicio la Colección de Movimientos a Generar
            List<Movimiento> movimientosImpacto = new List<Movimiento>();

            // Recorro la Coleccion de Movimientos
            foreach (MovimientoCajaCierreView i in movimientos)
            {
                // Chequeo si la Caja de Impacto es NULL
                if (i.CajaImpactoID.HasValue)
                {
                    // Chequeo que la Caja de Impacto este Asignada
                    if (i.CajaUsuarioImpacto == null)
                        throw new NotLoggeableException("La Caja de Impacto no se Encuentra Asignada");

                    // Chequeo que las Cajas de Origen y Destino No sean las Mismas
                    if (i.CajaUsuarioImpacto.Id != caja.Id)
                    {
                        /**
                         * El Saldo es del Movimiento Original, es decir, en este debe ser Inverso
                         * Si es > 0 (Fue un Ingreso que ahora es Egreso) -> Caja de Cierre es Origen
                         * Si es < 0 (Fue un Egreso que ahora es Ingreso) -> Caja de Cierre es Destino
                         * */
                        // Acumulo para Cancelar
                        saldoRestante += (i.Saldo * -1);

                        decimal saldo = Math.Abs(i.Saldo);

                        /**
                         * Movimiento en Caja de Ciere
                         * */
                        // Creo el Item del Movimiento de Cierre
                        MovimientoCajaItem itemCierre = new MovimientoCajaItem();

                        itemCierre.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                        itemCierre.CajaImpacto = i.CajaImpacto;
                        itemCierre.CajaImpactoID = i.CajaImpactoID;
                        if (i.Saldo > 0)
                        {
                            itemCierre.ImporteIngreso = 0;
                            itemCierre.ImporteEgreso = saldo;
                            itemCierre.Descripcion = "Transferencia a Caja " + i.CajaImpacto.Name;
                        }
                        else
                        {
                            itemCierre.ImporteIngreso = saldo;
                            itemCierre.ImporteEgreso = 0;
                            itemCierre.Descripcion = "Transferencia desde Caja " + i.CajaImpacto.Name;
                        }

                        // Lo Agrego a la Colección
                        itemsCierre.Add(itemCierre);

                        /**
                         * Movimiento en Caja de Impacto
                         * */
                        // Creo el Movimiento
                        Movimiento movimiento = new Movimiento();

                        // Creo el Movimiento en la Caja de Impacto
                        MovimientoCaja movimientoImpacto = new MovimientoCaja();

                        // Creo el Item
                        MovimientoCajaItem itemImpacto = new MovimientoCajaItem();

                        // Asigno los Datos del Movimiento
                        movimientoImpacto.CajaUsuario = i.CajaUsuarioImpacto;
                        movimientoImpacto.CajaUsuarioID = i.CajaUsuarioImpacto.Id;
                        movimientoImpacto.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
                        movimientoImpacto.Descripcion = "Transferencia por Cierre";

                        // Asigno los Datos del Item
                        itemImpacto.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                        itemImpacto.CajaImpacto = caja.Caja;
                        itemImpacto.CajaImpactoID = caja.Caja.Id;

                        if (i.Saldo > 0)
                        {
                            movimientoImpacto.ImporteIngreso = saldo;
                            movimientoImpacto.ImporteEgreso = 0;

                            itemImpacto.ImporteIngreso = saldo;
                            itemImpacto.ImporteEgreso = 0;
                            itemImpacto.Descripcion = "Transferencia desde Caja " + caja.NameCaja;
                        }
                        else
                        {
                            // Chequeo que la Caja tenga Saldo
                            this.CajaUsuarioChequearSaldoEgreso(movimientoImpacto.CajaUsuario, saldo, false);

                            movimientoImpacto.ImporteIngreso = 0;
                            movimientoImpacto.ImporteEgreso = saldo;

                            itemImpacto.ImporteIngreso = 0;
                            itemImpacto.ImporteEgreso = saldo;
                            itemImpacto.Descripcion = "Transferencia a Caja " + caja.NameCaja;
                        }

                        // Inserto el Movimiento en la Colección
                        movimiento.MovimientoCaja = movimientoImpacto;
                        movimiento.Items.Add(itemImpacto);
                        movimientosImpacto.Add(movimiento);
                    }
                }
            }

            // Chequeo si Quedo Saldo restante para hacer la Cancelacion
            if (saldoRestante != 0)
            {
                // Creo el Item del Movimiento de Cierre
                MovimientoCajaItem itemSaldoRestante = new MovimientoCajaItem();

                itemSaldoRestante.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.SaldoRestante;
                itemSaldoRestante.Descripcion = "Cancelación de Saldo Restante";
                itemSaldoRestante.ImporteIngreso = 0;
                itemSaldoRestante.ImporteEgreso = saldoRestante;
                itemSaldoRestante.CajaImpactoID = null;

                // Lo Agrego a la Colección
                itemsCierre.Add(itemSaldoRestante);
            }

            // Si no tengo Items, genero uno en Cero
            if (itemsCierre.Count == 0)
            {
                // Creo el Item del Movimiento de Cierre
                MovimientoCajaItem itemCierre = new MovimientoCajaItem();

                itemCierre.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.TransferenciaCierre;
                itemCierre.CajaImpactoID = null;
                itemCierre.ImporteIngreso = 0;
                itemCierre.ImporteEgreso = 0;
                itemCierre.Descripcion = "Cierre de Caja Sin Saldo";

                // Lo Agrego a la Colección
                itemsCierre.Add(itemCierre);
            }

            // Inserto el Movimiento de Cierrecon los Items
            this.MovimientoCajaInsert(movimientoCierre, itemsCierre, false);

            // Inserto los Movimientos en las Cajas Impactadas
            foreach (Movimiento mov in movimientosImpacto)
                this.MovimientoCajaInsert(mov.MovimientoCaja, mov.Items, true);

            // Cierro la Caja
            caja.SaldoEfectivo = 0;
            caja.SaldoNoEfectivo = 0;
            caja.FechaCierre = enfoke.IO.Time.Now;


            // Actualizo
            caja = dalEngine.Update<CajaUsuario>(caja);

            return null;
        }

        /// <summary>
        /// Realizo una Transferencia entre dos Cajas Asignadas
        /// </summary>
        /// <param name="cajaOrigen">Caja Origen</param>
        /// <param name="cajaDestino">Caja Destino</param>
        /// <param name="importe">Importe de la Transferencia</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual void CajaUsuarioTransferir(MovimientoCajaTransferencia movimientoTransferencia)
        {
            // Chequeo que el movimiento de transferencia se encuentre confirmado
            if (movimientoTransferencia.EstadoTransferenciaId == (int)EstadoTransferenciaEnum.Confirmada)
            {
                CajaUsuario cajaOrigen = dalEngine.GetById<CajaUsuario>(movimientoTransferencia.CajaUsuarioIdOrigen);
                CajaUsuario cajaDestino = dalEngine.GetById<CajaUsuario>(movimientoTransferencia.CajaUsuarioIdDestino);
                decimal importe = movimientoTransferencia.Importe;

                // Creo el Movimiento Origen
                MovimientoCaja movimientoOrigen = new MovimientoCaja();

                // Asigno los Datos
                movimientoOrigen.CajaUsuario = cajaOrigen;
                movimientoOrigen.CajaUsuarioID = cajaOrigen.Id;
                movimientoOrigen.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
                movimientoOrigen.ImporteIngreso = 0;
                movimientoOrigen.ImporteEgreso = importe;
                movimientoOrigen.Descripcion = "Transferencia Manual";

                // Creo el Item Origen
                MovimientoCajaItem itemOrigen = new MovimientoCajaItem();

                itemOrigen.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Transferencia;
                itemOrigen.ImporteIngreso = 0;
                itemOrigen.ImporteEgreso = importe;
                itemOrigen.CajaImpacto = cajaDestino.Caja;
                itemOrigen.CajaImpactoID = cajaDestino.Caja.Id;
                itemOrigen.Descripcion = "Transferencia Manual a Caja " + cajaDestino.NameCaja;

                // Inserto el Movimiento con el Item
                // No actualizo el saldo porque este ya fue
                // actualizado al momento de crear el movimiento de transferencia
                this.MovimientoCajaInsert(movimientoOrigen, itemOrigen, false);

                // Chequeo que la Caja Destino quede con Saldo
                this.CajaUsuarioChequearSaldoIngreso(cajaDestino, importe, true);

                // Creo el Movimiento Destino
                MovimientoCaja movimientoDestino = new MovimientoCaja();

                // Asigno los Datos
                movimientoDestino.CajaUsuario = cajaDestino;
                movimientoDestino.CajaUsuarioID = cajaDestino.Id;
                movimientoDestino.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
                movimientoDestino.ImporteIngreso = importe;
                movimientoDestino.ImporteEgreso = 0;
                movimientoDestino.Descripcion = "Transferencia Manual";

                // Creo el Item
                MovimientoCajaItem itemDestino = new MovimientoCajaItem();

                itemDestino.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Transferencia;
                itemDestino.ImporteIngreso = importe;
                itemDestino.ImporteEgreso = 0;
                itemDestino.CajaImpacto = cajaOrigen.Caja;
                itemDestino.CajaImpactoID = cajaOrigen.Caja.Id;
                itemDestino.Descripcion = "Transferencia Manual desde Caja " + cajaOrigen.NameCaja;

                // Inserto el Movimiento con el Item
                this.MovimientoCajaInsert(movimientoDestino, itemDestino, true);
            }
            else
                throw new NotLoggeableException("El movimiento de transferencia no se encuentra confirmado");
        }

        /// <summary>
        /// Realizo una Transferencia entre dos Cajas Asignadas
        /// </summary>
        /// <param name="cajaOrigen">Caja Origen</param>
        /// <param name="cajaDestino">Caja Destino</param>
        /// <param name="importe">Importe de la Transferencia</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual void CajaUsuarioTransferir(CajaUsuario cajaOrigen, CajaUsuario cajaDestino, decimal importe)
        {
            // Chequeo que la Caja Origen tenga Saldo
            this.CajaUsuarioChequearSaldoEgreso(cajaOrigen, importe, true);

            // Creo el Movimiento Origen
            MovimientoCaja movimientoOrigen = new MovimientoCaja();

            // Asigno los Datos
            movimientoOrigen.CajaUsuario = cajaOrigen;
            movimientoOrigen.CajaUsuarioID = cajaOrigen.Id;
            movimientoOrigen.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
            movimientoOrigen.ImporteIngreso = 0;
            movimientoOrigen.ImporteEgreso = importe;
            movimientoOrigen.Descripcion = "Transferencia Manual";

            // Creo el Item Origen
            MovimientoCajaItem itemOrigen = new MovimientoCajaItem();

            itemOrigen.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Transferencia;
            itemOrigen.ImporteIngreso = 0;
            itemOrigen.ImporteEgreso = importe;
            itemOrigen.CajaImpacto = cajaDestino.Caja;
            itemOrigen.CajaImpactoID = cajaDestino.Caja.Id;
            itemOrigen.Descripcion = "Transferencia Manual a Caja " + cajaDestino.NameCaja;

            // Inserto el Movimiento con el Item
            this.MovimientoCajaInsert(movimientoOrigen, itemOrigen, true);

            // Chequeo que la Caja Destino quede con Saldo
            this.CajaUsuarioChequearSaldoIngreso(cajaDestino, importe, true);

            // Creo el Movimiento Destino
            MovimientoCaja movimientoDestino = new MovimientoCaja();

            // Asigno los Datos
            movimientoDestino.CajaUsuario = cajaDestino;
            movimientoDestino.CajaUsuarioID = cajaDestino.Id;
            movimientoDestino.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Transferencia;
            movimientoDestino.ImporteIngreso = importe;
            movimientoDestino.ImporteEgreso = 0;
            movimientoDestino.Descripcion = "Transferencia Manual";

            // Creo el Item
            MovimientoCajaItem itemDestino = new MovimientoCajaItem();

            itemDestino.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Transferencia;
            itemDestino.ImporteIngreso = importe;
            itemDestino.ImporteEgreso = 0;
            itemDestino.CajaImpacto = cajaOrigen.Caja;
            itemDestino.CajaImpactoID = cajaOrigen.Caja.Id;
            itemDestino.Descripcion = "Transferencia Manual desde Caja " + cajaOrigen.NameCaja;

            // Inserto el Movimiento con el Item
            this.MovimientoCajaInsert(movimientoDestino, itemDestino, true);
        }

        /// <summary>
        /// Realizo un Ajuste de una Caja Asignada
        /// </summary>
        /// <param name="caja">Caja a Ajustar</param>
        /// <param name="importe">Importe del Ajuste</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        [RequiresTransaction]
        public virtual void CajaUsuarioAjustar(CajaUsuario caja, decimal importe)
        {
            // Creo el Movimiento
            MovimientoCaja movimiento = new MovimientoCaja();

            // Asigno los Datos
            movimiento.CajaUsuario = caja;
            movimiento.CajaUsuarioID = caja.Id;
            movimiento.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Ajuste;
            movimiento.ImporteIngreso = importe;
            movimiento.ImporteEgreso = 0;
            movimiento.Descripcion = "Ajuste Manual de Caja";

            // Creo el Item
            MovimientoCajaItem item = new MovimientoCajaItem();

            item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Ajuste;
            item.ImporteIngreso = importe;
            item.ImporteEgreso = 0;
            item.CajaImpactoID = null;
            item.Descripcion = "Ajuste Manual de Caja";

            // Inserto el Movimiento con el Item
            this.MovimientoCajaInsert(movimiento, item, true);
        }

        /// <summary>
        /// Obtengo el Saldo de una Caja
        /// </summary>
        /// <param name="id">Id de la Asignación</param>
        /// <returns>Saldo Actual</returns>
        public decimal CajaUsuarioObtenerSaldo(int id)
        {
            CajaUsuario caja = dalEngine.GetById<CajaUsuario>(id);
            if (caja == null)
                throw new Exception("CajaUsuario no encontrado.");
            else
                return caja.SaldoEfectivo;
        }

        public void CajaUsuarioObtenerSaldo(CajaUsuario caja)
        {
            enfoke.Context.Data.Session.Evict(caja);
            CajaUsuario _caja = dalEngine.GetById<CajaUsuario>(caja.Id);

            if (_caja == null)
                throw new Exception("CajaUsuario no encontrado.");
            else
            {
                caja.SaldoEfectivo = _caja.SaldoEfectivo;
                caja.SaldoNoEfectivo = _caja.SaldoNoEfectivo;
            }
        }

        /// <summary>
        /// Chequeo el Saldo al Egresar un Importe (Importe no Mayor)
        /// </summary>
        /// <param name="caja">CajaUsuario de Egreso del Importe</param>
        /// <param name="importe">Importe a Sacar</param>
        /// <param name="actualizarSaldo">Marca si debo Actualizar el Saldo de la Caja</param>
        public void CajaUsuarioChequearSaldoEgreso(CajaUsuario caja, decimal importe, bool actualizarSaldo)
        {
            // Actualizo el Saldo de la Caja
            if (actualizarSaldo)
            {
                try
                {
                    this.CajaUsuarioObtenerSaldo(caja);
                }
                catch (Exception ex)
                {
                    throw new Exception("Error al Actualizar el Saldo de la Caja de Origen.", ex);
                }
            }

            // Chequeo que la Caja tenga Saldo
            if (importe > caja.SaldoEfectivo)
                throw new NotLoggeableException("La Caja " + caja.NameCaja + " No tiene Saldo Suficiente.");
        }

        /// <summary>
        /// Chequeo el Saldo al Ingresar un Importe (no Negativo)
        /// </summary>
        /// <param name="caja">CajaUsuario de Ingreso de Importe</param>
        /// <param name="importe">Importe a Impactar</param>
        /// <param name="actualizarSaldo">Marca si debo Actualizar el Saldo de la Caja</param>
        public void CajaUsuarioChequearSaldoIngreso(CajaUsuario caja, decimal importe, bool actualizarSaldo)
        {
            // Actualizo el Saldo de la Caja
            if (actualizarSaldo)
            {
                try
                {
                    this.CajaUsuarioObtenerSaldo(caja);
                }
                catch (Exception ex)
                {
                    throw new Exception("Error al Actualizar el Saldo de la Caja de Origen.", ex);
                }
            }

            // Chequeo que la Caja quede con Saldo
            if ((caja.SaldoEfectivo + importe) < 0)
                throw new NotLoggeableException("La Caja " + caja.NameCaja + " No quedaría con Saldo Suficiente.");
        }

        #endregion

        #region CajaUsuarioHabilitado

        public EntityCollection<SecurityUser> CajaUsuarioReadByCajaUsuario(int cajaId)
        {
            String hql = "select distinct  su from CajaUsuarioHabilitado cu, SecurityUser su " +
                         "where cu.UsuarioId = su.Id " +
                         "and cu.CajaId = :cajaId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaId", cajaId);
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        /// <summary>
        /// [PC] Retorno todos los  SecurityUser pertenecientes a una caja usuario habilitado
        /// </summary>
        /// <returns>Colección de SecurityUser</returns>
        public EntityCollection<SecurityUser> CajaUsuarioHabilitadoReadAllSecurityUser()
        {
            // [PC]
            //***************************//
            //  preguntar este metodo    //
            //  en donde se puede ubicar //
            //***************************//

            String hql = "select distinct su " +
                         "from SecurityUser su, CajaUsuarioHabilitado cuh " +
                         "where su.Id = cuh.UsuarioId " +
                         "and cuh.Deleted = false " +
                         "order by su.LastName asc, su.Name asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        /// <summary>
        /// [PC] Retorno todas las  CajaUsuarioHabilitado para un usuario
        /// </summary>
        /// <param name="usuarioId">ID del usuario</param>
        /// <returns>Todas las  CajaUsuarioHabilitado para un usuario</returns>
        public EntityCollection<CajaUsuarioHabilitado> CajaUsuarioHabilitadoReadByUsuario(int usuarioId)
        {
            Filter filterCajaUsuarioHabilitado = new Filter();
            filterCajaUsuarioHabilitado.Add(CajaUsuarioHabilitado.Properties.UsuarioId, "=", usuarioId);
            filterCajaUsuarioHabilitado.Add(BooleanOp.And, CajaUsuarioHabilitado.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<CajaUsuarioHabilitado>(filterCajaUsuarioHabilitado);
        }

        public bool CajaUsuarioHabilitadoReadCheckExisteByUsuario(int usuarioId)
        {
            string hql = "select cuh.Id " +
                         "from CajaUsuarioHabilitado cuh " +
                         "where cuh.UsuarioId = :usuarioId " +
                         "and cuh.Deleted = false";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("usuarioId", usuarioId);
            query.SetMaxResults(1);
            Object retObj = query.UniqueResult<Object>();
            if (retObj != null)
                return true;
            else
                return false;
        }

        /// <summary>
        /// [PC] Retorno todas las  CajaUsuarioHabilitado para una caja
        /// </summary>
        /// <param name="CajaId">ID de la Caja</param>
        /// <returns>Todas las  CajaUsuarioHabilitado para una Caja</returns>
        public EntityCollection<CajaUsuarioHabilitado> CajaUsuarioHabilitadoReadByCaja(int cajaId)
        {
            Filter filterCajaUsuarioHabilitado = new Filter();
            filterCajaUsuarioHabilitado.Add(CajaUsuarioHabilitado.Properties.CajaId, "=", cajaId);
            filterCajaUsuarioHabilitado.Add(BooleanOp.And, CajaUsuarioHabilitado.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<CajaUsuarioHabilitado>(filterCajaUsuarioHabilitado);
        }

        /// <summary>
        /// [PC] Elimina Colección de CajaUsuarioHabilitado
        /// </summary>
        /// <param name="cajasUsuarioHabilitado">Colección de CajaUsuarioHabilitado</param>
        public void CajaUsuarioHabilitadoDelete(EntityCollection<CajaUsuarioHabilitado> cajasUsuarioHabilitado)
        {
            if (cajasUsuarioHabilitado != null)
                foreach (CajaUsuarioHabilitado cuh in cajasUsuarioHabilitado)
                    cuh.Deleted = true;

            this.CajaUsuarioHabilitadoCreate(cajasUsuarioHabilitado);
        }

        /// <summary>
        /// [PC] Inserta colección de CajaUsuarioHabilitado
        /// </summary>
        /// <param name="cajasUsuarioHabilitado">Colección de CajaUsuarioHabilitado</param>
        public void CajaUsuarioHabilitadoCreate(EntityCollection<CajaUsuarioHabilitado> cajasUsuarioHabilitado)
        {
            dalEngine.UpdateCollection<CajaUsuarioHabilitado>(cajasUsuarioHabilitado);
        }

        #endregion

        #region MovimientoMedioPago

        public EntityCollection<MovimientoMedioPagoView> MovimientoMedioPagoReadByCajaUsuario(CajaUsuario cajaUsuario)
        {
            //IQuery query = dalEngine.CreateQuery("FROM MovimientoMedioPagoView mmp WHERE mmp.CajaUsuarioId = :idCajaUsuario ");
            //query.SetParameter("idCajaUsuario", cajaUsuario.Id);

            //return dalEngine.GetManyByQuery<MovimientoMedioPagoView>(query);

            ReadManyCommand<MovimientoMedioPagoView> readCmd = new ReadManyCommand<MovimientoMedioPagoView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(
                MovimientoMedioPagoView.Properties.CajaUsuarioId,
                "=",
                cajaUsuario.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();

            //return dalEngine.GetManyByProperty<MovimientoMedioPagoView>(MovimientoMedioPagoView.Properties.CajaUsuarioId, cajaUsuario.Id);
        }

        #endregion

        #region MovimientoDetalleTarjeta

        public EntityCollection<MovimientoDetalleTarjetaView> MovimientoDetalleTarjetaReadByCajaUsuario(
            CajaUsuario cajaUsuario)
        {
            //IQuery query = dalEngine.CreateQuery("FROM MovimientoDetalleTarjetaView mdt WHERE mdt.CajaUsuarioId = :idCajaUsuario ");
            //query.SetParameter("idCajaUsuario", cajaUsuario.Id);

            //return dalEngine.GetManyByQuery<MovimientoDetalleTarjetaView>(query);

            ReadManyCommand<MovimientoDetalleTarjetaView> readCmd =
                new ReadManyCommand<MovimientoDetalleTarjetaView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(
                MovimientoDetalleTarjetaView.Properties.CajaUsuarioId,
                " = ",
                cajaUsuario.Id);
            readCmd.Filter = filter;

            return readCmd.Execute();

            //return dalEngine.GetManyByProperty<MovimientoDetalleTarjetaView>(MovimientoDetalleTarjetaView.Properties.CajaUsuarioId, cajaUsuario.Id);           
        }

        #endregion

        #region MovimientoCajaCierre

        /// <summary>
        /// Retorno una Colección de Pares Caja-Saldo de Movimientos de Cierre para una Asignación
        /// </summary>
        /// <param name="id">ID de la CajaUsuario Cerrandose</param>
        /// <returns>Colección de Items</returns>
        public EntityCollection<MovimientoCajaCierreView> MovimientoCajaCierreReadByCajaUsuario(int cajaUsuarioID)
        {
            ReadManyCommand<MovimientoCajaCierreView> readCmd = new ReadManyCommand<MovimientoCajaCierreView>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, MovimientoCajaCierreView.Properties.CajaUsuarioID,
                       "=", cajaUsuarioID);

            readCmd.Filter = filter;

            EntityCollection<MovimientoCajaCierreView> movimientos = readCmd.Execute();

            // Obtengo los Objetos según los IDs                        
            foreach (MovimientoCajaCierreView mcc in movimientos)
                ObtenerObjetos(mcc);

            return movimientos;
        }

        private void ObtenerObjetos(MovimientoCajaCierreView mcc)
        {
            if (mcc.CajaImpactoID.HasValue)
            {
                mcc.CajaImpacto = dalEngine.GetById<Caja>(mcc.CajaImpactoID.Value);
                mcc.CajaUsuarioImpacto = this.CajaUsuarioReadByCaja(mcc.CajaImpactoID.Value);
            }
        }

        #endregion

        #region TipoMovimientoCaja

        /// <summary>
        /// Retorno un TipoMovimientoCaja para un ID
        /// </summary>
        /// <param name="id">ID del TipoMovimientoCaja</param>
        /// <returns>El TipoMovimientoCaja correspondiente</returns>

        #endregion

        #region TipoMovimientoCajaItem











        #endregion

        #region MovimientoCajaForList

        public EntityCollection<PagoMovimientoCajaForList> PagoMovimientoCajaForListReadByCajaUsuarioId(
            int cajaUsuarioId)
        {
            String hql = "from PagoMovimientoCajaForList pmc " +
                         "where pmc.MovimientoCajaForList.CajaUsuarioID = :cajaUsuarioId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaUsuarioId", cajaUsuarioId);
            return dalEngine.GetManyByQuery<PagoMovimientoCajaForList>(query);
        }

        public EntityCollection<MovimientoCajaForList> MovimientoCajaForListReadByCajaUsuarioId(int cajaUsuarioId)
        {
            EntityCollection<MovimientoCajaForList> mov = new EntityCollection<MovimientoCajaForList>();
            String hql = "from MovimientoCajaForList mc " +
                         "where mc.CajaUsuarioID = :cajaUsuarioId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaUsuarioId", cajaUsuarioId);
            mov = dalEngine.GetManyByQuery<MovimientoCajaForList>(query);

            EntityCollection<PagoMovimientoCajaForList> pagosMovCaja =
                this.PagoMovimientoCajaForListReadByCajaUsuarioId(cajaUsuarioId);

            // Si existen pagos para el movimiento
            if (pagosMovCaja != null)
            {
                foreach (PagoMovimientoCajaForList pagMovCaja in pagosMovCaja)
                {
                    int index = mov.IndexOf(pagMovCaja.MovimientoCajaForList);
                    if (index > 0)
                    {
                        mov[index].Pagos.Add(pagMovCaja.PagoForList);
                    }
                }
            }

            return mov;
        }

        #endregion

        #region DatosMovimientoCaja

        private void CompletarProtocolo(EntityCollection<ReporteMovimientoCaja> movimientos)
        {
            List<int> turnos = new List<int>();
            foreach (ReporteMovimientoCaja mov in movimientos)
                if (mov.FormularioTurnoId.HasValue)
                    turnos.Add(mov.FormularioTurnoId.Value);

            EntityCollection<Turno> turnosProtocolo = Context.Session.TurnosDalc.TurnoProtocoloReadByTurnos(turnos);
            foreach (Turno tur in turnosProtocolo)
            {
                IEnumerable<ReporteMovimientoCaja> repMovimientos = movimientos.FindAll(delegate(ReporteMovimientoCaja movimiento) { return movimiento.FormularioTurnoId.GetValueOrDefault(0) == tur.Id; });
                foreach (ReporteMovimientoCaja rep in repMovimientos)
                    rep.Protocolo = tur.Protocolo;
            }
        }

        private void CompletarPlanes(EntityCollection<ReporteMovimientoCaja> movimientos)
        {
            List<int> turnos = new List<int>();
            foreach (ReporteMovimientoCaja mov in movimientos)
                if (mov.FormularioTurnoId.HasValue)
                    turnos.Add(mov.FormularioTurnoId.Value);

            EntityCollection<Turno> turnosProtocolo = Context.Session.TurnosDalc.TurnoPlanNameByTurnos(turnos);
            foreach (Turno tur in turnosProtocolo)
            {
                IEnumerable<ReporteMovimientoCaja> repMovimientos = movimientos.FindAll(delegate(ReporteMovimientoCaja movimiento) { return movimiento.FormularioTurnoId.GetValueOrDefault(0) == tur.Id; });
                foreach (ReporteMovimientoCaja rep in repMovimientos)
                {
                    rep.Plan = tur.PlanName;
                    rep.ObraSocial = tur.ObraSocialName;
                }
            }
        }

        private void CompletarEntidadPago(EntityCollection<ReporteMovimientoCaja> movimientos)
        {
            EntityCollection<EntidadPago> entidadesPago = EntidadPagoReadAll();
            foreach (EntidadPago entidad in entidadesPago)
            {
                IEnumerable<ReporteMovimientoCaja> repMovimientos = movimientos.FindAll(delegate(ReporteMovimientoCaja movimiento) { return movimiento.EntidadPagoId.GetValueOrDefault(0) == entidad.Id; });
                foreach (ReporteMovimientoCaja rep in repMovimientos)
                    rep.EntidadPagoDescripcion = entidad.Name;
            }
        }

        public EntityCollection<ReporteMovimientoCaja> ReporteMovimientoCajaReadByParameters(DateTime? fechaDesde,
                                                                                                         DateTime? fechaHasta,
                                                                                                         int? usuarioId,
                                                                                                         int? cajaId,
                                                                                                         int? sucursalId,
                                                                                                         bool cajaAbierta,
                                                                                                         bool cajaCerrada,
                                                                                                         int? cajaUsuario,
                                                                                                         DatoReporteMovimientoCaja.TipoTotalizador tipoTotalizador,
                                                                                                         DatoReporteMovimientoCaja.TipoReporte tipoReporte)
        {

            List<int> filtroMovimientos = new List<int>();
            filtroMovimientos.Add((int)TipoMovimientoCajaEnum.Anulacion);

            if (tipoReporte != DatoReporteMovimientoCaja.TipoReporte.DetalleMovimiento || tipoTotalizador != DatoReporteMovimientoCaja.TipoTotalizador.Caja)
            {
                filtroMovimientos.Add((int)TipoMovimientoCajaEnum.Apertura);
                filtroMovimientos.Add((int)TipoMovimientoCajaEnum.Cierre);
                filtroMovimientos.Add((int)TipoMovimientoCajaEnum.Ajuste);
                filtroMovimientos.Add((int)TipoMovimientoCajaEnum.Transferencia);
            }

            EntityCollection<ReporteMovimientoCaja> result = new EntityCollection<ReporteMovimientoCaja>();
            EntityCollection<ReporteMovimientoCaja> itemsSinFormulario = ReporteMovimientoCajaReadByParametersSinFormulario(fechaDesde, fechaHasta, usuarioId, cajaId, sucursalId, cajaAbierta, cajaCerrada, cajaUsuario, filtroMovimientos);
            EntityCollection<ReporteMovimientoCaja> itemsConFormulario = ReporteMovimientoCajaReadByParametersConFormulario(fechaDesde, fechaHasta, usuarioId, cajaId, sucursalId, cajaAbierta, cajaCerrada, cajaUsuario, filtroMovimientos);

            result.AddRange(itemsSinFormulario);
            result.AddRange(itemsConFormulario);

            if (result.Count == 0)
                return new EntityCollection<ReporteMovimientoCaja>();

            CompletarProtocolo(result);
            CompletarEntidadPago(result);
            CompletarPlanes(result);

            return new DatoReporteMovimientoCaja(tipoTotalizador, tipoReporte, result).MovimientosCaja;
        }

        private EntityCollection<ReporteMovimientoCaja> ReporteMovimientoCajaReadByParametersSinFormulario(DateTime? fechaDesde,
                                                                                                         DateTime? fechaHasta,
                                                                                                         int? usuarioId,
                                                                                                         int? cajaId,
                                                                                                         int? sucursalId,
                                                                                                         bool cajaAbierta,
                                                                                                         bool cajaCerrada,
                                                                                                         int? cajaUsuario,
                                                                                                         List<int> filtroEstados)
        {

            StringBuilder hql = new StringBuilder();
            hql.Append(" select distinct new  enfoke.Eges.Entities.Results.ReporteMovimientoCaja( ");
            hql.Append(" moi.MovimientoCaja.Id ");
            hql.Append(" , moi.MovimientoCaja.Descripcion ");
            hql.Append(" , moi.MovimientoCaja.Fecha ");
            hql.Append(" , moi.MovimientoCaja.ImporteIngreso ");
            hql.Append(" , moi.MovimientoCaja.ImporteEgreso ");
            hql.Append(" , cau.Caja.Sucursal.Id ");
            hql.Append(" , cau.Caja.Id ");
            hql.Append(" , cau.Caja.Name ");
            hql.Append(" , cau.Caja.Sucursal.Name) ");
            hql.Append(" from MovimientoCajaItem as moi, CajaUsuario as cau, CajaPuntoVenta as cpv ");
            hql.Append(" where moi.MovimientoCaja.CajaUsuarioID = cau.Id ");
            hql.Append(" and cau.Caja.Id = cpv.CajaId ");
            hql.Append(" and moi.MovimientoCaja.TipoMovimientoCajaID not in (:filtroEstados) ");
            hql.Append(" and moi.FormularioID is null ");

            if (cajaUsuario.HasValue)
                hql.Append(" and cau.Id = :cajaUsuario ");

            if (cajaAbierta && !cajaCerrada)
                hql.Append(" and cau.FechaCierre is null ");

            if (!cajaAbierta && cajaCerrada)
                hql.Append(" and cau.FechaCierre is not null ");

            if (usuarioId.HasValue)
                hql.Append(" and cau.Usuario.Id = :usuarioId ");

            if (cajaId.HasValue)
                hql.Append(" and cau.Caja.Id  = :cajaId ");

            if (sucursalId.HasValue)
                hql.Append("and cau.Caja.Sucursal.Id  = :sucursalId ");

            if (fechaDesde.HasValue)
                hql.Append(" and moi.MovimientoCaja.Fecha >= :fechaDesde ");

            if (fechaHasta.HasValue)
                hql.Append(" and moi.MovimientoCaja.Fecha < :fechaHasta ");

            hql.Append(" order by moi.MovimientoCaja.Fecha desc ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value.Date.AddDays(1));

            query.SetParameterList("filtroEstados", filtroEstados);

            if (usuarioId.HasValue)
                query.SetParameter("usuarioId", usuarioId.Value);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);

            if (cajaUsuario.HasValue)
                query.SetParameter("cajaUsuario", cajaUsuario.Value);

            return dalEngine.GetManyByQuery<ReporteMovimientoCaja>(query);
        }


        private EntityCollection<ReporteMovimientoCaja> ReporteMovimientoCajaReadByParametersConFormulario(DateTime? fechaDesde,
                                                                                                       DateTime? fechaHasta,
                                                                                                       int? usuarioId,
                                                                                                       int? cajaId,
                                                                                                       int? sucursalId,
                                                                                                       bool cajaAbierta,
                                                                                                       bool cajaCerrada,
                                                                                                       int? cajaUsuario,
                                                                                                      List<int> filtroEstados)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select distinct new  enfoke.Eges.Entities.Results.ReporteMovimientoCaja( ");
            hql.Append(" moi.MovimientoCaja.Id ");
            hql.Append(" , moi.MovimientoCaja.Descripcion ");
            hql.Append(" , moi.MovimientoCaja.Fecha ");
            hql.Append(" , moi.MovimientoCaja.ImporteIngreso ");
            hql.Append(" , moi.MovimientoCaja.ImporteEgreso ");
            hql.Append(" , cau.Caja.Sucursal.Id ");
            hql.Append(" , cau.Caja.Id ");
            hql.Append(" , cau.Caja.Name ");
            hql.Append(" , cau.Caja.Sucursal.Name ");
            hql.Append(" , frm.Empresa.RazonSocial ");
            hql.Append(" , frm.Id ");
            hql.Append(" , tfr.Abreviatura ");
            hql.Append(" , frm.Clase ");
            hql.Append(" , frm.Sucursal ");
            hql.Append(" , frm.Numero ");
            hql.Append(" , frm.RazonSocial ");
            hql.Append(" , frm.FechaAnulacion ");
            hql.Append(" , frm.TurnoID ");
            hql.Append(" , pag.Importe ");
            hql.Append(" , med.Name ");
            hql.Append(" , pag.LoteTarjeta ");
            hql.Append(" , pag.EntidadPagoID ");
            hql.Append(" ) ");
            hql.Append(" from MovimientoCajaItem as moi, CajaUsuario as cau, CajaPuntoVenta as cpv, Formulario frm, TipoFormulario tfr ");
            hql.Append(" , PagoMovimientoCaja pmc, Pago pag, MedioPago med ");
            hql.Append(" where moi.MovimientoCaja.CajaUsuarioID = cau.Id ");
            hql.Append(" and moi.MovimientoCaja.Id = pmc.MovimientoCaja.Id ");
            hql.Append(" and pmc.Pago.Id = pag.Id ");
            hql.Append(" and pag.MedioPagoID = med.Id ");
            hql.Append(" and moi.FormularioID = frm.Id ");
            hql.Append(" and frm.TipoFormularioID = tfr.Id ");
            hql.Append(" and cau.Caja.Id = cpv.CajaId ");
            hql.Append(" and moi.MovimientoCaja.TipoMovimientoCajaID not in (:filtroEstados) ");

            if (cajaUsuario.HasValue)
                hql.Append(" and cau.Id = :cajaUsuario ");

            if (cajaAbierta && !cajaCerrada)
                hql.Append(" and cau.FechaCierre is null ");

            if (!cajaAbierta && cajaCerrada)
                hql.Append(" and cau.FechaCierre is not null ");

            if (usuarioId.HasValue)
                hql.Append("and cau.Usuario.Id = :usuarioId ");

            if (cajaId.HasValue)
                hql.Append("and cau.Caja.Id  = :cajaId ");

            if (sucursalId.HasValue)
                hql.Append("and cau.Caja.Sucursal.Id  = :sucursalId ");

            if (fechaDesde.HasValue)
                hql.Append(" and moi.MovimientoCaja.Fecha >= :fechaDesde ");

            if (fechaHasta.HasValue)
                hql.Append(" and moi.MovimientoCaja.Fecha < :fechaHasta ");

            hql.Append(" order by moi.MovimientoCaja.Fecha desc ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value.Date.AddDays(1));

            query.SetParameterList("filtroEstados", filtroEstados);

            if (usuarioId.HasValue)
                query.SetParameter("usuarioId", usuarioId.Value);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);

            if (cajaUsuario.HasValue)
                query.SetParameter("cajaUsuario", cajaUsuario.Value);

            return dalEngine.GetManyByQuery<ReporteMovimientoCaja>(query);
        }


        private static void AddProtocoloInReportMovimientoCaja(EntityCollection<DatosReportMovimientoCaja> datos, List<int> turnoIds)
        {
            if (turnoIds.Count > 0)
            {
                List<int> osps = new List<int>();
                EntityCollection<Turno> turnos = Context.Session.TurnosDalc.TurnosReadByIds(turnoIds);
                foreach (Turno tur in turnos)
                    if (!osps.Contains(tur.Orden.ObraSocialPlanId))
                        osps.Add(tur.Orden.ObraSocialPlanId);

                EntityCollection<ObraSocialPlan> planes = Context.Session.ObrasSocialesDalc.ObraSocialPlanReadByIds(osps);
                foreach (DatosReportMovimientoCaja item in datos)
                {
                    if (item.Formulario != null)
                        if (item.Formulario.TurnoId.HasValue)
                        {
                            Turno turno = turnos.FindByKey(item.Formulario.TurnoId.Value);
                            if (turno != null)
                            {
                                ObraSocialPlan osp = planes.FindByKey(turno.Orden.ObraSocialPlanId);
                                item.ObraSocial = osp.ObraSocial.Name;
                                item.Protocolo = turno.Orden.Protocolo.ProtocoloFull;
                            }
                        }
                }
            }
        }

        public EntityCollection<DatosReportMovimientoCaja> DatosReportMovimientoCajaReadByParameters(DateTime fechaDesde,
                                                                                                         DateTime fechaHasta,
                                                                                                         int? usuarioId,
                                                                                                         int? cajaId,
                                                                                                         int? sucursalId,
                                                                                                         bool cajaAbierta,
                                                                                                         bool cajaCerrada,
                                                                                                         bool cierreConSaldo)
        {
            EntityCollection<DatosReportMovimientoCaja> datos = new EntityCollection<DatosReportMovimientoCaja>();
            String hql =
                "select distinct new  enfoke.Eges.Entities.Results.DatosReportMovimientoCaja(m.MovimientoCajaHQL.Id, " +
                "m.MovimientoCajaHQL.CajaUsuario.Id , " +
                "m.MovimientoCajaHQL.Descripcion , " +
                "m.MovimientoCajaHQL.Fecha , " +
                "m.MovimientoCajaHQL.FechaAnulacion , " +
                "m.ImporteEgreso , " +
                "m.ImporteIngreso , " +
                "m.MovimientoCajaHQL.TurnoHQL.Id , " +
                "m.MovimientoCajaHQL.TipoMovimientoCaja.Id , " +
                "m.MovimientoCajaHQL.CajaUsuario.Caja.Sucursal.Id, " +
                "m.MovimientoCajaHQL.CajaUsuario.Caja.Id, " +
                "m.MovimientoCajaHQL.CajaUsuario.Caja.Name, " +
                "m.MovimientoCajaHQL.CajaUsuario.Caja.Sucursal.Name, " +
                "m.TipoMovimientoCajaItem, " +
                "f " +
                ", pv.Empresa.Id, " +
                "pv.Empresa.RazonSocial) " +
                "from MovimientoCajaItemHQL m LEFT JOIN m.Formulario f " +
                ", CajaPuntoVenta cpv join cpv.PuntoVenta pv " +
                "where m.MovimientoCajaHQL.Fecha >= :fechaDesde " +
                "and m.MovimientoCajaHQL.Fecha < :fechaHasta " +
                "and (m.ImporteEgreso <> 0 or m.ImporteIngreso <> 0) " +
                "and  m.MovimientoCajaHQL.CajaUsuario.Caja.Id = cpv.CajaId " +
                "and (m.TipoMovimientoCajaItem.Id != :PercepcionIIBB  and m.TipoMovimientoCajaItem.Id != :DevolucionIIBB ) ";

            if (cajaCerrada && !cajaAbierta)
                hql += "and  m.MovimientoCajaHQL.CajaUsuario.FechaCierre is not null ";
            else if (!cajaCerrada && cajaAbierta)
                hql += "and  m.MovimientoCajaHQL.CajaUsuario.FechaCierre is null ";

            if (cierreConSaldo)
                hql += "and (m.TipoMovimientoCajaItem.Id != :movimientoApertura and m.TipoMovimientoCajaItem.Id != :movimientoCierre) ";

            //"and  (m.Formulario is null or m.Formulario.Numero is not null) ";

            if (usuarioId.HasValue)
                hql += "and m.MovimientoCajaHQL.CajaUsuario.Usuario.Id = :usuarioId ";

            if (cajaId.HasValue)
                hql += "and m.MovimientoCajaHQL.CajaUsuario.Caja.Id  = :cajaId ";

            if (sucursalId.HasValue)
                hql += "and m.MovimientoCajaHQL.CajaUsuario.Caja.Sucursal.Id  = :sucursalId ";

            hql += "order by m.MovimientoCajaHQL.Id asc ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (usuarioId.HasValue)
                query.SetParameter("usuarioId", usuarioId.Value);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);


            if (cierreConSaldo)
            {
                query.SetParameter("movimientoApertura", (int)TipoMovimientoCajaItemEnum.Apertura);
                query.SetParameter("movimientoCierre", (int)TipoMovimientoCajaItemEnum.TransferenciaCierre);
            }

            query.SetParameter("DevolucionIIBB", (int)TipoMovimientoCajaItemEnum.DevolucionIIBB);
            query.SetParameter("PercepcionIIBB", (int)TipoMovimientoCajaItemEnum.PercepcionIIBB);
            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1));

            datos = dalEngine.GetManyByQuery<DatosReportMovimientoCaja>(query);
            EntityCollection<DatosReportMovimientoCaja> _datosAux = new EntityCollection<DatosReportMovimientoCaja>();
            List<string> frmIds = new List<string>();
            List<int> turnoIds = new List<int>();
            List<int> movIds = new List<int>();
            if (datos != null && datos.Count > 0)
            {
                EntityCollection<PagoMovimientoCajaForList> pagosMovCaja = this.PagoMovimientoCajaForListReadByParameters(datos);

                foreach (DatosReportMovimientoCaja item in datos)
                {
                    string claveFormulario = string.Empty;
                    if (item.Formulario != null)
                        claveFormulario = item.Formulario.Id.ToString() + (item.MovimientoCajaForList.TipoMovimientoId.ToString());

                    if (!movIds.Contains(item.MovimientoCajaForList.Id) && (item.Formulario == null || !frmIds.Contains(claveFormulario)))
                    {
                        if (item.Formulario != null && item.Formulario.TurnoId.HasValue)
                            turnoIds.Add(item.Formulario.TurnoId.Value);

                        movIds.Add(item.MovimientoCajaForList.Id);

                        item.MovimientoCajaForList.Pagos = new List<PagoForList>();
                        foreach (PagoMovimientoCajaForList pag in pagosMovCaja)
                        {
                            if (item.MovimientoCajaForList.Id == pag.MovimientoCajaForList.Id)
                                item.MovimientoCajaForList.Pagos.Add(pag.PagoForList);
                        }

                        _datosAux.Add(item);
                    }

                    if (item.Formulario != null)
                        frmIds.Add(claveFormulario);
                }

                AddProtocoloInReportMovimientoCaja(datos, turnoIds);
            }

            return _datosAux;
        }

        public EntityCollection<DatosReportMovimientoCajaItem> DatosReportMovimientoCajaItemReadByParameters(
            DateTime fecha, int? usuarioId, int? cajaId, int? sucursalId)
        {
            String hql =
                "select new enfoke.Eges.Entities.Results.DatosReportMovimientoCajaItem(mi.Descripcion, mi.ImporteIngreso, mi.ImporteEgreso, mc, cu.Caja.Sucursal.Id, cu.Caja.Id, cu.Caja.Name, cu.Caja.Sucursal.Name, pv.Empresa.Id, pv.Empresa.RazonSocial) " +
                "from MovimientoCajaItem mi, MovimientoCajaForList mc, CajaUsuario cu, CajaPuntoVenta cpv join cpv.PuntoVenta pv " +
                "where mi.MovimientoCaja.Id = mc.Id " +
                "and   cu.Caja.Id = cpv.CajaId " +
                "and   mc.CajaUsuarioID = cu.Id " +
                "and   cu.FechaAlta < :fechaAlta " +
                "and   cu.FechaCierre >= :fecha ";

            if (usuarioId.HasValue)
                hql += "and cu.Usuario.Id = :usuarioId ";

            if (cajaId.HasValue)
                hql += "and cu.Caja.Id  = :cajaId ";

            if (sucursalId.HasValue)
                hql += "and cu.Caja.Sucursal.Id  = :sucursalId ";

            hql += "order by mi.Id asc ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (usuarioId.HasValue)
                query.SetParameter("usuarioId", usuarioId.Value);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);

            query.SetParameter("fechaAlta", fecha.AddDays(1));
            query.SetParameter("fecha", fecha);

            return dalEngine.GetManyByQuery<DatosReportMovimientoCajaItem>(query);
        }

        #endregion

        #region PagoMovimientoCajaForList


        private EntityCollection<PagoMovimientoCajaForList> PagoMovimientoCajaForListReadByParameters(EntityCollection<DatosReportMovimientoCaja> datos)
        {
            List<int> movCajaIds = new List<int>();
            string movIds = string.Empty;
            foreach (DatosReportMovimientoCaja _dato in datos)
            {
                movCajaIds.Add(_dato.MovimientoCajaForList.Id);
                movIds += _dato.MovimientoCajaForList.Id.ToString() + ",";
            }

            String hql = "select pmc " +
                        "from PagoMovimientoCajaForList pmc, CajaUsuario cu " +
                        "where pmc.MovimientoCajaForList.CajaUsuarioID = cu.Id ";

            if (movCajaIds.Count > 0)
                hql += " and pmc.MovimientoCajaForList.Id in (:movCajaIds) ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (movCajaIds.Count > 0)
                query.SetParameterList("movCajaIds", movCajaIds);



            return dalEngine.GetManyByQuery<PagoMovimientoCajaForList>(query);
        }



        public EntityCollection<PagoMovimientoCajaForList> PagoMovimientoCajaForListReadByParameters(DateTime fecha,
                                                                                                    int? usuarioId,
                                                                                                    int? cajaId,
                                                                                                    int? sucursalId,
                                                                                                    bool cajaAbierta,
                                                                                                    bool cajaCerrada)
        {
            String hql = "select pmc " +
                         "from PagoMovimientoCajaForList pmc, CajaUsuario cu " +
                         "where pmc.MovimientoCajaForList.CajaUsuarioID = cu.Id " +
                         "and cu.FechaAlta < :fechaAlta " +
                         "and cu.FechaAlta >= :fecha ";

            if (cajaAbierta != cajaCerrada)
            {
                if (cajaCerrada)
                    hql += "and cu.FechaCierre is not null ";
                else if (cajaAbierta)
                    hql += "and cu.FechaCierre is null ";
            }

            if (usuarioId.HasValue)
                hql += "and cu.Usuario.Id = :usuarioId ";

            if (cajaId.HasValue)
                hql += "and cu.Caja.Id  = :cajaId ";

            if (sucursalId.HasValue)
                hql += "and cu.Caja.Sucursal.Id  = :sucursalId ";

            IQuery query = dalEngine.CreateQuery(hql);

            if (usuarioId.HasValue)
                query.SetParameter("usuarioId", usuarioId.Value);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);

            query.SetParameter("fechaAlta", fecha.AddDays(1));
            query.SetParameter("fecha", fecha);

            return dalEngine.GetManyByQuery<PagoMovimientoCajaForList>(query);
        }


        #endregion

        #region MovimientoCaja

        /// <summary>
        /// [PC] Crea movimiento sin persistirlo
        /// </summary>
        /// <param name="importe">Importe del movimiento</param>
        /// <param name="caja">Caja de Impacto del Item</param>
        /// <param name="int">Tipo del movimiento de caja</param>
        /// <param name="descripcion">Descripción del movimiento</param>
        /// <returns>MovimientoCaja</returns>
        private MovimientoCaja MovimientoCajaCreate(Decimal importe, CajaUsuario caja, int tipoMovimiento,
                                                    String descripcion)
        {
            // Creo el Movimiento
            MovimientoCaja movimiento = new MovimientoCaja();

            // Asigno los Datos
            movimiento.CajaUsuario = caja;
            movimiento.CajaUsuarioID = caja.Id;
            movimiento.TipoMovimientoCajaID = tipoMovimiento;

            if (importe > 0)
            {
                movimiento.ImporteIngreso = 0;
                movimiento.ImporteEgreso = importe;
            }
            else
            {
                movimiento.ImporteIngreso = importe * -1;
                movimiento.ImporteEgreso = 0;
            }

            movimiento.Descripcion = descripcion;

            return movimiento;
        }






        /// <summary>
        /// Retorno los Movimientos que Apliquen a los Parametros
        /// </summary>
        /// <param name="cajaUsuario">Asignación de la Cabecera</param>
        /// <param name="fechaDesde">Fecha Desde de Cierre de la Asignación de la Cabecera</param>
        /// <param name="fechaHasta">Fecha Hasta de Cierre de la Asignación de la Cabecera</param>
        /// <param name="caja">Caja de Impacto del Item</param>
        /// <returns>Colección de MovimientoCaja</returns>
        public EntityCollection<MovimientoCaja> MovimientoCajaReadForReport(CajaUsuario cajaUsuario,
                                                                            DateTime? fechaDesde, DateTime? fechaHasta,
                                                                            Caja caja)
        {
            // Obtengo los Movimientos
            string hql = "select distinct mci.MovimientoCaja from MovimientoCajaItem mci, CajaUsuario cu "
                         + " where cu.Id = mci.MovimientoCaja.CajaUsuarioID ";
            if (cajaUsuario != null)
                hql += " and cu = :cajaUsuario";
            if (fechaDesde.HasValue)
                hql += " and cu.FechaCierre >= :fechaDesde";
            if (fechaHasta.HasValue)
                hql += " and cu.FechaCierre <= :fechaHasta";
            if (caja != null)
                hql += " and mci.CajaImpactoID <= :cajaId";
            hql += " ORDER BY mci.Caja.Id ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            if (cajaUsuario != null)
                query.SetEntity("cajaUsuario", cajaUsuario);
            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value);
            if (caja != null)
                query.SetParameter("cajaId", caja.Id);

            EntityCollection<MovimientoCaja> movimientos = dalEngine.GetManyByQuery<MovimientoCaja>(query);

            // Obtengo los Objetos
            foreach (MovimientoCaja movimiento in movimientos)
            {
                if (movimiento.PagoID.HasValue)
                {
                    movimiento.Pago = dalEngine.GetById<Pago>(movimiento.PagoID.Value);
                    movimiento.Pago.MedioPago = dalEngine.GetById<MedioPago>(movimiento.Pago.MedioPagoID);
                }
            }

            return movimientos;
        }

        /// <summary>
        /// Inserto un Movimiento de Caja
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Insertar</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        /// <param name="actualizaSaldo">Marca si se debe Actualizar el Saldo o NO</param>
        [RequiresTransaction]
        protected virtual MovimientoCaja MovimientoCajaInsert(MovimientoCaja movimiento, bool actualizaSaldo)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            // Asigno los Datos
            movimiento.Fecha = enfoke.IO.Time.Now;
            movimiento.UsuarioID = user.Id;


            // Inserto el Movimiento
            movimiento = dalEngine.Update<MovimientoCaja>(movimiento);

            // Actualizo el Saldo
            if (actualizaSaldo)
                ActualizarSaldo(movimiento);

            return movimiento;
        }

        public void ActualizarSaldo(MovimientoCaja movimiento)
        {
            CajaUsuario caja = movimiento.CajaUsuario;

            Decimal pagoEfectivo = 0;
            Decimal pagoNoEfectivo = 0;

            //Si existen pagos realizados (cobranza)
            if (movimiento.Pagos != null)
            {
                //Por cada pago
                foreach (Pago pago in movimiento.Pagos)
                {
                    //Acumulo los importes dependiendo del tipo medio de pago
                    if (pago.MedioPago.Id == (int)MedioPagoEnum.Efectivo)
                        pagoEfectivo += pago.Importe;
                    else
                        pagoNoEfectivo += pago.Importe * (pago.CotizacionDolar.HasValue ? pago.CotizacionDolar.Value : 1);
                }

                //Si es un movimiento de Egreso transformo los valores a negativos
                if (movimiento.ImporteIngreso < movimiento.ImporteEgreso)
                {
                    pagoNoEfectivo = (-1) * pagoNoEfectivo;
                    pagoEfectivo = (-1) * pagoEfectivo;
                }
            }
            else
            {
                //Si no existen pagos realizados tomo todo el importe como efectivo
                pagoEfectivo = movimiento.ImporteIngreso - movimiento.ImporteEgreso;
            }

            //caja.Saldo = caja.Saldo + (movimiento.ImporteIngreso - movimiento.ImporteEgreso);
            caja.SaldoNoEfectivo = caja.SaldoNoEfectivo + (pagoNoEfectivo);

            caja.SaldoEfectivo = caja.SaldoEfectivo + (pagoEfectivo);
            caja = dalEngine.Update<CajaUsuario>(caja);
        }

        /// <summary>
        /// Inserto un Movimiento de Caja con sus Items
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Insertar</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        /// <param name="item">Item del Movimiento</param>
        /// <param name="actualizaSaldo">Marca si se debe Actualizar el Saldo o NO</param>
        [RequiresTransaction]
        protected virtual MovimientoCaja MovimientoCajaInsert(MovimientoCaja movimiento, MovimientoCajaItem item,
                                                  bool actualizaSaldo)
        {
            // Inserto el Movimiento
            movimiento = this.MovimientoCajaInsert(movimiento, actualizaSaldo);

            // Asigno los Datos del Item
            item.MovimientoCaja = movimiento;

            // Inserto el Item
            this.MovimientoCajaItemInsert(item);

            return movimiento;
        }

        /// <summary>
        /// Inserto un Movimiento de Caja con sus Items
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Insertar</param>
        /// <param name="user">Usuario Conectado al Sistema</param>
        /// <param name="items">Items del Movimiento</param>
        /// <param name="actualizaSaldo">Marca si se debe Actualizar el Saldo o NO</param>
        [RequiresTransaction]
        protected virtual MovimientoCaja MovimientoCajaInsert(MovimientoCaja movimiento,
                                                  EntityCollection<MovimientoCajaItem> items, bool actualizaSaldo)
        {
            // Inserto el Movimiento
            movimiento = this.MovimientoCajaInsert(movimiento, actualizaSaldo);

            foreach (MovimientoCajaItem item in items)
            {
                MovimientoCajaItem itemInsert = item;

                // Asigno los Datos
                itemInsert.MovimientoCaja = movimiento;

                // Inserto el Item
                this.MovimientoCajaItemInsert(itemInsert);
            }

            return movimiento;
        }

        [RequiresTransaction]
        protected virtual MovimientoCaja MovimientoCajaInsert(MovimientoCaja movimiento,
                                                  EntityCollection<MovimientoCajaItem> items,
                                                  EntityCollection<Pago> pagos, bool actualizaSaldo)
        {
            PagoMovimientoCaja pagoMovimientoCaja;

            //Agrego los pagos al movimiento de caja ya que luego sera evaluado el tipo de pago 
            //para ajustar el saldo de la caja
            movimiento.Pagos = pagos;

            movimiento = this.MovimientoCajaInsert(movimiento, items, actualizaSaldo);

            foreach (Pago pago in pagos)
            {
                pagoMovimientoCaja = new PagoMovimientoCaja();
                pagoMovimientoCaja.MovimientoCaja = movimiento;
                pagoMovimientoCaja.Pago = pago;

                dalEngine.Update(pagoMovimientoCaja);
            }

            return movimiento;
        }

        /// <summary>
        /// Retorno los Movimientos de Caja de una Asignación
        /// </summary>
        /// <param name="id">ID de la CajaUsuario a Obtener sus Movimientos</param>
        /// <returns>Colección de MovimientoCaja</returns>
        public EntityCollection<MovimientoCaja> MovimientoCajaReadByCajaUsuario(int cajaUsuarioID)
        {
            return dalEngine.GetManyByProperty<MovimientoCaja>(MovimientoCaja.Properties.CajaUsuarioID, cajaUsuarioID,
                                                               MovimientoCaja.Properties.Fecha);
        }

        public EntityCollection<MovimientoCajaFormulario> MovimientoCajaFormularioReadByParameters(DateTime desde, DateTime hasta, int sucursalId, int cajaId, int usuarioId)
        {
            EntityCollection<MovimientoCajaFormulario> movCaja = MovimientoCajaSinFormularioReadByParameters(desde, hasta, sucursalId, cajaId, usuarioId);
            EntityCollection<MovimientoCajaFormulario> movCajaFrm = MovimientoCajaConFormularioReadParameters(desde, hasta, sucursalId, cajaId, usuarioId);
            movCaja.AddRange(movCajaFrm);
            return movCaja;
        }

        /// <summary>
        /// [PC] Retorno los Movimientos de Caja
        /// </summary>
        /// <param name="desde">Fecha desde del movimiento</param>
        /// <param name="hasta">Fecha hasta del movimiento</param>
        /// <param name="sucursalId">Id de la sucursal de la caja</param>
        /// <param name="cajaId">Id de la caja</param>
        /// <param name="usuarioId">Id del usuario de la caja</param>
        /// <returns>Colección de MovimientoCaja</returns>
        private EntityCollection<MovimientoCajaFormulario> MovimientoCajaSinFormularioReadByParameters(DateTime desde, DateTime hasta,
                                                                               int sucursalId, int cajaId, int usuarioId)
        {
            //[pc]
            // el parametro de la sucursal estaría demás...

            string hql = "select distinct new enfoke.Eges.Entities.Results.MovimientoCajaFormulario(mov) " +
                         "from MovimientoCaja mov, CajaUsuario cu, MovimientoCajaItem moi " +
                         "where mov.CajaUsuarioID = cu.Id " +
                         "and mov.Id = moi.MovimientoCaja.Id " +
                         "and moi.FormularioID is null " +
                         "and mov.Fecha >= :desde " +
                         "and mov.Fecha < :hasta ";

            if (cajaId > 0)
                hql += "and cu.Caja.Id = :cajaId ";

            if (usuarioId > 0)
                hql += "and cu.Usuario.Id = :usuarioId ";

            hql += "and cu.Caja.Sucursal.Id = :sucursalId " +
                   "order by mov.Id asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1));
            query.SetParameter("sucursalId", sucursalId);

            if (cajaId > 0)
                query.SetParameter("cajaId", cajaId);

            if (usuarioId > 0)
                query.SetParameter("usuarioId", usuarioId);

            return dalEngine.GetManyByQuery<MovimientoCajaFormulario>(query);
        }


        ///// <summary>
        ///// Busco todos los Formularios Asociados a un MovimientoCaja
        ///// </summary>
        ///// <param name="movimiento">MovimientoCaja a Buscar sus Formularios</param>
        ///// <returns>Colección de Formularios Asociados a un MovimientoCaja</returns>
        private EntityCollection<MovimientoCajaFormulario> MovimientoCajaConFormularioReadParameters(DateTime desde, DateTime hasta,
                                                                               int sucursalId, int cajaId, int usuarioId)
        {
            string hql = "select distinct new enfoke.Eges.Entities.Results.MovimientoCajaFormulario(m.MovimientoCaja, f)"
                       + " from Formulario f, MovimientoCajaItem m, CajaUsuario cu "
                       + " where f.Id = m.FormularioID "
                       + " and m.MovimientoCaja.CajaUsuarioID = cu.Id "
                       + " and m.MovimientoCaja.Fecha >= :desde "
                       + " and m.MovimientoCaja.Fecha < :hasta ";

            if (cajaId > 0)
                hql += "and cu.Caja.Id = :cajaId ";

            if (usuarioId > 0)
                hql += "and cu.Usuario.Id = :usuarioId ";

            hql += "and cu.Caja.Sucursal.Id = :sucursalId " +
                   "order by m.MovimientoCaja.Id asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("desde", desde.Date);
            query.SetParameter("hasta", hasta.Date.AddDays(1));
            query.SetParameter("sucursalId", sucursalId);

            if (cajaId > 0)
                query.SetParameter("cajaId", cajaId);

            if (usuarioId > 0)
                query.SetParameter("usuarioId", usuarioId);

            return dalEngine.GetManyByQuery<MovimientoCajaFormulario>(query);
        }

        /// <summary>
        /// Retorno un MovimientoCaja correspondiente a un Formulario
        /// </summary>
        /// <param name="id">ID del Formulario</param>
        /// <returns>MovimientoCaja del Formulario</returns>
        public MovimientoCaja MovimientoCajaReadByFormulario(int formularioId)
        {
            // Obtengo los Movimientos
            string hql = "select distinct mci.MovimientoCaja from MovimientoCajaItem mci "
                         + " where mci.FormularioID = :formularioId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("formularioId", formularioId);

            EntityCollection<MovimientoCaja> movimientos = dalEngine.GetManyByQuery<MovimientoCaja>(query);

            if (movimientos.Count > 0)
                return movimientos[0];
            else
                return null;
        }

        public EntityCollection<MovimientoCaja> MovimientosCajaReadByCajaUsuarioAndTipo(int cajaUsuarioId, int tipoMovimiento)
        {
            ReadManyCommand<MovimientoCaja> readCmd = new ReadManyCommand<MovimientoCaja>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, MovimientoCaja.Properties.CajaUsuarioID, "=", cajaUsuarioId);

            filter.Add(BooleanOp.And, MovimientoCaja.Properties.TipoMovimientoCajaID,
                       "=",
                       tipoMovimiento);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(MovimientoCaja.Properties.Fecha, SortingDirection.Desc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        /// <summary>
        /// Retorno una Colección de MovimientoCaja no de Anulacion correspondiente a un Turno
        /// </summary>
        /// <param name="id">ID del Turno</param>
        /// <returns>MovimientoCaja's del Turno</returns>
        public EntityCollection<MovimientoCaja> MovimientosCajaReadByTurno(int id)
        {
            ReadManyCommand<MovimientoCaja> readCmd = new ReadManyCommand<MovimientoCaja>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, MovimientoCaja.Properties.TurnoID,
                       "=", id);

            filter.Add(BooleanOp.And, MovimientoCaja.Properties.TipoMovimientoCajaID,
                       "!=",
                       (int)TipoMovimientoCajaEnum.Anulacion);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(MovimientoCaja.Properties.Fecha, SortingDirection.Desc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        #endregion

        #region MovimientoCajaTransferencia

        public void MovimientoCajaTransferenciaInsert(MovimientoCajaTransferencia movimientoCajaTransferencia)
        {
            dalEngine.Update<MovimientoCajaTransferencia>(movimientoCajaTransferencia);
        }

        public void MovimientoCajaTransferenciaUpdate(MovimientoCajaTransferencia movimientoCajaTransferencia)
        {
            dalEngine.Update<MovimientoCajaTransferencia>(movimientoCajaTransferencia);
        }






        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaRead(int? centro,
                                                                                             DateTime fechaDesde,
                                                                                             DateTime fechaHasta)
        {
            String hql = "select mct " +
                         "from MovimientoCajaTransferencia mct, CajaUsuario cu " +
                         "where mct.CajaUsuarioIdOrigen = cu.Id " +
                         "and mct.CreateDate >= :fechaDesde " +
                         "and mct.CreateDate < :fechaHasta ";

            if (centro.HasValue)
                hql += "and (cu.Caja.Sucursal.Id = :centro " +
                       " or cu.Caja.Sucursal.Id = :centro)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            if (centro.HasValue)
                query.SetParameter("centro", centro.Value);

            return dalEngine.GetManyByQuery<MovimientoCajaTransferencia>(query);
        }

        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaRead(int? centro,
                                                                                             DateTime fechaDesde,
                                                                                             DateTime fechaHasta,
                                                                                             int usuario)
        {
            String hql = "select mct " +
                         "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud" +
                         "where mct.CajaUsuarioIdOrigen = cu.Id " +
                         "and mct.CajaUsuarioIdDestino = cud.Id " +
                         "and mct.CreateDate >= :fechaDesde " +
                         "and mct.CreateDate < :fechaHasta " +
                         "and (cu.Usuario.Id = :usuario or cud.Usuario.Id = :usuario) ";

            if (centro.HasValue)
                hql += "and (cu.Caja.Sucursal.Id = :centro " +
                       " or cu.Caja.Sucursal.Id = :centro)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            query.SetParameter("usuario", usuario);

            if (centro.HasValue)
                query.SetParameter("centro", centro.Value);

            return dalEngine.GetManyByQuery<MovimientoCajaTransferencia>(query);
        }

        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaReadWithObjects(int? centro,
                                                                                                        DateTime
                                                                                                            fechaDesde,
                                                                                                        DateTime
                                                                                                            fechaHasta)
        {
            EntityCollection<DatosMovimientoCajaTransferencia> datosMovimientos =
                this.DatosMovimientoCajaTransferenciaRead(centro, fechaDesde, fechaHasta);
            EntityCollection<MovimientoCajaTransferencia> movimientos =
                new EntityCollection<MovimientoCajaTransferencia>();

            if (datosMovimientos != null)
            {
                foreach (DatosMovimientoCajaTransferencia datoMov in datosMovimientos)
                {
                    movimientos.Add(datoMov.MovimientoCajaTransferencia);
                }
            }

            return movimientos;
        }

        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaReadUsuarioWithObjects(
            int? centro, DateTime fechaDesde, DateTime fechaHasta)
        {
            EntityCollection<DatosMovimientoCajaTransferencia> datosMovimientos =
                this.DatosMovimientoCajaTransferenciaReadUsuario(centro, fechaDesde, fechaHasta);

            EntityCollection<MovimientoCajaTransferencia> movimientos =
                new EntityCollection<MovimientoCajaTransferencia>();

            if (datosMovimientos != null)
            {
                foreach (DatosMovimientoCajaTransferencia datoMov in datosMovimientos)
                {
                    movimientos.Add(datoMov.MovimientoCajaTransferencia);
                }
            }

            return movimientos;
        }

        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaReadByCajaDestinoIdWithObjects(
            int cajaDestinoId, int estadoTransferencia)
        {
            EntityCollection<DatosMovimientoCajaTransferencia> datosMovimientos =
                this.DatosMovimientoCajaTransferenciaReadByCajaDestinoIdAndEstadoId(cajaDestinoId, estadoTransferencia);

            EntityCollection<MovimientoCajaTransferencia> movimientos =
                new EntityCollection<MovimientoCajaTransferencia>();

            if (datosMovimientos != null)
            {
                foreach (DatosMovimientoCajaTransferencia datoMov in datosMovimientos)
                {
                    movimientos.Add(datoMov.MovimientoCajaTransferencia);
                }
            }

            return movimientos;
        }

        public EntityCollection<MovimientoCajaTransferencia> MovimientoCajaTransferenciaReadByIdsWithObjects(
            List<int> movimientoIds)
        {
            EntityCollection<DatosMovimientoCajaTransferencia> datosMovimientos =
                this.DatosMovimientoCajaTransferenciaReadByIds(movimientoIds);

            EntityCollection<MovimientoCajaTransferencia> movimientos =
                new EntityCollection<MovimientoCajaTransferencia>();

            if (datosMovimientos != null)
            {
                foreach (DatosMovimientoCajaTransferencia datoMov in datosMovimientos)
                {
                    movimientos.Add(datoMov.MovimientoCajaTransferencia);
                }
            }

            return movimientos;
        }

        public EntityCollection<MovimientoCajaTransferencia>
            MovimientoCajaTransferenciaReadByCajaUsuarioIdAndEstadoIdWithObjects(int cajaUsuarioId,
                                                                                 int estadoTransferencia)
        {
            EntityCollection<DatosMovimientoCajaTransferencia> datosMovimientos =
                this.DatosMovimientoCajaTransferenciaReadByCajaUsuarioIdAndEstadoId(cajaUsuarioId, estadoTransferencia);

            EntityCollection<MovimientoCajaTransferencia> movimientos =
                new EntityCollection<MovimientoCajaTransferencia>();

            if (datosMovimientos != null)
            {
                foreach (DatosMovimientoCajaTransferencia datoMov in datosMovimientos)
                {
                    movimientos.Add(datoMov.MovimientoCajaTransferencia);
                }
            }

            return movimientos;
        }

        #endregion

        #region DatosMovimientoCajaTransferencia

        private EntityCollection<DatosMovimientoCajaTransferencia> DatosMovimientoCajaTransferenciaReadUsuario(
            int? centro, DateTime fechaDesde, DateTime fechaHasta)
        {
            int usuario = Security.Current.UserInfo.User.Id;
            String hql =
                "select new  enfoke.Eges.Entities.Results.DatosMovimientoCajaTransferencia(mct, cu, cud, est) " +
                "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud , EstadoTransferencia est " +
                "where mct.CajaUsuarioIdOrigen = cu.Id " +
                "and mct.CajaUsuarioIdDestino = cud.Id " +
                "and mct.EstadoTransferenciaId = est.Id " +
                "and mct.CreateDate >= :fechaDesde " +
                "and mct.CreateDate < :fechaHasta " +
                "and (cu.Usuario.Id = :usuario or cud.Usuario.Id = :usuario) ";

            if (centro.HasValue)
                hql += "and (cu.Caja.Sucursal.Id = :centro " +
                       " or cu.Caja.Sucursal.Id = :centro)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            query.SetParameter("usuario", usuario);

            if (centro.HasValue)
                query.SetParameter("centro", centro.Value);

            return dalEngine.GetManyByQuery<DatosMovimientoCajaTransferencia>(query);
        }

        private EntityCollection<DatosMovimientoCajaTransferencia> DatosMovimientoCajaTransferenciaRead(int? centro,
                                                                                                        DateTime
                                                                                                            fechaDesde,
                                                                                                        DateTime
                                                                                                            fechaHasta)
        {
            String hql =
                "select new  enfoke.Eges.Entities.Results.DatosMovimientoCajaTransferencia(mct, cu, cud, est) " +
                "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud , EstadoTransferencia est " +
                "where mct.CajaUsuarioIdOrigen = cu.Id " +
                "and mct.CajaUsuarioIdDestino = cud.Id " +
                "and mct.EstadoTransferenciaId = est.Id " +
                "and mct.CreateDate >= :fechaDesde " +
                "and mct.CreateDate < :fechaHasta ";

            if (centro.HasValue)
                hql += "and (cu.Caja.Sucursal.Id = :centro)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            if (centro.HasValue)
                query.SetParameter("centro", centro.Value);

            return dalEngine.GetManyByQuery<DatosMovimientoCajaTransferencia>(query);
        }

        private EntityCollection<DatosMovimientoCajaTransferencia>
            DatosMovimientoCajaTransferenciaReadByCajaDestinoIdAndEstadoId(int cajaDestino, int estadoTransferencia)
        {
            String hql =
                "select new  enfoke.Eges.Entities.Results.DatosMovimientoCajaTransferencia(mct, cu, cud, est) " +
                "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud , EstadoTransferencia est " +
                "where mct.CajaUsuarioIdOrigen = cu.Id " +
                "and mct.CajaUsuarioIdDestino = cud.Id " +
                "and mct.EstadoTransferenciaId = est.Id " +
                "and cud.Caja.Id = :cajaDestino " +
                "and cud.FechaCierre is null ";

            if (estadoTransferencia > 0)
                hql += "and mct.EstadoTransferenciaId = :estadoTransferencia ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaDestino", cajaDestino);

            if (estadoTransferencia > 0)
                query.SetParameter("estadoTransferencia", estadoTransferencia);

            return dalEngine.GetManyByQuery<DatosMovimientoCajaTransferencia>(query);
        }

        private EntityCollection<DatosMovimientoCajaTransferencia> DatosMovimientoCajaTransferenciaReadByIds(
            List<int> movimientoIds)
        {
            String hql =
                "select new  enfoke.Eges.Entities.Results.DatosMovimientoCajaTransferencia(mct, cu, cud, est) " +
                "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud , EstadoTransferencia est " +
                "where mct.CajaUsuarioIdOrigen = cu.Id " +
                "and mct.CajaUsuarioIdDestino = cud.Id " +
                "and mct.EstadoTransferenciaId = est.Id " +
                "and mct.Id in (:movimientoIds) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("movimientoIds", movimientoIds.ToArray());

            return dalEngine.GetManyByQuery<DatosMovimientoCajaTransferencia>(query);
        }

        private EntityCollection<DatosMovimientoCajaTransferencia>
            DatosMovimientoCajaTransferenciaReadByCajaUsuarioIdAndEstadoId(int cajaUsuarioId, int estadoTransferencia)
        {
            String hql =
                "select new  enfoke.Eges.Entities.Results.DatosMovimientoCajaTransferencia(mct, cu, cud, est) " +
                "from MovimientoCajaTransferencia mct, CajaUsuario cu , CajaUsuario cud , EstadoTransferencia est " +
                "where mct.CajaUsuarioIdOrigen = cu.Id " +
                "and mct.CajaUsuarioIdDestino = cud.Id " +
                "and mct.EstadoTransferenciaId = est.Id " +
                "and (mct.CajaUsuarioIdOrigen = :cajaUsuarioId or mct.CajaUsuarioIdDestino = :cajaUsuarioId) ";

            if (estadoTransferencia > 0)
                hql += "and mct.EstadoTransferenciaId = :estadoTransferencia ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaUsuarioId", cajaUsuarioId);

            if (estadoTransferencia > 0)
                query.SetParameter("estadoTransferencia", estadoTransferencia);

            return dalEngine.GetManyByQuery<DatosMovimientoCajaTransferencia>(query);
        }

        #endregion

        #region EstadoTransferencia






        public EstadoTransferencia EstadoTransferenciaReadById(int estadoTransferenciaId)
        {
            return dalEngine.GetById<EstadoTransferencia>(estadoTransferenciaId);
        }

        #endregion

        #region MovimientoCajaItem

        /// <summary>
        /// Inserto un Item Movimiento de Caja
        /// </summary>
        /// <param name="item">MovimientoCaja a Insertar</param>
        private void MovimientoCajaItemInsert(MovimientoCajaItem item)
        {

            // Inserto el Item
            item = dalEngine.Update<MovimientoCajaItem>(item);
        }

        /// <summary>
        /// Inserto varios Items de Movimiento de Caja
        /// </summary>
        /// <param name="items">MovimientoCaja a Insertar</param>
        [RequiresTransaction]
        protected virtual void MovimientoCajaItemInsert(EntityCollection<MovimientoCajaItem> items)
        {
            foreach (MovimientoCajaItem item in items)
                this.MovimientoCajaItemInsert(item);
        }

        /// <summary>
        /// Retorno los Items de un Movimientos de Caja
        /// </summary>
        /// <param name="id">ID del Movimiento</param>
        /// <returns>Colección de MovimientoCajaItem</returns>
        public EntityCollection<MovimientoCajaItem> MovimientoCajaItemReadByMovimiento(int id, bool cargarObjetos)
        {
            EntityCollection<MovimientoCajaItem> items =
                dalEngine.GetManyByProperty<MovimientoCajaItem>(MovimientoCajaItem.Properties.MovimientoCaja.Id, id,
                                                                MovimientoCajaItem.Properties.Id);

            // Obtengo los Objetos según los IDs
            if (cargarObjetos)
                foreach (MovimientoCajaItem mci in items)
                    CargarObjetos(mci);

            return items;
        }

        private void CargarObjetos(MovimientoCajaItem mci)
        {
            mci.TipoMovimientoCajaItem = dalEngine.GetById<TipoMovimientoCajaItem>(mci.TipoMovimientoCajaItemID);
            if (mci.CajaImpactoID.HasValue)
                mci.CajaImpacto = dalEngine.GetById<Caja>(mci.CajaImpactoID.Value);
        }
















        /// <summary>
        /// Retorno los Items que Apliquen a los Parametros
        /// </summary>
        /// <param name="cajaUsuario">Asignación de la Cabecera</param>
        /// <param name="fechaDesde">Fecha Desde de Cierre de la Asignación de la Cabecera</param>
        /// <param name="fechaHasta">Fecha Hasta de Cierre de la Asignación de la Cabecera</param>
        /// <param name="caja">Caja de Impacto del Item</param>
        /// <returns>Colección de MovimientoCajaItem</returns>
        public EntityCollection<MovimientoCajaItem> MovimientoCajaItemReadForReport(CajaUsuario cajaUsuario,
                                                                                    DateTime? fechaDesde,
                                                                                    DateTime? fechaHasta, Caja caja)
        {
            // Obtengo los Movimientos
            string hql = "select mci from MovimientoCajaItem mci, CajaUsuario cu "
                         + " where cu.Id = mci.MovimientoCaja.CajaUsuarioID ";
            if (cajaUsuario != null)
                hql += " and cu = :cajaUsuario";
            if (fechaDesde.HasValue)
                hql += " and cu.FechaCierre >= :fechaDesde";
            if (fechaHasta.HasValue)
                hql += " and cu.FechaCierre <= :fechaHasta";
            if (caja != null)
                hql += " and mci.CajaImpactoID <= :cajaId";
            hql += " ORDER BY mci.Id ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            if (cajaUsuario != null)
                query.SetEntity("cajaUsuario", cajaUsuario);
            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde.Value);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta.Value);
            if (caja != null)
                query.SetParameter("cajaId", caja.Id);

            EntityCollection<MovimientoCajaItem> items = dalEngine.GetManyByQuery<MovimientoCajaItem>(query);

            // Obtengo los Objetos
            foreach (MovimientoCajaItem item in items)
            {
                if (item.MovimientoCaja.PagoID.HasValue)
                {
                    item.MovimientoCaja.Pago = dalEngine.GetById<Pago>(item.MovimientoCaja.PagoID.Value);
                    item.MovimientoCaja.Pago.MedioPago = dalEngine.GetById<MedioPago>(item.MovimientoCaja.Pago.MedioPagoID);
                }
            }

            return items;
        }

        /// <summary>
        /// Retorno los Items que Apliquen a los Parametros
        /// </summary>
        /// <param name="caja">Caja de Impacto del Item</param>
        /// <param name="fechaDesde">Fecha Desde del Movimiento</param>
        /// <param name="fechaHasta">Fecha Hasta del Movimiento</param>
        /// <returns>Colección de MovimientoCajaItem</returns>
        public EntityCollection<MovimientoCajaItem> MovimientoCajaItemReadForReportPorFecha(Caja caja,
                                                                                            DateTime fechaDesde,
                                                                                            DateTime fechaHasta)
        {
            int[] estados = new int[]
                                {
                                    (int) TipoMovimientoCajaEnum.Ajuste, (int) TipoMovimientoCajaEnum.Anticipo,
                                    (int) TipoMovimientoCajaEnum.Anulacion, (int) TipoMovimientoCajaEnum.Cobro,
                                    (int) TipoMovimientoCajaEnum.Deposito, (int) TipoMovimientoCajaEnum.Devolucion
                                }; //,(int)TipoMovimientoCajaEnum.Transferencia };
            string hql = "select mci from MovimientoCajaItem mci, CajaUsuario cu "
                         + " where cu.Id = mci.MovimientoCaja.CajaUsuarioID  AND cu.FechaCierre is not null "
                         + " and mci.MovimientoCaja.TipoMovimientoCajaID in " +
                         Utils.EnumerableConvert.ToString(estados) + " "
                         + " and mci.CajaImpactoID = :cajaId "
                         + " and mci.MovimientoCaja.Fecha >= :fechaDesde "
                         + " and mci.MovimientoCaja.Fecha < :fechaHasta "
                         + " order by mci.id asc";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaId", caja.Id);
            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            return dalEngine.GetManyByQuery<MovimientoCajaItem>(query);
        }

        public EntityCollection<MovimientoCajaItemFormulario> MovimientoCajaItemReadForReportPorFechaCajaAndSucursal(
            int? empresaId, int? sucursalId, int? cajaId, DateTime fechaDesde, DateTime fechaHasta)
        {
            Caja caja = null;
            if (cajaId.HasValue == true)
                caja = dalEngine.GetById<Caja>(cajaId.Value);

            List<int> estadosMovimiento = new List<int>();
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Ajuste);
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Anticipo);
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Anulacion);
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Cobro);
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Deposito);
            estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Devolucion);

            if (caja != null && caja.Operativa == (int)CajaEnum.Fondeo)
                estadosMovimiento.Add((int)TipoMovimientoCajaEnum.Transferencia);

            string hql = " select distinct new enfoke.Eges.Entities.Results.MovimientoCajaItemFormulario(mci, frm) ";

            hql += " from MovimientoCajaItem mci, CajaUsuario cu, Caja caj, Formulario frm ";
            hql += " where cu.Id = mci.MovimientoCaja.CajaUsuarioID ";

            //Bug #8216.-
            //if (caja != null && caja.Operativa != (int)CajaEnum.Fondeo)
            //    hql += "and cu.FechaCierre is not null ";

            hql += " and mci.CajaImpactoID = caj.Id "
                + " and mci.MovimientoCaja.TipoMovimientoCajaID in (:estadosMovimiento) ";

            if (cajaId.HasValue)
            {
                if (caja != null && caja.Operativa != (int)CajaEnum.Fondeo)
                    hql += " and caj.Id = :cajaId ";
                else
                    hql += " and cu.Caja.Id = :cajaId ";
            }

            if (sucursalId.HasValue)
            {
                if (caja != null && caja.Operativa != (int)CajaEnum.Fondeo)
                {
                    hql += " and ( caj.Sucursal.Id = :sucursalId) ";
                }
                else
                {
                    hql += " and ( cu.Caja.Sucursal.Id = :sucursalId )";
                }
            }

            if (empresaId.HasValue)
                hql += "and (mci.FormularioID is null or (mci.FormularioID = frm.Id and frm.Empresa.Id = :empresaId ))";
            else
                hql += "and (mci.FormularioID is null or mci.FormularioID = frm.Id)";

            hql += " and mci.MovimientoCaja.Fecha >= :fechaDesde "
                   + " and mci.MovimientoCaja.Fecha < :fechaHasta "
                   + " order by mci.id asc";

            IQuery query = dalEngine.CreateQuery(hql);

            if (cajaId.HasValue)
                query.SetParameter("cajaId", cajaId.Value);

            if (sucursalId.HasValue)
                query.SetParameter("sucursalId", sucursalId.Value);

            if (empresaId.HasValue)
                query.SetParameter("empresaId", empresaId.Value);

            query.SetParameter("fechaDesde", fechaDesde);
            query.SetParameter("fechaHasta", fechaHasta.AddDays(1));
            query.SetParameterList("estadosMovimiento", estadosMovimiento);
            return dalEngine.GetManyByQuery<MovimientoCajaItemFormulario>(query);
        }

        public void MovimientoCajaItemDeleteByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            dalEngine.Delete(this.MovimientoCajaItemReadByMovimiento(movimientoCaja.Id, true));
        }

        #endregion

        #region TipoFormulario




















        #endregion

        #region TipoTalonario
















        #endregion

        #region MedioPago











        /// <summary>
        /// Retorno todos los MedioPago
        /// </summary>
        /// <returns>Todos los MedioPago</returns>
        public EntityCollection<MedioPago> MedioPagoReadAll()
        {
            return dalEngine.GetAll<MedioPago>("Id");
        }

        #endregion

        #region EntidadPago











        /// <summary>
        /// Retorno todas las EntidadPago
        /// </summary>
        /// <returns>Todas las EntidadPago</returns>
        public EntityCollection<EntidadPago> EntidadPagoReadAll()
        {
            return dalEngine.GetAll<EntidadPago>(EntidadPago.Properties.Name);
        }

        public EntityCollection<EntidadPago> EntidadPagoReadByMedioPago(int medioPagoId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from EntidadPago ent ");
            hql.Append("where ent.Deleted = :eliminado ");
            hql.Append("and ent.MedioPagoId = :medioPagoId ");
            hql.Append("order by ent.Name desc ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("eliminado", false);
            query.SetParameter("medioPagoId", medioPagoId);
            return dalEngine.GetManyByQuery<EntidadPago>(query);
        }



        #endregion

        #region Cobranza

        [RequiresTransaction]
        [Private]
        public virtual void FormularioInsert(Turno turno, Formulario formulario)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;

            FacturacionDalc.FormularioInsert(formulario, turno);
        }

        /// <summary>
        /// Creo una Cobranza con su Factura, Recibos, Pago, Movimientos
        /// </summary>
        /// <param name="turno">Turno de la Cobranza</param>
        /// <param name="factura">Datos de la Factura a Generar</param>
        /// <param name="recibos">Datos de los Recibos de Médico a Generar</param>
        /// <param name="pago">Datos del Pago a Generar</param>
        /// <param name="caja">Asignación de Caja del Pago</param>
        /// <param name="condicionIVA">Condicion de IVA</param>
        /// <param name="user">Usuario de la Operacion</param>
        /// <param name="porcentajeIVA">Porcentaje de IVA Utilizado Internamente</param>
        /// <returns>El Movimiento Creado</returns>
        [RequiresTransaction]
        public virtual MovimientoCaja CobranzaCreate(Turno turno, Formulario factura, List<ReciboHonorarioMedico> recibos,
                                           EntityCollection<Pago> pagos, CajaUsuario caja, CondicionIVA condicionIVA,
                                           decimal porcentajeIVA, bool crearFormulariosRM,
                                           FacturaNumeracion facturaNumeracion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            bool actualizarTurno = true;

            MovimientoCaja movimientoCaja = null;

            // ID utilizado como referencia para los ReciboMedico
            int idFormularioRecibo = 0;

            // Obtengo la Practica Principal del Turno
            EntityCollection<PracticaTurno> practicas = TurnosDalc.PracticaTurnoReadByTurno(turno.Id, PracticaTurnoTipoEnum.Principal);
            PracticaTurno practica = null;
            if (practicas.Count > 0)
                practica = practicas[0];
            else
                throw new Exception("No se Encontro la Práctica Principal del Turno [" + turno.Id.ToString() + "].");

            // Obtengo los Tipo de Movimiento de Item
            TipoMovimientoCajaItem tipoMovimientoItemCobroInterno =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.CobroInterno);
            if (tipoMovimientoItemCobroInterno.CajaID.HasValue)
                tipoMovimientoItemCobroInterno.Caja = dalEngine.GetById<Caja>(tipoMovimientoItemCobroInterno.CajaID.Value);
            TipoMovimientoCajaItem tipoMovimientoItemCobroHonorarios =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.CobroHonorarios);
            if (tipoMovimientoItemCobroHonorarios.CajaID.HasValue)
                tipoMovimientoItemCobroHonorarios.Caja =
                    dalEngine.GetById<Caja>(tipoMovimientoItemCobroHonorarios.CajaID.Value);
            TipoMovimientoCajaItem tipoMovimientoItemRecuperoGastos =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.RecuperoGastos);
            if (tipoMovimientoItemRecuperoGastos.CajaID.HasValue)
                tipoMovimientoItemRecuperoGastos.Caja = dalEngine.GetById<Caja>(tipoMovimientoItemRecuperoGastos.CajaID.Value);

            // Inserto los Medio de Pago
            decimal importe = 0;
            for (int i = pagos.Count -1; i >=0; i--)
            {
                // Si es un medio de pago con importe 0 o negativo
                // => no se persiste, lo saco de la colleccion asi despues no molesta
                if (pagos[i].Importe <= 0)
                    pagos.RemoveAt(i);
                else
                {
                    pagos[i] = this.PagoInsert(pagos[i]);
                    importe += pagos[i].Importe*(pagos[i].CotizacionDolar.HasValue ? pagos[i].CotizacionDolar.Value : 1);
                }
            }

            // Creo el Movimiento con los Items
            Movimiento movimiento = new Movimiento();

            // Creo el Movimiento
            movimientoCaja = new MovimientoCaja();

            // Si no esta, obtengo el Paciente
            if (turno.Orden.Paciente == null)
                turno.Orden.Paciente = TurnosDalc.PacienteReadById(turno.Orden.PacienteId);

            // Asigno los Datos del Movimiento
            movimientoCaja.CajaUsuario = caja;
            movimientoCaja.CajaUsuarioID = caja.Id;
            movimientoCaja.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Cobro;
            movimientoCaja.Descripcion = "Cobro - Pract.: " + practica.Practica.Name + " - Prot.: " +
                                         turno.Orden.Protocolo.ProtocoloFull;
            movimientoCaja.ImporteIngreso = importe;
            movimientoCaja.ImporteEgreso = 0;
            movimientoCaja.TurnoID = turno.Id;
            if (facturaNumeracion != null)
                movimientoCaja.FacturaNumeracionId = facturaNumeracion.Id;

            // Guardo el Movimiento para luego Insertarlo
            movimiento.MovimientoCaja = movimientoCaja;

            // Guardo los Pagos para luego relacionarlos
            movimiento.Pagos = pagos;

            PuntoVenta pos = facturaNumeracion.PuntoVenta;
            if (pos == null)
                pos = (Context.Session.PuntoVentaDalc).PuntoVentaReadFirstByCajaAndEmpresaId(caja.Caja.Id, factura.Sucursal.GetValueOrDefault(0), factura.Empresa.Id);

            // Si Corresponde Creo el Item de la Factura
            if (factura.ImporteNeto > 0)
            {
                // Valido que el Número siga Siendo el Mismo
                if (pos != null && !pos.UsaImpresoraFiscal) //Si no usa impresora fiscal
                {
                    FacturaNumeracion numero = FacturacionDalc.FacturaNumeracionReadByTalonarioTipoAndPuntoVenta((int)TipoTalonarioEnum.Factura,
                                                                                 factura.Clase,
                                                                                 pos.Id);
                    if (int.Parse("0" + factura.Numero) != numero.Numero)
                        throw new NotLoggeableException(
                            "El Número de Factura Ha sido Asignado a otra Cobraza. Debe Obtener uno Nuevo para Continuar.");

                    // Incremento la Númeracion
                    FacturacionDalc.FacturaNumeracionIncrementar(numero);
                }

                // Inserto la Factura
                factura = FacturacionDalc.FormularioInsert(factura, turno);

                MovimientoCajaItem item = new MovimientoCajaItem();

                // Asigno los Datos del Item
                item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.CobroInterno;
                item.CajaImpacto = tipoMovimientoItemCobroInterno.Caja;
                item.CajaImpactoID = tipoMovimientoItemCobroInterno.CajaID;
                item.Descripcion = "Cobro - Factura: " + factura.Clase + "-" +
                                   factura.Sucursal.Value.ToString().Trim().PadLeft(4, '0') +
                                   ((factura.Numero != null)
                                        ? "-" + factura.Numero.Value.ToString().Trim().PadLeft(8, '0')
                                        : "Factura Fiscal");
                item.ImporteIngreso = Decimal.Add((factura.ImporteNeto), factura.ImporteIVA);
                item.ImporteEgreso = 0;
                item.FormularioID = factura.Id;

                // Guardo el Item para luego Insertarlo
                movimiento.Items.Add(item);

                // Guardo el ID de la Factura como referencia para los ReciboMedico
                idFormularioRecibo = factura.Id;
                this.AgregarItemsDePercepcionIIBB(factura, tipoMovimientoItemCobroHonorarios, movimiento);
            }

            // Inserto los Recibos y Creo los Items
            foreach (ReciboHonorarioMedico recibo in recibos)
            {
                // Chequeo si debo crear los Formularios de RM
                if (crearFormulariosRM)
                {
                    recibo.Empresa = facturaNumeracion.PuntoVenta.Empresa;
                    Formulario formulario = CrearFormulario(turno, condicionIVA, factura.PorcentajeIVA.GetValueOrDefault(0), FacturacionDalc, recibo);

                    // Obtengo el Porcentaje de Recupero de Gastos
                    decimal recupero = recibo.Medico.PorcentajeRecuperoHonorariosCaja;

                    // Calculo el Importe y el IVA de Recupero
                    decimal importeRecupero = Decimal.Round(formulario.ImporteNeto * recupero / 100, 2,
                                                            MidpointRounding.AwayFromZero);
                    decimal IVARecupero = Decimal.Round(importeRecupero * porcentajeIVA / 100, 2,
                                                        MidpointRounding.AwayFromZero);

                    MovimientoCajaItem itemCobro = new MovimientoCajaItem();
                    itemCobro.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.CobroHonorarios;
                    itemCobro.CajaImpacto = tipoMovimientoItemCobroHonorarios.Caja;
                    itemCobro.CajaImpactoID = tipoMovimientoItemCobroHonorarios.CajaID;
                    itemCobro.Descripcion = "Cobro Honorarios - Recibo: (" + recibo.Recibo.ToString() + ") " +
                                            recibo.Medico.FullName;
                    itemCobro.ImporteIngreso = formulario.ImporteNeto + formulario.ImporteIVA - (importeRecupero + IVARecupero);
                    itemCobro.ImporteEgreso = 0;
                    itemCobro.FormularioID = formulario.Id;

                    // Guardo el Item para luego Insertarlo
                    movimiento.Items.Add(itemCobro);

                    // Si Corresponde, genero el Item de Recupero
                    if (recupero > 0)
                    {
                        MovimientoCajaItem itemRecupero = new MovimientoCajaItem();
                        itemRecupero.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.RecuperoGastos;
                        itemRecupero.CajaImpacto = tipoMovimientoItemRecuperoGastos.Caja;
                        itemRecupero.CajaImpactoID = tipoMovimientoItemRecuperoGastos.CajaID;
                        itemRecupero.Descripcion = "Recupero de Gastos - Recibo: (" + recibo.Recibo.ToString() + ") " +
                                                   recibo.Medico.FullName + " - " + recupero.ToString() + "%";
                        itemRecupero.ImporteIngreso = importeRecupero + IVARecupero;
                        itemRecupero.ImporteEgreso = 0;
                        itemRecupero.FormularioID = formulario.Id;
                        movimiento.Items.Add(itemRecupero);
                    }

                    idFormularioRecibo = formulario.Id;
                    this.AgregarItemsDePercepcionIIBB(formulario, tipoMovimientoItemCobroHonorarios, movimiento);
                }

                this.CrearReciboMedico(turno, condicionIVA, factura.PorcentajeIVA.GetValueOrDefault(0), idFormularioRecibo, recibo);

            }


            this.MovimientoCajaInsert(movimiento.MovimientoCaja, movimiento.Items, movimiento.Pagos, true);
            bool desinhibirEntrega = false;

            if (actualizarTurno)
            {
                // Registro Log
                TurnosDalc.LogRegistrar((int)LogEventoEnum.ModificacionCobranzaVigente,
                                        "Se generó la cobranza del turno.", turno.Id);
                TurnosDalc.TurnoUpdateCobranzaVigenteUpdate(turno.Id, movimientoCaja.Id, desinhibirEntrega);
            }

            return movimientoCaja;
        }

        private void CrearReciboMedico(Turno turno, CondicionIVA condicionIVA, decimal porcentajeIVA, int idFormularioRecibo, ReciboHonorarioMedico recibo)
        {
            ReciboMedico reciboMedico = new ReciboMedico();
            reciboMedico.PorcentajeIVA = porcentajeIVA;
            reciboMedico.CondicionIVAID = condicionIVA.Id;
            reciboMedico.FormularioID = idFormularioRecibo;
            reciboMedico.ImporteNeto = recibo.Total;
            reciboMedico.ImporteIVA = Decimal.Round(recibo.Total * condicionIVA.Porcentaje / 100, 2,
                                                    MidpointRounding.AwayFromZero);
            reciboMedico.ImporteBonificado = recibo.Bonificacion;
            reciboMedico.MedicoID = recibo.Medico.Id;
            reciboMedico.TurnoID = turno.Id;
            reciboMedico.LiquidacionAplicaDescuento = recibo.LiquidacionAplicaDescuento;
            reciboMedico = ReciboMedicoInsert(reciboMedico);
        }

        private void AgregarItemsDePercepcionIIBB(Formulario factura, TipoMovimientoCajaItem tipoMovimientoItemCobroHonorarios, Movimiento movimiento)
        {
            // Si no hay importe IIBB => no se registra nada en BD
            if (factura.ImporteIIBB <= 0)
                return;

            Caja cajaVirtualImpuestos = this.CajaReadByTag(Caja.CAJA_VIRTUAL_IMPUESTOS_TAG);
            string percepcionIIBB = "Percepción";
            int tipoMovimientoCajaItem = (int)TipoMovimientoCajaItemEnum.PercepcionIIBB;
            if ((TipoFormularioEnum)factura.TipoFormularioID == TipoFormularioEnum.NotaCredito)
            {
                percepcionIIBB = "Devolución";
                tipoMovimientoCajaItem = (int)TipoMovimientoCajaItemEnum.DevolucionIIBB;
            }

            MovimientoCajaItem itemIIBB = new MovimientoCajaItem();
            itemIIBB.TipoMovimientoCajaItemID = tipoMovimientoCajaItem;
            itemIIBB.CajaImpacto = cajaVirtualImpuestos;
            itemIIBB.CajaImpactoID = cajaVirtualImpuestos.Id;
            itemIIBB.Descripcion = string.Format("{0} IIBB. Factura: {1} ", percepcionIIBB, factura.Descripcion);
            itemIIBB.ImporteIngreso = factura.ImporteIIBB;
            itemIIBB.ImporteEgreso = 0;
            itemIIBB.FormularioID = factura.Id;
            movimiento.Items.Add(itemIIBB);
        }

        private static Formulario CrearFormulario(Turno turno, CondicionIVA condicionIVA, decimal porcentajeIVA, FacturacionDalc FacturacionDalc, ReciboHonorarioMedico recibo)
        {
            // Creo el Formulario del RM
            Formulario formulario = new Formulario();
            formulario.RazonSocial = recibo.RazonSocial;
            formulario.Domicilio = recibo.Domicilio;
            formulario.CUIT = recibo.Cuit;
            formulario.TipoFormularioID = (int)recibo.TipoFormulario;
            formulario.Numero = recibo.Recibo;
            formulario.ImporteNeto = recibo.Total;
            formulario.CondicionIVAID = condicionIVA.Id;
            formulario.PorcentajeIVA = porcentajeIVA;
            formulario.ImporteRedondeo = recibo.Redondeo;
            formulario.ImporteIVA = Decimal.Round((recibo.Total * condicionIVA.Porcentaje / 100), 2,
                                                  MidpointRounding.AwayFromZero);
            formulario.ImporteBonificacion = recibo.Bonificacion;
            formulario.MedicoID = recibo.Medico.Id;
            formulario.TurnoID = turno.Id;
            formulario.Empresa = recibo.Empresa;
            // Inserto el Formulario de Recibo
            formulario = FacturacionDalc.FormularioInsert(formulario, turno);
            return formulario;
        }

        /// <summary>
        /// Anulo un Movimiento de Cobranza
        /// </summary>
        /// <param name="movimiento">Movimiento a Anular</param>
        /// <param name="noDebeOrden">Marca si debe Poner la marca de Debe Orden del Turno en NoDebe</param>
        /// <param name="user">Usuario de la Operacion</param>
        [Private]
        [RequiresTransaction]
        public virtual void CobranzaAnular(MovimientoCaja movimiento, bool noDebeOrden,
                                 ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            /*
             * [JR] [14/08/2008]
             * No Permito anular una Nota de Crédito ya que:
             * - Un cambio en la valorizacion de admision puede llevar a habilitar una factura 
             *   cuyo importe es inválido respecto de lo que habría que cobrar al paciente
             * - Una re-asignacion de medico entre la nota de credito y su anulacion podría 
             *   generar que los recibos queden asignados a un medico incorrecto
             */

            // Obtengo los Formularios a Anular
            EntityCollection<Formulario> formularios = FacturacionDalc.FormularioReadByMovimientoCaja(movimiento.Id);

            // Obtengo los Recibos Medicos a Anular
            EntityCollection<ReciboMedico> recibos = ReciboMedicoReadByMovimientoCaja(movimiento.Id);

            // Chequeo si algún Recibo Médico fue Liquidado
            foreach (ReciboMedico recibo in recibos)
                if (recibo.LiquidacionHonorariosID.HasValue)
                    throw new NotLoggeableException("No se Puede Anular la Cobranza, ya que el Recibo Médico de " +
                                                    recibo.Medico.Apellido + " se encuentra Liquidado.");

            // Si el movimiento es una nota de credito 
            if (movimiento.TipoMovimientoCajaID == (int)TipoMovimientoCajaEnum.Devolucion)
                if (movimiento.MovimientoOriginalID.HasValue)
                {
                    // Quito la fecha de anulacion del formulario padre
                    MovimientoCaja movimientoPadre = dalEngine.GetById<MovimientoCaja>(movimiento.MovimientoOriginalID.Value);
                    movimientoPadre.FechaAnulacion = null;

                    dalEngine.Update(movimientoPadre);
                }

            // Chequeo que la Caja tenga Saldo
            // [PC] Se debe permitir anular en cualquier momento, si queda saldo negativo se en el cierre de la caja
            //this.CajaUsuarioChequearSaldoEgreso(movimiento.CajaUsuario, Decimal.Subtract(movimiento.ImporteIngreso, movimiento.ImporteEgreso), true);

            #region Movimiento de Anulacion

            // Creo el Movimiento con los Items
            Movimiento movimientoAnulacion = new Movimiento();

            // Creo el Movimiento
            MovimientoCaja movimientoCaja = new MovimientoCaja();

            // Asigno los Datos del Movimiento
            movimientoCaja.CajaUsuario = movimiento.CajaUsuario;
            movimientoCaja.CajaUsuarioID = movimiento.CajaUsuarioID;
            movimientoCaja.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Anulacion;
            movimientoCaja.Descripcion = "[Anulación] " + movimiento.Descripcion;
            movimientoCaja.ImporteIngreso =
                Decimal.Negate(Decimal.Subtract(movimiento.ImporteIngreso, movimiento.ImporteEgreso));
            movimientoCaja.ImporteEgreso = 0;
            movimientoCaja.TurnoID = movimiento.TurnoID;
            movimientoCaja.MovimientoOriginal = movimiento;
            movimientoCaja.MovimientoOriginalID = movimiento.Id;

            // Guardo el Movimiento para luego Insertarlo
            movimientoAnulacion.MovimientoCaja = movimientoCaja;

            // Creo los Items de Anulación por cada Item del Original
            EntityCollection<MovimientoCajaItem> items = this.MovimientoCajaItemReadByMovimiento(movimiento.Id, false);

            foreach (MovimientoCajaItem i in items)
            {
                // Creo el Item de Recupero de Gastos
                MovimientoCajaItem item = new MovimientoCajaItem();

                // Asigno los Datos del Item
                item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Anulacion;
                item.CajaImpactoID = i.CajaImpactoID;
                item.Descripcion = "[Anulación] " + i.Descripcion;
                item.ImporteIngreso = Decimal.Negate(Decimal.Subtract(i.ImporteIngreso, i.ImporteEgreso));
                item.ImporteEgreso = 0;
                item.FormularioID = i.FormularioID;

                // Guardo el Item para luego Insertarlo
                movimientoAnulacion.Items.Add(item);
            }

            //Cargo los pagos del movimiento para luego evaluar los medios de pago 
            //y ajustar el saldo
            movimientoAnulacion.MovimientoCaja.Pagos = movimiento.Pagos;

            // Inserto el Movimiento con sus Items Actualizando Saldos
            this.MovimientoCajaInsert(movimientoAnulacion.MovimientoCaja, movimientoAnulacion.Items, true);

            #endregion

            #region Formularios

            // Anulo todos los Formularios Asociados al Movimiento
            for (int i = 0; i < formularios.Count; i++)
            {
                if (formularios[i].EnvioERP)
                    throw new NotLoggeableException("No se Puede Anular la Cobranza, ya que fué enviada al ERP");

                formularios[i].FechaAnulacion = enfoke.IO.Time.Now;
            }

            // Actualizo los Formularios
            formularios = dalEngine.UpdateCollection<Formulario>(formularios);

            #endregion

            #region Recibos Medicos

            // Anulo todos los Recibos Medicos Asociados al Movimiento
            for (int i = 0; i < recibos.Count; i++)
                recibos[i].FechaAnulacion = enfoke.IO.Time.Now;

            // Actualizo los Recibos Medicos
            recibos = dalEngine.UpdateCollection<ReciboMedico>(recibos);

            #endregion

            #region Movimiento Anulado

            // Anulo el Movimiento
            movimiento.FechaAnulacion = enfoke.IO.Time.Now;

            dalEngine.Update(movimiento);

            // Si Corresponde, DesRelaciono la Cobranza del Turno y/o cambio el estado del DebeOrdenMedica
            if (movimiento.TurnoID.HasValue)
            {
                Turno turno = TurnosDalc.TurnoReadById(movimiento.TurnoID.Value);

                string cambioCobranza = String.Empty;

                bool modificado = noDebeOrden;

                // Si la Cobranza Vigente es el Movimiento Actual -> la DesRelaciono
                if (turno.CobranzaVigenteID.GetValueOrDefault(0) == movimiento.Id)
                {
                    cambioCobranza = "Se desrelacionó la cobranza original.";

                    turno.CobranzaVigenteID = null;

                    modificado = true;
                }

                // Chequeo si debo cambiar el estado del DebeOrdenMedica
                if (noDebeOrden)
                {
                    // Si el paciente no debe nada, desinhibo la entrega del informe
                    if (PacienteNoDebeAbonar(turno.Id, modalidadCoseguro))
                        turno.RequiereCobranza = false;
                }

                // Si corresponde, actualizo los datos del turno
                if (modificado)
                {
                    // Registro Log
                    if (!String.IsNullOrEmpty(cambioCobranza))
                        TurnosDalc.LogRegistrar((int)LogEventoEnum.ModificacionCobranzaVigente, cambioCobranza,
                                                turno.Id);

                    TurnosDalc.TurnoUpdateCobranzaVigenteUpdate(turno.Id, turno.CobranzaVigenteID, false);
                    dalEngine.Update(turno.Orden);
                    TurnosDalc.TurnoUpdate(turno);
                }
            }

            #endregion
        }

        /// <summary>
        /// Chequeo según la valorizacion de admisión si hay Importe a Abonar por el Paciente
        /// </summary>
        /// <param name="turnoID">ID del Turno</param>
        /// <returns>True/False si el Importe Total a Abonar por el Paciente es 0</returns>
        [Private]
        public bool PacienteNoDebeAbonar(int turnoID, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionesDalc ValorizacionesDalc = Context.Session.ValorizacionesDalc;

            Entities.Valorizacion valorizacion = ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turnoID,
                                                                                                            (int)
                                                                                                            ValorizacionTiposEnum
                                                                                                                .
                                                                                                                Admision);


            if (valorizacion == null)
                return false;

            ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, valorizacion.Items);
            FullValorizacion fullValorizacion = new FullValorizacion(valorizacionInfo, modalidadCoseguro);

            return (valorizacionInfo.ImporteTotalPaciente == valorizacion.Turno.ImportePagado.GetValueOrDefault(0) || valorizacionInfo.ImporteTotalPaciente == 0);
        }

        /// <summary>
        /// Obtengo un Formulario Dado Asociado a un MovimientoCaja y su Tipo
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Obtener la Factura Interna</param>
        /// <param name="tipo">Tipo de Formulario a Retornar</param>
        /// <returns>Colección de Formularios del MovimientoCaja y el Tipo</returns>
        [Private]
        public EntityCollection<Formulario> FormulariosReadByMovimientoCajaAndTipo(int movimientoCajaID, int[] tipoIds)
        {
            string tipoList = Utils.EnumerableConvert.ToString(tipoIds);
            // Obtengo los Movimientos
            string hql = "select distinct f from MovimientoCajaItem mci, Formulario f "
                         + " where mci.FormularioID = f.Id and mci.MovimientoCaja.Id = :movimientoCajaId "
                         + " and mci.TipoMovimientoCajaItemID in " + tipoList
                         + " order by f.MedicoID";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("movimientoCajaId", movimientoCajaID);

            EntityCollection<Formulario> frms = dalEngine.GetManyByQuery<Formulario>(query);

            return frms;
        }

        /// <summary>
        /// Obtengo todos los formularios relacionados con el formulario original
        /// </summary>
        /// <param name="formularioOriginal">Id del formulario original</param>
        /// <returns>Colección de formularios relacionados con el formulario original</returns>
        public EntityCollection<Formulario> FormulariosReadByFormularioOriginalIds(List<int> formularioOriginals)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("select frm from Formulario frm ");
            hql.Append("where frm.FormularioOriginalID in (:formularioOriginals) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("formularioOriginals", formularioOriginals);

            EntityCollection<Formulario> frms = dalEngine.GetManyByQuery<Formulario>(query);

            return frms;
        }

        /// <summary>
        /// Creo una Cobranza Anticipada con su Factura, Pago, Movimientos
        /// </summary>
        /// <param name="factura">Datos de la Factura a Generar</param>
        /// <param name="pago">Datos del Pago a Generar</param>
        /// <param name="caja">Asignación de Caja del Pago</param>
        /// <param name="user">Usuario de la Operacion</param>
        /// <returns>El MovimientoCaja del Cobro Anticipado</returns>
        [RequiresTransaction]
        public virtual MovimientoCaja CobranzaAnticipadaCreate(Formulario factura, EntityCollection<Pago> pagos,
                                                     CajaUsuario caja, FacturaNumeracion facturaNumeracion)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;

            MovimientoCaja movimientoCaja = null;
            decimal importe = 0;

            // Obtengo los Tipo de Movimiento de Item
            TipoMovimientoCajaItem tipoMovimientoItemCobroAnticipo =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.CobroAnticipo);
            if (tipoMovimientoItemCobroAnticipo.CajaID.HasValue)
                tipoMovimientoItemCobroAnticipo.Caja = dalEngine.GetById<Caja>(tipoMovimientoItemCobroAnticipo.CajaID.Value);

            // Inserto los Medios de Pago
            for (int i = pagos.Count -1; i >=0; i--)
            {
                // Si es un medio de pago con importe 0 o negativo
                // => no se persiste, lo saco de la colleccion asi despues no molesta
                if (pagos[i].Importe <= 0)
                         pagos.RemoveAt(i);   
                else
                {
                    pagos[i] = this.PagoInsert(pagos[i]);
                    importe += pagos[i].Importe * (pagos[i].CotizacionDolar.HasValue ? pagos[i].CotizacionDolar.Value : 1);
                }
            }
            // Creo el Movimiento con los Items
            Movimiento movimiento = new Movimiento();

            // Creo el Movimiento
            movimientoCaja = new MovimientoCaja();

            // Asigno los Datos del Movimiento
            movimientoCaja.CajaUsuario = caja;
            movimientoCaja.CajaUsuarioID = caja.Id;
            movimientoCaja.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Anticipo;
            movimientoCaja.Descripcion = "Cobro Anticipado";
            movimientoCaja.ImporteIngreso = importe;
            movimientoCaja.ImporteEgreso = 0;

            if (facturaNumeracion != null)
                movimientoCaja.FacturaNumeracionId = facturaNumeracion.Id;

            // Guardo el Movimiento para luego Insertarlo
            movimiento.MovimientoCaja = movimientoCaja;

            // Guardo los Pagos para luego Insertarlos
            movimiento.Pagos = pagos;

            // Si Corresponde Creo el Item de la Factura
            if (factura.ImporteNeto > 0)
            {

                PuntoVenta pos = facturaNumeracion.PuntoVenta;
                if (pos == null)
                    pos = Context.Session.PuntoVentaDalc.PuntoVentaReadFirstByCajaAndEmpresaId(caja.Caja.Id, factura.Sucursal.GetValueOrDefault(0), factura.Empresa.Id);

                if (pos.UsaImpresoraFiscal == false)
                {
                    // Valido que el Número siga Siendo el Mismo
                    FacturaNumeracion numero =
                        FacturacionDalc.FacturaNumeracionReadByTalonarioTipoAndPuntoVenta((int)TipoTalonarioEnum.Factura,
                                                                                 factura.Clase,
                                                                                 pos.Id);
                    if (int.Parse("0" + factura.Numero) != numero.Numero)
                        throw new NotLoggeableException(
                            "El Número de Factura Ha sido Asignado a otra Cobraza. Debe Obtener uno Nuevo para Continuar.");

                    // Incremento la Númeracion
                    FacturacionDalc.FacturaNumeracionIncrementar(numero);
                }

                // Inserto la Factura
                factura = FacturacionDalc.FormularioInsert(factura, null);

                MovimientoCajaItem item = new MovimientoCajaItem();

                // Asigno los Datos del Item
                item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.CobroAnticipo;
                item.CajaImpacto = tipoMovimientoItemCobroAnticipo.Caja;
                item.CajaImpactoID = tipoMovimientoItemCobroAnticipo.CajaID;
                item.Descripcion = "Cobro Anticipado - Factura: " + factura.Clase + "-" +
                                   factura.Sucursal.Value.ToString().Trim().PadLeft(4, '0') + "-" +
                                   ((factura.Numero == null)
                                        ? "Factura Fiscal"
                                        : factura.Numero.Value.ToString().Trim().PadLeft(8, '0'));
                item.ImporteIngreso = Decimal.Add(factura.ImporteNeto, factura.ImporteIVA);
                item.ImporteEgreso = 0;
                item.FormularioID = factura.Id;

                // Guardo el Item para luego Insertarlo
                movimiento.Items.Add(item);

                TipoMovimientoCajaItem tipoMovimientoItem = dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.CobroAnticipo);
                this.AgregarItemsDePercepcionIIBB(factura, tipoMovimientoItem, movimiento);
            }

            // Inserto el Movimiento con sus Items Actualizando Saldos
            this.MovimientoCajaInsert(movimiento.MovimientoCaja, movimiento.Items, movimiento.Pagos, true);

            return movimientoCaja;
        }

        /// <summary>
        /// Creo un Deposito con su Factura, Pago, Movimientos
        /// </summary>
        /// <param name="turno">Turno del Depósito</param>
        /// <param name="factura">Datos de la Factura a Generar</param>
        /// <param name="pago">Datos del Pago a Generar</param>
        /// <param name="caja">Asignación de Caja del Pago</param>
        /// <param name="total">Marca si el Depósito es Total o No (Parcial)</param>
        /// <param name="user">Usuario de la Operacion</param>
        /// <returns>El Movimiento Creado</returns>
        [RequiresTransaction]
        public virtual MovimientoCaja DepositoCreate(Turno turno, Formulario factura, EntityCollection<Pago> pagos,
                                           CajaUsuario caja, bool total, FacturaNumeracion facturaNumeracion)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            bool actualizarTurno = false;
            decimal importe = 0;

            actualizarTurno = true;

            MovimientoCaja movimientoCaja = null;
            // Obtengo la Practica Principal del Turno
            EntityCollection<PracticaTurno> practicas = TurnosDalc.PracticaTurnoReadByTurno(turno.Id,
                                                                                            PracticaTurnoTipoEnum.
                                                                                                Principal);
            PracticaTurno practica = null;
            if (practicas.Count > 0)
                practica = practicas[0];
            else
                throw new Exception("No se Encontro la Práctica Principal del Turno [" + turno.Id.ToString() + "].");

            // Obtengo los Tipo de Movimiento de Item
            TipoMovimientoCajaItem tipoMovimientoItemDepositoTotal =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.DepositoTotal);
            if (tipoMovimientoItemDepositoTotal.CajaID.HasValue)
                tipoMovimientoItemDepositoTotal.Caja = dalEngine.GetById<Caja>((int)tipoMovimientoItemDepositoTotal.CajaID);
            TipoMovimientoCajaItem tipoMovimientoItemDepositoParcial =
                dalEngine.GetById<TipoMovimientoCajaItem>((int)TipoMovimientoCajaItemEnum.DepositoParcial);
            if (tipoMovimientoItemDepositoParcial.CajaID.HasValue)
                tipoMovimientoItemDepositoParcial.Caja =
                    dalEngine.GetById<Caja>((int)tipoMovimientoItemDepositoParcial.CajaID);

            // Inserto el Medio de Pago
            for (int i = 0; i < pagos.Count; i++)
            {
                pagos[i] = this.PagoInsert(pagos[i]);
                importe += pagos[i].Importe * (pagos[i].CotizacionDolar.HasValue ? pagos[i].CotizacionDolar.Value : 1);
            }
            // Creo el Movimiento con los Items
            Movimiento movimiento = new Movimiento();

            // Creo el Movimiento
            movimientoCaja = new MovimientoCaja();

            // Si no esta, obtengo el Paciente
            if (turno.Orden.Paciente == null)
                turno.Orden.Paciente = TurnosDalc.PacienteReadById(turno.Orden.PacienteId);

            // Asigno los Datos del Movimiento
            movimientoCaja.CajaUsuario = caja;
            movimientoCaja.CajaUsuarioID = caja.Id;
            movimientoCaja.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Deposito;
            if (total)
                movimientoCaja.Descripcion = "Depósito Total - Pract.: " + practica.Practica.Name + " - Prot.: " +
                                             turno.Orden.Protocolo.ProtocoloFull;
            else
                movimientoCaja.Descripcion = "Depósito Parcial - Pract.: " + practica.Practica.Name + " - Prot.: " +
                                             turno.Orden.Protocolo.ProtocoloFull;
            movimientoCaja.ImporteIngreso = importe;
            movimientoCaja.ImporteEgreso = 0;
            movimientoCaja.TurnoID = turno.Id;

            if (facturaNumeracion != null)
                movimientoCaja.FacturaNumeracionId = facturaNumeracion.Id;

            // Guardo el Movimiento para luego Insertarlo
            movimiento.MovimientoCaja = movimientoCaja;

            // Guardo los Pagos para luego insertarlos
            movimiento.Pagos = pagos;

            // Si Corresponde Creo el Item de la Factura
            if (factura.ImporteNeto > 0)
            {
                PuntoVenta pos = facturaNumeracion.PuntoVenta;
                if (pos == null)
                    pos = Context.Session.PuntoVentaDalc.PuntoVentaReadFirstByCajaAndEmpresaId(caja.Caja.Id, factura.Sucursal.GetValueOrDefault(0), factura.Empresa.Id);

                if (pos.UsaImpresoraFiscal == false)
                {
                    // Valido que el Número siga Siendo el Mismo
                    FacturaNumeracion numero = FacturacionDalc.FacturaNumeracionReadByTalonarioTipoAndPuntoVenta((int)TipoTalonarioEnum.Factura,
                                                                                 factura.Clase,
                                                                                 pos.Id);
                    if (int.Parse("0" + factura.Numero) != numero.Numero)
                        throw new NotLoggeableException(
                            "El Número de Factura Ha sido Asignado a otra Cobraza. Debe Obtener uno Nuevo para Continuar.");

                    // Incremento la Númeracion
                    FacturacionDalc.FacturaNumeracionIncrementar(numero);
                }

                // Inserto la Factura
                factura = FacturacionDalc.FormularioInsert(factura, turno);

                MovimientoCajaItem item = new MovimientoCajaItem();

                // Asigno los Datos del Item
                if (total)
                {
                    item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.DepositoTotal;
                    item.CajaImpacto = tipoMovimientoItemDepositoTotal.Caja;
                    item.CajaImpactoID = tipoMovimientoItemDepositoTotal.CajaID;
                    item.Descripcion = "Depósito Total - Factura: " + factura.Clase + "-" +
                                       factura.Sucursal.Value.ToString().Trim().PadLeft(4, '0') + "-" +
                                       (factura.Numero == null
                                            ? "Fiscal"
                                            : factura.Numero.Value.ToString().Trim().PadLeft(8, '0'));
                }
                else
                {
                    item.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.DepositoParcial;
                    item.CajaImpacto = tipoMovimientoItemDepositoParcial.Caja;
                    item.CajaImpactoID = tipoMovimientoItemDepositoParcial.CajaID;
                    item.Descripcion = "Depósito Parcial - Factura: " + factura.Clase + "-" +
                                       factura.Sucursal.Value.ToString().Trim().PadLeft(4, '0') + "-" +
                                       (factura.Numero == null
                                            ? "Fiscal"
                                            : factura.Numero.Value.ToString().Trim().PadLeft(8, '0'));
                }
                item.ImporteIngreso = Decimal.Add(factura.ImporteNeto,factura.ImporteIVA);
                item.ImporteEgreso = 0;
                item.FormularioID = factura.Id;

                // Guardo el Item para luego Insertarlo
                movimiento.Items.Add(item);
            }

            // Inserto el Movimiento con sus Items Actualizando Saldos
            this.MovimientoCajaInsert(movimiento.MovimientoCaja, movimiento.Items, movimiento.Pagos, true);

            // Relaciono el Turno con el Movimiento Creado
            if (actualizarTurno)
            {
                // Registro Log
                TurnosDalc.LogRegistrar((int)LogEventoEnum.ModificacionCobranzaVigente,
                                        "Se generó el depósito del turno.", turno.Id);
                TurnosDalc.TurnoUpdateCobranzaVigenteUpdate(turno.Id, movimientoCaja.Id, false);
            }

            return movimientoCaja;
        }

        /// <summary>
        /// Creo una Nota de Crédito con su Devolución, Pago, Movimientos
        /// </summary>
        /// <param name="movimientoOriginal">MovimientoCaja Original de la Nota de Crédito</param>
        /// <param name="turno">Turno de la Nota de Crédito</param>
        /// <param name="facturas">Datos de la Devolución por Nota de Crédito a Generar</param>
        /// <param name="recibos">Recibos Medicos de Cancelacion</param>
        /// <param name="pago">Datos del Pago a Generar</param>
        /// <param name="caja">Asignación de Caja del Pago</param>
        /// <param name="user">Usuario de la Operacion</param>
        /// <returns>El MovimientoCaja de la NC</returns>
        [Private]
        [RequiresTransaction]
        public virtual MovimientoCaja NotaDeCreditoCreate(MovimientoCaja movimientoOriginal, Turno turno,
                                                EntityCollection<Formulario> facturas,
                                                EntityCollection<ReciboMedico> recibos, EntityCollection<Pago> pagos,
                                                CajaUsuario caja, FacturaNumeracion facturaNumeracion)
        {
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;

            MovimientoCaja movimientoCaja = null;

            decimal importe = 0;

            Decimal importeEfectivo = 0;
            //Si se realizaron pagos
            if (pagos != null)
            {
                //Recorro todos los pagos
                foreach (Pago pag in pagos)
                {
                    //Si el pago es en medio de pago efectivo
                    if (pag.MedioPago.Id == (int)MedioPagoEnum.Efectivo)
                        importeEfectivo = pag.Importe;
                }
            }
            // Chequeo que la Caja tenga Saldo en efectivo sufuciente
            this.CajaUsuarioChequearSaldoEgreso(caja, importeEfectivo, true);

            // Inserto los Medios de Pago
            for (int i = 0; i < pagos.Count; i++)
            {
                pagos[i] = this.PagoInsert(pagos[i]);
                importe += pagos[i].Importe * (pagos[i].CotizacionDolar.HasValue ? pagos[i].CotizacionDolar.Value : 1);
            }

            // Creo el Movimiento con los Items
            Movimiento movimiento = new Movimiento();

            // Creo el Movimiento
            movimientoCaja = new MovimientoCaja();

            // Asigno los Datos del Movimiento
            movimientoCaja.CajaUsuario = caja;
            movimientoCaja.CajaUsuarioID = caja.Id;
            movimientoCaja.TipoMovimientoCajaID = (int)TipoMovimientoCajaEnum.Devolucion;
            movimientoCaja.Descripcion = "[Devolución] " + movimientoOriginal.Descripcion;
            movimientoCaja.ImporteIngreso = 0;
            movimientoCaja.ImporteEgreso = importe;
            if (turno != null)
                movimientoCaja.TurnoID = turno.Id;
            //movimientoCaja.PagoID = pago.Id;
            movimientoCaja.MovimientoOriginalID = movimientoOriginal.Id;
            if (facturaNumeracion != null)
                movimientoCaja.FacturaNumeracionId = facturaNumeracion.Id;

            // Guardo el Movimiento para luego Insertarlo
            movimiento.MovimientoCaja = movimientoCaja;

            // Guardo los Pagos para luego relacionarlos
            movimiento.Pagos = pagos;

            for (int i = 0; i < facturas.Count; i++)
            {
                // Si Corresponde Creo el Item de la Factura
                if (facturas[i].ImporteNeto > 0)
                {
                    // Si es una nota de crédito verifico la numeración
                    if (facturas[i].TipoFormularioID == (int)TipoFormularioEnum.NotaCredito)
                    {
                        PuntoVenta pos = Context.Session.PuntoVentaDalc.PuntoVentaReadFirstByCajaAndEmpresaId(caja.Caja.Id, facturas[i].Sucursal.GetValueOrDefault(0), facturas[i].Empresa.Id);
                        if (pos.UsaImpresoraFiscal == false)
                        {
                            // Valido que el Número siga Siendo el Mismo
                            FacturaNumeracion numero =
                                FacturacionDalc.FacturaNumeracionReadByTalonarioTipoAndPuntoVenta(
                                    (int)TipoTalonarioEnum.NotaDeCredito, facturas[i].Clase,
                                    pos.Id);
                            if (int.Parse("0" + facturas[0].Numero) != numero.Numero)
                                throw new NotLoggeableException(
                                    "El Número de Factura Ha sido Asignado a otra Cobranza. Debe Obtener uno Nuevo para Continuar.");

                            // Incremento la Númeracion
                            FacturacionDalc.FacturaNumeracionIncrementar(numero);
                        }
                    }

                    // Inserto la Factura
                    facturas[i] = FacturacionDalc.FormularioInsert(facturas[i], turno);
                }
            }

            // Inserto los Movimientos de Devolución y Anulación de los Items del Movimiento Original
            EntityCollection<MovimientoCajaItem> items = this.MovimientoCajaItemReadByMovimiento(movimientoOriginal.Id,
                                                                                                 false);
            foreach (MovimientoCajaItem item in items)
            {
                // Creo el Item de Recupero de Gastos
                MovimientoCajaItem nvoItem = new MovimientoCajaItem();

                // Metodo para buscar los id de los nuevos formularios insertados en base a la referencia de los items
                Predicate<Formulario> predicate =
                    delegate(Formulario compare) { return compare.FormularioOriginalID == item.FormularioID; };

                //si debe anular, sino debe devolver (esto debe hacerlo en teoría una vez...se debería basar en los importes del mov original y no en la factura)
                if (item.TipoMovimientoCajaItemID != (int)TipoMovimientoCajaItemEnum.CobroInterno
                    && item.TipoMovimientoCajaItemID != (int)TipoMovimientoCajaItemEnum.CobroAnticipo
                    && item.TipoMovimientoCajaItemID != (int)TipoMovimientoCajaItemEnum.DepositoParcial
                    && item.TipoMovimientoCajaItemID != (int)TipoMovimientoCajaItemEnum.DepositoTotal)
                {
                    // Asigno los Datos del Item
                    if (item.TipoMovimientoCajaItemID == (int)TipoMovimientoCajaItemEnum.CobroHonorarios)
                        nvoItem.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.DevolucionCobroHonorarios;
                    else if (item.TipoMovimientoCajaItemID == (int)TipoMovimientoCajaItemEnum.RecuperoGastos)
                        nvoItem.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.DevolucionRecuperoGastos;
                    else if ((TipoMovimientoCajaItemEnum)item.TipoMovimientoCajaItemID == TipoMovimientoCajaItemEnum.PercepcionIIBB)
                        nvoItem.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.DevolucionIIBB;

                    nvoItem.Descripcion = "Devolución NM: " + item.Descripcion;
                    nvoItem.ImporteIngreso = 0;
                    nvoItem.ImporteEgreso = Decimal.Subtract(item.ImporteIngreso, item.ImporteEgreso);
                }
                else
                {
                    Formulario formAux = facturas.Find(predicate);
                    // Asigno los Datos del Item
                    nvoItem.TipoMovimientoCajaItemID = (int)TipoMovimientoCajaItemEnum.Devolucion;

                    string nc = string.Empty;
                    if (formAux != null)
                        nc = formAux.Descripcion;

                    nvoItem.Descripcion = "Devolución NC: " + nc;
                    nvoItem.ImporteIngreso = 0;
                    nvoItem.ImporteEgreso = Decimal.Add(facturas[0].ImporteNeto, facturas[0].ImporteIVA);
                }

                Formulario form = facturas.Find(predicate);

                if (form.ImporteNeto > 0)
                {
                    nvoItem.CajaImpacto = item.CajaImpacto;
                    nvoItem.CajaImpactoID = item.CajaImpactoID;
                    nvoItem.FormularioID = form.Id;
                    // Guardo el Item para luego Insertarlo
                    movimiento.Items.Add(nvoItem);
                }
            }

            // Inserto el Movimiento con sus Items Actualizando Saldos
            this.MovimientoCajaInsert(movimiento.MovimientoCaja, movimiento.Items, movimiento.Pagos, true);

            // Anulo el Movimiento Original
            movimientoOriginal.FechaAnulacion = enfoke.IO.Time.Now;

            dalEngine.Update(movimientoOriginal);

            // Guardo los Recibos Medicos de Cancelacion
            if (recibos.Count > 0)
            {
                // Predicate para buscar entre las Facturas la NC
                Predicate<Formulario> predicate =
                    delegate(Formulario compare) { return compare.TipoFormularioID == (int)TipoFormularioEnum.NotaCredito; };

                Formulario NC = facturas.Find(predicate);

                // Actualizo la Fecha de Creacion y el Formulario Asociado
                foreach (ReciboMedico recibo in recibos)
                {
                    recibo.FechaCreacion = enfoke.IO.Time.Now;

                    // Predicate para buscar entre las Facturas la que tenga como formulario original el formulario del recibo
                    predicate = delegate(Formulario form) { return form.FormularioOriginalID == recibo.FormularioID; };

                    Formulario original = facturas.Find(predicate);
                    if (original == null)
                        original = NC;

                    if (original != null && original.Id > 0)
                        recibo.FormularioID = original.Id;
                }

                recibos = dalEngine.UpdateCollection<ReciboMedico>(recibos);
            }

            return movimientoCaja;
        }

        /// <summary>
        /// Busco el Movimiento de Devolución Asociado a un Movimiento
        /// </summary>
        /// <param name="movimiento">Movimiento Anulado con NC</param>
        /// <returns>Movimiento de la Nota de Crédito</returns>
        public MovimientoCaja NotaDeCreditoFindByMovimiento(int movimiento)
        {
            ReadManyCommand<MovimientoCaja> readCmd = new ReadManyCommand<MovimientoCaja>(dalEngine);

            Filter filter = new Filter();

            filter.Add(MovimientoCaja.Properties.MovimientoOriginalID, "=", movimiento);

            filter.Add(BooleanOp.And,
                       MovimientoCaja.Properties.FechaAnulacion,
                       "IS",
                       null);

            readCmd.Filter = filter;

            EntityCollection<MovimientoCaja> movimientos = readCmd.Execute();

            if (movimientos.Count > 0)
                return movimientos[0];
            return null;
        }


        #endregion

        #region Pago

        /// <summary>
        /// Inserto un Nuevo Pago
        /// </summary>
        /// <param name="formulario">Pago a Insertar</param>
        /// <param name="user">Usuario de la Operacion</param>
        private Pago PagoInsert(Pago pago)
        {
            //// Si paga con cheque, chequeo que no exista en el sistema
            //if (pago.MedioPagoID == (int)MedioPagoEnum.Cheque && this.ExisteCheque(pago.Numero, pago.BancoEmisor))
            //    throw new NotLoggeableException("El cheque ingresado ya se encuentra registrado en el sistema.");

            pago.Id = 0;


            // Inserto el Formulario
            pago = dalEngine.Update<Pago>(pago);

            return pago;
        }


        public EntityCollection<Pago> PagoReadByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            EntityCollection<Pago> pagos = new EntityCollection<Pago>();

            foreach (
                PagoMovimientoCaja pagoMovimientoCaja in this.PagoMovimientoCajaReadByMovimientoCaja(movimientoCaja))
            {
                pagoMovimientoCaja.Pago.MedioPago = dalEngine.GetById<MedioPago>(pagoMovimientoCaja.Pago.MedioPagoID);
                if (pagoMovimientoCaja.Pago.EntidadPagoID.HasValue)
                    pagoMovimientoCaja.Pago.EntidadPago =
                        dalEngine.GetById<EntidadPago>((int)pagoMovimientoCaja.Pago.EntidadPagoID);
                pagos.Add(pagoMovimientoCaja.Pago);
            }
            return pagos;
        }

        public void PagosDeleteByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            dalEngine.Delete(this.PagoReadByMovimientoCaja(movimientoCaja));
        }

        public void PagoMovimientoCajaDeleteByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            dalEngine.Delete(this.PagoMovimientoCajaReadByMovimientoCaja(movimientoCaja));
        }

        #endregion

        #region ReciboMedico
        /// <summary>
        /// Devuelve la cantidad de ReciboMedico que tienen seteado el campo rem_liq_aplica_descuento.
        /// Posibilita saber si se debe o no solicitar al usuario que ingrese un valor referente al
        /// porcentaje a aplicar de descuento en caso de retornar mayor a cero ésta consulta.
        /// </summary>
        /// <param name="hasta">Hasta que fecha se quieren buscar ReciboMedico sin una liquidación de honorarios.</param>
        /// <returns>Cantidad de ReciboMedico que aplican descuento en la liquidacion de honorarios Caja.</returns>
        [Private]
        public int CantidadReciboMedicoQueAplicaDescuento(DateTime hasta)
        {
            string hql = "SELECT COUNT(rm.Id) " +
                         "FROM ReciboMedico rm " +
                         "WHERE rm.FechaCreacion < :fechaHasta " +
                         "AND rm.LiquidacionHonorariosID IS NULL " +
                         "AND rm.FechaAnulacion IS NULL " +
                         "AND rm.LiquidacionAplicaDescuento = true";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fechaHasta", hasta.AddDays(1));
            query.SetMaxResults(1);

            Object result = query.UniqueResult<Object>();

            if (result != null)
                return 0;
            else
                return (int)result;
        }

        /// <summary>
        /// Inserto un Nuevo Recibo Medico
        /// </summary>
        /// <param name="formulario">Recibo Medico a Insertar</param>
        /// <param name="user">Usuario de la Operacion</param>
        private ReciboMedico ReciboMedicoInsert(ReciboMedico recibo)
        {
            recibo.FechaCreacion = enfoke.IO.Time.Now;


            // Inserto el Formulario
            return dalEngine.Update<ReciboMedico>(recibo);
        }

        /// <summary>
        /// Obtengo los Recibos Medicos asociados a un MovimientoCaja
        /// </summary>
        /// <param name="movimiento">MovimientoCaja a Obtener la Factura Interna</param>
        /// <param name="tipo">Tipo de Formulario a Retornar</param>
        /// <returns>Colección de Formularios del MovimientoCaja y el Tipo</returns>
        public EntityCollection<ReciboMedico> ReciboMedicoReadByMovimientoCaja(int movimientoID)
        {
            string hql = "SELECT DISTINCT new ReciboMedico(rm, f.ImporteRedondeo) FROM ReciboMedico rm, Formulario f, MovimientoCajaItem mci " +
                         "WHERE rm.FormularioID = f.Id AND mci.FormularioID = f.Id " +
                         "AND mci.MovimientoCaja.Id = :movimientoID " +
                         "ORDER BY rm.MedicoID ASC ";

            IQuery query = dalEngine.CreateQuery(hql);

            query.SetParameter("movimientoID", movimientoID);

            EntityCollection<ReciboMedico> retorno = dalEngine.GetManyByQuery<ReciboMedico>(query);

            MedicosDalc MedicosDalc = Context.Session.MedicosDalc;
            FacturacionDalc FacturacionDalc = Context.Session.FacturacionDalc;
            // Obtengo los Objetos
            foreach (ReciboMedico recibo in retorno)
            {
                recibo.Medico = MedicosDalc.MedicoReadById(recibo.MedicoID);
                recibo.CondicionIVA = dalEngine.GetById<CondicionIVA>(recibo.CondicionIVAID);
            }

            return retorno;
        }

        [Private]
        public EntityCollection<ReciboMedico> ReciboMedicoReadNoAnuladosByTurno(int turnoID)
        {
            ReadManyCommand<ReciboMedico> readCmd = new ReadManyCommand<ReciboMedico>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, ReciboMedico.Properties.TurnoID,
                       "=", turnoID);

            filter.Add(BooleanOp.And, ReciboMedico.Properties.FechaAnulacion,
                       " IS ",
                       null);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        #endregion

        #region PuntoVenta

        public EntityCollection<Caja> CajaReadByPuntoVenta(PuntoVenta puntoVenta)
        {
            PuntoVentaDalc posDalc = Context.Session.PuntoVentaDalc;
            EntityCollection<CajaPuntoVenta> bposCollection = posDalc.ObtenerCajaPuntoVentaPorPuntoVenta(puntoVenta.Id);
            List<int> boxIds = new List<int>();
            foreach (CajaPuntoVenta bpos in bposCollection)
                boxIds.Add(bpos.CajaId);

            return dalEngine.GetManyByIds<Caja>(boxIds);
        }

        #endregion

        public EntityCollection<CajaPuntoVenta> CajaPuntoVentaReadByCajaEquipoOSPlan(int cajaId, int equipoId, int obraSocialPlanId)
        {
            ObrasSocialesDalc dalc = Context.Session.ObrasSocialesDalc;
            ObraSocialPlan osp = dalc.ObraSocialPlanReadById(obraSocialPlanId);
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select distinct cpv from Caja caj, CajaPuntoVenta cpv where ");
            hqlBuilder.Append("caj.Id = :caja ");
            hqlBuilder.Append("and caj.Id = cpv.CajaId ");

            hqlBuilder.Append(osp.ObraSocial.EsParticular
                      ? "and cpv.PuntoVenta.Particular = true "
                      : "and cpv.PuntoVenta.NoParticular = true ");

            hqlBuilder.Append("and :equipo not in ");
            hqlBuilder.Append("(Select pve.Equipo.Id from PuntoVentaEquipo pve ");
            hqlBuilder.Append("Where cpv.PuntoVenta.Id = pve.PuntoVentaId and (cpv.PuntoVenta.Particular = true or cpv.PuntoVenta.NoParticular = true)) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("equipo", equipoId);
            query.SetInt32("caja", cajaId);

            EntityCollection<CajaPuntoVenta> puntosVenta = dalEngine.GetManyByQuery<CajaPuntoVenta>(query);
            return puntosVenta;
        }

        public EntityCollection<CajaPuntoVenta> CajaPuntoVentaReadByCajaEquipoOSPlanEmpresa(int cajaId, int equipoId, int obraSocialPlanId, int empresaId)
        {
            EntityCollection<CajaPuntoVenta> parcial = CajaPuntoVentaReadByCajaEquipoOSPlan(cajaId, equipoId, obraSocialPlanId);
            EntityCollection<CajaPuntoVenta> result = new EntityCollection<CajaPuntoVenta>();
            foreach (CajaPuntoVenta bpos in parcial)
            {
                if (bpos.PuntoVenta.Empresa.Id == empresaId)
                    result.Add(bpos);
            }
            return result;
        }

        public EntityCollection<CajaPuntoVenta> CajaPuntoVentaReadByCajaYSector(int cajaId, int sectorId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select cpv from CajaPuntoVenta cpv, PuntoVentaSector pvs where ");
            hqlBuilder.Append("cpv.CajaId = :caja ");
            hqlBuilder.Append("and cpv.PuntoVenta.Id = pvs.PuntoVenta.Id ");
            hqlBuilder.Append(" and pvs.Sector.Id = :sector");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("sector", sectorId);
            query.SetInt32("caja", cajaId);

            return dalEngine.GetManyByQuery<CajaPuntoVenta>(query);
        }

        public EntityCollection<CajaPuntoVenta> CajaPuntoVentaReadByEquipoAndCaja(int cajaId, int equipoId, EntityCollection<CajaPuntoVenta> cajaPuntosVenta)
        {
            List<int> cpvIds = new List<int>();
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select distinct cpv from PuntoVentaEquipo pve, CajaPuntoVenta cpv where ");
            hqlBuilder.Append("pve.Equipo.Id = :equipo ");
            hqlBuilder.Append("and cpv.PuntoVenta.Id = pve.PuntoVentaId ");
            hqlBuilder.Append(" and cpv.CajaId = :caja");
            if (cajaPuntosVenta != null && cajaPuntosVenta.Count > 0)
            {
                hqlBuilder.Append(" and cpv.Id IN (:cpvs)");

                foreach (CajaPuntoVenta cajaPuntoVenta in cajaPuntosVenta)
                {
                    cpvIds.Add(cajaPuntoVenta.Id);
                }
            }

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("equipo", equipoId);
            query.SetInt32("caja", cajaId);

            if (cpvIds.Count > 0)
            {
                query.SetParameterList("cpvs", cpvIds);
            }

            return dalEngine.GetManyByQuery<CajaPuntoVenta>(query);
        }

        public EntityCollection<CajaPuntoVenta> CajaPuntoVentaReadByCaja(int cajaId)
        {
            return dalEngine.GetManyByProperty<CajaPuntoVenta>(CajaPuntoVenta.Properties.CajaId,
                                                                   cajaId);
        }

        #region PagoMovimientoCaja

        public EntityCollection<PagoMovimientoCaja> PagoMovimientoCajaReadByMovimientoCaja(MovimientoCaja movimientoCaja)
        {
            return dalEngine.GetManyByProperty<PagoMovimientoCaja>(PagoMovimientoCaja.Properties.MovimientoCaja,
                                                                   movimientoCaja);
        }






        #endregion

        #region SecurityUsers

        public EntityCollection<SecurityUser> SecurityUsersSectorCajaReadByCaja(int cajaId)
        {
            string hql = "select distinct su.SecurityUser from SecurityUserSector su, Caja c, SectorTipoSector sts where su.Sector = sts.Sector AND sts.TipoSector.Id = :tipoCaja AND (c.Sucursal = su.Sector.Sucursal or c.Sucursal is null)"
                         + " and c.Id = :cajaId  order by su.SecurityUser.LastName ASC";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaId", cajaId);
            query.SetParameter("tipoCaja", (int)TipoSectorEnum.Caja);
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        public EntityCollection<SecurityUser> SecurityUsersReadNoAsignados(Caja caja)
        {
            string hql = "select distinct su.SecurityUser from SecurityUserSector su, Caja c where c.Sucursal = su.Sector.Sucursal"
                         +
                         " and c.Id = :cajaId and not exists (select cu from CajaUsuario cu where cu.Usuario = su.SecurityUser "
                         + " and cu.FechaCierre is null) order by su.SecurityUser.LastName ASC";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("cajaId", caja.Id);
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        #endregion

        #region Banco

        public EntityCollection<Banco> BancoReadAll()
        {
            return BancoReadAll(null);
        }
        public EntityCollection<Banco> BancoReadAll(bool? eliminado)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from Banco bco ");

            if (eliminado.HasValue == true)
                hql.Append("where bco.Deleted = :eliminado ");

            hql.Append("order by bco.Descripcion desc ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (eliminado.HasValue == true)
                query.SetParameter("eliminado", eliminado);

            return dalEngine.GetManyByQuery<Banco>(query);
        }






        #endregion

        public EntityCollection<ItemFormularioView> ItemFormularioViewReadByFormularioId(int idFormulario)
        {
            return dalEngine.GetManyByProperty<ItemFormularioView>(ItemFormularioView.Properties.Id, idFormulario);
        }


        public EntityCollection<CajaUsuario> GetAllCajaUsuarioBySucursalIdAndUserId(int sucursalId, int userId)
        {
            EntityCollection<CajaUsuario> cajasUsuario = (from sucursal in dalEngine.Query<Sucursal>()
                                                          join
                                                              puntoVenta in dalEngine.Query<PuntoVenta>()
                                                              on sucursal.Id equals puntoVenta.Sucursal.Id
                                                          join
                                                              cajaPuntoVenta in dalEngine.Query<CajaPuntoVenta>()
                                                              on puntoVenta.Id equals cajaPuntoVenta.PuntoVenta.Id
                                                          join
                                                              cajaUsuario in dalEngine.Query<CajaUsuario>()
                                                              on cajaPuntoVenta.CajaId equals cajaUsuario.Caja.Id
                                                          where sucursal.Id == sucursalId && cajaUsuario.Usuario.Id == userId
                                                          select cajaUsuario).ToEntityCollection<CajaUsuario>();
            return cajasUsuario;
        }

        public Dictionary<Sucursal, EntityCollection<CajaUsuario>> GetAllSucursalesCajaUsuarios()
        {
            List<KeyValuePair<Sucursal, CajaUsuario>> sucursalCajaUsuario =
                        (from sucursal in dalEngine.Query<Sucursal>()
                         join
                             puntoVenta in dalEngine.Query<PuntoVenta>()
                             on sucursal.Id equals puntoVenta.Sucursal.Id
                         join
                             cajaPuntoVenta in dalEngine.Query<CajaPuntoVenta>()
                             on puntoVenta.Id equals cajaPuntoVenta.PuntoVenta.Id
                         join
                             cajaUsuario in dalEngine.Query<CajaUsuario>()
                             on cajaPuntoVenta.CajaId equals cajaUsuario.Caja.Id
                         select new KeyValuePair<Sucursal, CajaUsuario>(sucursal, cajaUsuario)).ToList();
            EntityCollection<Sucursal> todasSucursales = Context.Session.Dalc.GetAll<Sucursal>();
            Dictionary<Sucursal, EntityCollection<CajaUsuario>> diccSucursalesCajasUsuarios = new Dictionary<Sucursal, EntityCollection<CajaUsuario>>();

            foreach (Sucursal sucursal in todasSucursales)
            {
                EntityCollection<CajaUsuario> cajasUsuarios = GetAllCajaUsuarioBySucursal(sucursalCajaUsuario, sucursal);
                diccSucursalesCajasUsuarios.Add(sucursal, cajasUsuarios);
            }

            return diccSucursalesCajasUsuarios;
        }

        private EntityCollection<CajaUsuario> GetAllCajaUsuarioBySucursal(List<KeyValuePair<Sucursal, CajaUsuario>> sucursalCajaUsuario, Sucursal sucursal)
        {
            EntityCollection<CajaUsuario> cajasUsuarios = new EntityCollection<CajaUsuario>();
            foreach (KeyValuePair<Sucursal, CajaUsuario> sucCajaUsuario in sucursalCajaUsuario)
                if (sucCajaUsuario.Key.Id == sucursal.Id)
                    cajasUsuarios.Add(sucCajaUsuario.Value);
            return cajasUsuarios;
        }

        public EntityCollection<Conciliacion> GetConciliacionByFilters(DateTime? fechaDesde, DateTime? fechaHasta, DateTime? fechaRendicion, List<MedioPago> mediosPago, List<Sucursal> sucursales, Caja caja, SecurityUser cajero, bool pendientes, bool rechazados, bool aceptados)
        {
            //la consulta que genera las conciliaciones filtra x Sucursal FechaDesde, FechaHasta, FechaRendicion, Pendientes, Rechazados, Aceptados.
            List<int> estadosConciliacionesIds = GetEstadoConciliacionIdsByFilter(pendientes, rechazados, aceptados);
            List<int> sucursalesIds = new List<int>();
            if (sucursales != null && sucursales.Count > 0)
                sucursalesIds = (from suc in sucursales select suc.Id).ToList();

            EntityCollection<Conciliacion> conciliaciones = GetConciliaciones(fechaDesde, fechaHasta, fechaRendicion, estadosConciliacionesIds, sucursalesIds);

            //la consulta que genera el DETALLE de las conciliaciones filtra por Caja, Cajero, MediosPago
            if (conciliaciones.Count > 0)
            {
                List<int> formulariosIds = GetFormulariosIds(conciliaciones);
                List<int> mediosPagosIds = GetMediosPagosIds(mediosPago);
                EntityCollection<ConciliacionDetalle> conciliacionDetalles = GetConciliacionDetalle(caja, cajero, formulariosIds, mediosPagosIds);

                conciliaciones = MergeDetallesWithConciliaciones(conciliaciones, conciliacionDetalles);
            }

            return conciliaciones;
        }

        private EntityCollection<Conciliacion> MergeDetallesWithConciliaciones(EntityCollection<Conciliacion> conciliaciones, EntityCollection<ConciliacionDetalle> conciliacionDetalles)
        {
            EntityCollection<Conciliacion> conciliacionesConDetalle = new EntityCollection<Conciliacion>();
            foreach (Conciliacion conciliacion in conciliaciones)
            {
                EntityCollection<ConciliacionDetalle> detalles = FilterDetallesByConciliacion(conciliacion, conciliacionDetalles);
                if (detalles.Count > 0)
                {
                    //esto hay que hacerlo aca x como se trae la data, se pasa el ingreso a egreso
                    if (conciliacion.EsNotaCredito)
                        SetIngresosAsEgresos(detalles);

                    conciliacion.AddDetalles(detalles);
                    conciliacionesConDetalle.Add(conciliacion);
                }
            }
            return conciliacionesConDetalle;
        }

        private void SetIngresosAsEgresos(EntityCollection<ConciliacionDetalle> detalles)
        {
            foreach(ConciliacionDetalle detalle in detalles)
            {
                detalle.ImporteEgreso += detalle.ImporteIngreso;
                detalle.ImporteIngreso = 0;
            }
        }

        private EntityCollection<ConciliacionDetalle> FilterDetallesByConciliacion(Conciliacion conciliacion, EntityCollection<ConciliacionDetalle> conciliacionDetalles)
        {
            EntityCollection<ConciliacionDetalle> detallesFiltrados = new EntityCollection<ConciliacionDetalle>();
            foreach (ConciliacionDetalle cd in conciliacionDetalles)
                if (cd.ConciliacionId == conciliacion.Id)
                    detallesFiltrados.Add(cd);
            return detallesFiltrados;
        }

        private EntityCollection<ConciliacionDetalle> GetConciliacionDetalle(Caja caja, SecurityUser cajero, List<int> formulariosIds, List<int> mediosPagosIds)
        {
            if (formulariosIds.Count <= 0)
                throw new enfokeTrappedException("Busqueda de detalle sin formularios asociados");

            int tipoMCIPercepcionIIBB = (int)TipoMovimientoCajaItemEnum.PercepcionIIBB;
            int tipoMCIDevolucionIIBB = (int)TipoMovimientoCajaItemEnum.DevolucionIIBB;

            IQueryable<ConciliacionDetalle> queryConciliacionDetalle = (from mci in dalEngine.Query<MovimientoCajaItem>()
                                                                        join mc in dalEngine.Query<MovimientoCaja>()
                                                                        on mci.MovimientoCaja.Id equals mc.Id
                                                                        join cu in dalEngine.Query<CajaUsuario>()
                                                                        on mc.CajaUsuarioID equals cu.Id
                                                                        join pmc in dalEngine.Query<PagoMovimientoCaja>()
                                                                        on mc.Id equals pmc.MovimientoCaja.Id
                                                                        join pago in dalEngine.Query<PagoHQL>()
                                                                        on pmc.Pago.Id equals pago.Id
                                                                        where
                                                                           (mci.FormularioID != null) &&
                                                                           (cu.FechaCierre != null) &&
                                                                           (mci.TipoMovimientoCajaItemID != tipoMCIPercepcionIIBB) &&
                                                                           (mci.TipoMovimientoCajaItemID != tipoMCIDevolucionIIBB) &&
                                                                           formulariosIds.Contains(mci.FormularioID.Value)
                                                                        select new ConciliacionDetalle() { ConciliacionId = mci.FormularioID.Value, CajaId = cu.Caja.Id, Caja = cu.Caja.Name, CajeroId = cu.Usuario.Id, Cajero = cu.Usuario.Name, MedioPagoId = pago.MedioPago.Id, EntidadPago = pago.EntidadPago, MovimientoCajaId = mc.Id, ImporteIngreso = pago.Importe });
            
            //queryConciliacionDetalle = LinqInClause.AddInConstrains<ConciliacionDetalle>(formulariosIds, ConciliacionDetalle.Properties.ConciliacionId, queryConciliacionDetalle);

            if (caja != null)
                queryConciliacionDetalle = queryConciliacionDetalle.Where(conciliacion => conciliacion.CajaId == caja.Id);

            if (cajero != null)
                queryConciliacionDetalle = queryConciliacionDetalle.Where(conciliacion => conciliacion.CajeroId == cajero.Id);

            if (mediosPagosIds != null && mediosPagosIds.Count > 0)
                queryConciliacionDetalle = queryConciliacionDetalle.Where(conciliacion => mediosPagosIds.Contains(conciliacion.MedioPagoId));

            EntityCollection<ConciliacionDetalle> conciliacionDetalle = new EntityCollection<ConciliacionDetalle>(queryConciliacionDetalle);

            return conciliacionDetalle;
        }

        private EntityCollection<Conciliacion> GetConciliaciones(DateTime? fechaDesde, DateTime? fechaHasta, DateTime? fechaRendicion, List<int> estadosConciliacionesIds, List<int> sucursalesIds)
        {
            //trae conciliaciones con turno
            IQueryable<Conciliacion> queryConciliacionesConTurno = (from formulario in dalEngine.Query<Formulario>()
                                                                    join turno in dalEngine.Query<Turno>()
                                                                        on formulario.TurnoID.Value equals turno.Id
                                                                    join puntoVenta in dalEngine.Query<PuntoVenta>()
                                                                        on new { emp = formulario.Empresa.Id, nfs = formulario.Sucursal.Value }
                                                                          equals new { emp = puntoVenta.Empresa.Id, nfs = puntoVenta.NumeroFiscal }
                                                                    join sucursal in dalEngine.Query<Sucursal>()
                                                                        on puntoVenta.Sucursal.Id equals sucursal.Id
                                                                    where
                                                                        formulario.EstadoConciliacionId != null
                                                                    select new Conciliacion() { FormularioId = formulario.Id, ImporteTotal = formulario.ImporteNeto + formulario.ImporteIVA + formulario.ImporteIIBB, SucursalId = sucursal.Id, SucursalName = sucursal.Name, TipoFormularioId = formulario.TipoFormularioID, EstadoConciliacionId = formulario.EstadoConciliacionId.Value, Protocolo = turno.Orden.Protocolo.ProtocoloFull, RazonSocial = formulario.RazonSocial, Observaciones = formulario.Observaciones, FechaFactura = formulario.CreateDate, FechaRendicion = formulario.FechaRendicion, EnviadaERP = formulario.EnvioERP, TipoFactura = formulario.Clase, NumeroFiscalFactura = formulario.Sucursal, TalonarioFactura = formulario.Numero, ConceptoItem = formulario.Concepto });
            //trae conciliaciones sin turno
            IQueryable<Conciliacion> queryConciliacionesSinTurno = (from formulario in dalEngine.Query<Formulario>()
                                                                    join puntoVenta in dalEngine.Query<PuntoVenta>()
                                                                        on new { emp = formulario.Empresa, nfs = formulario.Sucursal.Value }
                                                                            equals new { emp = puntoVenta.Empresa, nfs = puntoVenta.NumeroFiscal }
                                                                    join sucursal in dalEngine.Query<Sucursal>()
                                                                        on puntoVenta.Sucursal.Id equals sucursal.Id
                                                                    where
                                                                        formulario.EstadoConciliacionId != null &&
                                                                        formulario.TurnoID == null
                                                                    select new Conciliacion() { FormularioId = formulario.Id, ImporteTotal = formulario.ImporteNeto + formulario.ImporteIVA + formulario.ImporteIIBB, SucursalId = sucursal.Id, SucursalName = sucursal.Name, TipoFormularioId = formulario.TipoFormularioID, EstadoConciliacionId = formulario.EstadoConciliacionId.Value, Protocolo = null, RazonSocial = formulario.RazonSocial, Observaciones = formulario.Observaciones, FechaFactura = formulario.CreateDate, FechaRendicion = formulario.FechaRendicion, EnviadaERP = formulario.EnvioERP, TipoFactura = formulario.Clase, NumeroFiscalFactura = formulario.Sucursal, TalonarioFactura = formulario.Numero, ConceptoItem = formulario.Concepto });


            if (estadosConciliacionesIds.Count > 0)
            {
                queryConciliacionesConTurno = queryConciliacionesConTurno.Where(conciliacion => estadosConciliacionesIds.Contains(conciliacion.EstadoConciliacionId));
                queryConciliacionesSinTurno = queryConciliacionesSinTurno.Where(conciliacion => estadosConciliacionesIds.Contains(conciliacion.EstadoConciliacionId));
            }

            if (sucursalesIds.Count > 0)
            {
                queryConciliacionesConTurno = queryConciliacionesConTurno.Where(conciliacion => sucursalesIds.Contains(conciliacion.SucursalId.Value));
                queryConciliacionesSinTurno = queryConciliacionesSinTurno.Where(conciliacion => sucursalesIds.Contains(conciliacion.SucursalId.Value));
            }

            if (fechaDesde.HasValue)
            {   //para hacer filtro a nivel de dia sobre la fecha! =)
                DateTime fechaDesdeNivelDia = fechaDesde.Value.Date;
                queryConciliacionesConTurno = queryConciliacionesConTurno.Where(conciliacion => fechaDesdeNivelDia <= conciliacion.FechaFactura);
                queryConciliacionesSinTurno = queryConciliacionesSinTurno.Where(conciliacion => fechaDesdeNivelDia <= conciliacion.FechaFactura);
            }

            if (fechaHasta.HasValue)
            {   //para hacer filtro a nivel de dia sobre la fecha! =)
                DateTime fechaHastaNivelDia = fechaHasta.Value.Date.AddDays(1.0);

                queryConciliacionesConTurno = queryConciliacionesConTurno.Where(conciliacion => conciliacion.FechaFactura < fechaHastaNivelDia);
                queryConciliacionesSinTurno = queryConciliacionesSinTurno.Where(conciliacion => conciliacion.FechaFactura < fechaHastaNivelDia);
            }

            if (fechaRendicion.HasValue)
            {
                DateTime fechaRendicionDesdeNivelDia = fechaRendicion.Value.Date;
                DateTime fechaRendicionHastaNivelDia = fechaRendicion.Value.Date.AddDays(1.0);
                queryConciliacionesConTurno = queryConciliacionesConTurno.Where(conciliacion => fechaRendicionDesdeNivelDia <= conciliacion.FechaRendicion && conciliacion.FechaRendicion < fechaRendicionHastaNivelDia);
                queryConciliacionesSinTurno = queryConciliacionesSinTurno.Where(conciliacion => fechaRendicionDesdeNivelDia <= conciliacion.FechaRendicion && conciliacion.FechaRendicion < fechaRendicionHastaNivelDia);
            }

            queryConciliacionesConTurno.Take(200);
            queryConciliacionesSinTurno.Take(200);

            EntityCollection<Conciliacion> conciliaciones = queryConciliacionesConTurno.ToEntityCollection();
            conciliaciones.AddRange(queryConciliacionesSinTurno);

            return conciliaciones;
        }

        private List<int> GetMediosPagosIds(List<MedioPago> mediosPago)
        {
            List<int> mediosPagosIds = new List<int>();
            foreach (MedioPago mp in mediosPago)
            {
                mediosPagosIds.Add(mp.Id);
            }
            return mediosPagosIds;
        }

        private List<int> GetFormulariosIds(EntityCollection<Conciliacion> conciliaciones)
        {
            List<int> formulariosIds = new List<int>();
            foreach (Conciliacion conciliacion in conciliaciones)
                formulariosIds.Add(conciliacion.FormularioId);
            return formulariosIds;
        }

        private List<int> GetEstadoConciliacionIdsByFilter(bool pendientes, bool rechazados, bool aceptados)
        {
            List<int> estadoConciliacionIds = new List<int>();
            if (pendientes)
                estadoConciliacionIds.Add((int)EstadoConciliacionEnum.Pendiente);
            if (rechazados)
                estadoConciliacionIds.Add((int)EstadoConciliacionEnum.Rechazado);
            if (aceptados)
                estadoConciliacionIds.Add((int)EstadoConciliacionEnum.Aceptado);
            return estadoConciliacionIds;
        }

        private List<int> GetIdsFromSucursales(List<Sucursal> sucursales)
        {
            List<int> sucursalesIds = new List<int>();
            foreach (Sucursal sucursal in sucursales)
                sucursalesIds.Add(sucursal.Id);
            return sucursalesIds;
        }

        public EntityCollection<DateTime> GetAllFechaRendiciones()
        {
            IQueryable<DateTime> queryAllFechas = (from formulario in dalEngine.Query<Formulario>()
                                                   where formulario.FechaRendicion != null
                                                   select formulario.FechaRendicion.Value);
            queryAllFechas = queryAllFechas.Distinct();
            EntityCollection<DateTime> allFechas = queryAllFechas.ToEntityCollection();
            return allFechas;
        }

        [RequiresTransaction]
        public virtual void CambiarEstadoFormulariosAsociadosACajaUsario(CajaUsuario cajaUsuario, EstadoConciliacionEnum estadoConciliacionEnum)
        {
            EntityCollection<Formulario> formularios = (from formulario in dalEngine.Query<Formulario>()
                                                        join mci in dalEngine.Query<MovimientoCajaItem>()
                                                        on formulario.Id equals mci.FormularioID
                                                        join mc in dalEngine.Query<MovimientoCaja>()
                                                        on mci.MovimientoCaja.Id equals mc.Id
                                                        join cu in dalEngine.Query<CajaUsuario>()
                                                        on mc.CajaUsuarioID equals cu.Id
                                                        where
                                                            (mci.FormularioID != null) &&
                                                            cu.Id == cajaUsuario.Id
                                                        select formulario).ToEntityCollection();

            foreach (Formulario formulario in formularios)
                formulario.EstadoConciliacionId = (int)estadoConciliacionEnum;

            Context.Session.Dalc.UpdateCollection<Formulario>(formularios);
        }
    }
}
