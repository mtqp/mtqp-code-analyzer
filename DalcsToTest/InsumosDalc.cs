using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using System.Linq;

using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;
using enfoke.Eges.Utils;

using NHibernate;
using enfoke.Data;
using enfoke.Eges.Auditoria;
using enfoke.Data.Filters;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class InsumosDalc : Dalc, IService
    {
        protected InsumosDalc(NotConstructable dummy) : base(dummy) { }

        #region Insumo
        public ReadAllCollection<Insumo> InsumoReadAll()
        {
            return new ReadAllCollection<Insumo>(dalEngine.GetAll<Insumo>(Insumo.Properties.Name));
        }

        //[Private]
        public ReadAllCollection<Insumo> InsumoReadAll(bool? deleted)
        {
            return InsumoReadAll(String.Empty, deleted);
        }

        /// <summary>
        /// Retorno todos los insumos
        /// </summary>
        /// <param name="name">Nombre de los insumos a buscar</param>
        /// <param name="deleted">Marca si traigo eliminadas o no [Null -> Todas | T/F -> Deleted = marca]</param>
        /// <returns>Listado de Insumos</returns>
        public ReadAllCollection<Insumo> InsumoReadAll(string name, bool? deleted)
        {
            Filter filter = new Filter();
            filter.Add(Insumo.Properties.Name, "LIKE", "%" + name + "%");
            if (deleted.HasValue)
                filter.Add(BooleanOp.And, Insumo.Properties.Deleted, "=", deleted.Value);
            return new ReadAllCollection<Insumo>(dalEngine.GetManyByFilter<Insumo>(filter));
        }

        public void InsumoPrecioUpdate(InsumoPrecio insumoPrecio, EntityCollection<InsumoPrecio> insumosPrecio)
        {
            InsumoPrecioUpdate(insumoPrecio, false, false, insumosPrecio);
        }

        public void InsumoPrecioFinalizar(InsumoPrecio insumoPrecio, DateTime fechaHasta)
        {
            // Seteo la Fecha Hasta
            insumoPrecio.FechaHasta = fechaHasta;

            // Actualizo
            InsumoPrecioUpdate(insumoPrecio, true, false, null);
        }

        public void UnidadArancelariaPlanEliminar(InsumoPrecio insumoPrecio)
        {
            // Marco como Eliminado
            insumoPrecio.Deleted = true;

            // Actualizo
            InsumoPrecioUpdate(insumoPrecio, false, true, null);
        }

        [RequiresTransaction]
        protected virtual void InsumoPrecioUpdate(InsumoPrecio insumoPrecio, bool finalizar, bool eliminar, EntityCollection<InsumoPrecio> insumosPrecio)
        {
            PracticasDalc praDalc = Context.Session.PracticasDalc;
            SecurityUser user = Security.Current.UserInfo.User;

            if (insumosPrecio == null || insumosPrecio.Count == 0)
                insumosPrecio = this.InsumoPrecioReadByInsumo(insumoPrecio.Insumo.Id);

            EntityCollection<InsumoPrecio> insumosMod = new EntityCollection<InsumoPrecio>();

            if (!eliminar)
            {

                if ((insumosPrecio == null || insumosPrecio.Count == 0))
                {
                    insumosPrecio = new EntityCollection<InsumoPrecio>();
                }
                // Obtengo las modificaciones y las Agrego a la coleccion de modificados
                insumosMod.AddRange(VigenciaUtils<InsumoPrecio>.ObtenerModificaciones(insumosPrecio, insumoPrecio, finalizar, user));
            }
            else
            {
                // Audito
                Audit.AuditDelete(insumoPrecio, user.Id);

                // Agrego a la coleccion de modificados
                insumosMod.Add(insumoPrecio);
            }


            // Actualizo
            insumosMod = dalEngine.UpdateCollection<InsumoPrecio>(insumosMod);

        }
        #endregion

        #region InsumoCategoria
        public ReadAllCollection<InsumoCategoria> InsumoCategoriaReadAll()
        {
            return new ReadAllCollection<InsumoCategoria>(dalEngine.GetAll<InsumoCategoria>(InsumoCategoria.Properties.Name));
        }


        public EntityCollection<InsumoCategoria> InsumoCategoriaReadByNames(IList<string> names)
        {
            return (from categoria in dalEngine.Query<InsumoCategoria>() where names.Contains(categoria.Name.TrimEnd()) select categoria).ToEntityCollection();
        }

        #endregion

        public ReadAllCollection<InsumoUnidad> InsumoUnidadReadAll()
        {
            return new ReadAllCollection<InsumoUnidad>(dalEngine.GetAll<InsumoUnidad>(InsumoUnidad.Properties.Name));
        }

        public InsumoPrecio InsumoPrecioReadByInsumoVigente(int insumoId)
        {
            string hql = "select inp " +
                         "from Insumo ins , InsumoPrecio inp " +
                         "where inp.Insumo.Id = ins.Id " +
                         "and inp.Deleted  = false " +
                         "and inp.FechaDesde <= :fecha " +
                         "and (inp.FechaHasta >= :fecha or inp.FechaHasta is null)" +
                         "and ins.Id = :insumoId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("insumoId", insumoId);
            query.SetParameter("fecha", enfoke.Time.Now.Date);
            EntityCollection<InsumoPrecio> insumosPrecios = dalEngine.GetManyByQuery<InsumoPrecio>(query);
            if (insumosPrecios != null && insumosPrecios.Count > 0)
                return insumosPrecios[0];
            else
                return null;
        }

        public EntityCollection<InsumoPrecio> InsumosPreciosReadByInsumosVigente(List<int> insumosIds)
        {
            if (insumosIds.Count == 0)
                return new EntityCollection<InsumoPrecio>();

            string hql = "select inp " +
                         "from InsumoPrecio inp " +
                         "where inp.Deleted  = false " +
                         "and inp.FechaDesde  <= :fecha " +
                         "and (inp.FechaHasta  >= :fecha or inp.FechaHasta is null)" +
                         "and inp.Insumo.Id  IN (:insumosIds) ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("insumosIds", insumosIds);
            query.SetParameter("fecha", enfoke.Time.Now.Date);
            return dalEngine.GetManyByQuery<InsumoPrecio>(query);
        }

        public EntityCollection<InsumoPrecio> InsumosPreciosReadByInsumosAndFecha(List<int> insumosIds, DateTime fecha)
        {
            if (insumosIds == null || insumosIds.Count == 0)
                return new EntityCollection<InsumoPrecio>();

            EntityCollection<InsumoPrecio> response = new EntityCollection<InsumoPrecio>();
            int topRow = Math.Min(insumosIds.Count, 1000);
            while (insumosIds.Count > 0)
            {
                List<int> ids = insumosIds.GetRange(0, topRow);
                IQueryable<InsumoPrecio> query = from insumo in dalEngine.Query<InsumoPrecio>() where ids.Contains(insumo.Insumo.Id) && insumo.Deleted == false && 
                                                 insumo.FechaDesde <= fecha.Date && (insumo.FechaHasta == null ||insumo.FechaHasta >= fecha.Date)
                                                 select new InsumoPrecio(insumo.Id, insumo.FechaDesde, insumo.FechaHasta, insumo.Importe, insumo.Insumo);
                response.AddRange(query);
                insumosIds.RemoveRange(0, topRow);
                topRow = Math.Min(insumosIds.Count, 1000);
            }
            
            return response;
        }

        public EntityCollection<Insumo> InsumosReadByCodes(List<string> codes)
        {
            if (codes == null || codes.Count == 0)
                return new EntityCollection<Insumo>();

            List<string> upperCodes = new List<string>();
            foreach (string code in codes)
                upperCodes.Add(code.ToUpper());

            string hql = "select ins from Insumo ins where upper(rtrim(ins.Code)) IN (:codes)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("codes", upperCodes);
            return dalEngine.GetManyByQuery<Insumo>(query);
        }

        public InsumoPrecio InsumoPrecioReadByInsumoAndFecha(int insumoId, DateTime fecha)
        {
            DateTime fec = fecha.Date;
            string hql = "select inp " +
                         "from Insumo ins , InsumoPrecio inp " +
                         "where inp.Insumo.Id = ins.Id " +
                         "and inp.Deleted  = false " +
                         "and inp.FechaDesde  <= :fec " +
                         "and (inp.FechaHasta  >= :fec or inp.FechaHasta is null)" +
                         "and ins.Id  = :insumoId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("insumoId", insumoId);
            query.SetParameter("fec", (fec == DateTime.MinValue) ? enfoke.Time.Today.Date : fec);
            EntityCollection<InsumoPrecio> insumoPrecio = dalEngine.GetManyByQuery<InsumoPrecio>(query);

            if (insumoPrecio != null && insumoPrecio.Count > 0)
                return insumoPrecio[0];
            else
                return null;
        }

        public EntityCollection<InsumoPrecio> InsumoPrecioReadByInsumo(int insumoId)
        {
            string hql = "from InsumoPrecio inp " +
                         "where inp.Insumo.Id = :insumoId " +
                         "and inp.Deleted  = false ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("insumoId", insumoId);
            return dalEngine.GetManyByQuery<InsumoPrecio>(query);
        }

        public EntityCollection<InsumoPrecio> InsumoPrecioReadByInsumoCategoriaAndFecha(int? categoriaId, DateTime fecha)
        {
            IQueryable<InsumoPrecio> insumos = null;
            if (categoriaId.HasValue)
                insumos = from inp in dalEngine.Query<InsumoPrecio>() where inp.Insumo.Categoria != null && inp.Insumo.Categoria.Id == categoriaId.Value select inp;
            else
                insumos = dalEngine.Query<InsumoPrecio>();

            insumos = from inp in insumos where inp.Deleted == false && inp.FechaDesde <= fecha.Date && (inp.FechaHasta == null || inp.FechaHasta >= fecha.Date) select inp;
            return insumos.ToEntityCollection();
        }

        public EntityCollection<Insumo> InsumoConTipoCategorias()
        {
            string hql = "from Insumo ins " +
                            "where ins.Categoria.TipoInsumoCategoriaInt is not null " +
                            "and ins.Deleted = false ";
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<Insumo>(query);
        }

        public EntityCollection<Insumo> InsumoConTipoCategoriasByCategoriaDescripcionCodigo(String categoria, String descripcion, String codigo)
        {
            string hql = "from Insumo ins " +
                         "where ins.Categoria.TipoInsumoCategoriaInt is not null " +
                         "and ins.Deleted = false ";

            if (!String.IsNullOrEmpty(categoria))
                hql += "and ins.Categoria.Name like '%" + categoria + "%' ";

            if (!String.IsNullOrEmpty(descripcion))
                hql += "and ins.Name like '%" + descripcion + "%' ";

            if (!String.IsNullOrEmpty(codigo))
                hql += "and ins.Code like '%" + codigo + "%' ";

            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<Insumo>(query);
        }

        public EntityCollection<Insumo> InsumoReadByPlanPractica(int planPracticaId)
        {
            string hql = "select ins " +
                         "from Insumo ins , PlanPracticaInsumo ppi " +
                         "where ppi.Insumo.Id = ins.Id " +
                         "and ins.Deleted = false " +
                         "and ins.Categoria.TipoInsumoCategoriaInt is not null " +
                         "and ppi.PlanPracticaPrecio.Id = :planPracticaId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("planPracticaId", planPracticaId);
            return dalEngine.GetManyByQuery<Insumo>(query);
        }

        public EntityCollection<Insumo> InsumoReadByPractica(int practicaId)
        {
            string hql = "select ins " +
                         "from Insumo ins , PracticaInsumo pi " +
                         "where pi.Insumo.Id = ins.Id " +
                         "and ins.Categoria.TipoInsumoCategoriaInt is not null " +
                         "and pi.PracticaID = :practicaId " +
                         "and pi.Deleted  = false " +
                         "and ins.Deleted = false ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practicaId", practicaId);
            return dalEngine.GetManyByQuery<Insumo>(query);
        }

        public EntityCollection<InsumoPrecio> InsumoPreciosReadByInsumoAndFechaVigencia(EntityCollection<InsumoPrecio> insumosPrecios, DateTime fechaVigencia)
        {
            if (insumosPrecios == null || insumosPrecios.Count == 0)
                return new EntityCollection<InsumoPrecio>();

            List<int> insumosIds = new List<int>();
            foreach (InsumoPrecio current in insumosPrecios)
                insumosIds.Add(current.Insumo.Id);

            return (from inp in dalEngine.Query<InsumoPrecio>()
                    where insumosIds.Contains(inp.Insumo.Id)
                        && inp.FechaDesde <= fechaVigencia.Date && (inp.FechaHasta == null || inp.FechaHasta >= fechaVigencia.Date)
                        && inp.Deleted == false
                    select inp).ToEntityCollection();
        }
    }
}

