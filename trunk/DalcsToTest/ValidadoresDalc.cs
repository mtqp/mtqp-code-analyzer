using System;
using System.Collections.Generic;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;
using NHibernate;
using enfoke.AOP;
using System.Text;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class ValidadoresDalc : Dalc, IService
    {
        protected ValidadoresDalc(NotConstructable dummy) : base(dummy) { }







        public ValidadorObraSocial ValidadorObrasocialReadByObraSocial(ObraSocial obraSocial)
        {
            EntityCollection<ValidadorObraSocial> col =
                dalEngine.GetManyByProperty<ValidadorObraSocial>(ValidadorObraSocial.Properties.ObraSocialId,
                                                                 obraSocial.Id);
            return col.Count == 0 ? null : col[0];
        }






        public ValidadorObraSocial ValidadorObrasocialReadByObraSocial(int obraSocialId)
        {
            EntityCollection<ValidadorObraSocial> col =
                dalEngine.GetManyByProperty<ValidadorObraSocial>(ValidadorObraSocial.Properties.ObraSocialId,
                                                                 obraSocialId);
            if (col.Count == 0)
                return null;
            return col[0];
        }

        public Validador ValidadorReadByObraSocial(ObraSocial obraSocial)
        {
            IQuery query = dalEngine.CreateQuery("SELECT v from Validador AS v, ValidadorObraSocial as vos "
                                                 + " WHERE vos.ObraSocialId = :obraSocialId AND vos.ValidadorId = v.Id ");
            query.SetParameter("obraSocialId", obraSocial.Id);

            return dalEngine.GetByQuery<Validador>(query);
        }

        public Validador ValidadorReadByObraSocialPlan(int ospId)
        {
            ObraSocialPlan osp = Context.Session.ObrasSocialesDalc.ObraSocialPlanReadById(ospId);
            return ValidadorReadByObraSocial(osp.ObraSocial);
        }

        /// <summary>
        /// Borro el ValidadorObraSocial viejo y creo este nuevo.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public ValidadorObraSocial ValidadorObraSocialUpdate(ValidadorObraSocial item)
        {
            return dalEngine.Update(item);
        }

        public Validador ValidadorGetById(int id)
        {
            return dalEngine.GetById<Validador>(id);
        }

        public EntityCollection<ValidacionTipoOperacion> ValidacionTipoOperacionGetByEnumsSinAnulacionOnline(
            ValidacionTipoOperacionEnum[] valTOEnums)
        {
            EntityCollection<ValidacionTipoOperacion> col = new EntityCollection<ValidacionTipoOperacion>();

            foreach (ValidacionTipoOperacionEnum enumTipoOpe in valTOEnums)
            {
                if (enumTipoOpe != ValidacionTipoOperacionEnum.AnularOnline)
                    col.Add(dalEngine.GetById<ValidacionTipoOperacion>((int)enumTipoOpe));
            }

            return col;
        }






        public ValidacionTipoOperacion ValidacionTipoOperacionReadById(int id)
        {
            return dalEngine.GetById<ValidacionTipoOperacion>(id);
        }

        public EntityCollection<ValidacionPendiente> ValidacionesPendientesReadByEstados(List<int> estadosIds)
        {
            if (estadosIds == null || estadosIds.Count <= 0)
                return new EntityCollection<ValidacionPendiente>();

            //Traigo todos los turnos en estado ErrorTemporal y los datos de su ultima validacion (que es erronea por el estado del turno).
            const string hql = "SELECT new enfoke.Eges.Entities.Results.ValidacionPendiente(v,tv.TipoOperacionId, t.Id, t.Fecha, t.Orden.ObraSocialPlanId, tv.NroAfiliado,tv.NroPreAutorizacion, vos) " +
                               "FROM Turno t, TurnoValidacion tv, ObraSocialPlan osp, ObraSocial os, ValidadorObraSocial vos, Validador v " +
                               "WHERE t.ValidacionEstadoId IN (:estados) " +
                               "AND t.Orden.ObraSocialPlanId = osp.Id " +
                               "AND osp.ObraSocial = os " +
                               "AND os.Id = vos.ObraSocialId " +
                               "AND vos.ValidadorId = v.Id " +
                               "AND tv.LoteValidacionId = t.UltimoLoteValidacion " +
                               "ORDER BY v.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("estados", estadosIds);
            return dalEngine.GetManyByQuery<ValidacionPendiente>(query);
        }

        public EntityCollection<PracticaValidacion> PracticaValidacionReadByLotes(EntityCollection<LoteValidacion> lotes)
        {
            if (lotes == null || lotes.Count == 0)
                return new EntityCollection<PracticaValidacion>();

            SQLBlockBuilder<int> itemsLote = new SQLBlockBuilder<int>(lotes.GetIds());
            string loteIds = itemsLote.BuildConstrainBlock("tv.LoteValidacionId");

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new enfoke.Eges.Entities.PracticaValidacion(pv, lv.TurnoId) ");
            hqlBuilder.Append("from PracticaValidacion pv, TurnoValidacion tv, LoteValidacion lv where tv.LoteValidacionId = lv.Id ");
            hqlBuilder.AppendFormat("and pv.TurnoValidacion = tv.Id and {0} ", loteIds);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<PracticaValidacion>(query);
        }

        public EntityCollection<ValidacionPendiente> ValidacionesPendientesReadByEstadosAndFecha(List<int> estadosIds, DateTime fecha)
        {
            if (estadosIds == null || estadosIds.Count <= 0)
                return new EntityCollection<ValidacionPendiente>();

            //Traigo todos los turnos en estado ErrorTemporal y los datos de su ultima validacion (que es erronea por el estado del turno).
            const string hql = "SELECT new enfoke.Eges.Entities.Results.ValidacionPendiente(v,tv.TipoOperacionId, t.Id, t.Fecha, t.Orden.ObraSocialPlanId, tv.NroAfiliado,tv.NroPreAutorizacion, vos) " +
                               "FROM Turno t, TurnoValidacion tv, ObraSocialPlan osp, ObraSocial os, ValidadorObraSocial vos, Validador v " +
                               "WHERE t.ValidacionEstadoId IN (:estados) " +
                               "AND t.Orden.ObraSocialPlanId = osp.Id " +
                               "AND osp.ObraSocial = os " +
                               "AND os.Id = vos.ObraSocialId " +
                               "AND vos.ValidadorId = v.Id " +
                               "AND tv.LoteValidacionId = t.UltimoLoteValidacion " +
                               "AND tv.Fecha >= :fecha " +
                               "ORDER BY v.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("estados", estadosIds);
            query.SetDateTime("fecha", fecha.Date);
            return dalEngine.GetManyByQuery<ValidacionPendiente>(query);
        }

        public EntityCollection<ValidacionEstado> ValidacionEstadoGetAll()
        {
            return dalEngine.GetAll<ValidacionEstado>();
        }

        public void ValidadorUpdate(Validador validador)
        {
            Validador val = dalEngine.GetById<Validador>(validador.Id);

            //Lo no configurable me lo traigo de la BD
            validador.Typename = val.Typename;
            validador.Nombre = val.Nombre;

            dalEngine.Update(validador);
        }

        public ValidadorObraSocial ValidadorObraSocialByTurno(int turnoId)
        {
            const string hql = "SELECT vos FROM Turno tur, ObraSocialPlan osp, ValidadorObraSocial vos " +
                               "WHERE tur.Id = :turnoId " +
                               "AND tur.Orden.ObraSocialPlanId = osp.Id " +
                               "AND osp.ObraSocial.Id = vos.ObraSocialId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("turnoId", turnoId);

            return dalEngine.GetByQuery<ValidadorObraSocial>(query);
        }

        public ValidadorObraSocial ValidadorObraSocialByObraSocialPlanId(int ObraSocialPlanId)
        {
            const string hql = "SELECT vos FROM ObraSocialPlan osp, ValidadorObraSocial vos " +
                               "WHERE osp.ObraSocial.Id = vos.ObraSocialId " +
                               "AND osp.Id = :obraSocialPlanId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("obraSocialPlanId", ObraSocialPlanId);

            try
            {
                return dalEngine.GetByQuery<ValidadorObraSocial>(query);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [RequiresTransaction]
        public virtual void BorrarHistorialValidacionPorOrden(int ordenId)
        {
            EntityCollection<Turno> turnos = Context.Session.TurnosDalc.TurnosReadByOrdenId(ordenId);
            foreach (Turno turno in turnos)
                BorrarHistorialPorTurno(turnos, turno);
        }

        private void BorrarHistorialPorTurno(EntityCollection<Turno> turnos, Turno turno)
        {
            EntityCollection<LoteValidacion> lotes = dalEngine.GetManyByProperty<LoteValidacion>(LoteValidacion.Properties.TurnoId, turno.Id);
            if (lotes.Count == 0)
                return;

            EntityCollection<TurnoValidacion> turnosValidacion = TurnoValidacionReadByLotes(lotes);
            if (turnosValidacion.Count == 0)
            {
                dalEngine.DeleteBatch(lotes);
                return;
            }

            EntityCollection<PracticaValidacion> practicas = PracticaValidacionReadByTurnos(turnosValidacion);
            dalEngine.DeleteBatch(practicas);
            dalEngine.DeleteBatch(turnosValidacion);
            dalEngine.DeleteBatch(lotes);
        }

        private EntityCollection<PracticaValidacion> PracticaValidacionReadByTurnos(EntityCollection<TurnoValidacion> turnos)
        {
            Filter filter = new Filter();
            filter.Add(PracticaValidacion.Properties.TurnoValidacion, "IN", turnos.GetIds());
            EntityCollection<PracticaValidacion> practicas = dalEngine.GetManyByFilter<PracticaValidacion>(filter);
            return practicas;
        }

        private EntityCollection<TurnoValidacion> TurnoValidacionReadByLotes(EntityCollection<LoteValidacion> lotes)
        {
            Filter filter = new Filter();
            filter.Add(TurnoValidacion.Properties.LoteValidacionId, "IN", lotes.GetIds());
            EntityCollection<TurnoValidacion> turnos = dalEngine.GetManyByFilter<TurnoValidacion>(filter);
            return turnos;
        }

        public LoteValidacion LoteValidacionReadLastSuccesfullByTurnoId(int turnoId)
        {
            Turno turno = Context.Session.TurnosDalc.TurnoReadById(turnoId);
            return LoteValidacionReadLastSuccesfullByTurno(turno);
        }

        public LoteValidacion LoteValidacionReadLastSuccesfullByTurno(Turno turno)
        {
            Filter filter = new Filter();
            filter.Add(LoteValidacion.Properties.Id, "=", turno.UltimoLoteValidacion);
            LoteValidacion loteValidacion = dalEngine.GetByFilter<LoteValidacion>(filter);
            if (loteValidacion != null)
                loteValidacion.Operaciones = dalEngine.GetManyByProperty<TurnoValidacion>(TurnoValidacion.Properties.LoteValidacionId, loteValidacion.Id);
            if (loteValidacion != null && loteValidacion.Operaciones != null && loteValidacion.Operaciones.Count > 0)
                AgregarSubOperaciones(loteValidacion);

            return loteValidacion;
        }

        private void AgregarSubOperaciones(LoteValidacion loteValidacion)
        {
            Filter filter = new Filter();
            filter.Add(PracticaValidacion.Properties.TurnoValidacion, "IN", loteValidacion.Operaciones.GetIds());
            EntityCollection<PracticaValidacion> practicas = dalEngine.GetManyByFilter<PracticaValidacion>(filter);
            foreach (TurnoValidacion turnoV in loteValidacion.Operaciones)
            {
                IEnumerable<PracticaValidacion> practicasPorTurno = practicas.FindAll(delegate(PracticaValidacion pval) { return pval.TurnoValidacion == turnoV.Id; });
                if (practicasPorTurno == null)
                    continue;

                turnoV.SubOperaciones = new EntityCollection<PracticaValidacion>();
                foreach (PracticaValidacion val in practicasPorTurno)
                    turnoV.SubOperaciones.Add(val);
            }
        }

        public EntityCollection<LoteValidacion> LoteValidacionReadLastByTurnosIds(List<int> turnosIds)
        {
            if (turnosIds == null || turnosIds.Count == 0)
                return new EntityCollection<LoteValidacion>();

            StringBuilder hqlBuilder = new StringBuilder();
            SQLBlockBuilder<int> itemsTur = new SQLBlockBuilder<int>(turnosIds);
            string turIds = itemsTur.BuildConstrainBlock("tur.Id");
            hqlBuilder.AppendFormat("select lot from LoteValidacion lot, Turno tur where lot.Id = tur.UltimoLoteValidacion and {0} ", turIds);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<LoteValidacion>(query);
        }

        /// <summary>
        /// Trae el nro de autorizacion de la orden multiple (TOM). Si la TOM ya se autorizo pero el turno no se recepciono (por si apreto registrar->anular->registrar) entonces tambien mando el Id del turno
        /// </summary>
        /// <param name="ordenMultipleId"></param>
        /// <param name="turnoId"></param>
        /// <returns></returns>
        public string ObtenerNroAutorizacionOrdenMultiple(int ordenMultipleId, int? turnoId)
        {
            Orden orden = dalEngine.GetById<Orden>(ordenMultipleId);

            if (!orden.EsMultiple)
                return String.Empty;

            TurnosDalc tDalc = Context.Session.TurnosDalc;
            EntityCollection<TurnoLight> turnos = tDalc.TurnoLightReadByOrdenId(ordenMultipleId);
            List<int> turnosIds = new List<int>();
            foreach (TurnoLight turno in turnos)
                turnosIds.Add(turno.Id);

            string hql = "SELECT tv FROM LoteValidacion lv, TurnoValidacion tv WHERE lv.Id = tv.LoteValidacionId AND lv.TurnoId IN (:turnosId) AND lv.Aprobada = :aprobada AND tv.TipoOperacionId = :tipoOperacionId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("turnosId", turnosIds);
            query.SetParameter("tipoOperacionId", (int)ValidacionTipoOperacionEnum.SolicitarAutorizacion);
            query.SetParameter("aprobada", true);
            EntityCollection<TurnoValidacion> turV = dalEngine.GetManyByQuery<TurnoValidacion>(query);

            if (turV.Count == 0)
                return String.Empty;

            if (turV.Count > 1)
            {
                foreach (TurnoValidacion turnoValidacion in turV)
                {
                    if (!String.IsNullOrEmpty(turnoValidacion.NroAutorizacion))
                        return turnoValidacion.NroAutorizacion;
                }

                return String.Empty;
            }

            return turV[0].NroAutorizacion;
        }

        // Me fijo que haya sido una fallada y le sumo la cantidad de fallos
        public bool ReintentoValidar(Turno turno, LoteValidacion turVal, out LoteValidacion turValAnterior)
        {
            // Busco la Ultima validacion del turno
            EntityCollection<LoteValidacion> colturValAnterior = dalEngine.GetManyByProperty<LoteValidacion>(LoteValidacion.Properties.TurnoId, turno.Id);
            LoteValidacion turValTMP = null;

            for (int i = 0; i < colturValAnterior.Count; i++)
                if (turValTMP == null || turValTMP.Operaciones[0].Fecha < colturValAnterior[i].Operaciones[0].Fecha)
                    turValTMP = colturValAnterior[i];

            turValAnterior = turValTMP;

            return (turValAnterior != null && !turValAnterior.Operaciones[0].Aprobada &&
                    turValAnterior.Operaciones[0].TipoOperacionId == turVal.Operaciones[0].TipoOperacionId &&
                //turValAnterior.NroPreAutorizacion == turVal.NroPreAutorizacion &&
                    turValAnterior.Operaciones[0].NroAfiliado == turVal.Operaciones[0].NroAfiliado);
        }

        public TurnoPracticaValidadorInfo[] TurnoValidacionPrepararPracticas(EntityCollection<PracticaTurno> colPracticaTurno, ValidadorObraSocial validador, DateTime fechaTurno, int obraSocialPlanId, EntityCollection<IDocumentacion> tdoc)
        {
            ObrasSocialesDalc OSDalc = Context.Session.ObrasSocialesDalc;
            List<TurnoPracticaValidadorInfo> practicas = new List<TurnoPracticaValidadorInfo>();
            for (int i = 0; i < colPracticaTurno.Count; i++)
            {
                PlanPracticaPrecio planPractica = OSDalc.PlanPracticaPrecioGetCurrentByObraSocialPlanAndPracticaAndEquipo(obraSocialPlanId, colPracticaTurno[i].Practica.Id, null, fechaTurno);
                if (planPractica != null)
                {
                    bool esFacturable = planPractica.Practica.EsFacturable.GetValueOrDefault(false);
                    bool esCantidadPositiva = colPracticaTurno[i].Cantidad > 0;
                    if (esCantidadPositiva &&
                        esFacturable &&
                        EsTipoOperacionRegistrarPractica(validador, planPractica) &&
                        ValidaTipoPractica(validador, planPractica))
                    {
                        TurnoPracticaValidadorInfo tpvi = new TurnoPracticaValidadorInfo
                        {
                            Cantidad = colPracticaTurno[i].Cantidad,
                            CodigoPractica = planPractica.CodigoInterno,
                            Descripcion = planPractica.Practica.Name,
                            NroPreautorizacion = string.Empty,
                            PracticaTurnoId = colPracticaTurno[i].Id
                        };

                        foreach (TurnoDocumentacion documentacion in tdoc)
                        {
                            if (documentacion.PracticaTurno.Practica.Id == planPractica.Practica.Id)
                            {
                                tpvi.NroPreautorizacion = documentacion.Valor;
                                break;
                            }
                        }

                        practicas.Add(tpvi);
                    }
                }
            }

            return practicas.ToArray();
        }

        private bool EsTipoOperacionRegistrarPractica(ValidadorObraSocial validador, PlanPracticaPrecio planPractica)
        {
            if(planPractica==null)
                return false;
            ValidacionTipoOperacionEnum tipoOpPlanPractica = planPractica.ValidacionTipoOperacionId.HasValue ? (ValidacionTipoOperacionEnum)planPractica.ValidacionTipoOperacionId : ValidacionTipoOperacionEnum.PredeterminadoObraSocial;
            ValidacionTipoOperacionEnum tipoOpValidador = (ValidacionTipoOperacionEnum)validador.TipoOperacionId;
            bool esTipoOpRegistrarPractica = tipoOpPlanPractica == ValidacionTipoOperacionEnum.RegistrarPractica;
            esTipoOpRegistrarPractica |= tipoOpValidador == ValidacionTipoOperacionEnum.RegistrarPractica && tipoOpPlanPractica == ValidacionTipoOperacionEnum.PredeterminadoObraSocial;
            return esTipoOpRegistrarPractica;
        }

        private static bool ValidaTipoPractica(ValidadorObraSocial validador, PlanPracticaPrecio planPractica)
        {
            return ((planPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.SetFarmacia && !validador.ValidaSetFarmacia) ||
                                planPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.Otro);
        }

        [RequiresTransaction]
        public virtual TurnoValidacionView TurnoValidacionGuardarValidacion(int turnoId, LoteValidacion loteValidacion, string observacion)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            return TurnoValidacionGuardarValidacion(turnoId, loteValidacion, observacion, user);
        }

        // Se encarga de actualizar un turno y su validacion
        [RequiresTransaction]
        public virtual TurnoValidacionView TurnoValidacionGuardarValidacion(int turnoId, LoteValidacion loteValidacion, string observacion, SecurityUser user)
        {
            TurnoValidacionView turValvw = new TurnoValidacionView();
            turValvw.TurnoId = turnoId;
            if (loteValidacion.TurnoId == 0)
                loteValidacion.TurnoId = turnoId;

            if (loteValidacion.Fecha == DateTime.MinValue)
                loteValidacion.Fecha = enfoke.Time.Now;

            // Si una operacion no fue exitosa, agarro el msg de error de esa
            bool aprobada = true;
            foreach (TurnoValidacion operacion in loteValidacion.Operaciones)
            {
                if (!operacion.Aprobada)
                {
                    loteValidacion.Descripcion = operacion.Descripcion;
                    aprobada = false;
                }
            }

            // Sino de cualquiera
            if (aprobada && loteValidacion.Operaciones.Count > 0)
                loteValidacion.Descripcion = loteValidacion.Operaciones[0].Descripcion;

            // Guardo el lote
            loteValidacion = dalEngine.Update(loteValidacion);

            // Asigno el Id a su detalle y Guardo
            foreach (TurnoValidacion operacion in loteValidacion.Operaciones)
            {
                if (operacion.Fecha == DateTime.MinValue)
                    operacion.Fecha = enfoke.Time.Now;
                operacion.LoteValidacionId = loteValidacion.Id;
            }
            loteValidacion.Operaciones = (EntityCollection<TurnoValidacion>)dalEngine.UpdateCollection(loteValidacion.Operaciones);

            // Asigno Id a SubOperacion y Guardo
            foreach (TurnoValidacion op in loteValidacion.Operaciones)
            {
                foreach (PracticaValidacion list in op.SubOperaciones)
                    list.TurnoValidacion = op.Id;

                turValvw.CondicionIVA = op.CondicionIVA;
                op.SubOperaciones = (EntityCollection<PracticaValidacion>)dalEngine.UpdateCollection(op.SubOperaciones);
            }

            turValvw.Descripcion = observacion;

            // Creo el objeto que voy a devolver
            turValvw.TurnoValidacionId = loteValidacion.Id;
            turValvw.Descripcion = loteValidacion.Descripcion;
            turValvw.EstadoTransaccion = loteValidacion.Operaciones[0].Aprobada;
            turValvw.Respuesta = loteValidacion.Respuesta;
            if (loteValidacion.Aprobada)
                turValvw.NroAutorizacion = loteValidacion.Operaciones[0].NroAutorizacion;

            // Salvo los cambios en el turno solo
            Turno turno = dalEngine.GetById<Turno>(turnoId);
            if (loteValidacion.TipoOperacionId != (int)ValidacionTipoOperacionEnum.AnularOnline)
                turno.ValidacionEstadoId = loteValidacion.Operaciones[0].ValidacionEstado;
            else if (loteValidacion.TipoOperacionId == (int)ValidacionTipoOperacionEnum.AnularOnline && loteValidacion.Operaciones[0].ValidacionEstado == (int)ValidacionEstadoEnum.AnuladoOnline)
                turno.ValidacionEstadoId = loteValidacion.Operaciones[0].ValidacionEstado;

            if (loteValidacion.Aprobada)
                turno.UltimoLoteValidacion = loteValidacion.Id;

            turno = dalEngine.Update(turno);
            // Devuelvo con que estado quedo el turno
            turValvw.TurnoValidacionEstadoId = turno.ValidacionEstadoId.Value;

            return turValvw;




}

        public EntityCollection<LoteValidacionForList> LoteValidacionHistorialByTurno(int turnoId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteValidacionForList(t.Id, t.Estado.Id, t.Fecha, pac.ApellidoNombre, lv.NumeroAfiliado, e.Servicio.Name, prt.Practica.Name, os.Name, osp.Name, lv.Id, lv.TipoOperacionId, vto.Nombre, lv.Aprobada, lv.Fecha, lv.ValidacionEstadoId, vae.Nombre, t.ValidacionEstado) " +
                                  "FROM TurnoHQL t, Paciente pac, ObraSocialPlan osp, ObraSocial os, PracticaTurno prt, Equipo e, ValidacionEstado vae, ValidacionTipoOperacion vto, LoteValidacion lv " +
                                  "WHERE t.Orden.ObraSocialPlan.Id  = osp.Id " +
                                  "AND osp.ObraSocial = os " +
                                  "AND t.Orden.Paciente.Id = pac.Id " +
                                  "AND prt.TurnoId = t.Id " +
                                  "AND prt.Tipo = :practicaPrincipal " +
                                  "AND t.Equipo.Id = e.Id " +
                                  "AND lv.ValidacionEstadoId = vae.Id " +
                                  "AND lv.TurnoId = t.Id " +
                                  "AND lv.TipoOperacionId = vto.Id " +
                                  "AND t.Id = :turnoId " +
                                  "ORDER BY lv.Fecha";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("practicaPrincipal", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("turnoId", turnoId);
            return dalEngine.GetManyByQuery<LoteValidacionForList>(query);
        }

        public EntityCollection<PracticaValidacion> PracticaValidacionGetByLoteValidacion(int LoteValidacionId)
        {

            string hql = "SELECT pv FROM TurnoValidacion tv, PracticaValidacion pv WHERE tv.Id = pv.TurnoValidacion AND tv.LoteValidacionId = :loteValidacionId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("loteValidacionId", LoteValidacionId);
            return dalEngine.GetManyByQuery<PracticaValidacion>(query);
        }

        public LoteValidacion LoteValidacionReadLastByTurno(int turnoId)
        {
            Turno t = dalEngine.GetById<Turno>(turnoId);

            return LoteValidacionReadLastByTurno(t);
        }

        public LoteValidacion LoteValidacionReadLastByTurno(Turno turno)
        {
            LoteValidacion tv = null;

            if (turno.UltimoLoteValidacion.HasValue)
            {
                tv = dalEngine.GetById<LoteValidacion>(turno.UltimoLoteValidacion.Value);
                if (tv == null)
                    return null;
                tv.Operaciones = dalEngine.GetManyByProperty<TurnoValidacion>(TurnoValidacion.Properties.LoteValidacionId, tv.Id);
                foreach (TurnoValidacion operacion in tv.Operaciones)
                    operacion.SubOperaciones = dalEngine.GetManyByProperty<PracticaValidacion>(PracticaValidacion.Properties.TurnoValidacion, operacion.Id);
            }

            return tv;
        }

        public EntityCollection<PracticaValidacion> PracticaValidacionReadByLoteValidacionId(int loteValidacionId)
        {
            string hql = "SELECT pv FROM TurnoValidacion tv, PracticaValidacion pv " +
                               "WHERE tv.LoteValidacionId = :loteValidacionId " +
                               "AND tv.Id = pv.TurnoValidacion ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("loteValidacionId", loteValidacionId);

            return dalEngine.GetManyByQuery<PracticaValidacion>(query);
        }






        public string GetValidadorLicencia(int turnoID, int ValidadorID)
        {
            const string hql = "SELECT vs FROM Turno tur, ValidadorSucursal vs, Equipo Eq " +
                               "WHERE tur.Id = :TurnoID " +
                               "AND tur.EquipoId = Eq.Id " +
                               "AND Eq.Sucursal.Id = vs.SucursalID " +
                               "AND vs.ValidadorId = :ValidadorID";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("TurnoID", turnoID);
            query.SetInt32("ValidadorID", ValidadorID);

            ValidadorSucursal vs;
            try
            {
                vs = dalEngine.GetByQuery<ValidadorSucursal>(query);
            }
            catch (Exception ex)
            {

                throw ex;
            }

            return string.IsNullOrEmpty(vs.Licencia) ? string.Empty : vs.Licencia;

        }

        public EntityCollection<ValidadorSucursal> GetValidadorSucursalByValidadorID(int ValidadorID)
        {
            const string hql = "SELECT vs FROM ValidadorSucursal vs WHERE vs.ValidadorId = :ValidadorID";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("ValidadorID", ValidadorID);

            return dalEngine.GetManyByQuery<ValidadorSucursal>(query);
        }

        public EntityCollection<ValidadorSucursal> SaveValidadoresSucursal(EntityCollection<ValidadorSucursal> items)
        {
            return (EntityCollection<ValidadorSucursal>)dalEngine.UpdateCollection(items);
        }

        public EntityCollection<LoteValidacionForList> LoteValidacionForListReadByFilters(DateTime? desdeTurno, DateTime? hastaTurno, DateTime? desdeOperacion, DateTime? hastaOperacion, string paciente, string numeroAfiliado, string obraSocial, int? validacionEstadoId, int? validacionTipoOperacionId, string protocolo)
        {
            EntityCollection<LoteValidacionForList> result = ValidacionesConOrdenes(desdeTurno, hastaTurno, desdeOperacion, hastaOperacion, paciente, numeroAfiliado, obraSocial, validacionEstadoId, validacionTipoOperacionId, protocolo);
            if(string.IsNullOrEmpty(protocolo))
                result.AddRange(ValidacionesSinOrdenes(desdeTurno, hastaTurno, desdeOperacion, hastaOperacion, paciente, numeroAfiliado, obraSocial, validacionEstadoId, validacionTipoOperacionId));
            return result;
        }

        private EntityCollection<LoteValidacionForList> ValidacionesSinOrdenes(DateTime? desdeTurno, DateTime? hastaTurno, DateTime? desdeOperacion, DateTime? hastaOperacion, string paciente, string numeroAfiliado, string obraSocial, int? validacionEstadoId, int? validacionTipoOperacionId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteValidacionForList(t.Id, t.Estado.Id, t.Fecha, pac.ApellidoNombre, lv.NumeroAfiliado, e.Servicio.Name, prt.Practica.Name, os.Name, osp.Name, lv.Id, lv.TipoOperacionId, vto.Nombre, lv.Aprobada, lv.Fecha, lv.ValidacionEstadoId, vae.Nombre, t.ValidacionEstado) " +
                         "FROM TurnoHQL t, Paciente pac, ObraSocialPlan osp, ObraSocial os, PracticaTurno prt, Equipo e, ValidacionEstado vae, ValidacionTipoOperacion vto, LoteValidacion lv " +
                         "WHERE t.Orden.ObraSocialPlan.Id = osp.Id " +
                         "AND osp.ObraSocial = os " +
                         "AND t.Orden.Paciente.Id = pac.Id " +
                         "AND prt.TurnoId = t.Id " +
                         "AND prt.Tipo = :practicaPrincipal " +
                         "AND t.Equipo.Id = e.Id " +
                         "AND lv.ValidacionEstadoId = vae.Id " +
                         "AND lv.Id = (select max(l.Id) from LoteValidacion l where l.TurnoId = t.Id) " +
                         "AND lv.TipoOperacionId = vto.Id and t.Orden is null ";
            if (desdeTurno.HasValue)
                hql += "AND t.Fecha >  :desdeTurno ";
            if (hastaTurno.HasValue)
                hql += "AND t.Fecha <= :hastaTurno ";
            if (desdeOperacion.HasValue)
                hql += "AND lv.Fecha >= :desdeOperacion ";
            if (hastaOperacion.HasValue)
                hql += "AND lv.Fecha <= :hastaOperacion ";
            if (!String.IsNullOrEmpty(paciente))
                hql += "AND pac.ApellidoNombre like :paciente ";
            if (!String.IsNullOrEmpty(numeroAfiliado))
                hql += "AND lv.NumeroAfiliado like :numeroAfiliado ";
            if (!String.IsNullOrEmpty(obraSocial))
                hql += "AND os.Name like :obraSocial ";
            if (validacionEstadoId.HasValue)
                hql += "AND t.ValidacionEstado.Id = :validacionEstadoId ";
            if (validacionTipoOperacionId.HasValue)
                hql += "AND lv.TipoOperacionId = :validacionTipoOperacionId ";

            hql += "ORDER BY t.Fecha";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("practicaPrincipal", (int)PracticaTurnoTipoEnum.Principal);
            if (desdeTurno.HasValue)
                query.SetParameter("desdeTurno", desdeTurno.Value.Date);
            if (hastaTurno.HasValue)
                query.SetParameter("hastaTurno", hastaTurno);
            if (desdeOperacion.HasValue)
                query.SetParameter("desdeOperacion", desdeOperacion.Value.Date);
            if (hastaOperacion.HasValue)
                query.SetParameter("hastaOperacion", hastaOperacion);
            if (!String.IsNullOrEmpty(paciente))
                query.SetParameter("paciente", paciente + "%");
            if (!String.IsNullOrEmpty(numeroAfiliado))
                query.SetParameter("numeroAfiliado", numeroAfiliado);
            if (!String.IsNullOrEmpty(obraSocial))
                query.SetParameter("obraSocial", obraSocial);
            if (validacionEstadoId.HasValue)
                query.SetInt32("validacionEstadoId", validacionEstadoId.Value);
            if (validacionTipoOperacionId.HasValue)
                query.SetInt32("validacionTipoOperacionId", validacionTipoOperacionId.Value);

            return dalEngine.GetManyByQuery<LoteValidacionForList>(query);
        }

        private EntityCollection<LoteValidacionForList> ValidacionesConOrdenes(DateTime? desdeTurno, DateTime? hastaTurno, DateTime? desdeOperacion, DateTime? hastaOperacion, string paciente, string numeroAfiliado, string obraSocial, int? validacionEstadoId, int? validacionTipoOperacionId, string protocolo)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteValidacionForList(t.Id, t.Estado.Id, t.Fecha, pac.ApellidoNombre, lv.NumeroAfiliado, e.Servicio.Name, prt.Practica.Name, os.Name, osp.Name, lv.Id, lv.TipoOperacionId, vto.Nombre, lv.Aprobada, lv.Fecha, lv.ValidacionEstadoId, vae.Nombre, t.ValidacionEstado, t.Orden.Protocolo.ProtocoloFull) " +
                         "FROM TurnoHQL t, Paciente pac, ObraSocialPlan osp, ObraSocial os, PracticaTurno prt, Equipo e, ValidacionEstado vae, ValidacionTipoOperacion vto, LoteValidacion lv " +
                         "WHERE t.Orden.ObraSocialPlan.Id = osp.Id " +
                         "AND osp.ObraSocial = os " +
                         "AND t.Orden.Paciente.Id = pac.Id " +
                         "AND prt.TurnoId = t.Id " +
                         "AND prt.Tipo = :practicaPrincipal " +
                         "AND t.Equipo.Id = e.Id " +
                         "AND lv.ValidacionEstadoId = vae.Id " +
                         "AND lv.Id = (select max(l.Id) from LoteValidacion l where l.TurnoId = t.Id) " +
                         "AND lv.TipoOperacionId = vto.Id ";
            if (desdeTurno.HasValue)
                hql += "AND t.Fecha >= :desdeTurno ";
            if (hastaTurno.HasValue)
                hql += "AND t.Fecha <  :hastaTurno ";
            if (desdeOperacion.HasValue)
                hql += "AND lv.Fecha >= :desdeOperacion ";
            if (hastaOperacion.HasValue)
                hql += "AND lv.Fecha <= :hastaOperacion ";
            if (!String.IsNullOrEmpty(paciente))
                hql += "AND pac.ApellidoNombre like :paciente ";
            if (!String.IsNullOrEmpty(numeroAfiliado))
                hql += "AND lv.NumeroAfiliado like :numeroAfiliado ";
            if (!String.IsNullOrEmpty(obraSocial))
                hql += "AND os.Name like :obraSocial ";
            if (validacionEstadoId.HasValue)
                hql += "AND t.ValidacionEstado.Id = :validacionEstadoId ";
            if (validacionTipoOperacionId.HasValue)
                hql += "AND lv.TipoOperacionId = :validacionTipoOperacionId ";
            if (!string.IsNullOrEmpty(protocolo))
                hql += "AND t.Orden.Protocolo.ProtocoloFull = :protocolo ";


            hql += "ORDER BY t.Fecha ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("practicaPrincipal", (int)PracticaTurnoTipoEnum.Principal);
            if (desdeTurno.HasValue)
                query.SetParameter("desdeTurno", desdeTurno.Value.Date);
            if (hastaTurno.HasValue)
                query.SetParameter("hastaTurno", hastaTurno.Value.Date);
            if (desdeOperacion.HasValue)
                query.SetParameter("desdeOperacion", desdeOperacion.Value.AddDays(-1));
            if (hastaOperacion.HasValue)
                query.SetParameter("hastaOperacion", hastaOperacion);
            if (!String.IsNullOrEmpty(paciente))
                query.SetParameter("paciente", paciente + "%");
            if (!String.IsNullOrEmpty(numeroAfiliado))
                query.SetParameter("numeroAfiliado", numeroAfiliado);
            if (!String.IsNullOrEmpty(obraSocial))
                query.SetParameter("obraSocial", obraSocial + "%");
            if (validacionEstadoId.HasValue)
                query.SetInt32("validacionEstadoId", validacionEstadoId.Value);
            if (validacionTipoOperacionId.HasValue)
                query.SetInt32("validacionTipoOperacionId", validacionTipoOperacionId.Value);
            if (!string.IsNullOrEmpty(protocolo))
                query.SetString("protocolo", protocolo);

            return dalEngine.GetManyByQuery<LoteValidacionForList>(query);
        }
        public EntityCollection<LoteValidacionDetalleForList> LoteValidacionDetalleForListReadByLoteValidacion(int LoteValidacionId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteValidacionDetalleForList(pva.CodigoPractica, prt.Practica.Name, pva.CantidadAutorizadas, tva.Descripcion, tva.NroPreAutorizacion, tva.NroTransaccion, tva.NroAutorizacion, tva.Aprobada) " +
                         "FROM TurnoValidacion tva, PracticaValidacion pva, PracticaTurno prt " +
                         "WHERE tva.LoteValidacionId = :loteValidacionId " +
                         "AND tva.Id = pva.TurnoValidacion " +
                         "AND pva.PracticaTurnoId = prt.Id " +
                               "ORDER BY pva.Id";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("loteValidacionId", LoteValidacionId);
            return dalEngine.GetManyByQuery<LoteValidacionDetalleForList>(query);
        }
    }
}
