using System;
using System.Collections.Generic;
using System.Text;
using enfoke.AOP;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.DisconnectedSupport;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.No_Mapeadas;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Eges.Persistance;
using System.Linq;
using enfoke.Eges.HL7.Client;

namespace enfoke.Eges.Data
{
    public class InformesDalc : Dalc, IService
    {
        protected InformesDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<InformeDeSMS> InformeDeSMSReadByInformes(List<int> centrosIds, List<int> serviciosIds, string mensaje)
        {
            if (centrosIds.Count == 0 || serviciosIds.Count == 0)
                return new EntityCollection<InformeDeSMS>();

            EntityCollection<TurnoInforme> informesAEntregar = ObtenerInformesAEntregar(centrosIds, serviciosIds);
            EntityCollection<Paciente> pacientes = this.ObtenerPacientesDeTurnosAfectados(informesAEntregar);
            return this.ConstruirInformes(informesAEntregar, pacientes, mensaje);
        }

        private IEnumerable<TurnoInforme> TurnosPorPaciente(EntityCollection<TurnoInforme> informesAfectados, Paciente paciente)
        {
            return informesAfectados.FindAll(delegate(TurnoInforme turnoInforme) { return turnoInforme.Turno.Orden.PacienteId == paciente.Id; });
        }

        private EntityCollection<TurnoInforme> ObtenerInformesAEntregar(List<int> centrosIds, List<int> serviciosIds)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.TurnoInforme(eih.TurnoInforme.EstadoInforme, tur, suc.Domicilio) ");
            hqlBuilder.Append("from Turno tur, Equipo equ, Paciente pac, Sucursal suc, EstadoInformeHistorico eih join eih.TurnoInforme tui ");
            hqlBuilder.Append("where ");
            hqlBuilder.Append("pac.Id = tur.Orden.PacienteId and ");
            hqlBuilder.Append("tui.EstadoInforme.Id = :estadoAEntregar and ");
            hqlBuilder.Append("tui.TurnoID = tur.Id and ");
            hqlBuilder.Append("tur.EquipoId = equ.Id and ");
            hqlBuilder.Append("equ.Sucursal.Id = suc.Id  and ");
            hqlBuilder.Append("eih.Estado.Id = :estadoAEntregar and ");
            hqlBuilder.Append("eih.Fecha >= :fechaActualMenos1 and ");
            hqlBuilder.Append("eih.Fecha < :fechaActual and ");
            hqlBuilder.Append("pac.NumeroValido = true and ");
            hqlBuilder.Append("equ.Servicio.Id in (:servicios) and ");
            hqlBuilder.Append("equ.Sucursal.Id in (:sucursales) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("estadoAEntregar", (int)EstadoInformeEnum.AEntregar);
            query.SetDateTime("fechaActualMenos1", enfoke.Time.Now.Date.AddDays(-1));
            query.SetDateTime("fechaActual", enfoke.Time.Now.Date);
            query.SetParameterList("servicios", serviciosIds);
            query.SetParameterList("sucursales", centrosIds);
            EntityCollection<TurnoInforme> informesAEntregar = dalEngine.GetManyByQuery<TurnoInforme>(query);
            return informesAEntregar;
        }

        private EntityCollection<Paciente> ObtenerPacientesDeTurnosAfectados(EntityCollection<TurnoInforme> turnosInformeAfectados)
        {
            if (turnosInformeAfectados.Count == 0)
                return new EntityCollection<Paciente>();

            List<int> idTurnos = new List<int>(turnosInformeAfectados.Count);
            List<IIdentificable> turnos = new List<IIdentificable>(turnosInformeAfectados.Count);
            foreach (TurnoInforme informe in turnosInformeAfectados)
            {
                idTurnos.Add(informe.TurnoID);
                turnos.Add(informe.Turno);
            }

            SQLBlockBuilder<IIdentificable> blockBuilder = new SQLBlockBuilder<IIdentificable>(turnos);
            string turnosIds = blockBuilder.BuildConstrainBlock("tur.Id");
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select distinct pac ");
            hqlBuilder.Append("from Paciente pac, Turno tur ");
            hqlBuilder.AppendFormat("where {0} and ", turnosIds);
            hqlBuilder.Append("tur.Orden.PacienteId = pac.Id ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());

            EntityCollection<Paciente> pacientes = dalEngine.GetManyByQuery<Paciente>(query);
            return pacientes;
        }


        // TipoEntregaInforme
        public ReadAllCollection<TipoEntregaInforme> TipoEntregaInformeReadAll()
        {
            return new ReadAllCollection<TipoEntregaInforme>(dalEngine.GetAll<TipoEntregaInforme>(TipoEntregaInforme.Properties.Name));
        }

        // CanalEntregaInforme
        public ReadAllCollection<CanalEntregaInforme> CanalEntregaInformeReadAll()
        {
            return new ReadAllCollection<CanalEntregaInforme>(dalEngine.GetAll<CanalEntregaInforme>(CanalEntregaInforme.Properties.Name));
        }

        public CanalEntregaInforme CanalEntregaInformeReadById(int id)
        {
            return dalEngine.GetById<CanalEntregaInforme>(id);
        }
        //

        // TipoInforme
        public EntityCollection<TipoInforme> TipoInformeReadByServicioId(int servicioId, bool incluirEliminados)
        {
            EntityCollection<TipoInformeServicio> tisCollection = TipoInformeServicioReadByServicioId(servicioId);

            List<int> tipoInformesIds = new List<int>();
            foreach (TipoInformeServicio tis in tisCollection)
                tipoInformesIds.Add(tis.TipoInformeId);

            ReadManyCommand<TipoInforme> query = QueryTipoInformeReadAll(incluirEliminados);
            if (tipoInformesIds.Count > 0)
                query.Filter.Add(BooleanOp.And, TipoInforme.Properties.Id, "in", tipoInformesIds);
            return query.Execute();
        }

        // RegionInforme
        public EntityCollection<RegionInforme> RegionInformeReadAll()
        {
            return dalEngine.GetAll<RegionInforme>(RegionInforme.Properties.Name);
        }

        public EntityCollection<TipoInformeServicio> TipoInformeServicioUpdateMany(EntityCollection<TipoInformeServicio> tisCollection)
        {
            IEntityCollection iTisActualizados = dalEngine.UpdateCollection(tisCollection);
            EntityCollection<TipoInformeServicio> tisActualizados = new EntityCollection<TipoInformeServicio>();
            foreach (object iTis in iTisActualizados)
                tisActualizados.Add(iTis as TipoInformeServicio);
            return tisActualizados;
        }

        public EntityCollection<TipoInforme> TipoInformeReadAll(bool incluirEliminados)
        {
            ReadManyCommand<TipoInforme> readCmd = QueryTipoInformeReadAll(incluirEliminados);
            return readCmd.Execute();
        }

        private ReadManyCommand<TipoInforme> QueryTipoInformeReadAll(bool incluirEliminados)
        {
            List<int> idsExcluidos = new List<int>();
            idsExcluidos.Add((int)TipoInformeEnum.Comentarios);
            idsExcluidos.Add((int)TipoInformeEnum.NoPatologico);
            idsExcluidos.Add((int)TipoInformeEnum.Patologico);

            ReadManyCommand<TipoInforme> readCmd = new ReadManyCommand<TipoInforme>(dalEngine);
            Filter filter = new Filter();
            if (!incluirEliminados)
                filter.Add(BooleanOp.And, TipoInforme.Properties.Deleted,
                       "=", false);

            filter.Add(BooleanOp.And, TipoInforme.Properties.Id,
                   "not in", idsExcluidos);

            readCmd.Filter = filter;
            return readCmd;
        }

        public void TipoInformeServicioDeleteByServicioId(int servicioId)
        {
            EntityCollection<TipoInformeServicio> tisAEliminar = dalEngine.GetManyByProperty<TipoInformeServicio>(TipoInformeServicio.Properties.ServicioId, servicioId);
            dalEngine.Delete(tisAEliminar);
        }

        public EntityCollection<TipoInformeServicio> TipoInformeServicioReadByServicioId(int servicioId)
        {
            return dalEngine.GetManyByProperty<TipoInformeServicio>(TipoInformeServicio.Properties.ServicioId, servicioId);
        }


        public EntityCollection<TipoInformeServicio> TipoInformeServicioReadByTipoInformeId(int tipoInformeId)
        {
            return dalEngine.GetManyByProperty<TipoInformeServicio>(TipoInformeServicio.Properties.TipoInformeId, tipoInformeId);
        }


        // RegionInforme
        public RegionInforme RegionInformeReadById(int id)
        {
            return dalEngine.GetById<RegionInforme>(id);
        }

        public RegionInforme RegionInformeReadByTag(string tag)
        {
            return dalEngine.GetByProperty<RegionInforme>(RegionInforme.Properties.Tag, tag);
        }

        public EntityCollection<RegionInforme> RegionInformeUpdateMany(EntityCollection<RegionInforme> rIs)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            foreach (RegionInforme ri in rIs)
            {
                ri.UpdateUser = user.Id;
                ri.UpdateDate = enfoke.Time.Now;
            }


            rIs = dalEngine.UpdateCollection<RegionInforme>(rIs);

            return rIs;
        }


        // CircuitoInforme
        public EntityCollection<CircuitoInforme> CircuitoInformeReadAll()
        {
            return dalEngine.GetAll<CircuitoInforme>(CircuitoInforme.Properties.Name);
        }

        // RegionInformante
        public EntityCollection<RegionInformante> RegionInformanteReadAll()
        {
            EntityCollection<RegionInformante> informantes = dalEngine.GetManyByProperty<RegionInformante>(RegionInformante.Properties.Deleted, false);

            // Ordeno
            informantes.Sort(new Comparison<RegionInformante>(delegate(RegionInformante left, RegionInformante right)
            {
                int retorno = left.Sucursal.Name.CompareTo(right.Sucursal.Name);

                if (retorno == 0)
                    retorno = left.Servicio.Name.CompareTo(right.Servicio.Name);

                if (retorno == 0)
                    retorno = left.RegionInforme.Name.CompareTo(right.RegionInforme.Name);

                if (retorno == 0)
                    retorno = left.CircuitoInforme.Name.CompareTo(right.CircuitoInforme.Name);

                if (retorno == 0)
                    retorno = left.Informante.ApyN.CompareTo(right.Informante.ApyN);

                return retorno;
            }));

            return informantes;
        }

        public RegionInformante RegionInformanteUpdate(RegionInformante informante)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Seteo los campos de creado
            SecurityUser user = Security.Current.UserInfo.User;
            informante.Deleted = false;
            informante.CreateDate = enfoke.Time.Now;
            informante.CreateUser = user.Id;

            // Chequeo que tenga sucursal
            if (informante.Sucursal == null)
                informante.Sucursal = TurnosDalc.SucursalReadAll()[0];


            // Actualizo
            return dalEngine.Update<RegionInformante>(informante);
        }

        public void RegionInformanteDelete(RegionInformante informante)
        {
            // Seteo los campos de eliminado
            SecurityUser user = Security.Current.UserInfo.User;
            informante.Deleted = true;
            informante.DeleteDate = enfoke.Time.Now;
            informante.DeleteUser = user.Id;


            // Actualizo
            dalEngine.Update<RegionInformante>(informante);
        }

        public EntityCollection<RegionInformante> RegionInformanteReadBySucursalAndServicio(int sucursalID, int servicioID, bool pediatra, bool soloMedicos)
        {
            return RegionInformanteRead(sucursalID, servicioID, null, null, pediatra, soloMedicos);
        }

        public EntityCollection<RegionInformante> RegionInformanteReadByInformante(int informanteId)
        {
            return dalEngine.GetManyByProperty<RegionInformante>(RegionInformante.Properties.Informante.Id, informanteId);
        }

        public EntityCollection<RegionInformante> RegionInformanteReadNoEliminadosByInformante(int informanteId)
        {
            const string hql = " from RegionInformante ri " +
                               " where ri.Informante.Id = :informante" +
                               " and ri.Deleted = false";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("informante", informanteId);

            return dalEngine.GetManyByQuery<RegionInformante>(query);
        }

        public EntityCollection<RegionInformante> RegionInformanteReadNoEliminadosByInformante(int informanteId, int centroId, string servicio)
        {
            StringBuilder hql = new StringBuilder(" from RegionInformante ri ");
            hql.Append(" where ri.Informante.Id = :informante");
            if (centroId != -1)
                hql.Append(" and ri.Sucursal.Id = :centroId");
            if (!String.IsNullOrEmpty(servicio))
                hql.Append(" and ri.Servicio.Name Like :servicio");
            hql.Append(" and ri.Deleted = false");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("informante", informanteId);
            if (centroId != -1)
                query.SetParameter("centroId", centroId);
            if (!String.IsNullOrEmpty(servicio))
                query.SetParameter("servicio", servicio.Trim().Replace(' ', '%').ToUpper() + "%");

            return dalEngine.GetManyByQuery<RegionInformante>(query);
        }

        public RegionInformante RegionInformanteReadBySucursalServicioRegionAndInformante(int sucursalID, int servicioID, int regionID, int informanteID, bool soloMedicos)
        {
            EntityCollection<RegionInformante> informantes = RegionInformanteRead(sucursalID, servicioID, regionID, informanteID, null, soloMedicos);

            return informantes.Count > 0 ? informantes[0] : null;
        }

        private EntityCollection<RegionInformante> RegionInformanteRead(int? sucursalID, int? servicioID, int? regionID, int? informanteID, bool? pediatra, bool soloMedicos)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select ri from RegionInformante ri  ");
            if (soloMedicos)
                hqlBuilder.Append(" join ri.Informante.Medico me ");
            else
                hqlBuilder.Append(" left join ri.Informante.Medico me ");

            hqlBuilder.Append("where ri.Deleted = false and ");

            if (sucursalID.HasValue)
                hqlBuilder.Append("ri.Sucursal.Id = :sucursalID and ");

            if (servicioID.HasValue)
                hqlBuilder.Append("ri.Servicio.Id = :servicioID and ");

            if (regionID.HasValue)
                hqlBuilder.Append("ri.RegionInforme.Id = :regionID and ");

            if (informanteID.HasValue)
                hqlBuilder.Append("ri.Informante.Id = :informanteID and ");

            if (pediatra.HasValue)
                hqlBuilder.Append("(me is null or (ri.DBTipoAtencion != :tipoMedicoExcluido or ri.DBTipoAtencion is null)) and ");

            hqlBuilder = hqlBuilder.Remove(hqlBuilder.Length - 4, 4);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (sucursalID.HasValue)
                query.SetInt32("sucursalID", sucursalID.Value);

            if (servicioID.HasValue)
                query.SetInt32("servicioID", servicioID.Value);

            if (regionID.HasValue)
                query.SetInt32("regionID", regionID.Value);

            if (informanteID.HasValue)
                query.SetInt32("informanteID", informanteID.Value);

            if (pediatra.HasValue)
            {
                int tipoMedicoExcluido = pediatra.Value ? (int)TipoAtencionEnum.Adulto : (int)TipoAtencionEnum.Pediatra;
                query.SetInt32("tipoMedicoExcluido", tipoMedicoExcluido);
            }

            return dalEngine.GetManyByQuery<RegionInformante>(query);
        }


        // EstadoInforme
        public ReadAllCollection<EstadoInforme> EstadoInformeReadAll()
        {
            string key = "all";
            // los cachea a nivel thread...
            EntityCollection<EstadoInforme> ret = ResultsThreadCache<EstadoInforme>.GetResults(key);
            if (ret == null)
            {
                ret = dalEngine.GetAll<EstadoInforme>(EstadoInforme.Properties.Name);
                ResultsThreadCache<EstadoInforme>.SetResults(key, ret);
            }
            return new ReadAllCollection<EstadoInforme>(ret);
        }

        public EstadoInforme EstadoInformeReadById(int id)
        {
            // los cachea a nivel thread...
            EstadoInforme ret = EntityThreadCache<EstadoInforme>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<EstadoInforme>(id);
                EntityThreadCache<EstadoInforme>.SetItem(id, ret);
            }
            return ret;
        }

        /// <summary>
        /// El método es Private porque para 
        /// obtenerse estos datos desde el cliente debe llamar a la clase Workflow.
        /// </summary>
        [Private]
        public EntityCollection<EstadoInforme> EstadoInformeObtenerSiguientes(int estadoID, int circuitoID)
        {
            string args = "sigs_" + estadoID.ToString() + "_" + circuitoID.ToString();
            // Se toma la libertad de cachearlo por thread
            EntityCollection<EstadoInforme> ret = ResultsThreadCache<EstadoInforme>.GetResults(args);
            // Si lo obtuvo, devuelve...
            if (ret != null)
                return ret;

            // IMPORTANTE: si se introduce un cambio en este método, reflejar esa lógica en 
            // la clase Workflow dentro de ClientComponents/Cache.

            // Los busca...
            IQuery query = dalEngine.CreateQuery("SELECT DISTINCT ewf.Destino " +
                "From EstadoInformeWorkflow ewf " +
                "WHERE ewf.CircuitoInforme = :circuitoInforme " +
                "AND ewf.Origen = :estadoID ");

            query.SetParameter("estadoID", estadoID);
            query.SetParameter("circuitoInforme", circuitoID);

            ret = dalEngine.GetManyByQuery<EstadoInforme>(query);

            // Los guarda en el thread-caché... esto optmiza las llamadas
            // por lote...
            ResultsThreadCache<EstadoInforme>.SetResults(args, ret);

            return ret;
        }

        [Private]
        public TurnoInforme TurnoInformeAvanzarEstado(int informeID, EstadoInformeEnum estado, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            return TurnoInformeAvanzarEstado(informeID, (int)estado, modalidadCoseguro);
        }

        [Private]
        public TurnoInforme TurnoInformeAvanzarEstado(TurnoInforme informe, EstadoInformeEnum estado, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            return TurnoInformeAvanzarEstado(informe.Id, (int)estado, modalidadCoseguro);
        }

        public TurnoInforme TurnoInformeAvanzarEstado(int informeId, int estado, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeId);
            EstadoInforme eiNuevo = EstadoInformeReadById(estado);

            if (informe.CircuitoInforme != null)
                return TurnoInformeAvanzarEstado(informe, eiNuevo, informe.CircuitoInforme.Id, modalidadCoseguro);
            else
                return new TurnoInforme();
        }

        public TurnoInforme TurnoInformeAvanzarEstado(int informeId, int estado, int circuitoId, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeId);
            EstadoInforme eiNuevo = EstadoInformeReadById(estado);

            return TurnoInformeAvanzarEstado(informe, eiNuevo, circuitoId, modalidadCoseguro);
        }

        /// <summary>
        /// Cambia el Estado de un TurnoInforme
        /// </summary>
        /// <param name="informe">Informe</param>
        /// <param name="estado">Estado al cual pasar</param>
        /// <param name="modalidadCoseguro">Usuario de la Operación</param>
        /// <remarks>
        /// - Se chequea que el nuevo estado sea válido via Workflow
        /// </remarks>
        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeAvanzarEstado(TurnoInforme informe, EstadoInforme eiNuevo, int circuitoId, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {

            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Chequeo si el cambio de estado es posible
            EntityCollection<EstadoInforme> posibles = EstadoInformeObtenerSiguientes(informe.EstadoInforme.Id, circuitoId);

            if (posibles.Contains(eiNuevo) == false && informe.EstadoInforme.Id != eiNuevo.Id)
                throw new StatusException("No puede cambiar el estado del informe de " + informe.EstadoInforme.Name + " a " + eiNuevo.Name);

            // Verifico que no deba cobranza
            if (eiNuevo.Id == (int)EstadoInformeEnum.Entregado)
            {
                Turno turno = TurnosDalc.TurnoReadById(informe.TurnoID);
                if (turno.RequiereCobranza && !turno.CobranzaVigenteID.HasValue)
                    throw new StatusException("No puede cambiar el estado del informe de " + informe.EstadoInforme.Name + " a " + eiNuevo.Name + " porque el turno tiene una cobranza pendiente");
            }

            // Verifico que si el informe esta publicadoHL7 entonces no puede revertirse de Informado
            if(informe.PublicadoHL7 && !eiNuevo.InformeInformado)
                throw new StatusException("No puede cambiar el estado del informe de " + informe.EstadoInforme.Name + " a " + eiNuevo.Name + " porque el informe esta publicado HL7");

            // Actualizo el Estado
            informe.EstadoInforme = eiNuevo;
            informe.CircuitoInforme = new CircuitoInforme { Id = circuitoId };

            // Desbloqueo el informe
            informe.UsuarioBloqueando = null;

            // Actualizo el Informe
            informe = dalEngine.Update(informe);

            // Guardo en el Historico
            EstadoInformeHistoricoCreate(false, informe);

            TurnosDalc.CambiarEstadoAlTurnoSiCorrespondeSegunElDelInforme(informe, modalidadCoseguro);

            // Chequeo si el informe es principal y el estado implica que se deben 
            // de cambiar los estados del los informes secundarios
            if (informe.UnificacionPrincipal == true &&
                eiNuevo.InformeUnificado == true)
            {
                EntityCollection<TurnoInforme> informesSecundarios = TurnoInformesSecundariosReadByTurnoInformePrincipalId(informe.Id);
                CircuitoInforme cir = dalEngine.GetById<CircuitoInforme>(circuitoId);
                foreach (TurnoInforme ti in informesSecundarios)
                {
                    ti.CircuitoInforme = cir;
                    TurnoInformeAvanzarEstado(ti.Id, eiNuevo.Id, modalidadCoseguro);
                }
            }

            return informe;
        }

        [Private]
        public bool EstadoInformeBuscarEnSiguientes(EstadoInforme origen, EstadoInforme requerido, int circuitoID)
        {
            bool esta = false;

            /*
             * Recorro el workflow del circuito partiendo desde el estado origen buscando el requerido
             * Chequeo si el estado que necesito (requerido) es uno de los siguientes al estado de origen
             * Si no, Recorro la cadena (de los siguientes al del origen) buscando el estado requerido
             */
            EntityCollection<EstadoInforme> estados = EstadoInformeObtenerSiguientes(origen.Id, circuitoID);

            if (estados.Count > 0)
            {
                if (estados.Contains(requerido))
                    esta = true;
                else
                    for (int i = 0; !esta && i < estados.Count; i++)
                        esta = EstadoInformeBuscarEnSiguientes(estados[i], requerido, circuitoID);
            }

            return esta;
        }

        /// <summary>
        /// Vuelve el informe a su estado anterior
        /// </summary>
        /// <param name="informeID">El ID del informe</param>
        /// <returns>El nuevo status</returns>
        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeRevertirEstado(int informeID)
        {
            return TurnoInformeRevertirEstado(informeID, false);
        }

        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeRevertirEstado(int informeID, bool esRechazo)
        {
            // Obtengo el Informe de la Base
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeID);

            return TurnoInformeRevertirEstado(informe, esRechazo);
        }

        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeRevertirEstado(TurnoInforme informe, bool esRechazo)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Traigo el Último Turno del Historico
            EstadoInformeHistorico actual = EstadoInformeHistoricoReadLast(informe.Id);
            if (actual.HistoricoPrevio == null)
                throw new Exception("No se pudo revertir porque no hay estado previo para el informe.");

            EstadoInformeHistorico previo = actual.HistoricoPrevio;

            EntityCollection<TurnoInforme> informesSecundarios = new EntityCollection<TurnoInforme>();

            // Si el estado actual indica que se pasaron todos juntos, entonces los revierto respecto a la Unificación
            // que tengan, entonces voy a buscar y chequear el tema de los secundarios, sino no.
            if (actual.Estado.InformeUnificado == true)
            {
                informesSecundarios = TurnoInformesSecundariosReadByTurnoInformePrincipalId(informe.Id);

                if (TurnoInformeUnificadoPosibleReverirEstado(informe, informesSecundarios, actual) == false)
                    throw new Exception(
                        "No se puede revertir el estado del informe dado que existen informes unificados con diferentes estados.");
            }

            if (esRechazo)
                informe.CantidadRechazos += 1;

            // Si estoy revirtiendo desde el estado "A Entregar", entonces implica que tengo que quitarle la asignación de plaquero que pudiera tener.
            if (informe.EstadoInforme.Id == (int)EstadoInformeEnum.AEntregar)
                informe.PlaqueroId = null;

            // Actualizo el Estado en el Informe
            informe.EstadoInforme = previo.Estado;

            // Actualizo el Informe
            informe = dalEngine.Update(informe);

            // Actualizo el Historico - Utilizo como Previo del nuevo el Previo del Previo (para poder seguir revirtiendo en cadena)
            EstadoInformeHistoricoCreate(true, informe, previo.HistoricoPrevio);

            // Chequeo si estoy saliendo de un estado del informe con estado del turno o desunificando un turno
            if (actual.Estado.EstadoTurno != null)
            {
                // Busco el Estado del Turno al cual tengo que revertir
                EstadoTurno aRevertir = EstadoTurnoAnterior(previo);

                if (aRevertir != null)
                {
                    // Chequeo si el resto de los informes ya pasaron por el estado que le corresponderia ir al turno
                    EntityCollection<TurnoInforme> informes = TurnoInformeReadByTurno(informe.TurnoID);

                    bool revertir = true;
                    foreach (TurnoInforme ti in informes)
                    {
                        // Salteo el informe actual
                        if (ti.Id != informe.Id)
                            revertir = EstadoTurnoBuscarEnAnteriores(aRevertir, ti.Id);
                    }

                    if (revertir)
                    {
                        // En caso que si, tengo que revertir el estado del turno también
                        Turno turno = TurnosDalc.TurnoReadById(informe.TurnoID);

                        while (turno.EstadoTurnoID != aRevertir.Id)
                            turno = TurnosDalc.TurnoRevertirEstado(turno, (CircuitoEnum)aRevertir.CircuitoId);
                    }
                }
            }

            // Revierto el estado para los informes secundarios si corresponde.
            if (informe.UnificacionPrincipal == true &&
                previo.Estado.InformeUnificado == true)
                foreach (TurnoInforme informeSecundario in informesSecundarios)
                    TurnoInformeRevertirEstado(informeSecundario, esRechazo);

            return informe;
        }

        /// <summary>
        /// Determina en base a un informe si es posible revertir su estado solo en el caso de Informes Unificados.
        /// </summary>
        /// <param name="informe">TurnoInforme a analizar.</param>
        /// <returns>Retorno true o false segun se pueda o no revertir el estado.</returns>
        [Private]
        public bool TurnoInformeUnificadoPosibleReverirEstado(TurnoInforme informe)
        {
            EntityCollection<TurnoInforme> informesSecundarios
                = TurnoInformesSecundariosReadByTurnoInformePrincipalId(informe.Id);

            return TurnoInformeUnificadoPosibleReverirEstado(informe, informesSecundarios);
        }

        /// <summary>
        /// Determina en base a un informe si es posible revertir su estado solo en el caso de Informes Unificados.
        /// </summary>
        /// <param name="informe">TurnoInforme a analizar.</param>
        /// <param name="informesSecundarios">Coleccion de informes secundarios sobre los cuales se quiere chequear la posibilidad de revertir estado.</param>
        /// <returns>Retorno true o false segun se pueda o no revertir el estado.</returns>
        [Private]
        public bool TurnoInformeUnificadoPosibleReverirEstado(TurnoInforme informe, EntityCollection<TurnoInforme> informesSecundarios)
        {
            EstadoInformeHistorico actual = EstadoInformeHistoricoReadLast(informe.Id);

            // El estado Pendiente no tiene ningun estado anterior.
            if (actual.Estado.Id == (int)EstadoInformeEnum.Pendiente)
                return true;

            if (actual.HistoricoPrevio == null)
                throw new Exception("No se pudo revertir porque no hay estado previo para el informe.");

            return TurnoInformeUnificadoPosibleReverirEstado(informe, informesSecundarios, actual);
        }

        public void TurnoInformeSobreUpdate(int TurnoInformeId, int? SobreId)
        {
            List<int> ids = new List<int>();
            ids.Add(TurnoInformeId);
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(ids, TurnoInforme.Properties.SobreId, SobreId);
        }

        public InformeListaView InformeListaViewReadByTurnoInformeId(int turnoInformeId)
        {
            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            hqlBuilder.Append("and  tui.Id = :turnoInformeId ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter(turnoInformeId, "turnoInformeId");
            return dalEngine.GetByQuery<InformeListaView>(query);
        }

        public EntityCollection<InformeListaView> InformeListaViewReadByTurnoInformePrincipal(int turnoInformePrincipalId)
        {
            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            hqlBuilder.Append("and tui.TurnoInformePrincipalID = :turnoInformePrincipalId ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter(turnoInformePrincipalId, "turnoInformePrincipalId");
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        public EntityCollection<InformeListaView> InformeListaViewReadByProtocolo(string protocolo)
        {
            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            hqlBuilder.Append("and tur.Orden.Protocolo.ProtocoloFull = :protocolo ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter(protocolo, "protocolo");
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }
        public EntityCollection<InformeListaView> InformeListaViewReadByTurno(int turnoId)
        {
            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            hqlBuilder.Append("and tur.Id = :turnoId ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter(turnoId, "turnoId");
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        /// <summary>
        /// Determina en base a un informe si es posible revertir su estado solo en el caso de Informes Unificados
        /// </summary>
        /// <param name="informe">TurnoInforme a analizar.</param>
        /// <param name="actual">EstadoInformeHistorico actual.</param>
        /// <returns>Retorno true o false segun se pueda o no revertir el estado.</returns>
        [Private]
        public bool TurnoInformeUnificadoPosibleReverirEstado(TurnoInforme informe, EntityCollection<TurnoInforme> informesSecundarios, EstadoInformeHistorico actual)
        {
            // Si no le pasé ninguna lista o bien no contiene ningun informe, 
            // entonces si puede revertir el estado del "principal".
            if (informesSecundarios == null || informesSecundarios.Count < 1)
                return true;

            EstadoInformeHistorico previo = actual.HistoricoPrevio;

            // Chequeo si tengo que revertir el estado de los unificados.
            if (informe.UnificacionPrincipal == true &&
                previo.Estado.InformeUnificado == true)
            {
                Predicate<TurnoInforme> predicate
                    = delegate(TurnoInforme compare) { return compare.EstadoInforme != actual.Estado; };

                // Si encuentro algún informe con distinto estado al del principal, entonces salgo por excepcion
                if (informesSecundarios.Find(predicate) != null)
                    return false;
            }

            return true;
        }

        private EstadoTurno EstadoTurnoAnterior(EstadoInformeHistorico historico)
        {
            // Chequeo si no llegue al final
            if (historico != null)
            {
                if (historico.Estado.EstadoTurno != null)
                    return historico.Estado.EstadoTurno;
                return EstadoTurnoAnterior(historico.HistoricoPrevio);
            }
            return null;
        }

        private bool EstadoTurnoBuscarEnAnteriores(EstadoTurno requerido, int informeID)
        {
            return EstadoInformeHistoricoContieneEstadoTurno(EstadoInformeHistoricoReadLast(informeID), requerido);
        }

        private static bool EstadoInformeHistoricoContieneEstadoTurno(EstadoInformeHistorico actual, EstadoTurno requerido)
        {
            // Chequeo si no llegue al final
            if (actual != null)
            {
                if (requerido.Equals(actual.Estado.EstadoTurno))
                    return true;
                return EstadoInformeHistoricoContieneEstadoTurno(actual.HistoricoPrevio, requerido);
            }
            return false;
        }

        /// <summary>
        /// Revierto un TurnoInforme hasta llegar al Estado destino
        /// </summary>
        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeRevertirEstado(int informeID, EstadoInformeEnum destino)
        {
            // Obtengo el Informe de la Base
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeID);

            return TurnoInformeRevertirEstado(informe, destino);
        }

        /// <summary>
        /// Revierto un TurnoInforme hasta llegar al Estado destino
        /// </summary>
        [Private]
        [RequiresTransaction]
        public virtual TurnoInforme TurnoInformeRevertirEstado(TurnoInforme informe, EstadoInformeEnum destino)
        {
            // Sigo revirtiendo mientras no llegue al estado que busco
            while (informe.EstadoInforme.Id != (int)destino)
                informe = TurnoInformeRevertirEstado(informe, false);

            return informe;
        }


        // EstadoInformeHistorico
        internal void EstadoInformeHistoricoCreate(bool isRollback, TurnoInforme informe)
        {
            // Utilizo Historico Actual del Informe como Previo del nuevo
            EstadoInformeHistoricoCreate(isRollback, informe, EstadoInformeHistoricoReadLast(informe.Id));
        }

        internal void EstadoInformeHistoricoCreate(bool isRollback, TurnoInforme informe, EstadoInformeHistorico previo)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Obtengo el actual por si estoy revirtiendo
            EstadoInformeHistorico anterior = EstadoInformeHistoricoReadLast(informe.Id);

            EstadoInformeHistorico eih = new EstadoInformeHistorico();

            SecurityUser user = Security.Current.UserInfo.User;
            // Asigno los nuevos datos del Historico
            eih.TurnoInforme = informe;
            eih.Estado = informe.EstadoInforme;
            eih.HistoricoPrevio = previo;
            eih.Fecha = enfoke.Time.Now;
            eih.User = user;


            // Guardo
            eih = dalEngine.Update<EstadoInformeHistorico>(eih);

            // Registro Log del Turno
            StringBuilder sb = new StringBuilder();
            sb.Append("Se ").Append(isRollback ? "revirtió" : "modificó").Append(" el estado del informe");
            if (informe.RegionInforme != null)
                sb.Append(" de la región ").Append(informe.RegionInforme.Name);
            sb.Append(" a '").Append(informe.EstadoInforme.Name).Append("'.");

            TurnosDalc.LogRegistrar((int)LogEventoEnum.CambioEstadoInforme, sb.ToString(), informe.TurnoID);

            // Registro Log del Informe
            TurnoInformeLogEnum? tile = TurnoInformeLog.TurnoInformeLogEnumGetByEstado((isRollback ? anterior : eih).Estado.Id);
            if (tile.HasValue)
                TurnoInformeLogUpdate(informe.Id, tile.Value, !isRollback);
        }

        internal EntityCollection<EstadoInformeHistorico> EstadoInformeHistoricoReadByTurnoInformeID(int idTurnoInforme)
        {
            return dalEngine.GetManyByProperty<EstadoInformeHistorico>(
                EstadoInformeHistorico.Properties.TurnoInforme.Id, idTurnoInforme,
                EstadoInformeHistorico.Properties.Id, enfoke.Data.SortOrder.Descending);
        }

        internal void EstadoInformeHistoricoDelete(EntityCollection<EstadoInformeHistorico> eih)
        {
            dalEngine.Delete(eih);
        }

        internal void EstadoInformeHistoricoDeleteByTurnoInformeID(int idTurnoInforme)
        {
            EntityCollection<EstadoInformeHistorico> eihCollection = EstadoInformeHistoricoReadByTurnoInformeID(idTurnoInforme);
            EstadoInformeHistoricoDelete(eihCollection);
        }

        private EstadoInformeHistorico EstadoInformeHistoricoReadLast(int informeID)
        {
            EntityCollection<EstadoInformeHistorico> historicos = dalEngine.GetManyByProperty<EstadoInformeHistorico>(EstadoInformeHistorico.Properties.TurnoInforme.Id, informeID, EstadoInformeHistorico.Properties.Id, enfoke.Data.SortOrder.Descending);

            return (historicos != null && historicos.Count > 0) ? historicos[0] : null;
        }


        // TurnoInforme
        [Private]
        public void TurnoInformeUpdateSobre(int turnoInforme, int? sobreId)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(new List<int>() { turnoInforme }, TurnoInforme.Properties.SobreId, sobreId);
        }






        public EntityCollection<TurnoInforme> TurnoInformesReadByIds(List<int> ids)
        {
            return dalEngine.GetManyByIds<TurnoInforme>(ids);
        }

        [Private]
        public string ProtocoloCodigoReadByTurnoInforme(int turnoInformeId)
        {
            string hql = "select tur.Orden.Protocolo.ProtocoloFull from Turno tur, TurnoInforme tui " +
                         "where tur.Id = tui.TurnoID and tui.Id = :turnoInformeId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("turnoInformeId", turnoInformeId);

            object protocoloFull = query.UniqueResult();

            if (protocoloFull != null)
                return (string)protocoloFull;

            return null;
        }


        /// <summary>
        /// Recupero los datos de un informe con los datos del protocolo del turno correspondiente.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public TurnoInforme TurnoInformeWithProcoloReadById(int id)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            TurnoInforme ti = dalEngine.GetById<TurnoInforme>(id);
            ti.Protocolo = TurnosDalc.ProtocoloReadByTurno(ti.TurnoID);

            return ti;
        }

        [AnonymousMethod]
        public EntityCollection<TurnoInforme> TurnoInformeReadByTurno(int turnoID)
        {
            return dalEngine.GetManyByProperty<TurnoInforme>(TurnoInforme.Properties.TurnoID, turnoID);
        }

        public EntityCollection<TurnoInforme> TurnoInformeReadByTurno(int turnoID, bool withTurnoInformePrincipal)
        {
            EntityCollection<TurnoInforme> tis = TurnoInformeReadByTurno(turnoID);

            if (withTurnoInformePrincipal == true)
                foreach (TurnoInforme ti in tis)
                    if (ti.TurnoInformePrincipalID.HasValue == true)
                        ti.TurnoInformePrincipal = dalEngine.GetById<TurnoInforme>(ti.TurnoInformePrincipalID.Value);

            return tis;
        }

        /// <summary>
        /// [GG][JR] Seteo la fecha de entrega prometida para el informe
        /// </summary>
        /// <param name="turnoID">Id del turno a marcar los informes</param>
        /// <param name="fechaPrometida">Fecha prometida</param>
        [AnonymousMethod]
        [RequiresTransaction]
        public virtual void TurnoInformeSetEntregaPrometida(int turnoID, DateTime fechaPrometida)
        {
            // Obtengo los Informes del Turno
            EntityCollection<TurnoInforme> informes = TurnoInformeReadByTurno(turnoID);

            List<int> informesIDs = new List<int>();
            foreach (TurnoInforme informe in informes)
                informesIDs.Add(informe.Id);

            // Los Marco como Prometidos
            TurnoInformeSetEntregaPrometida(informesIDs, fechaPrometida);
        }


        /// <summary>
        /// Seteo la fecha de entrega prometida para el informe
        /// </summary>
        /// <param name="informes">Ids de los informes</param>
        /// <param name="fechaPrometida">Fecha prometida</param>
        [RequiresTransaction]
        public virtual void TurnoInformeSetEntregaPrometida(List<int> informes, DateTime fechaPrometida)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            int turnoID = 0;

            // Marco la Entrega Prometida y seteo la Fecha
            foreach (int informeID in informes)
            {
                // Obtengo el Informe de la Base
                TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeID);

                informe.Prometido = true;
                informe.FechaEntrega = fechaPrometida;

                // Guardo
                informe = dalEngine.Update(informe);

                turnoID = informe.TurnoID;
            }

            // Logueo
            if (turnoID > 0)
            {
                TurnosDalc.LogRegistrar((int)LogEventoEnum.InformePrometido, "Se prometió el informe para el " + DateTimeUtils.FormatDateTime(fechaPrometida) + ".", turnoID);

                Turno turno = TurnosDalc.TurnoReadById(turnoID);
                turno.FechaEntregaInforme = fechaPrometida;
                TurnosDalc.TurnoUpdate(turno);
            }
        }

        /// <summary>
        /// Seteo el tipo de informe para el estudio
        /// </summary>
        /// <param name="informes">Ids de los informes</param>
        /// <param name="tipo">Tipo de Informe</param>
        [RequiresTransaction]
        public virtual EntityCollection<TurnoInforme> TurnoInformeSetTipoInforme(List<int> informesIds, TipoInforme tipo)
        {
            EntityCollection<TurnoInforme> informes = TurnoInformeReadByIds(informesIds);

            foreach (TurnoInforme informe in informes)
                informe.TipoInforme = tipo;

            dalEngine.UpdateCollection(informes);
            return informes;
        }

        [RequiresTransaction]
        public virtual void BloquearInforme(int turnoInformeId)
        {
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(turnoInformeId);
            informe.UsuarioBloqueando = Security.Current.UserInfo.User;
            dalEngine.Update(informe);


            // Me fijo si el informe es el principal de algunos. De esos, los bloqueo
            EntityCollection<TurnoInforme> infUnificados = Context.Session.Dalc.GetManyByProperty<TurnoInforme>(TurnoInforme.Properties.TurnoInformePrincipalID, turnoInformeId);
            if (infUnificados != null && infUnificados.Count > 0)
            {
                foreach (TurnoInforme ti in infUnificados)
                    ti.UsuarioBloqueando = Security.Current.UserInfo.User;
            }

            Context.Session.Dalc.UpdateCollection(infUnificados);
        }

        [RequiresTransaction]
        public virtual void DesbloquearInforme(int turnoInformeId)
        {
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(turnoInformeId);
            informe.UsuarioBloqueando = null;
            dalEngine.Update(informe);

            // Me fijo si el informe es el principal de algunos. De esos, los desbloqueo
            EntityCollection<TurnoInforme> infUnificados = Context.Session.Dalc.GetManyByProperty<TurnoInforme>(TurnoInforme.Properties.TurnoInformePrincipalID, turnoInformeId);
            if (infUnificados != null && infUnificados.Count > 0)
            {
                foreach (TurnoInforme ti in infUnificados)
                    ti.UsuarioBloqueando = null;
            }

            Context.Session.Dalc.UpdateCollection(infUnificados);
        }

        [Private]
        [RequiresTransaction]
        public virtual void TurnoEntregarInforme(int turnoID, TipoEntregaInformeEnum tipoEntrega, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<TurnoInforme> informes = TurnoInformesCambiarTipoEntregaInforme(turnoID, tipoEntrega);

            // Cambio el estado de los Informes a Entregado
            foreach (TurnoInforme informe in informes)
                TurnoInformeAvanzarEstado(informe.Id, EstadoInformeEnum.Entregado, modalidadCoseguro);

        }

        [Private]
        [RequiresTransaction]
        public virtual void TurnoEntregarInforme(List<int> informeIds, TipoEntregaInformeEnum tipoEntrega, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<TurnoInforme> informes = TurnoInformeCambiarTipoEntregaInforme(tipoEntrega, informeIds);

            // Cambio el estado de los Informes a Entregado
            foreach (TurnoInforme informe in informes)
                TurnoInformeAvanzarEstado(informe.Id, EstadoInformeEnum.Entregado, modalidadCoseguro);

        }

        [RequiresTransaction]
        protected virtual EntityCollection<TurnoInforme> TurnoInformesCambiarTipoEntregaInforme(int turnoID, TipoEntregaInformeEnum tipoEntrega)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Actualizo los Informes
            EntityCollection<TurnoInforme> informes = TurnoInformeReadByTurno(turnoID);

            TurnoInformeCambiarTipoEntregaInforme(tipoEntrega, informes);
            return informes;
        }

        [RequiresTransaction]
        public virtual EntityCollection<TurnoInforme> TurnoInformeCambiarTipoEntregaInforme(TipoEntregaInformeEnum tipoEntrega, List<int> informeIds)
        {
            EntityCollection<TurnoInforme> informes = TurnoInformesReadByIds(informeIds);
            TurnoInformeCambiarTipoEntregaInforme(tipoEntrega, informes);
            return informes;
        }

        private void TurnoInformeCambiarTipoEntregaInforme(TipoEntregaInformeEnum tipoEntrega, EntityCollection<TurnoInforme> informes)
        {
            if (informes == null || informes.Count == 0)
                return;

            int canalEntregaID = tipoEntrega == TipoEntregaInformeEnum.EntregadoMedico ? (int)CanalEntregaInformeEnum.EntregaMedico : (int)CanalEntregaInformeEnum.EntregaCanalNormal;

            TipoEntregaInforme tei = dalEngine.GetById<TipoEntregaInforme>((int)tipoEntrega);
            CanalEntregaInforme cei = dalEngine.GetById<CanalEntregaInforme>(canalEntregaID);
            int turnoId = 0;


            for (int i = 0; i < informes.Count; i++)
            {
                TurnoInforme informe = informes[i];
                turnoId = informes[i].TurnoID;
                informe.TipoEntregaInforme = tei;
                informe.CanalEntregaInforme = cei;

                dalEngine.Update(informe);
            }


            // Registro Log
            Context.Session.TurnosDalc.LogRegistrar((int)LogEventoEnum.EntregaInforme, "Se " + (tipoEntrega == TipoEntregaInformeEnum.NoEntregado ? "revirtió" : "realizó") + " la entrega del informe.", turnoId);
            Context.Session.TurnosDalc.TurnoLogUpdate(turnoId, TurnoLogFechasEnum.EntregaInforme, true);

        }

        [RequiresTransaction]
        public virtual void TurnoRevertirEntregaInforme(int turnoID, Sector sector)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            EntityCollection<TurnoInforme> informes = TurnoInformesCambiarTipoEntregaInforme(turnoID, TipoEntregaInformeEnum.NoEntregado);

            // Revierto el estado de los Informes
            foreach (TurnoInforme informe in informes)
                TurnoInformeRevertirEstado(informe, false);

            // Limpio la Cache del Sistema de Colas
            TurnosDalc.SistemaColaCacheItemClean(sector, turnoID, TipoDeColaEnum.Entrega);
        }

        [Private]
        [RequiresTransaction]
        public virtual void TurnoInformesAprobarDelInformante(int turnoID, SecurityUser userInf, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            EntityCollection<TurnoInforme> informes = TurnoInformeReadByTurno(turnoID);

            // Intento aprobar cada Informe del Informante
            for (int i = 0; i < informes.Count; i++)
            {
                TurnoInforme informe = informes[i];
                TurnoInforme informeUsar = DeterminarInformeUsar(informe);

                if (informeUsar.Informante != null && informeUsar.Informante.Id == userInf.Id)
                {
                    EntityCollection<EstadoInforme> siguientes = EstadoInformeObtenerSiguientes(informeUsar.EstadoInforme.Id, informeUsar.CircuitoInforme.Id);

                    if (EstadoInforme.ListContains(siguientes, EstadoInformeEnum.Aprobado))
                        TurnoInformeAvanzarEstado(informeUsar.Id, EstadoInformeEnum.Aprobado, modalidadCoseguro);
                }
            }
        }

        private TurnoInforme DeterminarInformeUsar(TurnoInforme informe)
        {
            TurnoInforme tiUsar = informe;

            if (informe.TurnoInformePrincipalID.HasValue == true)
            {
                if (informe.TurnoInformePrincipal == null)
                    informe.TurnoInformePrincipal
                        = dalEngine.GetById<TurnoInforme>(informe.TurnoInformePrincipalID.Value);

                tiUsar = informe.TurnoInformePrincipal;
            }

            return tiUsar;
        }

        public TurnoInforme TurnoInformeReadByTurnoAndRegion(int turnoId, int regionId)
        {
            StringBuilder hql = new StringBuilder();

            hql.Append(" from TurnoInforme tui ");
            hql.Append(" where tui.TurnoID = :turnoId");
            hql.Append("   and tui.RegionInforme.Id = :regionId");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("turnoId", turnoId);
            query.SetParameter("regionId", regionId);
            return dalEngine.GetByQuery<TurnoInforme>(query);
        }

        [AnonymousMethod()]
        public OrdenInformeEnvio OrdenInformeEnvioReadByOrdenId(int ordenId)
        {
            OrdenInformeEnvio envio = dalEngine.GetByProperty<OrdenInformeEnvio>(OrdenInformeEnvio.Properties.OrdenID, ordenId);
            if (envio == null)
                return null;
            return envio;
        }

        public OrdenInformeEnvio OrdenInformeEnvioReadById(int id)
        {
            OrdenInformeEnvio envio = dalEngine.GetByProperty<OrdenInformeEnvio>(OrdenInformeEnvio.Properties.Id, id);
            if (envio == null)
                return null;
            return envio;
        }






        [AnonymousMethod()]
        public void OrdenInformeEnvioDeleteByOrdenId(int ordenId)
        {
            OrdenInformeEnvio envio = OrdenInformeEnvioReadByOrdenId(ordenId);


            dalEngine.Delete(envio);
        }

        [AnonymousMethod()]
        public int OrdenInformeEnvioExistsByOrdenId(int ordenId)
        {
            OrdenInformeEnvio envio = OrdenInformeEnvioReadByOrdenId(ordenId);
            return (envio != null ? envio.Id : 0);
        }

        internal void TurnoInformeDeleteMany(EntityCollection<TurnoInforme> tiCollection)
        {
            dalEngine.Delete(tiCollection);
        }

        internal bool ExisteInformePrometidoByIdTurno(int idTurno)
        {
            const string hql = "SELECT count(ti.TurnoID) FROM TurnoInforme ti " +
                               "WHERE ti.TurnoID = :idTurno AND ti.Prometido = true ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurno", idTurno);

            object ret = query.UniqueResult();

            if (ret != null)
                return (int.Parse(ret.ToString()) > 0);
            else
                return false;
        }

        internal bool ExisteInformePrometidoByIdTurnoInforme(int idTurnoInforme)
        {
            const string hql = "SELECT count(ti.Id) FROM TurnoInforme ti " +
                               "WHERE ti.Id = :idTurnoInforme AND ti.Prometido = true ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurnoInforme", idTurnoInforme);

            object ret = query.UniqueResult();

            if (ret != null)
                return (int.Parse(ret.ToString()) > 0);
            else
                return false;
        }



        // TurnoInformeUnificado

        /// <summary>
        /// Se encarga de devolver el TurnoInforme Principal de una unificación.
        /// (No valida si el ID pertenece o no a una unificación)
        /// </summary>
        /// <param name="turnoInformeID">ID del TurnoInforme Principal o de alguno de sus Secundarios</param>
        /// <returns>TurnoInforme Principal</returns>
        public TurnoInforme TurnoInformePrincipalReadByTurnoInforme(TurnoInforme turnoInforme)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            TurnoInforme ti = turnoInforme;

            if (ti.TurnoInformePrincipalID.HasValue == true && ti.Id != ti.TurnoInformePrincipalID.Value)
                ti = dalEngine.GetById<TurnoInforme>(ti.TurnoInformePrincipalID.Value);

            ti.Protocolo = TurnosDalc.ProtocoloReadByTurno(ti.TurnoID);

            return ti;
        }

        /// <summary>
        /// Se encarga de devolver el TurnoInforme Principal de una unificación.
        /// (No valida si el ID pertenece o no a una unificación)
        /// </summary>
        /// <param name="turnoInformeID">ID del TurnoInforme Principal o de alguno de sus Secundarios</param>
        /// <returns>TurnoInforme Principal</returns>
        public TurnoInforme TurnoInformePrincipalReadById(int turnoInformeID)
        {
            TurnoInforme ti = dalEngine.GetById<TurnoInforme>(turnoInformeID);

            return TurnoInformePrincipalReadByTurnoInforme(ti);
        }

        public List<int> TurnoInformesSecundariosIdsReadByTurnoInformePrincipalId(int turnoInformeID)
        {
            EntityCollection<TurnoInformeUnificado> informes = TurnoInformesUnificadosSecundariosReadByTurnoInformePrincipalId(turnoInformeID);
            List<int> ids = new List<int>();

            foreach (TurnoInformeUnificado informe in informes)
                ids.Add(informe.TurnoInformeId);

            return ids;
        }

        public EntityCollection<TurnoInforme>
            TurnoInformesSecundariosReadByTurnoInformePrincipalId(int turnoInformeID)
        {
            return dalEngine.GetManyByProperty<TurnoInforme>
                (TurnoInforme.Properties.TurnoInformePrincipalID, turnoInformeID);
        }

        public EntityCollection<TurnoInformeUnificado>
            TurnoInformesUnificadosSecundariosReadByTurnoInformePrincipalId(int turnoInformeID)
        {
            // Obtengo el encabezado de la consulta sin la junta con RegionInformante
            string hql = GetHqlEncabezadoUnificacionInformes((int?)null);
            hql += "AND ti.TurnoInformePrincipalHQL.Id = :idTurnoInforme " +
                   "ORDER BY ti.Id ASC, pt.Tipo.Id ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurnoInforme", turnoInformeID);

            return ProcesarDatos(dalEngine.GetManyByQuery<TurnoInformeUnificado>(query));
        }

        [Private]
        public EntityCollection<TurnoInformeUnificado> TurnoInformesSecundariosPosiblesReadByTurnoInformeId(int turnoInformeID, Dictionary<string, int> rangoDias)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            EquiposDalc EquiposDalc = Context.Session.EquiposDalc;

            TurnoInforme turnoInforme = dalEngine.GetById<TurnoInforme>(turnoInformeID);
            turnoInforme.Turno = TurnosDalc.TurnoReadById(turnoInforme.TurnoID);
            turnoInforme.Turno.Orden.Protocolo = TurnosDalc.ProtocoloReadByTurno(turnoInforme.TurnoID);
            // Necesito el Equipo para aplicar Sumar/Restar Dias Laborales
            if (turnoInforme.Turno.Equipo == null && turnoInforme.Turno.EquipoId.HasValue)
                turnoInforme.Turno.Equipo = EquiposDalc.EquipoReadById(turnoInforme.Turno.EquipoId.Value);

            DateTime fechaAnterior = turnoInforme.Turno.Fecha.Value;
            if (rangoDias["Anterior"] >= 0)
                fechaAnterior = TurnosDalc.SumarDiasLaborales((turnoInforme.Turno.Equipo != null ? turnoInforme.Turno.Equipo.Sucursal.Id : (int?)null), turnoInforme.Turno.Fecha.Value, -rangoDias["Anterior"]);

            DateTime fechaPosterior = turnoInforme.Turno.Fecha.Value;
            if (rangoDias["Posterior"] >= 0)
                fechaPosterior = TurnosDalc.SumarDiasLaborales((turnoInforme.Turno.Equipo != null ? turnoInforme.Turno.Equipo.Sucursal.Id : (int?)null), turnoInforme.Turno.Fecha.Value, rangoDias["Posterior"]);

            // Obtengo el encabezado de la consulta con la junta con RegionInformante
            string hql = GetHqlEncabezadoUnificacionInformes(turnoInforme.Id);
            hql += "AND ti.TurnoInformePrincipalHQL.Id IS NULL " +
                   "AND ti.UnificacionPrincipal = false " +
                   "AND ti.Id != :idTurnoInforme " +
                   "AND ti.TurnoHQL.Orden.Paciente.Id = :idPaciente " +
                   "AND ti.TurnoHQL.Fecha BETWEEN :desde AND :hasta " +
                   "AND ti.EstadoInforme.Id = :estadoInforme " +
                   "AND ri.Informante.Id = :idInformante " +
                   "AND ti.TurnoHQL.Equipo.Sucursal.Id = :idCentro " +
                   "AND ti.TurnoHQL.Estado.Id IN (:estadoTurno) " +
                   (turnoInforme.Turno.Orden.MedicoSolicitanteID.HasValue ? "AND ti.TurnoHQL.Orden.MedicoSolicitante.Id = :idMedicoDerivante " : string.Empty) +
                   "ORDER BY ti.Id ASC, pt.Tipo.Id ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurnoInforme", turnoInforme.Id);
            query.SetParameter("idPaciente", turnoInforme.Turno.Orden.PacienteId);
            query.SetParameter("desde", fechaAnterior.Date);
            query.SetParameter("hasta", fechaPosterior.AddDays(1).Date);
            query.SetParameter("estadoInforme", (int)EstadoInformeEnum.Pendiente);
            // Busco siempre todos los informes posibles para el usuario logueado.
            query.SetParameter("idInformante", Data.Security.Current.UserInfo.User.Id);
            query.SetParameter("idCentro", turnoInforme.Turno.Orden.Protocolo.SucursalID);
            query.SetParameterList("estadoTurno", new int[] { 
                (int)EstadoTurnoEnum.Recepcionado, 
                (int)EstadoTurnoEnum.InicioPractica, 
                (int)EstadoTurnoEnum.FinPractica });

            if (turnoInforme.Turno.Orden.MedicoSolicitanteID.HasValue)
                query.SetParameter("idMedicoDerivante", turnoInforme.Turno.Orden.MedicoSolicitanteID.Value);

            EntityCollection<TurnoInformeUnificado> datos = dalEngine.GetManyByQuery<TurnoInformeUnificado>(query);

            // ProcesarDatos es para poder devolver una lista con cada uno de los TurnoInforme
            // y que estos tengan una lista de las practicas relacionadas.
            // Porque de no hacerlo devolveria tantos TurnoInformes repetidos como Practicas tengan.
            return ProcesarDatos(datos);
        }

        private EntityCollection<TurnoInformeUnificado> ProcesarDatos(EntityCollection<TurnoInformeUnificado> datos)
        {
            EntityCollection<TurnoInformeUnificado> informes = new EntityCollection<TurnoInformeUnificado>();

            // Hago un "Distinct" entre los TurnoInformes
            foreach (TurnoInformeUnificado dato in datos)
            {
                // Predicado
                Predicate<TurnoInformeUnificado> predicate = delegate(TurnoInformeUnificado compare)
                {
                    return compare.TurnoInformeId == dato.TurnoInformeId;
                };


                if (informes.Find(predicate) == null)
                    informes.Add(dato);
            }

            // A cada uno de los TurnoInformes le busco cual es el que figura en el "Distinct"
            // y le agrego los datos de las practicas.
            foreach (TurnoInformeUnificado dato in datos)
            {
                // Predicado
                Predicate<TurnoInformeUnificado> predicate = delegate(TurnoInformeUnificado compare)
                {
                    return compare.TurnoInformeId == dato.TurnoInformeId;
                };


                informes.Find(predicate).Practicas.Add(new PracticaName(dato.PracticaId, dato.Practica));
            }

            return informes;
        }

        /// <summary>
        /// Arma el encabeza con el select, from y condiciones necesarias obligatorias 
        /// para determinar informes unificados (tanto posibles como ya generados).
        /// </summary>
        /// <param name="idInforme">Id del TurnoInforme sobre el cual se pretende buscar posibles secundarios (solo para éste caso).
        /// Sino se lo deja null éste parametro.
        /// El pasar un id de informe se hará la junta necesaria con RegionInformante e InformeUnificacion.</param>
        /// <returns>Encabezado de consulta.</returns>
        private string GetHqlEncabezadoUnificacionInformes(int? idInforme)
        {
            int idServicio = 0;
            bool existenServiciosEnInformeUnificacion = false;

            if (idInforme.HasValue == true)
            {
                string hql = "SELECT ti.TurnoHQL.Equipo.Servicio.Id " +
                             "FROM TurnoInformeHQL ti " +
                             "WHERE ti.Id = :idInforme ";

                IQuery query = dalEngine.CreateQuery(hql);
                query.SetParameter("idInforme", idInforme.Value);

                idServicio = (int)query.UniqueResult();

                hql = "SELECT iu.Id " +
                      "FROM InformeUnificacion iu " +
                      "WHERE (iu.ServicioPrincipalID = :idServicio OR iu.ServicioUnificadoID = :idServicio) ";

                query = dalEngine.CreateQuery(hql);
                query.SetParameter("idServicio", idServicio);

                query.SetMaxResults(1);
                object result = query.UniqueResult();

                existenServiciosEnInformeUnificacion = result != null && (int)result > 0;
            }

            return "SELECT DISTINCT new enfoke.Eges.Entities.Results.TurnoInformeUnificado(ti.TurnoHQL.Id, " +
                                                                                      "ti.Id, " +
                                                                                      "ti.TurnoHQL.Orden.Protocolo.ProtocoloFull, " +
                                                                                      "ti.TurnoHQL.Orden.Protocolo.Id, " +
                                                                                      "ti.TurnoHQL.Fecha, " +
                                                                                      "ti.FechaEntrega, " +
                                                                                      "ti.FechaEntregaOriginal, " +
                                                                                      "ti.RegionInforme.Name, " +
                                                                                      "ti.RegionInforme.Id, " +
                                                                                      "ti.TurnoHQL.Equipo.Servicio.Name, " +
                                                                                      "ti.TurnoHQL.Equipo.Servicio.Id, " +
                                                                                      "ti.TurnoHQL.Equipo.Descripcion, " +
                                                                                      "ti.TurnoHQL.Equipo.Id, " +
                                                                                      "pt.Practica.Name, " +
                                                                                      "pt.Practica.Id, " +
                                                                                      "pt.Tipo.Id, " +
                                                                                      "ti.Prometido) " +
                   "FROM TurnoInformeHQL AS ti, PracticaTurnoHQL AS pt " +

                   // Agrego en el FROM de la consulta la junta con la tabla RegionInformante
                   (idInforme.HasValue == true ?
                       ", RegionInformanteHQL AS ri " +
                       (existenServiciosEnInformeUnificacion == true ?
                            ("JOIN ri.Informante.RegionInformanteHQL AS ri2 " +
                            "LEFT JOIN ri.ServicioHQL.InformeUnificacionPrincipal AS iu ") :
                            string.Empty) :
                       string.Empty) +

                   "WHERE ti.TurnoHQL.Id = pt.Turno.Id " +
                   "AND pt.Cantidad > 0 " +
                   "AND ti.RegionInforme.Id = pt.RegionInforme.Id " +

                   // En caso de haber juntado con la tabla RegionInformante, 
                // agrego la clausulas correspondientes.
                   (idInforme.HasValue == true ?
                        ((existenServiciosEnInformeUnificacion == true ?
                            ("AND ti.RegionInforme.Id = ri2.RegionInforme.Id " +
                            "AND ri.Informante = ri2.Informante " +
                            "AND ri.Sucursal.Id = ri2.Sucursal.Id " +
                            "AND ri.Deleted = ri2.Deleted " +
                            "AND ((iu.ServicioPrincipalID = " + idServicio.ToString() + " AND iu.ServicioUnificadoID = ri2.ServicioHQL.Id AND ti.TurnoHQL.Equipo.Servicio.Id = iu.ServicioUnificadoID) " +
                            " OR ( iu.ServicioUnificadoID =  " + idServicio.ToString() + " AND iu.ServicioPrincipalID = ri2.ServicioHQL.Id AND ti.TurnoHQL.Equipo.Servicio.Id = iu.ServicioPrincipalID))"
                            ) :
                            "AND ti.TurnoHQL.Equipo.Servicio.Id = ri.ServicioHQL.Id "
                        ) +
                        "AND ti.TurnoHQL.Equipo.Sucursal.Id = ri.Sucursal.Id " +
                        "AND ri.Deleted = false "
                        ) :
                        string.Empty
                   );
        }

        public DateTime? FechaEntregaMayorReadByTurnoInforme(List<int> turnoInformesIds)
        {
            string hql = "SELECT MAX(ti.FechaEntrega) FROM TurnoInforme AS ti " +
                       "WHERE ti.Id IN (:informesIDs) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("informesIDs", turnoInformesIds);

            object fechaMaxima = query.UniqueResult();

            if (fechaMaxima != null)
                return (DateTime?)fechaMaxima;

            return null;
        }

        public TurnoInformeUnificado TurnoInformeUnificadoPrincipalReadById(int informePrincipalId)
        {
            string hql = GetHqlEncabezadoUnificacionInformes((int?)null);
            hql += "AND ti.TurnoInformePrincipalHQL.Id IS NULL " +
                   "AND ti.Id = :idTurnoInforme " +
                   "ORDER BY ti.Id ASC, pt.Tipo.Id ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurnoInforme", informePrincipalId);

            EntityCollection<TurnoInformeUnificado> col = ProcesarDatos(dalEngine.GetManyByQuery<TurnoInformeUnificado>(query));
            if (col != null && col.Count > 0)
                return col[0];

            throw new Exception("No se encontró el informe principal de la unificación");
        }

        public ComposicionInformeUnificado ComposicionInformeUnificadoReadByInformeId(int turnoInformeId)
        {
            ComposicionInformeUnificado composicion = new ComposicionInformeUnificado();

            // Busco el informe para controlar si es el principal
            TurnoInforme tInforme = dalEngine.GetById<TurnoInforme>(turnoInformeId);

            // Si no es el principal, lo busco
            if (tInforme.TurnoInformePrincipalID.HasValue == true)
                tInforme = dalEngine.GetById<TurnoInforme>(tInforme.TurnoInformePrincipalID.Value);

            composicion.TurnoInformeUnificadoPrincipal = TurnoInformeUnificadoPrincipalReadById(tInforme.Id);
            composicion.ItemsTurnoInformeUnificado = TurnoInformesUnificadosSecundariosReadByTurnoInformePrincipalId(tInforme.Id);

            return composicion;
        }

        public EntityCollection<InformeListaView> InformeListaViewReadByTurnoInformePrincipalId(int turnoInformeId)
        {
            EntityCollection<InformeListaView> informes = new EntityCollection<InformeListaView>();
            informes.Add(InformeListaViewReadByTurnoInformeId(turnoInformeId));
            informes.AddRange(InformeListaViewReadByTurnoInformePrincipalId(turnoInformeId));

            return informes;
        }

        /// <summary>
        /// Recupero para un turno todos los informes principales relacioados relacionados 
        /// </summary>
        /// <param name="turnoID"></param>
        /// <returns></returns>
        public EntityCollection<TurnoInforme> TurnoInformePrincipalesReadByTurno(int turnoID)
        {
            EntityCollection<TurnoInforme> informes = new EntityCollection<TurnoInforme>();
            EntityCollection<TurnoInforme> informesPrincipales = new EntityCollection<TurnoInforme>();

            informes = TurnoInformeReadByTurno(turnoID);
            foreach (TurnoInforme informe in informes)
            {
                TurnoInforme infPrincipal = TurnoInformePrincipalReadById(informe.Id);
                if (!informesPrincipales.Contains(infPrincipal))
                    informesPrincipales.Add(infPrincipal);
            }

            return informesPrincipales;
        }

        [Private]
        public void TurnoInformeUnificacionSecundariosUpdate(int turnoInformePrincipalID, List<int> turnoInformeSecundariosIDs, DateTime? fechaEntrega)
        {
            TurnoInformeUnificacionSecundariosUpdate(
                dalEngine.GetById<TurnoInforme>(turnoInformePrincipalID),
                turnoInformeSecundariosIDs,
                fechaEntrega);
        }

        [Private]
        public void TurnoInformeUnificacionSecundariosUpdate(int turnoInformePrincipalID, EntityCollection<TurnoInforme> turnoInformeSecundarios, DateTime? fechaEntrega)
        {
            TurnoInformeUnificacionSecundariosUpdate(
                dalEngine.GetById<TurnoInforme>(turnoInformePrincipalID),
                turnoInformeSecundarios,
                fechaEntrega);
        }

        [Private]
        public void TurnoInformeUnificacionSecundariosUpdate(TurnoInforme turnoInformePrincipal, List<int> turnoInformeSecundariosIDs, DateTime? fechaEntrega)
        {
            TurnoInformeUnificacionSecundariosUpdate(
                turnoInformePrincipal,
                dalEngine.GetManyByIds<TurnoInforme>(turnoInformeSecundariosIDs),
                fechaEntrega);
        }

        [Private]
        public void TurnoInformeUnificacionSecundariosUpdate(TurnoInforme turnoInformePrincipal, EntityCollection<TurnoInforme> turnoInformeSecundarios, DateTime? fechaEntrega)
        {
            EntityCollection<TurnoInformeLog> turnoInformeLogs = new EntityCollection<TurnoInformeLog>();

            foreach (TurnoInforme informe in turnoInformeSecundarios)
            {
                ValidarInformeBloqueoParaUnificacion(informe);

                //informe.FechaEntrega = fechaEntrega;
                informe.TurnoInformePrincipalID = turnoInformePrincipal.Id;
                informe.CircuitoInforme = turnoInformePrincipal.CircuitoInforme;

                TurnoInformeLog log = TurnoInformeLogReadByInforme(informe.Id);
                log.TurnoInformePrincipalId = turnoInformePrincipal.Id;
                log.UnificacionFecha = enfoke.Time.Now;
                turnoInformeLogs.Add(log);
            }

            dalEngine.UpdateCollection(turnoInformeSecundarios);
            dalEngine.UpdateCollection(turnoInformeLogs);
        }

        private void ValidarInformeBloqueoParaUnificacion(TurnoInforme informe)
        {
            if (informe.UsuarioBloqueando != null && informe.UsuarioBloqueando.Id != Security.Current.UserInfo.User.Id)
            {
                if (informe.Turno == null)
                    informe.Turno = Context.Session.TurnosDalc.TurnoReadById(informe.TurnoID);

                string error = "El informe de la región " + informe.RegionInforme.Name + " del turno con fecha " + DateTimeUtils.FormatDate(informe.Turno.Fecha.Value) + " esta siendo bloqueado por el usuario " + informe.UsuarioBloqueando.ApyN + ". \r\nLa operación se cancela.";

                throw new NotLoggeableException(error);
            }
        }

        [Private]
        public void TurnoInformeUnificacionPrincipalUpdate(TurnoInforme turnoInformePrincipal, DateTime? fechaEntrega)//, int circuitoId)
        {
            //turnoInformePrincipal.CircuitoInforme = dalEngine.GetById<CircuitoInforme>(circuitoId);
            turnoInformePrincipal.FechaEntrega = fechaEntrega;
            turnoInformePrincipal.UnificacionPrincipal = true;

            turnoInformePrincipal = dalEngine.Update(turnoInformePrincipal);
        }

        [Private]
        public void TurnoInformeRevertirUnificacionSecundariosUpdate(List<int> turnoInformesSecundariosIDs)
        {
            TurnoInformeRevertirUnificacionSecundariosUpdate(
                dalEngine.GetManyByIds<TurnoInforme>(turnoInformesSecundariosIDs));
        }

        [Private]
        public void TurnoInformeRevertirUnificacionSecundariosUpdate(EntityCollection<TurnoInforme> turnoInformesSecundarios)
        {
            EntityCollection<TurnoInformeLog> turnoInformeLogs = new EntityCollection<TurnoInformeLog>();

            foreach (TurnoInforme informe in turnoInformesSecundarios)
            {
                //informe.FechaEntrega = informe.FechaEntregaOriginal;
                informe.TurnoInformePrincipalID = null;
                informe.CircuitoInforme = null;

                TurnoInformeLog log = TurnoInformeLogReadByInforme(informe.Id);
                log.TurnoInformePrincipalId = null;
                log.UnificacionFecha = null;
                turnoInformeLogs.Add(log);
            }

            dalEngine.UpdateCollection(turnoInformesSecundarios);
            dalEngine.UpdateCollection(turnoInformeLogs);
        }

        [Private]
        public void TurnoInformeRevertirUnificacionPrincipalUpdate(int turnoInformePrincipalID)
        {
            TurnoInformeRevertirUnificacionPrincipalUpdate(
                dalEngine.GetById<TurnoInforme>(turnoInformePrincipalID));
        }

        [Private]
        public void TurnoInformeRevertirUnificacionPrincipalUpdate(TurnoInforme turnoInformePrincipal)
        {
            turnoInformePrincipal.FechaEntrega = turnoInformePrincipal.FechaEntregaOriginal;
            turnoInformePrincipal.UnificacionPrincipal = false;

            turnoInformePrincipal = dalEngine.Update(turnoInformePrincipal);
        }

        public bool TurnoInformesSecundariosExistenReadByTurnoInformePrincipalId(int turnoInformePrincipalID)
        {
            string hql = "SELECT COUNT(ti.Id) FROM TurnoInformeHQL AS ti " +
                         "WHERE ti.TurnoInformePrincipalHQL.Id = :idTurnoInforme ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idTurnoInforme", turnoInformePrincipalID);

            object cantidadInformesSecundarios = query.UniqueResult();

            if (cantidadInformesSecundarios != null &&
                Convert.ToInt32(cantidadInformesSecundarios) > 0)
                return true;

            return false;
        }



        // TurnoInformeLog
        /// <summary>
        /// Actualiza el Log de un Informe
        /// </summary>
        /// <param name="informeID">ID del Informe a Actualizar el Log</param>
        /// <param name="log">Dato a Actualizar en el Log</param>
        /// <param name="setear">Marca si estoy logueando o des-logueando</param>
        [RequiresTransaction]
        public virtual void TurnoInformeLogUpdate(int informeID, TurnoInformeLogEnum log, bool setear)
        {
            try
            {
                SecurityUser user = Security.Current.UserInfo.User;
                SecurityUser userValue = null;
                DateTime? dateValue = null;
                if (setear)
                {
                    userValue = user;
                    dateValue = enfoke.Time.Now;
                }

                // Trae el item
                TurnoInformeLog logItem = dalEngine.GetByProperty<TurnoInformeLog>(
                                            TurnoInformeLog.Properties.TurnoInformeId, informeID);

                switch (log)
                {
                    case TurnoInformeLogEnum.Pendiente:
                        logItem.PendienteFecha = dateValue;
                        logItem.PendienteUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Creado:
                        logItem.CreadoFecha = dateValue;
                        logItem.CreadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Grabado:
                        logItem.GrabadoFecha = dateValue;
                        logItem.GrabadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Dictado:
                        logItem.DictadoFecha = dateValue;
                        logItem.DictadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Tipeado:
                        logItem.TipeadoFecha = dateValue;
                        logItem.TipeadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Aprobado:
                        logItem.AprobadoFecha = dateValue;
                        logItem.AprobadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.ACompaginar:
                        logItem.ACompaginarFecha = dateValue;
                        logItem.ACompaginarUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Compaginado:
                        logItem.CompaginadoFecha = dateValue;
                        logItem.CompaginadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Escaneado:
                        logItem.EscaneadoFecha = dateValue;
                        logItem.EscaneadoUsuario = userValue;
                        break;
                    case TurnoInformeLogEnum.Entregado:
                        logItem.EntregadoFecha = dateValue;
                        logItem.EntregadoUsuario = userValue;
                        break;
                    default:
                        throw new Exception("Valor del item de Enum de log de informe " + log + " no reconocido.");
                }

                dalEngine.Update(logItem);
            }
            catch (Exception ex)
            {
                throw new Exception("Error al actualizar el log del turno " + informeID + ".", ex);
            }
        }

        public TurnoInformeLog TurnoInformeLogReadByInforme(int informeID)
        {
            EntityCollection<TurnoInformeLog> logs = dalEngine.GetManyByProperty<TurnoInformeLog>(TurnoInformeLog.Properties.TurnoInformeId, informeID);

            if (logs.Count > 0)
                return logs[0];
            return null;
        }






        internal void TurnoInformeLogDeleteByTurnoInformeID(int idTurnoInforme)
        {
            TurnoInformeLog til = TurnoInformeLogReadByInforme(idTurnoInforme);

            if (til != null)
                dalEngine.Delete(til);
        }



        // Pantalla Informes

        //[MinuteTimeout]
        //public virtual EntityCollection<InformeListaView> InformeListaViewReadForCompaginacion(string strPractica, string strServicio, string strMedico, int centroId, DateTime? fechaDesde, DateTime? fechaHasta, string strProtocolo, string strPaciente, string strDni, bool? entregaADomicilio, int estadoId, bool reemplazaSecundariosPorSuPrincipal)
        //{
        //    TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

        //    int? tipoSector = null;
        //    EntityCollection<TipoSector> tipoSectores = new EntityCollection<TipoSector>();
        //    if (entregaADomicilio.HasValue && entregaADomicilio.Value)
        //        tipoSectores = TurnosDalc.TipoSectorReadByTag("DOMIC");
        //    else if (entregaADomicilio.HasValue)
        //        tipoSectores = TurnosDalc.TipoSectorReadByTag("ENTIN");

        //    if (tipoSectores.Count > 0)
        //        tipoSector = tipoSectores[0].Id;

        //    List<EstadoInformeEnum> estados = new List<EstadoInformeEnum>();
        //    estados.Add((EstadoInformeEnum)estadoId);

        //    EntityCollection<InformeListaView> informes = InformeReadByParameters(estados, strMedico, strPaciente, strServicio, strProtocolo, "", fechaDesde, fechaHasta, centroId, new List<RegionInforme>(), new List<int>(), strPractica, strDni, tipoSector, null, -1);
        //    EntityCollection<InformeListaView> resultados = new EntityCollection<InformeListaView>();

        //    if (reemplazaSecundariosPorSuPrincipal)
        //        resultados = ReemplazarInformeSecundariosPorPrincipales(informes);
        //    else
        //        resultados = informes;

        //    return resultados;
        //}

        [Private]
        public EntityCollection<InformeListaView> ReemplazarInformeSecundariosPorPrincipales(EntityCollection<InformeListaView> informesSinReemplazarPorPrincipales)
        {
            EntityCollection<InformeListaView> informes = new EntityCollection<InformeListaView>();
            List<int> InformesEnListado = new List<int>();

            foreach (InformeListaView informeListaView in informesSinReemplazarPorPrincipales)
            {
                InformeListaView informeAIngresar = null;
                if (informeListaView.TurnoInformePrincipalId.HasValue)
                {
                    if (!InformesEnListado.Contains(informeListaView.TurnoInformePrincipalId.Value))
                        informeAIngresar = InformeListaViewReadByTurnoInformeId(informeListaView.TurnoInformePrincipalId.Value);
                }
                else if (!InformesEnListado.Contains(informeListaView.Id))
                    informeAIngresar = informeListaView;

                // Ingresa el que no es secundario (sera principal o no unificado)
                if (informeAIngresar != null)
                {
                    informes.Add(informeAIngresar);
                    InformesEnListado.Add(informeAIngresar.Id);
                }
            }

            return informes;
        }

        /// <summary>
        /// Este metodo trae los TurnoInforme Nivel 1 de un protocolo. Nivel 1: Son los informes del protocolo mas los relacionados por unificacion de los mismos.
        /// </summary>
        /// <param name="protocolo"></param>
        /// <param name="region"></param>
        [Private]
        public EntityCollection<TurnoInformeAnalisisUnificacion> InformesNivel1ReadByProtocolo(int protocoloId, string region)
        {
            // me traigo todos los turno informes del protocoloId/region
            string hql = "SELECT new enfoke.Eges.Entities.Results.TurnoInformeAnalisisUnificacion(tui.Id, tui.TurnoInformePrincipalID, tur.Orden.Protocolo.Id) " +
                " FROM Turno tur, TurnoInforme tui WHERE tui.TurnoID = tur.Id and tur.Orden.Protocolo.Id = :protocoloId ";

            if (!String.IsNullOrEmpty(region))
                hql += " and tui.RegionInforme.Tag = :region ";

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("protocoloId", protocoloId);
            if (!String.IsNullOrEmpty(region))
                query.SetParameter("region", region);

            EntityCollection<TurnoInformeAnalisisUnificacion> col = dalEngine.GetManyByQuery<TurnoInformeAnalisisUnificacion>(query);

            if (col.Count <= 0)
                return col;

            // Me fijo todos los turnos informes principales
            List<int> turnoInformesPrincipales = new List<int>();
            foreach (TurnoInformeAnalisisUnificacion item in col)
            {
                if (!turnoInformesPrincipales.Contains(item.TurnoInformePrincipalID.HasValue ? item.TurnoInformePrincipalID.Value : item.Id))
                    turnoInformesPrincipales.Add(item.TurnoInformePrincipalID.HasValue ? item.TurnoInformePrincipalID.Value : item.Id);
            }

            // Traigo todos los TurnoInformeAnalisisUnificacion que implican estos principales
            hql = "SELECT new enfoke.Eges.Entities.Results.TurnoInformeAnalisisUnificacion(tui.Id, tui.TurnoInformePrincipalID, tur.Orden.Protocolo.Id) " +
                " FROM Turno tur, TurnoInforme tui WHERE tui.TurnoID = tur.Id and (tui.TurnoInformePrincipalID IN (:informesPrincipales1) OR tui.Id IN (:informesPrincipales2)) ";

            query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("informesPrincipales1", turnoInformesPrincipales);
            query.SetParameterList("informesPrincipales2", turnoInformesPrincipales);

            return dalEngine.GetManyByQuery<TurnoInformeAnalisisUnificacion>(query);
        }

        [Private]
        public EntityCollection<InformeCompaginacionNivel1View> InformeCompaginacionNivel1ViewReadByProtocoloPadre(string protocolo, string region)
        {
            Filter filter = new Filter(new FilterItem(InformeCompaginacionNivel1View.Properties.ProtocoloCodigoNivel0, "=", protocolo));

            if (String.IsNullOrEmpty(region) == false)
                filter.Add(BooleanOp.And, InformeCompaginacionNivel1View.Properties.RegionTagNivel0, "=", region);

            EntityCollection<InformeCompaginacionNivel1View> col = dalEngine.GetManyByFilter<InformeCompaginacionNivel1View>(filter);
            return col;
        }

        [Private]
        public EntityCollection<InformeListaView> AgregarInformesDelasUnificaciones(EntityCollection<InformeListaView> informes, List<int> InformeListaIdAgregados)
        {
            EntityCollection<InformeListaView> informesResult = new EntityCollection<InformeListaView>();
            List<int> InformesEnListado = new List<int>();

            foreach (InformeListaView informeListaView in informes)
            {
                EntityCollection<InformeListaView> informeAIngresar = new EntityCollection<InformeListaView>();

                // Es un unificado
                if (informeListaView.EsUnificado)
                {
                    // Me fijo el principal
                    int turnoPrincipal;
                    if (informeListaView.TurnoInformePrincipalId.HasValue)
                        turnoPrincipal = informeListaView.TurnoInformePrincipalId.Value;
                    else
                        turnoPrincipal = informeListaView.Id;

                    if (!InformesEnListado.Contains(turnoPrincipal))
                    {
                        // Traigo todos los informes de la unificacion
                        informeAIngresar = InformeListaViewReadByTurnoInformePrincipalId(turnoPrincipal);
                        InformesEnListado.Add(turnoPrincipal);
                    }
                }
                else if (!InformesEnListado.Contains(informeListaView.Id))
                {
                    informeAIngresar.Add(informeListaView);
                    InformesEnListado.Add(informeListaView.Id);
                }

                // Ingresa los informes (pueden ser secundarios, principales o no unificados)
                if (informeAIngresar != null && informeAIngresar.Count > 0)
                {
                    // Agrego los informes que todavia no agregue
                    foreach (InformeListaView aIngresar in informeAIngresar)
                    {
                        if (!InformeListaIdAgregados.Contains(aIngresar.Id))
                        {
                            informesResult.Add(aIngresar);
                            InformeListaIdAgregados.Add(aIngresar.Id);
                        }
                    }
                }
            }

            return informesResult;
        }

        [MinuteTimeout]
        public virtual EntityCollection<InformeListaView> InformeListaViewReadByRegionInformates(
          EntityCollection<RegionInformante> regionInformates,
          List<int> lstEstados, string medico, string paciente,
          string protocolo, DateTime? fechaDesde, DateTime? fechaHasta, int? tipoMedico, string pacienteNroDoc, bool? informable, bool soloEquiposPACS, bool? soloPublicados, int maxRows)
        {
            Filter filter = new Filter();
            AgregaFiltros(lstEstados, medico, paciente, protocolo, filter, informable);
            AgregaFiltros(fechaDesde, fechaHasta, tipoMedico, filter);

            if (soloEquiposPACS)
                filter.Add(BooleanOp.And, InformeListaView.Properties.TienePacs, "=", true);

            if (soloPublicados.HasValue)
                filter.Add(BooleanOp.And, InformeListaView.Properties.PublicadoHL7, "=", soloPublicados.Value);

            int dni;
            if (!String.IsNullOrEmpty(pacienteNroDoc) && int.TryParse(pacienteNroDoc, out dni))
                filter.Add(BooleanOp.And, InformeListaView.Properties.PacienteNroDoc,
                        "=", dni);

            if (regionInformates != null && regionInformates.Count > 0)
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));
                foreach (RegionInformante ri in regionInformates)
                {
                    filter.Add(new OpenParenthesis(BooleanOp.Or));
                    filter.Add(InformeListaView.Properties.ServicioId, "=", ri.Servicio.Id);
                    filter.Add(BooleanOp.And, InformeListaView.Properties.SucursalId, "=", ri.Sucursal.Id);
                    filter.Add(BooleanOp.And, InformeListaView.Properties.RegionId, "=", ri.RegionInforme.Id);
                    filter.Add(new CloseParenthesis());
                }
                filter.Add(new CloseParenthesis());
            }

            if (maxRows == 0) maxRows = -1;

            return dalEngine.GetManyByFilter<InformeListaView>(filter, maxRows);
        }

        private static void AgregaFiltros(DateTime? fechaDesde, DateTime? fechaHasta, int? tipoMedico, Filter filter)
        {
            if (fechaDesde.HasValue)
                filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaPrevista,
                        ">=", fechaDesde.Value);
            if (fechaHasta.HasValue)
                filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaPrevista,
                        "<=", fechaHasta.Value.Date.AddDays(1));
            if (tipoMedico.HasValue && tipoMedico.Value != (int)TipoAtencionEnum.Ambos)
            {
                int requierePediatra = (tipoMedico.Value == (int)TipoAtencionEnum.Pediatra) ? 1 : 0;
                filter.Add(BooleanOp.And, InformeListaView.Properties.RequierePediatra,
                        "=", requierePediatra);
            }
        }

        private static void AgregaFiltros(List<int> lstEstados, string medico, string paciente, string protocolo, Filter filter, bool? informable)
        {
            if (informable.HasValue)
                filter.Add(BooleanOp.And, InformeListaView.Properties.Informable, "=", informable.Value);

            if (lstEstados != null && lstEstados.Count > 0)
                filter.Add(BooleanOp.And, InformeListaView.Properties.EstadoId, "IN", lstEstados);

            if (!String.IsNullOrEmpty(medico))
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));
                filter.Add(BooleanOp.And, InformeListaView.Properties.Informante, "LIKE", medico.Replace(" ", "%") + "%");
                filter.Add(new CloseParenthesis());
            }
            if (!String.IsNullOrEmpty(paciente))
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));
                filter.Add(BooleanOp.And, InformeListaView.Properties.Paciente, "LIKE", paciente.Replace(" ", "%") + "%");
                filter.Add(new CloseParenthesis());
            }
            if (!String.IsNullOrEmpty(protocolo))
                filter.Add(BooleanOp.And, InformeListaView.Properties.ProtocoloCodigo,
                        "=", protocolo);
        }

        private static string ConvertForLike(string condicion)
        {
            return condicion.Trim().Replace(' ', '%').ToUpper() + "%";
        }

        [MinuteTimeout]
        public virtual EntityCollection<InformeListaView> InformeReadByParameters(List<EstadoInformeEnum> lstEstados, string medico, string paciente,
                      string servicio, string protocolo, string regionTag,
                      DateTime? fechaDesde, DateTime? fechaHasta, int sucursal,
                      List<RegionInforme> lstRegion, List<int> lstCircuitoInforme,
                      string practica, string dni, bool? informable, bool soloEquiposPACS, bool? soloPublicados, int maxRows)
        {
            return InformeReadByParameters(lstEstados, medico, paciente, servicio, protocolo, regionTag, fechaDesde, fechaHasta, sucursal, lstRegion, lstCircuitoInforme, practica, dni, null, informable, soloEquiposPACS, soloPublicados, maxRows);
        }

        public EntityCollection<InformeListaView> InformesReadTodos(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            const int DIAS_POR_MES = 30;
            StringBuilder hqlBuilder = ConstruirSelectInformesConMedicoCentroRegionServicioYPendiente(centroId);
            hqlBuilder.Append("and (tui.FechaEntrega > :fechaHoraActual ");
            hqlBuilder.Append("or (tui.FechaEntrega < :fechaHoraActual ");
            hqlBuilder.Append("and tui.FechaEntrega >= :fechaHoraUltimoMes)) ");
            hqlBuilder.Append("order by tui.FechaEntrega, tur.Orden.Protocolo.ProtocoloFull asc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            PonerValoresCentroServicioRegionYPendiente(centroId, regionesIds, serviciosIds, query);
            query.SetInt32("medicoId", medicoId);
            query.SetDateTime("fechaHoraActual", enfoke.Time.Now);
            query.SetDateTime("fechaHoraUltimoMes", enfoke.Time.Now.AddDays(-DIAS_POR_MES));
            query.SetMaxResults(maxRows);
            EntityCollection<InformeListaView> informes = dalEngine.GetManyByQuery<InformeListaView>(query);
            this.AgregarPracticas(informes, true);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        public EntityCollection<InformeListaView> InformesReadPorVencer(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            StringBuilder hqlBuilder = ConstruirSelectInformesConMedicoCentroRegionServicioYPendiente(centroId);
            hqlBuilder.Append("and tui.FechaEntrega  > :fechaHoraActual ");
            hqlBuilder.Append("order by tui.FechaEntrega, tur.Orden.Protocolo.ProtocoloFull asc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            PonerValoresCentroServicioRegionYPendiente(centroId, regionesIds, serviciosIds, query);
            query.SetInt32("medicoId", medicoId);
            query.SetDateTime("fechaHoraActual", enfoke.Time.Now);
            query.SetMaxResults(maxRows);
            EntityCollection<InformeListaView> informes = dalEngine.GetManyByQuery<InformeListaView>(query);
            this.AgregarPracticas(informes, true);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        public EntityCollection<InformeListaView> InformesReadVencidosUltimaSemana(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            const int DIAS_POR_SEMANA = 7;
            StringBuilder hqlBuilder = ConstruirSelectInformesConMedicoCentroRegionServicioYPendiente(centroId);
            hqlBuilder.Append("and tui.FechaEntrega < :fechaHoraActual ");
            hqlBuilder.Append("and tui.FechaEntrega >= :fechaHoraUltimaSemana ");
            hqlBuilder.Append("order by tui.FechaEntrega, tur.Orden.Protocolo.ProtocoloFull desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            PonerValoresCentroServicioRegionYPendiente(centroId, regionesIds, serviciosIds, query);
            query.SetInt32("medicoId", medicoId);
            query.SetDateTime("fechaHoraActual", enfoke.Time.Now);
            query.SetDateTime("fechaHoraUltimaSemana", enfoke.Time.Now.AddDays(-DIAS_POR_SEMANA));
            query.SetMaxResults(maxRows);
            EntityCollection<InformeListaView> informes = dalEngine.GetManyByQuery<InformeListaView>(query);
            this.AgregarPracticas(informes, true);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        public EntityCollection<InformeListaView> InformesReadByInformesRecientes(List<int> informesIds, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            EntityCollection<InformeListaView> response = ObtenerInformesPorInformesIds(informesIds, serviciosIds, regionesIds, centroId, true, false, null, maxRows, false);
            this.AgregarPracticas(response, true);
            this.AgregarMedicoSolicitante(response);
            return response;
        }

        public EntityCollection<InformeListaView> InformesAprobacionTodos(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            // traigo maxRows de los dos con orden
            EntityCollection<InformeListaView> propios = InformesAprobacionPropios(medicoId, centroId, regionesIds, serviciosIds, maxRows);
            EntityCollection<InformeListaView> otros = InformesAprobacionOtrosMedicos(medicoId, centroId, regionesIds, serviciosIds, maxRows);
            // y hago merge manual de las dos cosas ya ordenadas
            EntityCollection<InformeListaView> response = MergeSortedCollectionByDate(propios, otros, maxRows);
            this.AgregarPracticas(response, true);
            this.AgregarMedicoSolicitante(response);
            return response;
        }

        private EntityCollection<InformeListaView> MergeSortedCollectionByDate(EntityCollection<InformeListaView> left, EntityCollection<InformeListaView> rigth, int maxRows)
        {
            int leftIndex = 0;
            int rigthIndex = 0;
            int maxCount = left.Count > rigth.Count ? left.Count : rigth.Count;
            EntityCollection<InformeListaView> response = new EntityCollection<InformeListaView>();
            int index = 0;
            while (index < maxRows)
            {
                if (leftIndex == left.Count && rigthIndex == rigth.Count)
                    break;
                else if (leftIndex == left.Count)
                {
                    response.Add(rigth[rigthIndex]);
                    rigthIndex++;
                    index++;
                    continue;
                }
                else if (rigthIndex == rigth.Count)
                {
                    response.Add(left[leftIndex]);
                    leftIndex++;
                    index++;
                    continue;
                }

                if (left[leftIndex].FechaEntregaPrevista.GetValueOrDefault(DateTime.MinValue) < rigth[rigthIndex].FechaEntregaPrevista.GetValueOrDefault(DateTime.MinValue))
                {
                    response.Add(left[leftIndex]);
                    leftIndex++;
                }
                else if (left[leftIndex].FechaEntregaPrevista.GetValueOrDefault(DateTime.MinValue) == rigth[rigthIndex].FechaEntregaPrevista.GetValueOrDefault(DateTime.MinValue))
                {
                    if (left[leftIndex].Protocolo.CompareTo(rigth[rigthIndex].Protocolo) < 0)
                    {
                        response.Add(left[leftIndex]);
                        leftIndex++;
                    }
                    else
                    {
                        response.Add(rigth[rigthIndex]);
                        rigthIndex++;
                    }
                }
                else
                {
                    response.Add(rigth[rigthIndex]);
                    rigthIndex++;
                }

                index++;
            }

            return response;
        }

        public EntityCollection<InformeListaView> InformesAprobacionOtrosMedicos(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            EntityCollection<RegionInformante> regiones = RegionInformanteReadNoEliminadosByInformante(medicoId);
            DateTime fechaDesde = DateTime.Now.Date.AddMonths(-1);
            List<int> informesIds = InformesIdsPorRegiones(medicoId, centroId, regionesIds, serviciosIds, regiones, maxRows, fechaDesde, null);
            EntityCollection<InformeListaView> informes = ObtenerInformesPorInformesIds(informesIds, null, null, null, false, false, null, maxRows, false);
            this.AgregarPracticas(informes, true);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        private EntityCollection<TurnoInforme> InformesPorMedicoCreadosOTipeados(int medicoId)
        {
            Filter filter = new Filter();
            filter.Add(TurnoInforme.Properties.Informante.Id, "=", medicoId);
            filter.Add(BooleanOp.And, TurnoInforme.Properties.EstadoInforme, "IN", new List<int>() { (int)EstadoInformeEnum.Creado, (int)EstadoInformeEnum.Tipeado });
            Sort sort = new Sort();
            sort.Add(TurnoInforme.Properties.FechaEntrega, SortingDirection.Asc);
            EntityCollection<TurnoInforme> informes = dalEngine.GetManyByFilter<TurnoInforme>(filter, sort);
            return informes;
        }



        private List<int> InformesIdsPorRegiones(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, EntityCollection<RegionInformante> regionesNoEliminadas, int maxRows, DateTime? fechaDesde, DateTime? fechaHasta)
        {
            if (regionesIds == null || regionesIds.Count == 0 || serviciosIds == null || serviciosIds.Count == 0 || regionesNoEliminadas == null || regionesNoEliminadas.Count == 0)
                return new List<int>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tui.Id from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est ");
            hqlBuilder.Append("where (tui.EstadoInforme.Id =  " + (int)EstadoInformeEnum.Creado);
            hqlBuilder.Append("or tui.EstadoInforme.Id =  " + (int)EstadoInformeEnum.Tipeado + ") ");
            hqlBuilder.Append(" and tui.TurnoID = tur.Id ");
            hqlBuilder.Append(" and tur.EquipoId = equ.Id ");
            hqlBuilder.Append(" and tur.EstadoTurnoID  = est.Id ");
            hqlBuilder.Append(" and est.Informable = true ");
            hqlBuilder.Append(" and tui.Informante.Id <> :medicoId ");
            hqlBuilder.Append(" and tui.RegionInforme.Id IN (:regionesIds) ");
            if (fechaDesde.HasValue)
                hqlBuilder.Append(" and tui.FechaEntrega >= :fechaDesde ");
            if (fechaHasta.HasValue)
                hqlBuilder.Append(" and tui.FechaEntrega < :fechaHasta ");

            hqlBuilder.Append(" and ((select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append(" and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(" ) > 0 ");
            
            hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append(" and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2)  ");
            hqlBuilder.Append(" ) > 0) ");


            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");

            hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");
            hqlBuilder.Append("order by tui.FechaEntrega desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("medicoId", medicoId);
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);

            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta);

            query.SetParameterList("serviciosIds", serviciosIds);
            query.SetParameterList("regionesIds", regionesIds);

            return new List<int>(query.List<int>());
        }

        public EntityCollection<InformeListaView> InformesAprobacionPropios(int medicoId, int? centroId, List<int> regionesIds, List<int> serviciosIds, int maxRows)
        {
            EntityCollection<TurnoInforme> informes = InformesPorMedicoCreadosOTipeados(medicoId);

            if (informes.Count <= 0)
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");

            hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");

            SQLBlockBuilder<int> idsInformes = new SQLBlockBuilder<int>(informes.GetIds());
            string informesBlock = idsInformes.BuildConstrainBlock("tui.Id");

            hqlBuilder.Append("and ").Append(informesBlock).Append(" ");

            hqlBuilder.Append("order by tui.FechaEntrega, tur.Orden.Protocolo.ProtocoloFull asc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            PonerValoresCentroServicioRegion(centroId, regionesIds, serviciosIds, query);

            query.SetMaxResults(maxRows);
            EntityCollection<InformeListaView> response = dalEngine.GetManyByQuery<InformeListaView>(query);
            this.AgregarPracticas(response, true);
            this.AgregarMedicoSolicitante(response);
            return response;
        }

        private static StringBuilder GetCabeceraConsultaInformeListaView()
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            return hqlBuilder;
        }

        private static void PonerValoresCentroServicioRegion(int? centroId, List<int> regionesIds, List<int> serviciosIds, IQuery query)
        {
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);

            query.SetParameterList("regionesIds", regionesIds);
            query.SetParameterList("serviciosIds", serviciosIds);
        }

        private static void PonerValoresCentroServicioRegionYPendiente(int? centroId, List<int> regionesIds, List<int> serviciosIds, IQuery query)
        {
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);

            query.SetParameterList("regionesIds", regionesIds);
            query.SetParameterList("serviciosIds", serviciosIds);
            query.SetInt32("pendiente", (int)EstadoInformeEnum.Pendiente);
        }

        [Private]
        public int TotalInformesParaAprobar(int medicoId, int maxRows)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select count(tui.Id) from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est ");
            hqlBuilder.Append("where tui.Informante.Id = :medicoId ");
            hqlBuilder.Append("and tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and (tui.EstadoInforme.Id = :creado or tui.EstadoInforme.Id = :tipeado) ");
            hqlBuilder.Append("and tur.EstadoTurnoID = est.Id ");
            hqlBuilder.Append("and est.Informable = true ");
            hqlBuilder.Append("and equ.Servicio.TipoServicio = :tipoImagenes ");


            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("medicoId", medicoId);
            query.SetInt32("creado", (int)EstadoInformeEnum.Creado);
            query.SetInt32("tipeado", (int)EstadoInformeEnum.Tipeado);
            query.SetInt32("tipoImagenes", (int)TipoServicioEnum.Imagenes);
            query.SetMaxResults(maxRows);
            object ret = query.UniqueResult();
            if (ret != null)
                return int.Parse(ret.ToString());
            else
                return 0;
        }

        [Private]
        public int TotalInformesPorVencidosUltimaSemana(int medicoId, int maxRows)
        {
            const int DIAS_POR_SEMANA = 7;
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select count(tui.Id) from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.EstadoTurnoID = est.Id ");
            hqlBuilder.Append("and est.Informable  = true ");
            hqlBuilder.Append("and (tui.Informante.Id = :medicoId ");
            hqlBuilder.Append("or ((select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0 ");

            hqlBuilder.Append("or (select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0)) ");



            hqlBuilder.Append("and equ.Servicio.TipoServicio = :tipoImagenes ");
            hqlBuilder.Append("and tui.EstadoInforme.Id = :pendiente ");
            hqlBuilder.Append("and tui.FechaEntrega < :fechaHoraActual ");
            hqlBuilder.Append("and tui.FechaEntrega >= :fechaHoraUltimaSemana ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("medicoId", medicoId);
            query.SetInt32("pendiente", (int)EstadoInformeEnum.Pendiente);
            query.SetInt32("tipoImagenes", (int)TipoServicioEnum.Imagenes);
            query.SetDateTime("fechaHoraActual", enfoke.Time.Now);
            query.SetDateTime("fechaHoraUltimaSemana", enfoke.Time.Now.AddDays(-DIAS_POR_SEMANA));
            query.SetMaxResults(maxRows);
            object ret = query.UniqueResult();
            if (ret != null)
                return int.Parse(ret.ToString());
            else
                return 0;
        }

        [Private]
        public int TotalInformesPorVencer(int medicoId, int maxRows)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select count(tui.Id) from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.EstadoTurnoID = est.Id ");
            hqlBuilder.Append("and est.Informable = true ");
            hqlBuilder.Append("and (tui.Informante.Id = :medicoId ");

            hqlBuilder.Append("or ((select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0 ");

            hqlBuilder.Append("or (select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0)) ");

            hqlBuilder.Append("and equ.Servicio.TipoServicio = :tipoImagenes ");
            hqlBuilder.Append("and tui.EstadoInforme.Id = :pendiente ");
            hqlBuilder.Append("and tui.FechaEntrega  > :fechaHoraActual ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetInt32("medicoId", medicoId);
            query.SetDateTime("fechaHoraActual", enfoke.Time.Now);
            query.SetInt32("pendiente", (int)EstadoInformeEnum.Pendiente);
            query.SetInt32("tipoImagenes", (int)TipoServicioEnum.Imagenes);
            query.SetMaxResults(maxRows);
            object ret = query.UniqueResult();
            if (ret != null)
                return int.Parse(ret.ToString());
            else
                return 0;
        }

        private void AgregarPracticas(EntityCollection<InformeListaView> informes, bool agregarChild)
        {
            if (informes == null || informes.Count == 0)
                return;

            SortedDictionary<int, InformeListaView> informesOrdenadosPorTurnos = ObtenerInformesPorTurno(informes);

            // traigo las practicas de todos los informes ordenadas por tipo
            List<int> ids = new List<int>();
            foreach (int key in informesOrdenadosPorTurnos.Keys)
                ids.Add(key);

            EntityCollection<PracticaTurno> practicas = Context.Session.TurnosDalc.PracticaTurnoReadByTurnosIdsOrdenadasPorTipo(ids);
            if (practicas.Count == 0)
                return;

            // tengo que tomar la primera por cada turno de las que vienen ordenadas.
            this.ActualizarInformesConPracticaPorTurno(informes, practicas, agregarChild);
        }

        private void AgregarMedicoSolicitante(EntityCollection<InformeListaView> informes)
        {
            if (informes == null || informes.Count == 0)
                return;

            SortedDictionary<int, EntityCollection<InformeListaView>> informesOrdenadosPorSolicitante = ObtenerInformesPorSolicitante(informes);
            EntityCollection<MedicoAsociacion> medicosAsociaciones = Context.Session.MedicosDalc.MedicoAsociacionReadByIds(informesOrdenadosPorSolicitante.Keys);
            foreach (MedicoAsociacion medicoAsciacion in medicosAsociaciones)
            {
                EntityCollection<InformeListaView> informesOrdenados = informesOrdenadosPorSolicitante[medicoAsciacion.Id];
                foreach (InformeListaView informe in informesOrdenados)
                {
                    informe.MedicoDerivanteImportancia = medicoAsciacion.Importancia;
                    informe.MedicoSolicitanteApellidoNombre = medicoAsciacion.FullName;
                }
            }
        }

        private SortedDictionary<int, EntityCollection<InformeListaView>> ObtenerInformesPorSolicitante(EntityCollection<InformeListaView> informes)
        {
            SortedDictionary<int, EntityCollection<InformeListaView>> informesOrdenadosPorInformantes = new SortedDictionary<int, EntityCollection<InformeListaView>>();
            foreach (InformeListaView informe in informes)
            {
                if (informe.MedicoSolicitanteId.HasValue)
                {
                    EntityCollection<InformeListaView> informesPorInformante;
                    if (!informesOrdenadosPorInformantes.TryGetValue(informe.MedicoSolicitanteId.Value, out informesPorInformante))
                    {
                        informesPorInformante = new EntityCollection<InformeListaView>();
                        informesOrdenadosPorInformantes.Add(informe.MedicoSolicitanteId.Value, informesPorInformante);
                    }

                    informesPorInformante.Add(informe);
                }
            }

            return informesOrdenadosPorInformantes;
        }

        private SortedDictionary<int, InformeListaView> ObtenerInformesPorTurno(EntityCollection<InformeListaView> informes)
        {
            SortedDictionary<int, InformeListaView> informesOrdenadosPorTurnos = new SortedDictionary<int, InformeListaView>();
            foreach (InformeListaView informe in informes)
            {
                if (!informesOrdenadosPorTurnos.ContainsKey(informe.TurnoId))
                    informesOrdenadosPorTurnos.Add(informe.TurnoId, informe);
            }

            return informesOrdenadosPorTurnos;
        }

        private void ActualizarInformesConPracticaPorTurno(EntityCollection<InformeListaView> informes, EntityCollection<PracticaTurno> practicas, bool agregarChild)
        {
            EntityCollection<RegionInforme> regionInformes = Context.Session.Dalc.GetAll<RegionInforme>();
            foreach (InformeListaView inf in informes)
            {
                bool cargoPrincipal = false;
                IEnumerable<PracticaTurno> prts = practicas.FindAll(delegate(PracticaTurno practicaTurno) { return practicaTurno.TurnoId == inf.TurnoId; });
                // debería ser siempre una práctica
                foreach (PracticaTurno prt in prts)
                {
                    if (prt.Cantidad > 0)
                    {
                        if (prt.RegionInformeID.GetValueOrDefault(0) == inf.RegionId.GetValueOrDefault(0) && !cargoPrincipal)
                        {
                            inf.MezclarConPracticaTurno(prt);
                            cargoPrincipal = true;
                        }
                        else if (agregarChild)
                            inf.AgregarInfomeListaChild(new InformeListaView(prt, regionInformes.FindByKey(prt.RegionInformeID.GetValueOrDefault(0))));
                    }
                }
            }
        }

        public EntityCollection<RegionInforme> RegionesPorServicio(List<int> serviciosIds)
        {
            if (serviciosIds == null || serviciosIds.Count == 0)
                return new EntityCollection<RegionInforme>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select new RegionInforme(pra.ServicioEspecialidad.Servicio.Id, rei) from Practica pra, RegionInforme rei ");
            hqlBuilder.Append("where pra.RegionInformeId = rei.Id and pra.ServicioEspecialidad.Servicio.Id in (:servicios) ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("servicios", serviciosIds);
            EntityCollection<RegionInforme> regiones = dalEngine.GetManyByQuery<RegionInforme>(query);
            return DistinctDeRegionInformeYServicios(regiones);
        }

        private static EntityCollection<RegionInforme> DistinctDeRegionInformeYServicios(EntityCollection<RegionInforme> regiones)
        {
            EntityCollection<RegionInforme> response = new EntityCollection<RegionInforme>();
            foreach (RegionInforme regionInforme in regiones)
            {
                RegionInforme region = response.Find(delegate(RegionInforme rei) { return rei.ServicioId == regionInforme.ServicioId && rei.Id == regionInforme.Id; });
                if (region == null)
                    response.Add(regionInforme);
            }
            return response;
        }

        private static StringBuilder ConstruirSelectInformesConMedicoCentroRegionServicioYPendiente(int? centroId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append("and est.Informable = true ");
            hqlBuilder.Append("and (tui.Informante.Id = :medicoId ");
            hqlBuilder.Append("or ((select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0 ");

            hqlBuilder.Append("or (select count(rei) from RegionInformante rei where ");
            hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
            hqlBuilder.Append("and rei.Informante.Id = :medicoId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
            hqlBuilder.Append(") > 0)) ");
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");

            hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");
            hqlBuilder.Append("and tui.EstadoInforme.Id = :pendiente ");
            return hqlBuilder;
        }


        public virtual EntityCollection<InformeListaView> InformesReadByProtocolo(string protocolo, int maxRows)
        {
            IList<int> turnosIds = Context.Session.TurnosDalc.TurnosIdsReadByProtocolo(protocolo);
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append(" and tui.TurnoID in (:turnos) ");
            hqlBuilder.Append("order by tui.FechaEntrega asc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameterList("turnos", turnosIds.ToEntityCollection());
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }


        //public virtual EntityCollection<InformeListaView> InformesReadByProtocolo(string protocolo, int maxRows)
        //{
        //    IList<int> turnosIds = Context.Session.TurnosDalc.TurnosIdsReadByProtocolo(protocolo);
        //    StringBuilder hqlBuilder = new StringBuilder();
        //    hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, cei, esi, tei, tii, tur.Id, tur.Fecha, tur.RequierePediatra, ord, pro, est, equ, pac) ");
        //    hqlBuilder.Append("from TurnoInforme tui join tui.CanalEntregaInforme cei join tui.EstadoInforme esi join tui.TipoEntregaInforme tei join tui.TipoInforme tii, Turno tur join tur.Orden ord join ord.Protocolo pro, Equipo equ, EstadoTurno est, PacienteLight pac ");
        //    hqlBuilder.Append("where tui.TurnoID = tur.Id ");
        //    hqlBuilder.Append("and tur.EquipoId = equ.Id ");
        //    hqlBuilder.Append("and ord.PacienteId = pac.Id ");
        //    hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
        //    hqlBuilder.Append(" and tui.TurnoID in (:turnos) ");
        //    hqlBuilder.Append("order by tui.FechaEntrega asc ");
        //    IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
        //    query.SetParameterList("turnos", turnosIds.ToEntityCollection());
        //    query.SetMaxResults(maxRows);
        //    return dalEngine.GetManyByQuery<InformeListaView>(query);
        //}

        [MinuteTimeout]
        public virtual EntityCollection<InformeListaView> InformeReadByParameters(int? grupoEstado, List<int> medicosInformantesIds,
                  int? pacienteId, List<int> serviciosIds, List<int> regionesIds, DateTime? fechaDesde, DateTime? fechaHasta,
                  int? sucursalId, bool fechaPorTurno, bool soloEquiposPACS, bool? soloPublicados, bool ascOrder, int maxRows, int? usuarioId)
        {
            EntityCollection<InformeListaView> informes = new EntityCollection<InformeListaView>();
            // Si publica HL7 entonces puede NO filtrar...
            if (!soloEquiposPACS && (serviciosIds == null || serviciosIds.Count == 0 || regionesIds == null || regionesIds.Count == 0))
                return informes;

            if (pacienteId.HasValue)
            {
                EntityCollection<TurnoLight> turnos = Context.Session.TurnosDalc.TurnoLightReadByPaciente(pacienteId.Value);
                informes = ObtenerInformesPorTurnos(grupoEstado, serviciosIds, regionesIds, sucursalId, fechaDesde, fechaHasta, fechaPorTurno, medicosInformantesIds, soloEquiposPACS, soloPublicados, maxRows, ascOrder, turnos, usuarioId);
            }
            else if (fechaDesde.HasValue || fechaHasta.HasValue)
            {
                List<int> informesIds = (fechaPorTurno) ? TurnoInformesIdsReadByGrupoEstadoYFechasTurno(grupoEstado, medicosInformantesIds, fechaDesde, fechaHasta, usuarioId)
                                                : ((grupoEstado.HasValue) ? TurnoInformesIdsReadByGrupoEstadoYFechas(grupoEstado, medicosInformantesIds, fechaDesde, fechaHasta, usuarioId)
                                                       : TurnoInformesIdsReadByFechas(fechaDesde, fechaHasta, medicosInformantesIds, usuarioId));
                informes = ObtenerInformesPorInformesIds(informesIds, serviciosIds, regionesIds, sucursalId, true, soloEquiposPACS, soloPublicados, maxRows, ascOrder);
            }
            else
                informes = ObtenerInformesPorFiltros(grupoEstado, medicosInformantesIds, serviciosIds, regionesIds, sucursalId, soloEquiposPACS, soloPublicados, maxRows, ascOrder, usuarioId);

            if (informes == null || informes.Count == 0)
                return new EntityCollection<InformeListaView>();

            this.AgregarPracticas(informes, true);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        private EntityCollection<EstadoInforme> EstadoInformeReadByGrupo(int grupo)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select est from EstadoInforme est where est.GrupoEstadoInforme = :grupo ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter("grupo", grupo);
            return dalEngine.GetManyByQuery<EstadoInforme>(query);
        }


        private bool ExistePendienteEnGrupo(int grupo)
        {
            EntityCollection<EstadoInforme> estados = EstadoInformeReadByGrupo(grupo);
            if (estados != null && estados.Count > 0)
                return estados.FindByKey((int)EstadoInformeEnum.Pendiente) != null;

            return false;
        }

        public EntityCollection<InformeListaView> ObtenerInformesPorFiltros(List<EstadoInformeEnum> estados, string medico, string paciente, string servicio,
            string protocolo, string regionTag, DateTime? fechaDesde, DateTime? fechaHasta, int sucursalId, List<RegionInforme> regiones,
            List<int> circuitos, string dni, bool? informable, int maxRows)
        {
            List<int> estadoIds = new List<int>();
            List<int> regIds = new List<int>();
            List<int> informeIds = new List<int>();
            string pac = string.Empty;
            EntityCollection<Paciente> pacientes = new EntityCollection<Paciente>();
            List<int> pacienteIds = new List<int>();
            int? turnoId = null;
            List<int> serviciosIds = new List<int>();
            int? centroId = null;

            if (sucursalId > 0)
                centroId = sucursalId;

            foreach (EstadoInformeEnum est in estados)
                estadoIds.Add((int)est);

            if (!string.IsNullOrEmpty(servicio))
            {
                EntityCollection<Servicio> servicios = Context.Session.ServiciosDalc.ServicioReadByName(servicio);
                if (servicios != null && servicios.Count > 0)
                    serviciosIds = servicios.GetIds();
            }

            if (regiones != null && regiones.Count > 0)
                foreach (RegionInforme reg in regiones)
                    regIds.Add(reg.Id);

            if (!string.IsNullOrEmpty(protocolo))
            {
                Turno turno = Context.Session.TurnosDalc.TurnoReadByProtocolo(protocolo);
                turnoId = turno.Id;
            }

            EntityCollection<InformeListaView> informesConLote = TurnoInformesIdsReadByTipeoConLote(estadoIds, paciente, dni, regIds, serviciosIds, centroId, circuitos, turnoId, medico, fechaDesde, fechaHasta, informable, regionTag, maxRows);
            EntityCollection<InformeListaView> informes = TurnoInformesIdsReadByTipeo(estadoIds, paciente, dni, regIds, serviciosIds, centroId, circuitos, turnoId, medico, fechaDesde, fechaHasta, informable, regionTag, informesConLote.GetIds(), maxRows);
            informes.AddRange(informesConLote);

            if (informes == null || informes.Count == 0)
                return new EntityCollection<InformeListaView>();

            this.AgregarPracticas(informes, false);
            this.AgregarMedicoSolicitante(informes);
            return informes;
        }

        private EntityCollection<InformeListaView> TurnoInformesIdsReadByTipeo(List<int> estadoIds, string pacApellido, string pacDocumento, List<int> regionesIds, List<int> serviciosIds, int? centroId, List<int> circuitos, int? turnoId, string medico, DateTime? desde, DateTime? hasta, bool? informable, string regionTag, List<int> informeIdsExluir, int maxResult)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select Select new enfoke.Eges.Entities.InformeListaView(tul, tur, est, equ, pac) from TurnoInforme tul, Turno tur, Equipo equ , EstadoTurno est, Paciente pac where ");
            IQuery query = QueryTurnoInformesIdsReadByTipeo(hqlBuilder, estadoIds, pacApellido, pacDocumento, regionesIds, serviciosIds, centroId, circuitos, turnoId, medico, desde, hasta, informable, regionTag, informeIdsExluir, maxResult);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }


        private EntityCollection<InformeListaView> TurnoInformesIdsReadByTipeoConLote(List<int> estadoIds, string pacApellido, string pacDocumento, List<int> regionesIds, List<int> serviciosIds, int? centroId, List<int> circuitos, int? turnoId, string medico, DateTime? desde, DateTime? hasta, bool? informable, string regionTag, int maxResult)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select Select new enfoke.Eges.Entities.InformeListaView(tul, tur, est, equ, pac, lt) from TurnoInforme tul, Turno tur, Equipo equ , EstadoTurno est, Paciente pac, TurnoInformeLoteTraslado tilt, LoteTraslado lt ");
            hqlBuilder.Append(" where tilt.TurnoInformeId = tul.Id ");
            hqlBuilder.Append(" and tilt.LoteTraslado.Id = lt.Id and ");
            IQuery query = QueryTurnoInformesIdsReadByTipeo(hqlBuilder, estadoIds, pacApellido, pacDocumento, regionesIds, serviciosIds, centroId, circuitos, turnoId, medico, desde, hasta, informable, regionTag, null, maxResult);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        private IQuery QueryTurnoInformesIdsReadByTipeo(StringBuilder hqlBuilder, List<int> estadoIds, string pacApellido, string pacDocumento, List<int> regionesIds, List<int> serviciosIds, int? centroId, List<int> circuitos, int? turnoId, string medico, DateTime? desde, DateTime? hasta, bool? informable, string regionTag, List<int> informeIdsExluir, int maxResult)
        {
            hqlBuilder.Append(" tur.EstadoTurnoID = est.Id ");
            hqlBuilder.Append(" and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append(" and tul.EstadoInforme.Id in (:estadoIds) ");

            if (informable.HasValue)
                hqlBuilder.Append("and est.Informable = true ");

            hqlBuilder.Append(" and tul.TurnoID = tur.Id ");
            hqlBuilder.Append(" and tur.EquipoId = equ.Id ");
            hqlBuilder.Append(" and est.Cancelado = false ");

            if (turnoId.HasValue)
                hqlBuilder.Append(" and tul.TurnoID = :turnoId ");

            hqlBuilder.Append(" and est.Atendido = true ");

            if (!string.IsNullOrEmpty(regionTag))
                hqlBuilder.Append(" and tul.RegionInforme.Tag = :regionTag ");

            if (!string.IsNullOrEmpty(pacApellido))
                hqlBuilder.Append(" and pac.Apellido like '%").Append(pacApellido).Append("%'");

            if (!string.IsNullOrEmpty(pacDocumento))
                hqlBuilder.Append(" and pac.Dni = ").Append(pacDocumento);

            if (hasta.HasValue && desde.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde and tul.FechaEntrega < :fechaHasta ");
            else if (desde.HasValue || (!desde.HasValue && !hasta.HasValue))
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde ");
            else if (hasta.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega < :fechaHasta ");

            if (!string.IsNullOrEmpty(medico))
                hqlBuilder.Append(" and tul.Informante.LastName like '%" + medico + "%'");

            if (circuitos.Count > 0)
                hqlBuilder.Append(" and  tul.CircuitoInforme.Id in (:circuitos) ");

            if (regionesIds != null && regionesIds.Count > 0)
                hqlBuilder.Append("and tul.RegionInforme.Id IN (:regionesIds) ");
            if (serviciosIds != null && serviciosIds.Count > 0)
                hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");

            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");


            if (informeIdsExluir != null && informeIdsExluir.Count > 0)
                hqlBuilder.Append(" and tul.Id not in (:informeIdsExluir) ");
            
            hqlBuilder.Append("order by tul.FechaEntrega desc ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());

            if (centroId.HasValue)
                query.SetParameter("centroId", centroId);

            if (regionesIds != null && regionesIds.Count > 0)
                query.SetParameterList("regionesIds", regionesIds);

            if (serviciosIds != null && serviciosIds.Count > 0)
                query.SetParameterList("serviciosIds", serviciosIds);

            if (desde.HasValue || (!desde.HasValue && !hasta.HasValue))
                query.SetDateTime("fechaDesde", desde.HasValue ? desde.Value.Date : DateTime.Now.Date.AddMonths(-1));
            if (hasta.HasValue)
                query.SetDateTime("fechaHasta", hasta.Value.Date.AddDays(1));

            query.SetParameterList("estadoIds", estadoIds);

            if (circuitos.Count > 0)
                query.SetParameterList("circuitos", circuitos);

            if (turnoId.HasValue)
                query.SetParameter("turnoId", turnoId);

            if (!string.IsNullOrEmpty(regionTag))
                query.SetParameter("regionTag", regionTag);

            if (informeIdsExluir != null && informeIdsExluir.Count > 0)
                query.SetParameterList("informeIdsExluir", informeIdsExluir);
            
            query.SetMaxResults(maxResult);
            return query;
        }


        private EntityCollection<InformeListaView> ObtenerInformesPorFiltros(int? grupoEstado, List<int> medicoInformanteIds, List<int> serviciosIds, List<int> regionesIds, int? centroId, bool soloEquiposPACS, bool? soloPublicados, int maxRows, bool ascOrder, int? usuarioId)
        {
            // Si publica HL7 entonces puede NO filtrar...
            if (!soloEquiposPACS && (serviciosIds == null || serviciosIds.Count == 0 || regionesIds == null || regionesIds.Count == 0))
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append("and est.Cancelado = false ");
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");
            if (medicoInformanteIds != null)
            {
                if (medicoInformanteIds.Count == 1)
                    hqlBuilder.Append("and tui.Informante.Id = :medicoInformanteId ");
                else
                    hqlBuilder.Append("and tui.Informante.Id IN (:medicoInformantesIds) ");
            }    

            if (soloEquiposPACS)
                hqlBuilder.Append("and tur.Fecha > equ.FechaIntegracionPacs ");
            if (soloPublicados.HasValue)
                hqlBuilder.Append("and tui.PublicadoHL7 = :soloPublicados ");
                

            if (grupoEstado.HasValue)
            {
                hqlBuilder.Append("and tui.EstadoInforme.GrupoEstadoInforme = :grupoEstado ");
                if (ExistePendienteEnGrupo(grupoEstado.Value))
                    hqlBuilder.Append("and est.Informable = true ");
            }


            if (usuarioId.HasValue)
            {
                hqlBuilder.Append("and ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append("and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(") > 0  ");

                hqlBuilder.Append("or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append("equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append("and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(") > 0 ) ");
            }

            hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");

            if (ascOrder)
                hqlBuilder.Append("order by tui.FechaEntrega asc ");
            else
                hqlBuilder.Append("order by tui.FechaEntrega desc ");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (grupoEstado.HasValue)
                query.SetInt32("grupoEstado", grupoEstado.Value);
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);
            if (medicoInformanteIds != null)
            {
                if (medicoInformanteIds.Count == 1)
                    query.SetInt32("medicoInformanteId", medicoInformanteIds[0]);
                else
                    query.SetParameterList("medicoInformantesIds", medicoInformanteIds);
            }  
            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);
            if (soloPublicados.HasValue)
                query.SetBoolean("soloPublicados", soloPublicados.Value);
            
            query.SetParameterList("regionesIds", regionesIds);
            query.SetParameterList("serviciosIds", serviciosIds);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        private List<int> TurnoInformesIdsReadByFechas(DateTime? desde, DateTime? hasta, List<int> informantesIds, int? usuarioId)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tul.Id from TurnoInforme tul , Turno tur, Equipo equ ");
            hqlBuilder.Append(" where tul.TurnoID = tur.Id ");
            hqlBuilder.Append(" and tur.EquipoId = equ.Id ");

            if (hasta.HasValue && desde.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde and tul.FechaEntrega < :fechaHasta ");
            else if (desde.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde ");
            else if (hasta.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega < :fechaHasta ");

            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    hqlBuilder.Append("and tul.Informante.Id = :medicoInformanteId ");
                else
                    hqlBuilder.Append("and tul.Informante.Id IN (:medicoInformantesIds) ");
            }
            
            if (usuarioId.HasValue)
            {
                hqlBuilder.Append(" and  ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0  ");

                hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0)  ");
            }

            hqlBuilder.Append("order by tul.FechaEntrega desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (desde.HasValue)
                query.SetDateTime("fechaDesde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetDateTime("fechaHasta", hasta.Value.Date.AddDays(1));
            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    query.SetInt32("medicoInformanteId", informantesIds[0]);
                else
                    query.SetParameterList("medicoInformantesIds", informantesIds);
            } 
            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);

            return new List<int>(query.List<int>());
        }

        private List<int> TurnoInformesIdsReadByGrupoEstadoYFechasTurno(int? grupoEstado, List<int> medicosInformantesIds, DateTime? desde, DateTime? hasta, int? usuarioId)
        {

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tul.Id from TurnoInforme tul, Turno tur, Equipo equ , EstadoTurno est ");
            hqlBuilder.Append("where tur.EstadoTurnoID = est.Id ");

            if (grupoEstado.HasValue)
                hqlBuilder.Append("and tul.EstadoInforme.GrupoEstadoInforme =  " + grupoEstado.Value.ToString());

            if (ExistePendienteEnGrupo(grupoEstado.GetValueOrDefault(0)))
                hqlBuilder.Append("and est.Informable = true ");

            hqlBuilder.Append(" and tul.TurnoID = tur.Id ");
            hqlBuilder.Append(" and tur.EquipoId = equ.Id ");
            if (hasta.HasValue && desde.HasValue)
                hqlBuilder.Append(" and tur.Fecha >= :fechaDesde and tur.Fecha < :fechaHasta ");
            else if (desde.HasValue)
                hqlBuilder.Append(" and tur.Fecha >= :fechaDesde ");
            else if (hasta.HasValue)
                hqlBuilder.Append(" and tur.Fecha  < :fechaHasta ");


            if (medicosInformantesIds != null && medicosInformantesIds.Count > 0)
            {
                if (medicosInformantesIds.Count == 1)
                    hqlBuilder.Append("and tul.Informante.Id = :medicoInformanteId ");
                else
                    hqlBuilder.Append("and tul.Informante.Id IN (:medicoInformantesIds) ");
            }    

            if (usuarioId.HasValue)
            {
                hqlBuilder.Append(" and  ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0  ");

                hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0)  ");
            }


            hqlBuilder.Append("order by tur.Fecha  desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (desde.HasValue)
                query.SetDateTime("fechaDesde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetDateTime("fechaHasta", hasta.Value.Date.AddDays(1));

            if (medicosInformantesIds != null && medicosInformantesIds.Count > 0)
            {
                if (medicosInformantesIds.Count == 1)
                    query.SetInt32("medicoInformanteId", medicosInformantesIds[0]);
                else
                    query.SetParameterList("medicoInformantesIds", medicosInformantesIds);
            }

            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);
            // nhibernate no permite lista mayores de 1000 elementos

            return new List<int>(query.List<int>());
        }

        private List<int> TurnoInformesIdsReadByGrupoEstadoYFechas(int? grupoEstado, List<int> informantesIds, DateTime? desde, DateTime? hasta, int? usuarioId)
        {

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tul.Id from TurnoInforme tul, Turno tur, Equipo equ , EstadoTurno est ");
            hqlBuilder.Append("where tur.EstadoTurnoID = est.Id ");
            if (grupoEstado.HasValue)
                hqlBuilder.Append("and tul.EstadoInforme.GrupoEstadoInforme =  " + grupoEstado.ToString());

            if (ExistePendienteEnGrupo(grupoEstado.GetValueOrDefault(0)))
                hqlBuilder.Append("and est.Informable = true ");

            hqlBuilder.Append(" and tul.TurnoID = tur.Id ");
            hqlBuilder.Append(" and tur.EquipoId = equ.Id ");
            if (hasta.HasValue && desde.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde and tul.FechaEntrega < :fechaHasta ");
            else if (desde.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega >= :fechaDesde ");
            else if (hasta.HasValue)
                hqlBuilder.Append(" and tul.FechaEntrega < :fechaHasta ");

            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    hqlBuilder.Append("and tul.Informante.Id = :medicoInformanteId ");
                else
                    hqlBuilder.Append("and tul.Informante.Id IN (:medicoInformantesIds) ");
            }

            if (usuarioId.HasValue)
            {
                hqlBuilder.Append(" and  ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0  ");

                hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tul.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tul.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0)  ");
            }


            hqlBuilder.Append("order by tul.FechaEntrega desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (desde.HasValue)
                query.SetDateTime("fechaDesde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetDateTime("fechaHasta", hasta.Value.Date.AddDays(1));
            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    query.SetInt32("medicoInformanteId", informantesIds[0]);
                else
                    query.SetParameterList("medicoInformantesIds", informantesIds);
            }   
            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);
            // nhibernate no permite lista mayores de 1000 elementos

            return new List<int>(query.List<int>());
        }

        private List<int> TurnoInformeLogIdsReadByFecha(DateTime? desde, DateTime? hasta)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("select tul.Id from TurnoInformeLog tul ");
            hqlBuilder.Append("where ");
            if (hasta.HasValue && desde.HasValue)
                hqlBuilder.Append(" tul.EntregadoFecha >= :fechaDesde and tul.EntregadoFecha < :fechaHasta ");
            else if (desde.HasValue)
                hqlBuilder.Append(" tul.EntregadoFecha >= :fechaDesde ");
            else if (hasta.HasValue)
                hqlBuilder.Append(" tul.EntregadoFecha < :fechaHasta ");

            hqlBuilder.Append("order by tul.EntregadoFecha desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (desde.HasValue)
                query.SetDateTime("fechaDesde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetDateTime("fechaHasta", hasta.Value.Date.AddDays(1));
            return new List<int>(query.List<int>());
        }

        private EntityCollection<InformeListaView> ObtenerInformesPorInformesIds(List<int> informes, List<int> serviciosIds, List<int> regionesIds, int? centroId, bool ordenarPorProtocolo, bool soloEquiposPACS, bool? soloPublicados, int maxRows, bool ascOrder)
        {
            if ((informes == null || informes.Count == 0))
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append("and est.Cancelado = false ");
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");

            if (regionesIds != null && regionesIds.Count > 0)
                hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            if (serviciosIds != null && serviciosIds.Count > 0)
                hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");
            if (soloEquiposPACS)
                hqlBuilder.Append("and tur.Fecha > equ.FechaIntegracionPacs ");
            if (soloPublicados.HasValue)
                hqlBuilder.Append("and tui.PublicadoHL7 = :soloPublicados ");

            if (informes != null && informes.Count > 0)
            {
                SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(informes);
                string block = blockBuilder.BuildConstrainBlock("tui.Id");
                hqlBuilder.Append(" and ").Append(block).Append(" ");
            }


            if (ascOrder)
                hqlBuilder.Append(string.Format("order by tui.FechaEntrega{0} asc ", ordenarPorProtocolo ? ", tur.Orden.Protocolo.ProtocoloFull" : string.Empty));
            else
                hqlBuilder.Append(string.Format("order by tui.FechaEntrega{0} desc ", ordenarPorProtocolo ? ", tur.Orden.Protocolo.ProtocoloFull" : string.Empty));


            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);

            if (regionesIds != null && regionesIds.Count > 0)
                query.SetParameterList("regionesIds", regionesIds);
            if (serviciosIds != null && serviciosIds.Count > 0)
                query.SetParameterList("serviciosIds", serviciosIds);
            if (soloPublicados.HasValue)
                query.SetBoolean("soloPublicados", soloPublicados.Value);

            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        private EntityCollection<InformeListaView> ObtenerInformesEntregadosPorLogsIds(List<int> logs, List<int> serviciosIds, List<int> regionesIds, int? centroId, int? informanteId, int maxRows, int? usuarioId)
        {
            if (logs == null || logs.Count == 0 || serviciosIds == null || serviciosIds.Count == 0 || regionesIds == null || regionesIds.Count == 0)
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, TurnoInformeLog tul, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and tul.TurnoInformeId = tui.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append("and est.Cancelado = false ");
            hqlBuilder.Append("and tui.EstadoInforme.Id =  " + ((int)EstadoInformeEnum.Entregado).ToString());
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");

            if (informanteId.HasValue)
                hqlBuilder.Append("and tui.Informante.Id = :medicoInformanteId ");

            if (usuarioId.HasValue)
            {
                hqlBuilder.Append(" and  ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0  ");

                hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0)  ");
            }

            hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(logs);
            string blockLogs = blockBuilder.BuildConstrainBlock("tul.Id");
            hqlBuilder.AppendFormat("and  {0} ", blockLogs);

            hqlBuilder.Append("order by tul.EntregadoFecha desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);

            if (informanteId.HasValue)
                query.SetInt32("medicoInformanteId", informanteId.Value);

            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);

            query.SetParameterList("regionesIds", regionesIds);
            query.SetParameterList("serviciosIds", serviciosIds);

            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        private EntityCollection<InformeListaView> ObtenerInformesSinEstadoPorLogsIds(List<int> logs, int? servicioId, int? regionId, int? centroId, int maxRows)
        {
            if (logs == null || logs.Count == 0)
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, TurnoInformeLog tul, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and tul.TurnoInformeId = tui.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");
            if (regionId.HasValue)
                hqlBuilder.Append("and tui.RegionInforme.Id = :regionId ");
            if (servicioId.HasValue)
                hqlBuilder.Append("and equ.Servicio.Id = :servicioId ");

            hqlBuilder.Append(" and tul.Id in (:logsIds) ");
            hqlBuilder.Append("order by tui.FechaEntrega desc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);
            if (regionId.HasValue)
                query.SetInt32("regionId", regionId.Value);
            if (servicioId.HasValue)
                query.SetInt32("servicioId", servicioId.Value);
            query.SetParameterList("logsIds", logs);
            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        private EntityCollection<InformeListaView> ObtenerInformesPorTurnos(int? grupoEstado, List<int> serviciosIds, List<int> regionesIds, int? centroId, DateTime? fechaDesde,
            DateTime? fechaHasta, bool fechaPorTurno, List<int> informantesIds, bool soloEquiposPACS, bool? soloPublicados, int maxRows, bool ascOrder, EntityCollection<TurnoLight> turnos, int? usuarioId)
        {
            // Si publica HL7 entonces puede NO filtrar...
            if (!soloEquiposPACS && (serviciosIds == null || serviciosIds.Count == 0 || regionesIds == null || regionesIds.Count == 0 || turnos == null || turnos.Count == 0))
                return new EntityCollection<InformeListaView>();

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.EstadoTurnoID = est.Id ");
            hqlBuilder.Append("and est.Cancelado = false ");

            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(turnos.GetIds());
            string block = blockBuilder.BuildConstrainBlock("tur.Id");

            hqlBuilder.Append("and ").Append(block).Append(" ");

            if (grupoEstado.HasValue)
            {
                hqlBuilder.Append("and tui.EstadoInforme.GrupoEstadoInforme = :grupoEstado ");
                if (ExistePendienteEnGrupo(grupoEstado.Value))
                    hqlBuilder.Append("and est.Informable = true ");
            }
            if (centroId.HasValue)
                hqlBuilder.Append("and equ.Sucursal.Id = :centroId ");
            if (fechaDesde.HasValue)
            {
                if (fechaPorTurno)
                    hqlBuilder.Append("and tur.Fecha >= :fechaDesde ");
                else
                    hqlBuilder.Append("and tui.FechaEntrega >= :fechaDesde ");
            }

            if (fechaHasta.HasValue)
            {
                if (fechaPorTurno)
                    hqlBuilder.Append("and tur.Fecha < :fechaHasta ");
                else
                    hqlBuilder.Append("and tui.FechaEntrega < :fechaHasta ");
            }

            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    hqlBuilder.Append("and tui.Informante.Id = :medicoInformanteId ");
                else
                    hqlBuilder.Append("and tui.Informante.Id IN (:medicoInformantesIds) ");
            }    

            if (soloEquiposPACS)
                hqlBuilder.Append("and tur.Fecha > equ.FechaIntegracionPacs ");
            if (soloPublicados.HasValue)
                hqlBuilder.Append("and tui.PublicadoHL7 = :soloPublicados ");

            if (usuarioId.HasValue)
            {
                hqlBuilder.Append(" and  ((select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = true and (rei.DBTipoAtencion = 1 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0  ");

                hqlBuilder.Append(" or (select count(rei) from RegionInformante rei where ");
                hqlBuilder.Append(" equ.Sucursal.Id = rei.Sucursal.Id and equ.Servicio.Id = rei.Servicio.Id and rei.RegionInforme.Id = tui.RegionInforme.Id ");
                hqlBuilder.Append(" and rei.Informante.Id = :usuarioId and rei.Deleted = false and tur.Id = tui.TurnoID and tur.RequierePediatra = false and (rei.DBTipoAtencion = 0 or rei.DBTipoAtencion = 2) ");
                hqlBuilder.Append(" ) > 0)  ");
            }
            if (regionesIds != null && regionesIds.Count > 0)
                hqlBuilder.Append("and tui.RegionInforme.Id IN (:regionesIds) ");
            if (serviciosIds != null && serviciosIds.Count > 0)
                hqlBuilder.Append("and equ.Servicio.Id IN (:serviciosIds) ");
           
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            if (grupoEstado.HasValue)
                query.SetInt32("grupoEstado", (int)grupoEstado.Value);
            if (centroId.HasValue)
                query.SetInt32("centroId", centroId.Value);
            if (fechaDesde.HasValue)
                query.SetDateTime("fechaDesde", fechaDesde.Value.Date);
            if (fechaHasta.HasValue)
                query.SetDateTime("fechaHasta", fechaHasta.Value.Date);
            if (informantesIds != null && informantesIds.Count > 0)
            {
                if (informantesIds.Count == 1)
                    query.SetInt32("medicoInformanteId", informantesIds[0]);
                else
                    query.SetParameterList("medicoInformantesIds", informantesIds);
            }    
            if (usuarioId.HasValue)
                query.SetInt32("usuarioId", usuarioId.Value);
            if (soloPublicados.HasValue)
                query.SetBoolean("soloPublicados", soloPublicados.Value);
            if(regionesIds != null && regionesIds.Count > 0)
                query.SetParameterList("regionesIds", regionesIds);
            if (serviciosIds != null && serviciosIds.Count > 0)
                query.SetParameterList("serviciosIds", serviciosIds);

            query.SetMaxResults(maxRows);
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        [MinuteTimeout]
        public virtual EntityCollection<InformeListaView> InformeReadByParameters(List<EstadoInformeEnum> lstEstados, string medico,
                  string paciente, string servicio, string protocolo,
                  string regionTag, DateTime? fechaDesde, DateTime? fechaHasta,
                  int sucursal, List<RegionInforme> lstRegion, List<int> lstCircuitoInforme,
                  string practica, string dni, int? tipoSector, bool? informable, bool soloEquiposPACS, bool? soloPublicados, int maxRows)
        {
            ReadManyCommand<InformeListaView> readCmd = new ReadManyCommand<InformeListaView>(dalEngine);

            readCmd.Filter = new Filter();

            List<int> estados = null;
            if (lstEstados != null && lstEstados.Count > 0)
            {
                // Paso los estados a un string para poder ponerlo en la clausula where
                estados = new List<int>();
                foreach (EstadoInformeEnum estado in lstEstados)
                    estados.Add((int)estado);
            }
            AgregaFiltros(estados, medico, paciente, protocolo, readCmd.Filter, informable);

            if (lstRegion != null && lstRegion.Count > 0)
            {
                List<int> regiones = new List<int>();
                foreach (RegionInforme region in lstRegion)
                    regiones.Add(region.Id);

                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.RegionId,
                    "IN", regiones.ToArray());
            }

            if (!String.IsNullOrEmpty(practica))
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.Practica,
                    "LIKE", ConvertForLike(practica));

            int numberOfDNI;
            if (!String.IsNullOrEmpty(dni) && int.TryParse(dni, out numberOfDNI))
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.PacienteNroDoc,
                    "=", numberOfDNI);

            if (!String.IsNullOrEmpty(servicio))
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.ServicioName,
                    "LIKE", ConvertForLike(servicio));

            if (!String.IsNullOrEmpty(regionTag))
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.RegionTag,
                    "=", regionTag);

            if (soloEquiposPACS)
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.TienePacs, "=", true);

            if (soloPublicados.HasValue)
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.PublicadoHL7, "=", soloPublicados.Value);

            if (fechaDesde.HasValue)
            {
                // Si el informe esta entregado busco por la fecha real de entrega
                if (lstEstados != null)
                    if (lstEstados.Count > 0 && lstEstados[0] == EstadoInformeEnum.Entregado)
                    {
                        OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                        readCmd.Filter.Add(open);
                        readCmd.Filter.Add(InformeListaView.Properties.FechaEntregaReal,
                                           ">=", fechaDesde.Value.Date);


                        OpenParenthesis op = new OpenParenthesis(BooleanOp.Or);
                        readCmd.Filter.Add(op);
                        readCmd.Filter.Add(BooleanOp.Or, InformeListaView.Properties.FechaEntregaPrevista,
                                            ">=", fechaDesde.Value.Date);
                        readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.EstadoId,
                                            "=", (int)EstadoInformeEnum.EntregadoSinInforme);

                        CloseParenthesis cl = new CloseParenthesis();
                        readCmd.Filter.Add(cl);

                        CloseParenthesis close = new CloseParenthesis();
                        readCmd.Filter.Add(close);
                    }
                    else
                        readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaPrevista,
                                           ">=", fechaDesde.Value.Date);
            }

            if (fechaHasta.HasValue)
            {
                // Si el informe esta entregado busco por la fecha real de entrega
                if (lstEstados != null)
                    if (lstEstados.Count > 0 && lstEstados[0] == EstadoInformeEnum.Entregado)
                    {
                        OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                        readCmd.Filter.Add(open);
                        readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaReal,
                                           "<=", fechaHasta.Value.Date.AddDays(1));
                        OpenParenthesis op = new OpenParenthesis(BooleanOp.Or);
                        readCmd.Filter.Add(op);
                        readCmd.Filter.Add(BooleanOp.Or, InformeListaView.Properties.FechaEntregaPrevista,
                                          "<=", fechaHasta.Value.Date.AddDays(1));
                        readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.EstadoId,
                                            "=", (int)EstadoInformeEnum.EntregadoSinInforme);

                        CloseParenthesis cl = new CloseParenthesis();
                        readCmd.Filter.Add(cl);

                        CloseParenthesis close = new CloseParenthesis();
                        readCmd.Filter.Add(close);
                    }
                    else
                        readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaPrevista,
                                           "<=", fechaHasta.Value.Date.AddDays(1));
            }


            if (sucursal != -1)
            {
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.SucursalId,
                    "=", sucursal);
            }
            if (lstCircuitoInforme != null && lstCircuitoInforme.Count > 0)
                readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.CircuitoId,
                    "IN", lstCircuitoInforme.ToArray());

            if (tipoSector.HasValue)
            {
                if (tipoSector.Value == (int)TipoSectorEnum.EntregaDomicilio)
                    readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.EntregaDomicilio,
                        "=", true);
                else if (tipoSector.Value == (int)TipoSectorEnum.EntregaInformes)
                    readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.EntregaDomicilio,
                        "=", false);
            }

            readCmd.Filter.Add(BooleanOp.And, InformeListaView.Properties.FechaEntregaPrevista, "IS NOT", null);

            readCmd.Sort = new Sort();
            if (lstEstados.Count > 0 && lstEstados[0] == EstadoInformeEnum.Entregado)
                readCmd.Sort.Add(InformeListaView.Properties.FechaEntregaReal, SortingDirection.Asc);
            else
                readCmd.Sort.Add(InformeListaView.Properties.FechaEntregaPrevista, SortingDirection.Asc);

            if (maxRows > 0)
                readCmd.MaxResults = maxRows;

            return readCmd.Execute();
        }

        //public EntityCollection<InformeListaView> InformeReadByProtocolo(string protocolo, string region)
        //{
        //    return InformeReadByParameters(new List<EstadoInformeEnum>(), "", "", "", protocolo, region, null, null, -1, new List<RegionInforme>(), new List<int>(), "", "", null, 0);
        //}

        [MinuteTimeout]
        public virtual EntityCollection<InformeListaView> InformeListaViewReadByIds(List<int> ids)
        {
            SQLBlockBuilder<int> idsInformes = new SQLBlockBuilder<int>(ids);
            string informesBlock = idsInformes.BuildConstrainBlock("tui.Id");
            StringBuilder hqlBuilder = GetCabeceraConsultaInformeListaView();
            hqlBuilder.Append("and ").Append(informesBlock);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }


        // Escaneo

        public virtual OrdenPaginaEscaneo OrdenPaginaEscaneoInsertar(OrdenPaginaEscaneo ordenPaginaEscaneo)
        {
            if (ordenPaginaEscaneo.PaginaEscaneo != null && ordenPaginaEscaneo.PaginaEscaneo.Id <= 0)
                ordenPaginaEscaneo.PaginaEscaneo = dalEngine.Update(ordenPaginaEscaneo.PaginaEscaneo);

            return dalEngine.Update<OrdenPaginaEscaneo>(ordenPaginaEscaneo);
        }



        [AnonymousMethod]
        public EntityCollection<OrdenPaginaEscaneo> OrdenPaginaEscaneoReadByOrdenId(int ordenId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" from OrdenPaginaEscaneo ope where ope.OrdenId = :ordenId ORDER BY ope.PaginaEscaneo.NroEscaneo, ope.PaginaEscaneo.LadoEscaneo, ope.Id ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("ordenId", ordenId);
            return dalEngine.GetManyByQuery<OrdenPaginaEscaneo>(query);
        }

        [AnonymousMethod]
        public EntityCollection<OrdenPaginaEscaneo> OrdenPaginaEscaneoReadByOrdenIds(List<int> ordenIds)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append(" select ope ");
            hql.Append(" from OrdenPaginaEscaneo ope ");
            hql.Append(" where ope.OrdenId IN (:ordenIds) ORDER BY ope.PaginaEscaneo.NroEscaneo, ope.PaginaEscaneo.LadoEscaneo, ope.Id ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameterList("ordenIds", ordenIds);
            return dalEngine.GetManyByQuery<OrdenPaginaEscaneo>(query);
        }

        public int OrdenPaginaEscaneoReadMaxNroEscaneoByOrden(int ordenID)
        {
            int resultado = 0;

            StringBuilder hql = new StringBuilder(" Select max(ope.PaginaEscaneo.NroEscaneo) from OrdenPaginaEscaneo ope ");
            hql.Append(" where ope.OrdenId = :ordenID ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("ordenID", ordenID);

            object maxNroEscaneo = query.UniqueResult();

            if (maxNroEscaneo != null)
                resultado = (int)maxNroEscaneo;

            return resultado;
        }






        // Traslado Documentos

        public EntityCollection<TrasladoDocumento> TrasladoDocumentoReadByTipoTraslado(int tipoTraslado)
        {
            EntityCollection<TrasladoDocumento> col = dalEngine.GetManyByProperty<TrasladoDocumento>(TrasladoDocumento.Properties.TipoTraslado, tipoTraslado);
            return col;
        }

        public TrasladoDocumento TrasladoDocumentoReadById(int idTrasladoDocumento)
        {
            TrasladoDocumento td = dalEngine.GetById<TrasladoDocumento>(idTrasladoDocumento);
            return td;
        }

        public EstadoLoteTraslado EstadoLoteReadById(int idEstadoLote)
        {
            EstadoLoteTraslado elt = dalEngine.GetById<EstadoLoteTraslado>(idEstadoLote);
            return elt;
        }

        public EntityCollection<EstadoLoteTraslado> EstadoLoteReadAll()
        {
            return dalEngine.GetAll<EstadoLoteTraslado>(EstadoLoteTraslado.Properties.Nombre);
        }

        public EntityCollection<DatosLoteTraslado> LoteTrasladoReadByParameters(DateTime? desde, DateTime? hasta, long? loteNro, string descripcion, int estado, string tipoDocumento, int? tipoSectorDestinoId)
        {
            StringBuilder hql = new StringBuilder(" Select new enfoke.Eges.Entities.Results.DatosLoteTraslado(lt.CreateDate ,lt.Id, lt.Cantidad, lt.Descripcion, elt.Id, elt.Nombre, sec.Id, sec.Name, suc.Id, suc.Name, lt.TipoDocumento)");
            hql.Append(" from LoteTraslado lt, ");
            hql.Append(" EstadoLoteTraslado elt, ");
            hql.Append(" Sector sec, ");
            hql.Append(" Sucursal suc ");
            hql.Append(" where lt.dbEstado = elt.Id ");
            hql.Append("   and lt.SectorEnvioId = sec.Id ");
            hql.Append("   and sec.Sucursal.Id = suc.Id ");

            hql.Append(" AND lt.dbEstado = :estado");

            if (loteNro.HasValue)
                hql.Append(" AND lt.Id = :loteNro");
            if (desde.HasValue)
                hql.Append(" AND lt.CreateDate > :desde");
            if (hasta.HasValue)
                hql.Append(" AND lt.CreateDate < :hasta");
            if (!String.IsNullOrEmpty(descripcion))
                hql.Append(" AND lt.Descripcion like '" + descripcion + "%'");
            if (tipoSectorDestinoId.HasValue)
                hql.Append("   and lt.TipoSecDestinoId = :tipoSectorDestinoId ");

            // El filtro de tipo de documento es obligatorio
            hql.Append(" AND lt.TipoDocumento like '%" + tipoDocumento + "%'");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("estado", estado);
            if (loteNro.HasValue)
                query.SetParameter("loteNro", loteNro.Value);
            if (desde.HasValue)
                query.SetParameter("desde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetParameter("hasta", hasta.Value.Date.AddDays(1));
            if (tipoSectorDestinoId.HasValue)
                query.SetParameter("tipoSectorDestinoId", tipoSectorDestinoId);

            return dalEngine.GetManyByQuery<DatosLoteTraslado>(query);
        }

        public LoteTraslado LoteTrasladoReadByPrtocoloYTipoDocumento(string protocolo, string region, string tipoDocumento)
        {
            StringBuilder hql = new StringBuilder(" Select lt ");
            hql.Append(" from LoteTraslado lt, ");
            hql.Append(" LoteTrasladoDetalle ltd ");
            hql.Append(" where ltd.LoteTrasladoId = lt.Id");
            hql.Append("   and ltd.Protocolo = :protocolo");
            hql.Append("   and lt.TipoDocumento = :tipoDocumento");
            hql.Append("   and ltd.Delete = false ");

            if (string.IsNullOrEmpty(region) == false)
                hql.Append(" and ltd.RegionInforme = :region");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("protocolo", protocolo);
            query.SetParameter("tipoDocumento", tipoDocumento);

            if (string.IsNullOrEmpty(region) == false)
                query.SetParameter("region", region);

            return dalEngine.GetByQuery<LoteTraslado>(query);
        }

        public LoteTraslado LoteTrasladoActualizarEstado(int loteTrasladoId, int nuevoEstadoLote)
        {
            LoteTraslado lt = dalEngine.GetById<LoteTraslado>(loteTrasladoId);
            lt.Estado = (EstadoLoteTrasladoEnum)nuevoEstadoLote;

            lt = dalEngine.Update(lt);

            return lt;
        }

        public LoteTraslado LoteTrasladoReadById(int loteTrasladoId)
        {
            return dalEngine.GetById<LoteTraslado>(loteTrasladoId);
        }

        public EntityCollection<LoteTraslado> LoteTrasladoReadByIds(List<int> loteTrasladoIds)
        {
            Filter filter = new Filter();
            filter.Add(LoteTraslado.Properties.Id, "in", loteTrasladoIds);

            ReadManyCommand<LoteTraslado> readCmd = new ReadManyCommand<LoteTraslado>(dalEngine);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public LoteTraslado LoteTrasladoUpdate(LoteTraslado loteTraslado)
        {
            return dalEngine.Update(loteTraslado);
        }

        public LoteTrasladoDetalle LoteTrasladoDetalleUpdate(LoteTrasladoDetalle ltd)
        {
            return dalEngine.Update(ltd);
        }

        public EntityCollection<LoteTrasladoDetalle> LoteTrasladoDetalleReadByProtocolo(string protocolo, string region, string tipoDocumento)
        {
            StringBuilder hql = new StringBuilder(" Select ltd from LoteTrasladoDetalle ltd, LoteTraslado lt");
            hql.Append(" where ltd.LoteTrasladoId = lt.Id");
            hql.Append(" AND ltd.Protocolo = :protocolo");
            hql.Append(" AND lt.TipoDocumento = :tipoDocumento");
            hql.Append(" AND ltd.Delete = false");

            if (string.IsNullOrEmpty(region) == false)
                hql.Append(" AND ltd.RegionInforme = :region");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("protocolo", protocolo);
            query.SetParameter("tipoDocumento", tipoDocumento);

            if (string.IsNullOrEmpty(region) == false)
                query.SetParameter("region", region);

            //LoteTrasladoDetalle ltd = dalEngine.GetByQuery<LoteTrasladoDetalle>(query);
            EntityCollection<LoteTrasladoDetalle> ltd = dalEngine.GetManyByQuery<LoteTrasladoDetalle>(query);

            return ltd;
        }

        [MinuteTimeout]
        public virtual EntityCollection<TrasladoDocumentoView> TrasladoDocumentoViewReadByParameters(DateTime? fechaDesde, DateTime? fechaHasta, string servicio, string obraSocial, string paciente, string protocolo, int? centro, int? lote, List<int> estadosABuscar, string tipoDocumento)
        {
            ReadManyCommand<TrasladoDocumentoView> readCmd = new ReadManyCommand<TrasladoDocumentoView>(dalEngine);

            readCmd.Filter = new Filter();

            if (estadosABuscar.Count > 0)
            {
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.EstadoInforme,
                        "IN", estadosABuscar.ToArray());
            }

            if (fechaDesde.HasValue)
            {
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.FechaTurno,
                    ">=", fechaDesde.Value);
            }

            if (fechaHasta.HasValue)
            {
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.FechaTurno,
                    "<=", fechaHasta.Value.AddDays(1));
            }

            if (!String.IsNullOrEmpty(tipoDocumento))
            {
                string strTipoDocumento = tipoDocumento.Replace(" ", "%") + "%";
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.TipoDocumento,
                    "Like", strTipoDocumento);
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                string strServicio = servicio.Trim().Replace(" ", "%") + "%";
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.Servicio,
                    "Like", strServicio);
            }

            if (!String.IsNullOrEmpty(obraSocial))
            {
                string strObraSocial = obraSocial.Trim().Replace(" ", "%") + "%";
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.Os,
                    "Like", strObraSocial);
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                string strPaciente = paciente.Trim().Replace(" ", "%") + "%";
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.Paciente,
                    "Like", strPaciente);
            }

            if (!String.IsNullOrEmpty(protocolo))
            {
                if (protocolo.Substring(protocolo.Length - 3, 1) == "-")
                    protocolo = protocolo.Substring(0, protocolo.Length - 4);
                string strProtocolo = protocolo.Trim().Replace(" ", "%") + "%";
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.ProtocoloCodigo,
                    "Like", strProtocolo);
            }

            if (centro.HasValue)
            {
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.SucursalId,
                    "=", centro.Value);
            }

            if (lote.HasValue)
            {
                readCmd.Filter.Add(BooleanOp.And, TrasladoDocumentoView.Properties.LoteNro,
                    "=", lote.Value);
            }

            return readCmd.Execute();
        }

        private Filter TrasladoDocumentoViewReadByParametersInterfazFilterInforme(
            DateTime? fechaDesde, DateTime? fechaHasta, string servicio,
            string obraSocial, string paciente, string protocolo,
            int? centro, int? lote, List<int> estadosABuscar, string tipoDocumento)
        {
            Filter filter = new Filter();

            if (estadosABuscar.Count > 0)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.EstadoInforme,
                        "IN", estadosABuscar.ToArray());
            }

            if (fechaDesde.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.FechaTurno,
                    ">=", fechaDesde.Value);
            }

            if (fechaHasta.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.FechaTurno,
                    "<=", fechaHasta.Value.AddDays(1));
            }

            if (!String.IsNullOrEmpty(tipoDocumento))
            {
                string strTipoDocumento = tipoDocumento.Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.TipoDocumento,
                    "Like", strTipoDocumento);
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                string strServicio = servicio.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.Servicio,
                    "Like", strServicio);
            }

            if (!String.IsNullOrEmpty(obraSocial))
            {
                string strObraSocial = obraSocial.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.Os,
                    "Like", strObraSocial);
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                string strPaciente = paciente.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.Paciente,
                    "Like", strPaciente);
            }

            if (!String.IsNullOrEmpty(protocolo))
            {
                if (protocolo.Substring(protocolo.Length - 3, 1) == "-")
                    protocolo = protocolo.Substring(0, protocolo.Length - 4);
                string strProtocolo = protocolo.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.ProtocoloCodigo,
                    "Like", strProtocolo);
            }

            if (centro.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.SucursalId,
                    "=", centro.Value);
            }

            if (lote.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoInformeView.Properties.LoteNro,
                    "=", lote.Value);
            }

            return filter;
        }

        private static Filter TrasladoDocumentoViewReadByParametersInterfazFilterOrden(
            DateTime? fechaDesde, DateTime? fechaHasta, string servicio,
            string obraSocial, string paciente, string protocolo,
            int? centro, int? lote, List<int> estadosABuscar, string tipoDocumento)
        {
            Filter filter = new Filter();

            if (estadosABuscar.Count > 0)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.EstadoInforme,
                        "IN", estadosABuscar.ToArray());
            }

            if (fechaDesde.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.FechaTurno,
                    ">=", fechaDesde.Value);
            }

            if (fechaHasta.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.FechaTurno,
                    "<=", fechaHasta.Value.AddDays(1));
            }

            if (!String.IsNullOrEmpty(tipoDocumento))
            {
                string strTipoDocumento = tipoDocumento.Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.TipoDocumento,
                    "Like", strTipoDocumento);
            }

            if (!String.IsNullOrEmpty(servicio))
            {
                string strServicio = servicio.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.Servicio,
                    "Like", strServicio);
            }

            if (!String.IsNullOrEmpty(obraSocial))
            {
                string strObraSocial = obraSocial.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.Os,
                    "Like", strObraSocial);
            }

            if (!String.IsNullOrEmpty(paciente))
            {
                string strPaciente = paciente.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.Paciente,
                    "Like", strPaciente);
            }

            if (!String.IsNullOrEmpty(protocolo))
            {
                if (protocolo.Substring(protocolo.Length - 3, 1) == "-")
                    protocolo = protocolo.Substring(0, protocolo.Length - 4);
                string strProtocolo = protocolo.Trim().Replace(" ", "%") + "%";
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.ProtocoloCodigo,
                    "Like", strProtocolo);
            }

            if (centro.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.SucursalId,
                    "=", centro.Value);
            }

            if (lote.HasValue)
            {
                filter.Add(BooleanOp.And, TrasladoDocumentoOrdenView.Properties.LoteNro,
                    "=", lote.Value);
            }

            return filter;
        }

        [MinuteTimeout]
        public virtual EntityCollection<ITrasladoDocumento> TrasladoDocumentoViewReadByParametersInterfaz(
          DateTime? fechaDesde, DateTime? fechaHasta, string servicio,
          string obraSocial, string paciente, string protocolo,
          int? centro, int? lote, List<int> estadosABuscar, string tipoDocumento)
        {
            EntityCollection<ITrasladoDocumento> retorno = new EntityCollection<ITrasladoDocumento>();
            IEntityCollection items;

            switch (tipoDocumento)
            {
                case "ORDEN":
                    ReadManyCommand<TrasladoDocumentoOrdenView> readCmdOrden =
                        new ReadManyCommand<TrasladoDocumentoOrdenView>(dalEngine);
                    readCmdOrden.Filter = TrasladoDocumentoViewReadByParametersInterfazFilterOrden(
                        fechaDesde, fechaHasta, servicio,
                        obraSocial, paciente, protocolo,
                        centro, lote, estadosABuscar, tipoDocumento);
                    items = readCmdOrden.Execute();
                    break;
                case "INFORME":
                    ReadManyCommand<TrasladoDocumentoInformeView> readCmdInforme =
                        new ReadManyCommand<TrasladoDocumentoInformeView>(dalEngine);
                    readCmdInforme.Filter = TrasladoDocumentoViewReadByParametersInterfazFilterInforme(
                        fechaDesde, fechaHasta, servicio,
                        obraSocial, paciente, protocolo,
                        centro, lote, estadosABuscar, tipoDocumento);
                    items = readCmdInforme.Execute();
                    break;
                default:
                    throw new Exception("Tipo de Documento Traslado no reconocido.");

            }

            foreach (ITrasladoDocumento item in items)
                retorno.Add(item);

            return retorno;
        }

        public TrasladoDocumentoView TrasladoDocumentoViewReadByProtocolo(string strProtocolo, string tipoDocumento, string region)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            if (String.IsNullOrEmpty(strProtocolo))
                throw new Exception("Debe ingresar un protocolo para realizar la búsqueda.");

            Protocolo protocolo = TurnosDalc.ProtocoloReadByProtocolo(strProtocolo);

            StringBuilder hql = new StringBuilder(" from TrasladoDocumentoView tdv ");
            hql.Append(" where tdv.ProtocoloCodigo = :protocolo");

            if (!String.IsNullOrEmpty(region))
                hql.Append(" AND tdv.Region = :region");

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "INFORME")
                hql.Append(" AND (tdv.TipoDocumento = :tipoDocumento or tdv.TipoDocumento IS NULL)");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("protocolo", protocolo.ProtocoloFull);

            if (!String.IsNullOrEmpty(region))
                query.SetString("region", region);

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "INFORME")
                query.SetParameter("tipoDocumento", tipoDocumento);

            query.SetMaxResults(1);

            TrasladoDocumentoView tdv = dalEngine.GetByQuery<TrasladoDocumentoView>(query);

            return tdv;
        }

        public EntityCollection<TrasladoDocumentoView> TrasladoDocumentoViewReadByLote(int lote, string tipoDocumento)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from TrasladoDocumentoView tdv ");
            hql.Append("where tdv.LoteTrasladoId = :lote ");

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "INFORME")
                hql.Append(" and (tdv.TipoDocumento = :tipoDocumento or tdv.TipoDocumento is null) ");
            else if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "ORDEN")
                hql.Append(" and (tdv.TipoDocumento = :tipoDocumento) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("lote", lote);

            if (!String.IsNullOrEmpty(tipoDocumento) && (tipoDocumento == "INFORME" || tipoDocumento == "ORDEN"))
                query.SetParameter("tipoDocumento", tipoDocumento);

            return dalEngine.GetManyByQuery<TrasladoDocumentoView>(query);
        }

        public EntityCollection<TrasladoDocumentoOrdenView> TrasladoDocumentoOrdenViewReadByLote(int lote, string tipoDocumento)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from TrasladoDocumentoOrdenView tdv ");
            hql.Append("where tdv.LoteTrasladoId = :lote ");

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "ORDEN")
                hql.Append(" and (tdv.TipoDocumento = :tipoDocumento) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("lote", lote);

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "ORDEN")
                query.SetParameter("tipoDocumento", tipoDocumento);

            EntityCollection<TrasladoDocumentoOrdenView> result = dalEngine.GetManyByQuery<TrasladoDocumentoOrdenView>(query);
            result.SortByProperty(TrasladoDocumentoOrdenView.Properties.LoteDetalleOrden);
            return result;
        }

        public EntityCollection<TrasladoDocumentoInformeView> TrasladoDocumentoInformeViewReadByLote(int lote, string tipoDocumento)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("from TrasladoDocumentoInformeView tdv ");
            hql.Append("where tdv.LoteTrasladoId = :lote ");

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "INFORME")
                hql.Append(" and (tdv.TipoDocumento = :tipoDocumento or tdv.TipoDocumento is null) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            query.SetParameter("lote", lote);

            if (!String.IsNullOrEmpty(tipoDocumento) && tipoDocumento == "INFORME")
                query.SetParameter("tipoDocumento", tipoDocumento);

            EntityCollection<TrasladoDocumentoInformeView> result = dalEngine.GetManyByQuery<TrasladoDocumentoInformeView>(query);
            result.SortByProperty(TrasladoDocumentoInformeView.Properties.LoteDetalleOrden);
            return result;
        }

        public EntityCollection<ITrasladoDocumento> TrasladoDocumentoViewReadByLoteInterfaz(int lote, string tipoDocumento)
        {
            EntityCollection<ITrasladoDocumento> retorno = new EntityCollection<ITrasladoDocumento>();
            IEntityCollection items;

            switch (tipoDocumento)
            {
                case "ORDEN":
                    items = TrasladoDocumentoOrdenViewReadByLote(lote, tipoDocumento);
                    break;
                case "INFORME":
                    items = TrasladoDocumentoInformeViewReadByLote(lote, tipoDocumento);
                    break;
                default:
                    throw new Exception("Tipo de Documento Traslado no reconocido.");
            }

            foreach (ITrasladoDocumento item in items)
                retorno.Add(item);

            return retorno;
        }

        public TrasladoDocumentoView TrasladoDocumentoViewReadByInforme(int turnoInformeId)
        {
            return dalEngine.GetByProperty<TrasladoDocumentoView>
                (TrasladoDocumentoView.Properties.TurnoInformeId, turnoInformeId);
        }

        public EntityCollection<LoteTrasladoDetalle> LoteTrasladoDetalleReadByLoteId(int loteId)
        {
            StringBuilder hql = new StringBuilder("Select ltd from LoteTrasladoDetalle ltd, LoteTraslado lt");
            hql.Append(" where lt.Id = ltd.LoteTrasladoId ");
            hql.Append(" and ltd.LoteTrasladoId = :lote");
            hql.Append(" and ltd.Delete = false");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("lote", loteId);

            return dalEngine.GetManyByQuery<LoteTrasladoDetalle>(query);
        }



        // Plantilla Informes
        [Private]
        public EntityCollection<PlantillaInformeCampo> PlantillaInformeCampoReadByPlantillaInforme(int plantillaInformeId)
        {
            return dalEngine.GetManyByProperty<PlantillaInformeCampo>(PlantillaInformeCampo.Properties.PlantillaInformeId, plantillaInformeId, PlantillaInformeCampo.Properties.Nombre);
        }

        [RequiresTransaction]
        public virtual void AsociarPlantillaInformeTablaAPractica(int practica, EntityCollection<PlantillaInformeTabla> plantillasInforme)
        {
            EntityCollection<PracticaPlantillaInforme> practicasPlantillaInforme = PracticaPlantillaInformeReadByPractica(practica);
            if (practicasPlantillaInforme != null && practicasPlantillaInforme.Count > 0)
            {
                foreach (PracticaPlantillaInforme pra in practicasPlantillaInforme)
                {
                    dalEngine.Delete(pra);
                }
            }

            foreach (PlantillaInformeTabla item in plantillasInforme)
            {
                PracticaPlantillaInforme ppi = new PracticaPlantillaInforme();
                ppi.Plantilla = item;

                ppi.Practica = dalEngine.GetById<Practica>(practica);
                dalEngine.Update<PracticaPlantillaInforme>(ppi);
            }
        }

        [RequiresTransaction]
        public virtual void PlantillaInformeTablaUpdate(PlantillaInformeTabla plantillaInforme)
        {
            plantillaInforme.UsuarioModificacion = Security.Current.UserInfo.User.Id;

            plantillaInforme.FechaModificacion = enfoke.Time.Now;
            dalEngine.Update<PlantillaInformeTabla>(plantillaInforme);
        }

        //[RequiresTransaction]
        public virtual PlantillaInformeTabla PlantillaInformeTablaUpdateAndRefresh(PlantillaInformeTabla plantillaInforme, EntityCollection<PracticaPlantillaInforme> practicaPlantillaInformes, EntityCollection<MedicoPlantillaInforme> medicoPlantillaInformes)
        {
            plantillaInforme.UsuarioModificacion = Security.Current.UserInfo.User.Id;
            plantillaInforme.FechaModificacion = enfoke.Time.Now;
            PlantillaInformeTabla plantillaInformeTable = dalEngine.Update<PlantillaInformeTabla>(plantillaInforme);
            //Borro y actualizo las asociaciones de las plantillas con las practicas
            DeleteManyPracticaPlantillaInforme(plantillaInformeTable);
            // actualizo los nuevos
            foreach (PracticaPlantillaInforme plantilla in practicaPlantillaInformes)
                plantilla.Id = 0;
            dalEngine.UpdateCollection(practicaPlantillaInformes);
            //Borro y actualizo las asociaciones de las plantillas con los medicos
            DeleteManyMedicoPlantillaInforme(plantillaInformeTable);
            UpdateManyMedicoPlantillaInforme(medicoPlantillaInformes);

            return plantillaInformeTable;
        }

        //[RequiresTransaction]
        public virtual void PlantillaInformeTablaUpdate(PlantillaInformeTabla plantillaInforme, EntityCollection<PracticaPlantillaInforme> practicaPlantillaInformes, EntityCollection<MedicoPlantillaInforme> medicoPlantillaInformes)
        {
            PlantillaInformeTablaUpdateAndRefresh(plantillaInforme, practicaPlantillaInformes, medicoPlantillaInformes);
        }

        public PlantillaInformeTabla PlantillaInformeReadByArchivo(String archivo, int plantillaInformeTablaId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("FROM PlantillaInformeTabla pla ");
            hql.Append("WHERE pla.Archivo = :archivo ");
            hql.Append("AND pla.Id <> :plantillaInformeTablaId ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetString("archivo", archivo);
            query.SetInt32("plantillaInformeTablaId", plantillaInformeTablaId);
            return dalEngine.GetByQuery<PlantillaInformeTabla>(query);
        }

        public EntityCollection<PlantillaInformeTabla> PlantillaInformeTablaReadByServicio(int servicioId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("SELECT DISTINCT pla ");

            if (servicioId > 0)
            {
                hql.Append("FROM PlantillaInformeTabla pla, PracticaPlantillaInforme ppi ");
                hql.Append("WHERE pla = ppi.Plantilla ");
                hql.Append("AND ppi.Practica.ServicioEspecialidad.Servicio = :servicioId ");
            }
            else
            {
                hql.Append("FROM PlantillaInformeTabla pla ");
                hql.Append("WHERE pla.Id not in (select ppi.Plantilla.Id  from  PracticaPlantillaInforme ppi) ");
            }

            IQuery query = dalEngine.CreateQuery(hql.ToString());

            if (servicioId > 0)
                query.SetInt32("servicioId", servicioId);
            return dalEngine.GetManyByQuery<PlantillaInformeTabla>(query);
        }



        // Medico Plantilla Informe

        public EntityCollection<MedicoPlantillaInforme> MedicoPlantillaInformeReadByPracticaPlantillaInforme(int plantillaInformeTabla)
        {

            StringBuilder hql = new StringBuilder();
            hql.Append("SELECT DISTINCT mpi").Append(" ");
            hql.Append("FROM MedicoPlantillaInforme mpi").Append(" ");
            hql.Append("WHERE mpi.PlantillaInforme.id = :plantillaInformeTabla").Append(" ");
            //hql.Append("ORDER BY mpi.Medico.ApyN ASC").Append(" ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("plantillaInformeTabla", plantillaInformeTabla);
            return dalEngine.GetManyByQuery<MedicoPlantillaInforme>(query);
        }

        private void DeleteManyMedicoPlantillaInforme(PlantillaInformeTabla plantillaInformeTabla)
        {
            EntityCollection<MedicoPlantillaInforme> medicoPlantillaInformes = new EntityCollection<MedicoPlantillaInforme>();
            medicoPlantillaInformes = this.MedicoPlantillaInformeReadByPracticaPlantillaInforme(plantillaInformeTabla.Id);
            if (medicoPlantillaInformes != null && medicoPlantillaInformes.Count > 0)
            {
                dalEngine.Delete(medicoPlantillaInformes);
            }
        }

        private void UpdateManyMedicoPlantillaInforme(EntityCollection<MedicoPlantillaInforme> medicoPlantillaInformes)
        {
            if (medicoPlantillaInformes != null && medicoPlantillaInformes.Count > 0)
            {
                dalEngine.UpdateCollection<MedicoPlantillaInforme>(medicoPlantillaInformes);
            }
        }



        // Practica Plantilla Informe

        public EntityCollection<PracticaPlantillaInforme> PracticaPlantillaInformeReadByPractica(int practicaId)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("FROM PracticaPlantillaInforme ppi").Append(" ");
            hql.Append("WHERE ppi.Practica.Id = :practicaId").Append(" ");
            hql.Append("ORDER BY ppi.Plantilla.Descripcion ASC").Append(" ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("practicaId", practicaId);

            EntityCollection<PracticaPlantillaInforme> practicasPlantillaInforme = dalEngine.GetManyByQuery<PracticaPlantillaInforme>(query);
            return practicasPlantillaInforme;
        }

        public EntityCollection<PracticaPlantillaInforme> PracticaPlantillaInformeReadByPlantillaInformeTabla(int plantillaInformeTabla)
        {
            StringBuilder hql = new StringBuilder();
            hql.Append("FROM PracticaPlantillaInforme ppi").Append(" ");
            hql.Append("WHERE ppi.Plantilla.Id = :plantillaInformeTabla").Append(" ");
            hql.Append("ORDER BY ppi.Practica.Name ASC").Append(" ");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("plantillaInformeTabla", plantillaInformeTabla);
            return dalEngine.GetManyByQuery<PracticaPlantillaInforme>(query);
        }

        private void DeleteManyPracticaPlantillaInforme(PlantillaInformeTabla plantillaInformeTabla)
        {
            EntityCollection<PracticaPlantillaInforme> practicaPlantillaInformes = new EntityCollection<PracticaPlantillaInforme>();
            practicaPlantillaInformes = this.PracticaPlantillaInformeReadByPlantillaInformeTabla(plantillaInformeTabla.Id);
            if (practicaPlantillaInformes != null && practicaPlantillaInformes.Count > 0)
            {
                foreach (PracticaPlantillaInforme pra in practicaPlantillaInformes)
                {
                    dalEngine.Delete(pra);
                }
            }
        }

        private void UpdateManyPracticaPlantillaInforme(EntityCollection<PracticaPlantillaInforme> practicaPlantillaInformes)
        {
            if (practicaPlantillaInformes != null && practicaPlantillaInformes.Count > 0)
            {
                foreach (PracticaPlantillaInforme pra in practicaPlantillaInformes)
                {
                    dalEngine.Update<PracticaPlantillaInforme>(pra);
                }

            }
        }



        // Informe Compaginacion

        public InformeCompaginacion InformeCompaginacionReadByInforme(int turnoInformeId)
        {
            return dalEngine.GetByProperty<InformeCompaginacion>(InformeCompaginacion.Properties.TurnoInformeId, turnoInformeId);
        }

        public int InformeCompaginacionReadMaxGrupo()
        {
            int ret = 0;
            StringBuilder hql = new StringBuilder("select max(ic.Grupo) from InformeCompaginacion ic");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            object maximo = query.UniqueResult();

            if (maximo != null)
                ret = (int)maximo;

            return ret;
        }






        public void InformeCompaginacionDeleteByInforme(int informeId)
        {
            InformeCompaginacion informeCompaginacion = InformeCompaginacionReadByInforme(informeId);
            if (informeCompaginacion != null)
                dalEngine.Delete(informeCompaginacion);
        }


        // Publicacion Web
        public EntityCollection<TurnoInforme> ObtenerInformesPendientesPDF(DateTime fecha)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            StringBuilder hql = new StringBuilder();
            hql.Append("from TurnoInforme tui where tui.EstadoInforme = " + (int)EstadoInformeEnum.AEntregar);
            //hql.Append(" and (tui.PublicadoWeb IS NULL or  tui.PublicadoWeb <= :fecha)");
            hql.Append(" and tui.PublicadoWeb IS NULL");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            //query.SetParameter("fecha", fecha);

            EntityCollection<TurnoInforme> result = dalEngine.GetManyByQuery<TurnoInforme>(query);

            EntityCollection<TurnoInforme> finalresult = new EntityCollection<TurnoInforme>();

            foreach (TurnoInforme informe in result)
            {
                informe.Protocolo = TurnosDalc.ProtocoloReadByTurno(informe.TurnoID);
                informe.Turno = TurnosDalc.TurnoReadById(informe.TurnoID);
                informe.Turno.Orden.Paciente = dalEngine.GetById<Paciente>(informe.Turno.Orden.PacienteId);
                informe.Turno.Orden.MedicoSolicitante = dalEngine.GetById<MedicoAsociacion>(informe.Turno.Orden.MedicoSolicitanteID.Value);
                EntityCollection<PracticaTurno> praturs = informe.RegionInforme != null ? TurnosDalc.PracticaTurnoReadByTurnoAndRegion(informe.TurnoID, PracticaTurnoTipoEnum.Principal, informe.RegionInforme.Id) : TurnosDalc.PracticaTurnoReadByTurnoAndRegion(informe.TurnoID, PracticaTurnoTipoEnum.Principal, null);

                bool publicable = true;
                foreach (PracticaTurno pratur in praturs)
                {
                    if (!pratur.Practica.PublicaInforme || !pratur.Practica.ServicioEspecialidad.Servicio.PublicaInforme)
                    {
                        publicable = false;
                        break;
                    }
                }

                if (publicable && informe.Informante.Medico.PublicaInforme)
                    finalresult.Add(informe);
            }

            return finalresult;
        }

        public void ActualizarFechaPublicacion(int informeID)
        {
            TurnoInforme informe = dalEngine.GetById<TurnoInforme>(informeID);

            informe.PublicadoWeb = enfoke.Time.Now;

            dalEngine.Update(informe);
        }



        public void BorrarSobreInformes(List<int> informesId)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(informesId, TurnoInforme.Properties.SobreId, null);
        }

        private EntityCollection<InformeDeSMS> ConstruirInformes(EntityCollection<TurnoInforme> informesAfectados, EntityCollection<Paciente> pacientes, string mensaje)
        {
            EntityCollection<InformeDeSMS> response = new EntityCollection<InformeDeSMS>();
            EquiposDalc equipoDalc = Context.Session.EquiposDalc;
            SortByShitDate(informesAfectados);
            foreach (Paciente paciente in pacientes)
            {
                List<Turno> turnos = new List<Turno>();
                string direccion = string.Empty;
                foreach (TurnoInforme turnoInforme in this.TurnosPorPaciente(informesAfectados, paciente))
                {
                    direccion = turnoInforme.DireccionSucursal;
                    turnos.Add(turnoInforme.Turno);
                }

                InformeDeSMS informe = new InformeDeSMS(paciente);
                informe.TurnosAfectados = turnos;
                Equipo equipo = equipoDalc.EquipoReadById(turnos[0].EquipoId.Value);
                informe.Servicio = equipo.Servicio;
                informe.Centro = equipo.Sucursal;
                informe.FechaPrimerTurno = turnos[0].Fecha;
                informe.Mensaje = mensaje;
                informe.CantidadDeInformes = turnos.Count;
                informe.DireccionDeCentro = direccion;
                response.Add(informe);
            }
            return response;
        }

        private static void SortByShitDate(EntityCollection<TurnoInforme> informesAfectados)
        {
            if (informesAfectados == null)
                return;

            informesAfectados.Sort(delegate(TurnoInforme left, TurnoInforme right)
            {
                return left.Turno.Fecha.GetValueOrDefault(DateTime.MinValue).CompareTo(right.Turno.Fecha.GetValueOrDefault(DateTime.MinValue));
            });
        }

        public EntityCollection<TurnoInforme> TurnoInformeReadByProtocoloAndRegion(string protocolo, string region)
        {
            string hql = "SELECT tui FROM TurnoInforme tui LEFT JOIN tui.RegionInforme, Turno tur WHERE tui.TurnoID = tur.Id AND tur.Orden.Protocolo.ProtocoloFull = :protocolo";
            if (!String.IsNullOrEmpty(region))
                hql += " AND tui.RegionInforme.Tag = :region";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("protocolo", protocolo);
            if (!String.IsNullOrEmpty(region))
                query.SetParameter("region", region);

            return dalEngine.GetManyByQuery<TurnoInforme>(query);
        }

        // Plaquero






        [Private]
        public void PlaqueroDelete(Plaquero plaquero)
        {
            plaquero.Deleted = true;
            Audit.AuditDelete(plaquero, Security.Current.UserInfo.User.Id);

            PlaqueroUpdate(plaquero);
        }

        public Plaquero PlaqueroUpdateAndRefresh(Plaquero plaquero)
        {
            return dalEngine.Update<Plaquero>(plaquero);
        }

        public void PlaqueroUpdate(Plaquero plaquero)
        {
            PlaqueroUpdateAndRefresh(plaquero);
        }

        //[Private]
        //public EntityCollection<TurnoInforme> TurnoInformesReadByPlaqueroId(int idPlaquero)
        //{
        //    return dalEngine.GetManyByProperty<TurnoInforme>(
        //        TurnoInforme.Properties.PlaqueroId, idPlaquero,
        //        TurnoInforme.Properties.EstadoInforme);
        //}

        [Private]
        public EntityCollection<InformeListaView> InformeListaViewReadByPlaqueroId(int idPlaquero, int? idEstado)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.InformeListaView(tui, tur, est, equ, pac) ");
            hqlBuilder.Append("from TurnoInforme tui, Turno tur, Equipo equ, EstadoTurno est, Paciente pac, Plaquero pla ");
            hqlBuilder.Append("where tui.TurnoID = tur.Id ");
            hqlBuilder.Append("and tur.EquipoId = equ.Id ");
            hqlBuilder.Append("and tur.Orden.PacienteId = pac.Id ");
            hqlBuilder.Append("and est.Id = tur.EstadoTurnoID ");
            hqlBuilder.Append("and tui.PlaqueroId = pla.Id ");

            if (idEstado.HasValue)
                hqlBuilder.Append("and tui.EstadoInforme.Id = :idEstado");

            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            query.SetParameter("idPlaquero", idPlaquero);
            if (idEstado.HasValue)
                query.SetParameter("idEstado", idEstado.Value);

            return dalEngine.GetManyByQuery<InformeListaView>(query);
        }

        [Private]
        public List<int> TurnoInformesIdsReadByPlaqueroId(int idPlaquero)
        {
            string hql = "SELECT DISTINCT ti.Id FROM TurnoInforme ti " +
                         "WHERE ti.PlaqueroId = :idPlaquero ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idPlaquero", idPlaquero);

            List<int> turnoInformes = (List<int>)query.List<int>();

            return turnoInformes;

        }

        [Private]
        public EntityCollection<Plaquero> PlaquerosReadBySectorId(int idSector)
        {
            string hql = "SELECT pl " +
                         "FROM Plaquero pl " +
                         "WHERE pl.SectorId = :idSector " +
                         "AND pl.Deleted = false " +
                         "ORDER BY pl.Nombre ASC";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idSector", idSector);

            return dalEngine.GetManyByQuery<Plaquero>(query);
        }

        [Private]
        public Plaquero PlaqueroReadByNombreAndSectorIdAndNotDeleted(string nombrePlaquero, int idSector)
        {
            string hql = "SELECT pl " +
                         "FROM Plaquero pl " +
                         "WHERE pl.Nombre = :nombrePlaquero " +
                         "AND pl.SectorId = :idSector " +
                         "AND pl.Deleted = false ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("nombrePlaquero", nombrePlaquero);
            query.SetParameter("idSector", idSector);

            return dalEngine.GetByQuery<Plaquero>(query);
        }

        [Private]
        public void TurnoInformesUpdateBatchByPlaquero(List<int> idInformes, int? idPlaquero)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(idInformes, TurnoInforme.Properties.PlaqueroId, idPlaquero);
        }



        public EntityCollection<TurnoInforme> TurnoInformeReadByIds(List<int> informesId)
        {
            return dalEngine.GetManyByIds<TurnoInforme>(informesId);
        }

        public EntityCollection<TurnoInformeLight> TurnoInformeLightReadByIds(List<int> informesIds)
        {
            return dalEngine.GetManyByIds<TurnoInformeLight>(informesIds);
        }

        public List<DatosFormDesbloqueoInforme> DatosFormDesbloqueoInforme()
        {
            var qInformesIDs =
                from inf in dalEngine.Query<TurnoInforme>()
                where inf.TurnoInformePrincipalID == null && inf.UsuarioBloqueando != null
                select inf.Id;

            List<int> informesIds = qInformesIDs.ToList();
            if (informesIds.Count <= 0)
                return new List<DatosFormDesbloqueoInforme>();

            var query =
                from inf in dalEngine.Query<TurnoInforme>()
                join turno in dalEngine.Query<Turno>() on inf.TurnoID equals turno.Id
                join pac in dalEngine.Query<Paciente>() on turno.Orden.PacienteId equals pac.Id
                join equipo in dalEngine.Query<Equipo>() on turno.EquipoId equals equipo.Id
                join prt in dalEngine.Query<PracticaTurno>() on turno.Id equals prt.TurnoId
                where
                    informesIds.Contains(inf.Id)
                    && prt.Tipo == (int)PracticaTurnoTipoEnum.Principal
                select new DatosFormDesbloqueoInforme(inf.Id, inf.FechaEntrega, inf.RegionInforme.Name, inf.EstadoInforme.Name, turno.Fecha, pac.ApellidoNombre, turno.Orden.Protocolo.ProtocoloFull, equipo.Servicio.Name, equipo.Sucursal.Name, prt.Practica.Name, String.Format("{0} {1}", inf.UsuarioBloqueando.LastName, inf.UsuarioBloqueando.Name));

            return query.ToList();
        }


        public Dictionary<string, EntityCollection<EstudioAnterior>> GetEstudiosAnterioresByProtocolos(List<string> protocolos)
        {
            Dictionary<string, EntityCollection<EstudioAnterior>> estudiosAnterioresRet = new Dictionary<string, EntityCollection<EstudioAnterior>>();
            if (protocolos.Count > 0)
            {
                var estudiosAnterioresByProt =
                    (
                            from protocolo in dalEngine.Query<Protocolo>()
                            join orden in dalEngine.Query<Orden>()
                                on protocolo.Id equals orden.Protocolo.Id
                            join turnosEstudios in dalEngine.Query<TurnoEstudios>()
                                on orden.TurnoEstudiosID equals turnosEstudios.Id
                            join turnosEstudiosEstudioAnteriores in dalEngine.Query<TurnoEstudiosEstudioAnterior>()
                                on turnosEstudios.Id equals turnosEstudiosEstudioAnteriores.TurnoEstudios.Id
                            join estudioAnterior in dalEngine.Query<EstudioAnterior>()
                                on turnosEstudiosEstudioAnteriores.EstudioAnterior.Id equals estudioAnterior.Id
                            where
                                protocolos.Contains(protocolo.ProtocoloFull)
                            select new { Protocolo = protocolo.ProtocoloFull, EstudioAnterior = estudioAnterior }
                     );

                foreach (var estudio in estudiosAnterioresByProt)
                {
                    if (!estudiosAnterioresRet.ContainsKey(estudio.Protocolo))
                        estudiosAnterioresRet.Add(estudio.Protocolo, new EntityCollection<EstudioAnterior>() { estudio.EstudioAnterior });
                    else
                        estudiosAnterioresRet[estudio.Protocolo].Add(estudio.EstudioAnterior);
                }
            }
            return estudiosAnterioresRet;
        }

        #region Publicacion de informes

        [Private]
        public void PublicacionInformesPublicar(List<int> turnoInformeIds)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(turnoInformeIds, TurnoInforme.Properties.PublicadoHL7, true);
        }

        [Private]
        public void PublicacionInformesDesPublicar(List<int> turnoInformeIds)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(turnoInformeIds, TurnoInforme.Properties.PublicadoHL7, false);
        }

        [Private]
        public void InformeParametrizacionDeleteDetalles(InformeParametroPublicacion parametrizacionEditando)
        {
            dalEngine.DeleteBatchByProperty<InformeParametroMedico>(InformeParametroMedico.Properties.ParametroId, parametrizacionEditando.Id);
            dalEngine.DeleteBatchByProperty<InformeParametroServicio>(InformeParametroServicio.Properties.ParametroId, parametrizacionEditando.Id);
            dalEngine.DeleteBatchByProperty<InformeParametroCentro>(InformeParametroCentro.Properties.ParametroId, parametrizacionEditando.Id);
        }

        public EntityCollection<InformePublicacionListadoItem> InformePublicacionListadoItemReadBy(string descripcion, List<int> medicosDerivantes, List<int> medicosInformantes, List<int> centros, List<int> servicios)
        {
            EntityCollection<InformeParametroPublicacion> parametrizaciones = InformeParametroPublicacionReadBy(descripcion, medicosDerivantes, medicosInformantes, centros, servicios);

            if (parametrizaciones.Count <= 0)
                return new EntityCollection<InformePublicacionListadoItem>();

            // Agrupo los medicos por parametrizacion
            List<InformePublicacionListadoItem> items = AgruparALosMedicos(parametrizaciones);
            // Agrupo los centros por parametrizacion
            items = AgruparALosCentros(parametrizaciones.GetIds(), items);
            // Agrupo los servicios por parametrizacion
            items = AgruparALosServicios(parametrizaciones.GetIds(), items);

            // Si alguno de los grupos de Medicos, Servicio o centros tiene mas de 1 items => 
            // Borro la coleccion porque imprimiria (Varios...) y no hace falta traer su Name real.
            //FiltrarGruposConVariosItems(items);

            CompletarTextoObjetosGrupos(items);

            return items.ToEntityCollection();
        }

        // Por cada grupo hace un distinct de los ids a ulitizar
        private void CompletarTextoObjetosGrupos(List<InformePublicacionListadoItem> items)
        {
            List<int> serviciosIds = new List<int>();
            foreach (InformePublicacionListadoItem listadoItem in items.Where(item => item.ParametrizacionServicios.Count == 1).Select(item => item).ToList())
                serviciosIds.AddRange(listadoItem.ParametrizacionServicios.Select(cen => cen.ServicioId).ToList());
            serviciosIds = serviciosIds.Distinct().ToList();
            EntityCollection<ServicioName> serviciosName = dalEngine.GetManyByIds<ServicioName>(serviciosIds);
            foreach (InformePublicacionListadoItem item in items)
                if (item.ParametrizacionServicios.Count == 1)
                    item.TextoServicios = serviciosName.FindByKey(item.ParametrizacionServicios[0].ServicioId).Name;
                else
                    item.TextoServicios = "(Varios...)";

            List<int> centrosIds = new List<int>();
            foreach (InformePublicacionListadoItem listadoItem in items.Where(item => item.ParametrizacionCentros.Count == 1).Select(item => item).ToList())
                centrosIds.AddRange(listadoItem.ParametrizacionCentros.Select(cen => cen.SucursalId).ToList());
            centrosIds = centrosIds.Distinct().ToList();
            EntityCollection<SucursalName> sucursalesNames = dalEngine.GetManyByIds<SucursalName>(centrosIds);
            foreach (InformePublicacionListadoItem item in items)
                if (item.ParametrizacionCentros.Count == 1)
                    item.TextoCentros = sucursalesNames.FindByKey(item.ParametrizacionCentros[0].SucursalId).Name;
                else
                    item.TextoCentros = "(Varios...)";

            List<int> medicosNoDerivantesIds = new List<int>();
            foreach (InformePublicacionListadoItem listadoItem in items.Where(item => item.ParametrizacionMedicos.Where(med => !med.Concepto).ToList().Count == 1).Select(item => item).ToList())
                medicosNoDerivantesIds.AddRange(listadoItem.ParametrizacionMedicos.Where(med => !med.Concepto).Select(med => med.MedicoId).ToList());
            medicosNoDerivantesIds = medicosNoDerivantesIds.Distinct().ToList();
            EntityCollection<MedicoName> medicosNoDerivantes = dalEngine.GetManyByIds<MedicoName>(medicosNoDerivantesIds);
            foreach (InformePublicacionListadoItem item in items)
                if (item.ParametrizacionMedicos.Where(med => !med.Concepto).ToList().Count == 1)
                    item.TextoInformantes = medicosNoDerivantes.FindByKey((item.ParametrizacionMedicos.Where(med => !med.Concepto).ToList()[0]).MedicoId).FullName;
                else
                    item.TextoInformantes = "(Varios...)";

            List<int> derivantes = new List<int>();
            foreach (InformePublicacionListadoItem listadoItem in items.Where(item => item.ParametrizacionMedicos.Where(med => med.Concepto).ToList().Count == 1).Select(item => item).ToList())
                derivantes.AddRange(listadoItem.ParametrizacionMedicos.Where(med => med.Concepto).Select(med => med.MedicoId).ToList());
            derivantes = derivantes.Distinct().ToList();
            EntityCollection<MedicoAsociacion> medicoAsociacion = dalEngine.GetManyByIds<MedicoAsociacion>(derivantes);
            foreach (InformePublicacionListadoItem item in items)
                if (item.ParametrizacionMedicos.Where(med => med.Concepto).ToList().Count == 1)
                    item.TextoDerivantes = medicoAsociacion.FindByKey(item.ParametrizacionMedicos.Where(med => med.Concepto).ToList()[0].MedicoId).FullName;
                else
                    item.TextoDerivantes = "(Varios...)";
        }

        ///// <summary>
        ///// Si alguno de los grupos de Medicos, Servicio o centros tiene mas de 1 items => 
        ///// Borro la coleccion porque imprimiria (Varios...) y no hace falta traer su Name real.
        ///// </summary>
        //private void FiltrarGruposConVariosItems(List<InformePublicacionListadoItem> items)
        //{
        //    foreach (InformePublicacionListadoItem item in items)
        //    {
        //        if (item.ParametrizacionCentros.Count > 1)
        //        {
        //            item.ParametrizacionCentros = null;
        //            item.TextoCentros = "(Varios...)";
        //        }

        //        if (item.ParametrizacionServicios.Count > 1)
        //        {
        //            item.ParametrizacionServicios = null;
        //            item.TextoServicios = "(Varios...)";
        //        }

        //        // Saco derivantes
        //        if (item.ParametrizacionMedicos.Where(med => med.Concepto).ToList().Count > 1)
        //        {
        //            item.ParametrizacionMedicos.RemoveRange(item.ParametrizacionMedicos.Where(med => med.Concepto).ToList());
        //            item.TextoDerivantes = "(Varios...)";
        //        }

        //        // Saco informantes
        //        if (item.ParametrizacionMedicos.Where(med => !med.Concepto).ToList().Count > 1)
        //        {
        //            item.ParametrizacionMedicos.RemoveRange(item.ParametrizacionMedicos.Where(med => !med.Concepto).ToList());
        //            item.TextoDerivantes = "(Varios...)";
        //        }
        //    }
        //}

        private List<InformePublicacionListadoItem> AgruparALosServicios(List<int> parametrizacionesIds, List<InformePublicacionListadoItem> items)
        {
            // me traigo todas las parametrizaciones de servicios que intervienen
            var queryServicio = from servicio in dalEngine.Query<InformeParametroServicio>() where parametrizacionesIds.Contains(servicio.ParametroId) select servicio;
            EntityCollection<InformeParametroServicio> colServicio = queryServicio.ToEntityCollection();


            var query = (from servicio in colServicio
                         group servicio by servicio.ParametroId into serviciosGroup
                         join param in items on serviciosGroup.Key equals param.ParametrizacionPublicacion.Id
                         select new InformePublicacionListadoItem()
                         {
                             ParametrizacionPublicacion = param.ParametrizacionPublicacion,
                             ParametrizacionMedicos = param.ParametrizacionMedicos,
                             ParametrizacionCentros = param.ParametrizacionCentros,
                             ParametrizacionServicios = serviciosGroup.ToList()
                         });
            // Me trigo los objetos
            return query.ToList();
        }

        private List<InformePublicacionListadoItem> AgruparALosCentros(List<int> parametrizacionesIds, List<InformePublicacionListadoItem> items)
        {
            // me traigo todas las parametrizaciones de centros que intervienen
            var queryCentros = from centro in dalEngine.Query<InformeParametroCentro>() where parametrizacionesIds.Contains(centro.ParametroId) select centro;
            EntityCollection<InformeParametroCentro> colCentros = queryCentros.ToEntityCollection();

            var query = (from centro in colCentros
                         group centro by centro.ParametroId into centrosGroup
                         join param in items on centrosGroup.Key equals param.ParametrizacionPublicacion.Id
                         select new InformePublicacionListadoItem()
                         {
                             ParametrizacionPublicacion = param.ParametrizacionPublicacion,
                             ParametrizacionMedicos = param.ParametrizacionMedicos,
                             ParametrizacionCentros = centrosGroup.ToList()
                         });
            // Me trigo los objetos
            return query.ToList();
        }

        private List<InformePublicacionListadoItem> AgruparALosMedicos(EntityCollection<InformeParametroPublicacion> parametrizaciones)
        {
            // me traigo todas las parametrizaciones de medicos que intervienen
            var queryMedicos = from med in dalEngine.Query<InformeParametroMedico>() where parametrizaciones.GetIds().Contains(med.ParametroId) select med;
            EntityCollection<InformeParametroMedico> colMedicos = queryMedicos.ToEntityCollection();

            var query = (from medico in colMedicos
                         group medico by medico.ParametroId into medicosGroup
                         join param in parametrizaciones on medicosGroup.Key equals param.Id
                         select new InformePublicacionListadoItem()
                         {
                             ParametrizacionPublicacion = param,
                             ParametrizacionMedicos = medicosGroup.ToList()
                         });

            // Me trigo los objetos
            return query.ToList();
        }

        /// <summary>
        /// Obtengo todos los InformeParametroPublicacion que luego llenare para devolver los datos
        /// </summary>
        private EntityCollection<InformeParametroPublicacion> InformeParametroPublicacionReadBy(string descripcion, List<int> medicosDerivantes, List<int> medicosInformantes, List<int> centros, List<int> servicios)
        {
            var queryParam = from param in dalEngine.Query<InformeParametroPublicacion>() select param;

            if (!String.IsNullOrWhiteSpace(descripcion))
                queryParam = queryParam.Where(param => param.Descripcion.Contains(descripcion.Trim()));

            // Me fijo las parametrizaciones donde aparecen estos derivantes y los filtro en el query
            if (medicosDerivantes != null && medicosDerivantes.Count > 0)
            {
                var queryDerivantes = from derivante in dalEngine.Query<InformeParametroMedico>() where derivante.Concepto && medicosDerivantes.Contains(derivante.MedicoId) select derivante.ParametroId;
                List<int> paramFiltradoPorDerivantes = queryDerivantes.ToList();
                if (paramFiltradoPorDerivantes.Count == 0)
                    return new EntityCollection<InformeParametroPublicacion>();
                queryParam = queryParam.Where(param => paramFiltradoPorDerivantes.Contains(param.Id));
            }

            // Me fijo las parametrizaciones donde aparecen estos informantes y los filtro en el query
            if (medicosInformantes != null && medicosInformantes.Count > 0)
            {
                var queryInformantes = from informante in dalEngine.Query<InformeParametroMedico>() where !informante.Concepto && medicosInformantes.Contains(informante.MedicoId) select informante.ParametroId;
                List<int> paramFiltradoPorInformantes = queryInformantes.ToList();
                if (paramFiltradoPorInformantes.Count == 0)
                    return new EntityCollection<InformeParametroPublicacion>();
                queryParam = queryParam.Where(param => paramFiltradoPorInformantes.Contains(param.Id));
            }

            // Me fijo las parametrizaciones donde aparecen estos centros y los filtro en el query
            if (centros != null && centros.Count > 0)
            {
                var queryCentros = from centro in dalEngine.Query<InformeParametroCentro>() where centros.Contains(centro.SucursalId) select centro.ParametroId;
                List<int> paramFiltradoPorCentros = queryCentros.ToList();
                if (paramFiltradoPorCentros.Count == 0)
                    return new EntityCollection<InformeParametroPublicacion>();

                queryParam = queryParam.Where(param => paramFiltradoPorCentros.Contains(param.Id));
            }

            // Me fijo las parametrizaciones donde aparecen estos servicios y los filtro en el query
            if (servicios != null && servicios.Count > 0)
            {
                var queryServicios = from servicio in dalEngine.Query<InformeParametroServicio>() where servicios.Contains(servicio.ServicioId) select servicio.ParametroId;
                List<int> paramFiltradoPorServicios = queryServicios.ToList();
                if (paramFiltradoPorServicios.Count == 0)
                    return new EntityCollection<InformeParametroPublicacion>();
                queryParam = queryParam.Where(param => paramFiltradoPorServicios.Contains(param.Id));
            }

            return queryParam.ToEntityCollection();
        }


        public EntityCollection<InformeParametroPublicacion> InformeParametroPublicacionConDetalleReadAll()
        {
            // Me traigo las paramtrizaciones
            var query = from param in dalEngine.Query<InformeParametroPublicacion>() where !param.Deleted select param;
            EntityCollection<InformeParametroPublicacion> parametros = query.ToEntityCollection();

            // Y todos los detalles Servicio - Medicos y Sucursales
            var queryServicios = from param in dalEngine.Query<InformeParametroPublicacion>()
                                 join ser in dalEngine.Query<InformeParametroServicio>() on param.Id equals ser.ParametroId
                                 where !param.Deleted
                                 select ser;
            EntityCollection<InformeParametroServicio> servicios = queryServicios.ToEntityCollection();


            var queryMedicos = from param in dalEngine.Query<InformeParametroPublicacion>()
                               join med in dalEngine.Query<InformeParametroMedico>() on param.Id equals med.ParametroId
                               where !param.Deleted
                               select med;
            EntityCollection<InformeParametroMedico> medicos = queryMedicos.ToEntityCollection();

            var queryCentros = from param in dalEngine.Query<InformeParametroPublicacion>()
                               join cen in dalEngine.Query<InformeParametroCentro>() on param.Id equals cen.ParametroId
                               where !param.Deleted
                               select cen;
            EntityCollection<InformeParametroCentro> centros = queryCentros.ToEntityCollection();


            foreach (InformeParametroPublicacion param in parametros)
            {
                param.Centros = centros.Where(cen => cen.ParametroId == param.Id).ToEntityCollection();
                param.Medicos = medicos.Where(med => med.ParametroId == param.Id).ToEntityCollection();
                param.Servicios = servicios.Where(ser => ser.ParametroId == param.Id).ToEntityCollection();
            }

            return parametros;
        }
        #endregion

        #region Reasignacion de Informantes

        /// <summary>
        /// Me devuelve los pares de TurnoId, MedicoId con items liquidados. Pueden ser ValorizacionItem, ComprobanteItem o ReciboMedico
        /// </summary>
        [Private]
        public virtual List<KeyValuePair<int, int>> TurnoMedicoConHonorariosLiquidados(List<int> turnosIds)
        {
            if(turnosIds == null || turnosIds.Count <= 0)
                return new List<KeyValuePair<int, int>>();

            List<List<int>> splittedTurnosIds = LinqInClause.SplitIntoBucketsForOracle(turnosIds);
            List<KeyValuePair<int, int?>> turnoMedicoConHonorarios = new List<KeyValuePair<int, int?>>();

            foreach (List<int> turnosIdstmp in splittedTurnosIds)
            {
                // Esto deberia filtrar por Medico IS NOT NULL PERO al hacer vli.MedicoID != null genere en el SQL un OR horrrible.
                var queryVli = from vli in dalEngine.Query<ValorizacionItem>()
                               join liq in dalEngine.Query<LiquidacionHonorarios>() on vli.LiquidacionHonorariosID equals liq.Id
                               where turnosIdstmp.Contains(vli.Valorizacion.Turno.Id)
                               select new KeyValuePair<int, int?> (vli.Valorizacion.Turno.Id, vli.MedicoID);
                
                var queryReciboMedico = from rem in dalEngine.Query<ReciboMedico>()
                                        join liq in dalEngine.Query<LiquidacionHonorarios>() on rem.LiquidacionHonorariosID equals liq.Id
                            where turnosIdstmp.Contains(rem.TurnoID)
                                  && rem.LiquidacionHonorariosID != null
                               select new KeyValuePair<int, int?> (rem.TurnoID, (int?)rem.MedicoID);
                
                var queryCoi = from coi in dalEngine.Query<ComprobanteItem>()
                               join liq in dalEngine.Query<LiquidacionHonorarios>() on coi.LiquidacionHonorariosID equals liq.Id
                               join prt in dalEngine.Query<PracticaTurno>() on coi.PracticaTurnoID equals prt.Id
                            where turnosIdstmp.Contains(prt.TurnoId)
                                  && coi.LiquidacionHonorariosID != null
                               select new KeyValuePair<int, int?>(prt.TurnoId, (int?)prt.MedicoInformante.Id);

                List<KeyValuePair<int, int?>> tmp = queryVli.ToList();
                if (tmp != null && tmp.Count > 0)
                    turnoMedicoConHonorarios.AddRange(tmp);
                tmp = queryReciboMedico.ToList();
                if (tmp != null && tmp.Count > 0)
                    turnoMedicoConHonorarios.AddRange(tmp);
                tmp = queryCoi.ToList();
                if (tmp != null && tmp.Count > 0)
                    turnoMedicoConHonorarios.AddRange(tmp);
            }

            // Hago esto por que me pueden venir registros con MedicoId en null que no me sirven para el proposito del metodo
            List<KeyValuePair<int, int>> ret = new List<KeyValuePair<int, int>>();
            foreach (KeyValuePair<int, int?> turnoMedicoConHonorario in turnoMedicoConHonorarios)
            {
                if (turnoMedicoConHonorario.Value.HasValue)
                    ret.Add(new KeyValuePair<int, int>(turnoMedicoConHonorario.Key, turnoMedicoConHonorario.Value.Value)); 
            }
            return ret;
        }


        #endregion
    }
}

