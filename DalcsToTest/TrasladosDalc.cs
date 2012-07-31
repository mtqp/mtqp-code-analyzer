using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using System.Linq;
using enfoke.Eges.Persistence;
using enfoke.Data.DisconnectedSupport;
using enfoke.Eges.Entities;
using enfoke.Data.Filters;
using enfoke.Eges.Entities.Results;
using NHibernate;
using enfoke.Data;
using enfoke.AOP;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class TrasladosDalc : Dalc, IService
    {
        protected TrasladosDalc(NotConstructable dummy) : base(dummy) { }


        #region Busqueda de Entidades Trasladables

        /// <summary>
        /// Devuelve una coleccion de DocumentoTrasladableInforme 
        /// </summary>
        /// <param name="tipoTrasladoId">Si se especifica este valor entonces la coleccion viene cargada con los lotes correspondientes</param>
        /// <param name="documentosId"></param>
        /// <returns></returns>
        [Private]
        public EntityCollection<DocumentoTrasladableInforme> DocumentoTrasladableInformeReadByIds(int tipoTrasladoId, List<int> documentosId)
        {
            return DocumentoTrasladableInformeReadByIds((int?)tipoTrasladoId, documentosId);
        }

        [Private]
        public EntityCollection<DocumentoTrasladableInforme> DocumentoTrasladableInformeReadByIds(List<int> documentosId)
        {
            return DocumentoTrasladableInformeReadByIds((int?)null, documentosId);
        }

        private string Informe_GetHQLSelectForDocumentoTrasladableInforme()
        {
            return "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladableInformeParaFiltrar(tui.Id, tur.Fecha, tur.EstadoTurnoID, tui.EstadoInforme, tur.Orden.Protocolo.ProtocoloFull, " +
                         " pac.ApellidoNombre, osp.ObraSocial.Id, osp.ObraSocial.Name, ser.Name, suc.Name, " +
                         " tui.RegionInforme, tui.FechaEntrega, til.EntregadoFecha, tui.SobreId, tui.Prometido, tui.InformanteSolicitado, " +
                         " prt.Practica.Name, tur.CantPracticasAnestesia, tur.CantPracticasContraste, prt.Medico.Apellido, prt.Medico.Name, equ.Descripcion, tur.Orden.TurnoEstudiosID, tur.Id, tui.UnificacionPrincipal," +
                         " tui.TurnoInformePrincipalID, prt.RegionInformeID, prt.Tipo, prt.Practica.Id, tur.Orden.dbEntregaOrdenId, pac.Importancia, tui.EntregaPlacas) " +
                         " FROM TurnoInforme tui LEFT JOIN tui.InformanteSolicitado LEFT JOIN tui.RegionInforme, Turno tur, PracticaTurno prt, EstadoTurno est, Paciente pac, ObraSocialPlan osp, ServicioName ser, " +
                         " SucursalName suc, TurnoInformeLog til, Equipo equ  WHERE  tur.EquipoId = equ.Id and " +
                         " til.TurnoInformeId = tui.Id and suc.Id = tur.Orden.Protocolo.SucursalID and ser.Id = equ.Servicio.Id and tur.Orden.ObraSocialPlanId = osp.Id and tur.Orden.PacienteId = pac.Id and tui.TurnoID = tur.Id AND tur.Id = prt.TurnoId AND est.Atendido = :atendido ";

        }

        private string Placa_GetHQLSelectForDocumentoTrasladableInforme()
        {
            return "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladableInformeParaFiltrar(tui.Id, tur.Fecha, tui.dbTipoUbicacionPlaca, tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, tui.TurnoInformeLoteTrasladoUltimo.PosicionEnLote, tur.EstadoTurnoID, tui.EstadoInforme, tur.Orden.Protocolo.ProtocoloFull, " +
                         " pac.ApellidoNombre, osp.ObraSocial.Id, osp.ObraSocial.Name, ser.Name, suc.Name, " +
                         " tui.RegionInforme, tui.FechaEntrega, til.EntregadoFecha, tui.SobreId, tui.Prometido, tui.InformanteSolicitado, " +
                         " prt.Practica.Name, tur.CantPracticasAnestesia, tur.CantPracticasContraste, prt.Medico.Apellido, prt.Medico.Name, equ.Descripcion, tur.Orden.TurnoEstudiosID, tur.Id, tui.UnificacionPrincipal, tui.TurnoInformePrincipalID, prt.RegionInformeID, prt.Tipo, prt.Practica.Id, pac.Importancia, tui.EntregaPlacas) " +
                         " FROM TurnoInforme tui LEFT JOIN tui.InformanteSolicitado LEFT JOIN tui.RegionInforme LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, Turno tur, PracticaTurno prt, EstadoTurno est, Paciente pac, ObraSocialPlan osp, ServicioName ser, " +
                         " SucursalName suc, TurnoInformeLog til, Equipo equ  WHERE  tur.EquipoId = equ.Id and " +
                         " til.TurnoInformeId = tui.Id and suc.Id = tur.Orden.Protocolo.SucursalID and ser.Id = equ.Servicio.Id and tur.Orden.ObraSocialPlanId = osp.Id and tur.Orden.PacienteId = pac.Id and tui.TurnoID = tur.Id AND tur.Id = prt.TurnoId AND est.Atendido = :atendido ";

        }

        private EntityCollection<DocumentoTrasladableInforme> DocumentoTrasladableInformeReadByIds(int? tipoTrasladoId, List<int> documentosId)
        {
            if (documentosId.Count <= 0)
                return new EntityCollection<DocumentoTrasladableInforme>();

            EntityCollection<EstadoTurno> estados = Context.Session.Dalc.GetManyByProperty<EstadoTurno>(EstadoTurno.Properties.Atendido, true);
            string hql = Informe_GetHQLSelectForDocumentoTrasladableInforme();
            hql += " AND tui.Id IN (:documentosId)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("documentosId", documentosId);
            query.SetParameter("atendido", true);
            EntityCollection<DocumentoTrasladableInformeParaFiltrar> col = dalEngine.GetManyByQuery<DocumentoTrasladableInformeParaFiltrar>(query);
            EntityCollection<DocumentoTrasladableInforme> informes = FiltrarDocumentoTrasladableInformeParaFiltrar(col);

            if (tipoTrasladoId.HasValue)
                DocumentoTrasladableInformeCargarInfoLoteTraslado(tipoTrasladoId.Value, informes);

            return informes;
        }

        private void DocumentoTrasladableInformeCargarInfoLoteTraslado(int tipoTrasladoId, EntityCollection<DocumentoTrasladableInforme> col)
        {
            EntityCollection<TurnoInformeLoteTraslado> colTurnoInformeLote = new EntityCollection<TurnoInformeLoteTraslado>();
            List<int> ids = new List<int>();
            int cant = 0;
            for (int i = 0; i < col.Count; i++)
            {
                cant = cant + 1;
                ids.Add(col[i].Id);
                if (cant == 999)
                {
                    colTurnoInformeLote.AddRange(TurnoInformeLoteTrasladoReadByDocIdsAndTipoTraslado(tipoTrasladoId, ids));
                    ids = new List<int>();
                    cant = 0;
                }
            }

            if (cant > 0)
                colTurnoInformeLote.AddRange(TurnoInformeLoteTrasladoReadByDocIdsAndTipoTraslado(tipoTrasladoId, ids));

            foreach (DocumentoTrasladableInforme inf in col)
            {
                Predicate<TurnoInformeLoteTraslado> predicate = delegate(TurnoInformeLoteTraslado compare) { return (compare.TurnoInformeId == inf.Id); };
                TurnoInformeLoteTraslado tilt = colTurnoInformeLote.Find(predicate);
                if (tilt != null)
                {
                    inf.Lote = tilt.LoteTraslado;
                    inf.PosicionEnLote = tilt.PosicionEnLote;
                }
            }
        }

        private EntityCollection<TurnoInformeLoteTraslado> TurnoInformeLoteTrasladoReadByDocIdsAndTipoTraslado(int tipoTrasladoId, List<int> documentosId)
        {
            if (documentosId.Count <= 0)
                return new EntityCollection<TurnoInformeLoteTraslado>();

            string hql = " FROM TurnoInformeLoteTraslado tilt WHERE tilt.TurnoInformeId IN (:documentosId) AND tilt.LoteTraslado.TipoTraslado.Id = :tipoTrasladoId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("documentosId", documentosId);
            query.SetParameter("tipoTrasladoId", tipoTrasladoId);
            return dalEngine.GetManyByQuery<TurnoInformeLoteTraslado>(query);
        }

        [Private]
        public EntityCollection<DocumentoTrasladable> DocumentoTrasladableOrdenReadByIds(List<int> documentosId)
        {
            if (documentosId.Count <= 0)
                return new EntityCollection<DocumentoTrasladable>();

            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tur.Orden.Id, tur.Fecha, tur.Orden.dbEntregaOrdenId, tur.Orden.LoteTraslado, tur.Orden.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.Paciente.Importancia, tur.Id) " +
                         " FROM TurnoHQL tur LEFT JOIN tur.Orden.LoteTraslado WHERE tur.Orden.Id IN (:documentosId)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("documentosId", documentosId);
            EntityCollection<DocumentoTrasladable> col = dalEngine.GetManyByQuery<DocumentoTrasladable>(query);
            EntityCollection<DocumentoTrasladable> ret = EliminarDocumentoTrasladableDuplicadosPorFechaMenor(col);

            ret = MergeWithEntregaPlacas(ret);

            return ret;
        }

        private EntityCollection<DocumentoTrasladable> EliminarDocumentoTrasladableDuplicadosPorFechaMenor(EntityCollection<DocumentoTrasladable> col)
        {
            Dictionary<int, DateTime?> fechaPorOrden = new Dictionary<int, DateTime?>();
            Dictionary<int, int> posicionOrdenEnColResultado = new Dictionary<int, int>();
            EntityCollection<DocumentoTrasladable> colResultado = new EntityCollection<DocumentoTrasladable>();

            foreach (DocumentoTrasladable doc in col)
            {
                // Si no esta todavia
                if (!fechaPorOrden.ContainsKey(doc.Id))
                {
                    fechaPorOrden.Add(doc.Id, doc.FechaTurno);
                    colResultado.Add(doc);
                    posicionOrdenEnColResultado.Add(doc.Id, colResultado.Count - 1);
                    continue;
                }

                // esta pero tengo que reemplazar por fecha menor
                if ((!fechaPorOrden[doc.Id].HasValue) || (doc.FechaTurno.HasValue && doc.FechaTurno.Value < fechaPorOrden[doc.Id].Value))
                {
                    fechaPorOrden[doc.Id] = doc.FechaTurno;
                    colResultado[posicionOrdenEnColResultado[doc.Id]].FechaTurno = doc.FechaTurno;
                }
            }

            return colResultado;
        }

        [Private]
        public EntityCollection<DocumentoTrasladableInforme> TrasladoPlacasReadDocumentoTrasladableInformeByIds(List<int> documentosId)
        {
            if (documentosId.Count <= 0)
                return new EntityCollection<DocumentoTrasladableInforme>();

            string hql = Placa_GetHQLSelectForDocumentoTrasladableInforme();
            hql += " AND tui.Id IN (:documentosId)";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("documentosId", documentosId);
            query.SetParameter("atendido", true);

            EntityCollection<DocumentoTrasladableInformeParaFiltrar> col = dalEngine.GetManyByQuery<DocumentoTrasladableInformeParaFiltrar>(query);
            return FiltrarDocumentoTrasladableInformeParaFiltrar(col);
        }

        [Private]
        public EntityCollection<DocumentoTrasladable> TrasladoPlacasReadDocumentoTrasladableByIds(List<int> documentosId)
        {
            if (documentosId.Count <= 0)
                return new EntityCollection<DocumentoTrasladable>();

            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tui.Id, tur.Fecha, tui.dbTipoUbicacionPlaca, tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, tui.TurnoInformeLoteTrasladoUltimo.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tui.RegionInforme.Tag, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.Paciente.Importancia, tur.Id, tui.EntregaPlacas) " +
                         " FROM TurnoInforme tui LEFT JOIN tui.RegionInforme LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, TurnoHQL tur WHERE tui.TurnoID = tur.Id AND tui.Id IN (:documentosId) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("documentosId", documentosId);
            return dalEngine.GetManyByQuery<DocumentoTrasladable>(query);
        }

        public EntityCollection<DocumentoTrasladable> DocumentoTrasladableOrdenReadByFilters(DateTime? fechaDesde, DateTime? fechaHasta, string servicio, string obraSocial, string paciente, string protocolo, int? centroId, int? loteNro, int? estado)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tur.Orden.Id, tur.Fecha, tur.Orden.dbEntregaOrdenId, tur.Orden.LoteTraslado, tur.Orden.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.dbEntregaOrdenId, tur.Orden.Paciente.Importancia, tur.Id) " +
                         " FROM TurnoHQL tur LEFT JOIN tur.Orden.LoteTraslado WHERE tur.Estado.Cancelado = 0 ";

            if (fechaDesde.HasValue)
                hql += " AND tur.Fecha >= :fechaDesde ";
            if (fechaHasta.HasValue)
                hql += " AND tur.Fecha < :fechaHasta ";
            if (!String.IsNullOrEmpty(protocolo))
                hql += " AND tur.Orden.Protocolo.ProtocoloFull = :protocolo ";
            if (!String.IsNullOrEmpty(servicio))
            {
                servicio = servicio.Replace(" ", "%");
                hql += " AND tur.Equipo.Servicio.Name like '" + servicio + "%'";
            }
            if (!String.IsNullOrEmpty(obraSocial))
            {
                obraSocial = obraSocial.Replace(" ", "%");
                hql += " AND tur.Orden.ObraSocialPlan.ObraSocial.Name like '" + obraSocial + "%'";
            }
            if (!String.IsNullOrEmpty(paciente))
                hql += " AND tur.Orden.Paciente.ApellidoNombre like '" + paciente + "%'";
            if (centroId.HasValue)
                hql += " AND tur.Equipo.Sucursal.Id = :centroId ";
            if (loteNro.HasValue)
                hql += " AND tur.Orden.LoteTraslado.Id = :loteNro ";
            if (estado.HasValue)
                hql += " AND tur.Orden.dbEntregaOrdenId = :estado ";

            IQuery query = dalEngine.CreateQuery(hql);
            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta);
            if (!String.IsNullOrEmpty(protocolo))
                query.SetParameter("protocolo", protocolo);
            if (centroId.HasValue)
                query.SetParameter("centroId", centroId);
            if (loteNro.HasValue)
                query.SetParameter("loteNro", loteNro);
            if (estado.HasValue)
                query.SetParameter("estado", estado);

            EntityCollection<DocumentoTrasladable> col = dalEngine.GetManyByQuery<DocumentoTrasladable>(query);
            EntityCollection<DocumentoTrasladable> ret = EliminarDocumentoTrasladableDuplicadosPorFechaMenor(col);

            ret = MergeWithEntregaPlacas(ret);

            return ret;
        }

        public EntityCollection<DocumentoTrasladable> DocumentoTrasladablePlacaReadByFilters(DateTime? fechaDesde, DateTime? fechaHasta, string servicio, string obraSocial, string paciente, string protocolo, int? centroId, int? loteNro, int? estado)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tui.Id, tur.Fecha, tui.dbTipoUbicacionPlaca, tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, tui.TurnoInformeLoteTrasladoUltimo.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tui.RegionInforme.Tag, pac.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, pac.Importancia, tur.Id, tui.EntregaPlacas) " +
                         " FROM TurnoInforme tui LEFT JOIN tui.RegionInforme LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo LEFT JOIN tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado, TurnoHQL tur, Paciente pac WHERE tui.TurnoID = tur.Id and tur.Orden.Paciente.Id = pac.Id and tur.Estado.Cancelado = 0 ";

            if (fechaDesde.HasValue)
                hql += " AND tur.Fecha >= :fechaDesde ";
            if (fechaHasta.HasValue)
                hql += " AND tur.Fecha <= :fechaHasta ";
            if (!String.IsNullOrEmpty(protocolo))
                hql += " AND tur.Orden.Protocolo.ProtocoloFull = :protocolo ";
            if (!String.IsNullOrEmpty(servicio))
                hql += " AND tur.Equipo.Servicio.Name like '" + servicio + "%'";
            if (!String.IsNullOrEmpty(obraSocial))
                hql += " AND tur.Orden.ObraSocial.Name like '" + obraSocial + "%'";
            if (!String.IsNullOrEmpty(paciente))
                hql += " AND pac.ApellidoNombre like '" + paciente + "%'";
            if (centroId.HasValue)
                hql += " AND tur.Equipo.Sucursal.Id = :centroId ";
            if (loteNro.HasValue)
                hql += " AND tui.TurnoInformeLoteTrasladoUltimo.LoteTraslado.Id = :loteNro ";
            if (estado.HasValue)
                hql += " AND tui.dbTipoUbicacionPlaca = :estado ";

            IQuery query = dalEngine.CreateQuery(hql);
            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta);
            if (!String.IsNullOrEmpty(protocolo))
                query.SetParameter("protocolo", protocolo);
            if (centroId.HasValue)
                query.SetParameter("centroId", centroId);
            if (loteNro.HasValue)
                query.SetParameter("loteNro", loteNro);
            if (estado.HasValue)
                query.SetParameter("estado", estado);
            return dalEngine.GetManyByQuery<DocumentoTrasladable>(query);

        }

        public EntityCollection<DocumentoTrasladable> DocumentoTrasladableInformeReadByFilters(int tipoTrasladoId, DateTime? fechaDesde, DateTime? fechaHasta, string servicio, string obraSocial, string paciente, string protocolo, int? centroId, int? loteNro, int? estado)
        {
            string hql = Informe_GetHQLSelectForDocumentoTrasladableInforme();

            if (fechaDesde.HasValue)
                hql += " AND tur.Fecha >= :fechaDesde ";
            if (fechaHasta.HasValue)
                hql += " AND tur.Fecha <= :fechaHasta ";
            if (!String.IsNullOrEmpty(protocolo))
                hql += " AND tur.Orden.Protocolo.ProtocoloFull = :protocolo ";
            if (!String.IsNullOrEmpty(servicio))
                hql += " AND ser.Name like '" + servicio + "%'";
            if (!String.IsNullOrEmpty(obraSocial))
                hql += " AND osp.ObraSocial.Name like '" + obraSocial + "%'";
            if (!String.IsNullOrEmpty(paciente))
                hql += " AND pac.ApellidoNombre like '" + paciente + "%'";
            if (centroId.HasValue)
                hql += " AND suc.Id = :centroId ";
            if (estado.HasValue)
                hql += " AND tui.EstadoInforme.Id = :estado ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("atendido", true);
            if (fechaDesde.HasValue)
                query.SetParameter("fechaDesde", fechaDesde);
            if (fechaHasta.HasValue)
                query.SetParameter("fechaHasta", fechaHasta);
            if (!String.IsNullOrEmpty(protocolo))
                query.SetParameter("protocolo", protocolo);
            if (centroId.HasValue)
                query.SetParameter("centroId", centroId);
            if (estado.HasValue)
                query.SetParameter("estado", estado);


            EntityCollection<DocumentoTrasladableInformeParaFiltrar> col = dalEngine.GetManyByQuery<DocumentoTrasladableInformeParaFiltrar>(query);
            EntityCollection<DocumentoTrasladableInforme> informes = FiltrarDocumentoTrasladableInformeParaFiltrar(col);

            DocumentoTrasladableInformeCargarInfoLoteTraslado(tipoTrasladoId, informes);
            EntityCollection<DocumentoTrasladableInforme> result = new EntityCollection<DocumentoTrasladableInforme>();

            if (loteNro.HasValue)
            {
                foreach (DocumentoTrasladableInforme doc in informes)
                {
                    if (doc.Lote != null && doc.Lote.Id == loteNro.Value)
                        result.Add(doc);
                }
            }
            else
                result = informes;

            return ConvertirDocumentoTrasladableInformeADocumentoTrasladable(result);
        }

        /// <summary>
        /// En la coleccion de DocumentoTrasladableInformeParaFiltrar vienen TurnoInformes x PracticaTurno (todos contra todos) y este metodo se encarga de devolver una coleccion con el practicaTurno correspondiente a cada TurnoInforme. 
        /// </summary>
        /// <param name="col"></param>
        /// <returns></returns>
        private EntityCollection<DocumentoTrasladableInforme> FiltrarDocumentoTrasladableInformeParaFiltrar(EntityCollection<DocumentoTrasladableInformeParaFiltrar> col)
        {
            // Los turnoInformes ya analizados se agregan con el ID como key. 
            // turnoInformeAgregadosEsPrincipal: Si se ingresa con True es que se agrego la practica principal. Si se agrega con False es que es cualquier practica de la misma region
            // turnoInformeAgregadosPosicionEnColeccion: El value es el index del turnoInforme en la coleccion result
            Dictionary<int, bool> turnoInformeAgregadosEsPrincipal = new Dictionary<int, bool>();
            Dictionary<int, int> turnoInformeAgregadosPosicionEnColeccion = new Dictionary<int, int>();

            EntityCollection<DocumentoTrasladableInforme> resultado = new EntityCollection<DocumentoTrasladableInforme>();
            foreach (DocumentoTrasladableInformeParaFiltrar item in col)
            {
                // Si el turno informe y la practica item no tienen region o tienen la misma
                if ((!item.RegionIdEnPracticaTurno.HasValue && item.Region == null)
                    || (item.RegionIdEnPracticaTurno.HasValue && item.Region != null && item.RegionIdEnPracticaTurno.Value == item.Region.Id))
                {
                    if (!turnoInformeAgregadosEsPrincipal.ContainsKey(item.Id))
                    {
                        resultado.Add((DocumentoTrasladableInforme)item);
                        turnoInformeAgregadosEsPrincipal.Add(item.Id, item.TipoPracticaId == (int)PracticaTurnoTipoEnum.Principal);
                        turnoInformeAgregadosPosicionEnColeccion.Add(item.Id, resultado.Count - 1);
                    }
                    else if (!turnoInformeAgregadosEsPrincipal[item.Id] && item.TipoPracticaId == (int)PracticaTurnoTipoEnum.Principal)
                    {
                        resultado[turnoInformeAgregadosPosicionEnColeccion[item.Id]] = item;
                        turnoInformeAgregadosEsPrincipal[item.Id] = true;
                    }
                    // Si no es la principal me fijo si es la practica con mayor ID (esto es para traer iguales resultados que la version anterior y poder testear bien)
                    else
                    {
                        if (((DocumentoTrasladableInformeParaFiltrar)resultado[turnoInformeAgregadosPosicionEnColeccion[item.Id]]).PracticaId < item.PracticaId)
                            resultado[turnoInformeAgregadosPosicionEnColeccion[item.Id]] = item;
                    }
                }
            }

            return resultado;
        }

        public EntityCollection<DocumentoTrasladable> DocumentoTrasladableReadByLote(List<int> lotesId)
        {
            LoteTraslado lote = dalEngine.GetById<LoteTraslado>(lotesId[0]);

            switch (lote.TipoTraslado.TipoEntidad)
            {
                case TipoEntidadEnum.Orden:
                    return DocumentoTrasladableReadOrdenByLote(lotesId);
                case TipoEntidadEnum.Informe:
                    return DocumentoTrasladableReadInformeByLote(lote.TipoTraslado.Id, lotesId);
                case TipoEntidadEnum.Placa:
                    return DocumentoTrasladableReadPlacaByLote(lotesId);
            }

            return null;
        }

        private EntityCollection<DocumentoTrasladable> DocumentoTrasladableReadPlacaByLote(List<int> lotesId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tui.Id, tur.Fecha, tui.dbTipoUbicacionPlaca, tilt.LoteTraslado, tilt.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tui.RegionInforme.Tag, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.Paciente.Importancia, tur.Id, tui.EntregaPlacas) " +
                         " FROM TurnoInforme tui LEFT JOIN tui.RegionInforme, TurnoHQL tur, TurnoInformeLoteTraslado tilt WHERE tui.TurnoID = tur.Id AND tui.Id = tilt.TurnoInformeId AND tilt.LoteTraslado.Id in (:lotesId) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("lotesId", lotesId);
            return dalEngine.GetManyByQuery<DocumentoTrasladable>(query);
        }

        private EntityCollection<DocumentoTrasladable> DocumentoTrasladableReadInformeByLote(int tipoTrasladoId, List<int> lotesId)
        {
            // Traigo todos los informes del lote y despues consultando por esos informes ;)            
            EntityCollection<TurnoInformeLoteTraslado> tilts = TurnoInformeLoteTrasladoReadByLotes(lotesId);
            List<int> informesIds = new List<int>();
            foreach (TurnoInformeLoteTraslado item in tilts)
                informesIds.Add(item.TurnoInformeId);

            EntityCollection<DocumentoTrasladableInforme> col = DocumentoTrasladableInformeReadByIds(tipoTrasladoId, informesIds);
            return ConvertirDocumentoTrasladableInformeADocumentoTrasladable(col);

        }

        public DocumentoTrasladable DocumentoTrasladableReadOrdenByProtocolo(string protocolo)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tur.Orden.Id, tur.Fecha, tur.Orden.dbEntregaOrdenId, tur.Orden.LoteTraslado, tur.Orden.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.Paciente.Importancia, tur.Id) " +
                         "FROM TurnoHQL tur LEFT JOIN tur.Orden.LoteTraslado WHERE tur.Orden.Protocolo.ProtocoloFull = :protocolo ORDER BY tur.Orden.Id DESC";


            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("protocolo", protocolo);

            EntityCollection<DocumentoTrasladable> documentos = dalEngine.GetManyByQuery<DocumentoTrasladable>(query);

            if (documentos == null)
                return null;

            documentos = MergeWithEntregaPlacas(documentos);

            return documentos[documentos.Count - 1];
        }



        public EntityCollection<DocumentoTrasladable> DocumentoTrasladableReadOrdenByLote(List<int> lotesId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.DocumentoTrasladable(tur.Orden.Id, tur.Fecha, tur.Orden.dbEntregaOrdenId, tur.Orden.LoteTraslado, tur.Orden.PosicionEnLote, tur.Orden.Protocolo.ProtocoloFull, tur.Orden.Paciente.ApellidoNombre, tur.Orden.ObraSocialPlan.ObraSocial.Id, tur.Orden.ObraSocialPlan.ObraSocial.Name, tur.Orden.Paciente.Importancia, tur.Id) " +
                         "FROM TurnoHQL tur WHERE tur.Orden.LoteTraslado.Id in (:lotesId) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("lotesId", lotesId);

            EntityCollection<DocumentoTrasladable> col = dalEngine.GetManyByQuery<DocumentoTrasladable>(query);
            EntityCollection<DocumentoTrasladable> ret = EliminarDocumentoTrasladableDuplicadosPorFechaMenor(col);
            ret = MergeWithEntregaPlacas(ret);

            return ret;
        }

        private EntityCollection<DocumentoTrasladable> MergeWithEntregaPlacas(EntityCollection<DocumentoTrasladable> documentos)
        {
            if (documentos == null || documentos.Count <= 0)
                return new EntityCollection<DocumentoTrasladable>();

            List<int> ordenIdsCompleta = new List<int>();
            foreach (DocumentoTrasladable dt in documentos)
                ordenIdsCompleta.Add(dt.Id);


            EntityCollection<OrdenEntregaPlaca> ordenesEntregaPlaca = Context.Session.TurnosDalc.OrdenEntregaPlacaByOrdenesId(ordenIdsCompleta);

            foreach (OrdenEntregaPlaca ordIdPlaca in ordenesEntregaPlaca)
                foreach (DocumentoTrasladable dt in documentos)
                    if (dt.Id == ordIdPlaca.OrdenId)
                        dt.PlacaEntregada = ordIdPlaca.EntregaPlaca;

            return documentos;
        }

        //private EntityCollection<DocumentoTrasladable> MergeWithEntregaPlacas(EntityCollection<DocumentoTrasladable> documentos)
        //{
        //    if (documentos == null || documentos.Count <= 0)
        //        return new EntityCollection<DocumentoTrasladable>();

        //    List<int> ordenIdsCompleta = (from dt in documentos select dt.Id).ToList();
        //    EntityCollection<List<int>> listOrdenIds = SplitOrdenesForOracle(ordenIdsCompleta);

        //    foreach (List<int> ordenIds in listOrdenIds)
        //    {
        //        var ordenesIdEntregaPlacas = (from tur in dalEngine.Query<Turno>()
        //                                      join tui in dalEngine.Query<TurnoInforme>()
        //                                        on tur.Id equals tui.TurnoID
        //                                      where ordenIds.Contains(tur.Orden.Id)
        //                                      select new { OrdenId = tur.Orden.Id, EntregaPlacas = tui.EntregaPlacas });

        //        foreach (var ordIdPlaca in ordenesIdEntregaPlacas)
        //            foreach (DocumentoTrasladable dt in documentos)
        //                if (dt.Id == ordIdPlaca.OrdenId)
        //                    dt.PlacaEntregada = ordIdPlaca.EntregaPlacas;
        //    }
        //    return documentos;
        //}

        private EntityCollection<List<int>> SplitOrdenesForOracle(List<int> ordenIdsCompleta)
        {//separa en subconjuntos de 999 elementos para que el motor de oracle pueda ejecutar la consulta
            EntityCollection<List<int>> splittedOrdenes = new EntityCollection<List<int>>();
            if (ordenIdsCompleta.Count < 1000)
                splittedOrdenes.Add(ordenIdsCompleta);
            else
            {
                List<int> ordenes = new List<int>();
                for (int index = 0; index < ordenIdsCompleta.Count; index++)
                {
                    ordenes.Add(ordenIdsCompleta[index]);
                    if (ordenes.Count >= 1000 || index == ordenIdsCompleta.Count - 1)
                    {
                        splittedOrdenes.Add(ordenes);
                        ordenes = new List<int>();
                    }
                }
            }
            return splittedOrdenes;
        }

        public EntityCollection<TurnoInformeLoteTraslado> TurnoInformeLoteTrasladoReadByLotes(List<int> lotesId)
        {
            string hql = "SELECT tilt  FROM TurnoInformeLoteTraslado tilt WHERE tilt.LoteTraslado.Id  in (:lotesId)";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("lotesId", lotesId);
            return dalEngine.GetManyByQuery<TurnoInformeLoteTraslado>(query);
        }


        public EntityCollection<LoteTraslado> LoteTrasladoReadByIds(List<int> Ids)
        {
            return dalEngine.GetManyByIds<LoteTraslado>(Ids);
        }

        #endregion

        #region TipoTraslado






        #endregion

        #region Manejo de LoteTraslado y LoteTrasladoDetalle

        public void LogEnvioPlacas(DocumentoTrasladable documento, Sector sectorEnvio)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            Context.Session.TurnosDalc.LogRegistrar((int)LogEventoEnum.EntregaPlacas, "Envio de placas al sector " + sectorEnvio.Name + " por el usuario " + Security.Current.UserInfo.User.ApyN + ". [" + documento.RegionTag + "]", documento.TurnoId);
        }

        public void TurnoInformeLoteTrasladoDelete(EntityCollection<DocumentoTrasladable> paraSacarDeLote)
        {
            // Busco las convinacion de turnoInforme con Lote a eliminar
            List<string> condiciones = new List<string>();
            foreach (DocumentoTrasladable item in paraSacarDeLote)
            {
                if (item.Lote != null)
                    condiciones.Add("tilt.LoteTraslado.Id =" + item.Lote.Id + " AND tilt.TurnoInformeId = " + item.Id);
            }

            if (condiciones.Count <= 0)
                return;

            string hql = "SELECT tilt from TurnoInformeLoteTraslado tilt WHERE ";
            for (int i = 0; i < condiciones.Count; i++)
            {
                hql += "(" + condiciones[i] + ")";
                if (i < condiciones.Count - 1)
                    hql += " OR ";
            }
            IQuery query = dalEngine.CreateQuery(hql);
            EntityCollection<TurnoInformeLoteTraslado> colToDelete = dalEngine.GetManyByQuery<TurnoInformeLoteTraslado>(query);
            dalEngine.DeleteBatch(colToDelete);
        }






        public LoteTraslado LoteTrasladadIncrementarCantidad(int idLoteTraslado)
        {
            LoteTraslado lt = dalEngine.GetById<LoteTraslado>(idLoteTraslado);
            lt.Cantidad += 1;
            return dalEngine.Update(lt);
        }

        /// <summary>
        /// Obtiene un n?mero de orden y luego persiste el detalle del lote 
        /// </summary>
        /// <param name="ltd"></param>
        /// <returns>El orden con que ingreso al nuevo registro</returns>

        public LoteTrasladoDetalle LoteTrasladoDetalleReadByLoteAndProtocolo(int loteId, string protocolo)
        {
            return LoteTrasladoDetalleReadByLoteAndProtocolo(loteId, protocolo, null);
        }

        public LoteTrasladoDetalle LoteTrasladoDetalleReadByLoteAndProtocolo(int loteId, string protocolo, string region)
        {
            string hql = "select ltd from LoteTrasladoDetalle ltd where ltd.LoteTrasladoId = :loteId and ltd.Protocolo = :protocolo and ltd.Delete = false ";
            if (!String.IsNullOrEmpty(region))
                hql += " AND ltd.RegionInforme = :region";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("protocolo", protocolo);
            query.SetParameter("loteId", loteId);
            if (!String.IsNullOrEmpty(region))
                query.SetParameter("region", region);

            return dalEngine.GetByQuery<LoteTrasladoDetalle>(query);
        }






        public EntityCollection<LoteTrasladoDetalle> LoteTrasladoDetalleReadByLoteId(int loteId)
        {
            string hql = "select ltd from LoteTrasladoDetalle ltd where ltd.LoteTrasladoId = :lote and ltd.Delete = false";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("lote", loteId);

            return dalEngine.GetManyByQuery<LoteTrasladoDetalle>(query);
        }
        #endregion

        #region Relacion DocumentosTrasladables con LoteTraslado

        [Private]
        public TurnoInformeLoteTraslado TurnoInformeLoteTrasladoUpdate(LoteTraslado loteTraslado, int turnoInformeId, int? turnoInformeLoteTrasladoAnteriorId, int posicionEnLote)
        {
            TurnoInformeLoteTraslado turnoInformeLoteTraslado = new TurnoInformeLoteTraslado();
            turnoInformeLoteTraslado.TurnoInformeId = turnoInformeId;
            turnoInformeLoteTraslado.LoteTraslado = loteTraslado;
            turnoInformeLoteTraslado.TurnoInformeLoteTrasladoAnteriorId = turnoInformeLoteTrasladoAnteriorId;
            turnoInformeLoteTraslado.PosicionEnLote = posicionEnLote;
            return dalEngine.Update<TurnoInformeLoteTraslado>(turnoInformeLoteTraslado);
        }

        #endregion

        #region Utils
        public EntityCollection<DocumentoTrasladable> ConvertirDocumentoTrasladableInformeADocumentoTrasladable(EntityCollection<DocumentoTrasladableInforme> col)
        {
            EntityCollection<DocumentoTrasladable> colResultCasteada = new EntityCollection<DocumentoTrasladable>();
            foreach (DocumentoTrasladableInforme documentoInforme in col)
                colResultCasteada.Add((DocumentoTrasladable)documentoInforme);

            return colResultCasteada;
        }

        #endregion

        public TurnoInformeLoteTraslado TurnoInformeLoteTrasladoPlacaActualReadByTurnoInforme(int turnoInformeId)
        {
            StringBuilder hql = new StringBuilder(" SELECT tui.TurnoInformeLoteTrasladoUltimo FROM TurnoInforme tui WHERE tui.Id = :turnoInformeId");
            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetInt32("turnoInformeId", turnoInformeId);
            return (TurnoInformeLoteTraslado)query.UniqueResult();
        }

        public void TurnoInformeUpdatePlaca(int turnoInformeId, int? turnoInformeLoteTrasladoId, int? estadoPlaca)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(new List<int>() { turnoInformeId }, TurnoInforme.Properties.TurnoInformeLoteTrasladoUltimo, turnoInformeLoteTrasladoId.HasValue ? (int?)turnoInformeLoteTrasladoId.Value : null);
            if (estadoPlaca.HasValue)
                dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(new List<int>() { turnoInformeId }, TurnoInforme.Properties.dbTipoUbicacionPlaca, estadoPlaca.Value);
        }

        public List<int> DocumentoTrasladableIdsReadByLote(int loteTrasladoId)
        {
            string hql = "SELECT til.TurnoInformeId FROM TurnoInformeLoteTraslado til WHERE til.LoteTraslado.Id = :loteTrasladoId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("loteTrasladoId", loteTrasladoId);
            List<int> turnosId = (List<int>)query.List<int>();
            return turnosId;
        }

        public EntityCollection<TurnoInformeLoteTraslado> TurnoInformeLoteTrasladoReadByLote(int loteId)
        {
            return dalEngine.GetManyByProperty<TurnoInformeLoteTraslado>(TurnoInformeLoteTraslado.Properties.LoteTraslado, loteId);
        }






        public void DocumentoTrasladablePlacaUpdateEstado(List<int> ids, TipoUbicacionPlacaEnum nuevoEstado)
        {
            dalEngine.UpdatePropertyBatchByIds<TurnoInforme>(ids, TurnoInforme.Properties.dbTipoUbicacionPlaca, (int)nuevoEstado);
        }

        public LoteTraslado LoteTrasladoInformeReadByInforme(int tipoTrasladoId, int informeId)
        {
            string hql = "SELECT tilt.LoteTraslado FROM TurnoInformeLoteTraslado tilt WHERE tilt.LoteTraslado.TipoTraslado.Id = :tipoTrasladoId AND tilt.TurnoInformeId = :informeId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("tipoTrasladoId", tipoTrasladoId);
            query.SetParameter("informeId", informeId);
            return dalEngine.GetByQuery<LoteTraslado>(query);
        }

        public LoteTraslado LoteTrasladoOrdenReadByOrden(int tipoTrasladoId, int ordenId)
        {
            string hql = "SELECT o.LoteTraslado FROM Orden o WHERE o.LoteTraslado.TipoTraslado.Id = :tipoTrasladoId AND o.LoteTraslado.Id = :ordenId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("tipoTrasladoId", tipoTrasladoId);
            query.SetParameter("ordenId", ordenId);
            return dalEngine.GetByQuery<LoteTraslado>(query);
        }

        public EntityCollection<LoteTrasladoDetalleHQL> LoteTrasladoDetalleHQLReadByLote(List<int> loteTrasladoIds)
        {
            LoteTraslado loteTraslado = dalEngine.GetById<LoteTraslado>(loteTrasladoIds[0]);

            switch (loteTraslado.TipoTraslado.TipoEntidad)
            {
                case TipoEntidadEnum.Informe:
                    return LoteTrasladoDeInformesDetalleHQLReadByLote(loteTrasladoIds);
                case TipoEntidadEnum.Orden:
                    return LoteTrasladoDeOrdenesDetalleHQLReadByLote(loteTrasladoIds);
                case TipoEntidadEnum.Placa:
                    return LoteTrasladoDeInformesDetalleHQLReadByLote(loteTrasladoIds);
                default:
                    throw new Exception("No implementado");
            }
        }

        private EntityCollection<LoteTrasladoDetalleHQL> LoteTrasladoDeOrdenesDetalleHQLReadByLote(List<int> loteTrasladoIds)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteTrasladoDetalleHQL(ltd.Id, tur.Fecha, ltd.Protocolo, ltd.RegionInforme, pac, tur.Orden.ObraSocialPlan.ObraSocial.Name, prt.MedicoInformante , ltd.Orden) " +
                            " FROM LoteTrasladoDetalle ltd, TurnoHQL tur JOIN tur.PracticaTurno prt, Paciente pac" +
                            " WHERE tur.DeleteFlag = false AND tur.Estado.Cancelado = false and ltd.Delete = false and ltd.LoteTrasladoId in (:loteTrasladoIds) and ltd.Protocolo = tur.Orden.Protocolo.ProtocoloFull and prt.Tipo = :tipoPractica AND tur.Orden.Paciente.Id = pac.Id ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("loteTrasladoIds", loteTrasladoIds);
            query.SetParameter("tipoPractica", (int)PracticaTurnoTipoEnum.Principal);
            return dalEngine.GetManyByQuery<LoteTrasladoDetalleHQL>(query);
        }

        private EntityCollection<LoteTrasladoDetalleHQL> LoteTrasladoDeInformesDetalleHQLReadByLote(List<int> loteTrasladoIds)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.LoteTrasladoDetalleHQL(ltd.Id, tur.Fecha, ltd.Protocolo, ltd.RegionInforme, pac, tur.Orden.ObraSocialPlan.ObraSocial.Name, tui.Informante.Medico, ltd.Orden) " +
                            " FROM LoteTrasladoDetalle ltd, TurnoHQL tur, TurnoInforme tui LEFT JOIN tui.Informante LEFT JOIN tui.Informante.Medico, Paciente pac " +
                            " WHERE tur.DeleteFlag = false AND tur.Estado.Cancelado = false and ltd.Delete = false and ltd.LoteTrasladoId in (:loteTrasladoIds) and ltd.Protocolo = tur.Orden.Protocolo.ProtocoloFull  AND tur.Orden.Paciente.Id = pac.Id  " +
                            " and tui.TurnoID = tur.Id and (ltd.RegionInforme is null or tui.RegionInforme.Tag = ltd.RegionInforme) ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("loteTrasladoIds", loteTrasladoIds);
            return dalEngine.GetManyByQuery<LoteTrasladoDetalleHQL>(query);
        }

        [Private]
        public bool LoteHasInternacionReadByLoteTrasladoId(int idLoteTraslado)
        {
            string hql = "SELECT o.InfoInternacion FROM OrdenHQL o " +
                         "WHERE o.LoteTraslado.Id = :idLoteTraslado ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idLoteTraslado", idLoteTraslado);
            query.SetMaxResults(1);

            object internacion = query.UniqueResult();

            return internacion != null;
        }

        public int LoteTrasladoCantidadReadByLoteTrasladoId(int idLoteTraslado)
        {
            string hql = "SELECT l.Cantidad FROM LoteTraslado l WHERE l.Id = :idLoteTraslado";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idLoteTraslado", idLoteTraslado);

            object cantidad = query.UniqueResult();

            if (cantidad == null)
                throw new Exception("El lote n?mero " + idLoteTraslado.ToString() + " no existe.");

            return Convert.ToInt32(cantidad);
        }

        public EntityCollection<DatosLoteTraslado> LoteTrasladoReadByParameters(bool incluyeIndividuales, DateTime? desde, DateTime? hasta, long? loteNro, string descripcion, int estado, int? centroEnvioId, int tipoTrasladoId)
        {
            StringBuilder hql = new StringBuilder(" SELECT new enfoke.Eges.Entities.Results.DatosLoteTraslado(lt.CreateDate ,lt.Id, lt.Cantidad, lt.Descripcion, elt.Id, elt.Nombre, sec.Id, sec.Name, suc.Id, suc.Name, lt.TipoTraslado.dbTipoEntidad)");
            hql.Append(" FROM LoteTraslado lt, EstadoLoteTraslado elt, Sector sec, Sucursal suc ");
            hql.Append(" WHERE lt.dbEstado = elt.Id AND lt.SectorEnvioId = sec.Id AND sec.Sucursal.Id = suc.Id AND lt.dbEstado = :estado AND lt.TipoTraslado.Id = :tipoTrasladoId ");

            if (loteNro.HasValue)
                hql.Append(" AND lt.Id = :loteNro");
            if (desde.HasValue)
                hql.Append(" AND lt.CreateDate > :desde");
            if (hasta.HasValue)
                hql.Append(" AND lt.CreateDate < :hasta");
            if (!String.IsNullOrEmpty(descripcion))
                hql.Append(" AND lt.Descripcion like '" + descripcion + "%'");
            if (centroEnvioId.HasValue)
                hql.Append(" AND suc.Id = :centroEnvioId ");
            if (!incluyeIndividuales)
                hql.Append(" AND lt.EsIndividual = false ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            query.SetParameter("estado", estado);
            query.SetParameter("tipoTrasladoId", tipoTrasladoId);
            if (loteNro.HasValue)
                query.SetParameter("loteNro", loteNro.Value);
            if (desde.HasValue)
                query.SetParameter("desde", desde.Value.Date);
            if (hasta.HasValue)
                query.SetParameter("hasta", hasta.Value.Date.AddDays(1));
            if (centroEnvioId.HasValue)
                query.SetParameter("centroEnvioId", centroEnvioId);

            EntityCollection<DatosLoteTraslado> items = dalEngine.GetManyByQuery<DatosLoteTraslado>(query);

            foreach (DatosLoteTraslado item in items)
            {
                Filter filter = new Filter();
                filter.Add(BooleanOp.And, LoteTrasladoDetalle.Properties.LoteTrasladoId, "=", item.Id);
                filter.Add(BooleanOp.And, LoteTrasladoDetalle.Properties.Delete, "=", false);

                ReadManyCommand<LoteTrasladoDetalle> readCmd = new ReadManyCommand<LoteTrasladoDetalle>(dalEngine);
                readCmd.Filter = filter;

                item.LoteTrasladoDetalles = readCmd.Execute();
            }

            return items;
        }

    }
}
