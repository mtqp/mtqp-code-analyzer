using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Text;

using enfoke.Connector;
using enfoke.Eges;
using enfoke.Eges.Entities;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using enfoke.Eges.Valorizacion;


using enfoke.Eges.Persistence.DAL;
using NHibernate;
using enfoke.Data;
using enfoke.Eges.Auditoria;
using enfoke.Eges.Entities.Results;
using enfoke.Data.Filters;
using enfoke.Eges.Persistance;
using enfoke.Eges.Persistence;
using enfoke.Serialization;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class ValorizacionesDalc : Dalc, IService
    {
        protected ValorizacionesDalc(NotConstructable dummy) : base(dummy) { }

        #region ValorizacionTipo





        #endregion

        #region Valorizacion

        public EntityCollection<Entities.Valorizacion> ValorizacionReadByListaTurnoId(List<int> turnoIds, int tipo)
        {
            ReadManyCommand<Entities.Valorizacion> readCmd = new ReadManyCommand<Entities.Valorizacion>(dalEngine);
            Filter filter = new Filter();
            filter.Add(Entities.Valorizacion.Properties.Turno,
                "in", turnoIds);
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Tipo,
              "=", tipo);
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Deleted, "=", false);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<Entities.Valorizacion> ValorizacionReadByListaTurnoId(List<int> turnoIds, int tipo, int? turnoExcluido)
        {
            ReadManyCommand<Entities.Valorizacion> readCmd = new ReadManyCommand<Entities.Valorizacion>(dalEngine);
            Filter filter = new Filter();
            filter.Add(Entities.Valorizacion.Properties.Turno,
                "in", turnoIds);
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Tipo,
              "=", tipo);
            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Deleted, "=", false);

            if (turnoExcluido.HasValue)
                filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Turno, "!=", turnoExcluido.Value);

            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<Entities.Valorizacion> ValorizacionReadByTurnosAndTipoWithItems(List<int> turnos, int tipo)
        {
            EntityCollection<Entities.Valorizacion> valorizaciones = ValorizacionReadByListaTurnoId(turnos, tipo);

            foreach (Entities.Valorizacion valorizacion in valorizaciones)
                ValorizacionReadItems(valorizacion);

            return valorizaciones;
        }

        public Entities.Valorizacion ValorizacionReadByTurnoAndTipoWithItems(int turno, int tipo)
        {
            Entities.Valorizacion valorizacion = ValorizacionReadByTurnoAndTipo(turno, tipo);

            ValorizacionReadItems(valorizacion);

            return valorizacion;
        }

        private void ValorizacionReadItems(Entities.Valorizacion valorizacion)
        {
            if (valorizacion != null)
            {
                valorizacion.Items = ValorizacionItemsReadByValorizacion(valorizacion.Id);

                // Si existen items de la valorización
                if (valorizacion.Items != null)
                    foreach (ValorizacionItem item in valorizacion.Items)
                    {
                        if (item.Medico == null && item.MedicoID.HasValue)
                            item.Medico = Context.Session.MedicosDalc.MedicoReadById(item.MedicoID.Value);

                        // Si es un set de farmacia detallado busco la valorización de sus insumos
                        if (item.Practica.TipoPractica.Id == (int)TipoPracticaEnum.SetFarmacia && item.Practica.EsDetallado == true)
                        {
                            item.ValorizacionItemInsumo = this.ValorizacionItemInsumoReadByValorizacionItemId(item.Id);
                            item.ValorizacionItemCobInsumo = this.ValorizacionItemCobInsumoReadByValorizacionItemId(item.Id);
                        }
                    }
            }
        }

        // [GG] Este cambio es para performance en la facturación
        //public EntityCollection<Entities.Valorizacion> ValorizacionesReadByTurnoIdsAndTipoWithItems(List<int> turnosId, int tipo)
        //{
        //    EntityCollection<Entities.Valorizacion> valorizaciones = new EntityCollection<enfoke.Eges.Entities.Valorizacion>();
        //    // for
        //    foreach (Entities.Valorizacion valorizacion in valorizaciones)
        //    {
        // se va llenando

        //          // Trae los hijos
        //          EntityCollection<Entities.ValorizacionItem> valorizacionItems = 
        //                        dalEngine.GetManyByPropertyList<ValorizacionItem>(ValorizacionItem.Properties.Valorizacion.Id, valorizaciones.GetIds());
        //          valorizacionItems.SortByProperty(ValorizacionItem.Properties.Valorizacion.Id);
        //          SortedMultipartData<ValorizacionItem, int> valoresOrdenados = new SortedMultipartData<ValorizacionItem, int>(
        //                                                        ValorizacionItem.Properties.Valorizacion.Id);
        //          valoresOrdenados.Add(valorizacionItems);
        //          // Le coloca los hijos a cada uno

        //    }

        //     
        //    foreach (Entities.Valorizacion valorizacion in valorizaciones)
        //        valorizacion.Items = valoresOrdenados.GetManyBySorted(valorizacion.Id);

        //    return valorizaciones;
        //}

        public Entities.Valorizacion ValorizacionReadByTurnoAndTipo(int turno, int tipo)
        {
            ReadManyCommand<Entities.Valorizacion> readCmd = new ReadManyCommand<Entities.Valorizacion>(dalEngine);

            Filter filter = new Filter();

            filter.Add(Entities.Valorizacion.Properties.Turno,
                "=", turno);

            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Tipo,
                "=", tipo);

            filter.Add(BooleanOp.And, Entities.Valorizacion.Properties.Deleted,
                "=", false);

            readCmd.Filter = filter;

            EntityCollection<Entities.Valorizacion> v = readCmd.Execute();
            if (v.Count > 0)
                return v[0];
            else
                return null;
        }

        /// <summary>
        /// Devuelvo una Valorizacion según el Id
        /// </summary>
        /// <param name="turno">Id de la Valorización</param>
        /// <returns>Una Valorizacion</returns>
        public Entities.Valorizacion ValorizacionReadById(int id)
        {
            return dalEngine.GetById<Entities.Valorizacion>(id);
        }

        public Entities.Valorizacion ValorizacionReadByIdWithItems(int id)
        {
            Entities.Valorizacion valorizacion = ValorizacionReadById(id);

            if (valorizacion != null)
                valorizacion.Items = ValorizacionItemsReadByValorizacion(valorizacion.Id);

            return valorizacion;
        }

        [Private]
        [RequiresTransaction]
        public virtual Entities.Valorizacion InsertValorizacion(ValorizacionInfo valorizacion, Turno turno, int ospID)
        {
            return InsertValorizacion(valorizacion, turno, Context.Session.ObrasSocialesDalc.ObraSocialPlanReadById(ospID));
        }

        [Private]
        public virtual Entities.Valorizacion InsertValorizacion(ValorizacionInfo valorizacion, Turno turno, ObraSocialPlan osp)
        {
            return InsertValorizacion(valorizacion, turno, osp, true);
        }

        [Private]
        [RequiresTransaction]
        public virtual Entities.Valorizacion InsertValorizacion(ValorizacionInfo valorizacion, Turno turno, ObraSocialPlan osp, bool validaDuplicacion)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            ObrasSocialesDalc ObraSocialDalc = Context.Session.ObrasSocialesDalc;

            Entities.Valorizacion val = new Entities.Valorizacion();
            val.Turno = turno;
            val.Tipo = valorizacion.Tipo;
            val.Deleted = false;

            val.TipoPlan = valorizacion.TipoPlan;
            //Si se esta creando un presupuesto agrego el tipo plan que corresponde
            //a la obra social plan. Solamente si existe un solo registro
            if (valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Presupuesto)
            {
                EntityCollection<TipoPlan> tipoPlanes = ObraSocialDalc.GetTipoPlanByObraSocialPlan(turno.Orden.ObraSocialPlanId);
                if (tipoPlanes != null && tipoPlanes.Count == 1)
                    val.TipoPlan = tipoPlanes[0];
            }
            // En la recepcion del turno hubo una revalorizacion
            else if (valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Admision && !String.IsNullOrEmpty(valorizacion.SLogRevalorizacion))
                TurnosDalc.LogRegistrar((int)LogEventoEnum.RevalorizacionAdmision, valorizacion.SLogRevalorizacion, val.Turno.Id);

            if (valorizacion.IsInMemory)
            {
                if (valorizacion.PracticaInfo.PosibleTurno != null &&
                    valorizacion.PracticaInfo.PosibleTurno.Medicos.Actuante != null)
                    val.CategoriaMedico = valorizacion.PracticaInfo.PosibleTurno.Medicos.Actuante.CategoriaMedico;
                else if (valorizacion.PracticaInfo.Medico != null &&
                    valorizacion.PracticaInfo.Medico != null)
                    val.CategoriaMedico = valorizacion.PracticaInfo.Medico.CategoriaMedico;
            }
            else
                val.CategoriaMedico = valorizacion.Valorizacion.CategoriaMedico;

            val.Observaciones = valorizacion.Observaciones;
            val.PorcentajeBonifDerechos = valorizacion.PorcentajeBonifDerechos;
            val.PorcentajeBonifHonorarios = valorizacion.PorcentajeBonifHonorarios;
            val.ImporteBonifDerechos = valorizacion.ImporteBonifDerechos;
            val.ImporteBonifHonorarios = valorizacion.ImporteBonifHonorarios;
            val.ObraSocialPlan = osp;

            //if (turno.EsPresupuesto() && (!valorizacion.NroPresupuesto.HasValue || (valorizacion.Valorizacion != null && valorizacion.Valorizacion.Turno.EsPresupuesto() && valorizacion.Valorizacion.Turno != turno)))
            // Si es un presupuesto que todavia no tiene numero
            if (turno.EsPresupuesto() && !valorizacion.NroPresupuesto.HasValue)
                valorizacion.NroPresupuesto = Context.Session.ReservasDalc.ObtenerSiguienteNumeroPresupuesto(Security.Current.UserInfo.User);

            val.NroPresupuesto = valorizacion.NroPresupuesto;
            if (valorizacion.Valorizacion != null)
                val.PorcentajeIVA = valorizacion.Valorizacion.PorcentajeIVA;

            UpdateValorizacion(val, validaDuplicacion);

            // Si estoy revalorizando a una privada o de una entonces verifica si debo cambia el TCF
            Context.Session.TurnosDalc.TurnoTipoControlFacturacionUpdatePorRevalorizacion(turno, osp);

            // Veo si tengo que cambiar el principal y grabo practicas
            ActualizaPrincipalYCantidadDePracticasTurno(turno, valorizacion.Items);

            EntityCollection<ValorizacionItem> valItems = CreaItemsParaNuevaValorizacion(val, valorizacion.Items);


            foreach (ValorizacionItem item in valItems)
            {
                if (item.Cantidad > 0)
                    ValidarReticionPractica(valorizacion.Items, item);

                EntityCollection<ValorizacionItemInsumo> valorizacionItemsInsumo = item.ValorizacionItemInsumo;
                // Solo los modulos tienen (pt.PracticaAdicional != NULL). Me traigo pt porque el valorizacionItem esta Clonado
                // entonces hace qu al terminar las transaccion (si esta en una transaccion) el valor no termine updateado
                // Cambio la cantidad y el tipo si corresponde
                item.Medico = item.PracticaTurno.MedicoInformante;

                if (item.PracticaTurno.MedicoInformante != null)
                    item.MedicoID = item.PracticaTurno.MedicoInformante.Id;

                ValorizacionItemCobInsumo vici = null;
                if (item.ValorizacionItemCobInsumo != null)
                    vici = item.ValorizacionItemCobInsumo;

                int idAnterior = item.Id;
                int idActual = 0;
                ValorizacionItem newItem = dalEngine.Update(item);

                idActual = newItem.Id;

                // Copio la valorizacion de los insumos si estos existieran
                if (valorizacionItemsInsumo != null)
                {
                    EntityCollection<ValorizacionItemInsumo> newValorizacionItemInsumo =
                        new EntityCollection<ValorizacionItemInsumo>();
                    foreach (ValorizacionItemInsumo insItem in valorizacionItemsInsumo)
                    {
                        ValorizacionItemInsumo newinsItem = new ValorizacionItemInsumo();
                        newinsItem.ImporteCliente = insItem.ImporteCliente;
                        newinsItem.ImporteOs = insItem.ImporteOs;
                        newinsItem.PlanPracticaInsumo = insItem.PlanPracticaInsumo;
                        newinsItem.ValorizacionItemId = newItem.Id;
                        newinsItem.ImporteBonificaciones = insItem.ImporteBonificaciones;
                        newinsItem.PorcentajeBonificaciones = insItem.PorcentajeBonificaciones;
                        newinsItem.PorcentajeOS = insItem.PorcentajeOS;
                        newinsItem.TipoInsumoCategoria = insItem.TipoInsumoCategoria;

                        newValorizacionItemInsumo.Add(newinsItem);

                    }

                    this.ValorizacionItemInsumoUpadteMany(newValorizacionItemInsumo);

                }
                if (vici != null)
                {
                    if (idActual != idAnterior)
                        vici.Id = 0;

                    vici.ValorizacionItem = newItem;
                    dalEngine.Update(vici);
                }
            }

            return val;
        }

        private void ActualizaPrincipalYCantidadDePracticasTurno(Turno turno, EntityCollection<ValorizacionItem> valItems)
        {
            Entities.ValorizacionItem nuevoPrincipal = null;
            if (TienePrincipalInvalido(valItems))
                nuevoPrincipal = ObtienePosiblePrincipal(valItems);
            // 1. Si es nueva, la graba
            foreach (Entities.ValorizacionItem item in valItems)
            {
                if (item.PracticaTurno != null && item.PracticaTurno.Id == 0)
                    dalEngine.Update(item.PracticaTurno);

                // Esto sucede para ordenes multiples porque copia una valorizacion para cada turno y debe cambiarse el prt al que apunta
                if (turno.Orden.EsMultiple && item.PracticaTurno != null && item.PracticaTurno.TurnoId != turno.Id)
                    item.PracticaTurno = dalEngine.GetByProperty<PracticaTurno>(PracticaTurno.Properties.TurnoId, turno.Id);

                // Lee la entidad liviana de update
                // y actualiza atributos
                PracticaTurnoForUpdateCantidadTipo ptu = ResuelvePracticaTurnoForUpdate(turno, item);
                // Define principal
                if (item.DebeDejarDeSerPrincipal())
                    ptu.Tipo = (int)PracticaTurnoTipoEnum.Exposicion;
                if (item == nuevoPrincipal)
                    ptu.Tipo = (int)PracticaTurnoTipoEnum.Principal;
                // Pone cantidad
                ptu.Cantidad = item.Cantidad;
                PracticaTurno praTur = dalEngine.GetById<PracticaTurno>(item.PracticaTurno.Id);
                praTur.Tipo = ptu.Tipo;
                praTur.Cantidad = ptu.Cantidad;
                if (item.PracticaTurno != null)
                    praTur.NroBono = item.PracticaTurno.NroBono;

                item.PracticaTurno = praTur;
                item.PracticaTurno.Cantidad = item.Cantidad;
                // Graba
                dalEngine.Update(ptu);
            }
        }

        ValorizacionItem ObtienePosiblePrincipal(EntityCollection<ValorizacionItem> valItems)
        {
            foreach (ValorizacionItem item in valItems)
                if (item.PuedeSerPrincipal()) return item;
            throw new Exception("No hay una práctica principal posible.");
        }

        bool TienePrincipalInvalido(EntityCollection<ValorizacionItem> valItems)
        {
            foreach (Entities.ValorizacionItem item in valItems)
            {
                if (item.DebeDejarDeSerPrincipal())
                    return true;
            }
            return false;
        }

        private PracticaTurnoForUpdateCantidadTipo ResuelvePracticaTurnoForUpdate(Turno turno, ValorizacionItem item)
        {
            if (item.PracticaTurno == null)
                // En reserva no importa si tiene o no PracticaAdicional ya que no se puede repetir con y sin para la misma practica. Una o la otra.
                item.PracticaTurno = Context.Session.TurnosDalc.PracticaTurnoReadByTurnoAndPractica(turno.Id, item.Practica.Id);

            return dalEngine.GetById<PracticaTurnoForUpdateCantidadTipo>(item.PracticaTurno.Id);
        }

        private EntityCollection<ValorizacionItem> CreaItemsParaNuevaValorizacion(Entities.Valorizacion valorizacionPadre, EntityCollection<ValorizacionItem> valorizacionesItemExistentes)
        {
            EntityCollection<ValorizacionItem> valItems = new EntityCollection<ValorizacionItem>();
            for (int i = 0; i < valorizacionesItemExistentes.Count; i++)
            {
                ValorizacionItem itemCopiado = TypeUtils.Clone(valorizacionesItemExistentes[i]);
                itemCopiado.Id = 0;
                itemCopiado.Valorizacion = valorizacionPadre;
                itemCopiado.PracticaTurno = ResolverPracticaTurno(valorizacionPadre, valorizacionesItemExistentes[i].PracticaTurno);
                valItems.Add(itemCopiado);
            }
            return valItems;
        }

        /// <summary>
        /// Si se esta copiando la valorizacion de un turno a otro, aca es donde hay que buscar cual sería la nueva practicaTurno del valorizacionItem copiandose
        /// </summary>
        private PracticaTurno ResolverPracticaTurno(Entities.Valorizacion valorizacionPadre, PracticaTurno practicaTurno)
        {
            // Si es el mismo turno
            if (valorizacionPadre.Turno.Id == practicaTurno.TurnoId)
                return practicaTurno;

            // Sino busco dentro de los practicaTurno del nuevo turno cual se parece a este
            EntityCollection<PracticaTurno> pts = Context.Session.TurnosDalc.PracticaTurnoReadByTurno(valorizacionPadre.Turno.Id);
            foreach (PracticaTurno practicaNueva in pts)
            {
                if (practicaNueva.MismosValores(practicaTurno, true))
                    return practicaNueva;
            }

            throw new Exception("Se esta copiando la valorización de forma incorrecta");
        }

        [RequiresTransaction]
        internal protected virtual void ValorizacionDeleteFromDB(Entities.Valorizacion valorizacion)
        {
            ValorizacionItemDeleteFromDB(valorizacion);

            dalEngine.Delete(valorizacion);
        }

        internal void ValorizacionItemDeleteFromDB(Entities.Valorizacion valorizacion)
        {
            EntityCollection<ValorizacionItem> items = ValorizacionItemsReadByValorizacion(valorizacion.Id);

            dalEngine.DeleteBatch(items);
        }

        [Private]
        public virtual void ValorizacionDelete(Entities.Valorizacion valorizacion)
        {
            ValorizacionDelete(valorizacion, valorizacion.Items);
        }

        [Private]
        public virtual void ValorizacionUndoDelete(Entities.Valorizacion valorizacion)
        {
            ValorizacionUndoDelete(valorizacion, valorizacion.Items);
        }

        internal void ValorizacionDelete(Entities.Valorizacion valorizacion, bool validaInclucionComprobante)
        {
            ValorizacionDelete(valorizacion, valorizacion.Items, validaInclucionComprobante);
        }

        internal void ValorizacionDelete(Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items)
        {
            ValorizacionDelete(valorizacion, items, true);
        }

        internal void ValorizacionDelete(Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items, bool validaInclucionComprobante)
        {
            if (validaInclucionComprobante && valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Prefacturacion)
            {
                Turno turno = valorizacion.Turno;
                if (turno.TipoControlFacturacion.Id == (int)TipoControlFacturacionEnum.PreFacturado || turno.TipoControlFacturacion.Id == (int)TipoControlFacturacionEnum.Facturado)
                    throw new Exception("No se puede eliminar una valorización que se encuentra dentro de un comprobante.");
            }

            // Marco como Eliminada
            valorizacion.Deleted = true;

            // Audito
            Audit.AuditDelete(valorizacion, Security.Current.UserInfo.User.Id);
            // Guardo la Valorización
            valorizacion = UpdateValorizacion(valorizacion);

            //Elimino los items de la valorización
            if (items != null)
                this.ValorizacionItemDelete(items);
            else
            {
                //busco los items por si en la entidad no estaban cargados
                this.ValorizacionItemDelete(this.ValorizacionItemsReadByValorizacion(valorizacion.Id));
            }
        }

        internal void ValorizacionUndoDelete(Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items)
        {
            // Marco como Eliminada
            valorizacion.Deleted = false;
            valorizacion.DeleteDate = null;
            valorizacion.DeleteUser = null;

            // Guardo la Valorización
            valorizacion = UpdateValorizacion(valorizacion);

            //Elimino los items de la valorización
            if (items != null)
                this.ValorizacionItemUndoDelete(items);
            else
            {
                //busco los items por si en la entidad no estaban cargados
                this.ValorizacionItemUndoDelete(this.ValorizacionItemsReadByValorizacion(valorizacion.Id));
            }
        }

        /// <summary>
        /// Actualizo una Valorizacion con sus Items
        /// </summary>
        /// <param name="valorizacion">Valorizacion</param>
        /// <param name="items">Items</param>
        /// <param name="user">Usuario de la Operación</param>
        [RequiresTransaction]
        public virtual Entities.Valorizacion ValorizacionUpdate(Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items, String logModificaciones, String logRevalorizacion)
        {
            //hack: Modificiar el texto que loguea en las actualizaciones

            // Actualizo la Valorizacion
            valorizacion = UpdateValorizacion(valorizacion);

            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

            // Logueo Revalorizacion (si la hubo)
            ValorizacionLogUpdate(valorizacion, logRevalorizacion, logModificaciones);

            TurnosDalc turnosDalc = Context.Session.TurnosDalc;
            EntityCollection<PracticaTurno> practicasTurno = new EntityCollection<PracticaTurno>();

            foreach (ValorizacionItem item in items)
            {
                ValorizacionItem objItem = item;

                EntityCollection<ValorizacionItemInsumo> valorizacionItemInsumos = item.ValorizacionItemInsumo;
                ValorizacionItemCobInsumo valCobInsumo = item.ValorizacionItemCobInsumo;

                if (item.Valorizacion == null)
                    item.Valorizacion = valorizacion;


                // Actualizo el Item de la Valorizacion
                objItem = dalEngine.Update<ValorizacionItem>(objItem);

                EntityCollection<ValorizacionItemInsumo> newValorizacionItemInsumos = new EntityCollection<ValorizacionItemInsumo>();
                if (valorizacionItemInsumos != null)
                {
                    foreach (ValorizacionItemInsumo itemInsumo in valorizacionItemInsumos)
                    {
                        ValorizacionItemInsumo newItemInsumo = new ValorizacionItemInsumo();

                        newItemInsumo.ImporteCliente = itemInsumo.ImporteCliente;
                        newItemInsumo.ImporteOs = itemInsumo.ImporteOs;
                        newItemInsumo.PlanPracticaInsumo = itemInsumo.PlanPracticaInsumo;
                        newItemInsumo.ValorizacionItemId = objItem.Id;

                        newItemInsumo.ImporteBonificaciones = itemInsumo.ImporteBonificaciones;
                        newItemInsumo.PorcentajeBonificaciones = itemInsumo.PorcentajeBonificaciones;
                        newItemInsumo.PorcentajeOS = itemInsumo.PorcentajeOS;
                        newItemInsumo.TipoInsumoCategoria = itemInsumo.TipoInsumoCategoria;

                        newValorizacionItemInsumos.Add(newItemInsumo);
                    }
                }

                if (valCobInsumo != null)
                {
                    ValorizacionItemCobInsumo newValCobInsumo = new ValorizacionItemCobInsumo();

                    newValCobInsumo.Id = valCobInsumo.Id;
                    newValCobInsumo.BonifContraste = valCobInsumo.BonifContraste;
                    newValCobInsumo.BonifDescartable = valCobInsumo.BonifDescartable;
                    newValCobInsumo.BonifMedicamento = valCobInsumo.BonifMedicamento;
                    newValCobInsumo.BonifRadioactivo = valCobInsumo.BonifRadioactivo;
                    newValCobInsumo.CoberturaContraste = valCobInsumo.CoberturaContraste;
                    newValCobInsumo.CoberturaDescartable = valCobInsumo.CoberturaDescartable;
                    newValCobInsumo.CoberturaMedicamento = valCobInsumo.CoberturaMedicamento;
                    newValCobInsumo.CoberturaRadioactivo = valCobInsumo.CoberturaRadioactivo;
                    newValCobInsumo.ImporteContraste = valCobInsumo.ImporteContraste;
                    newValCobInsumo.ImporteDescartable = valCobInsumo.ImporteDescartable;
                    newValCobInsumo.ImporteMedicamento = valCobInsumo.ImporteMedicamento;
                    newValCobInsumo.ImporteRadioactivo = valCobInsumo.ImporteRadioactivo;
                    newValCobInsumo.ValorizacionItem = objItem;
                    dalEngine.Update(newValCobInsumo);
                }

                this.ValorizacionItemInsumoUpadteMany(newValorizacionItemInsumos);

                // Se encarga de actualizar las cantidad de las practicas turno en base a la cantidad seteada en la valorizacion.
                item.PracticaTurno.Cantidad = item.Cantidad;
                practicasTurno.Add(item.PracticaTurno);
            }

            if (practicasTurno.Count > 0)
                turnosDalc.PracticaTurnoUpdateMany(practicasTurno);

            CambiarPracticaPrincipalSiCorresponde(valorizacion, items);

            CrearEliminarTurnoInformesSiCorresponde(valorizacion, items);

            return valorizacion;
        }

        private void ValorizacionLogUpdate(Entities.Valorizacion valorizacion, string logRevalorizacion, string logModificaciones)
        {
            TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
            if (String.IsNullOrEmpty(logRevalorizacion) == false)
            {
                if (valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Admision)
                    TurnosDalc.LogRegistrar((int)LogEventoEnum.RevalorizacionAdmision, logRevalorizacion, valorizacion.Turno.Id);
                else
                    TurnosDalc.LogRegistrar((int)LogEventoEnum.ModificacionValorizacion, logRevalorizacion, valorizacion.Turno.Id);
            }
            else if (String.IsNullOrEmpty(logModificaciones) == false)
                TurnosDalc.LogRegistrar((int)LogEventoEnum.ModificacionValorizacion, logModificaciones, valorizacion.Turno.Id);
        }

        private void CrearEliminarTurnoInformesSiCorresponde(enfoke.Eges.Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items)
        {
            if (valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Presupuesto)
                return;

            // Solo realiza los cambios en los TurnoInforme si es que el servicio de la Practica NO tiene Informante por turno.
            if (items[0].Practica.ServicioEspecialidad.Servicio.InformantePorTurno == false)
            {
                TurnosDalc turnosDalc = Context.Session.TurnosDalc;
                InformesDalc informesDalc = Context.Session.InformesDalc;

                Turno turno = turnosDalc.TurnoReadById(valorizacion.Turno.Id);
                turno.Valorizacion = new ValorizacionInfo(valorizacion, items);

                EstadoTurno estadoTurno = turnosDalc.EstadoTurnoReadById(turno.EstadoTurnoID);

                // Si el estado del turno es Informado, no se agregan TurnoInformes ni se reasigna el EstadoTurno
                if (estadoTurno.Informado == true)
                    return;

                EntityCollection<TurnoInforme> turnoInformesNuevo = turnosDalc.CrearInformesTurno(turno, true);
                EntityCollection<TurnoInforme> turnoInformesViejos = informesDalc.TurnoInformeReadByTurno(turno.Id);

                InsertarTurnoInformesQueCorresponden(turnoInformesNuevo, turnoInformesViejos, informesDalc);

                // Si no existe ningun informe con fecha ya prometida, entonces guardo la nueva fecha que se le seteo al turno.
                if (informesDalc.ExisteInformePrometidoByIdTurno(turno.Id) == false)
                    turnosDalc.TurnoUpdate(turno);
            }
        }

        private void InsertarTurnoInformesQueCorresponden(EntityCollection<TurnoInforme> turnoInformesNuevo, EntityCollection<TurnoInforme> turnoInformesViejos, InformesDalc informesDalc)
        {
            EntityCollection<TurnoInforme> turnoInformesAgregar = new EntityCollection<TurnoInforme>();
            EntityCollection<TurnoInforme> turnoInformesActualizar = new EntityCollection<TurnoInforme>();

            // Ordeno los turnoInformesNuevos en base a la region y a la mayor cantidad de dias de demora (mayor fecha).
            turnoInformesNuevo.Sort(new Comparison<TurnoInforme>(delegate(TurnoInforme left, TurnoInforme right)
            {
                // Comparo los ids de las regiones
                int retorno = left.RegionInforme.Id.CompareTo(right.RegionInforme.Id);

                // Comparo las fechas de entrega para ordenarlas de mayor a menor.
                if (retorno == 0)
                    retorno = right.FechaEntrega.Value.CompareTo(left.FechaEntrega.Value);

                return retorno;
            }));

            // Selecciono los turnoInformes a insertar
            foreach (TurnoInforme ti in turnoInformesNuevo)
            {
                Predicate<TurnoInforme> igualRegion = delegate(TurnoInforme compare)
                {
                    return (compare.RegionInforme.Id == ti.RegionInforme.Id);
                };

                // Si no está persistido en la base el turnoInforme, lo agrego a la lista de los que voy a insertar.
                if (turnoInformesViejos.Find(igualRegion) == null)
                {
                    // Toma el turnoInforme con mayor fecha de entrega.
                    TurnoInforme tiAgregar = TurnoInforme.GetTurnoInformeConMayorFechaEntregaByRegion(turnoInformesNuevo, ti.RegionInforme.Id);

                    // Si ya no fue agregado, entonces lo inserto.
                    if (turnoInformesAgregar.Contains(tiAgregar) == false)
                        turnoInformesAgregar.Add(tiAgregar);
                }
            }

            // Selecciono los turnoInformes a actualizar
            foreach (TurnoInforme tiViejo in turnoInformesViejos)
            {
                if (informesDalc.ExisteInformePrometidoByIdTurnoInforme(tiViejo.Id) == false)
                {
                    // Toma el turnoInforme con mayor fecha de entrega.
                    TurnoInforme tiActualizar = TurnoInforme.GetTurnoInformeConMayorFechaEntregaByRegion(turnoInformesNuevo, tiViejo.RegionInforme.Id);

                    // Si encontró uno nuevo con mayor fecha, lo guardo en la lista de los que voy a actualizar
                    if (tiActualizar != null)
                    {
                        tiViejo.FechaEntrega = tiActualizar.FechaEntrega;
                        tiViejo.FechaEntregaOriginal = tiActualizar.FechaEntregaOriginal;

                        turnoInformesActualizar.Add(tiViejo);
                    }
                }
            }

            // Actualizo si hay algo para actualizar.
            if (turnoInformesActualizar.Count > 0)
            {
                turnoInformesActualizar = dalEngine.UpdateCollection<TurnoInforme>(turnoInformesActualizar);
            }

            // Inserto si hay algo para insertar.
            if (turnoInformesAgregar.Count > 0)
            {
                turnoInformesAgregar = dalEngine.UpdateCollection<TurnoInforme>(turnoInformesAgregar);

                foreach (TurnoInforme informe in turnoInformesAgregar)
                {
                    // Creo el Log del Informe
                    TurnoInformeLog til = new TurnoInformeLog(informe.Id);


                    til = dalEngine.Update<TurnoInformeLog>(til);

                    // Grabo el Historico de Estados
                    informesDalc.EstadoInformeHistoricoCreate(false, informe);
                }
            }
        }

        private void EliminarTurnoInformesQueCorresponden(EntityCollection<TurnoInforme> turnoInformesNuevo, EntityCollection<TurnoInforme> turnoInformesViejos, InformesDalc informesDalc, TurnosDalc turnosDalc)
        {
            EntityCollection<TurnoInforme> turnoinformesBorrar = new EntityCollection<TurnoInforme>();

            // se eliminan los turnoInformes que se encuentren en estado Pendiente y que
            // cuya practica ahora tiene cantidad cero.
            foreach (TurnoInforme ti in turnoInformesViejos)
            {
                Predicate<TurnoInforme> predicate = delegate(TurnoInforme compare)
                {
                    return (compare.RegionInforme.Id == ti.RegionInforme.Id);
                };

                if (turnoInformesNuevo.Find(predicate) == null &&
                    ti.EstadoInforme.Id == (int)EstadoInformeEnum.Pendiente)
                {
                    informesDalc.EstadoInformeHistoricoDeleteByTurnoInformeID(ti.Id);
                    informesDalc.TurnoInformeLogDeleteByTurnoInformeID(ti.Id);
                    turnoinformesBorrar.Add(ti);
                }
            }

            if (turnoinformesBorrar.Count > 0)
                informesDalc.TurnoInformeDeleteMany(turnoinformesBorrar);
        }

        /// <summary>
        /// Cambia en la practica principal del turno si corresponde.
        /// (Corresponde si: la practica principal tiene cantidad 0 y existe otra con cantidad > 0 del tipo practica sin se subsiguiente)
        /// </summary>
        /// <param name="valorizacion"></param>
        /// <param name="items"></param>
        private void CambiarPracticaPrincipalSiCorresponde(enfoke.Eges.Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items)
        {
            ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, items);

            if (valorizacionInfo.SeDebeCambiarPracticaPrincipal())
            {
                if (valorizacionInfo.EsPosibleCambiarPracticaPrincipal())
                {
                    PracticaTurno ptPrincipalToSubSiguiente = valorizacionInfo.GetPracticaPrincipalCCIC();
                    PracticaTurno ptSubSiguienteToPrincipal = valorizacionInfo.GetPracticaSubSiguienteToPrincipalCCMC();

                    if (ptPrincipalToSubSiguiente == null || ptSubSiguienteToPrincipal == null)
                        throw new Exception();

                    ptPrincipalToSubSiguiente.Tipo = (int)PracticaTurnoTipoEnum.Exposicion;
                    ptSubSiguienteToPrincipal.Tipo = (int)PracticaTurnoTipoEnum.Principal;

                    dalEngine.Update(ptPrincipalToSubSiguiente);
                    dalEngine.Update(ptSubSiguienteToPrincipal);
                }
                else
                    throw new Exception("En la valorización del turno no existe una práctica principal con cantidad mayor a cero. La operación no puede ser completada. Consulte con el administrador del sistema.");
            }
        }

        private Entities.Valorizacion UpdateValorizacion(Entities.Valorizacion valorizacion)
        {
            return UpdateValorizacion(valorizacion, true);
        }


        private Entities.Valorizacion UpdateValorizacion(Entities.Valorizacion valorizacion, bool validaDuplicacion)
        {
            // Si es nuevo, valido no estar duplicando un tipo
            if (valorizacion.Id == 0)
            {
                Turno turno = valorizacion.Turno;
                ValorizacionTipo valTipo = valorizacion.Tipo;
                if (validaDuplicacion)
                {
                    enfoke.Eges.Entities.Valorizacion valYaCreada = ValorizacionReadByTurnoAndTipo(turno.Id, valTipo.Id);
                    if (valYaCreada != null)
                        throw new Exception("Valorizacion Duplicada. Se esta intentando insertar una valorizacion duplicada.");
                }
            }

            valorizacion = dalEngine.Update<Entities.Valorizacion>(valorizacion);

            return valorizacion;
        }

        public bool ValorizacionModificada(Entities.Valorizacion valorizacionNueva, EntityCollection<ValorizacionItem> itemsNuevos, Entities.Valorizacion valorizacionAnterior, ref String logModificacion)
        {
            if (valorizacionAnterior != null && valorizacionAnterior.Tipo.Id != (int)ValorizacionTiposEnum.Presupuesto)
            {
                bool esModificacion = false;
                int? modificadoValorizacion = null;
                int? modAnt = valorizacionAnterior.Modificado;
                int? mod = valorizacionAnterior.Modificado;

                #region ModCantidadPracticas

                if (itemsNuevos.Count != valorizacionAnterior.Items.Count)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModCantidadPracticas) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModCantidadPracticas);

                    }
                    else
                        modificadoValorizacion = (int)ValorizacionModificacionEnum.ModCantidadPracticas;

                    logModificacion += "Cantidad de prácticas, ";
                    esModificacion = true;
                }

                #endregion

                #region ModObraSocialPlan

                if (valorizacionNueva.ObraSocialPlan.Id != valorizacionAnterior.ObraSocialPlan.Id)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModObraSocialPlan) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModObraSocialPlan);

                    }
                    else
                        modificadoValorizacion = ((modificadoValorizacion.HasValue ? modificadoValorizacion : 0) + (int)ValorizacionModificacionEnum.ModObraSocialPlan);

                    logModificacion += "Plan, ";
                    esModificacion = true;
                }

                #endregion

                #region ModPorcentajeIva

                if (valorizacionNueva.TipoPlan.Id != valorizacionAnterior.TipoPlan.Id)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModPorcentajeIva) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModPorcentajeIva);

                    }
                    else
                        modificadoValorizacion = ((modificadoValorizacion.HasValue ? modificadoValorizacion : 0) + (int)ValorizacionModificacionEnum.ModPorcentajeIva);

                    logModificacion += "Porcentaje de iva, ";
                    esModificacion = true;
                }

                #endregion

                #region ModHonorarios

                if (valorizacionNueva.ImporteBonifHonorarios != valorizacionAnterior.ImporteBonifHonorarios
                    || valorizacionNueva.PorcentajeBonifHonorarios != valorizacionAnterior.PorcentajeBonifHonorarios)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModHonorarios) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModHonorarios);

                    }
                    else
                        modificadoValorizacion = ((modificadoValorizacion.HasValue ? modificadoValorizacion : 0) + (int)ValorizacionModificacionEnum.ModHonorarios);

                    logModificacion += "Honorarios, ";
                    esModificacion = true;
                }

                #endregion

                #region ModDerechos

                if (valorizacionNueva.ImporteBonifDerechos != valorizacionAnterior.ImporteBonifDerechos ||
                    valorizacionNueva.PorcentajeBonifDerechos != valorizacionAnterior.PorcentajeBonifDerechos)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModDerechos) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModDerechos);

                    }
                    else
                        modificadoValorizacion = ((modificadoValorizacion.HasValue ? modificadoValorizacion : 0) + (int)ValorizacionModificacionEnum.ModDerechos);

                    logModificacion += "Derechos, ";
                    esModificacion = true;
                }
                #endregion

                #region ModObservaciones

                valorizacionAnterior.Observaciones = String.IsNullOrEmpty(valorizacionAnterior.Observaciones) ? String.Empty : valorizacionAnterior.Observaciones;
                valorizacionNueva.Observaciones = String.IsNullOrEmpty(valorizacionNueva.Observaciones) ? String.Empty : valorizacionNueva.Observaciones;

                if (valorizacionNueva.Observaciones != valorizacionAnterior.Observaciones)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = valorizacionAnterior.Modificado.Value & (int)ValorizacionModificacionEnum.ModObservaciones) == 0)
                            modAnt = ((modAnt.Value) + (int)ValorizacionModificacionEnum.ModObservaciones);
                    }
                    else
                        modificadoValorizacion = ((modificadoValorizacion.HasValue ? modificadoValorizacion : 0) + (int)ValorizacionModificacionEnum.ModObservaciones);

                    logModificacion += "Observaciones, ";
                    esModificacion = true;
                }

                #endregion

                if (modAnt.HasValue)
                    valorizacionNueva.Modificado = modAnt;
                else
                    valorizacionNueva.Modificado = modificadoValorizacion;

                // Evaluo que se modificó en cada item
                esModificacion = ValorizacionItemModificados(itemsNuevos, valorizacionAnterior, esModificacion, ref logModificacion);

                if (String.IsNullOrEmpty(logModificacion) == false)
                    logModificacion = logModificacion.Substring(0, logModificacion.Length - 2);

                return esModificacion;
            }
            else
                return true;
        }

        public void UpdateObservacionesValorizacion(List<int> turnoIDs, string observaciones, ValorizacionTiposEnum? valorizacionTipo)
        {
            Filter filter = new Filter
                                {
                                    {ValorizacionHQL.Properties.Turno.Id, "IN", turnoIDs},
                                    {BooleanOp.And, ValorizacionHQL.Properties.Deleted, "=", false}
                                };

            if (valorizacionTipo.HasValue)
                filter.Add(BooleanOp.And, ValorizacionHQL.Properties.Tipo.Id, "=", (int)valorizacionTipo.Value);

            dalEngine.UpdatePropertyBatchByFilter<ValorizacionHQL>(filter, ValorizacionHQL.Properties.Observaciones, observaciones);
        }

        private static bool ValorizacionItemModificados(EntityCollection<ValorizacionItem> itemsNuevos, Entities.Valorizacion valorizacionAnterior, bool esModificacion, ref String logModificacion)
        {
            EntityCollection<ValorizacionItem> itemsAnteriores = valorizacionAnterior.Items;
            itemsAnteriores.SortByProperty(ValorizacionItem.Properties.Id);
            itemsNuevos.SortByProperty(ValorizacionItem.Properties.Id);
            bool modItem = false;
            for (int i = 0; i < itemsAnteriores.Count; i++)
            {
                int? modificadoItem = null;
                int? modAnterior = null;
                int? mod = null;

                ValorizacionItem itemActual = itemsNuevos[i];
                ValorizacionItem itemAnterior = itemsAnteriores[i];

                mod = itemAnterior.Modificado;
                modAnterior = itemAnterior.Modificado;

                if (itemActual.CodigoFicticio != itemAnterior.CodigoFicticio)
                    esModificacion = true;

                if (itemActual.TipoCobertura.Id != itemAnterior.TipoCobertura.Id)
                    esModificacion = true;

                if (itemActual.TipoCoseguroID != itemAnterior.TipoCoseguroID)
                    esModificacion = true;

                #region ModCantPractica

                if (itemActual.Cantidad != itemAnterior.Cantidad)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModCantPractica) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModCantPractica);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModCantPractica);

                    logModificacion += "cantidad en item, ";

                    if (!modItem)
                        modItem = true;
                }

                #endregion

                #region Coseguro

                if (itemActual.ImporteCoseguro != itemAnterior.ImporteCoseguro)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModCoseguro) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModCoseguro);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModCoseguro);

                    logModificacion += "Coseguro, ";

                    if (!modItem)
                        modItem = true;
                }
                #endregion

                #region ModGastos

                if (itemActual.ImporteDerechos != itemAnterior.ImporteDerechos ||
                    itemActual.ImporteDerechosDif != itemAnterior.ImporteDerechosDif ||
                    itemActual.ImporteDerechosExt != itemAnterior.ImporteDerechosExt ||
                    itemActual.PorcentajeDerechos != itemAnterior.PorcentajeDerechos)
                {

                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModGastos) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModGastos);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModGastos);

                    logModificacion += "Derechos, ";

                    if (!modItem)
                        modItem = true;
                }

                #endregion

                #region ModHonorarios

                if (itemActual.ImporteHonorarios != itemAnterior.ImporteHonorarios ||
                    itemActual.ImporteHonorariosDif != itemAnterior.ImporteHonorariosDif ||
                    itemActual.ImporteHonorariosExt != itemAnterior.ImporteHonorariosExt ||
                    itemActual.PorcentajeHonorarios != itemAnterior.PorcentajeHonorarios ||
                    itemActual.ImporteHonorarioInterno != itemAnterior.ImporteHonorarioInterno)
                {

                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModHonorarios) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModHonorarios);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModHonorarios);

                    logModificacion += "Honorarios, ";

                    if (!modItem)
                        modItem = true;
                }

                #endregion

                #region ModModulo

                if (itemActual.ImporteModulo != itemAnterior.ImporteModulo ||
                    itemActual.ImporteModuloDif != itemAnterior.ImporteModuloDif ||
                    itemActual.ImporteModuloExt != itemAnterior.ImporteModuloExt ||
                    itemActual.PorcentajeModulo != itemAnterior.PorcentajeModulo)
                {
                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModModulo) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModModulo);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModModulo);

                    logModificacion += "Módulo, ";

                    if (!modItem)
                        modItem = true;
                }

                #endregion

                #region ModInsumo

                if (itemActual.ImporteInsumos != itemAnterior.ImporteInsumos ||
                    itemActual.ImporteInsumosDif != itemAnterior.ImporteInsumosDif ||
                   itemActual.ImporteInsumosExt != itemAnterior.ImporteInsumosExt ||
                    itemActual.PorcentajeInsumos != itemAnterior.PorcentajeInsumos ||
                    ValorizacionCoberturaInsumoModificada(itemActual.ValorizacionItemCobInsumo, itemAnterior.ValorizacionItemCobInsumo) == true)
                {

                    if (mod.HasValue)
                    {
                        if ((mod = itemAnterior.Modificado.Value & (int)ValorizacionItemModificacionEnum.ModInsumo) == 0)
                            modAnterior = ((modAnterior.Value) + (int)ValorizacionItemModificacionEnum.ModInsumo);
                    }
                    else
                        modificadoItem = ((modificadoItem.HasValue ? modificadoItem : 0) + (int)ValorizacionItemModificacionEnum.ModInsumo);

                    logModificacion += "Insumo, ";

                    if (!modItem)
                        modItem = true;
                }

                #endregion

                if (modAnterior.HasValue)
                    itemActual.Modificado = modAnterior;
                else
                    itemActual.Modificado = modificadoItem;

                if (!esModificacion)
                    esModificacion = modItem;
            }

            return esModificacion;
        }

        private static bool ValorizacionCoberturaInsumoModificada(ValorizacionItemCobInsumo newValCobInsumo, ValorizacionItemCobInsumo valCobInsumo)
        {

            if (newValCobInsumo == null && valCobInsumo == null)
                return false;
            else if (newValCobInsumo == null || valCobInsumo == null)
                return true;
            else if (newValCobInsumo.BonifContraste == valCobInsumo.BonifContraste &&
            newValCobInsumo.BonifDescartable == valCobInsumo.BonifDescartable &&
            newValCobInsumo.BonifMedicamento == valCobInsumo.BonifMedicamento &&
            newValCobInsumo.BonifRadioactivo == valCobInsumo.BonifRadioactivo &&
            newValCobInsumo.CoberturaContraste == valCobInsumo.CoberturaContraste &&
            newValCobInsumo.CoberturaDescartable == valCobInsumo.CoberturaDescartable &&
            newValCobInsumo.CoberturaMedicamento == valCobInsumo.CoberturaMedicamento &&
            newValCobInsumo.CoberturaRadioactivo == valCobInsumo.CoberturaRadioactivo &&
            newValCobInsumo.ImporteContraste == valCobInsumo.ImporteContraste &&
            newValCobInsumo.ImporteDescartable == valCobInsumo.ImporteDescartable &&
            newValCobInsumo.ImporteMedicamento == valCobInsumo.ImporteMedicamento &
            newValCobInsumo.ImporteRadioactivo == valCobInsumo.ImporteRadioactivo)
                return false;

            return true;
        }

        [Private]
        public EntityCollection<Entities.Valorizacion> ValorizacionActualizar(Entities.Valorizacion valorizacion, EntityCollection<ValorizacionItem> items, string logRevalorizacion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, items);
            FullValorizacion fullValorizacion = new FullValorizacion(valorizacionInfo, modalidadCoseguro);

            EntityCollection<FullValorizacion> fValorizacion = new EntityCollection<FullValorizacion>();
            fValorizacion.Add(fullValorizacion);

            return ValorizacionActualizar(fValorizacion, logRevalorizacion);

        }

        private void ValidarReticionPractica(EntityCollection<ValorizacionItem> items, ValorizacionItem itemABuscar)
        {
            if (itemABuscar != null && itemABuscar.PracticaTurno != null)
            {
                bool esDeModulo = (itemABuscar.PracticaTurno.PracticaAdicional != null);

                foreach (ValorizacionItem item in items)
                {
                    if (item.Cantidad > 0 && item.Id != itemABuscar.Id && item.Practica.Id == itemABuscar.Practica.Id)
                    {
                        if ((esDeModulo && item.PracticaTurno.PracticaAdicional == null) || (!esDeModulo && item.PracticaTurno.PracticaAdicional != null))
                            throw new Exception("No puede combinar un módulo con una práctica que no es adicional del mismo.");
                    }
                }
            }
        }

        [RequiresTransaction]
        [Private]
        public virtual EntityCollection<Entities.Valorizacion> ValorizacionActualizar(EntityCollection<FullValorizacion> valorizaciones, String logRevalorizacion)
        {
            EntityCollection<Entities.Valorizacion> ret = new EntityCollection<enfoke.Eges.Entities.Valorizacion>();

            foreach (FullValorizacion fullValorizacion in valorizaciones)
            {
                Entities.Valorizacion valorizacion = valorizacion = fullValorizacion.ValorizacionInfo.Valorizacion;
                EntityCollection<ValorizacionItem> items = fullValorizacion.ValorizacionInfo.Items;

                if (Context.Session.TurnosDalc.TurnoExisteEnComprobanteNoAnulado(fullValorizacion.ValorizacionInfo.Valorizacion.Turno.Id) &&
                    ((TipoControlFacturacionEnum)fullValorizacion.ValorizacionInfo.Valorizacion.Turno.TipoControlFacturacion.Id == TipoControlFacturacionEnum.PreFacturado || (TipoControlFacturacionEnum)fullValorizacion.ValorizacionInfo.Valorizacion.Turno.TipoControlFacturacion.Id == TipoControlFacturacionEnum.Facturado))
                    throw new NotLoggeableException("No es posible revalorizar ya que existen turnos incluidos en comprobantes.");

                TurnosDalc turnosDalc = Context.Session.TurnosDalc;
                foreach (ValorizacionItem item in items)
                    if (item.PracticaTurno.Id == 0) //Si no tiene el ID de PracticaTurno
                    {
                        //Primero lo trato de buscar por si ya está persistido en la base
                        PracticaTurno pt = turnosDalc.PracticaTurnoReadByTurnoAndPractica(item.PracticaTurno.TurnoId, item.PracticaTurno.Practica.Id, item.PracticaTurno.PracticaAdicional);

                        //Si no lo encuentro -> lo persisto.
                        if (pt == null)
                        {
                            item.PracticaTurno = dalEngine.Update(item.PracticaTurno);
                            salvarDocumentacionNuevaPracticaTurno(item);
                        }
                        else
                            item.PracticaTurno = pt;
                    }

                //Busco la valorizacion con los datos anteriores
                enfoke.Context.Data.Session.Evict(valorizacion);

                if (items != null && items.Count > 0)
                    foreach (ValorizacionItem item in items)
                        enfoke.Context.Data.Session.Evict(item);

                Entities.Valorizacion valorizacionActual = ValorizacionReadByTurnoAndTipoWithItems(valorizacion.Turno.Id, valorizacion.Tipo.Id);

                String logModificacion = String.Empty;
                if (this.ValorizacionModificada(valorizacion, items, valorizacionActual, ref logModificacion))
                {
                    // Elimino las valorizaciones de los insumos de los item de la valorización actual
                    foreach (ValorizacionItem item in valorizacionActual.Items)
                        if (item.ValorizacionItemInsumo != null)
                            this.ValorizacionItemInsumoDeleteMany(item.ValorizacionItemInsumo);

                    valorizacion = ValorizacionUpdate(valorizacion, items, logModificacion, logRevalorizacion);
                    //Regenero la valorización de pre-facturación
                    if (valorizacion.Tipo.Id == (int)ValorizacionTiposEnum.Admision)
                        ValorizacionGenerarPreFacturacion(valorizacion.Turno, true);
                }

                ret.Add(valorizacion);
            }

            return ret;
        }

        [Private]
        public virtual Entities.Valorizacion ValorizacionCopiar(int turnoIdOrigen, int turnoIdDestino, ValorizacionTiposEnum tipo)
        {
            Turno turno = Context.Session.TurnosDalc.TurnoReadById(turnoIdOrigen);
            Turno turnoDestino = Context.Session.TurnosDalc.TurnoReadById(turnoIdDestino);

            // Borro la valorizacion vieja del turno
            if (turnoIdOrigen != turnoIdDestino)
            {
                Entities.Valorizacion val = ValorizacionReadByTurnoAndTipo(turnoDestino.Id, (int)ValorizacionTiposEnum.Presupuesto);
                ValorizacionDelete(val);
            }

            return ValorizacionCopiar(turno, tipo, turnoDestino, tipo);
        }

        internal virtual Entities.Valorizacion ValorizacionCopiar(int turnoId, ValorizacionTiposEnum origen, ValorizacionTiposEnum destino)
        {
            Turno turno = Context.Session.TurnosDalc.TurnoReadById(turnoId);
            return ValorizacionCopiar(turno, origen, destino);
        }

        private void salvarDocumentacionNuevaPracticaTurno(ValorizacionItem vi)
        {
            if (!vi.PlanPracticaUsadoId.HasValue) return;

            EntityCollection<PlanPracticaDocumentacion> entPPD = new EntityCollection<PlanPracticaDocumentacion>();

            PlanPracticaPrecio usado = Context.Session.Dalc.GetById<PlanPracticaPrecio>(vi.PlanPracticaUsadoId.Value);
            entPPD = Context.Session.RequisitosDalc.PlanPracticaDocumentacionReadByPlanPractica(true, usado.Plan, usado.Practica);

            if (entPPD == null) return;

            EntityCollection<TurnoDocumentacion> entityTD = new EntityCollection<TurnoDocumentacion>();

            foreach (PlanPracticaDocumentacion ppd in entPPD)
            {
                TurnoDocumentacion td = new TurnoDocumentacion();
                td.Observaciones = ppd.Observaciones;
                td.TurnoId = vi.PracticaTurno.TurnoId;
                td.PlanPracticaDocumentacion = ppd;
                td.PracticaTurno = vi.PracticaTurno;
                entityTD.Add(td);
            }

            Context.Session.TurnosDalc.TurnoDocumentacionUpdateMany(entityTD);
        }

        internal virtual Entities.Valorizacion ValorizacionCopiar(Turno turno, ValorizacionTiposEnum origen, ValorizacionTiposEnum destino)
        {
            return ValorizacionCopiar(turno, origen, null, destino);
        }


        /// <summary>
        /// Genero una nueva valorización en base a otra
        /// </summary>
        /// <param name="turno">Turno al cual deseo Generarle una nueva Valorización</param>
        /// <param name="origen">Tipo de Valorización Origen</param>
        /// <param name="destino">Tipo de Valorización Destino</param>
        [RequiresTransaction]
        protected internal virtual Entities.Valorizacion ValorizacionCopiar(Turno turno, ValorizacionTiposEnum origen, Turno turnoDestino, ValorizacionTiposEnum destino)
        {
            // Veo si tengo que copiar de un turno al otro la valorizacion => que sean turnos validos entre si 
            // (se usa para copiar de un presupuesto al turno al que se vincula) .- al menos por ahora..
            if (turnoDestino == null)
                turnoDestino = turno;
            else
            {
                if (!Context.Session.TurnosDalc.TurnosTienenMismaComposicionPracticas(turno.Id, turnoDestino.Id, true))
                    throw new Exception("La valorización no se puede copiar ya que la composición de los turnos no es la misma.");
            }

            // Obtengo la Valorización de Origen
            Entities.Valorizacion valorizacion = ValorizacionReadByTurnoAndTipoWithItems(turno.Id, (int)origen);

            // Si no tiene generada la valorización de Admisión (aun no fue recepcionado)
            // no voy a generar la del tipo 3.
            if (valorizacion == null)
                return null;

            // [PDG] Clona la instancia porque va a precisar modificarla
            valorizacion = (Entities.Valorizacion)Serializator.Clone(valorizacion);
            // Obtengo los Items de la Valorización
            EntityCollection<ValorizacionItem> items = valorizacion.Items;

            // Seteo Seteo el Tipo de Destino y pongo el ID en 0 (nueva)
            valorizacion.Tipo = dalEngine.GetById<ValorizacionTipo>((int)destino);
            valorizacion.Id = 0;
            valorizacion.Turno = turnoDestino;

            // Si se está regenerando la valorización de prefacturación, no copio los valores de modificado
            if (destino == ValorizacionTiposEnum.Prefacturacion)
                valorizacion.Modificado = null;

            // Seteo los Valores para que sean Nuevos Items
            foreach (ValorizacionItem item in items)
            {
                ValorizacionItemCobInsumo vICI = this.ValorizacionItemCobInsumoReadByValorizacionItemId(item.Id);
                item.Id = 0;
                item.Valorizacion = valorizacion;
                item.ValorizacionItemCobInsumo = vICI;

                // Si se está regenerando la valorización de prefacturación, no copio los valores de modificado
                if (destino == ValorizacionTiposEnum.Prefacturacion)
                    item.Modificado = null;
                else
                    item.Modificado = item.Modificado;
            }

            // Creo una ValorizaciónInfo
            ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, items);

            // Si no esta, Obtengo el ObraSocialPlan
            if (turno.Orden.ObraSocialPlan == null)
            {
                ObrasSocialesDalc ObrasSocialesDalc = Context.Session.ObrasSocialesDalc;
                turno.Orden.ObraSocialPlan = ObrasSocialesDalc.ObraSocialPlanReadById(turno.Orden.ObraSocialPlanId);
            }

            // Inserto la Valorización. Si origen y destino son iguales, es que quiero duplicar a proposito => no valido
            return InsertValorizacion(valorizacionInfo, turnoDestino, turno.Orden.ObraSocialPlan, origen != destino);
        }

        /// <summary>
        /// Renovar Valorizacion de PreFacturacion (delete a la vigente y copia una igual)
        /// </summary>
        [RequiresTransaction]
        public virtual Entities.Valorizacion ValorizacionHacerCopia(int turnoId, ValorizacionTiposEnum tipoValorizacion)
        {
            Entities.Valorizacion valNueva = null;

            // Obtengo la Valorización de PreFacturación
            Entities.Valorizacion valorizacionPreFacturacion = ValorizacionReadByTurnoAndTipo(turnoId, (int)tipoValorizacion);
            if (valorizacionPreFacturacion != null)
            {
                TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

                // Copio de la de prefacturacion
                valNueva = ValorizacionCopiar(turnoId, tipoValorizacion, tipoValorizacion);

                // Elimino la Valorización de PreFacturación Vieja
                ValorizacionDelete(valorizacionPreFacturacion, false);
            }

            return valNueva;
        }

        /// <summary>
        /// Regenero la Valorización de PreFacturación en Base a la de Admisión
        /// </summary>
        /// <param name="turno">Turno sobre el cual Trabajar</param>
        /// <param name="user">Usuario de la Operación</param>
        [RequiresTransaction]
        public virtual void ValorizacionRegenerarPreFacturacion(Turno turno)
        {
            // Obtengo la Valorización de PreFacturación
            Entities.Valorizacion valorizacionPreFacturacion = ValorizacionReadByTurnoAndTipo(turno.Id, (int)ValorizacionTiposEnum.Prefacturacion);
            if (valorizacionPreFacturacion != null)
            {
                TurnosDalc TurnosDalc = Context.Session.TurnosDalc;
                // Elimino las PTs que se pudieron haber agregado y no estaban en admisión
                TurnosDalc.PracticaTurnoDeleteExtras(turno.Id, valorizacionPreFacturacion.Id);
                // Elimino la Valorización de PreFacturación Vieja
                ValorizacionDelete(valorizacionPreFacturacion);
                // Genero una Nueva Valorización de PreFacturación en base a la de Admisión
                ValorizacionCopiar(turno, ValorizacionTiposEnum.Admision, ValorizacionTiposEnum.Prefacturacion);
            }
            else
            {
                ValorizacionCopiar(turno, ValorizacionTiposEnum.Admision, ValorizacionTiposEnum.Prefacturacion);
            }
        }

        /// <summary>
        /// [CB] Genero o re-genero la PreFacturacion (dependiendo de donde la llame).
        /// </summary>
        /// <param name="turno">Turno relacionado con la Valorizacion</param>
        /// <param name="user">User en el Client que realiza la accion</param>
        /// <param name="regenerarPreFacturacion">Determina si hay que re-generar la PreFacturacion o no, 
        /// dependiendo si existia o no anteriormente. Es decir, desde el wizard de admision aun no existe 
        /// la PreFacturacion con lo cual el valor seria "false", mientras que desde Admision (Botón derecho del mouse) 
        /// ya existe, con lo cual el valor seria "true".</param>
        /// <returns>Cantidad de Items</returns>
        [RequiresTransaction]
        public virtual void ValorizacionGenerarPreFacturacion(Turno turno, bool regenerarPreFacturacion)
        {
            // Obtengo la Valorización de PreFacturación
            // Entities.Valorizacion valorizacionPreFacturacion = ValorizacionReadByTurnoAndTipo(turno.Id, (int)ValorizacionTiposEnum.Prefacturacion);
            if (regenerarPreFacturacion)
                ValorizacionRegenerarPreFacturacion(turno);
            else
                ValorizacionCopiar(turno, ValorizacionTiposEnum.Admision, ValorizacionTiposEnum.Prefacturacion);
        }

        /// <summary>
        /// Obtengo la Cantidad de Items que fueron Liquidados por Honorarios Médicos
        /// </summary>
        /// <param name="id">ID de la Valorizacion</param>
        /// <returns>Cantidad de Items</returns>
        public int ValorizacionObtenerItemsLiquidadosDeHonorariosMedicos(int id)
        {
            string sql = "SELECT ISNULL(COUNT(1), 0) FROM valorizacion_item WHERE vli_vlr_id = " + id.ToString() + " AND vli_liq_id IS NOT NULL";
            return (int)dalEngine.Connection.ExecuteScalar(sql);
        }
        #endregion

        #region ValorizacionItem

        public ValorizacionItem ValorizacionItemReadByTipoValorizacionAndPracticaTurno(ValorizacionTiposEnum tipoVal, int practicaTurnoId)
        {
            ReadManyCommand<ValorizacionItem> readCmd = new ReadManyCommand<ValorizacionItem>(dalEngine);

            Filter filter = new Filter();

            filter.Add(ValorizacionItem.Properties.Valorizacion.Tipo, "=", (int)tipoVal);
            filter.Add(BooleanOp.And, ValorizacionItem.Properties.PracticaTurno.Id, "=", practicaTurnoId);
            filter.Add(BooleanOp.And, ValorizacionItem.Properties.Valorizacion.Deleted, "=", false);

            readCmd.Filter = filter;

            EntityCollection<ValorizacionItem> col = readCmd.Execute();
            if (col.Count <= 0)
                return null;
            else
                return col[0];
        }

        [RequiresTransaction]
        protected virtual bool ValorizacionItemDelete(EntityCollection<ValorizacionItem> items)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            if (items != null && items.Count > 0)
            {
                foreach (ValorizacionItem item in items)
                    Audit.AuditDelete(item, user.Id);


                dalEngine.UpdateCollection<ValorizacionItem>(items);

                return true;
            }
            else
                return false;
        }

        [RequiresTransaction]
        protected virtual bool ValorizacionItemUndoDelete(EntityCollection<ValorizacionItem> items)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            if (items != null && items.Count > 0)
            {
                foreach (ValorizacionItem item in items)
                {
                    item.DeleteDate = null;
                    item.DeleteUser = null;
                }

                dalEngine.UpdateCollection<ValorizacionItem>(items);

                return true;
            }
            else
                return false;
        }

        /// <summary>
        /// Retorno si un Item de una Valorizacion se Encuentra en un Comprobante Posterior al Dado
        /// </summary>
        /// <param name="itemValorizacion">Código del Item de la Valorización</param>
        /// <param name="comprobante">Código del Comprobante</param>
        /// <param name="fecha">Fecha del Comprobante</param>
        /// <returns>Si existe un Comprobante Posterior con el Item en Cuestión</returns>
        [Private]
        public bool ValorizacionItemEnComprobantePosterior(int itemValorizacion, int comprobante, DateTime fecha)
        {
            /**
             * Cantidad de Items que:
             * - El Item de la Valorizacion sea el Dado
             * - El Comprobante No sea el Dado
             * - La Fecha del Comprobante sea Posterior a la Dada
             * */
            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT COUNT(1) ");
            sql.Append("FROM comprobante ");
            sql.Append("INNER JOIN comprobante_item ON coi_comprobante_id = com_id ");
            sql.Append("WHERE coi_valorizacion_item_id = ").Append(itemValorizacion).Append(" ");
            sql.Append("AND com_id != ").Append(comprobante).Append(" ");
            sql.Append("AND com_fecha > " + SQLPortable.ToDbDate(fecha, true));

            int cantidad = int.Parse(dalEngine.Connection.ExecuteScalar(sql.ToString()).ToString());

            // Retorno TRUE si Esta en otro
            return cantidad > 0;
        }

        public EntityCollection<Entities.ValorizacionItem> ValorizacionItemsReadByListaValorizacionId(List<int> valorizacionIds)
        {
            ReadManyCommand<Entities.ValorizacionItem> readCmd = new ReadManyCommand<Entities.ValorizacionItem>(dalEngine);
            Filter filter = new Filter();
            filter.Add(Entities.ValorizacionItem.Properties.Valorizacion,
                "in", valorizacionIds);
            filter.Add(BooleanOp.And, Entities.ValorizacionItem.Properties.DeleteDate, "is", null);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<ValorizacionItem> ValorizacionItemsReadByListaValorizacionIds(List<int> valIds)
        {
            EntityCollection<ValorizacionItem> valorizacionesItem = new EntityCollection<ValorizacionItem>();
            valorizacionesItem = ValorizacionItemsReadByListaValorizacionId(valIds);
            if (valorizacionesItem != null)
                foreach (ValorizacionItem item in valorizacionesItem)
                    if (item.PlanPracticaUsadoId.HasValue)
                    {
                        if (item.Practica.EsDetallado == true && item.Practica.TipoPractica.Id == (int)TipoPracticaEnum.SetFarmacia)
                        {
                            item.ValorizacionItemInsumo = this.ValorizacionItemInsumoReadByValorizacionItemId(item.Id);
                            item.ValorizacionItemCobInsumo = this.ValorizacionItemCobInsumoReadByValorizacionItemId(item.Id);
                        }
                    }

            return valorizacionesItem;
        }

        /// <summary>
        /// Obtengo los Items de una Valorizacion
        /// </summary>
        /// <param name="v">Valorizacion a Obtener sus Items</param>
        /// <returns>Colección de Items de Valorizacion</returns>
        public EntityCollection<ValorizacionItem> ValorizacionItemsReadByValorizacion(int valID)
        {
            EntityCollection<ValorizacionItem> valorizacionesItem = new EntityCollection<ValorizacionItem>();
            valorizacionesItem = dalEngine.GetManyByProperty<ValorizacionItem>(ValorizacionItem.Properties.Valorizacion.Id, valID);
            if (valorizacionesItem != null)
                foreach (ValorizacionItem item in valorizacionesItem)
                    if (item.PlanPracticaUsadoId.HasValue)
                    {
                        if (item.Practica.EsDetallado == true && item.Practica.TipoPractica.Id == (int)TipoPracticaEnum.SetFarmacia)
                        {
                            item.ValorizacionItemInsumo = this.ValorizacionItemInsumoReadByValorizacionItemId(item.Id);
                            item.ValorizacionItemCobInsumo = this.ValorizacionItemCobInsumoReadByValorizacionItemId(item.Id);
                        }
                    }

            return valorizacionesItem;
        }





















        public ValorizacionItemUpdateLiquidado ValorizacionItemUpdateLiquidadoReadByComprobanteItemId(int comprobanteItemId)
        {
            string hql = "select viul from ValorizacionItemUpdateLiquidado viul, ComprobanteItem coi where viul.Id = coi.ValorizacionItemID AND coi.Id = :comprobanteItemId ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("comprobanteItemId", comprobanteItemId);

            return dalEngine.GetByQuery<ValorizacionItemUpdateLiquidado>(query);
        }
        [RequiresTransaction]
        [Private]
        public virtual ValorizacionItemUpdateLiquidado ValorizacionItemUpdate(ValorizacionItemUpdateLiquidado item)
        {
            item = dalEngine.Update<ValorizacionItemUpdateLiquidado>(item);

            return item;
        }

        public virtual EntityCollection<ValorizacionItemUpdateLiquidado> ValorizacionItemsUpdate(EntityCollection<ValorizacionItemUpdateLiquidado> items)
        {
            return dalEngine.UpdateCollection(items);
        }

        [RequiresTransaction]
        public virtual void ValorizacionItemCopyFromPreFacturacion(EntityCollection<ValorizacionItem> items, int turnoID, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            Entities.Valorizacion valAdmision = this.ValorizacionReadByTurnoAndTipoWithItems(turnoID, (int)ValorizacionTiposEnum.Admision);
            EntityCollection<ValorizacionItem> NewItems = new EntityCollection<ValorizacionItem>();
            foreach (ValorizacionItem item in items)
            {
                ValorizacionItem itemAdmision = this.ValorizacionItemContienePractica(valAdmision.Items, item.Practica.Id);
                if (itemAdmision == null)
                {
                    itemAdmision = new ValorizacionItem();
                    itemAdmision.Practica = item.Practica;
                    itemAdmision.PracticaTurno = item.PracticaTurno;
                    itemAdmision.Valorizacion = valAdmision;
                }

                itemAdmision.Medico = item.Medico;
                itemAdmision.MedicoID = item.MedicoID;

                itemAdmision.Cantidad = item.Cantidad;
                itemAdmision.PorcentajeDerechos = item.PorcentajeDerechos;
                itemAdmision.PorcentajeHonorarios = item.PorcentajeHonorarios;
                itemAdmision.PorcentajeModulo = item.PorcentajeModulo;
                itemAdmision.PorcentajeInsumos = item.PorcentajeInsumos;
                itemAdmision.ImporteDerechosDif = item.ImporteDerechosDif;
                itemAdmision.ImporteDerechosExt = item.ImporteDerechosExt;
                itemAdmision.ImporteHonorariosDif = item.ImporteHonorariosDif;
                itemAdmision.ImporteHonorariosExt = item.ImporteHonorariosExt;
                itemAdmision.ImporteModuloDif = item.ImporteModuloDif;
                itemAdmision.ImporteModuloExt = item.ImporteModuloExt;
                itemAdmision.ImporteInsumosDif = item.ImporteInsumosDif;
                itemAdmision.ImporteInsumosExt = item.ImporteInsumosExt;
                itemAdmision.ImporteHonorarioInterno = item.ImporteHonorarioInterno;
                itemAdmision.TipoCobertura = item.TipoCobertura;
                itemAdmision.CodigoFicticio = item.CodigoFicticio;
                itemAdmision.ImporteModulo = item.ImporteModulo;
                itemAdmision.Modulado = item.Modulado;

                NewItems.Add(itemAdmision);
            }

            this.ValorizacionActualizar(valAdmision, NewItems, String.Empty, modalidadCoseguro);

        }

        private ValorizacionItem ValorizacionItemContienePractica(EntityCollection<ValorizacionItem> items, int practicaId)
        {
            foreach (ValorizacionItem item in items)
            {
                if (item.Practica.Id == practicaId)
                    return item;
            }

            return null;
        }

        internal EntityCollection<ValorizacionItem> ValorizacionItemReadByPracticaTurno(int ptID)
        {
            ReadManyCommand<ValorizacionItem> readCmd = new ReadManyCommand<ValorizacionItem>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, ValorizacionItem.Properties.PracticaTurno.Id,
                "=", ptID);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public List<int> ValorizacionItemsQueAplicanDescuento(DateTime hasta)
        {
            string hql = " select v.Id " +
                         " from Valorizacion v " +
                         " where v.Tipo.Id = :valorizacionTipo " +
                         " AND v.Deleted = false " +
                         " AND v.CreateDate < :fecha ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("fecha", hasta.AddDays(1));
            query.SetParameter("valorizacionTipo", (int)ValorizacionTiposEnum.Prefacturacion);

            IList<int> valorizacionIds = query.List<int>();

            List<int> result = new List<int>();
            List<int> aux = new List<int>();

            for (int i = 0; i < valorizacionIds.Count; i++)
            {
                aux.Add(valorizacionIds[i]);
                if (aux.Count == 1000)
                {
                    result.AddRange(ValorizacionItemsQueAplicanDescuentoLimitado(aux));
                    aux = new List<int>();
                }
            }

            if (aux.Count > 0)
                result.AddRange(ValorizacionItemsQueAplicanDescuentoLimitado(aux));

            return result;
        }

        private IList<int> ValorizacionItemsQueAplicanDescuentoLimitado(List<int> valorizacionIds)
        {
            string hql = " select vi.Id " +
                        " from ValorizacionItem vi JOIN vi.Valorizacion v2 " +
                        " where v2.Id in ( " +
                        " select v.Id " +
                        " from Valorizacion v " +
                        " where v.Id in (:valorizacionIds) " +
                        " group by v.Id " +
                        " ) " +
                        " AND vi.LiquidacionHonorariosID IS NULL " +
                        " AND ( vi.Cantidad * " +
                        " ( ( vi.PorcentajeDerechos / 100) * vi.ImporteDerechosExt " +
                        " + ( vi.PorcentajeHonorarios / 100)* vi.ImporteHonorariosExt " +
                        " + ( vi.PorcentajeInsumos / 100)* vi.ImporteInsumosExt " +
                        " + ( vi.PorcentajeModulo / 100) * vi.ImporteModuloExt ) ) > 0 ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("valorizacionIds", valorizacionIds);

            return (List<int>)query.List<int>();
        }

        #endregion

        #region ValorizacionItemCobInsumo

        /// <summary>
        /// Obtengo la valorizacion de los insumos de una práctica valorizada
        /// </summary>
        /// <param name="id">ID del item de valorizacion</param>
        /// <returns>Valorización de los insumos</returns>
        public ValorizacionItemCobInsumo ValorizacionItemCobInsumoReadByValorizacionItemId(int valorizacionItemId)
        {
            string hql = "from ValorizacionItemCobInsumo vici "
                     + " where vici.ValorizacionItem.Id = :valorizacionItemId "
                     + " order by vici.Id desc ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("valorizacionItemId", valorizacionItemId);

            EntityCollection<ValorizacionItemCobInsumo> item = dalEngine.GetManyByQuery<ValorizacionItemCobInsumo>(query);
            // Arreglo temporal esto asi esta mal
            if (item != null && item.Count > 0)
                return item[0];
            else
                return null;
        }

        public EntityCollection<ValorizacionItemCobInsumo> ValorizacionItemCobInsumoReadByValorizacionItemIds(List<int> valorizacionItemIds)
        {
            string hql = "from ValorizacionItemCobInsumo vici "
                     + " where vici.ValorizacionItem.Id in (:valorizacionItemIds) "
                     + " order by vici.Id desc ";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("valorizacionItemIds", valorizacionItemIds);
            return dalEngine.GetManyByQuery<ValorizacionItemCobInsumo>(query);
        }

        public EntityCollection<ValorizacionItemCobInsumo> ValorizacionItemCobInsumoReadManyByComprobanteItems(EntityCollection<DetalleItemComprobante> items)
        {
            string ids = "";
            foreach (DetalleItemComprobante item in items)
            {
                ids += item.ValorizacionItemId + ",";
            }
            ids = ids.Remove(ids.Length - 1);

            StringBuilder hql = new StringBuilder(" from ValorizacionItemCobInsumo ins ");
            hql.Append(" where ins.ValorizacionItem.Id IN(" + ids + ")");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<ValorizacionItemCobInsumo>(query);
        }






        #endregion

        #region ValorizacionMasivaDetalle

        public void ValorizacionMasivaDetalleUpdateMany(EntityCollection<ValorizacionMasivaDetalle> valorizacionesMasivaDetalle)
        {
            dalEngine.UpdateCollection<ValorizacionMasivaDetalle>(valorizacionesMasivaDetalle);
        }

        #endregion

        #region ValorizacionMasiva

        public ValorizacionMasiva ValorizacionMasivaUpdate(ValorizacionMasiva valorizacionMasiva)
        {
            valorizacionMasiva.SecurityUserId = Security.Current.UserInfo.User.Id;
            valorizacionMasiva.Fecha = enfoke.Time.Now;
            return dalEngine.Update(valorizacionMasiva);
        }

        #endregion

        #region ValorizacionItemInsumo

        /// <summary>
        /// Obtiene todas las valorizacion de los insumos para un item de la valorización
        /// </summary>
        /// <param name="valorizacionItemId">Id de la valorizacion item</param>
        /// <returns>Colección de ValorizacionItemInsumo</returns>
        public EntityCollection<ValorizacionItemInsumo> ValorizacionItemInsumoReadByValorizacionItemIdPlanPracticaId(int valorizacionItemId, int planPracticaId)
        {
            Filter filter = new Filter();
            filter.Add(Entities.ValorizacionItemInsumo.Properties.ValorizacionItemId, "=", valorizacionItemId);
            filter.Add(BooleanOp.And, Entities.ValorizacionItemInsumo.Properties.PlanPracticaInsumo.PlanPracticaPrecio.Id, "=", planPracticaId);
            filter.Add(BooleanOp.And, Entities.ValorizacionItemInsumo.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<ValorizacionItemInsumo>(filter);
        }

        public EntityCollection<ValorizacionItemInsumo> ValorizacionItemInsumoReadByValorizacionItemId(int valorizacionItemId)
        {
            Filter filter = new Filter();
            filter.Add(Entities.ValorizacionItemInsumo.Properties.ValorizacionItemId, "=", valorizacionItemId);
            filter.Add(BooleanOp.And, Entities.ValorizacionItemInsumo.Properties.Deleted, "=", false);
            return dalEngine.GetManyByFilter<ValorizacionItemInsumo>(filter);
        }

        public EntityCollection<ValorizacionItemInsumo> ValorizacionItemInsumoReadByValorizacionItemId(List<int> valorizacionItemIds)
        {
            Filter filter = new Filter();
            filter.Add(Entities.ValorizacionItemInsumo.Properties.ValorizacionItemId, "IN", valorizacionItemIds);
            return dalEngine.GetManyByFilter<ValorizacionItemInsumo>(filter);
        }

        public void ValorizacionItemInsumoUpadteMany(EntityCollection<ValorizacionItemInsumo> valorizacionesItemInsumo)
        {
            if (valorizacionesItemInsumo != null)
            {
                //foreach (ValorizacionItemInsumo item in valorizacionesItemInsumo)
                //    if (item.TipoInsumoCategoria == null)
                //        throw new Exception("test");

                dalEngine.UpdateCollection(valorizacionesItemInsumo);
            }
        }

        public void ValorizacionItemInsumoDeleteMany(EntityCollection<ValorizacionItemInsumo> valorizacionesItemInsumo)
        {
            if (valorizacionesItemInsumo != null)
                foreach (ValorizacionItemInsumo item in valorizacionesItemInsumo)
                    item.Deleted = true;

            this.ValorizacionItemInsumoUpadteMany(valorizacionesItemInsumo);
        }

        #endregion

        #region FullValorizacionMasiva


 
        public EntityCollection<DatosValorizacionMasiva> DatosValorizacionMasivaReadByTurnoIdsAndTipo(List<int> turnoIds , int tipoValorizacion)
        {
            EntityCollection<DatosValorizacionMasiva> valorizacion = new EntityCollection<DatosValorizacionMasiva>();
            valorizacion.AddRange(DatosValorizacionMasivaSinInsumoReadByTurnoIdsAndTipo(turnoIds,  tipoValorizacion));
            valorizacion.AddRange(DatosValorizacionMasivaConInsumoReadByTurnoIdsAndTipo(turnoIds,  tipoValorizacion));
            return valorizacion;
        }

        [Private]
        public EntityCollection<DatosValorizacionMasiva> DatosValorizacionMasivaSinInsumoReadByTurnoIdsAndTipo(List<int> turnoIds, int tipoValorizacion)
        {
            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(turnoIds);
            string ordenes = blockBuilder.BuildConstrainBlock("vli.Valorizacion.Turno.Id");

            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Results.DatosValorizacionMasiva(vli) ");
            hql.Append(" from ValorizacionItem vli ");
            hql.Append(" where vli.Valorizacion.Tipo.Id = ").Append(tipoValorizacion.ToString()).AppendFormat(" and {0} ", ordenes);
            hql.Append(" and vli.Valorizacion.Deleted = false ");
            hql.Append(" and vli.DeleteDate is null ");
            hql.Append(" and not exists (select 1 from ValorizacionItemInsumo ins where ins.ValorizacionItemId = vli.Id ) ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<DatosValorizacionMasiva>(query);

        }

        [Private]
        public EntityCollection<DatosValorizacionMasiva> DatosValorizacionMasivaConInsumoReadByTurnoIdsAndTipo(List<int> turnoIds, int tipoValorizacion)
        {
            SQLBlockBuilder<int> blockBuilder = new SQLBlockBuilder<int>(turnoIds);
            string ordenes = blockBuilder.BuildConstrainBlock("vici.ValorizacionItem.Valorizacion.Turno.Id");

            StringBuilder hql = new StringBuilder();
            hql.Append(" Select new enfoke.Eges.Entities.Results.DatosValorizacionMasiva(vii,vici) ");
            hql.Append(" from  ValorizacionItemCobInsumo vici, ValorizacionItemInsumo vii ");
            hql.Append(" where vici.ValorizacionItem.Id = vii.ValorizacionItemId ");
            hql.Append(" and  vici.ValorizacionItem.Valorizacion.Tipo.Id = ").Append(tipoValorizacion.ToString()).AppendFormat(" and {0} ", ordenes);
            hql.Append(" and vici.ValorizacionItem.Valorizacion.Deleted = false ");
            hql.Append(" and vici.ValorizacionItem.DeleteDate is null ");

            IQuery query = dalEngine.CreateQuery(hql.ToString());
            return dalEngine.GetManyByQuery<DatosValorizacionMasiva>(query);
        }

        #endregion

        [Private]
        public int ObtenerTotalOSValorizacionPresupuesto(Turno turno, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            // Si es un recitado, utilizo la valorizacion del original
            int turnoUtilizar = turno.TipoTurnoId != (int)TipoTurnoEnum.Recitado ? turno.Id : turno.TurnoOriginalID.Value;

            Entities.Valorizacion valorizacion = ValorizacionReadByTurnoAndTipoWithItems(turnoUtilizar, (int)ValorizacionTiposEnum.Presupuesto);
            EntityCollection<ValorizacionItem> items = valorizacion.Items;
            if (valorizacion == null) return 0;
            ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, items);
            FullValorizacion fullValorizacion = new FullValorizacion(valorizacionInfo, modalidadCoseguro);

            return (int)Decimal.Round(fullValorizacion.ValorizacionInfo.ImporteTotalOS, MidpointRounding.AwayFromZero);
        }

        [Private]
        public FullValorizacion FullValorizacionRead(DatosTurnoValorizacionMasiva turno, ValorizacionTiposEnum tipoValorizacion, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {
            return FullValorizacionRead(turno.TurnoId, turno.TurnoOriginalId, tipoValorizacion, turno.TipoTurnoId, modalidadCoseguro);
        }

        [Private]
        public FullValorizacion FullValorizacionRead(int idTurno, int? idTurnoOriginal, ValorizacionTiposEnum tipoValorizacion, int tipoTurnoId, ValorizacionItemModalidadCoseguroEnum modalidadCoseguro)
        {

            // Si es un recitado, utilizo la valorizacion del original
            int turnoUtilizar = tipoTurnoId != (int)TipoTurnoEnum.Recitado ? idTurno : idTurnoOriginal.Value;

            Entities.Valorizacion valorizacion = ValorizacionReadByTurnoAndTipoWithItems(turnoUtilizar, (int)tipoValorizacion);
            if (valorizacion != null)
            {
                EntityCollection<ValorizacionItem> items = valorizacion.Items;
                ValorizacionInfo valorizacionInfo = new ValorizacionInfo(valorizacion, items);
                FullValorizacion fullValorizacion = new FullValorizacion(valorizacionInfo, modalidadCoseguro);

                return fullValorizacion;
            }

            return null;
        }

        public ValorizacionInfo GetValorizacionInfoByTurnoId(int turnoId)
        {
            Entities.Valorizacion valorizacion = Context.Session.ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turnoId, (int)ValorizacionTiposEnum.Prefacturacion);
            if (valorizacion == null)
                valorizacion = Context.Session.ValorizacionesDalc.ValorizacionReadByTurnoAndTipoWithItems(turnoId, (int)ValorizacionTiposEnum.Prefacturacion);
            return (valorizacion != null) ? new ValorizacionInfo(valorizacion, valorizacion.Items) : null;
        }
    }
}

