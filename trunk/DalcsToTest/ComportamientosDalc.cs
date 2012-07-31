using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.Configuracion;
using enfoke.Eges.Persistence;

using enfoke.Data.DisconnectedSupport;
using enfoke.Data;
using enfoke.Data.Reference;
using NHibernate;
using enfoke.Data.Filters;
using enfoke.Utils;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Utils;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class ComportamientosDalc : Dalc, IService
    {
        protected ComportamientosDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<ModificacionInicioComportamiento> ModificacionesComportamientoReadAll()
        {
            return dalEngine.GetAll<ModificacionInicioComportamiento>();
        }


        #region Comportamiento
        private void ComportamientoCargarGruposHorarios(Comportamiento comportamiento)
        {
            comportamiento.Grupos = new List<ComportamientoGrupo>(ComportamientoGrupoReadByComportamiento(comportamiento.Id));
            comportamiento.Horarios = new List<ComportamientoHorario>(ComportamientoHorarioReadByComportamiento(comportamiento.Id));

            foreach (ComportamientoGrupo grupo in comportamiento.Grupos)
            {
                grupo.Items = ComportamientoGrupoItemReadByGrupo(grupo.Id);
                grupo.Adicionales = ComportamientoGrupoAdicionalReadByGrupo(grupo.Id);
            }
        }

        public Comportamiento ComportamientoReadByIdWithChildren(int id)
        {
            Comportamiento comportamiento = dalEngine.GetById<Comportamiento>(id);

            if (comportamiento != null)
                ComportamientoCargarGruposHorarios(comportamiento);

            return comportamiento;
        }

        [RequiresTransaction]
        public virtual Comportamiento ComportamientoUpdate(Comportamiento comportamiento)
        {
            ComportamientosHabilitadosCache.IncrementarComportamientoActualizacion();

            // Chequeo si es nuevo
            bool nuevo = comportamiento.Id == 0;

            // Auditoria
            comportamiento.UpdateDate = enfoke.Time.Now;
            comportamiento.UpdateUser = Security.Current.UserInfo.User.Id;


            // Guardo
            comportamiento = dalEngine.Update<Comportamiento>(comportamiento);

            // Si era nuevo, creo los grupos
            if (nuevo)
            {
                int gruposCrear = 0;

                switch (comportamiento.Condicion.Id)
                {
                    case (int)ComportamientoCondicion.CasosParticularesAgendaEnum.GrupoASolo:
                    case (int)ComportamientoCondicion.ModificacionDuracionTurnoEnum.GrupoASolo:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.GrupoAComienzo:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.GrupoAFinal:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.PracticaRestriccion:
                    case (int)ComportamientoCondicion.EntregaInformesEnum.GrupoASolo:
                        gruposCrear = 1;
                        break;
                    case (int)ComportamientoCondicion.CasosParticularesAgendaEnum.GrupoACombinadoGrupoB:
                    case (int)ComportamientoCondicion.ModificacionDuracionTurnoEnum.GrupoACombinadoGrupoB:
                    case (int)ComportamientoCondicion.ModificacionDuracionTurnoEnum.GrupoAConOtrosGrupos:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.GrupoAAntesGrupoB:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.GrupoADespuesGrupoB:
                    case (int)ComportamientoCondicion.RestriccionReservaEnum.GruposNoMismoDia:
                    case (int)ComportamientoCondicion.EntregaInformesEnum.GrupoACombinadoGrupoB:
                        gruposCrear = 2;
                        break;
                }

                if (gruposCrear > 0)
                {
                    EntityCollection<ComportamientoGrupo> grupos = new EntityCollection<ComportamientoGrupo>();
                    ComportamientoGrupoTipoItem tipoItemsPractica = dalEngine.GetById<ComportamientoGrupoTipoItem>((int)ComportamientoGrupoTipoItemsEnum.Practica);

                    for (int numero = 1; numero <= gruposCrear; numero++)
                        grupos.Add(new ComportamientoGrupo(comportamiento.Id, numero, "Grupo " + (numero == 1 ? "A" : "B"), tipoItemsPractica));


                    dalEngine.UpdateCollection<ComportamientoGrupo>(grupos);
                }

                comportamiento = ComportamientoReadByIdWithChildren(comportamiento.Id);
            }

            return comportamiento;
        }

        [RequiresTransaction]
        public virtual void ComportamientoDelete(int comportamientoID)
        {
            ComportamientosHabilitadosCache.IncrementarComportamientoActualizacion();

            // Obtengo el Comportamiento con sus Grupos
            Comportamiento comportamiento = ComportamientoReadByIdWithChildren(comportamientoID);

            // Elimino los Items y los Adicionales de cada Grupo
            foreach (ComportamientoGrupo grupo in comportamiento.Grupos)

            {
                dalEngine.Delete(grupo.Items);


                dalEngine.Delete(grupo.Adicionales);
            }

            // Elimino los Grupos del Comportamiento
            dalEngine.Delete(new EntityCollection<ComportamientoGrupo>(comportamiento.Grupos));

            // Elimino los Horarios del Comportamiento
            dalEngine.Delete(new EntityCollection<ComportamientoHorario>(comportamiento.Horarios));


            // Elimino el Comportamiento
            dalEngine.Delete(comportamiento);
        }

        public ReadAllCollection<Comportamiento> ComportamientoReadWithChildrenByTipo(int tipoId)
        {
            return ComportamientoReadByTipo(tipoId, true);
        }

        public ReadAllCollection<Comportamiento> ComportamientoReadByTipo(int tipoId)
        {
            return ComportamientoReadByTipo(tipoId, false);
        }

        private ReadAllCollection<Comportamiento> ComportamientoReadByTipo(int tipoId, bool children)
        {
            EntityCollection<Comportamiento> comportamientos = dalEngine.GetManyByProperty<Comportamiento>(Comportamiento.Properties.Tipo.Id, tipoId,
                new IPropertyReference[] { Comportamiento.Properties.OrdenEjecucion, Comportamiento.Properties.Name });

            if (children)
                foreach (Comportamiento comportamiento in comportamientos)
                    ComportamientoCargarGruposHorarios(comportamiento);

            return new ReadAllCollection<Comportamiento>(comportamientos);
        }

        [Private]
        public bool ComportamientoExistsByCondicion(int condicionID)
        {
            // Busco comportamientos que utilizen la Condicion
            EntityCollection<Comportamiento> comportamientos = dalEngine.GetAll<Comportamiento>();

            foreach (Comportamiento comportamiento in comportamientos)
                if (comportamiento.Condicion.Id == condicionID)
                    return true;

            return false;
        }

        internal EntityCollection<Comportamiento> ComportamientoReadHabilitadosByTipo(int tipoID)
        {
            Filter filter = new Filter();
            filter.Add(Comportamiento.Properties.Tipo.Id, "=", tipoID);
            filter.Add(BooleanOp.And, Comportamiento.Properties.Habilitado, "=", true);

            Sort sort = new Sort { Comportamiento.Properties.OrdenEjecucion, Comportamiento.Properties.Name };

            return dalEngine.GetManyByFilter<Comportamiento>(filter, sort);
        }

        [Private]
        public EntityCollection<Comportamiento> ComportamientosHabilitadosReadByTipoWithContents(ComportamientoTipo.TiposEnum tipoID)
        {
            return ComportamientosHabilitadosCache.ReadByTipo(tipoID);
        }

        [RequiresTransaction]
        public virtual Comportamiento ComportamientoAddGrupo(int comportamientoID, int numero)
        {
            // Creo el nuevo grupo
            ComportamientoGrupoTipoItem tipoItemsPractica = dalEngine.GetById<ComportamientoGrupoTipoItem>((int)ComportamientoGrupoTipoItemsEnum.Practica);
            ComportamientoGrupo grupo = new ComportamientoGrupo(comportamientoID, numero, "Grupo " + numero.ToString(), tipoItemsPractica);


            // Guardo
            grupo = dalEngine.Update<ComportamientoGrupo>(grupo);

            // Retorno el comportamiento con el nuevo grupo
            return this.ComportamientoReadByIdWithChildren(comportamientoID);
        }
        #endregion

        #region ComportamientoCondicion
        [Private]
        public EntityCollection<ComportamientoCondicion> ComportamientoCondicionReadAll()
        {
            return dalEngine.GetAll<ComportamientoCondicion>(ComportamientoCondicion.Properties.Name);
        }

        public EntityCollection<ComportamientoCondicion> ComportamientoCondicionReadByTipo(int tipoID)
        {
            return dalEngine.GetManyByProperty<ComportamientoCondicion>(ComportamientoCondicion.Properties.Tipo.Id, tipoID);
        }






        public ComportamientoCondicion ComportamientoCondicionUpdate(ComportamientoCondicion condicion)
        {
            ComportamientosHabilitadosCache.IncrementarComportamientoActualizacion();
            SecurityUser user = Security.Current.UserInfo.User;
            condicion.UpdateDate = enfoke.Time.Now;
            condicion.UpdateUser = user.Id;


            condicion = dalEngine.Update<ComportamientoCondicion>(condicion);

            return condicion;
        }
        #endregion

        #region ComportamientoRestriccion
        [Private]
        public EntityCollection<ComportamientoRestriccion> ComportamientoRestriccionReadAll()
        {
            return dalEngine.GetAll<ComportamientoRestriccion>(ComportamientoRestriccion.Properties.Name);
        }

        public EntityCollection<ComportamientoRestriccion> ComportamientoRestriccionReadByCondicion(int condicionID)
        {
            return dalEngine.GetManyByProperty<ComportamientoRestriccion>(ComportamientoRestriccion.Properties.Condicion.Id, condicionID);
        }






        public ComportamientoRestriccion ComportamientoRestriccionUpdate(ComportamientoRestriccion restriccion)
        {
            ComportamientosHabilitadosCache.IncrementarComportamientoActualizacion();
            SecurityUser user = Security.Current.UserInfo.User;
            restriccion.UpdateDate = enfoke.Time.Now;
            restriccion.UpdateUser = user.Id;


            restriccion = dalEngine.Update<ComportamientoRestriccion>(restriccion);

            return restriccion;
        }
        #endregion

        #region ComportamientoTipo
        public EntityCollection<ComportamientoTipo> ComportamientoTipoReadAll()
        {
            return dalEngine.GetAll<ComportamientoTipo>(ComportamientoTipo.Properties.Name);
        }





        #endregion

        #region ComportamientoGrupo
        [Private]
        public EntityCollection<ComportamientoGrupo> ComportamientoGrupoReadAllSorted()
        {
            return dalEngine.GetAll<ComportamientoGrupo>(
                new[] { ComportamientoGrupo.Properties.ComportamientoID, 
                    ComportamientoGrupo.Properties.Numero, ComportamientoGrupo.Properties.Name }
                );
        }

        private EntityCollection<ComportamientoGrupo> ComportamientoGrupoReadByComportamiento(int comportamientoID)
        {
            return dalEngine.GetManyByProperty<ComportamientoGrupo>
                        (ComportamientoGrupo.Properties.ComportamientoID, comportamientoID,
                            new[] { ComportamientoGrupo.Properties.Numero, ComportamientoGrupo.Properties.Name });
        }

        [RequiresTransaction]
        public virtual Comportamiento ComportamientoGrupoUpdateWithChilds(Comportamiento comportamiento, ComportamientoGrupo grupo)
        {

            // Actualizo el Grupo
            grupo = dalEngine.Update<ComportamientoGrupo>(grupo);

            // Elimino los Items Existentes
            EntityCollection<ComportamientoGrupoItem> items = ComportamientoGrupoItemReadByGrupo(grupo.Id);
            if (items.Count > 0)

            {
                dalEngine.Delete(items);
            }

            // Inserto los Items
            if (grupo.Items.Count > 0)
            {
                grupo.Items = dalEngine.UpdateCollection<ComportamientoGrupoItem>(grupo.Items);
            }

            // Elimino los Adicionales Existentes
            EntityCollection<CompGrupoAdicional> adicionales = ComportamientoGrupoAdicionalReadByGrupo(grupo.Id);
            if (adicionales.Count > 0)

            {
                dalEngine.Delete(adicionales);
            }

            // Inserto los Adicionales
            if (grupo.Adicionales.Count > 0)
            {
                grupo.Adicionales = dalEngine.UpdateCollection<CompGrupoAdicional>(grupo.Adicionales);
            }

            // Deshabilito el Comportamiento
            if (comportamiento.Habilitado)
            {
                comportamiento.Habilitado = false;

                comportamiento = ComportamientoUpdate(comportamiento);
            }
            else
                comportamiento = ComportamientoReadByIdWithChildren(comportamiento.Id);

            return comportamiento;
        }
        #endregion

        #region ComportamientoGrupoTipoItem
        public EntityCollection<ComportamientoGrupoTipoItem> ComportamientoGrupoTipoItemReadAll()
        {
            return dalEngine.GetAll<ComportamientoGrupoTipoItem>(ComportamientoGrupoTipoItem.Properties.Name);
        }





        #endregion

        #region ComportamientoGrupoItem
        [Private]
        public EntityCollection<ComportamientoGrupoItem> ComportamientoGrupoItemReadAllSorted()
        {
            return dalEngine.GetAll<ComportamientoGrupoItem>(ComportamientoGrupoItem.Properties.ComportamientoGrupoID);
        }

        public EntityCollection<ComportamientoGrupoItem> ComportamientoGrupoItemReadByGrupo(int grupoID)
        {
            return dalEngine.GetManyByProperty<ComportamientoGrupoItem>(ComportamientoGrupoItem.Properties.ComportamientoGrupoID, grupoID);
        }
        #endregion

        #region ComportamientoGrupoAdicional
        [Private]
        public EntityCollection<CompGrupoAdicional> ComportamientoGrupoAdicionalReadAllSorted()
        {
            return dalEngine.GetAll<CompGrupoAdicional>(CompGrupoAdicional.Properties.ComportamientoGrupoID);
        }

        public EntityCollection<CompGrupoAdicional> ComportamientoGrupoAdicionalReadByGrupo(int grupoID)
        {
            return dalEngine.GetManyByProperty<CompGrupoAdicional>(CompGrupoAdicional.Properties.ComportamientoGrupoID, grupoID);
        }
        #endregion

        #region ComportamientoHorario
        [Private]
        public EntityCollection<ComportamientoHorario> ComportamientoHorarioReadAllSorted()
        {
            return dalEngine.GetAll<ComportamientoHorario>(new[] { ComportamientoHorario.Properties.ComportamientoID });
        }

        public EntityCollection<ComportamientoHorario> ComportamientoHorarioReadByComportamiento(int comportamientoID)
        {
            return dalEngine.GetManyByProperty<ComportamientoHorario>(ComportamientoHorario.Properties.ComportamientoID, comportamientoID);
        }

        [RequiresTransaction]
        public virtual ComportamientoHorario ComportamientoHorarioUpdate(Comportamiento comportamiento, ComportamientoHorario horario)
        {
            // Seteo el Comportamiento del Horario
            horario.ComportamientoID = comportamiento.Id;


            // Actualizo el Horario
            horario = dalEngine.Update<ComportamientoHorario>(horario);

            return horario;
        }

        public virtual void ComportamientoHorarioDelete(Comportamiento comportamiento, ComportamientoHorario horario)
        {
            // Elimino el Horario
            dalEngine.Delete(horario);
        }
        #endregion
    }
}

