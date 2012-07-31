using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using enfoke.Connector;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using System.Linq;
using System.Linq.Expressions;

using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Persistence;
using enfoke.Data;
using NHibernate;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Entities.Reserva;
using enfoke.Eges.Persistance;
using enfoke.AOP;
using enfoke.Data.Reference;

namespace enfoke.Eges.Data
{
    /// <summary>
    /// Encargada de brindar los datos concernientes a un médico
    /// </summary>
    public class MedicosDalc : Dalc, IService
    {
        protected MedicosDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<Especialidad> EspecialidadReadAll()
        {
            return dalEngine.GetAll<Especialidad>(Especialidad.Properties.Descripcion);
        }

        public EntityCollection<ExcepcionHorarioMedico> ExcepcionHorarioMedicoReadByMedico(int medicoId)
        {
            return ExcepcionHorarioMedicoReadByMedico(medicoId, false);
        }

        public EntityCollection<ExcepcionHorarioMedico> ExcepcionHorarioMedicoReadByMedico(int medicoId, bool soloVigentes)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioMedico.Properties.MedicoId, " = ", medicoId);
            if (soloVigentes)
            {
                filter.Add(BooleanOp.And, ExcepcionHorarioMedico.Properties.Horario.FechaFin, " >= ", enfoke.Time.Now.Date.AddMinutes(-1)); // Justo cuando termino ayer
                filter.Add(BooleanOp.And, ExcepcionHorarioMedico.Properties.Horario.FechaInicio, " < ", enfoke.Time.Now.Date.AddDays(1).AddMinutes(1)); // Justo cuando empieza mañana
            }

            ReadManyCommand<ExcepcionHorarioMedico> readCmd = new ReadManyCommand<ExcepcionHorarioMedico>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Lee las excepciones para el médico
        /// </summary>
        /// <param name="from">Fecha desde donde leer</param>
        /// <param name="to">Fecha hasta donde leer</param>
        /// <param name="medicoId">Médico del cual buscar sus excepciones</param>
        /// <returns>Las excepciones del médico</returns>
        public EntityCollection<ExcepcionHorarioMedico> ExcepcionHorarioMedicoReadByMedico
            (DateTime from, DateTime to, int medico)
        {
            Filter filter = new Filter();

            HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, ExcepcionHorarioMedico.Properties.Horario, from, to);

            filter.Add(BooleanOp.And, ExcepcionHorarioMedico.Properties.MedicoId, " = ", medico);

            ReadManyCommand<ExcepcionHorarioMedico> readCmd = new ReadManyCommand<ExcepcionHorarioMedico>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }
        public EntityCollection<ExcepcionHorarioMedico> ExcepcionHorarioMedicoReadByMedicos
            (DateTime from, DateTime to, EntityCollection<Medico> medicos)
        {
            Filter filter = new Filter();
            HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, ExcepcionHorarioMedico.Properties.Horario, from, to);
            filter.Add(BooleanOp.And, ExcepcionHorarioMedico.Properties.MedicoId, "IN", medicos.GetIds());
            Sort sort = new Sort();
            sort.Add(ExcepcionHorarioMedico.Properties.MedicoId);
            return dalEngine.GetManyByFilter<ExcepcionHorarioMedico>(filter, sort);
        }

        [RequiresTransaction]
        public virtual EntityCollection<ExcepcionHorarioMedico> ExcepcionHorarioMedicoUpdateMany(EntityCollection<ExcepcionHorarioMedico> ehmc)
        {
            // Elimino los que se eliminaron
            if (ehmc.DeletedItems.Count > 0)
            {
                for (int i = 0; i < ehmc.DeletedItems.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (ehmc.DeletedItems[i].Id > 0)
                    {
                        if (dalEngine.GetById<ExcepcionHorarioMedico>(ehmc.DeletedItems[i].Id) != null)
                        {
                            dalEngine.Delete(ehmc.DeletedItems[i]);
                        }
                    }
                }
            }

            for (int i = 0; i < ehmc.Count; i++)
            {
                if (ehmc[i].Id == 0)
                {

                    // Inserto
                    ehmc[i].Horario = dalEngine.Update<Horario>(ehmc[i].Horario);
                    ehmc[i] = dalEngine.Update(ehmc[i]);

                    // Seteo el Grupo
                    ehmc[i].Grupo = ehmc[i].Horario.Id;
                    ehmc[i] = dalEngine.Update(ehmc[i]);
                }
            }

            return ehmc;
        }

        public void ExcepcionHorarioMedicoDeleteMany(EntityCollection<ExcepcionHorarioMedico> ehmc)
        {
            dalEngine.Delete(ehmc);
        }

        public EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoReadByEquipoAndMedico(DateTime from, DateTime to, int equipo, int medico)
        {
            Filter filter = new Filter();

            HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, ExcepcionHorarioMedicoEquipo.Properties.Horario, from, to);

            filter.Add(BooleanOp.And,
                ExcepcionHorarioMedicoEquipo.Properties.Equipo,
                "=", equipo);

            filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.MedicoId,
                "=", medico);

            ReadManyCommand<ExcepcionHorarioMedicoEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioMedicoEquipo>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        public EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoReadByMedico(int medicoId)
        {
            return ExcepcionHorarioMedicoEquipoReadByMedico(medicoId, false);
        }

        public EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoReadByMedico(int medicoId, bool soloVigentes)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioMedicoEquipo.Properties.MedicoId, " = ", medicoId);
            if (soloVigentes)
            {
                filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.Horario.FechaFin, " >= ", enfoke.Time.Now.Date.AddMinutes(-1)); // Justo cuando termino ayer
                filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.Horario.FechaInicio, " < ", enfoke.Time.Now.Date.AddDays(1).AddMinutes(1)); // Justo cuando empieza mañana
            }

            ReadManyCommand<ExcepcionHorarioMedicoEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioMedicoEquipo>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }


        [RequiresTransaction]
        public virtual EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoUpdateMany(EntityCollection<ExcepcionHorarioMedicoEquipo> ehmec)
        {
            ExcepcionHorarioMedicoEquipoDeleteMany(ehmec.DeletedItems, ehmec, enfoke.Time.Now);

            for (int i = 0; i < ehmec.Count; i++)
            {
                if (ehmec[i].Id == 0)
                {

                    // Inserto
                    ehmec[i].Horario = dalEngine.Update<Horario>(ehmec[i].Horario);
                    ehmec[i] = dalEngine.Update(ehmec[i]);

                    // Seteo el Grupo
                    ehmec[i].Grupo = ehmec[i].Horario.Id;
                    ehmec[i] = dalEngine.Update(ehmec[i]);
                }
            }

            return ehmec;
        }

        public void ExcepcionHorarioMedicoEquipoDeleteMany(List<ExcepcionHorarioMedicoEquipo> ehmeDelete, EntityCollection<ExcepcionHorarioMedicoEquipo> ehmesNuevos, DateTime fecha)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            if (ehmeDelete.Count > 0)
            {
                List<int> gruposEliminados = new List<int>();
                for (int i = 0; i < ehmeDelete.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (ehmeDelete[i].Id > 0)
                    {
                        if (!gruposEliminados.Contains(ehmeDelete[i].Grupo.Value))
                        {
                            // Recorro todos los MEGs del grupo
                            EntityCollection<ExcepcionHorarioMedicoEquipo> ehes = this.ExcepcionHorarioMedicoEquipoByGrupo(ehmeDelete[i].Grupo.Value);

                            /*
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es menor o igual a ayer, no hago nada
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es mayor a ayer, pongo la fecha hasta como ayer
                             * - si la fecha desde es mayor a ayer, lo elimino
                             * */
                            DateTime ayer = fecha.AddDays(-1).Date;
                            for (int j = 0; j < ehes.Count; j++)
                            {
                                ExcepcionHorarioMedicoEquipo ehme = ehes[j];

                                if (ehme.FechaInicio.Date <= ayer)
                                {
                                    if (ehme.FechaFin.Date > ayer)
                                    {
                                        ehme.FechaFin = ayer;

                                        if (ehmesNuevos == null || !EsModificacion(ehme, ehmesNuevos))
                                        {
                                            ehme.Horario = dalEngine.Update<Horario>(ehme.Horario);


                                            ehme = dalEngine.Update<ExcepcionHorarioMedicoEquipo>(ehme);
                                        }
                                        else
                                        {
                                            dalEngine.Delete(ehme);
                                        }
                                    }
                                }
                                else
                                {
                                    dalEngine.Delete(ehme);
                                }
                            }

                            gruposEliminados.Add(ehmeDelete[i].Grupo.Value);
                        }
                    }
                }
            }
        }

        private EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoByGrupo(int grupo)
        {
            ReadManyCommand<ExcepcionHorarioMedicoEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioMedicoEquipo>(dalEngine);
            Filter filter = new Filter();

            // filtro por horarios con el mismo grupo
            filter.Add(BooleanOp.And,
                ExcepcionHorarioMedicoEquipo.Properties.Horario.Grupo,
                "=", grupo);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(ExcepcionHorarioMedicoEquipo.Properties.Id, SortingDirection.Desc);
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<MedicoAsociacion> MedicoAsociacionReadByIds(IEnumerable<int> ids)
        {
            Filter filter = new Filter();
            filter.Add(MedicoAsociacion.Properties.Id, "IN", ids);
            return dalEngine.GetManyByFilter<MedicoAsociacion>(filter);
        }

        public EntityCollection<Medico> MedicoReadByEspecialidad(int especialidadId)
        {
            return dalEngine.GetManyByProperty<Medico>(Medico.Properties.Especialidad.Id, especialidadId);
        }

        public EntityCollection<Medico> MedicoActuanteReadAll(bool soloReales)
        {
            Filter filter = new Filter();
            filter.Add(Medico.Properties.IsActuante, "=", true);
            if (soloReales)
                filter.Add(BooleanOp.And, Medico.Properties.Id, ">", 0);
            return dalEngine.GetManyByFilter<Medico>(filter);
        }

        public ReadAllCollection<Medico> MedicoReadAll(bool soloReales)
        {
            Filter filtro = null;
            if (soloReales)
            {
                filtro = new Filter();
                filtro.Add(Medico.Properties.Id, ">", 0);
            }

            return new ReadAllCollection<Medico>(MedicoReadByFilter(filtro));
        }

        public EntityCollection<Medico> MedicoReadAllActuantesYDisponibles()
        {
            Filter filter = new Filter();
            filter.Add(Medico.Properties.Id, ">", 0);
            return MedicoReadByFilter(filter);
        }

        public EntityCollection<Medico> MedicoConActualizacionRead()
        {
            string hql = "select med from Medico med, MedicoActualizacion mac WHERE med.Id = mac.MedicoId ";
            hql += " AND med.Activo = :activo";
            hql += " ORDER BY med.Apellido asc, med.Name asc ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("activo", true);

            return dalEngine.GetManyByQuery<Medico>(query);
        }

        public EntityCollection<MedicoInformesBusquedas> MedicoInformesBusquedasReadByMedico(int medicoId)
        {
            return dalEngine.GetManyByProperty<MedicoInformesBusquedas>(MedicoInformesBusquedas.Properties.MedicoId, medicoId, MedicoInformesBusquedas.Properties.Id, SortOrder.Descending);
        }

        public void MedicoInformesBusquedasDelete(MedicoInformesBusquedas medicoInformesBusqueda)
        {
            dalEngine.Delete(medicoInformesBusqueda);
        }

        public EntityCollection<SecurityUser> InformantesReadByLastAndFirstName(string lastAndFirstName)
        {
            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select user from SecurityUser user where user.Medico.IsInformante = true ");
            hqlBuilder.Append("and ");
            hqlBuilder.Append(SQLPortable.StringConcat("user.Medico.Apellido", "user.Medico.Name"));
            hqlBuilder.Append(" like '%");
            hqlBuilder.Append(lastAndFirstName.Replace(' ', '%'));
            hqlBuilder.Append("%'");
            hqlBuilder.Append(" order by user.Medico.Apellido, user.Medico.Name asc ");
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        /// <summary>
        /// Devuelve todos los medicos con Honorarios por Practica
        /// </summary>
        /// <returns>Medicos</returns>
        public EntityCollection<Medico> MedicoReadConHonorariosPractica()
        {

            Filter filter = new Filter();

            // Solo los que son informantes
            filter.Add(Medico.Properties.IsInformante, "=", true);

            // muestro solo los que cobran algo distinto a SueldoFijo (Honorarios o Sueldo + Honorarios)
            filter.Add(BooleanOp.And, Medico.Properties.HonorariosID,
                "!=",
                (int)MedicoHonorariosEnum.SueldoFijo);

            return MedicoReadByFilter(filter);
        }

        /// <summary>
        /// Retorna los médicos tecnicos
        /// </summary>
        /// <returns>Retorna los médicos tecnicos</returns>
        public EntityCollection<Medico> MedicoReadTecnicos()
        {
            return dalEngine.GetManyByProperty<Medico>(Medico.Properties.IsTecnico, true, new enfoke.Data.Reference.IPropertyReference[] { Medico.Properties.Apellido, Medico.Properties.Name }, new SortOrder[] { SortOrder.Ascending, SortOrder.Ascending });
        }

        public EntityCollection<Medico> MedicoSearchByApellidoYMatricula(string apellido, string matriculaNacional, string matriculaProvincial, bool soloConHonorariosPractica, bool soloActivos)
        {
            return MedicoSearchByApellidoYMatricula(apellido, matriculaNacional, matriculaProvincial, soloConHonorariosPractica, soloActivos, false);
        }

        public EntityCollection<Medico> MedicoReadByParameters(string apellido, string matricula, bool soloConHonorariosPractica, bool soloInformantes, string especialidad, bool traeEliminados, bool traeActuantes, bool traeInformantes, bool traeTecnicos)
        {
            ReadManyCommand<Medico> readCmd = new ReadManyCommand<Medico>(dalEngine);

            Filter filter = new Filter();
            filter.Add(BooleanOp.And, Medico.Properties.Id, ">", 0);

            if (!String.IsNullOrEmpty(apellido))
                filter.Add(BooleanOp.And, Medico.Properties.Apellido, "LIKE", apellido + "%");
            if (!String.IsNullOrEmpty(matricula))
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));

                filter.Add(Medico.Properties.MatriculaNacional, "=", matricula);
                filter.Add(BooleanOp.Or, Medico.Properties.MatriculaProvincial, "=", matricula);

                filter.Add(new CloseParenthesis());
            }
            if (!String.IsNullOrEmpty(especialidad))
                filter.Add(BooleanOp.And, Medico.Properties.FirmaEspecialidad, "LIKE", "%" + especialidad.Replace(' ', '%') + "%");
            if (!traeEliminados)
                filter.Add(BooleanOp.And, Medico.Properties.Activo, "=", true);
            // Tipos
            filter.Add(new OpenParenthesis(BooleanOp.And));
            if (traeActuantes)
                filter.Add(BooleanOp.Or, Medico.Properties.IsActuante, "=", true);
            if (traeInformantes)
                filter.Add(BooleanOp.Or, Medico.Properties.IsInformante, "=", true);
            if (traeTecnicos)
                filter.Add(BooleanOp.Or, Medico.Properties.IsTecnico, "=", true);

            if (!(traeActuantes || traeInformantes || traeTecnicos))
            {
                filter.Add(BooleanOp.And, Medico.Properties.IsActuante, "=", false);
                filter.Add(BooleanOp.And, Medico.Properties.IsInformante, "=", false);
                filter.Add(BooleanOp.And, Medico.Properties.IsTecnico, "=", false);
            }

            filter.Add(new CloseParenthesis());

            // muestro solo los que cobran algo distinto a SueldoFijo (Honorarios o Sueldo + Honorarios)
            if (soloConHonorariosPractica)
                filter.Add(BooleanOp.And, Medico.Properties.HonorariosID, "!=", (int)MedicoHonorariosEnum.SueldoFijo);
            // traigo solo los que sean informantes o todos.
            if (soloInformantes)
                filter.Add(BooleanOp.And, Medico.Properties.IsInformante, "=", true);

            readCmd.Filter = filter;

            //Sort
            Sort sort = new Sort();

            sort.Add(Medico.Properties.Apellido, SortingDirection.Asc);
            sort.Add(Medico.Properties.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<Medico> MedicoSearchByApellidoYMatricula(string apellido, string matriculaNacional, string matriculaProvincial, bool soloConHonorariosPractica, bool soloActivos, bool soloInformantes)
        {
            ReadManyCommand<Medico> readCmd = new ReadManyCommand<Medico>(dalEngine);

            Filter filter = new Filter();

            // se filtra el apellido
            if (String.IsNullOrEmpty(apellido) == false)
            {
                filter.Add(BooleanOp.And, Medico.Properties.Apellido,
                           "LIKE", apellido + "%");
            }

            // se filtra la matrícula
            if (!String.IsNullOrEmpty(matriculaNacional) || !String.IsNullOrEmpty(matriculaProvincial))
            {
                filter.Add(new OpenParenthesis(BooleanOp.And));

                if (!String.IsNullOrEmpty(matriculaNacional))
                    filter.Add(Medico.Properties.MatriculaNacional, "=", matriculaNacional);

                if (!String.IsNullOrEmpty(matriculaProvincial))
                {
                    if (!String.IsNullOrEmpty(matriculaNacional))
                        filter.Add(BooleanOp.Or, Medico.Properties.MatriculaProvincial, "=", matriculaProvincial);
                    else
                        filter.Add(Medico.Properties.MatriculaProvincial, "=", matriculaProvincial);
                }

                filter.Add(new CloseParenthesis());
            }


            if (soloConHonorariosPractica == true)
            {
                // muestro solo los que cobran algo distinto a SueldoFijo (Honorarios o Sueldo + Honorarios)
                filter.Add(BooleanOp.And, Medico.Properties.HonorariosID,
                           "!=",
                           (int)MedicoHonorariosEnum.SueldoFijo);
            }

            // Le agrego el filtro de que sólo devuelva los médicos activos (que no fueron borrados)
            if (soloActivos == true)
                filter.Add(BooleanOp.And, Medico.Properties.Activo,
                           "=", true);

            // traigo solo los que sean informantes o todos.
            if (soloInformantes == true)
                filter.Add(BooleanOp.And, Medico.Properties.IsInformante,
                           "=", true);

            readCmd.Filter = filter;

            //Sort
            Sort sort = new Sort();

            sort.Add(Medico.Properties.Apellido, SortingDirection.Asc);
            sort.Add(Medico.Properties.Name, SortingDirection.Asc);

            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public EntityCollection<Medico> MedicoReadByPractica(int practicaId, bool onlyPediatras, List<int> exposicionesId)
        {
            Filter filter = new Filter();

            // se filtran los pediatras
            MedicoAddFiltersByPediatra(onlyPediatras, filter);

            MedicoAddFiltersByPractica(practicaId, exposicionesId, filter);

            return MedicoReadByFilter(filter);
        }

        private static void MedicoAddFiltersByPediatra(bool onlyPediatras, Filter filter)
        {
            // se filtran los que atienden solo a adultos
            if (onlyPediatras)
                filter.Add(BooleanOp.And, Medico.Properties.TipoMedico,
                    "<>", (int)TipoAtencionEnum.Adulto);
        }

        public EntityCollection<Medico> MedicoReadByPractica(int practicaId, List<int> exposicionesId)
        {
            Filter filter = new Filter();

            MedicoAddFiltersByPractica(practicaId, exposicionesId, filter);

            return MedicoReadByFilter(filter);
        }
        public EntityCollection<Medico> MedicoReadByPracticaTeniendoHorarios(int practicaId, bool onlyPediatras, List<int> exposicionesId)
        {
            Filter filter = new Filter();

            MedicoAddFiltersByPractica(practicaId, exposicionesId, filter);

            // se filtran los pediatras
            MedicoAddFiltersByPediatra(onlyPediatras, filter);

            MedicoAddFilterByExistsHorario(filter);

            return MedicoReadByFilter(filter);
        }

        private static void MedicoAddFilterByExistsHorario(Filter filter)
        {
            ExistsFilterItem<MedicoEquipoHorario> horarios =
                new ExistsFilterItem<MedicoEquipoHorario>(
                                BooleanOp.And,
                                Medico.Properties.Id,
                                MedicoEquipoHorario.Properties.Medico.Id);
            horarios.Add(MedicoEquipoHorario.Properties.Horario.FechaFin, ">=", enfoke.Time.Today);
            filter.Add(horarios);
        }

        /// <summary>
        /// Devuelve el médico con el id indicado
        /// </summary>
        /// <param name="id">id del médico a buscar</param>
        /// <returns>Medico con el id indicado</returns>
        public Medico MedicoReadById(int id)
        {
            // Se toma la libertad de cachearlo por thread
            Medico ret = EntityThreadCache<Medico>.GetItem(id);
            if (ret == null)
            {
                ret = dalEngine.GetById<Medico>(id);
                if (ret != null)
                    EntityThreadCache<Medico>.SetItem(id, ret);
            }
            return ret;
        }

        public EntityCollection<Medico> MedicoReadByPracticaAndCategoriaTeniendoHorarios(int practicaId, int categoriaId, bool onlyPediatras, List<int> exposicionesId)
        {

            Filter filter = new Filter();

            MedicoAddFiltersByPediatra(onlyPediatras, filter);
            // se filtra por categoría
            filter.Add(BooleanOp.And, Medico.Properties.CategoriaMedico,
                "=", categoriaId);

            MedicoAddFilterByExistsHorario(filter);

            MedicoAddFiltersByPractica(practicaId, exposicionesId, filter);

            return MedicoReadByFilter(filter);
        }
        public EntityCollection<Medico> MedicoReadByPracticaAndCategoria(int practicaId, int categoriaId, bool onlyPediatras, List<int> exposicionesId)
        {

            Filter filter = new Filter();

            MedicoAddFiltersByPediatra(onlyPediatras, filter);
            // se filtra por categoría
            filter.Add(BooleanOp.And, Medico.Properties.CategoriaMedico,
                "=", categoriaId);

            MedicoAddFiltersByPractica(practicaId, exposicionesId, filter);

            return MedicoReadByFilter(filter);
        }

        public EntityCollection<Medico> MedicoReadByServicioDesdePracticas(int servicioId)
        {
            string hql = "select distinct m from Medico m, MedicoPractica mp " +
                        " where m.Id = mp.MedicoId and mp.Practica.ServicioEspecialidad.Servicio.Id = :servicioId" +
                        " and m.Activo = true";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("servicioId", servicioId);
            return dalEngine.GetManyByQuery<Medico>(query);
        }
        private static void MedicoAddFiltersByPractica(int practicaId, List<int> exposicionesId, Filter filter)
        {
            // filtra por práctica
            List<int> practicasId = new List<int>();
            practicasId.Add(practicaId);

            // Agrego filtro para que el medicoId sea tambien de las subsiguientes
            if (exposicionesId != null)
                practicasId.AddRange(exposicionesId);

            // Agrega los filtros
            foreach (int practica in practicasId)
            {
                ExistsFilterItem<MedicoPractica> filterItem = new ExistsFilterItem<MedicoPractica>(BooleanOp.And,
                                                        Medico.Properties.Id, MedicoPractica.Properties.MedicoId);
                filterItem.Add(MedicoPractica.Properties.Practica.Id, "=", practica);

                filter.Add(filterItem);
            }
        }

        /// <summary>
        /// Retorna los médicos informantes
        /// </summary>
        /// <returns>Retorna los médicos informantes</returns>
        private EntityCollection<Medico> MedicoInformanteReadAllActive(bool soloActivos, bool soloReales)
        {
            string hql = "SELECT DISTINCT su.Medico From SecurityUser su " +
                         "WHERE su.Medico.IsInformante = true ";

            if (soloActivos)
                hql += "AND su.Medico.Activo = true ";

            if (soloReales)
                hql += "AND su.Medico.Id > 0 ";

            hql += "ORDER BY su.Medico.Apellido ASC, su.Medico.Name ASC ";

            IQuery query = dalEngine.CreateQuery(hql);

            return dalEngine.GetManyByQuery<Medico>(query);
        }

        /// <summary>
        /// Retorna los médicos informantes que tienen un usuario y estan activos
        /// </summary>
        /// <returns>Retorna los médicos informantes</returns>
        public EntityCollection<Medico> MedicoInformanteReadAllActive(bool soloReales)
        {
            return MedicoInformanteReadAllActive(true, soloReales);
        }

        /// <summary>
        /// Retorna los médicos informantes que tienen un usuario
        /// </summary>
        /// <returns>Retorna los médicos informantes</returns>
        public EntityCollection<Medico> MedicoInformanteReadAll(bool soloReales)
        {
            return MedicoInformanteReadAllActive(false, soloReales);
        }

        public Medico MedicoUpdate(Medico medico)
        {
            bool nuevo = (medico.Id == 0);

            medico = dalEngine.Update<Medico>(medico);

            if (nuevo)
                MedicoActualizacionUpdate(new MedicoActualizacion { Fecha = enfoke.Time.Now, MedicoId = medico.Id });

            return medico;
        }

        public void MedicoDelete(Medico medico)
        {
            MedicoActualizacionDelete(medico.Id);


            dalEngine.Delete(medico);
        }

        [Private]
        public EntityCollection<MedicoAsociacion> MedicoAsociacionReadByRead(
            DateTime? fechaDesde, DateTime? fechaHasta, string medico,
            string especialidad, string matricula, bool mostrarControlados, bool mostrarNoControlados, bool mostrarEliminados)
        {
            StringBuilder hqlBuilder = new StringBuilder();

            if (!string.IsNullOrEmpty(matricula))
                hqlBuilder.Append(" and (ma.MatriculaNacional = :matricula or ma.MatriculaProvincial = :matricula or ma.MatriculaEspecialidad = :matricula) ");

            if (fechaDesde.HasValue)
                hqlBuilder.Append(" and ma.CreateDate >= :fechaDesde ");

            if (fechaHasta.HasValue)
                hqlBuilder.Append(" and ma.CreateDate < :fechaHasta ");

            string medicoApellido = string.Empty;
            string medicoApellido1 = string.Empty;
            string medicoApellido2 = string.Empty;

            string medicoNombre2 = string.Empty;
            string medicoNombre = string.Empty;

            if (!String.IsNullOrEmpty(medico))
            {
                if (medico.Trim().Contains(" "))
                {
                    string[] nombres = medico.Trim().Split(' ');
                    if (nombres.Length == 3)
                    {
                        hqlBuilder.AppendFormat(" and ((ma.{0} like :medicoApellido1 ", MedicoAsociacion.Properties.LastName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoApellido2 ", MedicoAsociacion.Properties.LastName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoNombre2) ", MedicoAsociacion.Properties.FirstName.Name);

                        hqlBuilder.AppendFormat(" or (ma.{0} like :medicoApellido1 ", MedicoAsociacion.Properties.LastName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoNombre ", MedicoAsociacion.Properties.FirstName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoNombre2)) ", MedicoAsociacion.Properties.FirstName.Name);
                        medicoApellido1 = nombres[0];
                        medicoApellido2 = nombres[1];
                        medicoNombre = nombres[1];
                        medicoNombre2 = nombres[2];
                    }
                    else
                    {
                        hqlBuilder.AppendFormat(" and ((ma.{0} like :medicoApellido ", MedicoAsociacion.Properties.LastName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoNombre) ", MedicoAsociacion.Properties.FirstName.Name);
                        hqlBuilder.AppendFormat(" or (ma.{0} like :medicoApellido1 ", MedicoAsociacion.Properties.LastName.Name);
                        hqlBuilder.AppendFormat(" and ma.{0} like :medicoApellido2)) ", MedicoAsociacion.Properties.LastName.Name);
                        medicoApellido = medico.Trim().Split(' ')[0];
                        medicoNombre = medico.Trim().Split(' ')[1];

                        medicoApellido1 = medico.Trim().Split(' ')[0];
                        medicoApellido2 = medico.Trim().Split(' ')[1];
                    }

                }
                else
                {
                    hqlBuilder.AppendFormat(" and ma.{0} like :medicoApellido ", MedicoAsociacion.Properties.LastName.Name);
                    medicoApellido = medico.Trim();
                }
            }

            // XOR
            if (mostrarControlados ^ mostrarNoControlados)
            {
                hqlBuilder.Append(" and (ma.Controlado = :mostrarControlados ");

                if (mostrarNoControlados)
                    hqlBuilder.Append(" or ma.Controlado is null ");

                hqlBuilder.Append(") ");
            }

            if (!mostrarEliminados)
                hqlBuilder.AppendFormat(" and (ma.{0} <> :deleted or ma.{0} is null) ", MedicoAsociacion.Properties.Deleted.Name);

            if (!String.IsNullOrEmpty(especialidad))
                hqlBuilder.Append(" and ma.Especialidad.Descripcion like :especialidad ");

            string hql = hqlBuilder.ToString();
            if (!String.IsNullOrEmpty(hql))
                hql = hql.Substring(4);

            hql = "select ma from MedicoAsociacion ma where " + hql;

            IQuery query = dalEngine.CreateQuery(hql);
            if (!mostrarEliminados)
                query.SetBoolean("deleted", true);
            if (!string.IsNullOrWhiteSpace(matricula))
                query.SetString("matricula", matricula.Trim());

            if (fechaDesde.HasValue)
                query.SetDateTime("fechaDesde", fechaDesde.Value.Date);

            if (fechaHasta.HasValue)
                query.SetDateTime("fechaHasta", fechaHasta.Value.Date.AddDays(1));
            if (!String.IsNullOrEmpty(medicoApellido))
            {
                query.SetString("medicoApellido", "%" + medicoApellido.Trim() + "%");
            }

            if (!String.IsNullOrEmpty(medicoApellido1))
            {
                query.SetString("medicoApellido1", "%" + medicoApellido1.Trim() + "%");
            }

            if (!String.IsNullOrEmpty(medicoApellido2))
            {
                query.SetString("medicoApellido2", "%" + medicoApellido2.Trim() + "%");
            }

            if (!String.IsNullOrEmpty(medicoNombre))
            {
                query.SetString("medicoNombre", "%" + medicoNombre.Trim() + "%");
            }

            if (!String.IsNullOrEmpty(medicoNombre2))
            {
                query.SetString("medicoNombre2", "%" + medicoNombre2.Trim() + "%");
            }


            if (!String.IsNullOrEmpty(especialidad))
                query.SetString("especialidad", especialidad.Trim().Replace(" ", "%") + "%");

            // XOR
            if (mostrarControlados ^ mostrarNoControlados)
                query.SetBoolean("mostrarControlados", mostrarControlados);

            return dalEngine.GetManyByQuery<MedicoAsociacion>(query);
        }

        [Private]
        public EntityCollection<MedicoAsociacion> MedicoAsociaonReadByLastDays()
        {
            Filter filter = new Filter();
            filter.Add(MedicoAsociacion.Properties.CreateDate, ">=", enfoke.Time.Today.AddDays(-7));
            OpenParenthesis open2 = new OpenParenthesis(BooleanOp.And);
            filter.Add(open2);
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, "<>", true);
            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.Deleted, "is", null);
            CloseParenthesis close2 = new CloseParenthesis();
            filter.Add(close2);
            EntityCollection<MedicoAsociacion> medicos = MedicoAsociacionReadByFilter(filter);
            return medicos;
        }

        /// <summary>
        /// Devuelvo todos los MedicoAsociacion
        /// </summary>
        /// <returns>Todos los MedicoAsociacion</returns>
        public EntityCollection<MedicoAsociacion> MedicoAsociacionReadAll()
        {
            return MedicoAsociacionReadByFilter(null);
        }

        private EntityCollection<MedicoAsociacion> MedicoAsociacionReadByFilter(Filter filter)
        {
            ReadManyCommand<MedicoAsociacion> readCmd = new ReadManyCommand<MedicoAsociacion>(dalEngine);

            if (filter != null)
                readCmd.Filter = filter;

            readCmd.Sort = new Sort(new SortItem(MedicoAsociacion.Properties.LastName, SortingDirection.Asc));
            readCmd.Sort.Add(MedicoAsociacion.Properties.FirstName, SortingDirection.Asc);

            return readCmd.Execute();
        }

        /// <summary>
        /// Devuelve el médico con el id indicado
        /// </summary>
        /// <param name="id">id del médico a buscar</param>
        /// <returns>MedicoAsociacion con el id indicado</returns>
        [AnonymousMethod()]
        public MedicoAsociacion MedicoAsociacionReadById(int id)
        {
            return dalEngine.GetById<MedicoAsociacion>(id);
        }

        /// <summary>
        /// Devuelve el medicoId con la matricula nacional requerida
        /// </summary>
        /// <param name="matricula">La matrícula nacional a buscar</param>
        /// <returns>Médico que resulto de la búsqueda</returns>
        public MedicoAsociacion MedicoAsociacionReadByMatriculaNacional(string matricula)
        {
            if (String.IsNullOrEmpty(matricula))
                return null;

            Filter filter = new Filter();
            filter.Add(MedicoAsociacion.Properties.MatriculaNacional, "=", matricula);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, "<>", true);
            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.Deleted, "is", null);
            filter.Add(new CloseParenthesis());

            EntityCollection<MedicoAsociacion> medicos = MedicoAsociacionReadByFilter(filter);

            if (medicos.Count > 1)
                throw new NotLoggeableException("Hay más de un médico registrado con la matrícula nacional [" + matricula + "].");
            else if (medicos.Count == 0)
                return null;
            else
                return medicos[0];
        }

        /// <summary>
        /// Devuelve el medicoId con la matricula especialidad requerida
        /// </summary>
        /// <param name="matricula">La matrícula expecialidad a buscar</param>
        /// <returns>Médico que resulto de la búsqueda</returns>
        public MedicoAsociacion MedicoAsociacionReadByMatriculaEspecialidad(string matricula)
        {
            if (String.IsNullOrEmpty(matricula))
                return null;

            Filter filter = new Filter();
            filter.Add(MedicoAsociacion.Properties.MatriculaEspecialidad, "=", matricula);
            //filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Especialidad.Id, "=", especialidad);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, "<>", true);
            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.Deleted, "is", null);
            filter.Add(new CloseParenthesis());

            EntityCollection<MedicoAsociacion> medicos = MedicoAsociacionReadByFilter(filter);

            if (medicos.Count > 1)
                throw new NotLoggeableException("Hay un médico registrado con la matrícula especialidad [" + matricula + "].");
            else if (medicos.Count == 0)
                return null;
            else
                return medicos[0];
        }

        /// <summary>
        /// Devuelve el medicoId con la matricula provincial requerida
        /// </summary>
        /// <param name="matricula">La matrícula provincial a buscar</param>
        /// <returns>Médico que resulto de la búsqueda</returns>
        public MedicoAsociacion MedicoAsociacionReadByMatriculaProvincial(string matricula)
        {
            if (String.IsNullOrEmpty(matricula))
                return null;

            Filter filter = new Filter();
            filter.Add(MedicoAsociacion.Properties.MatriculaProvincial, "=", matricula);
            filter.Add(new OpenParenthesis(BooleanOp.And));
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, "<>", true);
            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.Deleted, "is", null);
            filter.Add(new CloseParenthesis());

            EntityCollection<MedicoAsociacion> medicos = MedicoAsociacionReadByFilter(filter);

            if (medicos.Count > 1)
                throw new NotLoggeableException("Hay más de un médico registrado con la matrícula provincial [" + matricula + "].");
            else if (medicos.Count == 0)
                return null;
            else
                return medicos[0];
        }

        /// <summary>
        /// Actualiza los datos del médico indicado
        /// </summary>
        /// <param name="medicoId">Médico a actulizar</param>
        public MedicoAsociacion MedicoAsociacionUpdate(MedicoAsociacion medico)
        {
            return dalEngine.Update<MedicoAsociacion>(medico);
            //return dalEngine.Update(medicoId);
        }

        /// <summary>
        /// Actualiza los datos de los medicos indicados
        /// </summary>
        /// <param name="medicoId">Médicos a actulizar</param>
        public void MedicoAsociacionUpdateBatch(List<int> medicosAsociacion, bool controlado)
        {
            dalEngine.UpdatePropertyBatchByIds<MedicoAsociacion>(medicosAsociacion, MedicoAsociacion.Properties.Controlado, controlado);
            dalEngine.UpdatePropertyBatchByIds<MedicoAsociacion>(medicosAsociacion, MedicoAsociacion.Properties.ControlDate, controlado == true ? enfoke.Time.Now : (DateTime?)null);
        }

        public bool MedicoAsociacionTieneTurnos(int medicoAsociacionId)
        {
            string hql = "SELECT count(t) FROM Turno t WHERE t.Orden.MedicoSolicitanteID = :medicoAsociacionId";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("medicoAsociacionId", medicoAsociacionId);

            object ret = query.UniqueResult();

            if (ret != null)
                return (int.Parse(ret.ToString()) > 0);
            else
                return false;
        }

        [Private]
        public void MedicoAsociacionDelete(MedicoAsociacion medico)
        {
            dalEngine.Update<MedicoAsociacion>(medico);
        }

        /// <summary>
        /// Devuelvo todos los MedicoAsociacion con los filtros dados
        /// </summary>
        /// <param name="matricula">Filtro por Matrícula</param>
        /// <param name="apellido">Filtro por Apellido</param>
        /// <param name="nombre">Filtro por Nombre</param>
        /// <returns>Todos los MedicoAsociacion con los filtros</returns>
        public EntityCollection<MedicoAsociacion> MedicoAsociacionRead(string matricula, string apellido, string nombre)
        {

            string strApellido = apellido.Trim().Replace(" ", "%") + "%";
            string strNombre = nombre.Trim().Replace(" ", "%") + "%";

            Filter filter = new Filter();

            AgregarFiltroMatricula(filter, matricula);

            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.LastName,
                "LIKE", strApellido);

            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.FirstName,
                "LIKE", strNombre);

            AgregarFiltroMedicoAsociacionNotDeleted(filter);

            return MedicoAsociacionReadByFilter(filter);
        }

        private void AgregarFiltroMedicoAsociacionNotDeleted(Filter filter)
        {
            OpenParenthesis open2 = new OpenParenthesis(BooleanOp.And);
            filter.Add(open2);
            filter.Add(BooleanOp.And, MedicoAsociacion.Properties.Deleted, "<>", true);
            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.Deleted, "is", null);
            CloseParenthesis close2 = new CloseParenthesis();
            filter.Add(close2);
        }

        private void AgregarFiltroMatricula(Filter filter, string matricula)
        {
            string strMatricula = matricula.Trim();

            if (string.IsNullOrEmpty(strMatricula) == true)
                return;

            filter.Add(new OpenParenthesis(BooleanOp.And));

            filter.Add(MedicoAsociacion.Properties.MatriculaNacional, "=", strMatricula);

            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.MatriculaProvincial, "=", strMatricula);

            filter.Add(BooleanOp.Or, MedicoAsociacion.Properties.MatriculaEspecialidad, "=", strMatricula);

            filter.Add(new CloseParenthesis());
        }

        public bool MedicoDerivanteHabilitado(MedicoAsociacion medicoAsociacion, ObraSocialPlan osp)
        {
            long matriculaNacional;
            if (!long.TryParse(medicoAsociacion.MatriculaNacional, out matriculaNacional))
                matriculaNacional = 0;

            long matriculaProvincial;
            if (!long.TryParse(medicoAsociacion.MatriculaProvincial, out matriculaProvincial))
                matriculaProvincial = 0;

            if (matriculaNacional == 0 && matriculaProvincial == 0)
                return false;

            Filter filter = new Filter { { PadronValidaDerivante.Properties.ObraSocialPlanId, "=", osp.Id } };

            filter.Add(new OpenParenthesis(BooleanOp.And));

            if (matriculaNacional > 0)
            {
                filter.Add(PadronValidaDerivante.Properties.MatriculaNacional, "=", matriculaNacional);
            }

            if (matriculaProvincial > 0)
            {
                if (matriculaNacional > 0)
                {
                    filter.Add(BooleanOp.Or, PadronValidaDerivante.Properties.MatriculaProvincial, "=", matriculaProvincial);
                }
                else
                {
                    filter.Add(PadronValidaDerivante.Properties.MatriculaProvincial, "=", matriculaProvincial);
                }
            }

            filter.Add(new CloseParenthesis());

            ReadManyCommand<PadronValidaDerivante> readCmd = new ReadManyCommand<PadronValidaDerivante>(dalEngine) { Filter = filter };

            EntityCollection<PadronValidaDerivante> padrones = readCmd.Execute();

            return padrones != null && padrones.Count > 0;
        }

        public EntityCollection<MedicoAsociacionEspecialidad> MedicoAsociacionEspecialidadReadAll()
        {
            return dalEngine.GetAll<MedicoAsociacionEspecialidad>(MedicoAsociacionEspecialidad.Properties.Descripcion);
        }

        public EntityCollection<MedicoAsociacionEspecialidad> MedicoAsociacionEspecialidadUpdateMany(EntityCollection<MedicoAsociacionEspecialidad> maes)
        {
            return dalEngine.UpdateCollection<MedicoAsociacionEspecialidad>(maes);
        }

        #region MedicoEquipoHorario
        /// <summary>
        /// Returna todos los equipos-horarios de un médico.
        /// </summary>
        /// <param name="userId">El id del médico</param>
        /// <param name="soloVigentes">Bool si traigo Solo los Vigentes o Todos</param>
        /// <returns>Los equipos-horarios del médico</returns>
        [RequiresTransaction]
        public virtual EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByMedico(int medicoId, bool soloVigentes)
        {
            EntityCollection<MedicoEquipoHorario> ret = null;
            ReadManyCommand<MedicoEquipoHorario> readCmd = new ReadManyCommand<MedicoEquipoHorario>(dalEngine);

            Filter filter = new Filter();
            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Medico.Id, "=", medicoId);

            readCmd.Filter = filter;

            if (soloVigentes)
            {
                EntityCollection<MedicoEquipoHorario> mehs = readCmd.Execute();
                EntityCollection<MedicoEquipoHorario> mehsVigentes = new EntityCollection<MedicoEquipoHorario>();

                List<int> gruposExistentes = new List<int>();
                foreach (MedicoEquipoHorario meh in mehs)
                {
                    if (meh.FechaFin >= enfoke.Time.Today && !gruposExistentes.Contains(meh.Grupo.Value))
                    {
                        EntityCollection<MedicoEquipoHorario> mehsGrupo = this.MedicoEquipoHorarioReadByGrupo(meh.Grupo.Value);

                        foreach (MedicoEquipoHorario mehGrupo in mehsGrupo)
                            mehsVigentes.Add(mehGrupo);

                        gruposExistentes.Add(meh.Grupo.Value);
                    }
                }

                ret = mehsVigentes;
            }
            else
                ret = readCmd.Execute();
            return ret;
        }

        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByEquipoAndMedico(DateTime horaInicio, DateTime horaFin, int equipo, int? medico, SucursalName sucursal, bool useFullTimeQuery)
        {
            Filter filter = new Filter();
            TurnosDalc turnoDalc = Context.Session.TurnosDalc;
            Calendario caledario = turnoDalc.CalendarioGetByFecha(horaInicio.Date, horaFin.Date);
            if (useFullTimeQuery)
                HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, MedicoEquipoHorario.Properties.Horario,
                            horaInicio, horaFin, DiaDeSemanaConverter.FromDayOfWeek(horaInicio.DayOfWeek));
            else
                HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, MedicoEquipoHorario.Properties.Horario, horaInicio, horaFin);

            if (medico.HasValue)
                filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Medico.Id,
                    "=", medico.Value);

            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Equipo,
                "=", equipo);

            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.EsAgendaRestringida, "=", caledario.EsDiaAgendaRestringida(sucursal.Id, horaInicio.Date));
            ReadManyCommand<MedicoEquipoHorario> readCmd = new ReadManyCommand<MedicoEquipoHorario>(dalEngine);
            readCmd.Filter = filter;
            return readCmd.Execute();
        }

        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByEquipo(int equipoId, bool soloVigentes)
        {
            EntityCollection<MedicoEquipoHorario> ret = null;
            ReadManyCommand<MedicoEquipoHorario> readCmd = new ReadManyCommand<MedicoEquipoHorario>(dalEngine);

            Filter filter = new Filter();
            filter.Add(MedicoEquipoHorario.Properties.Equipo, "=", equipoId);

            readCmd.Filter = filter;

            if (soloVigentes)
            {
                EntityCollection<MedicoEquipoHorario> mehs = readCmd.Execute();
                EntityCollection<MedicoEquipoHorario> mehsVigentes = new EntityCollection<MedicoEquipoHorario>();

                List<int> gruposExistentes = new List<int>();
                foreach (MedicoEquipoHorario meh in mehs)
                {
                    if (meh.FechaFin >= enfoke.Time.Today && !gruposExistentes.Contains(meh.Grupo.Value))
                    {
                        EntityCollection<MedicoEquipoHorario> mehsGrupo = this.MedicoEquipoHorarioReadByGrupo(meh.Grupo.Value);

                        foreach (MedicoEquipoHorario mehGrupo in mehsGrupo)
                            mehsVigentes.Add(mehGrupo);

                        gruposExistentes.Add(meh.Grupo.Value);
                    }
                }

                ret = mehsVigentes;
            }
            else
                ret = readCmd.Execute();

            return ret;
        }

        public EntityCollection<ResultadoMedicoEquipo> MedicoEquipoHorarioReadAllDistinctByServicioSucursal()
        {
            string hql = "select distinct new ResultadoMedicoEquipo(meh.Equipo, m) " +
                    " from MedicoEquipoHorario meh, Medico m where meh.Medico.Id = m.Id and m.Activo = true";
            IQuery query = dalEngine.CreateQuery(hql);
            return dalEngine.GetManyByQuery<ResultadoMedicoEquipo>(query);
        }

        [RequiresTransaction]
        public virtual EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioUpdateMany(EntityCollection<MedicoEquipoHorario> mehc)
        {
            // Elimino los que se eliminaron
            MedicoEquipoHorarioDelete(mehc.DeletedItems, mehc, enfoke.Time.Now.Date);

            // Recorro los que quedan buscando nuevos para insertarlos
            MedicoEquipoHorarioUpdate(mehc);

            return mehc;
        }

        private void MedicoEquipoHorarioDelete(List<MedicoEquipoHorario> mehDelete, EntityCollection<MedicoEquipoHorario> mehc, DateTime fecha)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            if (mehDelete.Count > 0)
            {
                List<int> gruposEliminados = new List<int>();
                for (int i = 0; i < mehDelete.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (mehDelete[i].Id > 0)
                    {
                        if (!gruposEliminados.Contains(mehDelete[i].Grupo.Value))
                        {
                            // Recorro todos los MEGs del grupo
                            EntityCollection<MedicoEquipoHorario> mehs = this.MedicoEquipoHorarioReadByGrupo(mehDelete[i].Grupo.Value);

                            /*
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es menor o igual a ayer, no hago nada
                             * - si la fecha desde es menor o igual a ayer y la fecha hasta es mayor a ayer, pongo la fecha hasta como ayer
                             * - si la fecha desde es mayor a ayer, lo elimino
                             * */
                            DateTime ayer = fecha.AddDays(-1).Date;
                            for (int j = 0; j < mehs.Count; j++)
                            {
                                MedicoEquipoHorario meh = mehs[j];

                                if (meh.FechaInicio.Date <= ayer)
                                {
                                    if (meh.FechaFin.Date > ayer)
                                    {
                                        meh.FechaFin = ayer;

                                        if (mehc == null || !EsModificacion(meh, mehc))
                                        {
                                            meh.UpdateUser = user.Id;


                                            meh.Horario = dalEngine.Update<Horario>(meh.Horario);


                                            meh = dalEngine.Update<MedicoEquipoHorario>(meh);
                                        }
                                        else
                                        {
                                            dalEngine.Delete(meh);
                                        }
                                    }
                                }
                                else
                                {
                                    dalEngine.Delete(meh);
                                }
                            }

                            gruposEliminados.Add(mehDelete[i].Grupo.Value);
                        }
                    }
                }
            }
        }

        private void MedicoEquipoHorarioUpdate(EntityCollection<MedicoEquipoHorario> mehc)
        {
            SecurityUser user = Security.Current.UserInfo.User;

            for (int i = 0; i < mehc.Count; i++)
            {
                // Si es un nuevo MEH, segun la frecuencia es que inserto
                if (mehc[i].Id == 0)
                {
                    mehc[i].CreateUser = user.Id;
                    mehc[i].CreateDate = enfoke.Time.Now;

                    if (mehc[i].FrecuenciaSemanal == FrecuenciaSemanalEnum.Semanal)
                    {

                        // Inserto
                        mehc[i].Horario = dalEngine.Update<Horario>(mehc[i].Horario);

                        // Seteo el Grupo
                        mehc[i].Grupo = mehc[i].Horario.Id;
                        mehc[i] = dalEngine.Update(mehc[i]);
                    }
                    else
                    {
                        // MedicoEquipoHorario y UpdateCommand a utilizar
                        MedicoEquipoHorario meh = null;
                        int grupo = 0; // Id del Grupo

                        // Tomo solo la fecha sin hora
                        DateTime dateFrom = mehc[i].FechaInicio.Date;
                        DateTime dateTo = mehc[i].FechaFin.Date;

                        // Voy avanzando los periodos
                        DateTime primero = dateFrom;
                        while (dateFrom <= dateTo)
                        {
                            if (dateFrom > primero)
                            {
                                // Creo un nuevo MedicoEquipoHorario y le asigno todos los datos
                                meh = new MedicoEquipoHorario(mehc[i]);

                                // Pongo fecha Desde y Hasta
                                meh.FechaInicio = dateFrom.AddDays(-7 * (int)mehc[i].FrecuenciaSemanal);
                                meh.FechaFin = meh.FechaInicio.AddDays(6);


                                // Inserto
                                meh.Horario = dalEngine.Update<Horario>(meh.Horario);
                                meh = dalEngine.Update(meh);

                                // Tomo el grupo del primero insertado
                                if (grupo == 0)
                                    grupo = meh.Horario.Id;

                                // Seteo el Grupo
                                meh.Grupo = grupo;
                                meh = dalEngine.Update(meh);
                            }

                            // Avanzo un periodo
                            dateFrom = dateFrom.AddDays(7 * (int)mehc[i].FrecuenciaSemanal);
                        }

                        // Inserto el ultimo MEH que quedo creado
                        // Creo un nuevo MedicoEquipoHorario y le asigno todos los datos
                        meh = new MedicoEquipoHorario(mehc[i]);

                        meh.CreateUser = user.Id;
                        meh.CreateDate = enfoke.Time.Now;

                        // Pongo fecha Desde y Hasta
                        meh.FechaInicio = dateFrom.AddDays(-7 * (int)mehc[i].FrecuenciaSemanal);
                        meh.FechaFin = meh.FechaInicio.AddDays(6);
                        if (meh.FechaFin > dateTo)
                            meh.FechaFin = dateTo;


                        // Inserto
                        meh.Horario = dalEngine.Update<Horario>(meh.Horario);
                        meh = dalEngine.Update(meh);

                        if (grupo == 0)
                            grupo = meh.Horario.Id;

                        // Seteo el Grupo
                        meh.Grupo = grupo;
                        meh = dalEngine.Update(meh);
                    }
                }
            }
        }

        private bool EsModificacion(MedicoEquipoHorario originalEnBase, EntityCollection<MedicoEquipoHorario> nuevosAInsertar)
        {
            bool modificacion = false;
            for (int i = 0; !modificacion && i < nuevosAInsertar.Count; i++)
            {
                if (nuevosAInsertar[i].Dias == originalEnBase.Dias &&
                    nuevosAInsertar[i].Equipo.Id == originalEnBase.Equipo.Id &&
                    nuevosAInsertar[i].FechaInicio == originalEnBase.FechaInicio &&
                    nuevosAInsertar[i].FrecuenciaSemanal == originalEnBase.FrecuenciaSemanal &&
                    nuevosAInsertar[i].HoraInicio == originalEnBase.HoraInicio &&
                    nuevosAInsertar[i].Medico == originalEnBase.Medico)
                    modificacion = true;
            }

            return modificacion;
        }

        private bool EsModificacion(ExcepcionHorarioMedicoEquipo originalEnBase, EntityCollection<ExcepcionHorarioMedicoEquipo> nuevosAInsertar)
        {
            bool modificacion = false;
            for (int i = 0; !modificacion && i < nuevosAInsertar.Count; i++)
            {
                if (nuevosAInsertar[i].Dias == originalEnBase.Dias &&
                    nuevosAInsertar[i].Equipo.Id == originalEnBase.Equipo.Id &&
                    nuevosAInsertar[i].FechaInicio == originalEnBase.FechaInicio &&
                    nuevosAInsertar[i].FrecuenciaSemanal == originalEnBase.FrecuenciaSemanal &&
                    nuevosAInsertar[i].HoraInicio == originalEnBase.HoraInicio &&
                    nuevosAInsertar[i].MedicoId == originalEnBase.MedicoId)
                    modificacion = true;
            }

            return modificacion;
        }

        /// <summary>
        /// Retorno todos los MedicoEquipoHorario de un Grupo
        /// </summary>
        /// <param name="grupo">Nro de Grupo</param>
        /// <returns>Colección de MedicoEquipoHorario</returns>
        private EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByGrupo(int grupo)
        {
            ReadManyCommand<MedicoEquipoHorario> readCmd = new ReadManyCommand<MedicoEquipoHorario>(dalEngine);
            Filter filter = new Filter();

            // filtro por horarios con el mismo grupo
            filter.Add(BooleanOp.And,
                MedicoEquipoHorario.Properties.Horario.Grupo,
                "=", grupo);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(MedicoEquipoHorario.Properties.Id, SortingDirection.Desc);
            readCmd.Sort = sort;

            return readCmd.Execute();
        }

        public void MedicoequipohorarioLogicalDeleteByMedicoId(int medicoId)
        {
            EntityCollection<MedicoEquipoHorario> mehCollection = MedicoEquipoHorarioReadByMedico(medicoId, true);
            List<MedicoEquipoHorario> mehList = new List<MedicoEquipoHorario>();

            foreach (MedicoEquipoHorario meh in mehCollection)
                mehList.Add(meh);

            MedicoEquipoHorarioDelete(mehList, null, enfoke.Time.Now.AddDays(1));
        }

        #region Reemplazo de profesional en agenda

        /// <summary>
        /// En base a un meh pasado busca y devuelve la interseccion con los horarios que realmente tiene persistido el medicoId en la base.
        /// </summary>
        /// <param name="meh">MedicoEquipoHorario con los datos necesarios cargados (FechaInicio, FechaFin, HoraInicio, HoraFin, Dias, FrecuenciaSemanal y MedicoId).</param>
        /// <returns>Coleccion de MedicoEquiposHorarios intersección entre los pasado por parametro y lo persistido en la base. Si la lista tiene 0 items, significa que no existe intersección.</returns>
        public EntityCollection<MedicoEquipoHorario> MedicoHorarioInterseccion(MedicoEquipoHorario meh)
        {
            return MedicoEquipoHorarioInterseccion(
                meh.FechaInicio, meh.FechaFin, meh.HoraInicio, meh.HoraFin,
                meh.Dias, meh.FrecuenciaSemanal, (int?)null, meh.Medico.Id, null);
        }

        /// <summary>
        /// En base a los parametros pasados busca y devuelve la interseccion con los horarios que realmente tiene persistido el medicoId en la base.
        /// </summary>
        /// <param name="fechaInicio"></param>
        /// <param name="fechaFin"></param>
        /// <param name="horaInicio"></param>
        /// <param name="horaFin"></param>
        /// <param name="dias"></param>
        /// <param name="frecuenciaSemanal"></param>
        /// <param name="medicoID">Busca los horarios del medicoId. Si el id es 0 (cero), busca todos los horarios de todos los medicos que cumplan con el período marcado por las fechas.</param>
        /// <returns>Coleccion de MedicoEquiposHorarios intersección entre los pasado por parametro y lo persistido en la base. Si la lista tiene 0 items, significa que no existe intersección.</returns>
        public EntityCollection<MedicoEquipoHorario> MedicoHorarioInterseccion(
            DateTime fechaInicio, DateTime fechaFin, TimeSpan horaInicio, TimeSpan horaFin,
            DiaDeSemana dias, FrecuenciaSemanalEnum frecuenciaSemanal, int medicoID)
        {
            return MedicoEquipoHorarioInterseccion(
                fechaInicio, fechaFin, horaInicio, horaFin,
                dias, frecuenciaSemanal, (int?)null, medicoID, null);
        }

        /// <summary>
        /// En base a un meh pasado busca y devuelve la interseccion con los horarios que realmente tiene persistido el medicoId en la base.
        /// </summary>
        /// <param name="meh">MedicoEquipoHorario con los datos necesarios cargados (FechaInicio, FechaFin, HoraInicio, HoraFin, Dias, FrecuenciaSemanal EquipoId y MedicoId).</param>
        /// <returns>Coleccion de MedicoEquiposHorarios intersección entre los pasado por parametro y lo persistido en la base. Si la lista tiene 0 items, significa que no existe intersección.</returns>
        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioInterseccion(MedicoEquipoHorario meh)
        {
            if (meh.Equipo == null)
                throw new NotLoggeableException("No se ha seleccionado un equipo");

            return MedicoEquipoHorarioInterseccion(
                meh.FechaInicio, meh.FechaFin, meh.HoraInicio, meh.HoraFin,
                meh.Dias, meh.FrecuenciaSemanal, meh.Equipo.Id, meh.Medico != null ? (int?)meh.Medico.Id : null, meh.Equipo.Sucursal);
        }

        /// <summary>
        /// En base a los parametros pasados busca y devuelve la interseccion con los horarios que realmente tiene persistido el medicoId en la base.
        /// </summary>
        /// <param name="fechaInicio"></param>
        /// <param name="fechaFin"></param>
        /// <param name="horaInicio"></param>
        /// <param name="horaFin"></param>
        /// <param name="dias"></param>
        /// <param name="frecuenciaSemanal"></param>
        /// <param name="equipoID">Busca los equipos del medicoId que correspondan. Si es nulo, busca todos los horarios del médico para cualquier equipo.</param>
        /// <param name="medicoID">Busca los horarios del medicoId. Si el id es 0 (cero), busca todos los horarios de todos los medicos que cumplan con el período marcado por las fechas.</param>
        /// <returns>Coleccion de MedicoEquiposHorarios intersección entre los pasado por parametro y lo persistido en la base. Si la lista tiene 0 items, significa que no existe intersección.</returns>
        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioInterseccion(
            DateTime fechaInicio, DateTime fechaFin, TimeSpan horaInicio, TimeSpan horaFin,
            DiaDeSemana dias, FrecuenciaSemanalEnum frecuenciaSemanal, int? equipoID, int? medicoID, SucursalName sucursal)
        {
            EntityCollection<MedicoEquipoHorario> mehsFinal = new EntityCollection<MedicoEquipoHorario>();
            EntityCollection<MedicoEquipoHorario> mehsDB = new EntityCollection<MedicoEquipoHorario>();

            if (equipoID.HasValue == true && equipoID.Value > 0)
                mehsDB = MedicoEquipoHorarioReadByEquipoAndMedico(fechaInicio, fechaFin, equipoID.Value, medicoID > 0 ? medicoID : (int?)null, sucursal, false);
            else
                mehsDB = MedicoEquipoHorarioReadByMedico(medicoID.Value, true);

            TimeSet miOcupacion = new TimeSet();

            foreach (MedicoEquipoHorario mehDB in mehsDB)
            {
                // Compruebo que exista intersección entre ambos.
                if (ExisteInterseccionDeDiasAndHorarios(fechaInicio, fechaFin, horaInicio, horaFin, dias, equipoID, mehDB))
                {
                    CalcularInterseccion(fechaInicio,
                        fechaFin,
                        horaInicio,
                        horaFin,
                        dias,
                        frecuenciaSemanal,
                        mehDB,
                        mehsFinal);
                }
            }

            return mehsFinal;
        }

        private bool ExisteInterseccionDeDiasAndHorarios(DateTime fechaInicio, DateTime fechaFin, TimeSpan horaInicio, TimeSpan horaFin, DiaDeSemana dias, int? equipoID, MedicoEquipoHorario mehDB)
        {
            return (dias & mehDB.Dias) > 0
                   && (
                          (horaInicio >= mehDB.HoraInicio && horaInicio < mehDB.HoraFin)
                          || (horaFin > mehDB.HoraInicio && horaFin <= mehDB.HoraFin)
                          || (horaInicio < mehDB.HoraInicio && horaFin > mehDB.HoraFin)
                      )
                   && (
                          (fechaInicio >= mehDB.FechaInicio && fechaInicio <= mehDB.FechaFin)
                          || (fechaFin >= mehDB.FechaInicio && fechaFin <= mehDB.FechaFin)
                          || (fechaInicio < mehDB.FechaInicio && fechaFin > mehDB.FechaFin)
                      )
                   && ((equipoID.HasValue == true && equipoID.Value > 0 && equipoID == mehDB.Equipo.Id)
                       || (equipoID.HasValue == true && equipoID.Value == 0)
                       || equipoID.HasValue == false
                      );
        }

        private void CalcularInterseccion(
            DateTime fechaInicio, DateTime fechaFin, TimeSpan horaInicio,
            TimeSpan horaFin, DiaDeSemana dias, FrecuenciaSemanalEnum frecuenciaSemanal,
            MedicoEquipoHorario mehDB, EntityCollection<MedicoEquipoHorario> mehsFinal)
        {
            MedicoEquipoHorario mehFinal = null;

            //if (frecuenciaSemanal == FrecuenciaSemanalEnum.Semanal)
            //{
            // Si el mehDB pertenece a un grupo del cual ya agregué un elemento anterior.
            Predicate<MedicoEquipoHorario> predicate =
                delegate(MedicoEquipoHorario compare)
                {
                    if (mehDB.Horario.Grupo.HasValue == true)
                        if (compare.Horario.Grupo.HasValue == true)
                            return (compare.Horario.Grupo.Value == mehDB.Horario.Grupo.Value);

                    return false;
                };
            MedicoEquipoHorario mehFromGroup = mehsFinal.Find(predicate);

            if (mehFromGroup != null)
            {
                if (mehFromGroup.FechaInicio >= mehDB.FechaInicio)
                {
                    if (fechaInicio >= mehDB.FechaInicio)
                        mehFromGroup.FechaInicio = fechaInicio;
                    else
                        mehFromGroup.FechaInicio = mehDB.FechaInicio;
                }
                else if (mehFromGroup.FechaFin < mehDB.FechaFin)
                {
                    if (fechaFin <= mehDB.FechaFin)
                        mehFromGroup.FechaFin = fechaFin;
                    else
                        mehFromGroup.FechaFin = mehDB.FechaFin;
                }
            }
            else
            {
                mehFinal = new MedicoEquipoHorario();

                // Cargo los datos de la intersección.
                mehFinal.Assign(mehDB);
                mehFinal.Dias = dias & mehDB.Dias;
                mehFinal.FechaInicio = fechaInicio >= mehDB.FechaInicio ? fechaInicio : mehDB.FechaInicio;
                mehFinal.FechaFin = fechaFin >= mehDB.FechaFin ? mehDB.FechaFin : fechaFin;
                mehFinal.HoraInicio = horaInicio >= mehDB.HoraInicio ? horaInicio : mehDB.HoraInicio;
                mehFinal.HoraFin = horaFin >= mehDB.HoraFin ? mehDB.HoraFin : horaFin;
                mehFinal.FrecuenciaSemanal = (int)frecuenciaSemanal > (int)mehDB.FrecuenciaSemanal
                                                 ? frecuenciaSemanal
                                                 : mehDB.FrecuenciaSemanal;

                mehsFinal.Add(mehFinal);
            }
            //}
            //else
            //{
            //    throw new Exception("Falta implementar medicoId horario equipo para frecuencia quincenal y mensual.");
            //}
        }

        #endregion

        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByPracticaYSucursal(DateTime horaInicio, DateTime horaFin, bool incluyeProvisorios, Practica pracitca, SucursalName sucursal)
        {
            TurnosDalc turnoDalc = Context.Session.TurnosDalc;
            Calendario caledario = turnoDalc.CalendarioGetByFecha(horaInicio.Date, horaFin.Date);
            EntityCollection<Medico> medicosRelacionados =
                    MedicoReadByPractica(pracitca.Id, null);
            EntityCollection<EquipoPractica> equiposRelacionados =
                (Context.Session.EquiposDalc).EquipoPracticaReadByPracticaId(pracitca.Id);
            List<int> equiposId = new List<int>();
            foreach (EquipoPractica eqp in equiposRelacionados)
                equiposId.Add(eqp.Equipo.Id);
            // Consulta los horarios
            if (equiposId == null || equiposId.Count == 0)
                return new EntityCollection<MedicoEquipoHorario>();

            Filter filter = new Filter();
            HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, MedicoEquipoHorario.Properties.Horario,
                        horaInicio, horaFin, DiaDeSemanaConverter.FromDayOfWeek(horaInicio.DayOfWeek));

            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.EsAgendaRestringida, "=", caledario.EsDiaAgendaRestringida(sucursal.Id, horaInicio.Date));
            if (medicosRelacionados != null && medicosRelacionados.Count > 0)
                filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Medico.Id, "IN", medicosRelacionados.GetIds());

            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Equipo.Id, "IN", equiposId);
            if (incluyeProvisorios == false)
                filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.EstadoDb, "=", (int)MedicoEquipoHorarioEstadoEnum.Definitivo);

            Sort sort = new Sort();
            sort.Add(MedicoEquipoHorario.Properties.Equipo.Id);
            sort.Add(MedicoEquipoHorario.Properties.Medico.Id);
            return dalEngine.GetManyByFilter<MedicoEquipoHorario>(filter, sort);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <param name="equipos"></param>
        /// <param name="medicos"></param>
        /// <param name="useFullTimeQuery"></param>
        /// <param name="incluyeProvisorios"></param>
        /// <returns></returns>
        [Private]
        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByEquiposAndMedicosSinFeriados(DateTime from, DateTime to, IList<Equipo> equipos, IList<Medico> medicos, bool useFullTimeQuery, bool incluyeProvisorios)
        {
            if (equipos == null || equipos.Count == 0 || medicos == null || medicos.Count == 0)
                return new EntityCollection<MedicoEquipoHorario>();

            Filter filter = new Filter();
            if (useFullTimeQuery)
            {
                HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, MedicoEquipoHorario.Properties.Horario,
                            from, to, DiaDeSemanaConverter.FromDayOfWeek(from.DayOfWeek));
            }
            else
            {
                HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, MedicoEquipoHorario.Properties.Horario, from, to);
            }

            if (medicos != null && medicos.Count > 0)
                filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Medico.Id, "IN", Utils.EnumerableConvert.ToIdList(medicos));

            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Equipo.Id, "IN", Utils.EnumerableConvert.ToIdList(equipos));
            if (incluyeProvisorios == false)
                filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.EstadoDb, "=", (int)MedicoEquipoHorarioEstadoEnum.Definitivo);

            Sort sort = new Sort();
            sort.Add(MedicoEquipoHorario.Properties.Equipo.Id);
            sort.Add(MedicoEquipoHorario.Properties.Medico.Id);
            return dalEngine.GetManyByFilter<MedicoEquipoHorario>(filter, sort);
        }

        public EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoReadByEquiposAndMedicos(DateTime from, DateTime to, List<int> equiposId, EntityCollection<Medico> medicos)
        {
            Filter filter = new Filter();

            HorarioQueryUtils.AddFilterHorarioInRangeQuery(filter, ExcepcionHorarioMedicoEquipo.Properties.Horario, from, to);

            filter.Add(BooleanOp.And,
                ExcepcionHorarioMedicoEquipo.Properties.Equipo.Id,
                "IN", equiposId);

            filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.MedicoId,
                "IN", medicos.GetIds());

            Sort sort = new Sort();
            sort.Add(ExcepcionHorarioMedicoEquipo.Properties.Equipo.Id);
            sort.Add(ExcepcionHorarioMedicoEquipo.Properties.MedicoId);

            return dalEngine.GetManyByFilter<ExcepcionHorarioMedicoEquipo>(filter, sort);
        }

        #endregion


        public EntityCollection<MedicoHonorario> MedicoHonorarioReadByMedico(int medicoId, bool excluirEliminados, bool withObraSocial) // bool eliminado)
        {
            ReadManyCommand<MedicoHonorario> readCmd = new ReadManyCommand<MedicoHonorario>(dalEngine);

            Filter filter = new Filter();

            filter.Add(BooleanOp.And, MedicoHonorario.Properties.MedicoID,
                "=", medicoId);

            if (excluirEliminados == true)
                filter.Add(BooleanOp.And, MedicoHonorario.Properties.Deleted,
                    "=", false);

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(MedicoHonorario.Properties.ObraSocialID, SortingDirection.Asc);
            sort.Add(MedicoHonorario.Properties.FechaDesde, SortingDirection.Asc);
            sort.Add(MedicoHonorario.Properties.FechaHasta, SortingDirection.Asc);
            readCmd.Sort = sort;

            EntityCollection<MedicoHonorario> result = readCmd.Execute();

            if (withObraSocial == true)
            {
                ObrasSocialesDalc _dalcOS = Context.Session.ObrasSocialesDalc;

                foreach (MedicoHonorario mh in result)
                    if (mh.ObraSocialID.HasValue == true)
                        mh.ObraSocialName = dalEngine.GetById<ObraSocialName>(mh.ObraSocialID.Value);
            }

            return result;
        }

        [RequiresTransaction]
        public virtual void MedicoHonorarioSave(MedicoHonorario mh, EntityCollection<MedicoHonorarioPractica> mhpc)
        {
            EntityCollection<IVigencia> MHs = new EntityCollection<IVigencia>();
            EntityCollection<MedicoHonorario> existentes = MedicoHonorarioReadByMedicoHonorario(mh);

            IList<IVigencia> existentesRecast = new List<IVigencia>();

            foreach (MedicoHonorario mhExist in existentes)
                existentesRecast.Add(mhExist);

            MHs.AddRange(VigenciaUtils<IVigencia>.ObtenerModificaciones(existentesRecast,
                            mh, false, Security.Current.UserInfo.User));

            foreach (IVigencia item in MHs)
                if (item is MedicoHonorario)
                    dalEngine.Update(item);

            // Leo el registro nuevo para obtenerlo actualizado.
            mh = MedicoHonorarioReadLastByMedicoAndObraSocial(mh.MedicoID, mh.ObraSocialID);

            // Creo los Items
            EntityCollection<MedicoHonorarioPractica> mhpsSave = new EntityCollection<MedicoHonorarioPractica>();

            for (int i = 0; i < mhpc.Count; i++)
            {
                MedicoHonorarioPractica mhp = mhpc[i];

                MedicoHonorarioPractica mhpNuevo = new MedicoHonorarioPractica();
                mhpNuevo.MedicoHonorarioID = mhp.MedicoHonorarioID;
                mhpNuevo.PracticaID = mhp.PracticaID;
                mhpNuevo.PorcentajeGastos = mhp.PorcentajeGastos;
                mhpNuevo.PorcentajeHonorarios = mhp.PorcentajeHonorarios;
                mhpNuevo.PorcentajeInsumos = mhp.PorcentajeInsumos;
                mhpNuevo.PorcentajeModulo = mhp.PorcentajeModulo;
                mhpNuevo.AplicaDescuento = mhp.AplicaDescuento;
                mhpNuevo.Valor = mhp.Valor;
                mhpNuevo.MedicoHonorarioID = mh.Id;

                mhpsSave.Add(mhpNuevo);
            }

            if (mhpsSave.Count > 0)
            {
                dalEngine.UpdateCollection<MedicoHonorarioPractica>(mhpsSave);
            }
        }

        private MedicoHonorario MedicoHonorarioReadLastByMedicoAndObraSocial(
            int idMedico, int? idObraSocial)
        {
            string hql = "SELECT mh FROM MedicoHonorario mh " +
                        "WHERE mh.MedicoID = :idMedico ";

            if (idObraSocial.HasValue)
                hql += "AND mh.ObraSocialID = :idObraSocial ";

            hql += "ORDER BY mh.Id DESC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idMedico", idMedico);
            if (idObraSocial.HasValue)
                query.SetParameter("idObraSocial", idObraSocial);

            query.SetMaxResults(1);

            return dalEngine.GetByQuery<MedicoHonorario>(query);
        }

        private EntityCollection<MedicoHonorario> MedicoHonorarioReadByMedicoHonorario(MedicoHonorario mh)
        {
            Filter filter = new Filter();

            filter.Add(MedicoHonorario.Properties.MedicoID, "=", mh.MedicoID);
            filter.Add(BooleanOp.And, MedicoHonorario.Properties.Deleted, "=", false);

            if (mh.ObraSocialID.HasValue)
                filter.Add(BooleanOp.And, MedicoHonorario.Properties.ObraSocialID, "=", mh.ObraSocialID.Value);

            return dalEngine.GetManyByFilter<MedicoHonorario>(filter);
        }

        public void MedicoHonorarioDelete(MedicoHonorario mh)
        {
            // Marco como eliminado
            mh.Deleted = true;

            Audit.AuditDelete(mh, Security.Current.UserInfo.User.Id);

            if (mh.FechaHasta.HasValue == false)
                mh.FechaHasta = enfoke.Time.Now;


            dalEngine.Update<MedicoHonorario>(mh);
        }

        /// <summary>
        /// Busco MedicoHonorario para un Medico y OS
        /// </summary>
        /// <param name="medicoId">ID del Medico [null = cualquiera]</param>
        /// <param name="os">ID de la ObraSocial [null = todas (no cualquiera)]</param>
        /// <returns>MHs que apliquen</returns>
        public EntityCollection<MedicoHonorario> MedicoHonorarioReadByMedicoAndObraSocial(int? medico, int? os)
        {
            string hql = "SELECT mh FROM MedicoHonorario mh " +
                         "WHERE mh.Deleted = false ";

            if (os.HasValue)
                hql += " AND mh.ObraSocialID = :os ";
            else
                hql += " AND mh.ObraSocialID IS NULL ";
            if (medico.HasValue)
                hql += " AND mh.MedicoID = :medico";

            IQuery query = dalEngine.CreateQuery(hql);
            if (os.HasValue)
                query.SetParameter("os", os.Value);
            if (medico.HasValue)
                query.SetParameter("medico", medico.Value);

            return dalEngine.GetManyByQuery<MedicoHonorario>(query);
        }

        public EntityCollection<MedicoHonorarioPractica> MedicoHonorarioPracticaReadByMedicoHonorario(int medicoHonorarioId)
        {
            return dalEngine.GetManyByProperty<MedicoHonorarioPractica>(MedicoHonorarioPractica.Properties.MedicoHonorarioID, medicoHonorarioId);
        }

        public MedicoHonorarioPractica MedicoHonorarioPracticaReadByMedicoHonorarioAndPractica(int idMH, int idPractica)
        {
            ReadManyCommand<MedicoHonorarioPractica> readCmd = new ReadManyCommand<MedicoHonorarioPractica>(dalEngine);

            readCmd.Filter =
                new Filter(new FilterItem(MedicoHonorarioPractica.Properties.MedicoHonorarioID, " = ", idMH));
            readCmd.Filter.Add(BooleanOp.And, MedicoHonorarioPractica.Properties.PracticaID, " = ", idPractica);

            readCmd.MaxResults = 1;

            EntityCollection<MedicoHonorarioPractica> result = readCmd.Execute();

            if (result != null && result.Count > 0)
                return result[0];

            return null;
        }

        public EntityCollection<MedicoHonorarioPracticaResult> MedicoHonorarioPracticaResultReadByMedicoHonorario(int medicoHonorarioId)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.MedicoHonorarioPracticaResult(mhp.MedicoHonorario.Id, " +
                                                                                               "mhp.Practica.ServicioEspecialidad.Servicio.Name, " +
                                                                                               "mhp.Practica.ServicioEspecialidad.Servicio.Id, " +
                                                                                               "mhp.Practica.Name, " +
                                                                                               "mhp.Practica.Id, " +
                                                                                               "mhp.PorcentajeGastos, " +
                                                                                               "mhp.PorcentajeHonorarios, " +
                                                                                               "mhp.PorcentajeModulo, " +
                                                                                               "mhp.PorcentajeInsumos, " +
                                                                                               "mhp.Valor, " +
                                                                                               "mhp.AplicaDescuento) " +
                   "FROM MedicoHonorarioPracticaHQL AS mhp " +
                   "WHERE mhp.MedicoHonorario.Id = :idMedicoHonorario ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idMedicoHonorario", medicoHonorarioId);

            return dalEngine.GetManyByQuery<MedicoHonorarioPracticaResult>(query);
        }

        /// <summary>
        /// Busco MedicoHonorarioPractica para un Medico, OS y Practica en una Fecha determinada
        /// </summary>
        /// <param name="medicoId">ID del Medico [null = cualquiera]</param>
        /// <param name="os">ID de la ObraSocial [null = todas (no cualquiera)]</param>
        /// <param name="practica">ID de la práctica</param>
        /// <returns>Un MHP que aplique</returns>
        public EntityCollection<MedicoHonorarioPractica> MedicoHonorarioPracticaReadByMedicoObraSocialAndPractica(int? medico, int? os, int practica)
        {
            string hql = "select mhp from MedicoHonorarioPractica mhp, MedicoHonorario mh "
                                + "where mh.Id = mhp.MedicoHonorarioID AND mh.Deleted = false "
                                + "AND mhp.PracticaID = :practica ";
            if (os.HasValue)
                hql += " AND mh.ObraSocialID = :os ";
            else
                hql += " AND mh.ObraSocialID IS NULL ";
            if (medico.HasValue)
                hql += " AND mh.MedicoID = :medico";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("practica", practica);
            if (os.HasValue)
                query.SetParameter("os", os.Value);
            if (medico.HasValue)
                query.SetParameter("medico", medico.Value);

            return dalEngine.GetManyByQuery<MedicoHonorarioPractica>(query);
        }

        /// <summary>
        /// Devuelve todos los honorarios del médico especificado, vigentes y no
        /// vigentes.
        /// </summary>
        /// <param name="userId">El id del médico</param>
        /// <returns>Los honorarios</returns>
        public EntityCollection<MedicoPractica> MedicoPracticaReadByMedico(int medicoId)
        {
            return dalEngine.GetManyByProperty<MedicoPractica>(MedicoPractica.Properties.MedicoId, medicoId);
        }

        public EntityCollection<MedicoPractica> MedicoPracticaReadByMedicoIdPracticaNameAndPracticaCode(int medicoId, string practicaName, string practicaCode)
        {
            string hql = "SELECT DISTINCT mp FROM MedicoPractica mp " +
                         "WHERE mp.MedicoId = :idMedico ";

            if (!String.IsNullOrEmpty(practicaName))
                hql += "AND mp.Practica.Name LIKE :practica ";

            if (!String.IsNullOrEmpty(practicaCode))
                hql += "AND mp.Practica.Code LIKE :code ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idMedico", medicoId);
            if (!String.IsNullOrEmpty(practicaName))
                query.SetParameter("practica", practicaName.Trim().Replace(' ', '%').ToUpper() + "%");
            if (!String.IsNullOrEmpty(practicaCode))
                query.SetParameter("code", practicaCode.Trim().Replace(' ', '%').ToUpper() + "%");

            return dalEngine.GetManyByQuery<MedicoPractica>(query);
        }

        public List<int> MedicoPracticaIdsReadByMedico(int medicoId)
        {
            string hql = "SELECT DISTINCT mp.Practica.Id " +
                         "FROM MedicoPractica mp " +
                         "WHERE mp.MedicoId = :idMedico ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("idMedico", medicoId);

            return (List<int>)query.List<int>();
        }

        public void MedicoPracticaDeleteMany(EntityCollection<MedicoPractica> mpc)
        {
            if (mpc != null && mpc.Count > 0)
            {
                EntityCollection<MedicoPractica> aEliminar = new EntityCollection<MedicoPractica>();
                aEliminar.AddRange(mpc.FindAll(delegate(MedicoPractica item) { return item.Id > 0; }));
                dalEngine.Delete(aEliminar);
            }
        }

        public EntityCollection<MedicoPractica> MedicoPracticaUpdateMany(EntityCollection<MedicoPractica> mpc)
        {
            EntityCollection<MedicoPractica> borrar = new EntityCollection<MedicoPractica>();
            EntityCollection<MedicoPractica> modificar = new EntityCollection<MedicoPractica>();

            foreach (MedicoPractica mp in mpc.DeletedItems)
                borrar.Add(mp);

            DescartarEliminadas(mpc, modificar);

            MedicoPracticaDeleteMany(borrar);

            if (modificar != null && modificar.Count > 0)
            {
                MedicoPractica mp = modificar[0];
                EntityCollection<MedicoPractica> existentes = Context.Session.MedicosDalc.MedicoPracticaReadByMedico(mp.MedicoId);
                EntityCollection<MedicoPractica> existentesEliminar = new EntityCollection<MedicoPractica>();
                for (int i = modificar.Count - 1; i >= 0; i--)
                {
                    Predicate<MedicoPractica> predicate = delegate(MedicoPractica compare) { return compare.Practica.Id == modificar[i].Practica.Id; };
                    existentesEliminar.AddRange(mpc.DeletedItems.FindAll(predicate));
                    if (existentesEliminar != null && existentesEliminar.Count > 0)
                        modificar.Remove(modificar[i]);
                }
            }

            return dalEngine.UpdateCollection<MedicoPractica>(modificar);

        }

        private static void DescartarEliminadas(EntityCollection<MedicoPractica> mpc, EntityCollection<MedicoPractica> modificar)
        {
            foreach (MedicoPractica mp in mpc)
            {
                EntityCollection<MedicoPractica> existentesEliminar = new EntityCollection<MedicoPractica>();
                if (mp.Id > 0)
                {
                    Predicate<MedicoPractica> predicate = delegate(MedicoPractica compare) { return compare.Id == mp.Id; };
                    existentesEliminar.AddRange(mpc.DeletedItems.FindAll(predicate));
                }

                if (existentesEliminar == null || existentesEliminar.Count == 0)
                    modificar.Add(mp);
            }
        }

        public EntityCollection<CondicionAtencionMedico> CondicionAtencionMedicoReadByMedicoId(int medicoId)
        {
            return dalEngine.GetManyByProperty<CondicionAtencionMedico>(CondicionAtencionMedico.Properties.MedicoId, medicoId);
        }

        [RequiresTransaction]
        public virtual void CondicionAtencionMedicoUpdateByMedico(EntityCollection<CondicionAtencionMedico> condiciones, int medicoId)
        {
            EntityCollection<CondicionAtencionMedico> condicionesViejas = this.CondicionAtencionMedicoReadByMedicoId(medicoId);
            foreach (CondicionAtencionMedico condicionVieja in condicionesViejas)
                dalEngine.Delete(condicionVieja);

            this.SetearCollectionComoNueva(condiciones);
            foreach (CondicionAtencionMedico condicionNueva in condiciones)
            {
                condicionNueva.MedicoId = medicoId;
                dalEngine.Update<CondicionAtencionMedico>(condicionNueva);
            }
        }

        public void CondicionAtencionMedicoDelete(CondicionAtencionMedico condicion)
        {
            dalEngine.Delete(condicion);
        }

        public EntityCollection<MedicoLight> MedicoFromPracticaTurnoReadByProperty(string propertyName, IList<int> values)
        {
            if (values == null || values.Count == 0)
                return new EntityCollection<MedicoLight>();

            SQLBlockBuilder<int> idsMedico = new SQLBlockBuilder<int>(values);
            string medicos = idsMedico.BuildConstrainBlock("ptu.Id");

            StringBuilder hqlBuilder = new StringBuilder();
            hqlBuilder.Append("Select new enfoke.Eges.Entities.MedicoLight(med, ptu.Id) from MedicoLight med, PracticaTurno ptu where ptu.");
            hqlBuilder.Append(propertyName);
            hqlBuilder.AppendFormat(" = med.Id and {0} ", medicos);
            IQuery query = dalEngine.CreateQuery(hqlBuilder.ToString());
            return dalEngine.GetManyByQuery<MedicoLight>(query);
        }

        private void SetearCollectionComoNueva(EntityCollection<CondicionAtencionMedico> condiciones)
        {
            foreach (CondicionAtencionMedico condicion in condiciones)
                condicion.Id = 0;
        }

        public EntityCollection<CondicionAtencionMedico> CondicionAtencionMedicoReadByMedicosIds(IList<int> medicosIds)
        {
            if (medicosIds.Count <= 0)
                return new EntityCollection<CondicionAtencionMedico>();

            Filter filter = new Filter();
            filter.Add(CondicionAtencionMedico.Properties.MedicoId, "IN", medicosIds);
            // va ordenado por id porque de todas las que machean se toma la primera valida.
            Sort sort = new Sort();
            sort.Add(CondicionAtencionMedico.Properties.Id);
            return dalEngine.GetManyByFilter<CondicionAtencionMedico>(filter, sort);
        }

        public EntityCollection<Medico> MedicoReadByPractica(List<int> practicasId)
        {
            string hql = "SELECT DISTINCT su.Medico " +
                         "FROM SecurityUser su, MedicoPractica mpr " +
                         "WHERE mpr.MedicoId = su.Medico.Id " +
                         "AND mpr.Practica.Id IN (:practicasId) " +
                         "AND su.Medico.IsInformante = true";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("practicasId", practicasId);

            return dalEngine.GetManyByQuery<Medico>(query);
        }

        public EntityCollection<TipoNovedadMedico> TipoNovedadMedicoReadByBloqueaTurnos(bool bloqueaTurnos)
        {
            return dalEngine.GetManyByProperty<TipoNovedadMedico>(TipoNovedadMedico.Properties.BloqueaTurnos, bloqueaTurnos);
        }

        /// <summary>
        /// Agrega un nuevo tipo de novedades proporcionado.
        /// </summary>
        /// <param name="tipoNovedadMedico">El tipo de novedad a agregar</param>
        public void TipoNovedadMedicoAdd(TipoNovedadMedico tipoNovedadMedico)
        {
            dalEngine.Update<TipoNovedadMedico>(tipoNovedadMedico);
        }

        /// <summary>
        /// Agrega todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El TipoNovedadMedicoCollection con todos los tipos a agregar</param>
        public void TipoNovedadMedicoAdd(EntityCollection<TipoNovedadMedico> novedades)
        {
            dalEngine.UpdateCollection<TipoNovedadMedico>(novedades);
        }

        /// <summary>
        /// Actualiza el tipo de novedades proporcionado.
        /// </summary>
        /// <param name="tipoNovedadMedico">El tipo de novedad a actualizar</param>
        public void TipoNovedadMedicoUpdate(TipoNovedadMedico tipoNovedadMedico)
        {
            dalEngine.Update<TipoNovedadMedico>(tipoNovedadMedico);
        }

        /// <summary>
        /// Actualiza todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El TipoNovedadMedicoCollection con todos los tipos a actualizar</param>
        public void TipoNovedadMedicoUpdate(EntityCollection<TipoNovedadMedico> novedades)
        {
            dalEngine.UpdateCollection<TipoNovedadMedico>(novedades);
        }

        public bool TipoNovedadMedicoPuedeEliminarse(TipoNovedadMedico tipoNovedadMedico)
        {
            return dalEngine.GetManyByProperty<ExcepcionHorarioMedico>(ExcepcionHorarioMedico.Properties.TipoNovedadMedico, tipoNovedadMedico).Count == 0;
        }

        /// <summary>
        /// Elimina todos los tipos de novedades proporcionados.
        /// </summary>
        /// <param name="novedades">El TipoNovedadMedicoCollection con todos los tipos a eliminar</param>
        public void TipoNovedadMedicoDelete(EntityCollection<TipoNovedadMedico> novedades)
        {
            dalEngine.Delete(novedades);
        }

        #region Leyendas

        public EntityCollection<Leyenda> LeyendaReadByMedico(int medicoId)
        {
            return LeyendaReadByMedico(medicoId, false);
        }


        [RequiresTransaction]
        public virtual EntityCollection<Leyenda> LeyendaUpdateMany(EntityCollection<Leyenda> leyendas)
        {
            // Elimino los que se eliminaron
            if (leyendas.DeletedItems.Count > 0)
            {
                for (int i = 0; i < leyendas.DeletedItems.Count; i++)
                {
                    // Solo elimino si no era uno nuevo
                    if (leyendas.DeletedItems[i].Id > 0)
                    {
                        dalEngine.Delete(leyendas.DeletedItems[i]);
                        dalEngine.Delete(leyendas.DeletedItems[i].Horario);
                    }
                }
            }

            for (int i = 0; i < leyendas.Count; i++)
            {


                // Inserto
                leyendas[i].Horario = dalEngine.Update<Horario>(leyendas[i].Horario);
                leyendas[i] = dalEngine.Update<Leyenda>(leyendas[i]);

                // Seteo el Grupo
                leyendas[i].Grupo = leyendas[i].Horario.Id;
                leyendas[i] = dalEngine.Update<Leyenda>(leyendas[i]);
            }
            return leyendas;
        }

        public EntityCollection<Leyenda> LeyendaReadByMedico(int medicoId, bool soloVigentes)
        {
            Filter filter = new Filter();

            filter.Add(Leyenda.Properties.Medico.Id, " = ", medicoId);
            if (soloVigentes)
            {
                filter.Add(BooleanOp.And, Leyenda.Properties.Horario.FechaFin, " >= ", enfoke.Time.Now.Date.AddMinutes(-1)); // Justo cuando termino ayer
                filter.Add(BooleanOp.And, Leyenda.Properties.Horario.FechaInicio, " < ", enfoke.Time.Now.Date.AddDays(1).AddMinutes(1)); // Justo cuando empieza mañana
            }

            ReadManyCommand<Leyenda> readCmd = new ReadManyCommand<Leyenda>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();

        }
        public EntityCollection<Leyenda> LeyendaReadByEquipo(int equipoId)
        {
            return dalEngine.GetManyByProperty<Leyenda>(Leyenda.Properties.Equipo.Id, equipoId);
        }
   
        #endregion

        #region Private Functions
        /// <summary>
        /// Lee todos los médicos, de acuerdo a un filtro
        /// </summary>
        /// <param name="filter">Filtro</param>
        /// <returns>Todos los médicos que corresponden al filtro pasado</returns>
        private EntityCollection<Medico> MedicoReadByFilter(Filter filter)
        {
            ReadManyCommand<Medico> readCmd = new ReadManyCommand<Medico>(dalEngine);

            if (filter == null)
                filter = new Filter();

            // Le agrego el filtro de que sólo devuelva los médicos activos (que no fueron borrados)
            filter.Add(BooleanOp.And, Medico.Properties.Activo,
                "=", true);
            readCmd.Filter = filter;

            //Sort
            Sort sort = new Sort();
            sort.Add(Medico.Properties.Apellido, SortingDirection.Asc);
            sort.Add(Medico.Properties.Name, SortingDirection.Asc);
            readCmd.Sort = sort;

            return readCmd.Execute();
        }
        #endregion

        #region MedicoActualizacion
        /// <summary>
        /// Actualizo la Fecha de Última Actualización de un Médico
        /// </summary>
        /// <param name="mac">Actualización del Médico</param>
        [Private]
        public MedicoActualizacion MedicoActualizacionUpdate(MedicoActualizacion mac)
        {

            if (!(mac.Id > 0))
            {
                MedicoActualizacion macInternal = MedicoActualizacionReadByMedico(mac.MedicoId);

                if (macInternal != null && macInternal.Id > 0)
                {
                    mac.Id = macInternal.Id;
                }
            }


            return dalEngine.Update<MedicoActualizacion>(mac);
        }

        public void MedicoActualizacionDelete(int medicoId)
        {
            MedicoActualizacion ma = MedicoActualizacionReadByMedico(medicoId);

            if (ma != null)
            {
                dalEngine.Delete(ma);
            }
        }


        /// <summary>
        /// Obtengo la Fecha de Última Actualización de un Médico
        /// </summary>
        /// <param name="medicoId">Id del Médico</param>
        /// <returns>Fecha de Última Actualización de un Médico</returns>
        public MedicoActualizacion MedicoActualizacionReadByMedico(int medico)
        {
            return dalEngine.GetByProperty<MedicoActualizacion>(MedicoActualizacion.Properties.MedicoId, medico);
        }
        #endregion

        #region MedicoServicio
        /// <summary>
        /// Obtengo todos los MedicoServicio de un Medico
        /// </summary>
        /// <param name="medicoId">Id del Médico</param>
        /// <returns>Todos los MedicoServicio del Médico</returns>
        public EntityCollection<MedicoServicio> MedicoServicioReadByMedico(int medico)
        {
            return MedicoServicioReadByMedico(medico, true);
        }

        internal EntityCollection<MedicoServicio> MedicoServicioReadByMedico(int medico, bool cargarObjetos)
        {
            EntityCollection<MedicoServicio> servicios = dalEngine.GetManyByProperty<MedicoServicio>(MedicoServicio.Properties.MedicoId, medico, MedicoServicio.Properties.ServicioId);
            if (cargarObjetos)
                CargarObjetos(servicios);
            return servicios;
        }

        private void CargarObjetos(EntityCollection<MedicoServicio> servicios)
        {
            ServiciosDalc ServiciosDalc = Context.Session.ServiciosDalc;
            EntityCollection<Servicio> serv = dalEngine.GetAll<Servicio>();

            // Obtengo los Objetos
            foreach (MedicoServicio servicio in servicios)
                if (servicio.Servicio == null)
                    servicio.Servicio = serv.FindByKey(servicio.ServicioId);
        }

        /// <summary>
        /// Obtengo un MedicoServicio
        /// </summary>
        /// <param name="medicoId">Id del Médico</param>
        /// <param name="servicioId">Id del Servicio</param>
        /// <returns>El MedicoServicio correspondiente al Médico y el Servicio</returns>
        public MedicoServicio MedicoServicioReadByMedicoAndServicio(int medico, int servicio)
        {
            ReadManyCommand<MedicoServicio> readCmd = new ReadManyCommand<MedicoServicio>(dalEngine);

            Filter filter = new Filter();
            filter.Add(MedicoServicio.Properties.MedicoId,
                "=", medico);
            filter.Add(BooleanOp.And, MedicoServicio.Properties.ServicioId,
                "=", servicio);
            readCmd.Filter = filter;

            EntityCollection<MedicoServicio> servicios = readCmd.Execute();

            if (servicios.Count > 0)
                return servicios[0];
            else
                return null;
        }

        /// <summary>
        /// Actualizo una Colección de MedicoServicio
        /// </summary>
        /// <param name="servicios">Colección de MedicoServicio a Actualizar</param>
        /// <returns>Colección de MedicoServicio Actualizados</returns>
        [RequiresTransaction]
        public virtual EntityCollection<MedicoServicio> MedicoServicioUpdateMany(EntityCollection<MedicoServicio> servicios)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            // Seteo Fecha y Usuario de Actualizacion
            foreach (MedicoServicio servicio in servicios)
            {
                servicio.User = user.Id;
                servicio.Date = enfoke.Time.Now;
            }

            // Actualizo
            servicios = dalEngine.UpdateCollection<MedicoServicio>(servicios);

            CargarObjetos(servicios);
            return servicios;
        }

        /// <summary>
        /// Creo un MedicoServicio en base a un Medico y un Servicio
        /// </summary>
        /// <param name="medicoId">Medico para el MedicoServicio</param>
        /// <param name="servicioId">Servicio para el MedicoServicio</param>
        /// <returns>El MedicoServicio Nuevo</returns>
        public MedicoServicio MedicoServicioCreate(Medico medico, Servicio servicio)
        {
            MedicoServicio ms = new MedicoServicio();
            ms.Servicio = servicio;
            ms.ServicioId = servicio.Id;
            ms.Medico = medico;
            ms.MedicoId = medico.Id;
            ms.PorcentajeRecuperoHonorariosOS = 0;
            ms.PorcentajeRecuperoHonorariosExternos = 0;

            return ms;
        }

        /// <summary>
        /// Elimino una Colección de MedicoServicio
        /// </summary>
        /// <param name="servicios">Colección de MedicoServicio a Eliminar</param>
        [RequiresTransaction]
        public virtual void MedicoServicioDeleteMany(EntityCollection<MedicoServicio> servicios)
        {

            // Elimino
            dalEngine.Delete(servicios);
        }
        #endregion

        #region MedicoLight

        public ReadAllCollection<MedicoLight> MedicoLightReadAll()
        {
            return new ReadAllCollection<MedicoLight>(dalEngine.GetManyByProperty<MedicoLight>(MedicoLight.Properties.Activo, true, new enfoke.Data.Reference.IPropertyReference[] { MedicoLight.Properties.Apellido, MedicoLight.Properties.Name }));
        }

        public ReadAllCollection<MedicoLight> MedicoLightReadAllHasPracticas()
        {
            return MedicoLightReadByNameAndMatricula(string.Empty, string.Empty, ReemplazoProfesionalAgendaEnum.AReemplazar);
        }

        public ReadAllCollection<MedicoLight> MedicoLightReadAllTecnicos()
        {
            return MedicoLightReadByNameAndMatricula(string.Empty, string.Empty, ReemplazoProfesionalAgendaEnum.Tecnico);
        }

        public EntityCollection<MedicoLight> MedicoLightReadByNameAndTipo(string name, bool traerActuantes, bool traerInformantes, bool traerTecnicos)
        {
            // ACTUANTES e INFORMANTES
            EntityCollection<MedicoLight> medicos = new EntityCollection<MedicoLight>();
            if (traerInformantes || !traerActuantes)
            {
                var actuanteseInformantes = from med in dalEngine.Query<MedicoLight>()
                                            join medpra in dalEngine.Query<MedicoPractica>() on med.Id equals medpra.MedicoId
                                            where med.Activo
                                            select med;

                if (!String.IsNullOrWhiteSpace(name))
                    actuanteseInformantes = actuanteseInformantes.Where(med => (med.Apellido.Contains(name) || med.Name.Contains(name) || med.MatriculaNacional.Contains(name) || med.MatriculaProvincial.Contains(name)));
                if (traerInformantes && !traerActuantes)
                    actuanteseInformantes = actuanteseInformantes.Where(med => med.IsInformante);

                medicos = actuanteseInformantes.Distinct().ToEntityCollection();
            }

            // TECNICOS
            if (traerTecnicos)
            {
                var queryTecnicos = from med in dalEngine.Query<MedicoLight>()
                                    where med.Activo == true
                                    && (String.IsNullOrWhiteSpace(name) || med.Apellido == "'%" + name + "%'" || med.Name == "'%" + name + "%'")
                                    && med.IsTecnico
                                    select med;
                EntityCollection<MedicoLight> tecnicos = queryTecnicos.ToEntityCollection();
                foreach (MedicoLight tecnico in tecnicos)
                {
                    if (!medicos.Contains(tecnico))
                        medicos.Add(tecnico);
                }
            }

            return medicos;
        }




        public ReadAllCollection<MedicoLight> MedicoLightReadByNameAndMatricula(string name, string matricula, ReemplazoProfesionalAgendaEnum tipoMedico)
        {
            return MedicoLightReadByNameMatriculaAndMedicoId(name, matricula, tipoMedico, null, false);
        }

        /// <summary>
        /// Busca todos los médicos segun los parametros
        /// </summary>
        /// <param name="name">Busca en el apellido y nombre del médico (LIKE).</param>
        /// <param name="matricula">Busca en la matricula del médico (LIKE).</param>
        /// <param name="tipoMedico">Busca por el tipoDeMedico. Solo para reemplazo de profesional en agenda está pensado.
        /// Los tipos son: AReemplazar, Actuante, Informante, Técnico.</param>
        /// <param name="medicoAReemplazar">Id del medicoId del que se quieren buscar el/los servicios de las practicas que tiene asociadas.
        /// Si se pasa como null, no se tiene en cuenta éste parametro de busqueda.</returns>
        public ReadAllCollection<MedicoLight> MedicoLightReadByNameMatriculaAndMedicoId(string name, string matricula, ReemplazoProfesionalAgendaEnum tipoMedico, int? medicoAReemplazar, bool traerMedicoNoEspecificado)
        {
            string medicoSearch = name.Trim().Replace(" ", "%") + "%";
            string matriculaSearch = matricula.Trim().Replace(" ", "%") + "%";

            string hql = "SELECT DISTINCT m FROM MedicoLight m ";

            if (tipoMedico != ReemplazoProfesionalAgendaEnum.Tecnico)
            {
                hql += ", MedicoPractica mpReemplazo ";

                // Para buscar los medicos que poseen practicas en los mismos servicios de las practicas del medicoId a reemplazar.
                if (medicoAReemplazar.HasValue)
                    hql += ", MedicoPractica mpAReemplazar ";
            }

            hql += "WHERE m.Activo = true " +
                "AND (m.Name LIKE :medico " +
                "OR m.Apellido LIKE :medico) " +
                "AND (m.MatriculaNacional LIKE :matriculaN OR m.MatriculaProvincial LIKE :matriculaP ) ";

            if (tipoMedico != ReemplazoProfesionalAgendaEnum.Tecnico)
            {
                hql += "AND m.Id = mpReemplazo.MedicoId ";

                if (medicoAReemplazar.HasValue)
                    hql += "AND mpAReemplazar.Practica.ServicioEspecialidad.Servicio = mpReemplazo.Practica.ServicioEspecialidad.Servicio ";
            }

            switch (tipoMedico)
            {
                case ReemplazoProfesionalAgendaEnum.AReemplazar:
                    break;
                case ReemplazoProfesionalAgendaEnum.Actuante:
                    if (medicoAReemplazar.HasValue)
                        hql += "AND mpAReemplazar.MedicoId = :medicoAReemplazar ";
                    break;
                case ReemplazoProfesionalAgendaEnum.Informante:
                    if (medicoAReemplazar.HasValue)
                        hql += "AND mpAReemplazar.MedicoId = :medicoAReemplazar ";
                    hql += "AND m.IsInformante = true ";
                    break;
                case ReemplazoProfesionalAgendaEnum.Tecnico:
                    hql += "AND m.IsTecnico = true ";
                    break;
                default:
                    break;
            }

            hql += "ORDER BY m.Apellido ASC, m.Name ASC, m.MatriculaNacional ASC, m.MatriculaProvincial ASC ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("medico", medicoSearch);
            query.SetParameter("matriculaN", matriculaSearch);
            query.SetParameter("matriculaP", matriculaSearch);
            switch (tipoMedico)
            {
                case ReemplazoProfesionalAgendaEnum.AReemplazar:
                    break;
                case ReemplazoProfesionalAgendaEnum.Actuante:
                case ReemplazoProfesionalAgendaEnum.Informante:
                    if (medicoAReemplazar.HasValue)
                        query.SetParameter("medicoAReemplazar", medicoAReemplazar.Value);
                    break;
                case ReemplazoProfesionalAgendaEnum.Tecnico:
                    break;
                default:
                    break;
            }

            EntityCollection<MedicoLight> col = dalEngine.GetManyByQuery<MedicoLight>(query);
            if (traerMedicoNoEspecificado)
                col.Add(dalEngine.GetById<MedicoLight>(-1));

            return new ReadAllCollection<MedicoLight>(col);
        }

        #endregion

        #region Medico - Turno

        public MedicoName MedicoPrincipalReadByTurno(int turnoId)
        {
            string hql = "SELECT DISTINCT m FROM TurnoLight t, MedicoName m, PracticaTurno pt " +
                    "WHERE pt.TurnoId = :turnoId " +
                    "AND m.Id = pt.Medico.Id " +
                    "AND pt.Tipo = :tipo";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("tipo", (int)PracticaTurnoTipoEnum.Principal);
            query.SetInt32("turnoId", turnoId);

            return dalEngine.GetByQuery<MedicoName>(query);
        }

        [Private]
        public EntityCollection<MedicoDerivanteTurno> MedicoDerivanteReadByMedicoServicioAndFecha(int medicoId, int? servicioId, DateTime fechaDesde, DateTime fechaHasta)
        {
            string hql = "SELECT new enfoke.Eges.Entities.Results.MedicoDerivanteTurno( " +
                                " ser, tur, pac, vli.ImporteDerechos, vli.ImporteHonorarios, vli.ImporteModulo, vli.ImporteInsumos, vli.ImporteDerechosDif, vli.ImporteHonorariosDif, vli.ImporteModuloDif, vli.ImporteInsumosDif, " +
                                " vli.PracticaTurno.Practica.Name, est.Name, ord.Protocolo) " +
                    "FROM ServicioName ser, Equipo equ, TurnoLight tur, Orden ord LEFT JOIN ord.Protocolo, PacienteLight pac, ValorizacionItem vli, EstadoTurno est, TipoTurno tpt " +
                    "WHERE ser.Id = equ.Servicio.Id AND equ.Id = tur.EquipoId AND tur.Orden.PacienteId = pac.Id AND vli.Valorizacion.Turno.Id = tur.Id " +
                    "AND tur.Orden.Id = ord.Id " +
                    "AND vli.Valorizacion.Tipo.Id = :facturacion " + // Val facturacion
                    "AND vli.Valorizacion.Deleted = false AND vli.Cantidad > 0 " + // Valorizacion no eliminada cant > 0
                    "AND est.Id = tur.EstadoTurnoID AND est.Atendido = true AND tpt.Id = tur.TipoTurnoID AND tpt.EsFacturable = true " + // Turno atendido
                    "AND tur.Orden.MedicoSolicitanteID = :medicoId AND tur.Fecha < :fechaHasta AND tur.Fecha > :fechaDesde ";
            if (servicioId.HasValue)
                hql += " AND ser.Id = :servicioId ";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetInt32("facturacion", (int)ValorizacionTiposEnum.Prefacturacion);
            query.SetInt32("medicoId", medicoId);
            query.SetParameter("fechaDesde", fechaDesde.Date);
            query.SetParameter("fechaHasta", fechaHasta.Date.AddDays(1));

            if (servicioId.HasValue)
                query.SetInt32("servicioId", servicioId.Value);

            return dalEngine.GetManyByQuery<MedicoDerivanteTurno>(query);
        }

        #endregion

        public EntityCollection<ExcepcionHorarioMedicoEquipo> ExcepcionHorarioMedicoEquipoReadByEquipo(int equipoId, bool soloVigentes)
        {
            Filter filter = new Filter();

            filter.Add(ExcepcionHorarioMedicoEquipo.Properties.Equipo.Id, " = ", equipoId);
            if (soloVigentes)
            {
                filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.Horario.FechaFin, " >= ", enfoke.Time.Now.Date.AddMinutes(-1)); // Justo cuando termino ayer
                filter.Add(BooleanOp.And, ExcepcionHorarioMedicoEquipo.Properties.Horario.FechaInicio, " < ", enfoke.Time.Now.Date.AddDays(1).AddMinutes(1)); // Justo cuando empieza mañana
            }

            ReadManyCommand<ExcepcionHorarioMedicoEquipo> readCmd = new ReadManyCommand<ExcepcionHorarioMedicoEquipo>(dalEngine);
            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /*
         * Devuelve todos los medicos tal que en la fecha actual, poseen el nivel pasado por parámetro
         */
        public EntityCollection<MedicoEquipoHorario> MedicoEquipoHorarioReadByNivelMedico(NivelMedico nivel)
        {
            DateTime getDate = enfoke.Time.Now;
            Filter filter = new Filter();
            filter.Add(MedicoEquipoHorario.Properties.NivelMedico.Id, "=", nivel.Id);
            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Horario.FechaInicio, "<=", getDate);
            filter.Add(BooleanOp.And, MedicoEquipoHorario.Properties.Horario.FechaFin, ">=", getDate);

            return dalEngine.GetManyByFilter<MedicoEquipoHorario>(filter);
        }



        public EntityCollection<MedicoAsociacion> MedicoAsociacionReadByNameOrMatricula(string name)
        {
            var query = from mea in dalEngine.Query<MedicoAsociacion>()
                        where (mea.LastName.Contains(name)
                                  || mea.FirstName.Contains(name)
                                  || mea.MatriculaEspecialidad.Contains(name)
                                  || mea.MatriculaNacional.Contains(name)
                                  || mea.MatriculaProvincial.Contains(name))
                                  && (mea.Deleted == null || !mea.Deleted.Value)
                        select mea;

            return query.ToEntityCollection();
        }
    }
}

