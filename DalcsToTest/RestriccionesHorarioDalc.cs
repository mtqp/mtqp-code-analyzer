using enfoke.Connector;
using System;
using enfoke.Eges.Persistence;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistance;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using System.Collections.Generic;
using NHibernate;
using enfoke.Data;
using System.Linq;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    /// <summary>
    /// Maneja los datos concernientes a las obras sociales
    /// </summary>
    public class RestriccionesHorarioDalc : Dalc, IService
    {
        protected RestriccionesHorarioDalc(NotConstructable dummy) : base(dummy) { }

        [Private]
        public EntityCollection<RestriccionHorario> RestriccionHorarioReadByEquipoMedico(
            Equipo equipo, Medico medico)
        {
            string query = "select restriccion from RestriccionHorario restriccion  " +
                                   "where 1=1 ";
            if (medico != null)
                query += "AND (restriccion.MedicoId = null OR restriccion.MedicoId = :medicoId) ";

            if (equipo != null)
                query +=
                                   "AND EXISTS (select restriccionEquipo from " +
                                        "RestriccionHorarioEquipo restriccionEquipo " +
                                        "WHERE restriccion = restriccionEquipo.Restriccion " +
                                        " AND (restriccionEquipo.Equipo.Id = null OR restriccionEquipo.Equipo.Id = (:equipoId))) ";
            query += "order by restriccion.Horario.FechaInicio";
            
            IQuery consulta = dalEngine.CreateQuery(query);
            if (equipo != null)
                consulta.SetParameter("equipoId", equipo.Id);
            if (medico != null)
                consulta.SetParameter("medicoId", medico.Id);
            // Le carga las colecciones hijas
            EntityCollection<RestriccionHorario> ret = dalEngine.GetManyByQuery<RestriccionHorario>(consulta);
            if (ret.Count > 0)
                LlenaColeccionesRestriccion(ret);
            return ret;
        }

        private void LlenaColeccionesRestriccion(EntityCollection<RestriccionHorario> ret)
        {
            EntityCollection<RestriccionHorarioPractica> cPracticas = dalEngine.GetManyByPropertyList<RestriccionHorarioPractica>
                                                    (RestriccionHorarioPractica.Properties.Restriccion.Id, ret.GetIds());
            foreach (RestriccionHorarioPractica cPractica in cPracticas)
            {
                RestriccionHorario horario = ret.FindByKey(cPractica.Restriccion.Id);
                horario.Practicas.Add(cPractica);
            }
            EntityCollection<RestriccionHorarioAdicional> cAdicionales = dalEngine.GetManyByPropertyList<RestriccionHorarioAdicional>
                                                    (RestriccionHorarioAdicional.Properties.Restriccion.Id, ret.GetIds());
            foreach (RestriccionHorarioAdicional cAdicional in cAdicionales)
            {
                RestriccionHorario horario = ret.FindByKey(cAdicional.Restriccion.Id);
                horario.Adicionales.Add(cAdicional);
            }
            EntityCollection<RestriccionHorarioEquipo> cEquipos = dalEngine.GetManyByPropertyList<RestriccionHorarioEquipo>
                                                (RestriccionHorarioEquipo.Properties.Restriccion.Id, ret.GetIds());
            foreach (RestriccionHorarioEquipo cEquipo in cEquipos)
            {
                RestriccionHorario horario = ret.FindByKey(cEquipo.Restriccion.Id);
                horario.Equipos.Add(cEquipo);
            }
        }

        [RequiresTransaction]
        public virtual void RestriccionHorarioDelete(RestriccionHorario restriccion)
        {
            // Borra previas
            BorraDependenciasRestriccion(restriccion);
            // Borra cabecera
            dalEngine.Delete(restriccion);
        }

        [RequiresTransaction]
        public virtual void RestriccionHorarioUpdate(RestriccionHorario restriccion)
        {
            // Graba cabecera
            dalEngine.Update(restriccion.Horario);
            dalEngine.Update(restriccion);
            // Borra previas
            BorraDependenciasRestriccion(restriccion);
            // Graba actuales
            if (restriccion.Equipos.Count == 0)
                throw new enfokeValidationException("No es válido grabar restricciones sin equipos. Si no hubiera indicaciones de equipo de insertarse un RestriccionHorarioEquipo con Equipo nulo.");
            if (restriccion.Practicas.Count == 0)
                throw new enfokeValidationException("No es válido grabar restricciones sin prácticas. Si no hubiera indicaciones de equipo de insertarse un RestriccionHorarioPractica con Práctca nulo.");
            // Se asegura que los padres estén bien
            ActualizaParentDeColeccionesYBorraId(restriccion);
            // Graba
            dalEngine.UpdateCollection(restriccion.Practicas);
            dalEngine.UpdateCollection(restriccion.Adicionales);
            dalEngine.UpdateCollection(restriccion.Equipos);
        }

        private static void ActualizaParentDeColeccionesYBorraId(RestriccionHorario restriccion)
        {
            foreach (RestriccionHorarioPractica practica in restriccion.Practicas)
            {
                practica.Restriccion = restriccion;
                practica.Id = 0;
            }
            foreach (RestriccionHorarioAdicional adicional in restriccion.Adicionales)
            {
                adicional.Restriccion = restriccion;
                adicional.Id = 0;
            }
            foreach (RestriccionHorarioEquipo equipos in restriccion.Equipos)
            {
                equipos.Restriccion = restriccion;
                equipos.Id = 0;
            }
        }

        private void BorraDependenciasRestriccion(RestriccionHorario restriccion)
        {
            // Borra los equipos preexistentes
            EntityCollection<RestriccionHorarioEquipo> equipos =
                dalEngine.GetManyByProperty<RestriccionHorarioEquipo>(RestriccionHorarioEquipo.Properties.Restriccion.Id, restriccion.Id);
            dalEngine.Delete(equipos);
            // Borra las prácticas preexistentes
            EntityCollection<RestriccionHorarioPractica> practicas =
                dalEngine.GetManyByProperty<RestriccionHorarioPractica>(RestriccionHorarioPractica.Properties.Restriccion.Id, restriccion.Id);
            dalEngine.Delete(practicas);
            // Borra las adicionales preexistentes
            EntityCollection<RestriccionHorarioAdicional> adicionales =
                dalEngine.GetManyByProperty<RestriccionHorarioAdicional>(RestriccionHorarioAdicional.Properties.Restriccion.Id, restriccion.Id);
            dalEngine.Delete(adicionales);
        }

        public EntityCollection<RestriccionHorarioEquipoMedico> RestriccionHorarioReadByEquiposAndMedicos(DateTime dateFrom, DateTime dateTo,
                            IList<Equipo> equipos, EntityCollection<Medico> medicos, IList<Practica> practicas, IList<PracticaAdicional> practicasRelacionadas)
        {
            // Convierte a Id
            List<int> practicasRelacionadasId;
            List<int> equiposId;
            List<int> medicosId;
            ConvierteListaIds(equipos, medicos, practicasRelacionadas, out equiposId, out medicosId, out practicasRelacionadasId);
            // Hace la consulta
            string query = "select new enfoke.Eges.Entities.Results.RestriccionHorarioEquipoMedico( " +
                                  "     restriccion.MedicoId," +
                                       " restriccionEquipo.Equipo.Id, " +
                                       " restriccion.Horario " +
                                   ") from RestriccionHorario restriccion,  " +
                                   "RestriccionHorarioPractica restriccionPractica , " +
                                   "RestriccionHorarioEquipo restriccionEquipo " +
                                   "where " +
                                   " restriccion = restriccionPractica.Restriccion " +
                                   " AND restriccion = restriccionEquipo.Restriccion " +
                                   " AND (restriccionEquipo.Equipo.Id is null OR restriccionEquipo.Equipo.Id in (:equiposId)) " +
                                   " AND (restriccion.MedicoId is null OR restriccion.MedicoId in (:medicosId)) " +
                                   " AND (restriccionPractica.Practica.Id is null OR restriccionPractica.Practica.Id in (:practicasId)) " +
                // Filtros de fecha
                                   " AND restriccion.Horario.FechaInicio < :dateTo " +
                                   " AND restriccion.Horario.FechaFin > :dateFrom ";
            query = AgregaFiltrosAdicionales(practicasRelacionadasId, query);
            query += "order by restriccionEquipo.Equipo.Id, restriccion.MedicoId";

            IQuery consulta = dalEngine.CreateQuery(query);
            consulta.SetCacheable(true);
            consulta.SetParameter("dateTo", dateTo.AddDays(1));
            consulta.SetParameter("dateFrom", dateFrom.AddDays(-1));
            consulta.SetParameterList("equiposId", equiposId);
            consulta.SetParameterList("medicosId", medicosId);
            consulta.SetParameterList("practicasId", new EntityCollection<Practica>(practicas).GetIds());
            AgregaParametrosAdicionales(practicasRelacionadasId, consulta);
            return new EntityCollection<RestriccionHorarioEquipoMedico>(consulta.List<RestriccionHorarioEquipoMedico>());
        }

        private static void AgregaParametrosAdicionales(List<int> practicasRelacionadasId, IQuery consulta)
        {
            if (practicasRelacionadasId.Count == 0)
                practicasRelacionadasId.Add(0);
            consulta.SetParameterList("practicasRelacionadasId", practicasRelacionadasId);
        }

        private string AgregaFiltrosAdicionales(List<int> practicasRelacionadasId, string query)
        {
            if (practicasRelacionadasId.Count == 0)
                practicasRelacionadasId.Add(0);
            // Query de prácticas adicionales
            //
            // La lógica es la siguiente:
            // - cuenta cuántas prácticasAdicionales de la restricción no están
            // en las praticas actuales.
            // - ese valor puede dar 0 (todas están) o más que 0.
            // a) si la restricción es de tipo 'debe tener', se tiene que quedar
            // con los casos que dieron cero.
            // b) si la restricción es de tipo 'no debe tener', se tiene que
            // quedar con los casos que dieron más de cero.
            // Para hacer eso en una sola consulta, en el caso (b) le resta
            // 1 (pasando a ser valores entre -1 y n) y le cambia el signo.
            // De esa forma deja al 0 (convertido a -1, luego a +1) fuera del
            // rango de '< 1' que es la condición que aplica.
            query += " AND (restriccion.InclusionAdicionalesDb = 2 OR " +
                        "((select count(practicasAdicionales) from RestriccionHorarioAdicional practicasAdicionales " +
                                    " where practicasAdicionales.Restriccion = restriccion " +
                                    " AND practicasAdicionales.PracticaAdicional.Id not in (:practicasRelacionadasId)) " +
                            " + restriccion.InclusionAdicionalesDb - 1) * " +
                                "(restriccion.InclusionAdicionalesDb * 2 - 1) < 1) ";
            return query;
        }

        private static void ConvierteListaIds(IList<Equipo> equipos, EntityCollection<Medico> medicos, IList<PracticaAdicional> practicasRelacionadas, out List<int> equiposId, out List<int> medicosId, out List<int> practicasRelacionadasId)
        {
            practicasRelacionadasId = new List<int>();
            if (practicasRelacionadas != null)
                foreach (PracticaAdicional practica in practicasRelacionadas)
                    practicasRelacionadasId.Add(practica.Adicional.Id);
            equiposId = new List<int>();
            if (equipos != null)
                foreach (Equipo equipo in equipos)
                    equiposId.Add(equipo.Id);
            medicosId = new List<int>();
            if (medicos != null)
                foreach (Medico medico in medicos)
                    medicosId.Add(medico.Id);
        }
        
        public DatosHorarioRestricciones DatosRestriccionesLeer(Equipo equipo, Medico medico)
        {
            DatosHorarioRestricciones ret = new DatosHorarioRestricciones();
            if (equipo == null)
            {   // Tiene médico
                ret.Practicas = new EntityCollection<Practica>((from medicoPractica in
                                                                    Context.Session.MedicosDalc.MedicoPracticaReadByMedico(medico.Id)
                                                                where medicoPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.Adicional
                                                                && medicoPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.SetFarmacia
                                                                && medicoPractica.Practica.Deleted == false
                                                                orderby medicoPractica.Practica.Name
                                                                select medicoPractica.Practica).ToList<Practica>());
                ret.Equipos = (from equipoLista in Context.Session.EquiposDalc.EquipoReadByMedico(medico.Id)
                              orderby equipoLista.Sucursal.Name, equipoLista.Servicio.Name, equipoLista.Descripcion
                              select equipoLista).ToEntityCollection<Equipo>();
            }
            else
            {
                ret.Practicas = new EntityCollection<Practica>((from equipoPractica in
                                                                    Context.Session.EquiposDalc.EquipoPracticaReadByEquipoId(equipo.Id)
                                                                where equipoPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.Adicional
                                                                && equipoPractica.Practica.TipoPractica.Id != (int)TipoPracticaEnum.SetFarmacia
                                                                && equipoPractica.Practica.Deleted == false
                                                                orderby equipoPractica.Practica.Name
                                                                select equipoPractica.Practica).ToList<Practica>());
                ret.Equipos = null;
            }
            if (ret.Practicas.Count > 0)
                ret.Adicionales = Context.Session.PracticasDalc.PracticaAdicionalPracticaReadByPracticasId(ret.Practicas.GetIds());
            else
                ret.Adicionales = new EntityCollection<Practica>();
            return ret;
        }
    }
}
