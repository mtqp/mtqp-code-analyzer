using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;

using enfoke.Data.Filters;
using enfoke.AOP;
using NHibernate;
using enfoke.Eges.Utils;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class MantenimientoDalc : Dalc, IService
    {
        protected MantenimientoDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<LogEntidadItems> LogEntidadItemsReadByTipoEntidadIdAndDates(Type tipo, int id, DateTime fechaDesde, DateTime fechaHasta)
        {
            LogEntidad entidadLogueable = LogEntidadReadByType(tipo.ToString());

            ReadManyCommand<LogEntidadItems> readCmd = new ReadManyCommand<LogEntidadItems>(dalEngine);
            Filter filter = new Filter();
            filter.Add(LogEntidadItems.Properties.EntityType.Id, "=", entidadLogueable.Id);
            filter.Add(BooleanOp.And, LogEntidadItems.Properties.EntityId, "=", id);
            filter.Add(BooleanOp.And, LogEntidadItems.Properties.Date, ">", fechaDesde.AddDays(-1));
            filter.Add(BooleanOp.And, LogEntidadItems.Properties.Date, "<", fechaHasta.AddDays(1));

            readCmd.Filter = filter;
            EntityCollection<LogEntidadItems> items = readCmd.Execute();

            // Orden por fecha descendiente
            items.Sort(new Comparison<LogEntidadItems>(delegate(LogEntidadItems left, LogEntidadItems right)
            {
                return right.Date.CompareTo(left.Date);
            }));

            return items;

        }

        private LogEntidad LogEntidadReadByType(string type)
        {
            return dalEngine.GetByProperty<LogEntidad>(LogEntidad.Properties.Type, type); 
        }

        public LogEntidad LogEntidadReadByType(Type type)
        {
            return dalEngine.GetByProperty<LogEntidad>(LogEntidad.Properties.Type, type.ToString());
        }

        [Private]
        public void LogueaReasignacionTurnos(EntityCollection<Turno> turnos, MedicoAsociacion medico, int? medicoReasignadoId)
        {
            MedicoAsociacion medicoReemplazante = Context.Session.MedicosDalc.MedicoAsociacionReadById(medicoReasignadoId.Value);

            string turnosInfo = String.Empty;
            foreach (Turno turno in turnos)
            {
                if(turno.Orden.Paciente == null)
                    turno.Orden.Paciente = Context.Session.TurnosDalc.PacienteReadById(turno.Orden.PacienteId);

                turnosInfo += String.Format("Fecha: {0}\rPaciente: {1}\rProtocolo: {2}\r\n\r\n", (turno.Fecha.HasValue ? DateTimeUtils.FormatDateTime(turno.Fecha.Value) : "[Sin Fecha]"), turno.Orden.Paciente.ApellidoNombre, turno.Orden.Protocolo != null ? turno.Orden.Protocolo.ProtocoloFull : "-");

                // Lo parto aca para que no se vaya mas del max de la columna. 
                if (turnosInfo.Length > 2000)
                {
                    enfoke.Log.Record.LogEntity(medico, "Reasignaci?n", String.Format("El m?dico ha sido eliminado. Sus turnos fueron reasignados a {0}. A continuaci?n el detalle de los mismos:\r\n{1}", medicoReemplazante.FullName, turnosInfo));
                    enfoke.Log.Record.LogEntity(medicoReemplazante, "Reasignaci?n", String.Format("Los turnos del m?dico {0} se le han sido asignados a causa de su eliminaci?n. A continuaci?n el detalle de los mismos:\r\n{1}", medico.FullName, turnosInfo));
                    turnosInfo = String.Empty;
                }
            }

            if (!String.IsNullOrEmpty(turnosInfo))
            {
                enfoke.Log.Record.LogEntity(medico, "Reasignaci?n", String.Format("El m?dico ha sido eliminado. Sus turnos fueron reasignados a {0}. A continuaci?n el detalle de los mismos:\r\n{1}", medicoReemplazante.FullName, turnosInfo));
                enfoke.Log.Record.LogEntity(medicoReemplazante, "Reasignaci?n", String.Format("Los turnos del m?dico {0} se le han sido asignados a causa de su eliminaci?n. A continuaci?n el detalle de los mismos:\r\n{1}", medico.FullName, turnosInfo));
            }
        }

    }
}
