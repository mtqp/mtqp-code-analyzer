using enfoke.AOP;
using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;

using NHibernate;

namespace enfoke.Eges.Data
{
    public class RegistrosDalc : Dalc, IService
    {
        protected RegistrosDalc(NotConstructable dummy) : base(dummy) { }

        [Private]
        public EntityCollection<LogSeguridad> GetLoginsSince(DateTime desde)
        {
            Filter f = new Filter();
            f.Add(LogSeguridad.Properties.Fecha, ">=", desde);
            f.Add(BooleanOp.And, LogSeguridad.Properties.Evento, "=", "Login");
            Sort sort = new Sort(new SortItem(LogSeguridad.Properties.Fecha, SortingDirection.Desc));
            return dalEngine.GetManyByFilter<LogSeguridad>(f, sort);
        }
    }
}
