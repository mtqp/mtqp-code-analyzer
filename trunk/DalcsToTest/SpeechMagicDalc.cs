using enfoke.AOP;
using enfoke.Connector;
using enfoke.Eges.Entities;
using System;
using enfoke.Eges.Persistence;
using NHibernate;
using System.Collections.Generic;

namespace enfoke.Eges.Data
{
    public class SpeechMagicDalc : Dalc, IService
    {
        protected SpeechMagicDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<SpeechMagicLog> SpeechMagicLogLeerAgrupadoPorMesUsuarioPuesto(bool mesAnterior)
        {
            List<string> periodos = new List<string>();
            DateTime fechaReferencia = enfoke.Time.Now;
            if (mesAnterior) fechaReferencia = fechaReferencia.AddMonths(-1);
            periodos.Add(GetPeriodoPorFecha(fechaReferencia));

            string hql = "select new SpeechMagicLog(l.Periodo, l.Subred, l.PcCliente, l.Usuario, "
                            +" count(l.Id), sum(l.Duracion)) "
            + " from SpeechMagicLog l where l.Periodo in (:periodos) and l.SessionId != 'Sincronización' "
            + " group by l.Periodo, l.Subred, l.PcCliente, l.Usuario";

            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameterList("periodos", periodos);

            EntityCollection<SpeechMagicLog> ret = dalEngine.GetManyByQuery<SpeechMagicLog>(query);
            
            // Completa las fechas de actualización
            foreach (SpeechMagicLog log in ret)
            {
                string hqlActualizacion = "from SpeechMagicLog l where l.PcCliente = :pc and l.Usuario = :usuario "
                            + " and l.SessionId = 'Sincronización' order by l.FechaGrabacion desc";
                IQuery queryAdaptacion = dalEngine.CreateQuery(hqlActualizacion);
                queryAdaptacion.SetParameter("pc", log.PcCliente);
                queryAdaptacion.SetParameter("usuario", log.Usuario);
                queryAdaptacion.SetMaxResults(1);
                SpeechMagicLog ultimaActualizacion = queryAdaptacion.UniqueResult<SpeechMagicLog>();
                if (ultimaActualizacion != null)
                {
                    log.UltimaActualizacion = ultimaActualizacion.FechaGrabacion;
                    log.ResultadoUltimaActualizacion = ultimaActualizacion.Adaptacion;
                }
            }
            //
            return ret;
        }

        private string GetPeriodoPorFecha(DateTime dateTime)
        {
            string ret = dateTime.Year.ToString();
            int mes = dateTime.Month;
            if (mes < 10) ret += "0";
            return ret + mes.ToString();
        }
        [Private]
        public void SpeechMagicLogUpdate(SpeechMagicLog log)
        {
            try
            {
                dalEngine.Update(log);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public void SpeechMagicLogUpdateHoraEncolado(string sessionId)
        {
            SpeechMagicLog logItem = dalEngine.GetByProperty<SpeechMagicLog>(SpeechMagicLog.Properties.SessionId, sessionId);
            if (logItem == null) return;
            logItem.FechaEncolado = enfoke.Time.Now;
            dalEngine.Update(logItem);
        }
        public void SpeechMagicLogUpdateEstadoAdaptation(string sessionId, string estado)
        {
            if (estado != null && estado.Length > 128) estado = estado.Substring(0, 128);
            SpeechMagicLog logItem = dalEngine.GetByProperty<SpeechMagicLog>(SpeechMagicLog.Properties.SessionId, sessionId);
            if (logItem == null) return;
            logItem.Adaptacion = estado;
            dalEngine.Update(logItem);
        }
    }
}

