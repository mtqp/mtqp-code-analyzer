using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using enfoke.Eges;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;

using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Reserva;
using enfoke.Eges.Security;
using enfoke.Eges.Utils;
using enfoke.Data.Filters;
using enfoke.AOP;
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class CartaDocumentoDalc : Dalc, IService
    {
        protected CartaDocumentoDalc(NotConstructable dummy) : base(dummy) { }

        /// <summary>
        /// [CB] Trae todos las Cartas Documento que cumplan con los parametros.
        /// </summary>
        /// <param name="fechaDesde">Fecha de inicio del periodo a buscar</param>
        /// <param name="fechaHasta">Fecha de finalización del periodo a buscar</param>
        /// <param name="usuario">Usuario de creación</param>
        /// <param name="remitente">Nombre del Remitente</param>
        /// <param name="destinatario">Nombre del Destinatario</param>
        /// <returns>Lista de Cartas Documento</returns>
        public EntityCollection<CartaDocumento> CartaDocumentoReadAll(DateTime fechaDesde, DateTime fechaHasta, int? usuario, string remitente, string destinatario)
        {
            ReadManyCommand<CartaDocumento> readCmd = new ReadManyCommand<CartaDocumento>(dalEngine);

            Filter filter = new Filter();

            filter.Add(CartaDocumento.Properties.Fecha,
               ">=", fechaDesde.Date);

            filter.Add(BooleanOp.And, CartaDocumento.Properties.Fecha,
                "<", fechaHasta.Date.AddDays(1));

            if (usuario != null)
            {
                filter.Add(BooleanOp.And, CartaDocumento.Properties.CreateUserID,
                    "=", usuario.Value);
            }

            if (!String.IsNullOrEmpty(remitente))
            {
                filter.Add(BooleanOp.And, CartaDocumento.Properties.NombreR,
                    "LIKE", remitente.Trim().Replace(" ", "%") + "%");
            }

            if (!String.IsNullOrEmpty(destinatario))
            {
                filter.Add(BooleanOp.And, CartaDocumento.Properties.NombreD,
                    "LIKE", destinatario.Trim().Replace(" ", "%") + "%");
            }

            readCmd.Filter = filter;

            //Sort
            Sort sort = new Sort();
            sort.Add(CartaDocumento.Properties.Fecha, SortingDirection.Asc);
            readCmd.Sort = sort;

            EntityCollection<CartaDocumento> cartas = readCmd.Execute();

            SeguridadDalc SeguridadDalc = Context.Session.SeguridadDalc;
            foreach (CartaDocumento carta in cartas)
                carta.CreateUser = SeguridadDalc.SecurityUserReadById(carta.CreateUserID);

            return cartas;
        }











        public CartaDocumento CartaDocumentoUpdate(CartaDocumento carta)
        {
            SecurityUser user = Security.Current.UserInfo.User;
            if (carta.Id == 0)
            {
                carta.CreateDate = enfoke.Time.Now;
                carta.CreateUserID = user.Id;
            }

            carta.UpdateDate = enfoke.Time.Now;
            carta.UpdateUser = user.Id;


            return dalEngine.Update<CartaDocumento>(carta);
        }

    }
}

