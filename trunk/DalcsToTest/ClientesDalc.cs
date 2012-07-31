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
using enfoke.Eges.Persistance;

namespace enfoke.Eges.Data
{
    public class ClientesDalc : Dalc, IService
    {
        protected ClientesDalc(NotConstructable dummy) : base(dummy) { }






        public EntityCollection<Cliente> ClienteSearchByFilter(string razonSocial, string cuit, string codigoCliente, bool? pagoDiferido, bool conImportes)
        {
            ReadManyCommand<Cliente> readCmd = new ReadManyCommand<Cliente>(dalEngine);

            Filter filter = new Filter();

            // se filtra el nombre
            if (!String.IsNullOrEmpty(razonSocial))
                filter.Add(BooleanOp.And, Cliente.Properties.RazonSocial, "LIKE", razonSocial.Trim().Replace(" ", "%") + "%");

            // se filtra el cuit
            if (!String.IsNullOrEmpty(cuit))
                filter.Add(BooleanOp.And, Cliente.Properties.Cuit, "LIKE", cuit.Trim().Replace(" ", "%") + "%");

            // se filtra el CodigoErp
            if (!String.IsNullOrEmpty(codigoCliente))
                filter.Add(BooleanOp.And, Cliente.Properties.CodigoErp, "LIKE", codigoCliente.Trim().Replace(" ", "%") + "%");

            if (pagoDiferido.HasValue)
                filter.Add(BooleanOp.And, Cliente.Properties.PagoDiferidoCaja, "=", pagoDiferido.Value ? "1" : "0");

            readCmd.Filter = filter;

            Sort sort = new Sort();
            sort.Add(Cliente.Properties.RazonSocial, SortingDirection.Asc);
            readCmd.Sort = sort;

            return readCmd.Execute();

        }

        public EntityCollection<Cliente> ClienteReadAll(bool showDeleted)
        {
            EntityCollection<Cliente> clientes = dalEngine.GetAll<Cliente>();

            if (!showDeleted)
            {
                foreach (Cliente cliente in clientes)
                {
                    if (cliente.DeleteFlag)
                    {
                        clientes.Remove(cliente);
                    }
                }
            }
            return clientes;
        }

        public void ClienteDelete(Cliente cli)
        {
            cli.DeleteDate = enfoke.Time.Now;
            dalEngine.Update(cli);
        }






        public Cliente ClienteReadById(int id)
        {
            return dalEngine.GetById<Cliente>(id);
        }

        public Cliente ClienteReadByObraSocialId(int obraSocialId)
        {
            string hql = "select oso.Cliente from ObraSocial oso where oso.Id = :obraSocialId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("obraSocialId", obraSocialId);
            return query.UniqueResult<Cliente>();
        }

        public EntityCollection<Cliente> ClienteReadConPagoDiferido()
        {
            return dalEngine.GetManyByProperty<Cliente>(Cliente.Properties.PagoDiferidoCaja, true);
        }

        public Cliente UpdateCliente(Cliente cliente)
        {
            return LobUpdater.UpdateClob<Cliente>(cliente, Cliente.Properties.Observaciones);
        }

    }
}
