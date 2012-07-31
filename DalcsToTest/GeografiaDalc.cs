using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Text;

using enfoke.Eges;
using enfoke.Connector;
using enfoke.Eges.Entities;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using enfoke.Eges.Valorizacion;


using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Persistence;
using enfoke.AOP;

namespace enfoke.Eges.Data
{
    public class GeografiaDalc : Dalc, IService
    {
        protected GeografiaDalc(NotConstructable dummy) : base(dummy) { }

        public ReadAllCollection<Pais> PaisReadAll()
        {
            return new ReadAllCollection<Pais>(dalEngine.GetAll<Pais>(Pais.Properties.Name));
        }

        public ReadAllCollection<Provincia> ProvinciaReadAll()
        {
            return new ReadAllCollection<Provincia>(dalEngine.GetAll<Provincia>(Provincia.Properties.Name));
        }

        public ReadAllCollection<Localidad> LocalidadReadAll()
        {
            return new ReadAllCollection<Localidad>(dalEngine.GetAll<Localidad>(Localidad.Properties.Name));
        }


        public EntityCollection<Localidad> LocalidadReadByProvincia(int provinciaID)
        {
            return dalEngine.GetManyByProperty<Localidad>(Localidad.Properties.ProvinciaId, provinciaID, Localidad.Properties.Name);
        }
    }
}

