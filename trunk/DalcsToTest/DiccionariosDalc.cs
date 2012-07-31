using System;
using System.Collections.Generic;
using System.Text;
using enfoke.AOP;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.DisconnectedSupport;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Entities.No_Mapeadas;
using enfoke.Eges.Entities.Results;
using enfoke.Eges.Persistence;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Eges.Persistance;
using System.Linq;

namespace enfoke.Eges.Data
{
    public class DiccionariosDalc : Dalc, IService
    {
        protected DiccionariosDalc(NotConstructable dummy) : base(dummy) { }

        [Private]
        public EntityCollection<Palabra> ObtenerPalabras(Diccionario diccionario)
        {
            return (from palabra in dalEngine.Query<Palabra>() where palabra.DiccionarioId == diccionario.Id
                    orderby palabra.Palabra
                    select palabra).ToEntityCollection<Palabra>();
        }


        [RequiresTransaction]
        public virtual void GenerarDiccionarioAdministrativo()
        {
            Dictionary<string, string> palabras = new Dictionary<string,string>();
            // Agrega los médicos
            EntityCollection<MedicoLight> ec = Context.Session.MedicosDalc.MedicoLightReadAll().Collection;
            foreach(MedicoLight medico in ec)
            {
                AddWordAndSplit(palabras, medico.Name, true);
                AddWordAndSplit(palabras, medico.Apellido, true);
            }
            EntityCollection<SucursalName> centros = Context.Session.Dalc.GetAll<SucursalName>();
            foreach (SucursalName centro in centros)
            {
                AddWordAndSplit(palabras, centro.Name, true);
            }
            // Limpia y agrega
            // Obtiene el Id del diccionario administrativo
            Diccionario dic = (from diccionario in dalEngine.Query<Diccionario>() where diccionario.Administrativo select diccionario).FirstOrDefault();
            if (dic == null) return;
            // Borra lo existente 
            dalEngine.DeleteBatchByProperty<Palabra>(Palabra.Properties.DiccionarioId, dic.Id);
            // Agrega
            foreach (string palabra in palabras.Keys)
                dalEngine.Update(new Palabra() { DiccionarioId = dic.Id, Palabra = palabra });
            // Listo
        }
        private void AddWordAndSplit(Dictionary<string, string> palabras, string p, bool standarizeToFirstUpper)
        {
            if (p == null) return;
            p = p.Replace(".", " ");
            p = p.Trim();
            string[] words = p.Split(' ');
            foreach (string word in words)
            {
                string word2 = word;
                word2 = word2.Trim();
                if (word2.Length > 1)
                    AddWord(palabras, word2, standarizeToFirstUpper);
            }
        }
        private void AddWord(Dictionary<string, string> palabras, string p, bool standarizeToFirstUpper)
        {
            if (p == null)
                return;
            p = p.Trim();
            if (p.Length == 0) return;
            // si es sólo símbolos, no lo agrega
            bool bIsValid = false;
            string plower = p.ToLower();
            for (int n = 0; n < p.Length; n++)
            {
                if (plower[n] >= 'a' &&
                    plower[n] <= 'z')
                {
                    bIsValid = true;
                    break;
                }
            }
            if (bIsValid == false) return;
            if (standarizeToFirstUpper)
            {
                p = p.ToLower();
                int n = 0;
                while (n != -1)
                {
                    p = p.Substring(0, n) + p.Substring(n, 1).ToUpper() + p.Substring(n + 1);
                    n = p.IndexOf(" ", n + 1);
                }
            }
            if (palabras.ContainsKey(p))
                return;
            palabras.Add(p, null);
        }

        [RequiresTransaction]
        public virtual Palabra AgregarPalabra(Diccionario diccionario, string palabraNueva)
        {
            palabraNueva = palabraNueva.Trim();
            // Se fija que no exista y la agrega
            Palabra existente = (from palabra in dalEngine.Query<Palabra>()
                 where palabra.DiccionarioId == diccionario.Id
                 && palabra.Palabra == palabraNueva
                 select palabra).FirstOrDefault();
            if (existente == null)
            {
                Palabra palabraDb = new Palabra();
                palabraDb.DiccionarioId = diccionario.Id;
                palabraDb.Palabra = palabraNueva;
                return dalEngine.Update(palabraDb);
            }
            else
                return existente;
        }
    }
}

