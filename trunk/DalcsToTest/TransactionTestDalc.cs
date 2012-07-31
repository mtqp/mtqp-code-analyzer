using System;
using System.Collections.Generic;
using System.Text;
using enfoke.Connector;
using enfoke.AOP;
using enfoke.Data;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;


namespace enfoke.Eges.Data
{
	/// <summary>
	/// Clase de prueba del procesamiento 
	/// de los atributos del tipo FilterAttribute.
	/// Por ejemplo los de transacci?n.
	/// </summary>
	public class TransactionTestDalc : Dalc, IService
	{
		class ManualException : Exception { }
		private const string TEST_NAME = "TEST_PAIS";

		protected TransactionTestDalc(NotConstructable nonConstruct)
			: base(nonConstruct) { }

		/// <summary>
		/// Testea que el atributo de transacci?n haga 
		/// rollback realmente en la base de datos. 
		/// Es decir, que el procesamiento de los 
		/// atributos v?a Proxies funcione.
		/// </summary>
		/// <returns></returns>
		[Private]
		public bool TestRollback()
		{
			PrepareTest();

			TryInsert();

			Pais pais = ReadTestPais();
			if(pais == null)
			{
				//Test passed
				return true;
			}
			else
			{
				//Test failed
				DeleteTestPais(pais);
				return false;
			}
		}

		/// <summary>
		/// Verifica que no haya quedado el registro
		/// a testear en la base de datos, si lo 
		/// encuentra lo borra para poder comenzar 
		/// el test.
		/// </summary>
		private void PrepareTest()
		{
			Pais pais = ReadTestPais();
			if(pais != null)
			{
				DeleteTestPais(pais);
			}
		}

		/// <summary>
		/// Inserta generando siempre una 
		/// excepci?n para forzar el rollback.
		/// </summary>
		[RequiresNewTransaction]
		protected virtual void InsertWithException()
		{

			Pais pais = CreateTestPais();
			dalEngine.Update<Pais>(pais);
			throw new ManualException();
		}

		private void TryInsert()
		{
			try
			{
				InsertWithException();
			}
			catch(ManualException)
			{
				//error esperado, ignora la excepci?n.
			}
			catch
			{
				//error inesperado
				throw;
			}

		}

		private Pais CreateTestPais()
		{
			Pais pais = new Pais();
			pais.Name = TEST_NAME;
			return pais;
		}

		private Pais ReadTestPais()
		{
			return
				dalEngine.GetByProperty<Pais>(Pais.Properties.Name, TEST_NAME);
		}

		private void DeleteTestPais(Pais pais)
		{
            dalEngine.Delete(pais);
		}
	}
}
