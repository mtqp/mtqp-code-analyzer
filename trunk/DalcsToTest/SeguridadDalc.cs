using enfoke.AOP;
using System;
using enfoke.Connector;
using enfoke.Data;
using enfoke.Data.Filters;
using enfoke.Eges.Entities;
using enfoke.Eges.Persistence;
using System.Linq;
using enfoke.Eges.Utils;
using NHibernate;
using enfoke.Utils;
using enfoke.Eges.Persistance;
using System.Collections.Generic;
using System.Linq.Expressions;
using enfoke.Eges.Entities.Results;
using enfoke.UI.Settings;

namespace enfoke.Eges.Data
{
    public class SeguridadDalc : Dalc, IService
    {
        protected SeguridadDalc(NotConstructable dummy) : base(dummy) { }

        public EntityCollection<SecurityProfile> SecurityProfileReadAll()
        {
            return dalEngine.GetAll<SecurityProfile>(SecurityProfile.Properties.Name);
        }

        #region SecurityUser
        private EntityCollection<SecurityUser> SecurityUserReadAll()
        {
            return dalEngine.GetManyByQuery<SecurityUser>(dalEngine.CreateQuery("From SecurityUser s ORDER BY s.LastName, s.FirstName"));
        }

        private EntityCollection<SecurityUser> SecurityUserInformanteReadAll(bool soloReales)
        {
            if (soloReales)
                return dalEngine.GetManyByQuery<SecurityUser>(dalEngine.CreateQuery("From SecurityUser s WHERE s.Medico.Id > 0 ORDER BY s.LastName, s.FirstName"));

            return dalEngine.GetManyByQuery<SecurityUser>(dalEngine.CreateQuery("From SecurityUser s ORDER BY s.LastName, s.FirstName"));
        }

        public EntityCollection<SecurityUser> SecurityUserConPerfilInformante()
        {
            String hql = "select spu.User from SecurityProfileUser as spu ";
            hql += " where spu.User.Deleted = false ";
            hql += " and spu.Profile.Name = :medicoInformante ";
            hql += " order by spu.User.LastName, spu.User.FirstName ";

            IQuery query = dalEngine.CreateQuery(hql);
            //query.SetParameter("medicoInformante", "MEDICO INFORMANTE");
            query.SetParameter("medicoInformante", "INFORMANTE EXTERNO");
            return dalEngine.GetManyByQuery<SecurityUser>(query);
        }

        public EntityCollection<SecurityUser> SecurityUserInformanteReadAll(bool soloReales, FilterFlag deleted)
        {
            // se filtran de acuerdo al criterio
            if (deleted == FilterFlag.NoFilter)
                return this.SecurityUserInformanteReadAll(soloReales);
            else
            {
                bool deletedValue = (deleted == FilterFlag.FilterTrue);
                Filter filter = new Filter();
                filter.Add(SecurityUser.Properties.Deleted, "=", deletedValue);

                if (soloReales)
                    filter.Add(BooleanOp.And, SecurityUser.Properties.Medico.Id, ">", 0);

                EntityCollection<SecurityUser> col = dalEngine.GetManyByFilter<SecurityUser>(filter);
                col.SortByProperty(SecurityUser.Properties.LastName);
                return col;
            }
        }

        public EntityCollection<SecurityUser> SecurityUserReadAll(FilterFlag deleted)
        {
            // se filtran de acuerdo al criterio
            if (deleted == FilterFlag.NoFilter)
                return this.SecurityUserReadAll();
            else
            {
                bool deletedValue = (deleted == FilterFlag.FilterTrue);
                return dalEngine.GetManyByProperty<SecurityUser>(SecurityUser.Properties.Deleted, deletedValue, SecurityUser.Properties.LastName);
            }
        }

        public EntityCollection<SecurityUser> SecurityUserReadByNameAndMedicoId(String name, int? medicoId)
        {
            Filter filter = new Filter();
            filter.Add(SecurityUser.Properties.Name, "=", name);
            filter.Add(BooleanOp.And, SecurityUser.Properties.Deleted, "=", false);

            if (medicoId.HasValue)
                filter.Add(BooleanOp.And, SecurityUser.Properties.Medico.Id, "=", medicoId.Value);

            return dalEngine.GetManyByFilter<SecurityUser>(filter);
        }

        public EntityCollection<SecurityUser> SecurityUserReadByNameAndMedicoIdForValidate(String name, int? medicoId)
        {
            Filter filter = new Filter();
            filter.Add(SecurityUser.Properties.Name, "=", name);
            filter.Add(BooleanOp.And, SecurityUser.Properties.Deleted, "=", false);

            if (medicoId.HasValue)
            {
                OpenParenthesis open = new OpenParenthesis(BooleanOp.And);
                filter.Add(open);
                filter.Add(SecurityUser.Properties.Medico.Id, "=", medicoId.Value);
                filter.Add(BooleanOp.Or, SecurityUser.Properties.Medico, "IS", null);
                CloseParenthesis close = new CloseParenthesis();
                filter.Add(close);
            }

            return dalEngine.GetManyByFilter<SecurityUser>(filter);
        }

        //[RequiresTransaction]
        //public virtual SecurityUser SecurityUserUpdate(SecurityUser user, EntityCollection<SecurityProfile> profiles, EntityCollection<AutorizacionGrupoUsuario> gruposAutorizacionesUsuario, string password, string claveAutorizaciones, EntityCollection<SecurityUserFingerprint> fingerPrints, EntityCollection<Sector> sectores)
        private SecurityUser SecurityUserUpdate(SecurityUser user, EntityCollection<SecurityProfile> profiles, EntityCollection<AutorizacionGrupoUsuario> gruposAutorizacionesUsuario, string password, string claveAutorizaciones, EntityCollection<SecurityUserFingerprint> fingerPrints, EntityCollection<Sector> sectores)
        {
            // Valido que el nombre de usuario o id del médico no esten ya asignados a otro usuario
            int? medicoId = null;
            if (user.Medico != null)
                medicoId = user.Medico.Id;

            // Obtengo las posibles repeticiones para el 
            EntityCollection<SecurityUser> users = this.SecurityUserReadByNameAndMedicoIdForValidate(user.Name.Trim(), medicoId);

            if (users != null)
            {
                // Discrimino si es el mismo Id
                foreach (SecurityUser usr in users)
                {
                    if (usr.Id != user.Id)
                        throw new NotLoggeableException("El nombre de usuario o médico asignado ya se encuentran registrados con otro usuario");
                }
            }

            //Si tiene algun grupo asignado
            if (gruposAutorizacionesUsuario.Count > 0)
            {
                // Si no se modifico la clave, viene vacia y no tengo que validar
                if (!String.IsNullOrEmpty(claveAutorizaciones))
                {
                    // Chequeo que la clave no exista para otro usuario
                    int? userClave = SecurityUserIdReadByClave(claveAutorizaciones);
                    if (userClave.HasValue && userClave.Value != user.Id)
                        throw new NotLoggeableException(" La Clave que ha ingresado para Autorizaciones es inválida o ya se encuentra siendo utilizada por otro usuario. Por favor ingrese otra. Las claves para Autorizaciones son únicas por usuario y no deben ser repetidas.");
                }
            }
            else
            {
                //Si no tiene guardo la clave en null
                claveAutorizaciones = null;
            }

            user = dalEngine.Update(user);
            SecurityUserUpdatePasswordClaveAutorizaciones(user.Id, password, claveAutorizaciones);

            // Actualizo los perfiles
            SecurityProfileUserUpdate(user, profiles);

            // Actualizo los grupo autorizaciones
            ServiciosDalc dalc = Context.Session.ServiciosDalc;
            dalc.AutorizacionGrupoUsuarioUpdate(user, gruposAutorizacionesUsuario);

            // Actualizo los sectores
            SectoresUserUpdate(user, sectores);

            // Actualiza huellas
            if (fingerPrints != null)
            {
                // Por si el usuario se está creando por primera vez, entonces le asocio el id que se genero recién para el mismo.
                foreach (SecurityUserFingerprint suf in fingerPrints)
                    suf.UserId = user.Id;


                fingerPrints = dalEngine.UpdateCollection<SecurityUserFingerprint>(fingerPrints);
            }

            return user;
        }

        [RequiresTransaction]
        public virtual SecurityUser SecurityUserUpdate(SecurityUser user, EntityCollection<SecurityProfile> profiles, EntityCollection<AutorizacionGrupoUsuario> gruposAutorizacionesUsuario, string password, string claveAutorizaciones, EntityCollection<SecurityUserFingerprint> fingerPrints, EntityCollection<Sector> sectores, DateTime? fechaExpiracion)
        {
            return SecurityUserUpdate(user, profiles, gruposAutorizacionesUsuario, password, claveAutorizaciones, fingerPrints, sectores, fechaExpiracion, Context.Session.TurnosDalc.GetSucursalesNameHabilitadasByUsuario(user));
        }

        [RequiresTransaction]
        public virtual SecurityUser SecurityUserUpdate(SecurityUser user, EntityCollection<SecurityProfile> profiles, EntityCollection<AutorizacionGrupoUsuario> gruposAutorizacionesUsuario, string password, string claveAutorizaciones, EntityCollection<SecurityUserFingerprint> fingerPrints, EntityCollection<Sector> sectores, DateTime? fechaExpiracion, EntityCollection<SucursalName> centrosHabilitados)
        {
            ActualizarVisibilidadPorCentros(user, centrosHabilitados);
            SecurityUser userUpdated = SecurityUserUpdate(user, profiles, gruposAutorizacionesUsuario, password, claveAutorizaciones, fingerPrints, sectores);
            SecurityUserForLogin userLogin = Context.Session.Dalc.GetById<SecurityUserForLogin>(userUpdated.Id);
            userLogin.PasswordExpireDate = fechaExpiracion;
            Context.Session.Dalc.Update<SecurityUserForLogin>(userLogin);
            return userUpdated;
        }

        private static void ActualizarVisibilidadPorCentros(SecurityUser user, EntityCollection<SucursalName> centrosHabilitados)
        {
            // borro todos los actuales
            EntityCollection<UsuarioCentroInhabilitado> anteriores = Context.Session.Dalc.GetManyByProperty<UsuarioCentroInhabilitado>(UsuarioCentroInhabilitado.Properties.UsuarioId, user.Id);
            Context.Session.Dalc.Delete(anteriores);

            //seteo los centros habilitados para el usuario
            Data.Security.Current.UserInfo.User.SucursalesHabilitadas = centrosHabilitados;

            //busco que centros son los que qdan inhabilitados y los guardo
            EntityCollection<SucursalName> sucursalesInhabilitadas = Context.Session.Dalc.GetAll<SucursalName>();
            sucursalesInhabilitadas.RemoveRange(centrosHabilitados);

            EntityCollection<UsuarioCentroInhabilitado> inhabilitados = new EntityCollection<UsuarioCentroInhabilitado>();
            foreach (SucursalName sucInhabilitada in sucursalesInhabilitadas)
            {
                UsuarioCentroInhabilitado userCentroInhablitado = new UsuarioCentroInhabilitado();
                userCentroInhablitado.SucursalId = sucInhabilitada.Id;
                userCentroInhablitado.UsuarioId = user.Id;
                inhabilitados.Add(userCentroInhablitado);
            }

            Context.Session.Dalc.UpdateCollection(inhabilitados);
        }


        public void SecurityUserUpdatePassword(int userID, string pwd, DateTime? newExpireDate)
        {
            SecurityUserForLogin user = dalEngine.GetById<SecurityUserForLogin>(userID);
            user.Password = pwd;
            user.PasswordExpireDate = newExpireDate;
            Context.Session.Dalc.Update<SecurityUserForLogin>(user);
        }

        private void SecurityUserUpdatePasswordClaveAutorizaciones(int userID, string password, string clave)
        {
            if (!String.IsNullOrEmpty(password) || !String.IsNullOrEmpty(clave))
            {
                SecurityUserForLogin user = dalEngine.GetById<SecurityUserForLogin>(userID);
                if (!String.IsNullOrEmpty(password))
                    user.Password = password;
                if (!String.IsNullOrEmpty(clave))
                    user.Clave = clave;


                dalEngine.Update<SecurityUserForLogin>(user);
            }
        }

        /// <summary>
        /// Retorno un SecurityUser para un ID
        /// </summary>
        /// <param name="id">ID del SecurityUser</param>
        /// <returns>El SecurityUser correspondiente</returns>
        [AnonymousMethod()]
        public SecurityUser SecurityUserReadById(int id)
        {
            return dalEngine.GetById<SecurityUser>(id);
        }

        [Private]
        public int? SecurityUserIdReadByClave(string clave)
        {
            SecurityUserForLogin user = SecurityUserReadByClave(clave);

            if (user != null)
                return user.Id;
            else
                return (int?)null;
        }

        [Private]
        public SecurityUserForLogin SecurityUserReadByClave(string clave)
        {
            ReadManyCommand<SecurityUserForLogin> readCmd = new ReadManyCommand<SecurityUserForLogin>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(SecurityUserForLogin.Properties.Clave,
                "=", clave);

            EntityCollection<SecurityUserForLogin> col = readCmd.Execute();

            return col.Count > 0 ? col[0] : null;
        }

        /// <summary>
        /// Retorno todos los SecurityUser que aparecen en la Historica de Estados de un turno
        /// </summary>
        /// <param name="id">ID del Turno</param>
        /// <returns>Todos los SecurityUser</returns>
        public EntityCollection<SecurityUser> SecurityUserReadByTurnoLog(int turnoID)
        {
            EntityCollection<SecurityUser> users = new EntityCollection<SecurityUser>();

            if (turnoID > 0)
            {
                TurnosDalc TurnosDalc = Context.Session.TurnosDalc;

                TurnoLog log = TurnosDalc.TurnoLogReadByTurno(turnoID);
                if (log.ControlFacturacionUsuario != null && !users.Contains(log.ControlFacturacionUsuario))
                {
                    log.ControlFacturacionUsuario.NombreSectorAsociado = "Facturación";
                    users.Add(log.ControlFacturacionUsuario);
                }
                if (log.InformadoUsuario != null && !users.Contains(log.InformadoUsuario))
                {
                    log.InformadoUsuario.NombreSectorAsociado = "Informe";
                    users.Add(log.InformadoUsuario);
                }
                if (log.InformeEnMesaUsuario != null && !users.Contains(log.InformeEnMesaUsuario))
                {
                    log.InformeEnMesaUsuario.NombreSectorAsociado = "Facturación";
                    users.Add(log.InformeEnMesaUsuario);
                }
                if (log.OrdenEnMesaUsuario != null && !users.Contains(log.OrdenEnMesaUsuario))
                {
                    log.OrdenEnMesaUsuario.NombreSectorAsociado = "Facturación";
                    users.Add(log.OrdenEnMesaUsuario);
                }
                if (log.RecepcionUsuario != null && !users.Contains(log.RecepcionUsuario))
                {
                    log.RecepcionUsuario.NombreSectorAsociado = "Recepción";
                    users.Add(log.RecepcionUsuario);
                }
                if (log.ReservaUsuario != null && !users.Contains(log.ReservaUsuario))
                {
                    log.ReservaUsuario.NombreSectorAsociado = "Reserva";
                    users.Add(log.ReservaUsuario);
                }

                // Ordeno por Name
                users.Sort(new Comparison<SecurityUser>(delegate(SecurityUser left, SecurityUser right)
                {
                    return left.Name.CompareTo(right.Name);
                }));
            }

            return users;
        }

        [Private]
        public SecurityUser SecurityUserReadByMedico(int medicoID)
        {
            Filter filter = new Filter();
            filter.Add(SecurityUser.Properties.Medico.Id, "=", medicoID);
            filter.Add(BooleanOp.And, SecurityUser.Properties.Deleted, "=", false);
            EntityCollection<SecurityUser> users = dalEngine.GetManyByFilter<SecurityUser>(filter);

            if (users != null && users.Count > 0)
                return users[0];
            else
                return null;
        }

        [Private]
        public SecurityUser SecurityUserReadByUserNameAndPassword(string userName, string passEnc)
        {

            ReadManyCommand<SecurityUserForLogin> readCmd = new ReadManyCommand<SecurityUserForLogin>(dalEngine);

            readCmd.Filter = new Filter();

            readCmd.Filter.Add(SecurityUserForLogin.Properties.Name,
                "=", userName);

            EntityCollection<SecurityUserForLogin> col = readCmd.Execute();

            if (col.Count > 0)
            {
                if (col[0].Password != passEnc)
                    throw new NotLoggeableException("Contraseña inválida para el usuario " + userName + ".");

                return SecurityUserReadById(col[0].Id);
            }
            else
                throw new NotLoggeableException("Usuario " + userName + " no encontrado.");
        }

        [Private]
        public bool CurrentUserHasProfileByProfileId(int profileId)
        {
            return SecurityUserHasProfileByUserAndProfileId(Security.Current.UserInfo.User, profileId);
        }

        [Private]
        public bool SecurityUserHasProfileByUserAndProfileId(SecurityUser user, int profile)
        {
            bool retorno = false;

            // Busco los Perfiles del User
            EntityCollection<SecurityProfileUser> spus = this.SecurityProfileUserReadByUser(user);

            // Recorro los Perfiles del User en busqueda del Pedido
            foreach (SecurityProfileUser spu in spus)
            {
                if (spu.Profile.Id == profile)
                {
                    retorno = true;
                    break;
                }
            }

            return retorno;
        }

        [Private]
        internal SecurityUser SecurityUserReadByUserName(string userName)
        {
            string hql = "from SecurityUser u where u.Deleted = false AND u.Name = :user";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("user", userName);
            return query.UniqueResult<SecurityUser>();
        }
        [Private]
        internal int SecurityUserIdReadRawByUserName(string userName)
        {
            // Devuelve el que autenticó el framework
            string sqltext = "select usr_id from security_users where usr_name = " + SqlPortable.ValueToString(userName);
            return enfoke.Context.Data.Session.ExecuteScalarAsInt(System.Data.CommandType.Text, sqltext, true);
        }
        #endregion

        [RequiresTransaction]
        public virtual void SecurityProfileUserAdd(EntityCollection<SecurityUser> users, EntityCollection<SecurityProfile> profiles)
        {
            foreach (SecurityUser user in users)
                SecurityProfileUserUpdate(user, profiles);
        }

        private void SecurityProfileUserUpdate(SecurityUser user, EntityCollection<SecurityProfile> profiles)
        {
            EntityCollection<SecurityProfileUser> existentes = SecurityProfileUserReadByUser(user);
            dalEngine.Delete(existentes);

            foreach (SecurityProfile profile in profiles)
            {
                SecurityProfileUser spu = new SecurityProfileUser();
                spu.User = user;
                spu.Profile = profile;


                spu = dalEngine.Update<SecurityProfileUser>(spu);
            }
        }
        [RequiresTransaction]
        protected virtual void SectoresUserUpdate(SecurityUser user, EntityCollection<Sector> sectores)
        {
            // Borra los existentes y luego graba
            EntityCollection<SecurityUserSector> sectoresExistentes = SecurityUserGetSecurityUserSector(user);
            dalEngine.Delete(sectoresExistentes);

            foreach (Sector sector in sectores)
            {
                SecurityUserSector sus = new SecurityUserSector();
                sus.SecurityUser = user;
                sus.Sector = sector;


                sus = dalEngine.Update<SecurityUserSector>(sus);
            }
        }

        public EntityCollection<SecurityProfileUser> SecurityProfileUserReadByUser(SecurityUser user)
        {
            ReadManyCommand<SecurityProfileUser> readCmd = new ReadManyCommand<SecurityProfileUser>(dalEngine);
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, SecurityProfileUser.Properties.User,
                "=", user.Id);

            readCmd.Filter = filter;

            EntityCollection<SecurityProfileUser> col = readCmd.Execute();

            return col;
        }

        public bool SecurityUserHasAction(SecurityUser user, string actionName)
        {
            var query = from spa in dalEngine.Query<SecurityProfileAction>()
                        join spu in dalEngine.Query<SecurityProfileUser>() on spa.Profile.Id equals spu.Profile.Id
                        where spu.User.Id == user.Id && spa.Action.Name == actionName
                        select spa;

            return query.FirstOrDefault() != null;
        }

        [Private]
        public EntityCollection<SecurityAction> SecurityActionReadByUser(SecurityUser user)
        {
            string hql = "select distinct spfa.Action from "
                        + " SecurityProfileAction spfa, SecurityProfileUser spu "
                        + " WHERE spfa.Profile = spu.Profile AND spu.User.Id = :userId";
            IQuery query = dalEngine.CreateQuery(hql);
            query.SetParameter("userId", user.Id);

            EntityCollection<SecurityAction> actions = dalEngine.GetManyByQuery<SecurityAction>(query);

            /*foreach (SecurityAction action in actions)
                action.Menu = dalEngine.GetById<SecurityMenu>(action.MenuID);
            */
            EntityCollection<SecurityMenu> menues = dalEngine.GetManyByIds<SecurityMenu>((from action in actions select action.MenuID).ToList<int>());
            SortedList<int, SecurityMenu> menuesOrdenado = new SortedList<int, SecurityMenu>();
            foreach (SecurityMenu menue in menues)
                menuesOrdenado.Add(menue.Id, menue);
            foreach (SecurityAction action in actions)
                action.Menu = menuesOrdenado[action.MenuID];
            return actions;

        }

        #region SecurityUserSection
        private EntityCollection<SecurityUserSector> SecurityUserGetSecurityUserSector(SecurityUser user)
        {

            ReadManyCommand<SecurityUserSector> readCmd = new ReadManyCommand<SecurityUserSector>(dalEngine);
            Filter filter = new Filter();
            filter.Add(SecurityUserSector.Properties.SecurityUser,
                "=", user.Id);

            readCmd.Filter = filter;

            return readCmd.Execute();
        }

        /// <summary>
        /// Devuelve una coleccion con los sectores del SecurityUser
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        public EntityCollection<Sector> SecurityUserGetSectores(SecurityUser user)
        {
            EntityCollection<SecurityUserSector> susCol = SecurityUserGetSecurityUserSector(user);
            EntityCollection<Sector> secCol = new EntityCollection<Sector>();

            TurnosDalc turnosDalc = Context.Session.TurnosDalc;
            foreach (SecurityUserSector susItem in susCol)
            {
                susItem.Sector.EsRecepcion = turnosDalc.SectorHasTipoSector(susItem.Sector.Id, TipoSectorEnum.Recepcion);
                secCol.Add(susItem.Sector);
            }

            // Los ordena
            secCol.Sort(new Comparison<Sector>(delegate(Sector lhs, Sector rhs)
            {
                return lhs.Name.CompareTo(rhs.Name);
            }));

            return secCol;
        }
        #endregion

        #region SecurityUserFingerprint [GG]
        [RequiresTransaction]
        public virtual SecurityUserFingerprint SecurityUserFingerprintUpdate(SecurityUserFingerprint userFingerprint)
        {
            SecurityUserFingerprint securityUserFingerprint = new SecurityUserFingerprint();


            securityUserFingerprint = dalEngine.Update<SecurityUserFingerprint>(userFingerprint);

            return securityUserFingerprint;
        }

        [RequiresTransaction]
        public virtual void SecurityUserFingerPrintDeleteById(int FpId)
        {
            try
            {
                SecurityUserFingerprint suf = SecurityUserFingerprintReadById(FpId);
                dalEngine.Delete(suf);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public SecurityUserFingerprint SecurityUserFingerprintReadById(int FpId)
        {

            return dalEngine.GetById<SecurityUserFingerprint>(FpId);

        }
        public EntityCollection<SecurityUserFingerprint> SecurityUserFingerprintReadByUser(int userId)
        {

            ReadManyCommand<SecurityUserFingerprint> readCmd = new ReadManyCommand<SecurityUserFingerprint>(dalEngine);
            Filter filter = new Filter();
            filter.Add(BooleanOp.And, SecurityUserFingerprint.Properties.UserId,
                "=", userId);

            readCmd.Filter = filter;

            EntityCollection<SecurityUserFingerprint> col = readCmd.Execute();

            return col;
        }

        [AnonymousMethod()]
        public EntityCollection<SecurityUserFingerprint> SecurityUserFingerprintReadAll()
        {
            return dalEngine.GetAll<SecurityUserFingerprint>();
        }
        #endregion

        [Private]
        public void LogSeguridadUpdate(LogSeguridad logEntry)
        {
            dalEngine.Update<LogSeguridad>(logEntry);
        }

        public bool SecurityUserHasProfileByUserAndProfileName(string profile)
        {
            return SecurityUserHasProfileByUserAndProfileName(Data.Security.Current.UserInfo.User, profile);
        }

        public bool SecurityUserHasProfileByUserAndProfileName(SecurityUser user, string profile)
        {
            bool retorno = false;

            // Busco los Perfiles del User
            EntityCollection<SecurityProfileUser> spus = this.SecurityProfileUserReadByUser(user);

            // Recorro los Perfiles del User en busqueda del Pedido
            foreach (SecurityProfileUser spu in spus)
            {
                if (spu.Profile.Name.ToUpper() == profile.ToUpper())
                {
                    retorno = true;
                    break;
                }
            }

            return retorno;
        }

        public EntityCollection<SecurityUserPerfilSectorName> SecurityUserPerfilSectorNameReadAll(FilterFlag deleted)
        {
            // se filtran de acuerdo al criterio
            EntityCollection<SecurityUser> users = new EntityCollection<SecurityUser>();
            if (deleted == FilterFlag.NoFilter)
                users = SecurityUserReadAll();
            else
            {
                bool deletedValue = (deleted == FilterFlag.FilterTrue);
                users = dalEngine.GetManyByProperty<SecurityUser>(SecurityUser.Properties.Deleted, deletedValue, SecurityUser.Properties.LastName);
            }
            List<int> usersIds = users.GetIds();
            EntityCollection<SecurityUserSector> usersSectors = SectoresReadByUsersIds(usersIds);
            EntityCollection<SecurityProfileUser> usersProfiles = PerfilesReadByUsersIds(usersIds);
            return CreateSecurityUserPerfilSectorNames(users, usersSectors, usersProfiles);
        }

        public EntityCollection<SecurityProfileUser> PerfilesReadByUserId(int userId)
        {
            EntityCollection<SecurityProfileUser> usersProfiles = (from userProfile in dalEngine.Query<SecurityProfileUser>()
                                                                   where userProfile.User.Id == userId
                                                                   select userProfile).ToEntityCollection<SecurityProfileUser>();
            return usersProfiles;
        }

        public EntityCollection<SecurityUserSector> SectoresReadByUserId(int userId)
        {
            EntityCollection<SecurityUserSector> usersSectors = (from userSector in dalEngine.Query<SecurityUserSector>()
                                                                 where userId == userSector.SecurityUser.Id
                                                                 select userSector).ToEntityCollection<SecurityUserSector>();
            return usersSectors;
        }

        public EntityCollection<SecurityProfileUser> PerfilesReadByUsersIds(List<int> usersIds)
        {
            EntityCollection<SecurityProfileUser> usersProfiles = (from userProfile in dalEngine.Query<SecurityProfileUser>()
                                                                   where usersIds.Contains(userProfile.User.Id)
                                                                   select userProfile).ToEntityCollection<SecurityProfileUser>();
            return usersProfiles;
        }

        public EntityCollection<SecurityUserSector> SectoresReadByUsersIds(List<int> usersIds)
        {
            EntityCollection<SecurityUserSector> usersSectors = (from userSector in dalEngine.Query<SecurityUserSector>()
                                                                 where usersIds.Contains(userSector.SecurityUser.Id)
                                                                 select userSector).ToEntityCollection<SecurityUserSector>();
            return usersSectors;
        }

        private EntityCollection<SecurityUserPerfilSectorName> CreateSecurityUserPerfilSectorNames(EntityCollection<SecurityUser> users, EntityCollection<SecurityUserSector> sectors, EntityCollection<SecurityProfileUser> profiles)
        {
            EntityCollection<SecurityUserPerfilSectorName> usersProfileSector = new EntityCollection<SecurityUserPerfilSectorName>();
            foreach (SecurityUser user in users)
                usersProfileSector.Add(new SecurityUserPerfilSectorName(user, GetProfile(user, profiles), GetSectors(user, sectors)));
            return usersProfileSector;
        }

        private List<string> GetProfile(SecurityUser user, EntityCollection<SecurityProfileUser> profiles)
        {
            List<string> profile_names = new List<string>();
            foreach (SecurityProfileUser profile in profiles)
                if (profile.User.Id == user.Id)
                    profile_names.Add(profile.Profile.Name);
            return profile_names;
        }

        private List<string> GetSectors(SecurityUser user, EntityCollection<SecurityUserSector> sectors)
        {
            List<string> sector_names = new List<string>();
            foreach (SecurityUserSector sector in sectors)
                if (sector.SecurityUser.Id == user.Id)
                    sector_names.Add(sector.Sector.Name);
            return sector_names;
        }

        [RequiresTransaction]
        public virtual void UIRegistrar(int userId, object collection)
        {
            List<SettingsContainer> settingsCollection = (List<SettingsContainer>)collection;
            // Recorre cada container
            foreach (SettingsContainer settings in settingsCollection)
            {
                if (settings.Items.Count == 0)
                    DeleteUISetting(userId, settings);
                else
                {
                    // Mala noticia: para grabar tiene que borrar
                    DeleteUISetting(userId, settings);
                    // Arma el encabezado
                    UISettings header = new UISettings();
                    header.CanCustomize = settings.CanCustomize;
                    header.CanSort = settings.CanSort;
                    header.Label = settings.Label;
                    header.MaxRows = settings.MaxRows;
                    header.Name = settings.Name;
                    header.SortColumn = settings.SortColumn;
                    header.UserId = userId;
                    header.Name = header.Name ?? " ";
                    dalEngine.Update(header);
                    // Arma los items
                    foreach (SettingsItem settingsItem in settings.Items)
                    {
                        if (settingsItem.IsDirty)
                        {
                            UISettingsItem item = new UISettingsItem();
                            item.Label = settingsItem.Label;
                            item.Name = settingsItem.Name;
                            item.NeverVisible = settingsItem.NeverVisible;
                            item.Order = settingsItem.Order;
                            item.Required = settingsItem.Required;
                            item.Setting = header;
                            item.Visible = settingsItem.Visible;
                            item.Width = settingsItem.Width;
                            item.Name = !string.IsNullOrEmpty(item.Name) ? item.Name : " ";
                            dalEngine.Update(item);
                        }
                    }
                }

            }
        }

        private void DeleteUISetting(int userId, SettingsContainer setting)
        {
            // Es un delete
            IList<int> itemsId = (from settingsItem in dalEngine.Query<UISettingsItem>()
                                  where settingsItem.Setting.UserId == userId
                                  && settingsItem.Setting.Name == setting.Name
                                  select settingsItem.Id).ToList<int>();
            dalEngine.DeleteBatchByIds<UISettingsItem>(itemsId);
            IList<int> headersId = (from settingsHeader in dalEngine.Query<UISettings>()
                                    where settingsHeader.UserId == userId
                                  && settingsHeader.Name == setting.Name
                                    select settingsHeader.Id).ToList<int>();
            dalEngine.DeleteBatchByIds<UISettings>(headersId);
        }

        [Private]
        public SettingsSet CargaConfiguracionUI(int userId)
        {
            SettingsSet ret = new SettingsSet();
            // Lee las tablas de configuración de interfaz para el usuario activo
            EntityCollection<UISettingsItem> items = (from settingsItem in dalEngine.Query<UISettingsItem>()
                                                      where settingsItem.Setting.UserId == userId
                                                      orderby settingsItem.Setting.Name, settingsItem.Setting.Id
                                                      select settingsItem).ToEntityCollection<UISettingsItem>();
            // Lo recorre armando los containers...
            SettingsContainer container = null;
            foreach (UISettingsItem dataItem in items)
            {
                if (container != null && container.Name != dataItem.Setting.Name)
                {  // hace un corte
                    ret[container.Name] = container;
                    container = null;
                }
                if (container == null)
                {   // esto lo hace la primera vez y después de los cortes
                    container = new SettingsContainer(dataItem.Setting.Name);
                    container.CanCustomize = dataItem.Setting.CanCustomize;
                    container.CanSort = dataItem.Setting.CanSort;
                    container.Label = dataItem.Setting.Label;
                    container.MaxRows = dataItem.Setting.MaxRows;
                    container.SortColumn = dataItem.Setting.SortColumn;
                }
                // Lee el item
                SettingsItem item = new SettingsItem(dataItem.Name);
                item.Container = container;
                item.Label = dataItem.Label;
                item.NeverVisible = dataItem.NeverVisible;
                item.Order = dataItem.Order;
                item.Required = dataItem.Required;
                item.Visible = dataItem.Visible;
                item.Width = dataItem.Width;
                // Lo agrega al container
                container.Items.Add(item);
            }
            if (container != null)
                ret[container.Name] = container;
            return ret;
        }
    }
}

