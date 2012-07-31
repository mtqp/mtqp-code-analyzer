using enfoke.AOP;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Text;

using enfoke.Connector;
using enfoke.Data.Filters;
using enfoke.Eges;
using enfoke.Eges.Entities;
using enfoke.Eges.Reserva;
using enfoke.Eges.Utils;
using enfoke.Eges.Valorizacion;


using enfoke.Eges.Persistence.DAL;
using enfoke.Eges.Persistence;

namespace enfoke.Eges.Data
{
    public class ReportesDalc : Dalc, IService
    {
        protected ReportesDalc(NotConstructable dummy) : base(dummy) { }






        [AnonymousMethod()]
        public ReporteMetaData ReporteMetaDataReadByTag(ReporteMetaDataTagsEnum tag)
        {
            return dalEngine.GetByProperty<ReporteMetaData>(ReporteMetaData.Properties.Tag, EnumDescription.GetDescription(tag));
        }

        [AnonymousMethod()]
        public ReporteMetaData ReporteMetaDataReadByTag(string tag)
        {
            return ReporteMetaDataReadByTag(ReporteMetaData.GetTagEnum(tag));
        }

        [AnonymousMethod()]
        public EntityCollection<ReporteMetaData> ReporteMetaDataReadByGrupoTag(GrupoTagEnum grupoTag)
        {
            return dalEngine.GetManyByProperty<ReporteMetaData>(ReporteMetaData.Properties.GrupoTag,
                                                                EnumDescription.GetDescription(grupoTag));
        }


        public ReporteMetaData ReporteMetaDataReadByTagAndTypename(ReporteMetaDataTagsEnum reporteMetaDataTagsEnum, string reporteEntidadTypename)
        {
            Filter filter = new Filter
                                {
                                    {
                                        ReporteMetaData.Properties.Tag, " = ",
                                        EnumDescription.GetDescription(reporteMetaDataTagsEnum)
                                        },
                                    {BooleanOp.And, ReporteMetaData.Properties.TypeName, " = ", reporteEntidadTypename}
                                };

            return dalEngine.GetByFilter<ReporteMetaData>(filter);
        }





    }
}

