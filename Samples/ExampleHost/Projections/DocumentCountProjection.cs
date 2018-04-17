using System;
using System.Collections.Generic;
using System.Linq;
using FluentNHibernate.Automapping;
using FluentNHibernate.Automapping.Alterations;

namespace LiquidProjections.ExampleHost.Projections
{
    public class DocumentCountProjection : IPersistable
    {
        public DocumentCountProjection()
        {
            Periods = new HashSet<ValidityPeriod>();
        }

        public virtual string Id { get; set; }

        public virtual string Type { get; set; }

        public virtual string Kind { get; set; }

        public virtual string State { get; set; }

        public virtual Guid Country { get; set; }

        public virtual DateTime? NextReviewAt { get; set; }

        public virtual DateTime? LifetimePeriodEnd { get; set; }

        public virtual DateTime? StartDateTime { get; set; }

        public virtual DateTime? EndDateTime { get; set; }

        public virtual ISet<ValidityPeriod> Periods { get; set; }

        public virtual string RestrictedArea { get; set; }

        /// <summary>
        /// Marks the projection as corrupt because of some non-transient exception. </summary>
        public virtual bool Corrupt { get; set; }

        public virtual ValidityPeriod GetOrAddPeriod(int sequence)
        {
            var period = Periods.FirstOrDefault(p => p.Sequence == sequence);
            if (period == null)
            {
                period = new ValidityPeriod
                {
                    Sequence = sequence
                };

                Periods.Add(period);
            }

            return period;
        }

        public override string ToString()
        {
            return $"Id: {Id}, Kind:{Kind} State:{State}";
        }
    }

    public class DocumentCountProjectionMappingOverride : IAutoMappingOverride<DocumentCountProjection>
    {
        public void Override(AutoMapping<DocumentCountProjection> mapping)
        {
            mapping.HasMany(x => x.Periods).Component(c =>
            {
                c.Map(x => x.From).Column("FromTimestamp");
                c.Map(x => x.Sequence);
                c.Map(x => x.Status);
                c.Map(x => x.To).Column("ToTimestamp");
            }).Cascade.AllDeleteOrphan().Not.KeyNullable().Not.KeyUpdate();

            mapping.Map(p => p.Corrupt).Index("IX_Query");
            mapping.Map(p => p.Kind).Index("IX_Query");
            mapping.Map(p => p.Country).Index("IX_Query");
            mapping.Map(p => p.State).Index("IX_Query");
        }
    }
}