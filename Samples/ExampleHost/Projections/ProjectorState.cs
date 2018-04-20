using System;
using LiquidProjections.NHibernate;

namespace LiquidProjections.ExampleHost.Projections
{
    public class ProjectorState : IProjectorState, IPersistable
    {
        public virtual string Id { get; set; }
        public virtual long Checkpoint { get; set; }
        public virtual DateTime LastUpdateUtc { get; set; }

        public virtual string LastStreamId { get; set; }
    }
}