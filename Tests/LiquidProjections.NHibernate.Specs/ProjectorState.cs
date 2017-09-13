using System;

using FluentNHibernate.Mapping;

namespace LiquidProjections.NHibernate.Specs
{
    public class ProjectorState : IProjectorState
    {
        public virtual string Id { get; set; }
        public virtual long Checkpoint { get; set; }
        public virtual DateTime LastUpdateUtc { get; set; }
    }

    internal sealed class ProjectorStateClassMap : ClassMap<ProjectorState>
    {
        public ProjectorStateClassMap()
        {
            Id(x => x.Id).Not.Nullable().Length(150);
            Map(x => x.Checkpoint);
            Map(x => x.LastUpdateUtc);
        }
    }
}