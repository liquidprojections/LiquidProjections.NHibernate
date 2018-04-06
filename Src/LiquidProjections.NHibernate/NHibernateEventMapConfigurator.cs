using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Threading.Tasks;
using NHibernate;
using NHibernate.Util;

namespace LiquidProjections.NHibernate
{
    internal sealed class NHibernateEventMapConfigurator<TProjection, TKey>
        where TProjection : class, new()
    {
        private readonly Action<TProjection, TKey> setIdentity;
        private readonly IEventMap<NHibernateProjectionContext> map;
        private readonly IEnumerable<INHibernateChildProjector> children;
        private IProjectionCache<TProjection, TKey> cache = new PassthroughCache<TProjection, TKey>();

        public NHibernateEventMapConfigurator(
            IEventMapBuilder<TProjection, TKey, NHibernateProjectionContext> mapBuilder, Action<TProjection, TKey> setIdentity,
            IEnumerable<INHibernateChildProjector> children = null)
        {
            this.setIdentity = setIdentity;
            if (mapBuilder == null)
            {
                throw new ArgumentNullException(nameof(mapBuilder));
            }

            map = BuildMap(mapBuilder);
            this.children = children?.ToList() ?? new List<INHibernateChildProjector>();
        }

        public IProjectionCache<TProjection, TKey> Cache
        {
            get => cache;
            set => cache = value ?? throw new ArgumentNullException(nameof(value));
        }

        private IEventMap<NHibernateProjectionContext> BuildMap(
            IEventMapBuilder<TProjection, TKey, NHibernateProjectionContext> mapBuilder)
        {
            return mapBuilder.Build(new ProjectorMap<TProjection, TKey, NHibernateProjectionContext>
            {
                Create = OnCreate,
                Update = OnUpdate,
                Delete = OnDelete,
                Custom = (context, projector) => projector()
            });
        }

        private async Task OnCreate(TKey key, NHibernateProjectionContext context, Func<TProjection, Task> projector, Func<TProjection, bool> shouldOverwrite)
        {
            TProjection projection = await cache.Get(key, () => Task.FromResult(context.Session.Get<TProjection>(key)));
            if ((projection == null) || shouldOverwrite(projection))
            {
                if (projection == null)
                {
                    projection = new TProjection();
                    setIdentity(projection, key);

                    context.Session.Save(projection);
                    cache.Add(projection);
                }
                else
                {
                    // Reattach it to the session
                    // See also https://stackoverflow.com/questions/2932716/nhibernate-correct-way-to-reattach-cached-entity-to-different-session
                    context.Session.Lock(projection, LockMode.None);
                }
                
                await projector(projection).ConfigureAwait(false);
            }
        }

        private async Task OnUpdate(TKey key, NHibernateProjectionContext context, Func<TProjection, Task> projector, Func<bool> createIfMissing)
        {
            TProjection projection = await cache.Get(key, () => Task.FromResult(context.Session.Get<TProjection>(key)));
            if ((projection == null) && createIfMissing())
            {
                projection = new TProjection();
                setIdentity(projection, key);

                context.Session.Save(projection);
                cache.Add(projection);
            }
            else
            {
                context.Session.Lock(projection, LockMode.None);
            }

            await projector(projection).ConfigureAwait(false);
        }

        private async Task<bool> OnDelete(TKey key, NHibernateProjectionContext context)
        {
            TProjection existingProjection = 
                await cache.Get(key, () => Task.FromResult(context.Session.Get<TProjection>(key)));

            if (existingProjection != null)
            {
                context.Session.Delete(existingProjection);
                cache.Remove(key);

                return true;
            }

            return false;
        }

        public async Task ProjectEvent(object anEvent, NHibernateProjectionContext context)
        {
            foreach (INHibernateChildProjector child in children)
            {
                await child.ProjectEvent(anEvent, context).ConfigureAwait(false);
            }

            await map.Handle(anEvent, context).ConfigureAwait(false);
        }
    }
}