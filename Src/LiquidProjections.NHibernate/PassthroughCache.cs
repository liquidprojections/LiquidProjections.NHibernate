using System;
using System.Threading.Tasks;

namespace LiquidProjections.NHibernate
{
    public class PassthroughCache<TProjection, TKey> : IProjectionCache<TProjection, TKey>
    {
        public void Add(TProjection projection)
        {
            if (projection == null)
            {
                throw new ArgumentNullException(nameof(projection));
            }

            // Do nothing.
        }

        public Task<TProjection> Get(TKey key, Func<Task<TProjection>> createProjection)
        {
            if (key == null)
            {
                throw new ArgumentException("Key is missing.", nameof(key));
            }

            if (createProjection == null)
            {
                throw new ArgumentNullException(nameof(createProjection));
            }

            return createProjection();
        }

        public void Remove(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentException("Key is missing.", nameof(key));
            }

            // Do nothing.
        }

        public void Clear()
        {
            // Do nothing
        }

        public Task<TProjection> TryGet(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentException("Key is missing.", nameof(key));
            }

            return Task.FromResult(default(TProjection));
        }
    }
}