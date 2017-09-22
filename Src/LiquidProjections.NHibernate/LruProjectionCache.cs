using System;
using System.Threading.Tasks;
using FluidCaching;

namespace LiquidProjections.NHibernate
{
    /// <inheritdoc />
    /// <summary>
    /// An implementation of the <see cref="T:LiquidProjections.NHibernate.IProjectionCache`2" /> based on https://www.nuget.org/packages/FluidCaching.Sources/
    /// </summary>
    public class LruProjectionCache<TProjection, TKey> : IProjectionCache<TProjection, TKey> where TProjection : class
    {
        private readonly IIndex<TKey, TProjection> index;
        private readonly FluidCache<TProjection> cache;

        public LruProjectionCache(int capacity, TimeSpan minimumRetention, TimeSpan maximumRetention, Func<TProjection, TKey> getKey, Func<DateTime> getUtcNow)
        {
            if (capacity < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            if (minimumRetention < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(minimumRetention));
            }

            if (maximumRetention < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumRetention));
            }

            if (minimumRetention > maximumRetention)
            {
                throw new ArgumentException("Minimum retention is greater than maximum retention.");
            }

            if (getUtcNow == null)
            {
                throw new ArgumentNullException(nameof(getUtcNow));
            }

            cache = new FluidCache<TProjection>(capacity, minimumRetention, maximumRetention, () => getUtcNow());
            index = cache.AddIndex("projections", projection => getKey(projection));
        }

        public long Hits => cache.Statistics.Hits;

        public long Misses => cache.Statistics.Misses;

        public long CurrentCount => cache.Statistics.Current;

        public async Task<TProjection> Get(TKey key, Func<Task<TProjection>> createProjection)
        {
            return await index.GetItem(key, async _ => await createProjection());
        }

        public void Remove(TKey key)
        {
            index.Remove(key);
        }

        public void Add(TProjection projection)
        {
            cache.Add(projection);
        }

        public void Clear()
        {
            cache.Clear();
        }
    }
}