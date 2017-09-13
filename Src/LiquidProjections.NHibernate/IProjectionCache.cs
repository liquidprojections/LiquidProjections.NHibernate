using System;
using System.Threading.Tasks;

namespace LiquidProjections.NHibernate
{
    /// <summary>
    /// Defines a write-through cache that the <see cref="NHibernateProjector{TProjection,TKey,TState}"/> can use to avoid unnecessary loads
    /// of the projection.
    /// </summary>
    public interface IProjectionCache<TProjection, in TKey>
    {
        /// <summary>
        /// Adds a new item to the cache or ignores the call if the item is already in the cache. 
        /// </summary>
        void Add(TProjection projection);

        /// <summary>
        /// Attempts to get the item identified by <paramref name="key"/> from the cache, or creates a new one. 
        /// </summary>
        Task<TProjection> Get(TKey key, Func<Task<TProjection>> createProjection);

        /// <summary>
        /// Removes the item identified by <paramref name="key"/> from the cache.
        /// </summary>
        void Remove(TKey key);

        /// <summary>
        /// Removes all the cached items from the cache. 
        /// </summary>
        void Clear();
    }
}