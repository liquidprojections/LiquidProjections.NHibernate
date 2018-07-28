using System.Collections;
using System.Collections.Generic;

namespace LiquidProjections.NHibernate
{
    internal static class EnumerableExtensions
    {
        /// <summary>
        /// Splits up the set of items in batches of the specified size.
        /// </summary>
        /// <param name="items">The set of items to split up.</param>
        /// <param name="batchSize">The maximum amount of items in one batch.</param>
        /// <returns>A list of batches containing the items.</returns>
        public static IEnumerable<Batch<T>> InBatchesOf<T>(this IEnumerable<T> items, int batchSize)
        {
            var batch = new List<T>(batchSize);

            using (var enumerator = items.GetEnumerator())
            {
                bool hasMore = enumerator.MoveNext();

                while (hasMore)
                {
                    batch.Add(enumerator.Current);
                    hasMore = enumerator.MoveNext();

                    if (batch.Count >= batchSize)
                    {
                        yield return new Batch<T>(batch, !hasMore);
                        batch = new List<T>(batchSize);
                    }
                }
            }

            if (batch.Count > 0)
            {
                yield return new Batch<T>(batch, true);
            }
        }
    }

    /// <summary>
    /// This class represents the enumerable containing all items in one batch.
    /// </summary>
    internal sealed class Batch<T> : IEnumerable<T>
    {
        private readonly IList<T> batch;
        
        /// <summary>
        /// Indicates if this is the last batch of the set of items.
        /// </summary>
        public bool IsLast { get; }

        public Batch(IList<T> batch, bool isLast)
        {
            IsLast = isLast;
            this.batch = batch;
        }
        
        public IEnumerator<T> GetEnumerator()
        {
            return batch.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) batch).GetEnumerator();
        }

        public IList<T> ToList()
        {
            return batch;
        }
    }
}