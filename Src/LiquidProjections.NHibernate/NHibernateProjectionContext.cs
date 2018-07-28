using NHibernate;

namespace LiquidProjections.NHibernate
{
    public sealed class NHibernateProjectionContext : ProjectionContext
    {
        private bool wasHandled;
        
        public ISession Session { get; set; }

        /// <summary>
        /// Indicates that at least one event in the current batch was mapped in the event map and thus was handled by the
        /// projector.
        /// </summary>
        internal bool WasHandled
        {
            get => wasHandled;
            set => wasHandled |= value;
        }
    }
}