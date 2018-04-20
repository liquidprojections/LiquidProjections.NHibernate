
namespace LiquidProjections.ExampleHost.Projections
{
    internal class CountryLookup : IPersistable
    {
        public virtual string Id { get; set; }
        public virtual string Name { get; set; }
    }
}