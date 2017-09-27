# Liquid Projections for NHibernate [![Build status](https://ci.appveyor.com/api/projects/status/8shaimqx3u366bji/branch/master?svg=true)](https://ci.appveyor.com/project/dennisdoomen/liquidprojections-nhibernate-69l6u/branch/master)

## What is this?
This [package](https://www.nuget.org/packages/LiquidProjections.NHibernate/) provides an `NHibernateProjector` that allows you to build [NHibernate]((https://www.nuget.org/packages/NHibernate/))-backed projectors for use with [LiquidProjections'](https://github.com/liquidprojections/LiquidProjections) event mapping API and `Dispatcher` class.

## What does it offer?
* Supports NHibernate 4.0 or later. 
* Allows customizing how many LiquidProjections [Transaction](https://github.com/liquidprojections/LiquidProjections/blob/master/Src/LiquidProjections.Abstractions/Transaction.cs)s it should process in a single database transaction.
* Supports nesting one or more `NHibernateChildProjector`s that can be used for maintaining lookup tables and which are part of the same database transaction as the parent projector creates.
* Ensures autonomy by storing the  `IProjectionState` (e.g. the last transaction checkpoint) in the same transaction as the actual projection data.
* Exposes the `ShouldRetry` delegate so that you can handle projection exceptions and get full control on whether or not the projector should retry (again).
* If the implementation of `IProjectorStat`e has additional custom properties, you can use the `EnrichState` delegate of the `NHibernateProjector` to update the state object before it is written to the database. This is called as part of the same database transaction that encompasses the projector code.

## Caching support
This package provides a simple caching mechanism in the form of the `IProjectionCache` and ships with the `LruProjectionCache` based on the [FluidCaching](https://www.nuget.org/packages/FluidCaching.Sources/) project. The `IProjectionCache` is meant for the most common scenarios and thus has some limitations:
   * If the projector performs database modifications directly on the NHibernate `ISession`, that projector must make sure the cache is updated or cleared accordingly.
   * The cache doesn't understand relationships where a projection refers to another projection maintained by the same projector. For instance, a projector that maintains a graph of parents and children, where a child is also a (direct or indirect) parent must use a more advanced type of caching. 

If you need more advanced caching, please refer to NHibernate's [Second Level Caching](http://nhibernate.info/doc/nhibernate-reference/caches.html) feature.

## How to get started?
Check out the [unit tests](https://github.com/liquidprojections/LiquidProjections.NHibernate/blob/master/Tests/LiquidProjections.NHibernate.Specs/NHibernateProjectorSpecs.cs) to learn how the many features work together. You can also look at the non-NHibernate example [here](https://github.com/liquidprojections/LiquidProjections/blob/master/Samples/ExampleHost/ExampleProjector.cs#L25) and replace `Projector` with `NHibernateProjector`. A proper example project for NHibernate is [planned](https://github.com/liquidprojections/LiquidProjections.NHibernate/issues/5).
