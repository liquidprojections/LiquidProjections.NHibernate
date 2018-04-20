# Liquid Projections for NHibernate [![Build status](https://ci.appveyor.com/api/projects/status/8shaimqx3u366bji/branch/master?svg=true)](https://ci.appveyor.com/project/dennisdoomen/liquidprojections-nhibernate-69l6u/branch/master)

## What is this?
This [package](https://www.nuget.org/packages/LiquidProjections.NHibernate/) provides an `NHibernateProjector` that allows you to build [NHibernate]((https://www.nuget.org/packages/NHibernate/))-backed projectors for use with [LiquidProjections'](https://github.com/liquidprojections/LiquidProjections) event mapping API and `Dispatcher` class.

## What does it offer?
* Supports NHibernate 4.0 or later. 
* Allows customizing how many LiquidProjections [Transaction](https://github.com/liquidprojections/LiquidProjections/blob/master/Src/LiquidProjections.Abstractions/Transaction.cs)s it should process in a single database transaction.
* Supports nesting one or more `NHibernateChildProjector`s that can be used for maintaining lookup tables and which are part of the same database transaction as the parent projector creates.
* Ensures autonomy by storing the  `IProjectionState` (e.g. the last transaction checkpoint) in the same transaction as the actual projection data.
* Allows the projector to be configured with an `ExceptionPolicy` delegate so that you can handle projection exceptions and get full control on whether or not the projector should retry the entire batch, individual transactions, ignore or abort the exception.
* If the implementation of `IProjectorStat`e has additional custom properties, you can use the `EnrichState` delegate of the `NHibernateProjector` to update the state object before it is written to the database. This is called as part of the same database transaction that encompasses the projector code.

## Why would I use NHibernate for something like a projection
Some have argued that using raw SQL is the fasted method for updating  RDBMS-backed projections. And quite often this is true. But depending on the nature of the projection, in particular when it touches the same projection many times in the same batch of events, the unit-of-work (the `ISession`) can provide some significant speed improvements.

## Caching support
This package provides a simple caching mechanism in the form of the `IProjectionCache` and ships with the `LruProjectionCache` based on the [FluidCaching](https://www.nuget.org/packages/FluidCaching.Sources/) project. The `IProjectionCache` is meant for the most common scenarios and thus has some limitations:
   * If the projector performs database modifications directly on the NHibernate `ISession`, that projector must make sure the cache is updated or cleared accordingly.
   * The cache doesn't understand relationships where a projection refers to another projection maintained by the same projector. For instance, a projector that maintains a graph of parents and children, where a child is also a (direct or indirect) parent must use a more advanced type of caching. 

If you need more advanced caching, please refer to NHibernate's [Second Level Caching](http://nhibernate.info/doc/nhibernate-reference/caches.html) feature.

## About the sample application?
You'll find a sample application under the `Samples` folder that tries to demonstrate a couple of aspect of using `LiquidProjections` such as event maps, exception handling and lookups in combination with NHibernate. It uses the `JsonFileEventStore` example implementation to feed a list of events serialized into the `ExampleEvents.zip` into the `Dispatcher`. 

This dispatcher gets used by the `CountsProjector` to build a Sqlite-backed `DocumentCountProjection` (stored in the output directory under `projections.db`). Each projection record has a one-to-many relationship to the `ValidityPeriod` object to track when a document is valid or not. The projector also maintains a `CountryLookup` using a `NHibernateChildProjector` to map country codes to country names. 

The `CountsProjector` also demonstrates how to employ the `LruProjectionCache` to speed up lookups and updates of recently updated projections. And finally, the projector uses a pretty extensive example on how to use the `ExceptionResolution.RetryIndividual` to trace down the transaction that is causing the (simulated) non-transient exception and mark that projection as corrupt. In then uses the `Filter` property of the `NHibernateProjector` to skip any successive attempts to update that corrupted projection.

To expose the data from this projection, the sample program will expose an HTTP API end-point served through the `StatisticsController` at route `http://localhost:9000/api/Statistics/CountsPerState`. For example, run a GET on this URL to get some counters:

    http://localhost:9000/api/Statistics/CountsPerState?country=6DF7E2AC-6F06-420A-A0B5-14FB3865E850&kind=BypassOverrideCertificate

As all my open-source projects are written to be examples of proper coding conventions and Test Driven Development, also consider checking out the [unit tests](https://github.com/liquidprojections/LiquidProjections.NHibernate/blob/master/Tests/LiquidProjections.NHibernate.Specs/NHibernateProjectorSpecs.cs) to learn how the many features work together. 

