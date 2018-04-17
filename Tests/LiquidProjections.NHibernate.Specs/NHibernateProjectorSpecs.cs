using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Chill;
using FluentAssertions;
using FluentAssertions.Extensions;
using FluentNHibernate.Mapping;
using LiquidProjections.Abstractions;
using LiquidProjections.Testing;
using NHibernate;
using NHibernate.Linq;
using Xunit;
using Xunit.Sdk;
#pragma warning disable 1998

namespace LiquidProjections.NHibernate.Specs
{
    namespace NHibernateProjectorSpecs
    {
        public class Given_a_sqlite_projector_with_an_in_memory_event_source :
            GivenSubject<NHibernateProjector<ProductCatalogEntry, string, ProjectorState>>
        {
            protected EventMapBuilder<ProductCatalogEntry, string, NHibernateProjectionContext> Events;
            protected LruProjectionCache<ProductCatalogEntry, string> Cache;
            protected Exception ProjectionException;
            private readonly List<INHibernateChildProjector> children = new List<INHibernateChildProjector>();

            public Given_a_sqlite_projector_with_an_in_memory_event_source()
            {
                Given(() =>
                {
                    UseThe(new MemoryEventSource());

                    UseThe(new InMemorySQLiteDatabaseBuilder().Build());
                    UseThe(The<InMemorySQLiteDatabase>().SessionFactory);

                    Cache = new LruProjectionCache<ProductCatalogEntry, string>(1000, 1.Hours(), 2.Hours(), e => e.Id,
                        () => DateTime.UtcNow);

                    Events = new EventMapBuilder<ProductCatalogEntry, string, NHibernateProjectionContext>();

                    WithSubject(_ =>
                    {
                        return new NHibernateProjector<ProductCatalogEntry, string, ProjectorState>(
                            The<ISessionFactory>().OpenSession, Events, (entry, id) => entry.Id = id, children)
                        {
                            BatchSize = 10,
                            Cache = Cache,
                        };
                    });
                });
            }

            protected void AddChildProjector(INHibernateChildProjector childProjector)
            {
                children.Add(childProjector);
            }

            protected void StartProjecting() => StartProjecting(CancellationToken.None);

            protected void StartProjecting(CancellationToken cancellationToken)
            {
                The<MemoryEventSource>().Subscribe(0, new Subscriber
                {
                    HandleTransactions = async (transactions, _) =>
                    {
                        try
                        {
                            await Subject.Handle(transactions, cancellationToken);
                        }
                        catch (Exception e)
                        {
                            ProjectionException = e;
                        }
                    }
                }, "");
            }
        }

        #region Mapping Rules

        public class When_a_create_was_requested_but_the_database_already_contained_that_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_create_was_requested_but_the_database_already_contained_that_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    Cache.Add(new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "Gas"
                    });
                });

                When(async () =>
                {
                    StartProjecting();

                    await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_throw()
            {
                ProjectionException.Should()
                    .BeOfType<ProjectionException>().Which
                    .Message.Should().MatchEquivalentOf("*projection*key*already exists*");
            }
        }

        public class When_a_create_was_requested_but_the_cache_already_contained_that_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_create_was_requested_but_the_cache_already_contained_that_projection()
            {
                Given(() =>
                {
                    Events
                        .Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    var existingEntry = new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "Gas"
                    };

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(existingEntry);
                        session.Flush();
                    }

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_throw()
            {
                ProjectionException.Should()
                    .BeOfType<ProjectionException>();
            }
        }

        public class When_an_event_requires_a_create_of_a_new_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Transaction transaction;

            public When_an_event_requires_a_create_of_a_new_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    StartProjecting();
                });

                When(async () =>
                {
                    transaction = await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_create_a_new_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public void And_it_should_track_the_transaction_checkpoint()
            {
                long? checkpoint = Subject.GetLastCheckpoint();
                checkpoint.Should().Be(transaction.Checkpoint);
            }

            [Fact]
            public async Task And_it_should_add_the_projection_to_the_cache()
            {
                (await Cache.Get("c350E", () => null)).Should().NotBeNull();
            }
        }

        public class When_an_idempotent_create_was_requested_on_a_pre_existing_database_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_idempotent_create_was_requested_on_a_pre_existing_database_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .IgnoringDuplicates()
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "OldCategory"
                        });
                        session.Flush();
                    }

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "NewCategory"
                }));
            }

            [Fact]
            public void Then_it_should_not_do_anything()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("OldCategory");
                }
            }
        }

        public class When_an_idempotent_create_was_requested_on_a_cached_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_idempotent_create_was_requested_on_a_cached_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .IgnoringDuplicates()
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "OldCategory"
                        });
                        session.Flush();
                    }

                    Cache.Add(new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "OldCategory"
                    });

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "NewCategory"
                }));
            }

            [Fact]
            public void Then_it_should_flush_it_back_to_the_database_without_changing_the_category()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("OldCategory");
                }
            }
        }

        public class When_an_idempotent_create_was_requested_on_a_new_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Transaction transaction;

            public When_an_idempotent_create_was_requested_on_a_new_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .IgnoringDuplicates()
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    StartProjecting();
                });

                When(async () =>
                {
                    transaction = await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_store_a_new_projection_in_the_database()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public void And_it_should_track_the_transaction_checkpoint()
            {
                long? checkpoint = Subject.GetLastCheckpoint();
                checkpoint.Should().Be(transaction.Checkpoint);
            }

            [Fact]
            public async Task And_it_should_add_that_newly_created_projection_to_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => null);
                cachedItem.Should().NotBeNull();
            }
        }

        public class When_a_create_or_update_of_a_existing_database_projection_is_requested :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_create_or_update_of_a_existing_database_projection_is_requested()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .OverwritingDuplicates()
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Gas"
                        });

                        session.Flush();
                    }

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_update_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public async Task And_it_should_also_update_the_cached_item()
            {
                var cachedItem = await Cache.Get("c350E", () => null);
                cachedItem.Category.Should().Be("Hybrid");
            }
        }

        public class When_a_create_or_update_of_an_existing_cached_projection_is_requested :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_create_or_update_of_an_existing_cached_projection_is_requested()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .OverwritingDuplicates()
                        .Using((projection, @event, context) => projection.Category = @event.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "PersistedCategory"
                        });

                        session.Flush();
                    }

                    Cache.Add(new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "CachedCategory"
                    });

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "NewCategory"
                }));
            }

            [Fact]
            public async Task Then_it_should_update_the_cached_item()
            {
                var cachedItem = await Cache.Get("c350E", () => null);
                cachedItem.Category.Should().Be("NewCategory");
            }

            [Fact]
            public void And_it_should_update_the_projection_as_well()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("NewCategory");
                }
            }
        }

        public class When_a_create_or_update_of_a_new_projection_is_requested :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Transaction transaction;

            public When_a_create_or_update_of_a_new_projection_is_requested()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(productAddedToCatalogEvent => productAddedToCatalogEvent.ProductKey)
                        .OverwritingDuplicates()
                        .Using((productCatalogEntry, productAddedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productAddedToCatalogEvent.Category);

                    StartProjecting();
                });

                When(async () =>
                {
                    transaction = await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_create_a_new_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public void Then_it_should_track_the_transaction_checkpoint()
            {
                long? checkpoint = Subject.GetLastCheckpoint();
                checkpoint.Should().Be(transaction.Checkpoint);
            }

            [Fact]
            public async Task Then_it_should_add_the_item_to_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => null);
                cachedItem.Should().NotBeNull();
            }
        }

        public class When_updating_a_non_existing_projection_and_no_creation_is_allowed :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_updating_a_non_existing_projection_and_no_creation_is_allowed()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductMovedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_throw()
            {
                ProjectionException.Should().BeOfType<ProjectionException>();
            }
        }

        public class When_an_event_requires_an_update_of_an_existing_database_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Transaction transaction;

            public When_an_event_requires_an_update_of_an_existing_database_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Gas"
                        });
                        session.Flush();
                    }

                    StartProjecting();
                });

                When(async () =>
                {
                    transaction = await The<MemoryEventSource>().Write(new ProductMovedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_update_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public void And_it_should_track_the_transaction_checkpoint()
            {
                long? checkpoint = Subject.GetLastCheckpoint();
                checkpoint.Should().Be(transaction.Checkpoint);
            }

            [Fact]
            public async Task And_it_should_update_the_cache_as_well()
            {
                var cachedItem = await Cache.Get("c350E", () => null);
                cachedItem.Category.Should().Be("Hybrid");
            }
        }

        public class When_an_update_should_only_happen_if_the_projection_exists_and_the_database_does_not_contain_any :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_update_should_only_happen_if_the_projection_exists_and_the_database_does_not_contain_any()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .IgnoringMisses()
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_do_nothing()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public async Task And_the_cache_should_not_be_affected()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Should().BeNull();
            }
        }

        public class When_an_event_requires_an_update_if_a_projection_exists_and_it_does_indeed :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Transaction transaction;

            public When_an_event_requires_an_update_if_a_projection_exists_and_it_does_indeed()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .IgnoringMisses()
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Gas"
                        });
                        session.Flush();
                    }

                    StartProjecting();
                });

                When(async () =>
                {
                    transaction = await The<MemoryEventSource>().Write(new ProductMovedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public void Then_it_should_update_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.Category.Should().Be("Hybrid");
                }
            }

            [Fact]
            public void And_it_should_track_the_transaction_checkpoint()
            {
                long? checkpoint = Subject.GetLastCheckpoint();
                checkpoint.Should().Be(transaction.Checkpoint);
            }

            [Fact]
            public async Task And_the_cache_should_be_updated_as_well()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Category.Should().Be("Hybrid");
            }
        }

        public class When_an_event_requires_a_delete_of_an_existing_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_event_requires_a_delete_of_an_existing_projection()
            {
                Given(() =>
                {
                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        var entry = new ProductCatalogEntry
                        {
                            Id = "c350E"
                        };

                        session.Save(entry);
                        session.Flush();

                        Cache.Add(entry);
                    }

                    Events.Map<ProductDiscontinuedEvent>().AsDeleteOf(anEvent => anEvent.ProductKey);

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductDiscontinuedEvent
                {
                    ProductKey = "c350E",
                }));
            }

            [Fact]
            public void Then_it_should_remove_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public async Task And_remove_it_from_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Should().BeNull();
            }
        }

        public class When_deleting_existing_proejctions_only_and_the_projection_doesnt_exist :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_deleting_existing_proejctions_only_and_the_projection_doesnt_exist()
            {
                Given(() =>
                {
                    Events.Map<ProductDiscontinuedEvent>().AsDeleteOf(e => e.ProductKey);

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductDiscontinuedEvent
                {
                    ProductKey = "c350E",
                }));
            }

            [Fact]
            public void Then_it_should_throw()
            {
                ProjectionException.Should()
                    .BeOfType<ProjectionException>().Which
                    .Message.Should().MatchEquivalentOf("Could not delete*Entry*c350E*");
            }
        }


        public class When_deleting_a_modified_projection :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_deleting_a_modified_projection()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    Events.Map<ProductDiscontinuedEvent>()
                        .AsDeleteOf(productDiscontinuedEvent => productDiscontinuedEvent.ProductKey);

                    var existingEntry = new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "Gas"
                    };

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(existingEntry);
                        session.Flush();
                    }

                    Cache.Add(existingEntry);

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(
                    new ProductMovedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrids"
                    }, new ProductDiscontinuedEvent
                    {
                        ProductKey = "c350E",
                    }));
            }

            [Fact]
            public void Then_it_should_remove_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    var entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public async Task And_remove_it_from_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Should().BeNull();
            }
        }

        public class When_deleting_existing_projections_only_and_the_projection_exists :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_deleting_existing_projections_only_and_the_projection_exists()
            {
                Given(() =>
                {
                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        var entry = new ProductCatalogEntry
                        {
                            Id = "c350E"
                        };
                        session.Save(entry);

                        Cache.Add(entry);

                        session.Flush();
                    }

                    Events.Map<ProductDiscontinuedEvent>()
                        .AsDeleteOf(anEvent => anEvent.ProductKey)
                        .IgnoringMisses();

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductDiscontinuedEvent
                {
                    ProductKey = "c350E",
                }));
            }

            [Fact]
            public void Then_it_should_remove_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public async Task And_remove_it_from_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Should().BeNull();
            }
        }

        public class When_deleting_existing_projections_only_and_no_projection_exists :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private Func<Task> action;

            public When_deleting_existing_projections_only_and_no_projection_exists()
            {
                Given(() =>
                {
                    Events.Map<ProductDiscontinuedEvent>().AsDeleteOf(e => e.ProductKey).IgnoringMisses();

                    StartProjecting();
                });

                When(() =>
                {
                    action = () => The<MemoryEventSource>().Write(new ProductDiscontinuedEvent
                    {
                        ProductKey = "c350E",
                    });
                });
            }

            [Fact]
            public void Then_it_should_not_do_anything()
            {
                action.Should().NotThrow();
            }
        }

        public class When_deleting_existing_projections_only_and_a_modified_projection_exists :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_deleting_existing_projections_only_and_a_modified_projection_exists()
            {
                Given(() =>
                {
                    Events.Map<ProductMovedToCatalogEvent>()
                        .AsUpdateOf(productMovedToCatalogEvent => productMovedToCatalogEvent.ProductKey)
                        .Using((productCatalogEntry, productMovedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productMovedToCatalogEvent.Category);

                    Events.Map<ProductDiscontinuedEvent>()
                        .AsDeleteOf(productDiscontinuedEvent => productDiscontinuedEvent.ProductKey)
                        .IgnoringMisses();

                    var existingEntry = new ProductCatalogEntry
                    {
                        Id = "c350E",
                        Category = "Gas"
                    };

                    Cache.Add(existingEntry);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(existingEntry);
                        session.Flush();
                    }

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(
                    new ProductMovedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrids"
                    }, new ProductDiscontinuedEvent
                    {
                        ProductKey = "c350E",
                    }));
            }

            [Fact]
            public void Then_it_should_remove_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    var entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public async Task And_remove_it_from_the_cache()
            {
                var cachedItem = await Cache.Get("c350E", () => Task.FromResult<ProductCatalogEntry>(null));
                cachedItem.Should().BeNull();
            }
        }

        public class When_an_event_requires_a_custom_action :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private NHibernateProjectionContext context;

            public When_an_event_requires_a_custom_action()
            {
                Given(() =>
                {
                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        var entry = new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Hybrids"
                        };

                        session.Save(entry);
                        session.Flush();
                    }

                    Events.Map<CategoryDiscontinuedEvent>().As((categoryDiscontinuedEvent, context) =>
                    {
                        var entries = context.Session.Query<ProductCatalogEntry>()
                            .Where(entry => entry.Category == categoryDiscontinuedEvent.Category)
                            .ToList();

                        foreach (ProductCatalogEntry entry in entries)
                        {
                            context.Session.Delete(entry);
                        }

                        this.context = context;

                        return Task.FromResult(false);
                    });

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new Transaction
                    {
                        Checkpoint = 111,
                        Id = "MyTransactionId",
                        StreamId = "MyStreamId",
                        TimeStampUtc = 10.April(1979).At(13, 14, 15),
                        Headers = new Dictionary<string, object>
                        {
                            ["My custom header"] = "My custom header value"
                        },
                        Events = new List<EventEnvelope>
                        {
                            new EventEnvelope
                            {
                                Body = new CategoryDiscontinuedEvent
                                {
                                    Category = "Hybrids"
                                },
                                Headers = new Dictionary<string, object>
                                {
                                    ["Some event header"] = "Some event header value"
                                }
                            }
                        }
                    })
                );
            }

            [Fact]
            public void Then_it_should_have_executed_the_custom_action()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    var entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }

            [Fact]
            public void And_it_should_have_created_the_context()
            {
                context.Should().BeEquivalentTo(new NHibernateProjectionContext
                {
                    Checkpoint = 111,
                    TransactionId = "MyTransactionId",
                    StreamId = "MyStreamId",
                    TimeStampUtc = 10.April(1979).At(13, 14, 15),
                    TransactionHeaders = new Dictionary<string, object>
                    {
                        ["My custom header"] = "My custom header value"
                    },
                    EventHeaders = new Dictionary<string, object>
                    {
                        ["Some event header"] = "Some event header value"
                    }
                }, options => options.Excluding(c => c.Session));
            }
        }

        public class When_an_event_is_not_mapped_at_all :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_event_is_not_mapped_at_all()
            {
                Given(() => StartProjecting());

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_not_do_anything()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().BeNull();
                }
            }
        }

        #endregion

        #region General 

        public class When_a_custom_state_key_is_set : Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_custom_state_key_is_set()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(productAddedToCatalogEvent => productAddedToCatalogEvent.ProductKey)
                        .Using((productCatalogEntry, productAddedToCatalogEvent, context) =>
                            productCatalogEntry.Category = productAddedToCatalogEvent.Category);

                    Subject.StateKey = "CatalogEntries";

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "Hybrid"
                }));
            }

            [Fact]
            public void Then_it_should_store_projector_state_with_that_key()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    ProjectorState projectorState = session.Get<ProjectorState>("CatalogEntries");
                    projectorState.Should().NotBeNull();
                }
            }
        }

        public class When_the_projector_state_is_enriched : Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_the_projector_state_is_enriched()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((p, @event, context) => p.Category = @event.Category);

                    Subject.EnrichState = (state, transaction) =>
                    {
                        state.LastStreamId = transaction.StreamId;
                    };
                });

                When(() =>
                {
                    StartProjecting();

                    return The<MemoryEventSource>().Write(new Transaction
                    {
                        StreamId = "Product1",
                        Events = new[]
                        {
                            new EventEnvelope
                            {
                                Body = new ProductAddedToCatalogEvent
                                {
                                    ProductKey = "c350E",
                                    Category = "Hybrid"
                                }
                            }
                        }
                    });
                });
            }

            [Fact]
            public void Then_it_should_store_the_custom_property_along_with_the_state()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    ProjectorState projectorState = session.Get<ProjectorState>(Subject.StateKey);
                    projectorState.LastStreamId.Should().Be("Product1");
                }
            }
        }


        public class When_an_event_has_a_header : Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_an_event_has_a_header()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(anEvent => anEvent.ProductKey)
                        .Using((projection, anEvent, context) =>
                        {
                            projection.Category = anEvent.Category;
                            projection.AddedBy = (string) context.EventHeaders["UserName"];
                        });

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().WriteWithHeaders(
                    new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    },
                    new Dictionary<string, object>
                    {
                        ["UserName"] = "Pavel"
                    }));
            }

            [Fact]
            public void Then_it_should_use_the_header()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    var entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.AddedBy.Should().Be("Pavel");
                }
            }
        }

        public class When_a_transaction_has_a_header : Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_transaction_has_a_header()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(anEvent => anEvent.ProductKey)
                        .Using((projection, anEvent, context) =>
                        {
                            projection.Category = anEvent.Category;
                            projection.AddedBy = (string) context.TransactionHeaders["UserName"];
                        });

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(
                    new Transaction
                    {
                        Events = new[]
                        {
                            new EventEnvelope
                            {
                                Body = new ProductAddedToCatalogEvent
                                {
                                    ProductKey = "c350E",
                                    Category = "Hybrid"
                                }
                            }
                        },
                        Headers = new Dictionary<string, object>
                        {
                            ["UserName"] = "Pavel"
                        }
                    }));
            }

            [Fact]
            public void Then_it_should_use_the_header()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    var entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Should().NotBeNull();
                    entry.AddedBy.Should().Be("Pavel");
                }
            }
        }

        public class When_retrying_a_transaction_that_was_already_handled :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private ReadOnlyCollection<Transaction> transactions;

            public When_retrying_a_transaction_that_was_already_handled()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((p, @event) =>
                        {
                            if (p.Category == @event.Category)
                            {
                                throw new InvalidOperationException("The event should not be handled twice");
                            }

                            p.Category = @event.Category;
                            p.Name = @event.Name;
                        });

                    Transaction transaction = new TransactionBuilder()
                        .WithCheckpointNumber(5)
                        .WithEvent(new ProductAddedToCatalogEvent
                        {
                            Category = "some category",
                            Name = $"some product",
                            ProductKey = $"some key",
                        })
                        .Build();

                    transactions = new List<Transaction> {transaction}.AsReadOnly();

                    StartProjecting();
                });

                When(async () =>
                {
                    await Subject.Handle(transactions, CancellationToken.None);
                    await Subject.Handle(transactions, CancellationToken.None);
                });
            }

            [Fact]
            public void Then_it_should_not_handle_the_transactions()
            {
                ProjectionException.Should().BeNull();
            }
        }

        public class When_cancelling_in_the_middle_of_a_batch :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private CancellationTokenSource cancellationRequest = new CancellationTokenSource();

            public When_cancelling_in_the_middle_of_a_batch()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .OverwritingDuplicates()
                        .Using((p, @event) =>
                        {
                            p.Name = @event.Name;
                            p.Category = @event.Category;
                        });

                    Events
                        .Map<CategoryDiscontinuedEvent>()
                        .As((@event, context) =>
                        {
                            cancellationRequest.Cancel();
                        });

                    StartProjecting(cancellationRequest.Token);
                });

                When(async () =>
                {
                    await The<MemoryEventSource>().Write(
                        new ProductAddedToCatalogEvent
                        {
                            Category = "TheCategory",
                            ProductKey = "TheProduct",
                        });

                    await The<MemoryEventSource>().Write(
                        new CategoryDiscontinuedEvent()
                        {
                            Category = "TheCategory",
                        });

                    await The<MemoryEventSource>().Write(
                        new ProductAddedToCatalogEvent
                        {
                            Category = "AnotherCategory",
                            ProductKey = "TheProduct",
                        });
                });
            }

            [Fact]
            public void Then_it_should_rollback_the_transaction()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    session.Query<ProductCatalogEntry>().Single().Category.Should().Be("TheCategory");
                }
            }
        }

        public class When_canceling_before_the_first_transaction_is_processed :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private CancellationTokenSource cancellationRequest = new CancellationTokenSource();

            public When_canceling_before_the_first_transaction_is_processed()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .OverwritingDuplicates()
                        .Using((p, @event) =>
                        {
                            p.Name = @event.Name;
                            p.Category = @event.Category;
                        });

                    StartProjecting(cancellationRequest.Token);

                    cancellationRequest.Cancel();
                });

                When(async () =>
                {
                    await The<MemoryEventSource>().Write(
                        new ProductAddedToCatalogEvent
                        {
                            Category = "TheCategory",
                            ProductKey = "TheProduct",
                        });
                });
            }

            [Fact]
            public void Then_it_should_not_handle_any_events_anymore()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    session.Query<ProductCatalogEntry>().SingleOrDefault().Should().BeNull();
                }
            }
        }

        #endregion

        #region Filtering

        public class When_the_projection_being_updated_doesnt_pass_the_filter :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_the_projection_being_updated_doesnt_pass_the_filter()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsUpdateOf(@event => @event.ProductKey)
                        .Using((product, @event, _) => product.Category = @event.Category);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        session.Save(new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Old Value",
                            Corrupted = true
                        });

                        session.Flush();
                    }

                    Subject.Filter = projection => !projection.Corrupted; 

                    StartProjecting();
                });

                When(() => The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                {
                    ProductKey = "c350E",
                    Category = "New Value"
                }));
            }

            [Fact]
            public void Then_it_should_not_update_the_projection()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry entry = session.Get<ProductCatalogEntry>("c350E");
                    entry.Category.Should().Be("Old Value");
                }
            }
        }

        #endregion

        #region Child Projectors

        public class When_there_is_a_child_projector :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private readonly List<ChildProjectionState> childProjectionStates = new List<ChildProjectionState>();

            public When_there_is_a_child_projector()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(anEvent => anEvent.ProductKey)
                        .Using((entry, anEvent) => entry.Category = anEvent.Category);

                    Events.Map<ProductAddedToCatalogEvent>().As((anEvent, context) =>
                    {
                        ProductCatalogChildEntry childEntry1 = context.Session.Get<ProductCatalogChildEntry>("c350E");
                        ProductCatalogChildEntry childEntry2 = context.Session.Get<ProductCatalogChildEntry>("c350F");

                        childProjectionStates.Add(new ChildProjectionState
                        {
                            Entry1Exists = childEntry1 != null,
                            Entry2Exists = childEntry2 != null
                        });
                    });

                    var childMapBuilder = new EventMapBuilder<ProductCatalogChildEntry, string, NHibernateProjectionContext>();

                    childMapBuilder.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(anEvent => anEvent.ProductKey)
                        .Using((entry, anEvent) => entry.Category = anEvent.Category);

                    AddChildProjector(new NHibernateChildProjector<ProductCatalogChildEntry, string>(
                        childMapBuilder, (childEntry, id) => childEntry.Id = id));
                });

                When(async () =>
                {
                    StartProjecting();

                    var transaction1 = new Transaction
                    {
                        Events = new[]
                        {
                            new EventEnvelope
                            {
                                Body = new ProductAddedToCatalogEvent
                                {
                                    ProductKey = "c350E",
                                    Category = "Hybrid"
                                }
                            }
                        }
                    };

                    var transaction2 = new Transaction
                    {
                        Events = new[]
                        {
                            new EventEnvelope
                            {
                                Body = new ProductAddedToCatalogEvent
                                {
                                    ProductKey = "c350F",
                                    Category = "Gas"
                                }
                            }
                        }
                    };

                    await The<MemoryEventSource>().Write(transaction1, transaction2);
                });
            }

            [Fact]
            public void Then_the_parent_projector_should_project_all_the_transactions()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogEntry parentEntry1 = session.Get<ProductCatalogEntry>("c350E");
                    parentEntry1.Should().NotBeNull();
                    parentEntry1.Category.Should().Be("Hybrid");

                    ProductCatalogEntry parentEntry2 = session.Get<ProductCatalogEntry>("c350F");
                    parentEntry2.Should().NotBeNull();
                    parentEntry2.Category.Should().Be("Gas");
                }
            }

            [Fact]
            public void Then_the_child_projector_should_project_all_the_transactions()
            {
                using (ISession session = The<ISessionFactory>().OpenSession())
                {
                    ProductCatalogChildEntry childEntry1 = session.Get<ProductCatalogChildEntry>("c350E");
                    childEntry1.Should().NotBeNull();
                    childEntry1.Category.Should().Be("Hybrid");

                    ProductCatalogChildEntry childEntry2 = session.Get<ProductCatalogChildEntry>("c350F");
                    childEntry2.Should().NotBeNull();
                    childEntry2.Category.Should().Be("Gas");
                }
            }

            [Fact]
            public void Then_the_child_projector_should_process_each_transaction_before_the_parent_projector()
            {
                childProjectionStates[0].Should().BeEquivalentTo(new ChildProjectionState
                {
                    Entry1Exists = true,
                    Entry2Exists = false
                });

                childProjectionStates[1].Should().BeEquivalentTo(new ChildProjectionState
                {
                    Entry1Exists = true,
                    Entry2Exists = true
                });
            }

            private class ChildProjectionState
            {
                public bool Entry1Exists { get; set; }
                public bool Entry2Exists { get; set; }
            }
        }

        public class When_a_child_projector_has_its_own_cache :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private LruProjectionCache<ProductCatalogChildEntry, string> childCache;

            public When_a_child_projector_has_its_own_cache()
            {
                Given(() =>
                {
                    var childMapBuilder = new EventMapBuilder<ProductCatalogChildEntry, string, NHibernateProjectionContext>();

                    childMapBuilder.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(anEvent => anEvent.ProductKey)
                        .Using((entry, anEvent) => entry.Category = anEvent.Category);

                    childCache = new LruProjectionCache<ProductCatalogChildEntry, string>(1000, 1.Hours(), 2.Hours(), e => e.Id,
                        () => DateTime.UtcNow);

                    AddChildProjector(new NHibernateChildProjector<ProductCatalogChildEntry, string>(
                        childMapBuilder, (childEntry, id) => childEntry.Id = id)
                    {
                        Cache = childCache
                    });

                    StartProjecting();
                });

                When(async () =>
                {
                    await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                    {
                        ProductKey = "c350E",
                        Category = "Hybrid"
                    });
                });
            }

            [Fact]
            public async Task It_should_add_the_item_to_the_cache()
            {
                var cachedItem = await childCache.Get("c350E", () => Task.FromResult<ProductCatalogChildEntry>(null));
                cachedItem.Should().NotBeNull();
                cachedItem.Category.Should().Be("Hybrid");
            }
        }

        #endregion

        #region Exception Handling

        public class When_any_exception_is_thrown :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_any_exception_is_thrown()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(@event => @event.ProductKey)
                        .Using((p, @event) =>
                        {
                            p.Category = @event.Category;
                            p.Name = @event.Name;
                        });

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) => throw new InvalidOperationException());

                    StartProjecting();
                });

                When(async () =>
                {
                    await The<MemoryEventSource>()
                        .Write(new TransactionBuilder()
                            .WithEvent(new ProductAddedToCatalogEvent
                            {
                                Category = "some category",
                                Name = "some product",
                                ProductKey = "some key",
                            })
                            .WithEvent(new CategoryDiscontinuedEvent
                            {
                                Category = "some category"
                            })
                            .Build());
                });
            }

            [Fact]
            public void Then_it_should_rollback_the_transaction()
            {
                using (var session = The<ISessionFactory>().OpenSession())
                {
                    session.QueryOver<ProductCatalogEntry>().List().Should().BeEmpty();
                }
            }
        }


        public class When_a_projection_exception_occurs :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_projection_exception_occurs()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    Cache.Add(new ProductCatalogEntry
                    {
                        Id = "c350E"
                    });

                    StartProjecting();
                });

                When(async () =>
                {
                    try
                    {
                        await The<MemoryEventSource>().Write(new CategoryDiscontinuedEvent
                        {
                            Category = "Hybrids"
                        });
                    }
                    catch
                    {
                        // ignored
                    }
                });
            }

            [Fact]
            public void Then_it_should_completely_clear_the_cache()
            {
                Cache.CurrentCount.Should().Be(0);
            }
        }

        public class When_a_database_exception_occurs : Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_a_database_exception_occurs()
            {
                Given(() =>
                {
                    Events.Map<ProductAddedToCatalogEvent>()
                        .AsCreateOf(e => e.ProductKey)
                        .Using((p, e) => p.Name = e.Name);

                    using (ISession session = The<ISessionFactory>().OpenSession())
                    {
                        var entry = new ProductCatalogEntry
                        {
                            Id = "c350E",
                            Category = "Hybrids",
                            Name = "DuplicateNameThatWillThrowUniqueConstraintException"
                        };

                        session.Save(entry);
                        session.Flush();
                    }

                    Cache.Add(new ProductCatalogEntry
                    {
                        Id = "SomeOtherCachedId"
                    });

                    StartProjecting();
                });

                When(async () =>
                {
                    try
                    {
                        await The<MemoryEventSource>().Write(new ProductAddedToCatalogEvent
                        {
                            ProductKey = "i3",
                            Category = "Hybrids",
                            Name = "DuplicateNameThatWillThrowUniqueConstraintException"
                        });
                    }
                    catch
                    {
                        // Ignored since we were expecting it.
                    }
                });
            }

            [Fact]
            public void Then_it_should_completely_clear_the_cache()
            {
                Cache.CurrentCount.Should().Be(0);
            }
        }

        public class When_the_exception_handler_requests_to_ignore_any_exceptions :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_the_exception_handler_requests_to_ignore_any_exceptions()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    StartProjecting();

                    Subject.ExceptionHandler = (exception, attempts, cancellationToken) =>
                    {
                        return Task.FromResult(ExceptionResolution.Ignore);
                    };
                });

                When(() => The<MemoryEventSource>().Write(new TransactionBuilder()
                    .WithEvent(new CategoryDiscontinuedEvent())
                    .Build()));
            }

            [Fact]
            public void Then_it_should_not_throw_any_projection_exceptions()
            {
                ProjectionException.Should().BeNull();
            }
        }


        public class When_event_handling_fails_and_no_exception_policy_has_been_setup :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            public When_event_handling_fails_and_no_exception_policy_has_been_setup()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    StartProjecting();

                    UseThe(new Transaction
                    {
                        Events = new[]
                        {
                            UseThe(new EventEnvelope
                            {
                                Body = new CategoryDiscontinuedEvent()
                            })
                        }
                    });
                });

                When(() => The<MemoryEventSource>().Write(The<Transaction>()));
            }

            [Fact]
            public void Then_it_should_throw_projection_exception_with_the_original_exception_nested()
            {
                ProjectionException.Should().BeOfType<ProjectionException>()
                    .Which.InnerException.Should().BeOfType<InvalidOperationException>();
            }

            [Fact]
            public void Then_it_should_identify_the_projector_via_the_projection_type()
            {
                ProjectionException.Should().BeOfType<ProjectionException>()
                    .Which.Projector.Should().Be(typeof(ProductCatalogEntry).ToString());
            }

            [Fact]
            public void Then_it_should_include_the_current_event()
            {
                ProjectionException.Should().BeOfType<ProjectionException>()
                    .Which.CurrentEvent.Should().BeSameAs(The<EventEnvelope>());
            }

            [Fact]
            public void Then_it_should_include_the_current_transaction_batch()
            {
                ProjectionException.Should().BeOfType<ProjectionException>()
                    .Which.TransactionBatch.Should().AllBeEquivalentTo(The<Transaction>());
            }
        }

        public class When_the_exception_handler_requests_a_retry_for_a_number_of_times_and_then_aborts :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private int numberOfAttempts;
            private const int ExpectedNumberOfRetries = 3;

            public When_the_exception_handler_requests_a_retry_for_a_number_of_times_and_then_aborts()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    StartProjecting();

                    Subject.ExceptionHandler = (exception, attempts, cancellationToken) =>
                    {
                        numberOfAttempts = attempts;
                        if (attempts <= ExpectedNumberOfRetries)
                        {
                            return Task.FromResult(ExceptionResolution.Retry);
                        }

                        return Task.FromResult(ExceptionResolution.Abort);
                    };
                });

                When(() => The<MemoryEventSource>().Write(new TransactionBuilder()
                    .WithEvent(new CategoryDiscontinuedEvent())
                    .Build()));
            }

            [Fact]
            public void Then_it_should_keep_retrying_as_long_as_requested()
            {
                numberOfAttempts.Should().Be(ExpectedNumberOfRetries + 1);
            }

            [Fact]
            public void Then_it_should_eventually_rethrow_the_projection_exception()
            {
                ProjectionException.Should().NotBeNull();
            }
        }

        public class When_the_exception_handler_requests_a_retry_of_individual_commits_and_then_ignores_the_exception :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private readonly List<Transaction[]> receivedTransactions = new List<Transaction[]>();
            private Transaction transaction1;
            private Transaction transaction2;

            public When_the_exception_handler_requests_a_retry_of_individual_commits_and_then_ignores_the_exception()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    StartProjecting();

                    Subject.ExceptionHandler = async (exception, attempts, cancellationToken) =>
                    {
                        receivedTransactions.Add(exception.TransactionBatch.ToArray());

                        if (attempts == 1)
                        {
                            return ExceptionResolution.RetryIndividual;
                        }
                        else if (attempts == 2)
                        {
                            return ExceptionResolution.Ignore;
                        }
                        else
                        {
                            return ExceptionResolution.Abort;
                        }
                    };
                });

                When(() =>
                {
                    transaction1 = new TransactionBuilder()
                        .WithCheckpointNumber(12)
                        .WithEvent(new CategoryDiscontinuedEvent())
                        .Build();

                    transaction2 = new TransactionBuilder()
                        .WithCheckpointNumber(34)
                        .WithEvent(new CategoryDiscontinuedEvent())
                        .Build();

                    return The<MemoryEventSource>().Write(transaction1, transaction2);
                });
            }

            [Fact]
            public void Then_it_should_first_receive_the_full_batch_and_then_the_individual_transactions()
            {
                receivedTransactions.Should().BeEquivalentTo(
                    new[] {transaction1, transaction2},
                    new[] {transaction1},
                    new[] {transaction2});
            }
        }

        public class When_the_exception_handler_requests_a_retry_of_individual_commits_and_then_aborts :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private readonly List<Transaction[]> receivedTransactions = new List<Transaction[]>();
            private Transaction transaction1;
            private Transaction transaction2;

            public When_the_exception_handler_requests_a_retry_of_individual_commits_and_then_aborts()
            {
                Given(() =>
                {
                    UseThe(new InvalidOperationException());

                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw The<InvalidOperationException>();
                    });

                    StartProjecting();

                    Subject.ExceptionHandler = (exception, attempts, cancellationToken) =>
                    {
                        receivedTransactions.Add(exception.TransactionBatch.ToArray());

                        if (attempts == 1)
                        {
                            return Task.FromResult(ExceptionResolution.RetryIndividual);
                        }
                        else 
                        {
                            return Task.FromResult(ExceptionResolution.Abort);
                        }
                    };
                });

                When(() =>
                {
                    transaction1 = new TransactionBuilder()
                        .WithCheckpointNumber(12)
                        .WithEvent(new CategoryDiscontinuedEvent())
                        .Build();

                    transaction2 = new TransactionBuilder()
                        .WithCheckpointNumber(34)
                        .WithEvent(new CategoryDiscontinuedEvent())
                        .Build();

                    return The<MemoryEventSource>().Write(transaction1, transaction2);
                });
            }

            [Fact]
            public void Then_it_should_first_receive_the_full_batch_and_then_the_individual_transaction()
            {
                receivedTransactions.Should().BeEquivalentTo(
                    new[] {transaction1, transaction2},
                    new[] {transaction1});
            }

            [Fact]
            public void And_it_should_bubble_up_the_successive_projection_exception()
            {
                ProjectionException.Should().BeOfType<ProjectionException>().Which.InnerException.Should()
                    .BeOfType<InvalidOperationException>();
            }
        }

        public class When_the_processing_request_is_canceled_while_the_exception_policy_is_being_executed :
            Given_a_sqlite_projector_with_an_in_memory_event_source
        {
            private TimeSpan exceptionHandlingTime;
            private CancellationTokenSource cancellation = new CancellationTokenSource();
            private ManualResetEventSlim exceptionBeingHandled = new ManualResetEventSlim();

            public When_the_processing_request_is_canceled_while_the_exception_policy_is_being_executed()
            {
                Given(() =>
                {
                    Events.Map<CategoryDiscontinuedEvent>().As((@event, context) =>
                    {
                        throw new InvalidOperationException();
                    });

                    StartProjecting();

                    Subject.ExceptionHandler = async (exception, attempts, cancellationToken) =>
                    {
                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        
                        exceptionBeingHandled.Set();

                        await Task.Delay(10.Seconds(), cancellationToken);

                        exceptionHandlingTime = stopwatch.Elapsed;

                        return ExceptionResolution.Ignore;
                    };
                });

                When(() =>
                {
                    The<MemoryEventSource>().WriteWithoutWaiting(new TransactionBuilder()
                        .WithEvent(new CategoryDiscontinuedEvent())
                        .Build());

                    exceptionBeingHandled.Wait();
                    cancellation.Cancel();
                });
            }

            [Fact]
            public void Then_it_should_cancel_any_long_running_code_in_the_exception_handler()
            {
                exceptionHandlingTime.Should().BeLessThan(1.Seconds());
            }
        }

        #endregion

        #region Supporting Types

        public class ProductCatalogEntry
        {
            public virtual string Id { get; set; }
            public virtual string Category { get; set; }
            public virtual string AddedBy { get; set; }
            public virtual string Name { get; set; }
            public virtual bool Corrupted { get; set; }
        }

        internal class ProductCatalogEntryClassMap : ClassMap<ProductCatalogEntry>
        {
            public ProductCatalogEntryClassMap()
            {
                Id(p => p.Id).Not.Nullable().Length(100);
                Map(p => p.Category).Nullable().Length(100);
                Map(p => p.AddedBy).Nullable().Length(100);
                Map(p => p.Name).Nullable().Unique();
                Map(p => p.Corrupted).Nullable();
            }
        }

        public class ProductCatalogChildEntry
        {
            public virtual string Id { get; set; }
            public virtual string Category { get; set; }
        }

        internal class ProductCatalogChildEntryClassMap : ClassMap<ProductCatalogChildEntry>
        {
            public ProductCatalogChildEntryClassMap()
            {
                Id(p => p.Id).Not.Nullable().Length(100);
                Map(p => p.Category).Nullable().Length(100);
            }
        }

        public class ProductAddedToCatalogEvent
        {
            public string ProductKey { get; set; }
            public string Category { get; set; }
            public string Name { get; set; }
        }

        public class ProductMovedToCatalogEvent
        {
            public string ProductKey { get; set; }
            public string Category { get; set; }
        }

        public class ProductDiscontinuedEvent
        {
            public string ProductKey { get; set; }
        }

        public class CategoryDiscontinuedEvent
        {
            public string Category { get; set; }
        }

        public class TransactionBuilder
        {
            private readonly List<object> events = new List<object>();
            private long? checkpointNumber;

            public TransactionBuilder WithEvent(object @event)
            {
                events.Add(@event);
                return this;
            }

            public Transaction Build()
            {
                Transaction transaction = new Transaction
                {
                    Events = events.Select(e => new EventEnvelope
                    {
                        Body = e
                    }).ToArray()
                };

                if (checkpointNumber.HasValue)
                {
                    transaction.Checkpoint = checkpointNumber.Value;
                }

                return transaction;
            }

            public TransactionBuilder WithCheckpointNumber(long checkpointNumber)
            {
                this.checkpointNumber = checkpointNumber;
                return this;
            }
        }

        #endregion
    }
}