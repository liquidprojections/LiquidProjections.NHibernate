using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentNHibernate.Utils;
using LiquidProjections.ExampleHost.Events;
using LiquidProjections.ExampleHost.Projections;
using LiquidProjections.NHibernate;
using NHibernate;
using NHibernate.Linq;

namespace LiquidProjections.ExampleHost
{
    public class CountsProjector
    {
        private readonly Dispatcher dispatcher;
        private readonly Func<ISession> sessionFactory;
        private readonly Stopwatch stopwatch = new Stopwatch();
        private NHibernateProjector<DocumentCountProjection, string, ProjectorState> documentProjector;
        private NHibernateChildProjector<CountryLookup, string> countryProjector;
        private readonly LruProjectionCache<DocumentCountProjection, string> cache;
        private long eventCount;

        public CountsProjector(Dispatcher dispatcher, Func<ISession> sessionFactory)
        {
            this.dispatcher = dispatcher;
            this.sessionFactory = sessionFactory;
            cache = new LruProjectionCache<DocumentCountProjection, string>(
                20000, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2), p => p.Id, () => DateTime.UtcNow);

            BuildCountryProjector();
            BuildDocumentProjector();
        }

        private void BuildCountryProjector()
        {
            var countryMapBuilder = new EventMapBuilder<CountryLookup, string, NHibernateProjectionContext>();

            countryMapBuilder
                .Map<CountryRegisteredEvent>()
                .AsCreateOf(anEvent => anEvent.Code)
                .Using((country, anEvent) => country.Name = anEvent.Name);

            countryProjector = new NHibernateChildProjector<CountryLookup, string>(countryMapBuilder, (lookup, identity) => lookup.Id = identity)
            {
                // We use a local LRU cache for the countries to avoid unnecessary database lookups. 
                Cache = new LruProjectionCache<CountryLookup, string>(
                    20000, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2), p => p.Id, () => DateTime.UtcNow)
            };
        }

        private void BuildDocumentProjector()
        {
            var documentMapBuilder = new EventMapBuilder<DocumentCountProjection, string, NHibernateProjectionContext>();
            
            documentMapBuilder
                .Map<WarrantAssignedEvent>()
                .AsCreateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "Warrant";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<CertificateIssuedEvent>()
                .AsCreateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "Certificate";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<ConstitutionEstablishedEvent>()
                .AsUpdateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "Constitution";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<LicenseGrantedEvent>()
                .AsCreateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "Audit";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<ContractNegotiatedEvent>()
                .AsCreateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "Task";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<BondIssuedEvent>()
                .AsCreateOf(anEvent => anEvent.Number)
                .Using((document, anEvent) =>
                {
                    document.Type = "IsolationCertificate";
                    document.Kind = anEvent.Kind;
                    document.Country = anEvent.Country;
                    document.State = anEvent.InitialState;
                });

            documentMapBuilder
                .Map<AreaRestrictedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.RestrictedArea = anEvent.Area);

            documentMapBuilder
                .Map<AreaRestrictionCancelledEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.RestrictedArea = null);

            documentMapBuilder
                .Map<StateTransitionedEvent>()
                .When(anEvent => anEvent.State != "Closed")
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.State = anEvent.State);

            documentMapBuilder
                .Map<StateRevertedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.State = anEvent.State);

            documentMapBuilder
                .Map<DocumentArchivedEvent>()
                .AsDeleteOf(anEvent => anEvent.DocumentNumber);

            documentMapBuilder
                .Map<CountryCorrectedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.Country = anEvent.Country);

            documentMapBuilder
                .Map<NextReviewScheduledEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.NextReviewAt = anEvent.NextReviewAt);

            documentMapBuilder
                .Map<LifetimeRestrictedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    // For this particular document, we're throwing an exception to simulate the effect of the exception policy
                    // set-up in the OnException method.
                    if (document.Id == "0000000047")
                    {
                        throw new InvalidOperationException("Simulating an exception");
                    }
                    
                    document.LifetimePeriodEnd = anEvent.PeriodEnd;
                });

            documentMapBuilder
                .Map<LifetimeRestrictionRemovedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) => document.LifetimePeriodEnd = null);

            documentMapBuilder
                .Map<ValidityPeriodPlannedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    ValidityPeriod period = document.GetOrAddPeriod(anEvent.Sequence);
                    period.From = anEvent.From;
                    period.To = anEvent.To;
                });

            documentMapBuilder
                .Map<ValidityPeriodResetEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    ValidityPeriod period = document.GetOrAddPeriod(anEvent.Sequence);
                    period.From = null;
                    period.To = null;
                });

            documentMapBuilder
                .Map<ValidityPeriodApprovedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    ValidityPeriod period = document.GetOrAddPeriod(anEvent.Sequence);
                    period.Status = "Valid";

                    ValidityPeriod lastValidPeriod = document.Periods.LastOrDefault(aPeriod => aPeriod.Status == "Valid");

                    ValidityPeriod[] contiguousPeriods = GetPreviousContiguousValidPeriods(document.Periods, lastValidPeriod)
                        .OrderBy(x => x.Sequence).ToArray();

                    document.StartDateTime = contiguousPeriods.Any() ? contiguousPeriods.First().From : DateTime.MinValue;
                    document.EndDateTime = contiguousPeriods.Any() ? contiguousPeriods.Last().To : DateTime.MaxValue;
                });

            documentMapBuilder
                .Map<ValidityPeriodClosedEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    ValidityPeriod period = document.GetOrAddPeriod(anEvent.Sequence);
                    period.Status = "Closed";
                    period.To = anEvent.ClosedAt;
                });

            documentMapBuilder
                .Map<ValidityPeriodCanceledEvent>()
                .AsUpdateOf(anEvent => anEvent.DocumentNumber)
                .Using((document, anEvent) =>
                {
                    ValidityPeriod period = document.GetOrAddPeriod(anEvent.Sequence);
                    period.Status = "Canceled";
                });

            documentProjector = new NHibernateProjector<DocumentCountProjection, string, ProjectorState>(
                sessionFactory,
                documentMapBuilder,
                (projection, identity) => projection.Id = identity,
                new[] { countryProjector })
            {
                BatchSize = 20,
                Cache = cache,
                ExceptionHandler = OnException,
                
                // Make sure that we no longer process corrupted projections. 
                Filter = p => !p.Corrupt
            };
        }

        private static IEnumerable<ValidityPeriod> GetPreviousContiguousValidPeriods(IEnumerable<ValidityPeriod> allPeriods,
            ValidityPeriod period)
        {
            while (period != null)
            {
                yield return period;

                ValidityPeriod previousPeriod =
                    allPeriods.SingleOrDefault(p => p.Status == "Valid" && p.To.Equals(period.From) && p.Sequence != period.Sequence);

                period = previousPeriod;
            }
        }

        private async Task<NHibernate.ExceptionResolution> OnException(ProjectionException exception, int attempts, CancellationToken cancellationToken)
        {
            if (IsTransient(attempts))
            {
                return await OnTransientException(attempts);
            }
            else
            {
                return OnNonTransientException(exception);
            }
        }

        private bool IsTransient(int attempts)
        {
            // For this example, we pretend the first three attempts are transient and treat and successive ones as non-transient.
            return attempts < 3;
        }

        private static async Task<NHibernate.ExceptionResolution> OnTransientException(int attempts)
        {
            if (attempts < 3)
            {
                // If this is one of the first three attempts, just retry the entire batch of transactions.
                // Maybe it was a transient exception
                await Task.Delay(TimeSpan.FromSeconds(attempts ^ 2));

                return NHibernate.ExceptionResolution.Retry;
            }
            else
            {
                return NHibernate.ExceptionResolution.Abort;
            }
        }

        private NHibernate.ExceptionResolution OnNonTransientException(ProjectionException exception)
        {
            if (exception.TransactionBatch.Count > 1)
            {
                // If we have more than one transition, let's try to run them one by one to trace down the one that really fails.
                return NHibernate.ExceptionResolution.RetryIndividual;
            }
            else
            {
                // So we found the failing transaction. So let's mark it as corrupt and ignore the transaction.
                Console.WriteLine(exception.Message);

                using (var session = sessionFactory())
                {
                    string failingStreamId = exception.TransactionBatch.Single().StreamId;
                    if (failingStreamId != null)
                    {
                        var projection = session.Query<DocumentCountProjection>().SingleOrDefault(x => x.Id == failingStreamId);
                        if (projection != null)
                        {
                            projection.Corrupt = true;
                            cache.Clear();
                            session.Flush();
                        }
                    }
                }

                return NHibernate.ExceptionResolution.Ignore;
            }
        }

#pragma warning disable 1998
        public void Start()
#pragma warning restore 1998
        {
            long? lastCheckpoint = documentProjector.GetLastCheckpoint();

            dispatcher.Subscribe(lastCheckpoint, async (transactions, info) =>
            {
                await documentProjector.Handle(transactions, info.CancellationToken ?? CancellationToken.None);

                ReportStatistics(transactions);
            });
        }

        private void ReportStatistics(IReadOnlyList<Transaction> transactions)
        {
            if (!stopwatch.IsRunning)
            {
                stopwatch.Start();
            }

            eventCount += transactions.Sum(t => t.Events.Count);

            long elapsedTotalSeconds = (long) stopwatch.Elapsed.TotalSeconds;

            if (elapsedTotalSeconds > 0)
            {
                int ratePerSecond = (int) (eventCount / elapsedTotalSeconds);

                Console.WriteLine($"{DateTime.Now}: Processed {eventCount} events " +
                                  $"(rate: {ratePerSecond}/second, hits: {cache.Hits}, Misses: {cache.Misses})");
            }
        }
    }
}
