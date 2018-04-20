using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using LiquidProjections.ExampleHost.Projections;
using NHibernate;
using NHibernate.Linq;

namespace LiquidProjections.ExampleHost
{
    [RoutePrefix("Statistics")]
    public class StatisticsController : ApiController
    {
        private readonly Func<ISession> sessionFactory;

        public StatisticsController(Func<ISession> sessionFactory)
        {
            this.sessionFactory = sessionFactory;
        }

        [Route("{CountsPerState}")]
        [HttpGet]
        public dynamic GetCountsPerState(Guid country, string kind)
        {
            using (var session = sessionFactory())
            {
                string countryName = session.Query<CountryLookup>().Single(c => c.Id == country.ToString()).Name;

                var staticResults =
                    from document in session.Query<DocumentCountProjection>()
                    let dynamicStates = new[] {"Active"}
                    where !dynamicStates.Contains(document.State) && !document.Corrupt
                    where document.Kind == kind && document.Country == country
                    group document by new {document.Country, AreaRestrictions = document.RestrictedArea, document.Kind, document.State}
                    into grp
                    select new Result
                    {
                        Kind = grp.Key.Kind,
                        Country = grp.Key.Country,
                        CountryName = countryName,
                        AreaRestrictions = grp.Key.AreaRestrictions,
                        State = grp.Key.State,
                        Count = grp.Count()
                    };

                var evaluator = new RealtimeStateEvaluator();

                var dynamicResults =
                    from document in session.Query<DocumentCountProjection>()
                    let dynamicStates = new[] {"Active"}
                    where document.Kind == kind && document.Country == country
                    where !dynamicStates.Contains(document.State) && !document.Corrupt
                    select document;

                var results = new List<Result>(staticResults);

                foreach (var projection in dynamicResults.ToArray())
                {
                    var actualState = evaluator.Evaluate(new RealtimeStateEvaluationContext
                    {
                        StaticState = projection.State,
                        Country = projection.Country,
                        NextReviewAt = projection.NextReviewAt,
                        PlannedPeriod = new ValidityPeriod(projection.StartDateTime, projection.EndDateTime),
                        ExpirationDateTime = projection.LifetimePeriodEnd
                    });

                    var result = staticResults.SingleOrDefault(r => r.State == actualState);
                    if (result == null)
                    {
                        result = new Result
                        {
                            Kind = kind,
                            Country = country,
                            CountryName = countryName,
                            AreaRestrictions = projection.RestrictedArea, 
                            State = actualState,
                            Count = 1
                        };

                        results.Add(result);
                    }

                    result.Count++;
                }

                return results;
            }
        }

        public class Result
        {
            public Guid Country { get; set; }

            public string CountryName { get; set; }

            public string AreaRestrictions { get; set; }

            public string Kind { get; set; }

            public string State { get; set; }

            public int Count { get; set; }
        }
    }
}