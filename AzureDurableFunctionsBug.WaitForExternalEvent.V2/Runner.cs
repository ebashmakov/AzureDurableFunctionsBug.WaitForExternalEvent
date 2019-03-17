using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace AzureDurableFunctionsBug.WaitForExternalEvent.V2
{
    public class Runner
    {
        private const string EventsQueueName = "events";

        [FunctionName("StartOrchestration")]
        public static async Task StartOrchestration(
            [HttpTrigger(AuthorizationLevel.Anonymous, "POST", Route = "start")] HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient client,
            [Queue(EventsQueueName)] IAsyncCollector<SomeOrchestrationEventPair> eventsQueue,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            var events = Enumerable.Range(0, 5)
                .Select(x => new SomeEvent { Id = Guid.NewGuid().ToString() })
                .ToList();
            var orchestration = new SomeOrchestration
            {
                Id = Guid.NewGuid().ToString(),
                ExpectedEvents = events
            };

            logger.LogInformation("{Orchestration} is going to wait the following events:\n{Events}",
                orchestration.Id,
                orchestration.AllEventIds);
            await client.StartNewAsync(nameof(WaitForEvents), orchestration.Id, orchestration);

            foreach (var e in events)
            {
                await eventsQueue.AddAsync(new SomeOrchestrationEventPair
                {
                    OrchestrationId = orchestration.Id,
                    Event = e
                }, cancellationToken);
            }
        }

        [FunctionName(nameof(SendEvent))]
        public static async Task SendEvent(
            [QueueTrigger(EventsQueueName)] SomeOrchestrationEventPair data,
            [OrchestrationClient] DurableOrchestrationClient client,
            ExecutionContext executionContext,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            await client.RaiseEventAsync(data.OrchestrationId, data.Event.Id, data.Event);
            logger.LogInformation("{Event} is sent to {Orchestration}.", data.Event.Id, data.OrchestrationId);
        }

        [FunctionName(nameof(WaitForEvents))]
        public static async Task WaitForEvents(
            [OrchestrationTrigger] DurableOrchestrationContext context,
            CancellationToken cancellationToken,
            ILogger logger)
        {
            var orchestration = context.GetInput<SomeOrchestration>();
            logger.LogInformation("{Orchestration} is started.", orchestration.Id);

            var externalEvents = orchestration.ExpectedEvents.Select(x => context.WaitForExternalEvent<SomeEvent>(x.Id, TimeSpan.FromMinutes(1)));
            logger.LogInformation("{Orchestration} is going to wait for events:\n{Events}", orchestration.Id, orchestration.AllEventIds);

            await Task.WhenAll(externalEvents);
            logger.LogInformation("{Orchestration} is finished.", orchestration.Id);
        }

        public class SomeOrchestration
        {
            public string Id { get; set; }

            public IEnumerable<SomeEvent> ExpectedEvents { get; set; } = new List<SomeEvent>();

            public string AllEventIds => ExpectedEvents != null
                ? string.Join("\n", ExpectedEvents.Select(x => x.Id))
                : null;
        }

        public class SomeEvent
        {
            public string Id { get; set; }
        }

        public class SomeOrchestrationEventPair
        {
            public string OrchestrationId { get; set; }
            public SomeEvent Event { get; set; }
        }
    }
}
