using Microsoft.ServiceBus.Messaging;

namespace Akka.Streams.Azure.EventHub
{
    public sealed class SingelProcessorFactory : IEventProcessorFactory
    {
        private readonly IEventProcessor _processor;

        public SingelProcessorFactory(IEventProcessor processor)
        {
            _processor = processor;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context) => _processor;
    }
}
