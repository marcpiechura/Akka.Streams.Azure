using Microsoft.ServiceBus.Messaging;

namespace Akka.Streams.Azure.EventHub
{
    /// <summary>
    /// A processor factory that always returns the given processor
    /// </summary>
    public sealed class SingelProcessorFactory : IEventProcessorFactory
    {
        private readonly IEventProcessor _processor;

        /// <summary>
        /// Creates a new instance of the <see cref="SingelProcessorFactory"/>
        /// </summary>
        /// <param name="processor">The processor</param>
        public SingelProcessorFactory(IEventProcessor processor)
        {
            _processor = processor;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context) => _processor;
    }
}
