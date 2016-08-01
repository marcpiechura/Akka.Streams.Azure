using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Azure.Utils;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Microsoft.ServiceBus.Messaging;

namespace Akka.Streams.Azure.ServiceBus
{
    /// <summary>
    /// a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus
    /// </summary>
    public class ServiceBusSource : GraphStage<SourceShape<BrokeredMessage>>
    {
        #region Logic

        private sealed class Logic : TimerGraphStageLogic
        {
            private const string TimerKey = "PollTimer";
            private readonly ServiceBusSource _source;
            private readonly Decider _decider;
            private Action<Task<IEnumerable<BrokeredMessage>>> _messagesReceived;

            public Logic(ServiceBusSource source, Attributes attributes) :  base(source.Shape)
            {
                _source = source;
                _decider = attributes.GetDeciderOrDefault();

                SetHandler(source.Out, PullQueue);
            }

            public override void PreStart()
                => _messagesReceived = GetAsyncCallback<Task<IEnumerable<BrokeredMessage>>>(OnMessagesReceived);
            
            private void PullQueue() =>
                _source._client.ReceiveBatchAsync(_source._maxMessageCount, _source._serverWaitTime)
                    .ContinueWith(_messagesReceived);

            private void OnMessagesReceived(Task<IEnumerable<BrokeredMessage>> task)
            {
                if (task.IsFaulted || task.IsCanceled)
                {
                    if (_decider(task.Exception) == Directive.Stop)
                        FailStage(task.Exception);
                    else
                        ScheduleOnce(TimerKey, _source._pollInterval);

                    return;
                }

                // Try again if the queue is empty
                if (task.Result == null || !task.Result.Any())
                    ScheduleOnce(TimerKey, _source._pollInterval);
                else
                    EmitMultiple(_source.Out, task.Result);
            }

            protected override void OnTimer(object timerKey) => PullQueue();
        }

        #endregion

        /// <summary>
        /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus  
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="maxMessageCount">The maximum number of messages to receive in a batch</param>
        /// <param name="serverWaitTime">The time span that the server will wait for the message batch to arrive before it times out. Default = 3 seconds</param>
        /// <param name="pollInterval">The intervall in witch the queue should be polled if it is empty. Default = 10 seconds</param>
        public static Source<BrokeredMessage, NotUsed> Create(QueueClient client, int maxMessageCount = 100, TimeSpan? serverWaitTime = null, TimeSpan? pollInterval = null)
        {
            return Source.FromGraph(new ServiceBusSource(client, maxMessageCount, serverWaitTime, pollInterval));
        }

        /// <summary>
        /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus  
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="maxMessageCount">The maximum number of messages to receive in a batch</param>
        /// <param name="serverWaitTime">The time span that the server will wait for the message batch to arrive before it times out. Default = 3 seconds</param>
        /// <param name="pollInterval">The intervall in witch the queue should be polled if it is empty. Default = 10 seconds</param>
        public static Source<BrokeredMessage, NotUsed> Create(SubscriptionClient client, int maxMessageCount = 100, TimeSpan? serverWaitTime = null, TimeSpan? pollInterval = null)
        {
            return Source.FromGraph(new ServiceBusSource(client, maxMessageCount, serverWaitTime, pollInterval));
        }

        private readonly IBusClient _client;
        private readonly int _maxMessageCount;
        private readonly TimeSpan _serverWaitTime;
        private readonly TimeSpan _pollInterval;

        private ServiceBusSource(IBusClient client, int maxMessageCount, TimeSpan? serverWaitTime, TimeSpan? pollInterval)
        {
            _client = client;
            _maxMessageCount = maxMessageCount;
            _serverWaitTime = serverWaitTime ?? TimeSpan.FromSeconds(3);
            _pollInterval = pollInterval ?? TimeSpan.FromSeconds(10);

            Shape = new SourceShape<BrokeredMessage>(Out);
        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSource"/> 
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="maxMessageCount">The maximum number of messages to receive in a batch</param>
        /// <param name="serverWaitTime">The time span that the server will wait for the message batch to arrive before it times out. Default = 3 seconds</param>
        /// <param name="pollInterval">The intervall in witch the queue should be polled if it is empty. Default = 10 seconds</param>
        public ServiceBusSource(QueueClient client, int maxMessageCount = 100, TimeSpan? serverWaitTime = null, TimeSpan? pollInterval = null)
            : this(new QueueClientWrapper(client), maxMessageCount, serverWaitTime, pollInterval)
        {

        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSource"/> 
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="maxMessageCount">The maximum number of messages to receive in a batch</param>
        /// <param name="serverWaitTime">The time span that the server will wait for the message batch to arrive before it times out. Default = 3 seconds</param>
        /// <param name="pollInterval">The intervall in witch the queue should be polled if it is empty. Default = 10 seconds</param>
        public ServiceBusSource(SubscriptionClient client, int maxMessageCount = 100, TimeSpan? serverWaitTime = null, TimeSpan? pollInterval = null)
            : this(new SubscriptionClientWrapper(client), maxMessageCount, serverWaitTime, pollInterval)
        {

        }

        public Outlet<BrokeredMessage> Out { get; } = new Outlet<BrokeredMessage>("ServiceBusSource.Out");

        public override SourceShape<BrokeredMessage> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
            => new Logic(this, inheritedAttributes);
    }
}
