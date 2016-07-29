using System;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Microsoft.ServiceBus.Messaging;

namespace Akka.Streams.Azure.ServiceBus
{
    /// <summary>
    /// a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus
    /// </summary>
    public class ServiceBusSource : GraphStage<SourceShape<BrokeredMessage>>
    {
        #region Logic

        private sealed class Logic : GraphStageLogic
        {
            private readonly ServiceBusSource _source;
            private Action<Tuple<BrokeredMessage, TaskCompletionSource<NotUsed>>> _onMessageCallback;
            private Tuple<BrokeredMessage, TaskCompletionSource<NotUsed>> _pendingMessage;

            public Logic(ServiceBusSource source) :  base(source.Shape)
            {
                _source = source;
                SetHandler(source.Out, TryPush);
            }

            public override void PreStart()
            {
                _onMessageCallback = GetAsyncCallback<Tuple<BrokeredMessage, TaskCompletionSource<NotUsed>>>(OnMessage);
                _source._client.OnMessageAsync(message =>
                {
                    var completion = new TaskCompletionSource<NotUsed>();
                    _onMessageCallback(Tuple.Create(message, completion));
                    return completion.Task;
                }, _source._options);
            }

            private void OnMessage(Tuple<BrokeredMessage, TaskCompletionSource<NotUsed>> t)
            {
                _pendingMessage = t;
                TryPush();
            }

            private void TryPush()
            {
                if (_pendingMessage != null && IsAvailable(_source.Out))
                {
                    Push(_source.Out, _pendingMessage.Item1);
                    _pendingMessage.Item2.TrySetResult(NotUsed.Instance);
                    _pendingMessage = null;
                }
            }
        }

        #endregion
        
        /// <summary>
        /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus  
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="options">The options that are used for the <see cref="QueueClient.OnMessageAsync(Func{BrokeredMessage, Task}, OnMessageOptions)"/> call</param>
        public static Source<BrokeredMessage, NotUsed> Create(QueueClient client, OnMessageOptions options = null)
        {
            return Source.FromGraph(new ServiceBusSource(client, options));
        }

        /// <summary>
        /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure ServiceBus  
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="options">The options that are used for the <see cref="SubscriptionClient.OnMessageAsync(Func{BrokeredMessage, Task}, OnMessageOptions)"/> call</param>
        public static Source<BrokeredMessage, NotUsed> Create(SubscriptionClient client, OnMessageOptions options = null)
        {
            return Source.FromGraph(new ServiceBusSource(client, options));
        }

        private readonly IBusClient _client;
        private readonly OnMessageOptions _options;

        private ServiceBusSource(IBusClient client, OnMessageOptions options)
        {
            _client = client;
            _options = options ?? new OnMessageOptions();

            Shape = new SourceShape<BrokeredMessage>(Out);
        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSource"/> 
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="options">The options that are used for the <see cref="QueueClient.OnMessageAsync(Func{BrokeredMessage, Task}, OnMessageOptions)"/> call</param>
        public ServiceBusSource(QueueClient client, OnMessageOptions options = null)
            : this(new QueueClientWrapper(client), options)
        {

        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSource"/> 
        /// </summary>
        /// <param name="client">The client</param>
        /// <param name="options">The options that are used for the <see cref="SubscriptionClient.OnMessageAsync(Func{BrokeredMessage, Task}, OnMessageOptions)"/> call</param>
        public ServiceBusSource(SubscriptionClient client, OnMessageOptions options = null)
            : this(new SubscriptionClientWrapper(client), options)
        {

        }

        public Outlet<BrokeredMessage> Out { get; } = new Outlet<BrokeredMessage>("ServiceBusSource.Out");

        public override SourceShape<BrokeredMessage> Shape { get; }
        
        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }
}
