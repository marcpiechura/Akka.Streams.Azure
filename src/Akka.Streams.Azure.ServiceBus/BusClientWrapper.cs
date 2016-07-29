using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace Akka.Streams.Azure.ServiceBus
{
    internal interface IBusClient
    {
        void OnMessageAsync(Func<BrokeredMessage, Task> callback, OnMessageOptions options);

        Task SendBatchAsync(IEnumerable<BrokeredMessage> messages);
    }
    
    internal sealed class QueueClientWrapper : IBusClient
    {
        private readonly QueueClient _client;

        public QueueClientWrapper(QueueClient client)
        {
            _client = client;
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> callback, OnMessageOptions options)
            => _client.OnMessageAsync(callback, options);

        public Task SendBatchAsync(IEnumerable<BrokeredMessage> messages) => _client.SendBatchAsync(messages);
    }

    internal sealed class SubscriptionClientWrapper : IBusClient
    {
        private readonly SubscriptionClient _client;

        public SubscriptionClientWrapper(SubscriptionClient client)
        {
            _client = client;
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> callback, OnMessageOptions options)
            => _client.OnMessageAsync(callback, options);

        public Task SendBatchAsync(IEnumerable<BrokeredMessage> messages)
        {
            throw new NotImplementedException();
        }
    }

    internal sealed class TopicClientWrapper : IBusClient
    {
        private readonly TopicClient _client;

        public TopicClientWrapper(TopicClient client)
        {
            _client = client;
        }

        public void OnMessageAsync(Func<BrokeredMessage, Task> callback, OnMessageOptions options)
        {
            throw new NotImplementedException();
        }

        public Task SendBatchAsync(IEnumerable<BrokeredMessage> messages) => _client.SendBatchAsync(messages);
    }
}