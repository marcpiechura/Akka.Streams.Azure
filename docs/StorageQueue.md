# Azure Storage Queue adapter


#### QueueSource

You can create a `Source` for the Storage Queue either via `Source.FromGraph(new QueueSource)` or by calling the `QueueSource.Create` method. 

By default the `Source` will fail the stream if a call to the queue for new messages failed, you can change that behavior by using `Restart` or `Resume` `SupervisionStrategy`.

```csharp
QueueSource.Create(Queue, pollInterval: TimeSpan.FromSeconds(1))
    .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.ResumingDecider))
```

The `Source` reads messages in batches from the queue and then emits the single messages into the stream, once all messages are emitted another request is send to the queue. The number of messages that are requested per batch can be configured via the `prefetchCount` parameter, by default 10 messages. If you want a behavior were the source is making the request for new messages while the messages from the previous request are still not completely processed, you can easily do that by adding a `Buffer` directly after the `Source` like this: 

```csharp
QueueSource.Create(Queue, pollInterval: TimeSpan.FromSeconds(1))
    .Buffer(5, OverflowStrategy.Backpressure)
```

This will send the next request to the queue once the first five messages have been processed with five messages left in the `Buffer`.

If the queue is empty the source will periodically poll for new messages, this interval can be configured via the `pollInterval` parameter, by default 10 seconds.

#### QueueSink

TODO

#### Examples

You can find some examples in the [test project](https://github.com/Silv3rcircl3/Akka.Streams.Azure/tree/master/src/Akka.Streams.Azure.StorageQueue.Tests).