namespace EKG.Common.Kafka.Consumer;

using System.Reactive.Disposables;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Internal;
using Serilog;

public class AsyncConsumerService<TKey, TValue> : ConsumerService<TKey, TValue>, IAsyncTopicConsumer<TKey, TValue>
{
    private readonly ILogger _logger;
    private readonly List<Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask>> _messageHandlers = new();

    public AsyncConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder) : base(builder)
        => _logger = builder.Logger.ForContext<AsyncConsumerService<TKey, TValue>>();

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.Information("Subscribing {type} to topic {topic}", GetType().Name, Builder.TopicName);
        foreach (var subscription in Builder?.Subscribtions ?? [])
            Subscribe(subscription);
        var consumerSubscription = Builder.BuildWithAsyncSubscription(StoppingToken, OnMessageAsync, OnHeartbeatAsync, OnSkippedMessageAsync);
        Consumer = consumerSubscription.Consumer;
        ConsumptionTask = consumerSubscription.SubscriptionTask;
        return Task.CompletedTask;
    }

    public IDisposable Subscribe(Action<ConsumeResult<TKey, TValue>> messageHandler)
        => Subscribe((m, _) => { messageHandler(m); return ValueTask.CompletedTask; });

    public IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, ValueTask> messageHandler)
        => Subscribe((m, _) => messageHandler(m));

    public IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> messageHandler)
    {
        if (messageHandler == null) throw new ArgumentException("Invalid message handler");
        _messageHandlers.Add(messageHandler);
        return Disposable.Create(() => _messageHandlers.Remove(messageHandler));
    }

    protected virtual ValueTask OnMessageAsync(object sender, ConsumeResult<TKey, TValue> e, CancellationToken cancellationToken)
    {
        try { return ValueTaskTools.WhenAll(_messageHandlers.Select(h => h(e, cancellationToken)).ToArray()); }
        catch (Exception exception) { _logger.Error(exception, "OnMessage"); return ValueTask.CompletedTask; }
    }

    protected virtual ValueTask OnHeartbeatAsync(object sender, TKey heartbeatKey, long heartbeatNumber, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    protected virtual ValueTask OnSkippedMessageAsync(object sender, TopicPartitionOffset partitionOffset, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;
}
