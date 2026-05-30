namespace EKG.Common.Kafka.Consumer;

using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using Nito.AsyncEx;
using Serilog;

public class EventConsumerService<TKey, TValue> : ConsumerService<TKey, TValue>, ITopicConsumer<TKey, TValue>
{
    private readonly ILogger _logger;
    public event EventHandler<ConsumeResult<TKey, TValue>> OnMessageHandler;
    protected bool HasSubscribers => OnMessageHandler != null;

    public EventConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder) : base(builder)
        => _logger = builder.Logger.ForContext<ConsumerServicePartitionEOF<TKey, TValue>>();

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.Information("Subscribing {type} to topic {topic}", GetType().Name, Builder.TopicName);
        foreach (var subscription in Builder?.Subscribtions ?? [])
            Subscribe(subscription);
        var consumerSubscription = Builder.BuildWithSubscription(StoppingToken, OnMessage, OnHeartbeat, OnSkippedMessage);
        Consumer = consumerSubscription.Consumer;
        ConsumptionTask = consumerSubscription.SubscriptionTask;
        return Task.CompletedTask;
    }

    [Obsolete("Reactive subscription is deprecated; use Subscribe method.")]
    public virtual IObservable<EventPattern<ConsumeResult<TKey, TValue>>> TopicObservableMessage()
        => Observable.FromEventPattern<ConsumeResult<TKey, TValue>>(
            h => OnMessageHandler += h, h => OnMessageHandler -= h, ImmediateScheduler.Instance);

    public IDisposable Subscribe(Action<ConsumeResult<TKey, TValue>> messageHandler)
    {
        void SyncHandler(object _, ConsumeResult<TKey, TValue> m) => messageHandler(m);
        OnMessageHandler += SyncHandler;
        return Disposable.Create(() => OnMessageHandler -= SyncHandler);
    }

    public IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, ValueTask> messageHandler)
        => Subscribe((m, _) => messageHandler(m));

    public IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> messageHandler)
    {
        void SyncHandler(object _, ConsumeResult<TKey, TValue> m)
            => AsyncContext.Run(() => messageHandler(m, CancellationToken.None).AsTask());
        OnMessageHandler += SyncHandler;
        return Disposable.Create(() => OnMessageHandler -= SyncHandler);
    }

    protected virtual void OnMessage(object sender, ConsumeResult<TKey, TValue> e)
    {
        try { OnMessageHandler?.Invoke(sender, e); }
        catch (Exception exception) { _logger.Error(exception, "OnMessage"); }
    }

    protected virtual void OnHeartbeat(object sender, TKey heartbeatKey, long heartbeatNumber) { }
    protected virtual void OnSkippedMessage(object sender, TopicPartitionOffset partitionOffset) { }
}
