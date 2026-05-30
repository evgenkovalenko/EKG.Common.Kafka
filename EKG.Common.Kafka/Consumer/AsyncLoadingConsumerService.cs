namespace EKG.Common.Kafka.Consumer;

using System.Diagnostics;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Serialization;
using Microsoft.Extensions.Options;
using Serilog;

public class AsyncLoadingConsumerService<TKey, TValue> : AsyncConsumerService<TKey, TValue>, IAsyncLoadingConsumer<TKey, TValue>
{
    private readonly IOptionsMonitor<LoadingConsumerConfiguration> _options;
    private readonly ILogger _logger;
    private readonly HashSet<TopicPartition> _assignedPartitions = new();
    private readonly ChannelReader<ConsumeResult<TKey, RentedBytes>> _channelReader;
    private readonly ChannelWriter<ConsumeResult<TKey, RentedBytes>> _channelWriter;
    private readonly TaskCompletionSource<bool> _loadCompletionSource = new();
    private long _itemsProcessed;
    private Task _messageConsumerTask;
    private Task _watchDogTask;

    public Task LoadingCompilation => _loadCompletionSource?.Task;

    public AsyncLoadingConsumerService(
        FilterableConsumerBuilderTopic<TKey, TValue> builder,
        IOptionsMonitor<LoadingConsumerConfiguration> options) : base(builder)
    {
        _options = options;
        _logger = builder.Logger.ForContext<EventConsumerService<TKey, TValue>>();
        var channel = Channel.CreateBounded<ConsumeResult<TKey, RentedBytes>>(new BoundedChannelOptions(_options.CurrentValue.MaxUnprocessedMessages)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });
        _channelReader = channel.Reader;
        _channelWriter = channel.Writer;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.Information("Subscribing {type} to topic {topic}", GetType().Name, Builder.TopicName);
        var subscription = Builder.BuildWithAsyncRawSubscription(StoppingToken, OnRawMessageAsync);
        Consumer = subscription.Consumer;
        ConsumptionTask = subscription.SubscriptionTask;
        _messageConsumerTask = StartMessageConsumer(StoppingToken);
        _watchDogTask = StartWatchDog(StoppingToken);
        return !_options.CurrentValue.Blocking ? Task.CompletedTask : LoadingCompilation;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await _messageConsumerTask;
        await _watchDogTask;
    }

    private ValueTask OnRawMessageAsync(object consumer, ConsumeResult<TKey, RentedBytes> message, CancellationToken cancellationToken)
    {
        if (message == null) return new ValueTask();
        if (message.IsPartitionEOF)
        {
            _assignedPartitions.Add(message.TopicPartition);
            if (_loadCompletionSource.Task.IsCompleted || _assignedPartitions.Count < Consumer.Assignment.Count)
                return new ValueTask();
        }
        return _channelWriter.WriteAsync(message, cancellationToken);
    }

    private Task StartMessageConsumer(CancellationToken cancellationToken) =>
        Task.Factory.StartNew(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var rawConsumeResult = await _channelReader.ReadAsync(cancellationToken);
                    var message = rawConsumeResult?.Message;
                    if (message == null)
                    {
                        _loadCompletionSource.TrySetResult(true);
                        continue;
                    }
                    if (!_loadCompletionSource.Task.IsCompleted) _itemsProcessed += 1;
                    if (!ExecuteFilterPredicate(message.Headers)) continue;
                    var parsedMessage = ParseRawMessage(rawConsumeResult);
                    var onMessageValueTask = OnMessageAsync(Consumer, parsedMessage, cancellationToken);
                    if (!onMessageValueTask.IsCompletedSuccessfully) await onMessageValueTask;
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cancellationToken) { }
                catch (Exception ex) { _logger.Error(ex, "Message Parser error."); }
            }
        }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private static readonly Headers EmptyHeaders = new();

    private bool ExecuteFilterPredicate(Headers headers)
    {
        try { return Builder.FilterPredicate(headers ?? EmptyHeaders); }
        catch (Exception ex) { _logger.Warning(ex, "Filter predicate invocation error"); return false; }
    }

    private Task StartWatchDog(CancellationToken cancellationToken) => Task.Run(async () =>
    {
        var duration = Stopwatch.StartNew();
        while (!cancellationToken.IsCancellationRequested && !_loadCompletionSource.Task.IsCompleted)
        {
            _logger.Debug("Waiting for {topic} / {items} processed...", Builder.TopicName, _itemsProcessed);
            await Task.Delay(1000, cancellationToken);
        }
        duration.Stop();
        _logger.Information("Initially loaded {items} in {ms}ms from {topic}.", _itemsProcessed, duration.ElapsedMilliseconds, Builder.TopicName);
    }, cancellationToken);

    private ConsumeResult<TKey, TValue> ParseRawMessage(ConsumeResult<TKey, RentedBytes> rawConsumeResult)
        => EKG.Common.Kafka.Builder.ConsumerBuilderExtensions.CreateResult(
            rawConsumeResult,
            Builder.InternalValueDeserializer.Deserialize(rawConsumeResult.Message.Value,
                new SerializationContext(MessageComponentType.Value, rawConsumeResult.Topic, rawConsumeResult.Message?.Headers)));

    public class LoadingConsumerConfiguration
    {
        public int MaxUnprocessedMessages { get; set; } = 100000;
        public bool Blocking { get; set; }
    }
}
