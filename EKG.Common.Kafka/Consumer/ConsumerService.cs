namespace EKG.Common.Kafka.Consumer;

using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Serialization;
using Microsoft.Extensions.Hosting;
using Serilog;

public abstract class ConsumerService<TKey, TValue> : IHostedService, IAsyncDisposable
{
    public int PartitionAssignedCount => Consumer?.Assignment.Count ?? 0;

    private readonly CancellationTokenSource _cts = new();
    protected readonly FilterableConsumerBuilderTopic<TKey, TValue> Builder;
    private readonly ILogger _logger;
    private readonly string _serviceType;

    protected CancellationToken StoppingToken => _cts.Token;

    protected ConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder)
    {
        _serviceType = GetType().Name;
        _logger = builder.Logger.ForContext<EventConsumerService<TKey, TValue>>();
        Builder = builder;
        Builder.SetPartitionsAssignedHandler(PartitionsAssignedHandler);
        Builder.SetPartitionsRevokedHandler(PartitionsRevokedHandler);
        Builder.SetPartitionsLostHandler(PartitionsLostHandler);
    }

    protected IConsumer<TKey, RentedBytes> Consumer { get; set; }
    protected Task ConsumptionTask { get; set; }

    public abstract Task StartAsync(CancellationToken cancellationToken);

    public virtual async Task StopAsync(CancellationToken cancellationToken)
    {
        await DisposeAsync();
        _logger.Information("{KafkaConsumerServiceType} disposed.", _serviceType);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(10));
        await Task.WhenAny(ConsumptionTask, timeoutTask);
        if (timeoutTask.IsCompleted && !ConsumptionTask.IsCompleted)
            _logger.Error("{KafkaConsumerServiceType} did not stop in time.", _serviceType);
    }

    protected virtual void PartitionsAssignedHandler(IConsumer<TKey, RentedBytes> consumer, List<TopicPartition> topicsPartitions)
    {
        foreach (var topicPartitions in topicsPartitions.GroupBy(x => x.Topic))
            _logger.Information("{KafkaConsumerServiceType} with topic {TopicNameKey} has been assigned the following partitions: {AssignedPartitions}",
                _serviceType, topicPartitions.Key, string.Join(",", topicPartitions.Select(tp => tp.Partition.Value)));
    }

    protected virtual void PartitionsRevokedHandler(IConsumer<TKey, RentedBytes> consumer, List<TopicPartitionOffset> topicPartitionOffsets)
    {
        foreach (var topicPartitions in topicPartitionOffsets.GroupBy(x => x.Topic))
            _logger.Information("{KafkaConsumerServiceType} with topic {TopicNameKey} has had the following partitions revoked: {RevokedPartitions}",
                _serviceType, topicPartitions.Key, string.Join(",", topicPartitions.Select(tp => $"Partition [{tp.Partition.Value}] at offset ({tp.Offset.Value})")));
    }

    protected virtual void PartitionsLostHandler(IConsumer<TKey, RentedBytes> consumer, List<TopicPartitionOffset> topicPartitionOffsets)
    {
        foreach (var topicPartitions in topicPartitionOffsets.GroupBy(x => x.Topic))
            _logger.Information("{KafkaConsumerServiceType} with topic {TopicNameKey} has had the following partitions lost: {LostPartitions}",
                _serviceType, topicPartitions.Key, string.Join(",", topicPartitions.Select(tp => $"Partition [{tp.Partition.Value}] at offset ({tp.Offset.Value})")));
    }
}
