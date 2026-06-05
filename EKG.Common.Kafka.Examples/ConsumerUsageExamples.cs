namespace EKG.Common.Kafka.Examples;

using System.Reactive.Linq;
using Confluent.Kafka;
using EKG.Common.Kafka.Consumer;
using Microsoft.Extensions.Hosting;
using Serilog;

public class ConsumerUsageExamples : IHostedService
{
    private readonly ITopicConsumer<int, MyDtoFiltered> _filteredConsumer;
    private readonly ITopicConsumer<int, MyDto> _consumer;
    private readonly ITopicConsumerEOF<int, MyDtoProcessor> _partitionedConsumer;
    private readonly ITopicConsumerEOF<int, MyDtoProcessorBatching> _partitionedBatchingConsumer;
    private readonly ILogger _logger;

    public ConsumerUsageExamples(
        ITopicConsumer<int, MyDtoFiltered> filteredConsumer,
        ITopicConsumer<int, MyDto> consumer,
        ITopicConsumerEOF<int, MyDtoProcessor> partitionedConsumer,
        ITopicConsumerEOF<int, MyDtoProcessorBatching> partitionedBatchingConsumer,
        ILogger logger)
    {
        _filteredConsumer = filteredConsumer;
        _consumer = consumer;
        _partitionedConsumer = partitionedConsumer;
        _partitionedBatchingConsumer = partitionedBatchingConsumer;
        _logger = logger.ForContext<ConsumerUsageExamples>();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
#pragma warning disable CS0618
        //_filteredConsumer.TopicObservableMessage().Select(c => c.EventArgs).Subscribe(OnNextFiltered);
        _consumer.TopicObservableMessage().Select(c => c.EventArgs).Subscribe(OnNext);
        //_partitionedConsumer.TopicObservablePartitionEOF().Select(c => c.EventArgs).Subscribe(OnNextPartitionFinished);
        //_partitionedBatchingConsumer.TopicObservablePartitionEOF().Select(c => c.EventArgs).Subscribe(OnNextPartitionBatching);
#pragma warning restore CS0618
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private void OnNextFiltered(ConsumeResult<int, MyDtoFiltered> e)
        => _logger.Information("MyDtoFiltered: {Partition}-{Offset} Key: {Key}", e.Partition, e.Offset, e.Message.Key);

    private void OnNext(ConsumeResult<int, MyDto> e)
        => _logger.Information("MyDto: {Partition}-{Offset} Key: {Key}", e.Partition, e.Offset, e.Message.Key);

    private void OnNextPartitionFinished(List<ConsumeResult<int, MyDtoProcessor>> e)
        => _logger.Information("MyDtoProcessor: {Partition} Processed: {Count}", e.First().Partition, e.Count);

    private void OnNextPartitionBatching(List<ConsumeResult<int, MyDtoProcessorBatching>> e)
        => _logger.Information("MyDtoProcessorBatching: {Partition} Processed: {Count}", e.First().Partition, e.Count);
}
