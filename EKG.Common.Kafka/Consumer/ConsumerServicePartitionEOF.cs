namespace EKG.Common.Kafka.Consumer;

using System.Collections.Concurrent;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using Microsoft.Extensions.Options;
using Serilog;

public class ConsumerServicePartitionEOF<TKey, TValue> : EventConsumerService<TKey, TValue>, ITopicConsumerEOF<TKey, TValue>
{
    private readonly ILogger _logger;
    private readonly IOptionsMonitor<EOFConsumerConfiguration> _configuration;
    private readonly ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>> _partitionedCache;

    public event EventHandler<List<ConsumeResult<TKey, TValue>>> OnPartitionEOFHandler;

    public ConsumerServicePartitionEOF(
        FilterableConsumerBuilderTopic<TKey, TValue> builder,
        IOptionsMonitor<EOFConsumerConfiguration> configuration) : base(builder)
    {
        _logger = builder.Logger.ForContext<ConsumerServicePartitionEOF<TKey, TValue>>();
        _configuration = configuration;
        _partitionedCache = new ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>>();
    }

    protected override void OnMessage(object sender, ConsumeResult<TKey, TValue> e)
    {
        try
        {
            if (e.IsPartitionEOF)
            {
                if (_partitionedCache.TryRemove(e.Partition, out var results))
                {
                    OnPartitionEOFHandler?.Invoke(this, results);
                    Consumer.Commit(new List<TopicPartitionOffset> { e.TopicPartitionOffset });
                    if (_configuration.CurrentValue.PartitionEOFPauseTime != TimeSpan.Zero)
                    {
                        Consumer.Pause(new List<TopicPartition> { e.TopicPartition });
                        Task.Factory.StartNew(async () =>
                        {
                            await Task.Delay(_configuration.CurrentValue.PartitionEOFPauseTime);
                            Consumer.Resume(new List<TopicPartition> { e.TopicPartition });
                        });
                    }
                }
            }
            else
            {
                var messages = _partitionedCache.GetOrAdd(e.Partition, new List<ConsumeResult<TKey, TValue>>());
                messages.Add(e);
                if (messages.Count == _configuration.CurrentValue.PartitionBatchSize)
                {
                    OnPartitionEOFHandler?.Invoke(this, messages);
                    Consumer.Commit(new List<TopicPartitionOffset> { e.TopicPartitionOffset });
                    _partitionedCache.TryRemove(e.Partition, out _);
                }
                else
                    base.OnMessage(sender, e);
            }
        }
        catch (Exception exception) { _logger.Error(exception, "OnMessage"); }
    }

    public virtual IObservable<EventPattern<List<ConsumeResult<TKey, TValue>>>> TopicObservablePartitionEOF()
        => Observable.FromEventPattern<List<ConsumeResult<TKey, TValue>>>(
            h => OnPartitionEOFHandler += h, h => OnPartitionEOFHandler -= h, ImmediateScheduler.Instance);

    public class EOFConsumerConfiguration
    {
        public TimeSpan PartitionEOFPauseTime { get; set; }
        public int PartitionBatchSize { get; set; } = int.MaxValue;
    }
}
