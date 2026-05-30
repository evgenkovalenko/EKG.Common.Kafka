namespace EKG.Common.Kafka.Consumer;

using System.Collections.Concurrent;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Serialization;

public abstract class AbstractRevokingConsumerService<TKey, TValue, TDistinctKey>
    : AbstractLockingConsumerService<TKey, TValue, TDistinctKey>
{
    private readonly ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>> _partitionedCacheForRevoke;

    protected AbstractRevokingConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder) : base(builder)
        => _partitionedCacheForRevoke = new ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>>();

    protected override void OnMessage(object sender, ConsumeResult<TKey, TValue> e)
    {
        if (!e.IsPartitionEOF)
            _partitionedCacheForRevoke.AddOrUpdate(e.Partition,
                _ => new List<ConsumeResult<TKey, TValue>> { e },
                (_, list) => { list.Add(e); return list; });
        base.OnMessage(sender, e);
    }

    protected override void PartitionsRevokedHandler(IConsumer<TKey, RentedBytes> arg1, List<TopicPartitionOffset> arg2)
    {
        foreach (var revokedPartition in arg2)
        {
            if (_partitionedCacheForRevoke.TryRemove(revokedPartition.Partition, out var partitionMessages))
                RaiseMessageReceivedEvent(this, GenerateRevokeMessages(partitionMessages).ToList());
            else
                throw new ArgumentException($"Should've had partition: {revokedPartition.Partition.Value} inside the _partitionedCacheForRevoke");
        }
    }

    protected abstract IEnumerable<ConsumeResult<TKey, TValue>> GenerateRevokeMessages(
        List<ConsumeResult<TKey, TValue>> existingMessages);
}
