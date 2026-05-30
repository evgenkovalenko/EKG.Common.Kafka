namespace EKG.Common.Kafka.Consumer;

using System.Collections.Concurrent;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using Serilog;

public abstract class AbstractLockingConsumerService<TKey, TValue, TDistinctKey> : EventConsumerService<TKey, TValue>
{
    protected readonly ILogger Logger;
    public event EventHandler<List<ConsumeResult<TKey, TValue>>> OnMessagesReceived;

    private readonly ManualResetEventSlim _resetHandle = new(false);
    private readonly ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>> _partitionedCache;

    protected AbstractLockingConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder) : base(builder)
    {
        Logger = builder.Logger.ForContext<ConsumerBuilder<TKey, TValue>>();
        _partitionedCache = new ConcurrentDictionary<Partition, List<ConsumeResult<TKey, TValue>>>();
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken);
        _resetHandle.Wait(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _resetHandle.Dispose();
    }

    protected abstract TDistinctKey GetDistinctKey(TValue item);

    private IEnumerable<ConsumeResult<TKey, TValue>> Distinct(List<ConsumeResult<TKey, TValue>> existingMessages)
    {
        var newMsgs = new Dictionary<TDistinctKey, ConsumeResult<TKey, TValue>>();
        foreach (var msg in existingMessages)
            newMsgs[GetDistinctKey(msg.Message.Value)] = msg;
        return newMsgs.Values;
    }

    protected void RaiseMessageReceivedEvent(object sender, List<ConsumeResult<TKey, TValue>> results)
        => OnMessagesReceived?.Invoke(this, Distinct(results).ToList());

    protected override void OnMessage(object sender, ConsumeResult<TKey, TValue> e)
    {
        try
        {
            if (e.IsPartitionEOF)
            {
                _partitionedCache.TryAdd(e.Partition, new List<ConsumeResult<TKey, TValue>>());
                if (Consumer.Assignment.Count == _partitionedCache.Count || _resetHandle.IsSet)
                {
                    foreach (var partition in _partitionedCache)
                        if (_partitionedCache.TryRemove(partition.Key, out var results))
                            RaiseMessageReceivedEvent(this, results);
                    _resetHandle.Set();
                }
            }
            else
            {
                base.OnMessage(sender, e);
                _partitionedCache.GetOrAdd(e.Partition, new List<ConsumeResult<TKey, TValue>>()).Add(e);
            }
        }
        catch (Exception exception) { Logger.Error(exception, "Error processing received result"); }
    }
}
