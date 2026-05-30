namespace EKG.Common.Kafka.Consumer;

using Confluent.Kafka;
using EKG.Common.Kafka.Builder;

public class CommitOnDoneConsumerService<TKey, TValue> : EventConsumerService<TKey, TValue>
{
    public CommitOnDoneConsumerService(FilterableConsumerBuilderTopic<TKey, TValue> builder) : base(builder)
    {
        var autoCommit = builder.Configuration.SingleOrDefault(x => x.Key == "enable.auto.commit");
        if (autoCommit.Value == null || autoCommit.Value.ToLowerInvariant() != "false")
            throw new Exception($"{nameof(CommitOnDoneConsumerService<TKey, TValue>)} requires enable.auto.commit to be set to false.");
    }

    protected override void OnMessage(object sender, ConsumeResult<TKey, TValue> e)
    {
        if (!HasSubscribers)
            throw new Exception($"Proper usage of the service requires that the event {nameof(OnMessageHandler)} has subscriber. Will not commit to the consumer");
        base.OnMessage(this, e);
        Consumer.Commit(new List<TopicPartitionOffset> { e.TopicPartitionOffset });
    }
}
