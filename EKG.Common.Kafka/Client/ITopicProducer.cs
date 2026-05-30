namespace EKG.Common.Kafka.Client;

using Confluent.Kafka;

public interface ITopicProducer<TKey, TValue> : IDisposable
{
    string Topic { get; }
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(Message<TKey, TValue> message);
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topicName, Message<TKey, TValue> message);
    void Produce(Message<TKey, TValue> message);
    void Produce(Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler);
    void Produce(int partitionNumber, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler);
    void Produce(string topicName, Message<TKey, TValue> message);
    void ProduceWithoutValidation(Message<TKey, TValue> message);
    void ProduceWithoutValidation(Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler);
    void ProduceWithoutValidation(Message<TKey, TValue> message, TopicPartition topicPartition, Action<DeliveryReport<TKey, TValue>> deliveryHandler);
    void InitTransactions(TimeSpan timeout);
    void BeginTransaction();
    void CommitTransaction(TimeSpan timeout);
    void AbortTransaction(TimeSpan timeout);
    void Flush(CancellationToken cancellationToken = default);
    void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout);
}
