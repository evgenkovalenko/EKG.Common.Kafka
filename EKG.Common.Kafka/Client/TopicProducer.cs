namespace EKG.Common.Kafka.Client;

using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

internal class TopicProducer<TKey, TValue> : ITopicProducer<TKey, TValue>
{
    public string Topic { get; }
    private readonly IProducer<TKey, TValue> _producer;

    public TopicProducer(string topic, IProducer<TKey, TValue> producer)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        _producer = producer;
    }

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(Message<TKey, TValue> message)
    {
        Validate(message.Value);
        return _producer.ProduceAsync(Topic, message);
    }

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topicName, Message<TKey, TValue> message)
    {
        Validate(message.Value);
        return _producer.ProduceAsync(topicName, message);
    }

    public void Produce(Message<TKey, TValue> message)
    {
        Validate(message.Value);
        _producer.Produce(Topic, message);
    }

    public void ProduceWithoutValidation(Message<TKey, TValue> message) => _producer.Produce(Topic, message);

    public void Produce(Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
    {
        Validate(message.Value);
        _producer.Produce(Topic, message, deliveryHandler);
    }

    public void ProduceWithoutValidation(Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
        => _producer.Produce(Topic, message, deliveryHandler);

    public void ProduceWithoutValidation(Message<TKey, TValue> message, TopicPartition topicPartition, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
        => _producer.Produce(topicPartition, message, deliveryHandler);

    public void Produce(int partitionNumber, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler)
    {
        Validate(message.Value);
        _producer.Produce(new TopicPartition(Topic, new Partition(partitionNumber)), message, deliveryHandler);
    }

    public void Produce(string topicName, Message<TKey, TValue> message)
    {
        Validate(message.Value);
        _producer.Produce(topicName, message);
    }

    public void InitTransactions(TimeSpan timeout) => _producer.InitTransactions(timeout);
    public void BeginTransaction() => _producer.BeginTransaction();
    public void CommitTransaction(TimeSpan timeout) => _producer.CommitTransaction(timeout);
    public void AbortTransaction(TimeSpan timeout) => _producer.AbortTransaction(timeout);
    public void Flush(CancellationToken cancellationToken = default) => _producer.Flush(cancellationToken);

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        => _producer.SendOffsetsToTransaction(offsets, groupMetadata, timeout);

    public void Dispose() => _producer?.Dispose();

    private static void Validate(TValue val) => Validator<TValue>.Validate(val);

    private static class Validator<T>
    {
        private static readonly bool IsValueType = typeof(T).IsValueType;
        private static readonly EqualityComparer<T> EqualityComparer = EqualityComparer<T>.Default;

        public static void Validate(T value)
        {
            if (IsValueType || EqualityComparer.Equals(value, default)) return;
            var validationContext = new ValidationContext(value, null, null);
            System.ComponentModel.DataAnnotations.Validator.ValidateObject(value, validationContext, true);
        }
    }
}
