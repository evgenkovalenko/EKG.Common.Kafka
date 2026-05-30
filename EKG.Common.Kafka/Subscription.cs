namespace EKG.Common.Kafka;

using Confluent.Kafka;

public class Subscription<TKey, TValue>
{
    public Subscription(Task subscriptionTask, IConsumer<TKey, TValue> consumer)
    {
        SubscriptionTask = subscriptionTask;
        Consumer = consumer;
    }

    public Task SubscriptionTask { get; }
    public IConsumer<TKey, TValue> Consumer { get; }
}
