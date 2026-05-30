namespace EKG.Common.Kafka.Consumer;

using System.Reactive;
using Confluent.Kafka;

public interface IPartitionConsumer<TKey, TValue>
{
    IDisposable Subscribe(Action<ConsumeResult<TKey, TValue>> messageHandler);
    IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, ValueTask> messageHandler);
    IDisposable Subscribe(Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> messageHandler);
}

public interface IAsyncLoadingConsumer<TKey, TValue> : IAsyncTopicConsumer<TKey, TValue>
{
    Task LoadingCompilation { get; }
}

public interface ITopicConsumer<TKey, TValue> : IPartitionConsumer<TKey, TValue>
{
    IObservable<EventPattern<ConsumeResult<TKey, TValue>>> TopicObservableMessage();
    event EventHandler<ConsumeResult<TKey, TValue>> OnMessageHandler;
}

public interface IAsyncTopicConsumer<TKey, TValue> : IPartitionConsumer<TKey, TValue> { }

public interface ITopicConsumerEOF<TKey, TValue> : ITopicConsumer<TKey, TValue>
{
    IObservable<EventPattern<List<ConsumeResult<TKey, TValue>>>> TopicObservablePartitionEOF();
    event EventHandler<List<ConsumeResult<TKey, TValue>>> OnPartitionEOFHandler;
}
