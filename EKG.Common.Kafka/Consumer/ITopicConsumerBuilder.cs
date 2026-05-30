namespace EKG.Common.Kafka.Consumer;

using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using Microsoft.Extensions.DependencyInjection;

public interface ITopicConsumerBuilder<TKey, TValue>
{
    IServiceCollection Services { get; }
    string ConfigurationSectionName { get; }
    Predicate<Headers> Filter { get; set; }
    bool Loading { get; set; }
    Action<ITopicConsumerBuilder<TKey, TValue>, IServiceProvider> PreConfigureDelegate { get; set; }
    Action<FilterableConsumerBuilderTopic<TKey, TValue>, IServiceProvider> ConfigureDelegate { get; set; }
    ITopicConsumerBuilder<TKey, TValue> Configure(Action<FilterableConsumerBuilderTopic<TKey, TValue>> configure);
    ITopicConsumerBuilder<TKey, TValue> Configure(Action<FilterableConsumerBuilderTopic<TKey, TValue>, IServiceProvider> configure);
    ITopicConsumerBuilder<TKey, TValue> PreConfigure(Action<ITopicConsumerBuilder<TKey, TValue>, IServiceProvider> configure);
    bool IsAsync { get; set; }
}

public enum LoadingMode { None, Normal, Blocking }
