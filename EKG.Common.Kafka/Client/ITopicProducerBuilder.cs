namespace EKG.Common.Kafka.Client;

using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

public interface ITopicProducerBuilder<TKey, TValue>
{
    IServiceCollection Services { get; }
    string ConfigurationSectionName { get; }
    ITopicProducerBuilder<TKey, TValue> Configure(Action<ProducerBuilder<TKey, TValue>> configure);
    ITopicProducerBuilder<TKey, TValue> Configure(Action<ProducerBuilder<TKey, TValue>, IServiceProvider> configure);
    Action<ProducerBuilder<TKey, TValue>, IServiceProvider> ConfigureDelegate { get; set; }
}
