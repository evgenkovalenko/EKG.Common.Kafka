namespace EKG.Common.Kafka.Builder;

using Confluent.Kafka;
using EKG.Common.Kafka.Client;
using Microsoft.Extensions.DependencyInjection;

public static class ProducerBuilderExtensions
{
    public static ProducerBuilder<TKey, TValue> SetDefaultErrorHandlers<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
    {
        if (builder is not ProducerBuilderTopic<TKey, TValue> topicBuilder)
            throw new InvalidOperationException("Use KafkaClientBuilder CreateProducer method to create producer.");

        var logger = topicBuilder.Logger.ForContext<ProducerBuilder<TKey, TValue>>();
        return builder.SetErrorHandler((sender, error) =>
        {
            if (error.IsFatal)
                logger.Error("Producer '{Name}' fatal error. Error: {@Error} ", sender.GetType().Name, error);
            else
                logger.Warning("Producer '{Name}' transient error. Error: {@Error} ", sender.GetType().Name, error);
        });
    }

    public static ITopicProducer<TKey, TValue> BuildTopicProducer<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
    {
        if (builder is not ProducerBuilderTopic<TKey, TValue> topicBuilder)
            throw new InvalidOperationException("Use KafkaClientBuilder CreateProducer method to create producer.");

        var producer = builder.Build();
        return new TopicProducer<TKey, TValue>(topicBuilder.TopicName, producer);
    }

    public static ITopicProducerBuilder<TKey, TValue> AddTopicProducer<TKey, TValue>(
        this IServiceCollection services, string configurationSectionName)
        => new TopicProducerBuilder<TKey, TValue> { Services = services, ConfigurationSectionName = configurationSectionName };

    public static void Build<TKey, TValue>(this ITopicProducerBuilder<TKey, TValue> builder)
    {
        builder.Services.AddSingleton(s =>
        {
            var builderTopic = s.GetService<KafkaClientBuilder>()
                .CreateProducer<TKey, TValue>(builder.ConfigurationSectionName)
                .SetDefaultErrorHandlers();
            builder.ConfigureDelegate?.Invoke(builderTopic, s);
            return builderTopic.BuildTopicProducer();
        });
    }

    private class TopicProducerBuilder<TKey, TValue> : ITopicProducerBuilder<TKey, TValue>
    {
        public IServiceCollection Services { get; set; }
        public string ConfigurationSectionName { get; set; }
        public Action<ProducerBuilder<TKey, TValue>, IServiceProvider> ConfigureDelegate { get; set; }

        public ITopicProducerBuilder<TKey, TValue> Configure(Action<ProducerBuilder<TKey, TValue>> configure)
            => Configure((b, _) => configure(b));

        public ITopicProducerBuilder<TKey, TValue> Configure(Action<ProducerBuilder<TKey, TValue>, IServiceProvider> configure)
        {
            var current = ConfigureDelegate;
            ConfigureDelegate = current == null ? configure : (f, sp) => { current(f, sp); configure(f, sp); };
            return this;
        }
    }
}
