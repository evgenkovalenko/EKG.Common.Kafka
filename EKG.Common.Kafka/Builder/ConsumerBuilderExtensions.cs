namespace EKG.Common.Kafka.Builder;

using Confluent.Kafka;
using EKG.Common.Kafka.Consumer;
using EKG.Common.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public static class ConsumerBuilderExtensions
{
    public static FilterableConsumerBuilderTopic<TKey, TValue> SetDefaultErrorHandlers<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder)
    {
        var logger = builder.Logger.ForContext<ConsumerBuilder<TKey, TValue>>();
        builder.SetErrorHandler((sender, error) =>
        {
            if (error.IsFatal)
                logger.Error("Consumer '{Name}' fatal error. Error: {@Error} ", sender.GetType().Name, error);
            else
                logger.Warning("Consumer '{Name}' transient error. Error: {@Error} ", sender.GetType().Name, error);
        });
        return builder;
    }

    public static ITopicConsumerBuilder<TKey, TValue> AddTopicConsumer<TKey, TValue>(
        this IServiceCollection services, string configurationSectionName)
        => new TopicConsumerBuilder<TKey, TValue> { Services = services, ConfigurationSectionName = configurationSectionName };

    public static ITopicConsumerBuilder<TKey, TValue> AddFilter<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, Predicate<Headers> filter)
    {
        builder.Filter = filter;
        return builder;
    }

    public static ITopicConsumerBuilder<TKey, TValue> AddFilter<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, Func<IServiceProvider, Predicate<Headers>> filterFactory)
    {
        builder.PreConfigure((b, sp) => { b.Filter = filterFactory(sp); });
        return builder;
    }

    public static IServiceCollection Build<TConsumerService, TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, bool asHostedService = true)
        where TConsumerService : EventConsumerService<TKey, TValue>
    {
        var services = builder.Services;
        services.AddSingleton(s =>
        {
            builder.PreConfigureDelegate?.Invoke(builder, s);
            var builderTopic = s.GetService<KafkaClientBuilder>()
                .CreateConsumer<TKey, TValue>(builder.ConfigurationSectionName, builder.Loading, builder.Filter)
                .SetDefaultErrorHandlers();
            builder.ConfigureDelegate?.Invoke(builderTopic, s);
            return builderTopic;
        });
        services.AddSingleton<TConsumerService, TConsumerService>();
        services.AddSingleton<ITopicConsumer<TKey, TValue>>(s => s.GetService<TConsumerService>());
        if (asHostedService)
            services.AddSingleton<IHostedService>(s => s.GetService<TConsumerService>());
        return services;
    }

    public static IServiceCollection BuildAsync<TConsumerService, TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, bool asHostedService = true)
        where TConsumerService : AsyncConsumerService<TKey, TValue>
    {
        var services = builder.Services;
        services.AddSingleton(s =>
        {
            builder.PreConfigureDelegate?.Invoke(builder, s);
            var builderTopic = s.GetService<KafkaClientBuilder>()
                .CreateConsumer<TKey, TValue>(builder.ConfigurationSectionName, builder.Loading, builder.Filter)
                .SetDefaultErrorHandlers();
            builder.ConfigureDelegate?.Invoke(builderTopic, s);
            return builderTopic;
        });
        services.AddSingleton<TConsumerService, TConsumerService>();
        services.AddSingleton<IAsyncTopicConsumer<TKey, TValue>>(s => s.GetService<TConsumerService>());
        if (asHostedService)
            services.AddSingleton<IHostedService>(s => s.GetService<TConsumerService>());
        return services;
    }

    public static IServiceCollection BuildAsync<TKey, TValue>(this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        if (builder.Loading)
        {
            builder.BuildAsync<AsyncLoadingConsumerService<TKey, TValue>, TKey, TValue>();
            builder.Services.AddSingleton<IAsyncLoadingConsumer<TKey, TValue>>(s =>
                s.GetService<AsyncLoadingConsumerService<TKey, TValue>>());
            return builder.Services;
        }
        builder.BuildAsync<AsyncConsumerService<TKey, TValue>, TKey, TValue>();
        return builder.Services;
    }

    public static IServiceCollection BuildAsyncLoading<TKey, TValue>(this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        builder.BuildAsync<AsyncLoadingConsumerService<TKey, TValue>, TKey, TValue>();
        return builder.Services;
    }

    public static IServiceCollection Build<TKey, TValue>(this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        if (builder.Loading) return builder.BuildAsync();

        var clientConfigBuilder = builder.Services.BuildServiceProvider().GetService<KafkaClientBuilder>();
        var config = clientConfigBuilder.GetConsumerConfig<TKey, TValue>(builder.ConfigurationSectionName, builder.Loading, out var topicConfig);
        var isEoFEnabled = config.FirstOrDefault(s => s.Key == "enable.partition.eof").Value ?? "False";

        if (bool.Parse(isEoFEnabled))
        {
            builder.Build<ConsumerServicePartitionEOF<TKey, TValue>, TKey, TValue>();
            builder.Services.AddSingleton<ITopicConsumerEOF<TKey, TValue>>(s =>
                s.GetService<ConsumerServicePartitionEOF<TKey, TValue>>());
        }
        else if (topicConfig.Async)
            builder.BuildAsync<AsyncConsumerService<TKey, TValue>, TKey, TValue>();
        else
            builder.Build<EventConsumerService<TKey, TValue>, TKey, TValue>();

        return builder.Services;
    }

    [Obsolete("Use Build() with SetPauseOnEOF() if pause is required.")]
    public static IServiceCollection BuildWithPartitionEOF<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder,
        Microsoft.Extensions.Configuration.IConfigurationSection configurationSection)
    {
        builder.SetPauseOnEOF(configurationSection);
        builder.Build();
        return builder.Services;
    }

    public static ITopicConsumerBuilder<TKey, TValue> SetLoading<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, bool blocking = false)
    {
        builder.Loading = true;
        builder.Services.Configure<AsyncLoadingConsumerService<TKey, TValue>.LoadingConsumerConfiguration>(c => c.Blocking = blocking);
        return builder;
    }

    public static ITopicConsumerBuilder<TKey, TValue> SetPauseOnEOF<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder,
        Microsoft.Extensions.Configuration.IConfigurationSection configurationSection)
    {
        builder.Services.Configure<ConsumerServicePartitionEOF<TKey, TValue>.EOFConsumerConfiguration>(configurationSection);
        return builder;
    }

    public static IServiceCollection SetPauseOnEOF<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder, TimeSpan pauseTime)
    {
        builder.Services.Configure<ConsumerServicePartitionEOF<TKey, TValue>.EOFConsumerConfiguration>(a =>
            a.PartitionEOFPauseTime = pauseTime);
        return builder.Services;
    }

    public static Subscription<TKey, RentedBytes> BuildWithSubscription<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder,
        CancellationToken cancellationToken,
        EventHandler<ConsumeResult<TKey, TValue>> messageHandler,
        Action<IConsumer<TKey, RentedBytes>, TKey, long> heartbeatHandler,
        Action<IConsumer<TKey, RentedBytes>, TopicPartitionOffset> messageSkippedHandler)
    {
        var consumer = builder.Build();
        consumer.Subscribe(builder.TopicName.Split(','));

        var subscriptionTask = Task.Run(() =>
        {
            var logger = builder.Logger.ForContext<ConsumerBuilder<TKey, TValue>>();
            var isDisposed = false;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var rawConsumeResult = consumer.Consume(cancellationToken);
                    if (rawConsumeResult.Message.TryGetHeartbeatHeaderValue(out var heartbeatTimestamp))
                    {
                        if (builder.HeartbeatPredicate(heartbeatTimestamp, rawConsumeResult.Message.Headers ?? EmptyHeaders))
                            heartbeatHandler?.Invoke(consumer, rawConsumeResult.Message.Key, heartbeatTimestamp);
                        continue;
                    }
                    if (rawConsumeResult.IsPartitionEOF)
                    {
                        messageHandler.Invoke(consumer, CreateResult(rawConsumeResult, default(TValue)));
                    }
                    else if (builder.FilterPredicate(rawConsumeResult.Message.Headers ?? EmptyHeaders))
                    {
                        var deserializedValue = builder.InternalValueDeserializer.Deserialize(rawConsumeResult.Message.Value,
                            new SerializationContext(MessageComponentType.Value, rawConsumeResult.Topic, rawConsumeResult.Message?.Headers));
                        messageHandler.Invoke(consumer, CreateResult(rawConsumeResult, deserializedValue));
                    }
                    else
                        messageSkippedHandler(consumer, rawConsumeResult.TopicPartitionOffset);
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cancellationToken)
                    { logger.Information("Consumer '{Name}' polling operation canceled.", builder.Name); }
                catch (ObjectDisposedException) { isDisposed = true; break; }
                catch (Exception e) { logger.Error(e, "Consumer '{Name}' polling error.", builder.Name); }
            }
            if (!isDisposed) { consumer.Close(); consumer.Dispose(); }
        });

        return new Subscription<TKey, RentedBytes>(subscriptionTask, consumer);
    }

    public static Subscription<TKey, RentedBytes> BuildWithAsyncRawSubscription<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder,
        CancellationToken cancellationToken,
        Func<IConsumer<TKey, RentedBytes>, ConsumeResult<TKey, RentedBytes>, CancellationToken, ValueTask> messageHandler)
    {
        var consumer = builder.Build();
        consumer.Subscribe(builder.TopicName.Split(',').Select(t => t.Trim()));

        var subscriptionTask = Task.Run(async () =>
        {
            var logger = builder.Logger.ForContext<ConsumerBuilder<TKey, TValue>>();
            var isDisposed = false;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var rawConsumeResult = consumer.Consume(cancellationToken);
                    await messageHandler(consumer, rawConsumeResult, cancellationToken);
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cancellationToken)
                    { logger.Information("Consumer '{Name}' polling operation canceled.", builder.Name); }
                catch (ObjectDisposedException) { isDisposed = true; break; }
                catch (Exception e) { logger.Error(e, "Consumer '{Name}' polling error.", builder.Name); }
            }
            if (!isDisposed) { consumer.Close(); consumer.Dispose(); }
        });

        return new Subscription<TKey, RentedBytes>(subscriptionTask, consumer);
    }

    public static Subscription<TKey, RentedBytes> BuildWithAsyncSubscription<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder,
        CancellationToken cancellationToken,
        Func<IConsumer<TKey, RentedBytes>, ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> messageHandler,
        Func<IConsumer<TKey, RentedBytes>, TKey, long, CancellationToken, ValueTask> heartbeatHandler,
        Func<IConsumer<TKey, RentedBytes>, TopicPartitionOffset, CancellationToken, ValueTask> messageSkippedHandler)
    {
        var consumer = builder.Build();
        consumer.Subscribe(builder.TopicName.Split(',').Select(x => x.Trim()));

        var subscriptionTask = Task.Run(async () =>
        {
            var logger = builder.Logger.ForContext<ConsumerBuilder<TKey, TValue>>();
            var isDisposed = false;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var rawConsumeResult = consumer.Consume(cancellationToken);
                    if (rawConsumeResult.Message.TryGetHeartbeatHeaderValue(out var heartbeatTimestamp))
                    {
                        if (builder.HeartbeatPredicate(heartbeatTimestamp, rawConsumeResult.Message.Headers ?? EmptyHeaders))
                            await heartbeatHandler(consumer, rawConsumeResult.Message.Key, heartbeatTimestamp, cancellationToken);
                        continue;
                    }
                    if (rawConsumeResult.IsPartitionEOF)
                        await messageHandler(consumer, CreateResult(rawConsumeResult, default(TValue)), cancellationToken);
                    else if (builder.FilterPredicate(rawConsumeResult.Message.Headers ?? EmptyHeaders))
                    {
                        var deserializedValue = builder.InternalValueDeserializer.Deserialize(rawConsumeResult.Message.Value,
                            new SerializationContext(MessageComponentType.Value, rawConsumeResult.Topic, rawConsumeResult.Message?.Headers));
                        await messageHandler(consumer, CreateResult(rawConsumeResult, deserializedValue), cancellationToken);
                    }
                    else
                        await messageSkippedHandler(consumer, rawConsumeResult.TopicPartitionOffset, cancellationToken);
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cancellationToken) { }
                catch (ObjectDisposedException) { isDisposed = true; break; }
                catch (Exception e) { logger.Error(e, "Consumer '{Name}' polling error.", builder.Name); }
            }
            if (!isDisposed) { consumer.Close(); consumer.Dispose(); }
        });

        return new Subscription<TKey, RentedBytes>(subscriptionTask, consumer);
    }

    public static Subscription<TKey, RentedBytes> BuildWithSubscription<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder,
        EventHandler<ConsumeResult<TKey, TValue>> messageHandler,
        Action<IConsumer<TKey, RentedBytes>, TKey, long> heartbeatHandler,
        Action<IConsumer<TKey, RentedBytes>, TopicPartitionOffset> messageSkippedHandler)
        => builder.BuildWithSubscription(CancellationToken.None, messageHandler, heartbeatHandler, messageSkippedHandler);

    internal static ConsumeResult<TKey, TValue> CreateResult<TKey, TValue>(
        ConsumeResult<TKey, RentedBytes> original, TValue payload)
    {
        var message = original.Message == null ? null : new Message<TKey, TValue>
        {
            Headers = original.Message.Headers,
            Timestamp = original.Message.Timestamp,
            Key = original.Message.Key,
            Value = payload
        };
        return new ConsumeResult<TKey, TValue>
        {
            IsPartitionEOF = original.IsPartitionEOF,
            Offset = original.Offset,
            Partition = original.Partition,
            Topic = original.Topic,
            TopicPartitionOffset = original.TopicPartitionOffset,
            Message = message
        };
    }

    private static readonly Headers EmptyHeaders = new();

    private class TopicConsumerBuilder<TKey, TValue> : ITopicConsumerBuilder<TKey, TValue>
    {
        public IServiceCollection Services { get; set; }
        public string ConfigurationSectionName { get; set; }
        public bool Loading { get; set; }
        public Predicate<Headers> Filter { get; set; } = _ => true;
        public HeartbeatPredicate HeartbeatFilter { get; set; } = (_, _) => true;
        public Action<FilterableConsumerBuilderTopic<TKey, TValue>, IServiceProvider> ConfigureDelegate { get; set; }
        public Action<ITopicConsumerBuilder<TKey, TValue>, IServiceProvider> PreConfigureDelegate { get; set; }
        public bool IsAsync { get; set; }

        public ITopicConsumerBuilder<TKey, TValue> Configure(Action<FilterableConsumerBuilderTopic<TKey, TValue>> configure)
            => Configure((b, _) => configure(b));

        public ITopicConsumerBuilder<TKey, TValue> PreConfigure(Action<ITopicConsumerBuilder<TKey, TValue>, IServiceProvider> configure)
        {
            var current = PreConfigureDelegate;
            PreConfigureDelegate = current == null ? configure : (f, sp) => { current(f, sp); configure(f, sp); };
            return this;
        }

        public ITopicConsumerBuilder<TKey, TValue> Configure(Action<FilterableConsumerBuilderTopic<TKey, TValue>, IServiceProvider> configure)
        {
            var current = ConfigureDelegate;
            ConfigureDelegate = current == null ? configure : (f, sp) => { current(f, sp); configure(f, sp); };
            return this;
        }
    }
}
