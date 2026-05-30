namespace EKG.Common.Kafka.Builder;

using Confluent.Kafka;
using EKG.Common.Kafka.Configuration;
using Microsoft.Extensions.Options;
using Serilog;

public class KafkaClientBuilder
{
    private readonly KafkaConfig _kafkaConfig;
    private readonly ILogger _logger;

    public KafkaClientBuilder(IOptionsMonitor<KafkaConfig> kafkaConfig, ILogger logger)
        : this(kafkaConfig.CurrentValue, logger) { }

    public KafkaClientBuilder(KafkaConfig kafkaConfig, ILogger logger)
    {
        _kafkaConfig = kafkaConfig ?? throw new ArgumentNullException(nameof(kafkaConfig));
        _logger = logger;
    }

    public ProducerBuilder<TKey, TValue> CreateProducer<TKey, TValue>(string name)
    {
        var configuration = GetProducerConfig<TKey, TValue>(name, out var clientConfig);
        return new ProducerBuilderTopic<TKey, TValue>(_logger, configuration, clientConfig.TopicName);
    }

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name)
        => CreateConsumer<TKey, TValue>(name, NoFiltrationPredicate);

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name, Predicate<Headers> filterPredicate)
        => CreateConsumer<TKey, TValue>(name, false, filterPredicate, NoHeartbeatPredicate);

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name, HeartbeatPredicate heartbeatPredicate)
        => CreateConsumer<TKey, TValue>(name, false, NoFiltrationPredicate, heartbeatPredicate);

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name, Predicate<Headers> filterPredicate, HeartbeatPredicate heartbeatPredicate)
        => CreateConsumer<TKey, TValue>(name, false, filterPredicate, heartbeatPredicate);

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name, bool ensureEof, Predicate<Headers> filterPredicate)
        => CreateConsumer<TKey, TValue>(name, ensureEof, filterPredicate, NoHeartbeatPredicate);

    public FilterableConsumerBuilderTopic<TKey, TValue> CreateConsumer<TKey, TValue>(string name, bool ensureEof, Predicate<Headers> filterPredicate, HeartbeatPredicate heartbeatPredicate)
    {
        if (filterPredicate == null) throw new ArgumentNullException(nameof(filterPredicate));
        var configuration = GetConsumerConfig<TKey, TValue>(name, ensureEof, out var clientConfig);
        return new FilterableConsumerBuilderTopic<TKey, TValue>(_logger, configuration, clientConfig.TopicName, filterPredicate, heartbeatPredicate, name);
    }

    internal IEnumerable<KeyValuePair<string, string>> GetProducerConfig<TKey, TValue>(string name, out TopicConfig clientConfig)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("Producer name cannot be null or empty", nameof(name));
        if (_kafkaConfig.Producers == null || !_kafkaConfig.Producers.TryGetValue(name, out clientConfig))
            throw new KeyNotFoundException($"Configuration for producer with name '{name}' not found");
        return MergeConfigs(ClientType.Producer, clientConfig);
    }

    internal IEnumerable<KeyValuePair<string, string>> GetConsumerConfig<TKey, TValue>(string name, bool ensureEof, out TopicConfig clientConfig)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("Consumer name cannot be null or empty", nameof(name));
        if (_kafkaConfig.Consumers == null || !_kafkaConfig.Consumers.TryGetValue(name, out clientConfig))
            throw new KeyNotFoundException($"Configuration for consumer with name '{name}' not found");

        var baseConfig = MergeConfigs(ClientType.Consumer, clientConfig);
        if (!ensureEof) return baseConfig;

        var list = baseConfig.Where(x => x.Key != "enable.partition.eof").ToList();
        list.Add(new KeyValuePair<string, string>("enable.partition.eof", "True"));
        return list;
    }

    private ICollection<KeyValuePair<string, string>> MergeConfigs(ClientType type, TopicConfig clientConfig)
    {
        var result = _kafkaConfig.Config?.Base?.ToList() ?? new List<KeyValuePair<string, string>>();
        var baseClientConfig = type == ClientType.Producer ? _kafkaConfig.Config?.Producer : _kafkaConfig.Config?.Consumer;
        if (baseClientConfig != null) result.AddRange(baseClientConfig);
        if (clientConfig?.Config != null && clientConfig.Config.Any()) result.AddRange(clientConfig.Config);
        return result.GroupBy(x => x.Key).Select(g => g.Last()).ToList();
    }

    private enum ClientType { Producer, Consumer }
    private Predicate<Headers> NoFiltrationPredicate => _ => true;
    private HeartbeatPredicate NoHeartbeatPredicate => (_, _) => true;
}
