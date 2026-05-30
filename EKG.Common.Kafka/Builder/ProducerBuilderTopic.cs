namespace EKG.Common.Kafka.Builder;

using Confluent.Kafka;
using Internal;
using Serilog;

internal class ProducerBuilderTopic<TKey, TValue> : ProducerBuilder<TKey, TValue>
{
    internal ILogger Logger { get; }
    public string TopicName { get; }

    public ProducerBuilderTopic(ILogger logger, IEnumerable<KeyValuePair<string, string>> config, string topicName)
        : base(ReplaceTransactionalIdKeywords(config))
    {
        Logger = logger;
        TopicName = topicName;
    }

    protected internal IEnumerable<KeyValuePair<string, string>> Configuration => Config;

    private static IEnumerable<KeyValuePair<string, string>> ReplaceTransactionalIdKeywords(
        IEnumerable<KeyValuePair<string, string>> config)
    {
        const string transactionalKey = "transactional.id";
        var newConfig = config.ToDictionary(s => s.Key, s => s.Value);
        if (!newConfig.TryGetValue(transactionalKey, out var value)) return newConfig;
        newConfig[transactionalKey] = value
            .Replace("[HostName]", Environment.MachineName)
            .Replace("[Guid]", Guid.NewGuid().ToString("n")[..10]);
        return newConfig;
    }
}
