namespace EKG.Common.Kafka.Builder;

using System.Collections.Immutable;
using Confluent.Kafka;
using EKG.Common.Kafka.Serialization;
using Internal;
using Serilog;

public delegate bool HeartbeatPredicate(long heartbeatTimestamp, Headers headers);

public class FilterableConsumerBuilderTopic<TKey, TValue> : ConsumerBuilder<TKey, RentedBytes>
{
    internal ILogger Logger { get; }
    public Predicate<Headers> FilterPredicate { get; }
    public HeartbeatPredicate HeartbeatPredicate { get; }
    public IRentedBytesDeserializer<TValue> InternalValueDeserializer { get; private set; }
    public string TopicName { get; }
    public string Name { get; }

    public HashSet<Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask>> Subscribtions { get; set; }
        = new();

    public FilterableConsumerBuilderTopic(ILogger logger, IEnumerable<KeyValuePair<string, string>> config,
        string topicName, Predicate<Headers> filter, HeartbeatPredicate heartbeatPredicate, string name)
        : base(ReplaceConsumerGroupIdKeywords(config))
    {
        Logger = logger;
        TopicName = topicName;
        FilterPredicate = filter;
        HeartbeatPredicate = heartbeatPredicate;
        Name = name;
    }

    public FilterableConsumerBuilderTopic(ILogger logger, IEnumerable<KeyValuePair<string, string>> config,
        string topicName, Predicate<Headers> filter, HeartbeatPredicate heartbeatPredicate)
        : this(logger, config, topicName, filter, heartbeatPredicate, Guid.NewGuid().ToString()) { }

    public void AddSubscription(Func<ConsumeResult<TKey, TValue>, CancellationToken, ValueTask> handler)
        => Subscribtions.Add(handler);

    public ImmutableDictionary<string, string> Configuration => Config.ToImmutableDictionary();

    public void SetInternalDeserializer(IRentedBytesDeserializer<TValue> deserializer)
        => InternalValueDeserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));

    private static IEnumerable<KeyValuePair<string, string>> ReplaceConsumerGroupIdKeywords(
        IEnumerable<KeyValuePair<string, string>> config)
    {
        const string groupIdKey = "group.id";
        var newConfig = config.ToDictionary(s => s.Key, s => s.Value);
        if (newConfig.TryGetValue(groupIdKey, out var value))
            newConfig[groupIdKey] = NamingTools.FillInVariableValues(value);
        return newConfig;
    }
}
