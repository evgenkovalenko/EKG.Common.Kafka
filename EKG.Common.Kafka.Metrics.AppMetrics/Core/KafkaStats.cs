namespace EKG.Common.Kafka.Metrics.AppMetrics.Core;

using System.Text.Json.Serialization;

public class KafkaStats
{
    [JsonPropertyName("topics")]
    public Dictionary<string, TopicStats> Topics { get; set; }
}

public class TopicStats
{
    [JsonPropertyName("topic")]
    public string Topic { get; set; }

    [JsonPropertyName("partitions")]
    public Dictionary<string, PartitionStats> Partitions { get; set; }
}

public class PartitionStats
{
    [JsonPropertyName("partition")]
    public string Partition { get; set; }

    [JsonPropertyName("consumer_lag")]
    public long ConsumerLag { get; set; }

    [JsonPropertyName("consumer_lag_stored")]
    public long ConsumerLagStored { get; set; }

    [JsonPropertyName("stored_offset")]
    public long StoredOffset { get; set; }

    [JsonPropertyName("committed_offset")]
    public long CommittedOffset { get; set; }
}
