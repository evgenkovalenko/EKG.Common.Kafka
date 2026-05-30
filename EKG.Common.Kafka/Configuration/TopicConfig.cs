namespace EKG.Common.Kafka.Configuration;

public class TopicConfig
{
    public required string TopicName { get; set; }
    public bool Async { get; set; }
    public bool Loading { get; set; }
    public Dictionary<string, string> Config { get; set; }
}
