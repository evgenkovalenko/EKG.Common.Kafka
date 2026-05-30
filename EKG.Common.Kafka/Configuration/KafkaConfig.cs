namespace EKG.Common.Kafka.Configuration;

public class KafkaConfig
{
    public KafkaGeneralConfig Config { get; set; }
    public Dictionary<string, TopicConfig> Producers { get; set; }
    public Dictionary<string, TopicConfig> Consumers { get; set; }
}
