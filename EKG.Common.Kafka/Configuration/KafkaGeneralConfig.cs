namespace EKG.Common.Kafka.Configuration;

public class KafkaGeneralConfig
{
    public Dictionary<string, string> Base { get; set; }
    public Dictionary<string, string> Producer { get; set; }
    public Dictionary<string, string> Consumer { get; set; }
}
