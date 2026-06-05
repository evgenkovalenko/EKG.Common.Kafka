namespace EKG.Common.Kafka.Configuration;

public class KafkaGeneralConfig
{
    // Env-var-friendly alternatives to Base dictionary keys that contain dots
    // (POSIX env var names cannot contain dots, so "bootstrap.servers" etc. can't be set directly).
    // When non-empty, these override the corresponding Base dictionary entries.
    public string? BootstrapServers { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }

    public Dictionary<string, string> Base { get; set; }
    public Dictionary<string, string> Producer { get; set; }
    public Dictionary<string, string> Consumer { get; set; }
}
