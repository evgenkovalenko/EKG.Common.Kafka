namespace EKG.Common.Kafka.Headers.Abstractions;

public readonly struct MonitoringHeadersInfo
{
    public MonitoringHeadersInfo(string appName, string machineName)
    {
        AppName = appName;
        MachineName = machineName;
    }

    public string AppName { get; }
    public string MachineName { get; }
}
