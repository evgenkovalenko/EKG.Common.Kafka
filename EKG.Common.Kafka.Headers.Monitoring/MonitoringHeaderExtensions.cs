namespace EKG.Common.Kafka.Headers.Monitoring;

using Confluent.Kafka;
using EKG.Common.Kafka.Headers.Abstractions;

public static class MonitoringHeaderExtensions
{
    public static bool TryGetMonitoringHeaderValue<TValue, TMessage>(
        this Message<TValue, TMessage> message, out MonitoringHeadersInfo monitoringHeadersInfo)
    {
        if (message?.Headers is null || !message.Headers.TryGetHeaderValueUtf8(BaseHeaders.XAppName, out var appName))
        {
            monitoringHeadersInfo = default;
            return false;
        }

        var applicationName = string.IsNullOrEmpty(appName) ? BaseHeaders.NotProvided : appName;
        var machineName = message.Headers.GetHeaderValueUtf8(BaseHeaders.XMachineName) ?? BaseHeaders.NotProvided;
        monitoringHeadersInfo = new MonitoringHeadersInfo(applicationName, machineName);
        return true;
    }

    public static Message<TValue, TMessage> AddMonitoringHeaders<TValue, TMessage>(
        this Message<TValue, TMessage> message, MonitoringHeadersInfo monitoringHeadersInfo)
    {
        message.Headers = (message.Headers ?? new Headers())
            .AddUtf8StringHeader(BaseHeaders.XAppName, monitoringHeadersInfo.AppName)
            .AddUtf8StringHeader(BaseHeaders.XMachineName, monitoringHeadersInfo.MachineName);
        return message;
    }
}
