namespace EKG.Common.Kafka;

using Confluent.Kafka;

public static class HeartbeatHeaders
{
    public const string XHeartbeat = "X-Heartbeat";
}

public static class HeartbeatHeadersExtensions
{
    public static bool TryGetHeartbeatHeaderValue<TValue, TMessage>(this Message<TValue, TMessage> message, out long heartbeatNumber)
        => (heartbeatNumber = message?.Headers?.GetHeaderValueLong(HeartbeatHeaders.XHeartbeat) ?? -1) != -1;

    public static Message<TValue, TMessage> AddHeartbeatHeader<TValue, TMessage>(this Message<TValue, TMessage> message, long heartbeatNumber)
    {
        message.Headers = (message.Headers ?? new Headers()).AddLongHeader(HeartbeatHeaders.XHeartbeat, heartbeatNumber);
        return message;
    }
}
