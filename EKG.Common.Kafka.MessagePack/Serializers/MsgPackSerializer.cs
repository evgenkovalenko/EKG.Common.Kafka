namespace EKG.Common.Kafka.Serializers;

using Confluent.Kafka;
using MessagePack;

public class MsgPackSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
        => data == null ? null : MessagePackSerializer.Serialize(data);
}
