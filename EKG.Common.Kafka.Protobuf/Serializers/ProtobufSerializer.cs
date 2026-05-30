namespace EKG.Common.Kafka.Serializers;

using Confluent.Kafka;
using Google.Protobuf;

public class ProtobufSerializer<T> : ISerializer<T> where T : IMessage<T>
{
    public byte[] Serialize(T data, SerializationContext context)
        => data == null ? null : data.ToByteArray();
}
