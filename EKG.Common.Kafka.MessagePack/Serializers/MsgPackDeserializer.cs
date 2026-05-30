namespace EKG.Common.Kafka.Serializers;

using Confluent.Kafka;
using EKG.Common.Kafka.Serialization;
using MessagePack;

public class MsgPackDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => isNull ? default : MessagePackSerializer.Deserialize<T>(data.ToArray());
}

public class MsgPackRentedBytesDeserializer<T> : IRentedBytesDeserializer<T>
{
    public T Deserialize(RentedBytes rentedBytes, SerializationContext context)
    {
        if (rentedBytes.IsEmpty) return default;
        try { return MessagePackSerializer.Deserialize<T>(rentedBytes.AsReadOnlyMemory()); }
        finally { rentedBytes.Free(); }
    }
}
