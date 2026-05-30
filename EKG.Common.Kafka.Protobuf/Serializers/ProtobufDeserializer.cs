namespace EKG.Common.Kafka.Serializers;

using Confluent.Kafka;
using EKG.Common.Kafka.Serialization;
using Google.Protobuf;

public class ProtobufDeserializer<T> : IDeserializer<T> where T : IMessage<T>, new()
{
    private readonly MessageParser<T> _parser = new(() => new T());

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => isNull ? default : _parser.ParseFrom(data);
}

public class ProtobufRentedBytesDeserializer<T> : IRentedBytesDeserializer<T> where T : IMessage<T>, new()
{
    private readonly MessageParser<T> _parser = new(() => new T());

    public T Deserialize(RentedBytes rentedBytes, SerializationContext context)
    {
        if (rentedBytes.IsEmpty) return default;
        try { return _parser.ParseFrom(rentedBytes.AsByteArray()); }
        finally { rentedBytes.Free(); }
    }
}
