namespace EKG.Common.Kafka.Serialization;

using Confluent.Kafka;

public interface IRentedBytesDeserializer<out T>
{
    T Deserialize(RentedBytes rentedBytes, SerializationContext context);
}

public static class KafkaRentedBytesDeserializer
{
    public static readonly IRentedBytesDeserializer<Ignore> Ignore = new IgnoreRentedBytesDeserializer();
    public static readonly IRentedBytesDeserializer<Null> Null = new NullRentedBytesDeserializer();
    public static readonly IRentedBytesDeserializer<byte[]> ByteArray = new ByteArrayRentedBytesDeserializer();

    private class ByteArrayRentedBytesDeserializer : IRentedBytesDeserializer<byte[]>
    {
        public byte[] Deserialize(RentedBytes rentedBytes, SerializationContext context)
        {
            if (rentedBytes.IsEmpty) return Array.Empty<byte>();
            try { return rentedBytes.AsReadOnlyMemory().ToArray(); }
            finally { rentedBytes.Free(); }
        }
    }

    private class IgnoreRentedBytesDeserializer : IRentedBytesDeserializer<Ignore>
    {
        public Ignore Deserialize(RentedBytes rentedBytes, SerializationContext context)
        {
            rentedBytes.Free();
            return null;
        }
    }

    private class NullRentedBytesDeserializer : IRentedBytesDeserializer<Null>
    {
        public Null Deserialize(RentedBytes rentedBytes, SerializationContext context)
        {
            if (!rentedBytes.IsEmpty)
                throw new ArgumentException("Deserializer<Null> may only be used to deserialize data that is null.");
            return (Confluent.Kafka.Null)null;
        }
    }
}

public sealed class KafkaRentedBytesDeserializer<T> : IRentedBytesDeserializer<T>
{
    private readonly IDeserializer<T> _kafkaDeserializer;

    public KafkaRentedBytesDeserializer(IDeserializer<T> kafkaDeserializer)
        => _kafkaDeserializer = kafkaDeserializer;

    public T Deserialize(RentedBytes rentedBytes, SerializationContext context)
        => _kafkaDeserializer.Deserialize(rentedBytes.AsReadOnlySpan(), rentedBytes.IsEmpty, context);
}
