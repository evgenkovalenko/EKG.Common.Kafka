namespace EKG.Common.Kafka.Examples;

using System.Buffers;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Client;
using EKG.Common.Kafka.Consumer;
using EKG.Common.Kafka.Serialization;

/// <summary>
/// Strips the 5-byte Confluent Schema Registry wire-format header
/// (magic byte 0x00 + 4-byte schema ID) then deserializes the JSON payload.
/// </summary>
public class JsonSchemaRentedBytesDeserializer<T> : IRentedBytesDeserializer<T>
{
    private const int HeaderSize = 5; // 0x00 + schema-id (4 bytes)

    public T Deserialize(RentedBytes rentedBytes, SerializationContext context)
    {
        if (rentedBytes.IsEmpty) return default;
        try
        {
            var span = rentedBytes.AsReadOnlySpan();
            var json = span.Length > HeaderSize ? span[HeaderSize..] : span;
            return JsonSerializer.Deserialize<T>(json);
        }
        finally { rentedBytes.Free(); }
    }
}

public static class SchemaRegistryJsonConsumerExtensions
{
    private static readonly IDeserializer<RentedBytes> SharedRentedBytesDeserializer =
        new RentedBytesDeserializer(ArrayPool<byte>.Shared);

    public static ITopicConsumerBuilder<TKey, TValue> SetDeserializersJson<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        builder.Configure(b =>
        {
            b.SetKeyDeserializer(GetKeyDeserializer<TKey>());
            b.SetValueDeserializer(SharedRentedBytesDeserializer);
            b.SetInternalDeserializer(new JsonSchemaRentedBytesDeserializer<TValue>());
        });
        return builder;
    }

    private static IDeserializer<T> GetKeyDeserializer<T>()
    {
        if (typeof(T) == typeof(int)) return (IDeserializer<T>)Deserializers.Int32;
        if (typeof(T) == typeof(long)) return (IDeserializer<T>)Deserializers.Int64;
        if (typeof(T) == typeof(string)) return (IDeserializer<T>)Deserializers.Utf8;
        if (typeof(T) == typeof(Null)) return (IDeserializer<T>)Deserializers.Null;
        if (typeof(T) == typeof(Ignore)) return (IDeserializer<T>)Deserializers.Ignore;
        return new JsonKeyDeserializer<T>();
    }

    private class JsonKeyDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => isNull ? default : JsonSerializer.Deserialize<T>(data);
    }
}

public static class SchemaRegistryJsonProducerExtensions
{
    public static ITopicProducerBuilder<TKey, TValue> SetSerializersJson<TKey, TValue>(
        this ITopicProducerBuilder<TKey, TValue> builder, ISchemaRegistryClient schemaRegistry)
        where TValue : class
    {
        builder.Configure(b =>
        {
            b.SetKeySerializer(GetKeySerializer<TKey>());
            b.SetValueSerializer(new JsonSerializer<TValue>(schemaRegistry));
        });
        return builder;
    }

    private static ISerializer<T> GetKeySerializer<T>()
    {
        if (typeof(T) == typeof(int)) return (ISerializer<T>)Serializers.Int32;
        if (typeof(T) == typeof(long)) return (ISerializer<T>)Serializers.Int64;
        if (typeof(T) == typeof(string)) return (ISerializer<T>)Serializers.Utf8;
        if (typeof(T) == typeof(Null)) return (ISerializer<T>)Serializers.Null;
        return new JsonKeySerializer<T>();
    }

    private class JsonKeySerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
            => data == null ? null : JsonSerializer.SerializeToUtf8Bytes(data);
    }
}
