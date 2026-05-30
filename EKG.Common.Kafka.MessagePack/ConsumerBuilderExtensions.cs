namespace EKG.Common.Kafka;

using System.Buffers;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Consumer;
using EKG.Common.Kafka.Serialization;
using EKG.Common.Kafka.Serializers;

public static class MessagePackConsumerBuilderExtensions
{
    private static readonly IDeserializer<RentedBytes> SharedRentedBytesDeserializer =
        new RentedBytesDeserializer(ArrayPool<byte>.Shared);

    public static FilterableConsumerBuilderTopic<TKey, TValue> SetDeserializersMessagePack<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder)
    {
        builder.SetKeyDeserializer(GetDeserializer<TKey>());
        builder.SetValueDeserializer(GetValueDeserializer<TValue>());
        builder.SetInternalDeserializer(GetRentedBytesDeserializer<TValue>());
        return builder;
    }

    public static ConsumerBuilder<TKey, TValue> SetDeserializersMessagePack<TKey, TValue>(
        this ConsumerBuilder<TKey, TValue> builder)
        => builder.SetKeyDeserializer(GetDeserializer<TKey>()).SetValueDeserializer(GetDeserializer<TValue>());

    public static ITopicConsumerBuilder<TKey, TValue> SetDeserializersMessagePack<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        builder.Configure(topicBuilder => topicBuilder.SetDeserializersMessagePack());
        return builder;
    }

    private static IDeserializer<T> GetDeserializer<T>()
    {
        if (typeof(T) == typeof(RentedBytes)) return (IDeserializer<T>)SharedRentedBytesDeserializer;
        if (typeof(T) == typeof(Ignore)) return (IDeserializer<T>)Deserializers.Ignore;
        if (typeof(T) == typeof(Null)) return (IDeserializer<T>)Deserializers.Null;
        if (typeof(T) == typeof(byte[])) return (IDeserializer<T>)Deserializers.ByteArray;
        return new MsgPackDeserializer<T>();
    }

    private static IDeserializer<RentedBytes> GetValueDeserializer<T>()
    {
        if (typeof(T) == typeof(Ignore) || typeof(T) == typeof(Null))
            return RentedBytesDeserializer.Empty;
        return SharedRentedBytesDeserializer;
    }

    private static IRentedBytesDeserializer<T> GetRentedBytesDeserializer<T>()
    {
        if (typeof(T) == typeof(Ignore)) return (IRentedBytesDeserializer<T>)KafkaRentedBytesDeserializer.Ignore;
        if (typeof(T) == typeof(Null)) return (IRentedBytesDeserializer<T>)KafkaRentedBytesDeserializer.Null;
        if (typeof(T) == typeof(byte[])) return (IRentedBytesDeserializer<T>)KafkaRentedBytesDeserializer.ByteArray;
        return new MsgPackRentedBytesDeserializer<T>();
    }
}
