namespace EKG.Common.Kafka;

using System.Buffers;
using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Consumer;
using EKG.Common.Kafka.Serialization;
using EKG.Common.Kafka.Serializers;
using Google.Protobuf;

public static class ProtobufConsumerBuilderExtensions
{
    private static readonly IDeserializer<RentedBytes> SharedRentedBytesDeserializer =
        new RentedBytesDeserializer(ArrayPool<byte>.Shared);

    public static ITopicConsumerBuilder<TKey, TValue> SetDeserializersProtobuf<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
        where TValue : IMessage<TValue>, new()
        where TKey : IMessage<TKey>, new()
    {
        builder.Configure(topicBuilder => topicBuilder.SetDeserializersProtobuf());
        return builder;
    }

    public static ITopicConsumerBuilder<TKey, TValue> SetKeyDeserializerProtobuf<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
        where TKey : IMessage<TKey>, new()
    {
        builder.Configure(topicBuilder => topicBuilder.SetKeyDeserializerProtobuf());
        return builder;
    }

    public static ITopicConsumerBuilder<TKey, TValue> SetValueDeserializerProtobuf<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
        where TValue : IMessage<TValue>, new()
    {
        builder.Configure(topicBuilder =>
        {
            topicBuilder.SetValueDeserializer(SharedRentedBytesDeserializer);
            topicBuilder.SetInternalDeserializer(new ProtobufRentedBytesDeserializer<TValue>());
        });
        return builder;
    }

    public static FilterableConsumerBuilderTopic<TKey, TValue> SetDeserializersProtobuf<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder)
        where TValue : IMessage<TValue>, new()
        where TKey : IMessage<TKey>, new()
    {
        builder.SetKeyDeserializerProtobuf();
        builder.SetValueDeserializer(SharedRentedBytesDeserializer);
        builder.SetInternalDeserializer(new ProtobufRentedBytesDeserializer<TValue>());
        return builder;
    }

    public static ConsumerBuilder<TKey, TValue> SetKeyDeserializerProtobuf<TKey, TValue>(
        this ConsumerBuilder<TKey, TValue> builder)
        where TKey : IMessage<TKey>, new()
    {
        builder.SetKeyDeserializer(new ProtobufDeserializer<TKey>());
        return builder;
    }

    public static ConsumerBuilder<TKey, TValue> SetValueDeserializerProtobuf<TKey, TValue>(
        this ConsumerBuilder<TKey, TValue> builder)
        where TValue : IMessage<TValue>, new()
    {
        builder.SetValueDeserializer(new ProtobufDeserializer<TValue>());
        return builder;
    }
}
