namespace EKG.Common.Kafka;

using Confluent.Kafka;
using EKG.Common.Kafka.Client;
using EKG.Common.Kafka.Serializers;
using Google.Protobuf;

public static class ProtobufProducerBuilderExtensions
{
    public static ProducerBuilder<TKey, TValue> SetSerializersProtobuf<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
        where TKey : IMessage<TKey>, new()
        where TValue : IMessage<TValue>, new()
        => builder.SetKeySerializer(new ProtobufSerializer<TKey>()).SetValueSerializer(new ProtobufSerializer<TValue>());

    public static ProducerBuilder<TKey, TValue> SetKeySerializerProtobuf<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
        where TKey : IMessage<TKey>, new()
        => builder.SetKeySerializer(new ProtobufSerializer<TKey>());

    public static ProducerBuilder<TKey, TValue> SetValueSerializerProtobuf<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
        where TValue : IMessage<TValue>, new()
        => builder.SetValueSerializer(new ProtobufSerializer<TValue>());

    public static ITopicProducerBuilder<TKey, TValue> SetSerializersProtobuf<TKey, TValue>(
        this ITopicProducerBuilder<TKey, TValue> builder)
        where TKey : IMessage<TKey>, new()
        where TValue : IMessage<TValue>, new()
    {
        builder.Configure(topicBuilder => topicBuilder.SetSerializersProtobuf());
        return builder;
    }
}
