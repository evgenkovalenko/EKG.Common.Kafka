namespace EKG.Common.Kafka;

using System.Reflection;
using System.Runtime.Serialization;
using Confluent.Kafka;
using EKG.Common.Kafka.Client;
using EKG.Common.Kafka.Serializers;
using MessagePack;

public static class MessagePackProducerBuilderExtensions
{
    public static ProducerBuilder<TKey, TValue> SetSerializersMessagePack<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
    {
        ValidateType(typeof(TKey));
        ValidateType(typeof(TValue));
        return builder.SetKeySerializer(new MsgPackSerializer<TKey>()).SetValueSerializer(new MsgPackSerializer<TValue>());
    }

    public static ProducerBuilder<TKey, TValue> SetKeySerializerMessagePack<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
    {
        ValidateType(typeof(TKey));
        return builder.SetKeySerializer(new MsgPackSerializer<TKey>());
    }

    public static ProducerBuilder<TKey, TValue> SetValueSerializerMessagePack<TKey, TValue>(
        this ProducerBuilder<TKey, TValue> builder)
    {
        ValidateType(typeof(TValue));
        return builder.SetValueSerializer(new MsgPackSerializer<TValue>());
    }

    public static ITopicProducerBuilder<TKey, TValue> SetSerializersMessagePack<TKey, TValue>(
        this ITopicProducerBuilder<TKey, TValue> builder)
    {
        builder.Configure(topicBuilder => topicBuilder.SetSerializersMessagePack());
        return builder;
    }

    private static void ValidateType(Type t)
    {
        if (!t.IsClass || t == typeof(Null) || t == typeof(string)) return;
        if (t.GetCustomAttribute<DataContractAttribute>() == null &&
            t.GetCustomAttribute<MessagePackObjectAttribute>() == null)
            throw new Exception($"{t.Name} does not have DataContractAttribute or MessagePackObjectAttribute");
    }
}
