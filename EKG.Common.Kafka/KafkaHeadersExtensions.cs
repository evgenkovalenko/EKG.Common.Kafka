namespace EKG.Common.Kafka;

using System.Runtime.CompilerServices;
using Confluent.Kafka;

public static class KafkaHeadersExtensions
{
    internal const string EmployeeIdKey = "EmployeeId";
    internal const string SiteIdKey = "SiteId";
    internal const string FeedIdKey = "FeedId";

    private static readonly byte[] TrueValue = { 1 };
    private static readonly byte[] FalseValue = { 0 };

    private static readonly IDeserializer<int> IntDeserializer = Deserializers.Int32;
    private static readonly IDeserializer<long> LongDeserializer = Deserializers.Int64;
    private static readonly IDeserializer<string> Utf8Deserializer = Deserializers.Utf8;
    private static readonly ISerializer<int> IntSerializer = Serializers.Int32;
    private static readonly ISerializer<long> LongSerializer = Serializers.Int64;
    private static readonly ISerializer<string> Utf8Serializer = Serializers.Utf8;

    public static bool TryGetFeedId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, out int feedId)
        => consumeResult.TryGetInt(FeedIdKey, out feedId);

    public static int? GetFeedId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult)
        => consumeResult.GetInt(FeedIdKey);

    public static Message<TKey, TValue> SetFeedId<TKey, TValue>(this Message<TKey, TValue> message, int feedId)
        => message.SetInt(FeedIdKey, feedId);

    public static bool TryGetEmployeeId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, out int employeeId)
        => consumeResult.TryGetInt(EmployeeIdKey, out employeeId);

    public static int? GetEmployeeId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult)
        => consumeResult.GetInt(EmployeeIdKey);

    public static Message<TKey, TValue> SetEmployeeId<TKey, TValue>(this Message<TKey, TValue> message, int employeeId)
        => message.SetInt(EmployeeIdKey, employeeId);

    public static bool TryGetSiteId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, out int siteId)
        => consumeResult.TryGetInt(SiteIdKey, out siteId);

    public static int? GetSiteId<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult)
        => consumeResult.GetInt(SiteIdKey);

    public static Message<TKey, TValue> SetSiteId<TKey, TValue>(this Message<TKey, TValue> message, int siteId)
        => message.SetInt(SiteIdKey, siteId);

    public static bool? GetHeaderValueBool(this Headers headers, string key)
    {
        if (headers.TryGetLastBytes(key, out var result) && result.Length is 1)
            return result[0] is 1;
        return null;
    }

    public static int? GetHeaderValueInt(this Headers headers, string key)
    {
        if (headers.TryGetLastBytes(key, out var result))
            return IntDeserializer.Deserialize(result, result == null, SerializationContext.Empty);
        return null;
    }

    public static long? GetHeaderValueLong(this Headers headers, string key)
    {
        if (headers.TryGetLastBytes(key, out var result))
            return LongDeserializer.Deserialize(result, result == null, SerializationContext.Empty);
        return null;
    }

    public static string GetHeaderValueUtf8(this Headers headers, string key)
        => headers.TryGetLastBytes(key, out var result)
            ? Utf8Deserializer.Deserialize(result, result == null, SerializationContext.Empty)
            : null;

    public static bool TryGetHeaderValueUtf8(this Headers headers, string key, out string value)
    {
        if (headers.TryGetLastBytes(key, out var result))
            return TryDeserializeUtf8(result, out value);
        value = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryDeserializeUtf8(byte[] bytes, out string value)
        => Utf8Deserializer.TryDeserialize(bytes, bytes is null || bytes.Length == 0, out value);

    public static bool TryGetBool<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, string key, out bool value)
    {
        value = default;
        var header = GetHeaderValueBool(consumeResult.Message.Headers, key);
        if (!header.HasValue) return false;
        value = header.Value;
        return true;
    }

    public static bool TryGetInt<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, string key, out int value)
    {
        value = default;
        var header = GetHeaderValueInt(consumeResult.Message.Headers, key);
        if (!header.HasValue) return false;
        value = header.Value;
        return true;
    }

    public static int? GetInt<TKey, TValue>(this ConsumeResult<TKey, TValue> consumeResult, string key)
        => GetHeaderValueInt(consumeResult.Message.Headers, key);

    public static IList<string> GetHeaderValueArray(this Headers headers, string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        var result = new List<string>();
        for (var i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            if (header?.Key != key) continue;
            var valueBytes = header.GetValueBytes();
            if (valueBytes?.Length > 0 && Utf8Deserializer.TryDeserialize(header.GetValueBytes(), false, out string value))
                result.Add(value);
        }
        return result;
    }

    public static Message<TKey, TValue> SetInt<TKey, TValue>(this Message<TKey, TValue> message, string key, int value)
    {
        message.Headers ??= new Headers();
        message.Headers.AddIntHeader(key, value);
        return message;
    }

    public static Headers AddBoolHeader(this Headers headers, string key, bool value)
    {
        headers ??= new Headers();
        headers.Add(key, value ? TrueValue : FalseValue);
        return headers;
    }

    public static Headers TryAddBoolHeader(this Headers headers, string key, bool? value)
        => value.HasValue ? headers.AddBoolHeader(key, value.Value) : headers;

    public static Headers AddIntHeader(this Headers headers, string key, int value)
    {
        headers ??= new Headers();
        headers.Add(key, IntSerializer.Serialize(value, SerializationContext.Empty));
        return headers;
    }

    public static Headers AddLongHeader(this Headers headers, string key, long value)
    {
        headers ??= new Headers();
        headers.Add(key, LongSerializer.Serialize(value, SerializationContext.Empty));
        return headers;
    }

    public static Headers AddUtf8StringHeader(this Headers headers, string key, string value)
    {
        headers ??= new Headers();
        headers.Add(key, Utf8Serializer.Serialize(value, SerializationContext.Empty));
        return headers;
    }

    public static Headers TryAddUtf8StringHeader(this Headers headers, string key, string value)
    {
        headers ??= new Headers();
        if (string.IsNullOrEmpty(value)) return headers;
        headers.Add(key, Utf8Serializer.Serialize(value, SerializationContext.Empty));
        return headers;
    }

    public static Headers AddHeader(this Headers headers, string key, byte[] bytes)
    {
        headers ??= new Headers();
        headers.Add(key, bytes);
        return headers;
    }

    public static Headers AddArrayHeader(this Headers headers, string key, IList<string> values)
    {
        headers ??= new Headers();
        if (string.IsNullOrEmpty(key) || values == null || values.Count == 0) return headers;
        foreach (var value in values)
            headers.Add(key, Utf8Serializer.Serialize(value, SerializationContext.Empty));
        return headers;
    }
}

internal static class SerializationExtensions
{
    public static bool TryDeserialize<T>(this IDeserializer<T> deserializer, ReadOnlySpan<byte> data, bool isNull, out T value)
        => deserializer.TryDeserialize(data, isNull, default, out value);

    public static bool TryDeserialize<T>(this IDeserializer<T> deserializer, ReadOnlySpan<byte> data, bool isNull, T defaultValue, out T value)
    {
        try { value = deserializer.Deserialize(data, isNull, SerializationContext.Empty); return true; }
        catch { value = defaultValue; return false; }
    }
}
