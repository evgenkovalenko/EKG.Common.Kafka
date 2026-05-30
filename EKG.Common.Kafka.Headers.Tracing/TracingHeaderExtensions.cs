namespace EKG.Common.Kafka.Headers.Tracing;

using System.Diagnostics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Cysharp.Text;
using EKG.Common.Kafka.Headers.Abstractions;

public static class TracingHeaderExtensions
{
    private const int W3CTraceParentHeaderByteLength = 55;
    private static readonly ActivityTraceId DefaultTraceId = ActivityTraceId.CreateFromUtf8String(Encoding.UTF8.GetBytes("00000000000000000000000000000000"));
    private static readonly ActivitySpanId DefaultSpanId = ActivitySpanId.CreateFromUtf8String(Encoding.UTF8.GetBytes("0000000000000000"));

    public static bool TryGetTracingHeaderValue<TValue, TMessage>(
        this Message<TValue, TMessage> message, out TracingHeadersInfo tracingHeadersInfo)
    {
        if (message?.Headers is null || !TryGetLastHeadersBytes(out var headerBytes) || headerBytes.Length is not W3CTraceParentHeaderByteLength)
        {
            tracingHeadersInfo = default;
            return false;
        }

        var bytesSpan = headerBytes.AsSpan();

        Span<char> versionCharsSpan = stackalloc char[2];
        Encoding.UTF8.GetChars(bytesSpan[..2], versionCharsSpan);
        if (!byte.TryParse(versionCharsSpan, NumberStyles.HexNumber, NumberFormatInfo.InvariantInfo, out var version) || version is not 0)
        {
            tracingHeadersInfo = default;
            return false;
        }

        var traceIdHexBytes = bytesSpan.Slice(3, 32);
        var parentSpanIdHexBytes = bytesSpan.Slice(36, 16);

        Span<char> flagCharsSpan = stackalloc char[2];
        Encoding.UTF8.GetChars(bytesSpan[53..], flagCharsSpan);
        if (!byte.TryParse(flagCharsSpan, NumberStyles.HexNumber, NumberFormatInfo.InvariantInfo, out var flags))
        {
            tracingHeadersInfo = default;
            return false;
        }

        tracingHeadersInfo = new TracingHeadersInfo(
            ActivityTraceId.CreateFromUtf8String(traceIdHexBytes),
            ActivitySpanId.CreateFromUtf8String(parentSpanIdHexBytes),
            flags);

        var isValid = tracingHeadersInfo.TraceId != default
                      && tracingHeadersInfo.TraceId != DefaultTraceId
                      && tracingHeadersInfo.ParentSpanId != default
                      && tracingHeadersInfo.ParentSpanId != DefaultSpanId;

        if (!isValid) tracingHeadersInfo = default;
        return isValid;

        bool TryGetLastHeadersBytes(out byte[]? bytes)
        {
            bytes = null;
            for (var index = message.Headers.Count - 1; index >= 0; --index)
            {
                if (message.Headers[index].Key == TracingHeaders.TraceParent)
                {
                    bytes = message.Headers[index].GetValueBytes();
                    return true;
                }
            }
            return false;
        }
    }

    public static Message<TValue, TMessage> AddTracingHeaders<TValue, TMessage>(
        this Message<TValue, TMessage> message, TracingHeadersInfo tracingHeadersInfo)
    {
        message.Headers = (message.Headers ?? new Headers()).AddTracingHeaders(tracingHeadersInfo);
        return message;
    }

    public static Headers AddTracingHeaders(this Headers headers, TracingHeadersInfo tracingHeadersInfo)
    {
        if (tracingHeadersInfo.TraceId == default || tracingHeadersInfo.ParentSpanId == default)
            return headers;

        using var stringBuilder = ZString.CreateUtf8StringBuilder();
        stringBuilder.AppendFormat("{0}-{1}-{2}-{3}",
            tracingHeadersInfo.Version.ToString("X2"),
            tracingHeadersInfo.TraceId.ToHexString(),
            tracingHeadersInfo.ParentSpanId.ToHexString(),
            tracingHeadersInfo.Flags.ToString("X2"));

        headers.TryAddUtf8StringHeader(TracingHeaders.TraceParent, stringBuilder.ToString());
        return headers;
    }
}
