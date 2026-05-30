namespace EKG.Common.Kafka.Headers.Tracing;

using System.Diagnostics;

public readonly struct TracingHeadersInfo
{
    private const byte NoneFlag = 0b0;
    private const byte SampledFlag = 0b1;
    private const byte UnknownFlag = (0b1 << 4) | 0b1;

    public TracingHeadersInfo(ActivityTraceId traceId, ActivitySpanId parentSpanId, byte flags)
    {
        TraceId = traceId;
        ParentSpanId = parentSpanId;
        Flags = flags;
    }

    public byte Version { get; } = 0b0;
    public ActivityTraceId TraceId { get; }
    public ActivitySpanId ParentSpanId { get; }
    public byte Flags { get; }

    public TracingHeadersInfo WithTraceId(ActivityTraceId traceId) => new(traceId, ParentSpanId, Flags);
    public TracingHeadersInfo WithParentSpanId(ActivitySpanId parentSpanId) => new(TraceId, parentSpanId, Flags);
    public TracingHeadersInfo WithFlags(byte flags) => new(TraceId, ParentSpanId, flags);
    public TracingHeadersInfo WithFlags(bool isRecorded) => new(TraceId, ParentSpanId, isRecorded ? SampledFlag : NoneFlag);
    public TracingHeadersInfo WithNoneFlag() => new(TraceId, ParentSpanId, NoneFlag);
    public TracingHeadersInfo WithSampledFlag() => new(TraceId, ParentSpanId, SampledFlag);
    public TracingHeadersInfo WithUnknownFlag() => new(TraceId, ParentSpanId, UnknownFlag);

    public bool IsWithNoneFlag() => Flags is NoneFlag;
    public bool IsWithSampledFlag() => Flags is SampledFlag;
    public bool IsWithUnknownFlag() => Flags is UnknownFlag;

    public ActivityTraceFlags GetActivityTraceFlags()
        => Flags switch
        {
            NoneFlag => ActivityTraceFlags.None,
            SampledFlag => ActivityTraceFlags.Recorded,
            _ => ActivityTraceFlags.None
        };
}
