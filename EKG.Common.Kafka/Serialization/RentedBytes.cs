namespace EKG.Common.Kafka.Serialization;

using System.Buffers;
using System.Runtime.CompilerServices;
using Confluent.Kafka;

public readonly struct RentedBytes : IDisposable
{
    public static readonly RentedBytes Empty = new(ReadOnlySpan<byte>.Empty, true, ArrayPool<byte>.Shared);

    private readonly ArrayPool<byte> _pool;
    private readonly byte[] _rentedBytes;

    public RentedBytes(ReadOnlySpan<byte> data, bool isNull, ArrayPool<byte> pool)
    {
        _pool = pool;
        if (isNull || data.Length is 0)
        {
            Size = 0;
            _rentedBytes = null;
            return;
        }

        _rentedBytes = _pool.Rent(data.Length);
        Size = data.Length;
        data.CopyTo(_rentedBytes);
    }

    public bool IsEmpty
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Size is 0;
    }

    public int Size
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Free();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Free()
    {
        if (_rentedBytes != null)
            _pool.Return(_rentedBytes);
    }

    public void CopyTo(Span<byte> target) => _rentedBytes?.CopyTo(target);

    public ReadOnlySpan<byte> AsReadOnlySpan()
        => Size is 0 ? ReadOnlySpan<byte>.Empty : new ReadOnlySpan<byte>(_rentedBytes, 0, Size);

    public ReadOnlyMemory<byte> AsReadOnlyMemory()
        => Size is 0 ? ReadOnlyMemory<byte>.Empty : new ReadOnlyMemory<byte>(_rentedBytes, 0, Size);

    public byte[] AsByteArray()
        => Size == _rentedBytes.Length ? _rentedBytes : AsReadOnlyMemory().ToArray();
}

public sealed class RentedBytesDeserializer : IDeserializer<RentedBytes>
{
    public static IDeserializer<RentedBytes> Empty = new RentedBytesEmptyDeserializer();

    private readonly ArrayPool<byte> _pool;

    public RentedBytesDeserializer(ArrayPool<byte> pool) => _pool = pool;

    public RentedBytes Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => new(data, isNull, _pool);

    private sealed class RentedBytesEmptyDeserializer : IDeserializer<RentedBytes>
    {
        public RentedBytes Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => RentedBytes.Empty;
    }
}

public sealed class RentedBytesSerializer : ISerializer<RentedBytes>
{
    public byte[] Serialize(RentedBytes data, SerializationContext context) => data.AsByteArray();
}
