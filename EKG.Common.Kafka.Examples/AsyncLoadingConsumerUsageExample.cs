namespace EKG.Common.Kafka.Examples;

using Confluent.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Consumer;
using Microsoft.Extensions.Options;
using Serilog;

public class AsyncLoadingConsumerUsageExample : AsyncLoadingConsumerService<int, MyAsyncDto>
{
    private readonly ILogger _logger;

    public AsyncLoadingConsumerUsageExample(
        FilterableConsumerBuilderTopic<int, MyAsyncDto> builder,
        IOptionsMonitor<LoadingConsumerConfiguration> options,
        ILogger logger) : base(builder, options)
        => _logger = logger.ForContext<AsyncLoadingConsumerUsageExample>();

    protected override async ValueTask OnMessageAsync(object sender, ConsumeResult<int, MyAsyncDto> e, CancellationToken cancellationToken)
    {
        _logger.Information("MyAsyncDto: {Partition}-{Offset} Key: {Key}", e.Partition, e.Offset, e.Message?.Key);
        await Task.Delay(50, cancellationToken);
    }
}
