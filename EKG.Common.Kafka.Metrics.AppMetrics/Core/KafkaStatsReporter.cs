namespace EKG.Common.Kafka.Metrics.AppMetrics.Core;

using System.Text.Json;
using System.Threading.Channels;
using App.Metrics;
using App.Metrics.Gauge;
using Microsoft.Extensions.Hosting;
using Serilog;

public class KafkaStatsReporter : IHostedService
{
    private const string UnassignedPartitionId = "-1";

    private readonly CancellationTokenSource _cts = new();
    private readonly Channel<string> _statsChannel = Channel.CreateBounded<string>(
        new BoundedChannelOptions(10) { FullMode = BoundedChannelFullMode.DropOldest });

    private readonly IMetricsRoot _metricsRoot;
    private readonly ILogger _logger;

    private static GaugeOptions GaugeOptions => new()
    {
        Name = "KafkaConsumerLag",
        MeasurementUnit = Unit.Items,
        ResetOnReporting = true
    };

    private Task _reportingTask;

    public KafkaStatsReporter(IMetricsRoot metricsRoot, ILogger logger)
    {
        _metricsRoot = metricsRoot;
        _logger = logger.ForContext<KafkaStatsReporter>();
    }

    public void Report(string stats) => _statsChannel.Writer.TryWrite(stats);

    private async Task Listen(ChannelReader<string> reader, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var rawStats = await reader.ReadAsync(cancellationToken);
                ReportStats(JsonSerializer.Deserialize<KafkaStats>(rawStats));
            }
            catch (Exception exception)
            {
                _logger.Error(exception, "Error while reporting lag.");
            }
        }
    }

    private void ReportStats(KafkaStats kafkaStats)
    {
        var topicStats = kafkaStats.Topics.Values
            .SelectMany(t => t.Partitions.Values
                .Where(p => p.Partition != UnassignedPartitionId && p.ConsumerLagStored > -1)
                .Select(p => (t.Topic, p.Partition, ConsumerLagStored: Math.Max(0, p.ConsumerLagStored))));

        foreach (var stat in topicStats)
            _metricsRoot.Measure.Gauge.SetValue(GaugeOptions,
                new MetricTags(new[] { "topic", "partition" }, new[] { stat.Topic, stat.Partition }),
                stat.ConsumerLagStored);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _reportingTask = Listen(_statsChannel.Reader, _cts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_reportingTask is null) return;
        try { _cts.Cancel(); }
        finally { await Task.WhenAny(_reportingTask, Task.Delay(Timeout.Infinite, cancellationToken)); }
    }
}
