namespace EKG.Common.Kafka.Metrics.AppMetrics;

using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Consumer;
using EKG.Common.Kafka.Metrics.AppMetrics.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

public static class ConsumerBuilderExtensions
{
    public static ITopicConsumerBuilder<TKey, TValue> ReportAppMetrics<TKey, TValue>(
        this ITopicConsumerBuilder<TKey, TValue> builder)
    {
        builder.Services.TryAddSingleton<KafkaStatsReporter>();
        builder.Services.TryAddSingleton<IHostedService>(sp => sp.GetRequiredService<KafkaStatsReporter>());
        builder.Configure((b, sp) => b.ReportAppMetrics(sp.GetRequiredService<KafkaStatsReporter>()));
        return builder;
    }

    public static FilterableConsumerBuilderTopic<TKey, TValue> ReportAppMetrics<TKey, TValue>(
        this FilterableConsumerBuilderTopic<TKey, TValue> builder, KafkaStatsReporter kafkaStatsReporter)
    {
        builder.SetStatisticsHandler((_, s) => kafkaStatsReporter.Report(s));
        return builder;
    }
}
