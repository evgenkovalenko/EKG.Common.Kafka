namespace EKG.Common.Kafka;

using Confluent.Kafka;
using Microsoft.Extensions.Logging;

public static class KafkaDeliveryLoggerExtensions
{
    private const string DeliveryReportTemplate = "Kafka delivery {Outcome} on topic {KafkaTopic}. " +
        "Status: {KafkaPersistenceStatus}, ErrorCode: {KafkaErrorCode}, " +
        "Reason: {KafkaErrorReason}, Key: {KafkaMessageKey}, MetricsId: {MetricsId}";

    private const string DeliveryResultTemplate = "Kafka delivery {Outcome} on topic {KafkaTopic}. " +
        "Status: {KafkaPersistenceStatus}, Key: {KafkaMessageKey}, MetricsId: {MetricsId}";

    public static void LogDeliveryReport<TKey, TValue>(this ILogger logger, DeliveryReport<TKey, TValue> report)
    {
        try
        {
            if (report.Status is not (PersistenceStatus.NotPersisted or PersistenceStatus.PossiblyPersisted)) return;
            var (level, outcome) = report.Status == PersistenceStatus.NotPersisted
                ? (LogLevel.Error, "failed") : (LogLevel.Warning, "uncertain");
            logger.Log(level, DeliveryReportTemplate, outcome, report.Topic, report.Status,
                (int)report.Error.Code, report.Error.Reason,
                report.Message?.Key?.ToString(), TryGetMetricsId(report.Message?.Headers) ?? "-");
        }
        catch (Exception ex) { logger.LogError(ex, "Error in delivery report handler"); }
    }

    public static void LogDeliveryResult<TKey, TValue>(this ILogger logger, DeliveryResult<TKey, TValue> result)
    {
        if (result.Status is not (PersistenceStatus.NotPersisted or PersistenceStatus.PossiblyPersisted)) return;
        var (level, outcome) = result.Status == PersistenceStatus.NotPersisted
            ? (LogLevel.Error, "failed") : (LogLevel.Warning, "uncertain");
        logger.Log(level, DeliveryResultTemplate, outcome, result.Topic, result.Status,
            result.Message?.Key?.ToString(), TryGetMetricsId(result.Message?.Headers) ?? "-");
    }

    public static void LogProduceException<TKey, TValue>(this ILogger logger, ProduceException<TKey, TValue> ex)
        => logger.LogError(DeliveryReportTemplate, "failed", ex.DeliveryResult?.Topic, ex.DeliveryResult?.Status,
            (int)ex.Error.Code, ex.Error.Reason,
            ex.DeliveryResult?.Message?.Key?.ToString(),
            TryGetMetricsId(ex.DeliveryResult?.Message?.Headers) ?? "-");

    private static string TryGetMetricsId(Headers headers) => headers?.GetHeaderValueUtf8("X-Metrics-Id");
}
