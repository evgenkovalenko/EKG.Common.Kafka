namespace EKG.Common.Kafka.Extensions;

using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafka(this IServiceCollection services, string configSectionName = "kafka")
    {
        services.AddOptions<KafkaConfig>().BindConfiguration(configSectionName);
        services.TryAddSingleton(_ => Serilog.Log.Logger);
        services.AddSingleton<KafkaClientBuilder>();
        return services;
    }
}
