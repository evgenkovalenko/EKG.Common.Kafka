namespace EKG.Common.Kafka.Extensions;

using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Configuration;
using Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafka(this IServiceCollection services, string configSectionName = "kafka")
    {
        services.AddOptions<KafkaConfig>().BindConfiguration(configSectionName);
        services.AddSingleton<KafkaClientBuilder>();
        return services;
    }
}
