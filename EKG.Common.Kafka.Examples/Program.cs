using App.Metrics;
using Confluent.SchemaRegistry;
using EKG.Common.Kafka;
using EKG.Common.Kafka.Builder;
using EKG.Common.Kafka.Configuration;
using EKG.Common.Kafka.Examples;
using EKG.Common.Kafka.Metrics.AppMetrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", false)
    .Build();

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

var metrics = new MetricsBuilder()
    .Report.ToConsole()
    .Build();

var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
{
    Url = configuration["schemaRegistry:url"],
    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
    BasicAuthUserInfo = configuration["schemaRegistry:basicAuthUserInfo"]
});

var siteIds = new HashSet<int> { 5, 6 };

await new HostBuilder().ConfigureServices((_, services) =>
{
    services
        .AddSingleton<ILogger>(Log.Logger)
        .AddSingleton<IMetrics>(metrics)
        .AddSingleton<IMetricsRoot>(metrics);

    services.AddSingleton<IConfiguration>(configuration);
    services.Configure<KafkaConfig>(configuration.GetSection("kafka"));
    services.AddSingleton<KafkaClientBuilder>();

    services.AddTopicProducer<int, MyDto>("MyDto_Producer")
        .SetSerializersJson(schemaRegistry)
        .Build();

    services.AddTopicConsumer<int, MyDtoFiltered>("MyDtoConsumerOverride")
        .SetDeserializersJson()
        .ReportAppMetrics()
        .AddFilter(headers =>
        {
            var siteId = headers.GetHeaderValueInt("SiteId");
            return siteId.HasValue && siteIds.Contains(siteId.Value);
        })
        .Build();

    services.AddTopicConsumer<int, MyDto>("MyDto_Consumer1")
        .SetDeserializersJson()
        .Build();

    services.AddTopicConsumer<int, MyAsyncDto>("MyDto_AsyncLoadingConsumer")
        .SetDeserializersJson()
        .SetLoading(true)
        .BuildAsync<AsyncLoadingConsumerUsageExample, int, MyAsyncDto>();

    services.AddTopicConsumer<int, MyDtoProcessor>("MyDtoProcessorEOF")
        .SetDeserializersJson()
        .SetPauseOnEOF(configuration.GetSection("kafka:consumers:MyDtoProcessorEOF"))
        .Build();

    services.AddTopicConsumer<int, MyDtoProcessorBatching>("MyDtoProcessorBatchingEOF")
        .SetDeserializersJson()
        .SetPauseOnEOF(configuration.GetSection("kafka:consumers:MyDtoProcessorBatchingEOF"))
        .Build();

    services.AddHostedService<ProducerUsageExamples>();
    services.AddHostedService<ConsumerUsageExamples>();
})
.RunConsoleAsync();
