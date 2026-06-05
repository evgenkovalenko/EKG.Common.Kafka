namespace EKG.Common.Kafka.Examples;

using Confluent.Kafka;
using EKG.Common.Kafka.Client;
using Microsoft.Extensions.Hosting;
using Serilog;

public class ProducerUsageExamples : IHostedService
{
    private readonly ITopicProducer<int, MyDto> _producer;
    private readonly ILogger _logger;

    public ProducerUsageExamples(ITopicProducer<int, MyDto> producer, ILogger logger)
    {
        _producer = producer;
        _logger = logger.ForContext<ProducerUsageExamples>();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var numberOfDifferentIds = 2;
        Task.Run(async () =>
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    ++count;
                    var msg = new Message<int, MyDto>
                    {
                        Key = count % numberOfDifferentIds,
                        Value = new MyDto { Age = count % 126, FirstName = Guid.NewGuid().ToString() }
                    };
                    await _producer.ProduceAsync(msg);
                    _logger.Information("Produced message {Count} to {Topic}", count, _producer.Topic);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Failed to produce message {Count}", count);
                }
                await Task.Delay(5000, cancellationToken);
            }
        }, cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _producer.Dispose();
        return Task.CompletedTask;
    }
}
