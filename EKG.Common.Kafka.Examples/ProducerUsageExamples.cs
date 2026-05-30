namespace EKG.Common.Kafka.Examples;

using Confluent.Kafka;
using EKG.Common.Kafka.Client;
using Microsoft.Extensions.Hosting;

public class ProducerUsageExamples : IHostedService
{
    private readonly ITopicProducer<int, MyDto> _producer;

    public ProducerUsageExamples(ITopicProducer<int, MyDto> producer)
        => _producer = producer;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var numberOfDifferentIds = 2;
        Task.Run(async () =>
        {
            var count = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                ++count;
                var msg = new Message<int, MyDto>
                {
                    Key = count % numberOfDifferentIds,
                    Value = new MyDto { Age = count, FirstName = Guid.NewGuid().ToString() }
                };
                await _producer.ProduceAsync(msg);
                await Task.Delay(1000, cancellationToken);
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
