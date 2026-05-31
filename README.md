# EKG.Common.Kafka

Confluent Kafka wrapper for the EKG platform. Provides consumer and producer abstractions with DI integration, pluggable serializers (MessagePack, Protobuf, JSON Schema Registry), header utilities, and App.Metrics consumer-lag reporting.

Ported from `SBTech.Sports.Communication.Kafka`, targeting `.NET 10`.

---

## Packages

| NuGet Package | Description |
|---|---|
| `EKG.Common.Kafka` | Core — config, builder, consumer/producer abstractions, DI, header extensions |
| `EKG.Common.Kafka.MessagePack` | MessagePack serializer / deserializer extensions |
| `EKG.Common.Kafka.Protobuf` | Protobuf serializer / deserializer extensions |
| `EKG.Common.Kafka.Headers.Abstractions` | Shared header constants (`BaseHeaders`, `TracingHeaders`) and info structs |
| `EKG.Common.Kafka.Headers.Monitoring` | `TryGetMonitoringHeaderValue` / `AddMonitoringHeaders` |
| `EKG.Common.Kafka.Headers.Tracing` | W3C `traceparent` header parse / format (uses ZString) |
| `EKG.Common.Kafka.Metrics.AppMetrics` | Consumer-lag gauge reporting via App.Metrics |

All packages are published to GitHub Packages on every push to `main`.

---

## Installation

Add the GitHub Packages feed to your `nuget.config` (keep this file out of source control):

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="github" value="https://nuget.pkg.github.com/evgenkovalenko/index.json" />
  </packageSources>
  <packageSourceCredentials>
    <github>
      <add key="Username" value="evgenkovalenko" />
      <add key="ClearTextPassword" value="YOUR_GITHUB_PAT" />
    </github>
  </packageSourceCredentials>
</configuration>
```

Then add the packages you need:

```xml
<PackageReference Include="EKG.Common.Kafka" Version="1.0.*" />
<PackageReference Include="EKG.Common.Kafka.MessagePack" Version="1.0.*" />
```

---

## Configuration

Configuration is read from `appsettings.json` under the `kafka` key (default; override via `AddKafka("mySection")`).

```json
{
  "kafka": {
    "config": {
      "base": {
        "bootstrap.servers": "localhost:9092"
      },
      "producer": {
        "acks": "all"
      },
      "consumer": {
        "enable.auto.commit": true,
        "auto.commit.interval.ms": 1000,
        "statistics.interval.ms": 10000
      }
    },
    "producers": {
      "MyProducer": {
        "topicName": "my-topic",
        "config": {
          "auto.offset.reset": "latest"
        }
      }
    },
    "consumers": {
      "MyConsumer": {
        "topicName": "my-topic",
        "config": {
          "auto.offset.reset": "earliest",
          "group.id": "my-group"
        }
      }
    }
  }
}
```

### Config merge order

For each producer/consumer the final librdkafka config is built by merging (last wins):

1. `config.base` — shared by all clients
2. `config.producer` / `config.consumer` — shared by all producers or all consumers
3. `producers.<name>.config` / `consumers.<name>.config` — per-client overrides

### Dynamic placeholder values in config

| Placeholder | Replaced with |
|---|---|
| `[HostName]` | `Environment.MachineName` |
| `[Guid]` | Random 10-char hex string |
| `[Env:VAR]` | Environment variable `VAR` |

Useful for `group.id` and `transactional.id`:

```json
"group.id": "my-app.[HostName].[Guid]"
```

### Confluent Cloud

```json
{
  "kafka": {
    "config": {
      "base": {
        "bootstrap.servers": "<cluster>.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "<cluster-api-key>",
        "sasl.password": "<cluster-api-secret>",
        "session.timeout.ms": "45000",
        "client.id": "my-app"
      }
    }
  }
}
```

> Use a **Cluster API key** (not a Global API key) for Kafka authentication.

---

## DI Registration

```csharp
// Registers KafkaClientBuilder + binds IOptions<KafkaConfig> from "kafka" section
services.AddKafka();

// Custom section name
services.AddKafka("myKafkaSection");
```

`KafkaClientBuilder` requires `ILogger` (Serilog) in the DI container.

---

## Producers

### Register

```csharp
services.AddTopicProducer<int, MyMessage>("MyProducer")
    .SetSerializersMessagePack()   // or .SetSerializersProtobuf() / custom
    .Build();
```

### Inject and use

```csharp
public class MyService
{
    private readonly ITopicProducer<int, MyMessage> _producer;

    public MyService(ITopicProducer<int, MyMessage> producer)
        => _producer = producer;

    public async Task SendAsync(MyMessage msg)
    {
        await _producer.ProduceAsync(new Message<int, MyMessage>
        {
            Key   = msg.Id,
            Value = msg
        });
    }
}
```

### Transactions

```csharp
_producer.InitTransactions(TimeSpan.FromSeconds(10));
_producer.BeginTransaction();
try
{
    await _producer.ProduceAsync(msg1);
    await _producer.ProduceAsync(msg2);
    _producer.CommitTransaction(TimeSpan.FromSeconds(10));
}
catch
{
    _producer.AbortTransaction(TimeSpan.FromSeconds(10));
}
```

---

## Consumers

There are four consumer patterns:

| Pattern | Class | Interface | Use when |
|---|---|---|---|
| Event-based | `EventConsumerService<TKey,TValue>` | `ITopicConsumer<TKey,TValue>` | Synchronous processing, Rx observable |
| Async | `AsyncConsumerService<TKey,TValue>` | `IAsyncTopicConsumer<TKey,TValue>` | Async processing, multiple subscribers |
| Async loading | `AsyncLoadingConsumerService<TKey,TValue>` | `IAsyncLoadingConsumer<TKey,TValue>` | Read-all-on-startup then stream |
| Partition EOF | `ConsumerServicePartitionEOF<TKey,TValue>` | `ITopicConsumerEOF<TKey,TValue>` | Batch per partition, pause/resume |

### Event-based consumer

```csharp
// Register
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .Build();

// Use
public class MyHandler
{
    public MyHandler(ITopicConsumer<int, MyMessage> consumer)
    {
        consumer.Subscribe(result =>
        {
            Console.WriteLine($"Received: {result.Message.Value}");
        });
    }
}
```

### Async consumer

```csharp
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .BuildAsync();

// Use
public class MyHandler
{
    public MyHandler(IAsyncTopicConsumer<int, MyMessage> consumer)
    {
        consumer.Subscribe(async result =>
        {
            await ProcessAsync(result.Message.Value);
        });
    }
}
```

### Custom async consumer (subclass)

```csharp
public class MyConsumer : AsyncConsumerService<int, MyMessage>
{
    public MyConsumer(FilterableConsumerBuilderTopic<int, MyMessage> builder) : base(builder) { }

    protected override async ValueTask OnMessageAsync(object sender,
        ConsumeResult<int, MyMessage> e, CancellationToken cancellationToken)
    {
        await ProcessAsync(e.Message.Value, cancellationToken);
    }
}

// Register
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .BuildAsync<MyConsumer, int, MyMessage>();
```

### Async loading consumer (read all on startup)

Waits for partition EOF on all partitions before marking startup complete. Useful for cache-warming.

```csharp
public class MyCacheConsumer : AsyncLoadingConsumerService<int, MyMessage>
{
    public MyCacheConsumer(
        FilterableConsumerBuilderTopic<int, MyMessage> builder,
        IOptionsMonitor<LoadingConsumerConfiguration> options) : base(builder, options) { }

    protected override async ValueTask OnMessageAsync(object sender,
        ConsumeResult<int, MyMessage> e, CancellationToken cancellationToken)
    {
        _cache.Add(e.Message.Key, e.Message.Value);
    }
}

// Register — blocking: waits for full load before host starts
services.AddTopicConsumer<int, MyMessage>("MyCacheConsumer")
    .SetDeserializersMessagePack()
    .SetLoading(blocking: true)
    .BuildAsync<MyCacheConsumer, int, MyMessage>();
```

`appsettings.json` loading config (under `IOptions<LoadingConsumerConfiguration>`):

```json
{
  "consumers": {
    "MyCacheConsumer": {
      "topicName": "my-cache-topic",
      "config": {
        "group.id": "[Guid]",
        "auto.offset.reset": "earliest",
        "enable.partition.eof": true
      }
    }
  }
}
```

### Partition EOF consumer (batching)

Accumulates messages per partition, fires batch when EOF or batch size is reached.

```csharp
services.AddTopicConsumer<int, MyMessage>("MyEOFConsumer")
    .SetDeserializersMessagePack()
    .SetPauseOnEOF(configuration.GetSection("kafka:consumers:MyEOFConsumer"))
    .Build();

// Use
public class MyHandler
{
    public MyHandler(ITopicConsumerEOF<int, MyMessage> consumer)
    {
        consumer.OnPartitionEOFHandler += (sender, batch) =>
        {
            Console.WriteLine($"Batch of {batch.Count} from partition {batch[0].Partition}");
        };
    }
}
```

`appsettings.json` EOF config:

```json
{
  "consumers": {
    "MyEOFConsumer": {
      "topicName": "my-topic",
      "PartitionEOFPauseTime": "00:01:00",
      "PartitionBatchSize": 100,
      "config": {
        "group.id": "my-group",
        "enable.partition.eof": true,
        "enable.auto.offset.store": false
      }
    }
  }
}
```

### Header filter

```csharp
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .AddFilter(headers =>
    {
        var siteId = headers.GetHeaderValueInt("SiteId");
        return siteId.HasValue && allowedSiteIds.Contains(siteId.Value);
    })
    .Build();
```

---

## Serializers

### MessagePack (`EKG.Common.Kafka.MessagePack`)

Value types must have `[DataContract]` or `[MessagePackObject]` attribute.

```csharp
// Producer
services.AddTopicProducer<int, MyMessage>("MyProducer")
    .SetSerializersMessagePack()
    .Build();

// Consumer
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .Build();
```

### Protobuf (`EKG.Common.Kafka.Protobuf`)

Value types must implement `IMessage<T>` (generated from `.proto`).

```csharp
// Producer
services.AddTopicProducer<MyKey, MyProtoMessage>("MyProducer")
    .SetSerializersProtobuf()
    .Build();

// Consumer
services.AddTopicConsumer<MyKey, MyProtoMessage>("MyConsumer")
    .SetDeserializersProtobuf()
    .Build();
```

### JSON with Confluent Schema Registry

Use `Confluent.SchemaRegistry.Serdes.Json` for full Schema Registry integration. The schema is auto-registered on first produce and messages are visible in the Confluent Cloud viewer.

```csharp
// Configure Schema Registry client
var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
{
    Url = configuration["schemaRegistry:url"],
    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
    BasicAuthUserInfo = configuration["schemaRegistry:basicAuthUserInfo"]
});

// Producer — auto-registers schema on first message
services.AddTopicProducer<int, MyMessage>("MyProducer")
    .SetSerializersJson(schemaRegistry)
    .Build();

// Consumer — strips 5-byte SR wire header, deserializes JSON
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersJson()
    .Build();
```

See `EKG.Common.Kafka.Examples/SchemaRegistryJsonExtensions.cs` for the full implementation of `SetSerializersJson` / `SetDeserializersJson`.

`appsettings.json`:

```json
{
  "schemaRegistry": {
    "url": "https://psrc-xxxxx.region.confluent.cloud",
    "basicAuthUserInfo": "<sr-api-key>:<sr-api-secret>"
  }
}
```

> For message types: make `optional` string fields `string?` (nullable) so NJsonSchema marks them as nullable in the registered schema. Use property names exactly as NJsonSchema generates them (PascalCase by default) for consistent schema validation.

---

## Headers

### `EKG.Common.Kafka` — general header extensions

```csharp
// Read
int? siteId   = consumeResult.GetSiteId();
int? feedId   = consumeResult.GetFeedId();
int? empId    = consumeResult.GetEmployeeId();
long? hbNum   = message.Headers.GetHeaderValueLong("X-Heartbeat");
string? appNm = message.Headers.GetHeaderValueUtf8("X-AppName");

// Write
message.Headers
    .AddIntHeader("SiteId", 5)
    .AddLongHeader("X-Timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
    .AddUtf8StringHeader("X-AppName", "my-app")
    .AddArrayHeader("X-Tags", new[] { "a", "b" });

// Heartbeat
message.AddHeartbeatHeader(heartbeatNumber);
bool isHeartbeat = message.TryGetHeartbeatHeaderValue(out long number);
```

### `EKG.Common.Kafka.Headers.Monitoring`

```csharp
// Producer
message.AddMonitoringHeaders(new MonitoringHeadersInfo(
    appName:     "MyApp",
    machineName: Environment.MachineName));

// Consumer
if (message.TryGetMonitoringHeaderValue(out var info))
    Console.WriteLine($"From: {info.AppName} on {info.MachineName}");
```

### `EKG.Common.Kafka.Headers.Tracing`

Implements the W3C `traceparent` header format.

```csharp
// Producer
var activity = Activity.Current;
if (activity != null)
{
    message.AddTracingHeaders(new TracingHeadersInfo(
        activity.TraceId,
        activity.SpanId,
        (byte)(activity.ActivityTraceFlags)));
}

// Consumer
if (message.TryGetTracingHeaderValue(out var tracing))
{
    using var activity = new Activity("ProcessMessage");
    activity.SetParentId(tracing.TraceId, tracing.ParentSpanId,
        tracing.GetActivityTraceFlags());
    activity.Start();
}
```

---

## Consumer Lag Reporting (`EKG.Common.Kafka.Metrics.AppMetrics`)

Reports `KafkaConsumerLag` gauge per topic/partition to an `IMetricsRoot`.

```csharp
// Register IMetricsRoot first
services.AddSingleton<IMetricsRoot>(metrics);

// Wire up on any consumer
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .SetDeserializersMessagePack()
    .ReportAppMetrics()
    .Build();
```

Requires `statistics.interval.ms` set in the consumer config (e.g. `10000` = report every 10 seconds).

---

## Advanced — commit on done

Disables auto-commit; commits offset only after the message handler completes.

```csharp
public class MyConsumer : CommitOnDoneConsumerService<int, MyMessage>
{
    public MyConsumer(FilterableConsumerBuilderTopic<int, MyMessage> builder) : base(builder) { }
}

// appsettings.json must have enable.auto.commit = false
```

---

## Advanced — `AbstractLockingConsumerService`

Blocks `StartAsync` until all partitions have reached EOF (useful for warmup / initial load patterns).

```csharp
public class MyCacheLoader : AbstractLockingConsumerService<int, MyMessage, int>
{
    protected override int GetDistinctKey(MyMessage item) => item.Id;
}
```

## Advanced — `AbstractRevokingConsumerService`

Extends `AbstractLockingConsumerService`. Tracks messages per partition and raises a revocation event when partitions are rebalanced away, allowing in-flight batches to be drained.

---

## Delivery Logging

Helpers for structured logging of delivery outcomes (integrates with `Microsoft.Extensions.Logging`):

```csharp
// Async produce with result logging
var result = await _producer.ProduceAsync(msg);
logger.LogDeliveryResult(result);

// Fire-and-forget with delivery report callback
_producer.Produce(msg, report => logger.LogDeliveryReport(report));

// Catch produce exception
catch (ProduceException<int, MyMessage> ex)
{
    logger.LogProduceException(ex);
}
```

---

## RentedBytes serialization internals

Internally, all consumers use a two-stage deserialization pipeline to minimize allocations:

1. The Kafka consumer delivers raw bytes as `RentedBytes` (backed by `ArrayPool<byte>`)
2. An `IRentedBytesDeserializer<TValue>` converts `RentedBytes` → `TValue`

To implement a custom serialization format:

```csharp
public class MyDeserializer<T> : IRentedBytesDeserializer<T>
{
    public T Deserialize(RentedBytes rentedBytes, SerializationContext context)
    {
        if (rentedBytes.IsEmpty) return default;
        try   { return MyFormat.Deserialize<T>(rentedBytes.AsReadOnlySpan()); }
        finally { rentedBytes.Free(); }
    }
}

// Wire up
services.AddTopicConsumer<int, MyMessage>("MyConsumer")
    .Configure(b =>
    {
        b.SetValueDeserializer(new RentedBytesDeserializer(ArrayPool<byte>.Shared));
        b.SetInternalDeserializer(new MyDeserializer<MyMessage>());
    })
    .Build();
```

---

## Examples project

`EKG.Common.Kafka.Examples` is a runnable console app demonstrating all consumer patterns (event, async, async-loading, partition-EOF, filtered) with a producer, App.Metrics, and Confluent Schema Registry JSON serialization.

### Run

```bash
cd EKG.Common.Kafka.Examples
dotnet run
```

Configure `appsettings.json` (gitignored, create locally):

```json
{
  "schemaRegistry": {
    "url": "https://psrc-xxxxx.region.confluent.cloud",
    "basicAuthUserInfo": "<sr-key>:<sr-secret>"
  },
  "kafka": {
    "config": {
      "base": {
        "bootstrap.servers": "<cluster>.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "<cluster-api-key>",
        "sasl.password": "<cluster-api-secret>",
        "session.timeout.ms": "45000",
        "client.id": "my-client-id"
      },
      "consumer": {
        "enable.auto.commit": true,
        "auto.commit.interval.ms": 1000,
        "statistics.interval.ms": 10000
      }
    },
    "producers": {
      "MyDto_Producer": { "topicName": "MyDto_Topic" }
    },
    "consumers": {
      "MyDto_Consumer1": {
        "topicName": "MyDto_Topic",
        "config": { "auto.offset.reset": "latest", "group.id": "MyDto_Consumer1" }
      }
    }
  }
}
```

---

## CI / CD

GitHub Actions workflow (`.github/workflows/main.yml`) triggers on every push to `main`:

1. Restores packages (uses `NUGET_GITHUB_TOKEN` secret for the GitHub Packages feed)
2. Packs all 7 library projects with version `{major}.{minor}.{run_number}`
3. Pushes all `.nupkg` files to GitHub Packages

To set the required secret:

```bash
gh secret set NUGET_GITHUB_TOKEN --body "<github-pat>" --repo evgenkovalenko/EKG.Common.Kafka
```

---

## Repository structure

```
EKG.Common.Kafka/
├── EKG.Common.Kafka/                      Core library
│   ├── Builder/                           KafkaClientBuilder, FilterableConsumerBuilderTopic
│   │   ├── ConsumerBuilderExtensions.cs   AddTopicConsumer, Build, BuildAsync, SetLoading, SetPauseOnEOF
│   │   └── ProducerBuilderExtensions.cs   AddTopicProducer, Build
│   ├── Client/                            ITopicProducer, TopicProducer
│   ├── Configuration/                     KafkaConfig, KafkaGeneralConfig, TopicConfig
│   ├── Consumer/                          All consumer service types + interfaces
│   ├── Extensions/                        ServiceCollectionExtensions (AddKafka)
│   ├── Internal/                          NamingTools, ValueTaskTools
│   ├── Serialization/                     RentedBytes, IRentedBytesDeserializer
│   ├── HeartbeatHeadersExtensions.cs
│   ├── KafkaDeliveryLoggerExtensions.cs
│   ├── KafkaHeadersExtensions.cs
│   └── Subscription.cs
├── EKG.Common.Kafka.MessagePack/
├── EKG.Common.Kafka.Protobuf/
├── EKG.Common.Kafka.Headers.Abstractions/
├── EKG.Common.Kafka.Headers.Monitoring/
├── EKG.Common.Kafka.Headers.Tracing/
├── EKG.Common.Kafka.Metrics.AppMetrics/
└── EKG.Common.Kafka.Examples/
    ├── schemas/                           JSON Schema files for value objects
    └── SchemaRegistryJsonExtensions.cs    SetSerializersJson / SetDeserializersJson helpers
```
