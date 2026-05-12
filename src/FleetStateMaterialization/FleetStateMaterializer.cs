using System.Text.Json;
using Confluent.Kafka;
using StackExchange.Redis;

public sealed class FleetStateMaterializer : BackgroundService
{
    private const int DefaultRedisKeyTtlSeconds = 300;
    private const string LatestTelemetryPrefix = "fleet:rover:";
    private const string ActiveRoversKey = "fleet:rovers:active";
    private const string LatestAlertsKey = "fleet:alerts:latest";
    private const string AlertPrefix = "fleet:alert:";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IConnectionMultiplexer _redis;
    private readonly IConfiguration _configuration;
    private readonly ILogger<FleetStateMaterializer> _logger;
    private readonly TimeSpan _redisKeyTtl;

    public FleetStateMaterializer(
        IConnectionMultiplexer redis,
        IConfiguration configuration,
        ILogger<FleetStateMaterializer> logger)
    {
        _redis = redis;
        _configuration = configuration;
        _logger = logger;
        _redisKeyTtl = TimeSpan.FromSeconds(
            configuration.GetValue("Redis:KeyTtlSeconds", DefaultRedisKeyTtlSeconds));
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = new[]
        {
            ConsumeTopicAsync<CleanRoverTelemetryEvent>(
                _configuration["Kafka:TelemetryTopic"] ?? "rover.telemetry.clean",
                "telemetry",
                MaterializeTelemetryAsync,
                stoppingToken),
            ConsumeTopicAsync<FleetAlert>(
                _configuration["Kafka:AlertsTopic"] ?? "rover.alerts",
                "alerts",
                MaterializeAlertAsync,
                stoppingToken)
        };

        return Task.WhenAll(tasks);
    }

    private async Task ConsumeTopicAsync<TEvent>(
        string topic,
        string consumerName,
        Func<TEvent, IDatabase, Task> materialize,
        CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = _configuration["Kafka:GroupId"] ?? "fleet-state-materialization",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        var db = _redis.GetDatabase();

        consumer.Subscribe(topic);
        _logger.LogInformation("Consuming {ConsumerName} from Kafka topic {Topic}.", consumerName, topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    if (string.IsNullOrWhiteSpace(result.Message.Value))
                    {
                        continue;
                    }

                    var evt = JsonSerializer.Deserialize<TEvent>(result.Message.Value, JsonOptions);
                    if (evt is null)
                    {
                        throw new JsonException($"Could not deserialize {typeof(TEvent).Name}.");
                    }

                    await materialize(evt, db);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "Kafka consume error on topic {Topic}.", topic);
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Skipping invalid JSON message from topic {Topic}.", topic);
                }
                catch (RedisException ex)
                {
                    _logger.LogWarning(ex, "Redis write failed while handling topic {Topic}.", topic);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task MaterializeTelemetryAsync(CleanRoverTelemetryEvent evt, IDatabase db)
    {
        var json = JsonSerializer.Serialize(evt, JsonOptions);

        await Task.WhenAll(
            db.StringSetAsync($"{LatestTelemetryPrefix}{evt.RoverId}:latest", json, _redisKeyTtl),
            db.SetAddAsync(ActiveRoversKey, evt.RoverId),
            db.KeyExpireAsync(ActiveRoversKey, _redisKeyTtl));
    }

    private async Task MaterializeAlertAsync(FleetAlert alert, IDatabase db)
    {
        var json = JsonSerializer.Serialize(alert, JsonOptions);
        var batch = db.CreateBatch();

        var pushAlert = batch.ListLeftPushAsync(LatestAlertsKey, json);
        var trimAlerts = batch.ListTrimAsync(LatestAlertsKey, 0, 99);
        var expireAlerts = batch.KeyExpireAsync(LatestAlertsKey, _redisKeyTtl);
        var setAlert = batch.StringSetAsync($"{AlertPrefix}{alert.AlertId}", json, _redisKeyTtl);
        batch.Execute();

        await Task.WhenAll(pushAlert, trimAlerts, expireAlerts, setAlert);
    }
}
