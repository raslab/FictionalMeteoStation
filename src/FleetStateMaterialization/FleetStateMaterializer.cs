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
        var consumersPerTopic = Math.Max(1, _configuration.GetValue("Kafka:ConsumersPerTopic", 24));

        var telemetryTasks = Enumerable.Range(1, consumersPerTopic)
            .Select(consumerInstance => ConsumeTopicAsync<CleanRoverTelemetryEvent>(
                _configuration["Kafka:TelemetryTopic"] ?? "rover.telemetry.clean",
                "telemetry",
                consumerInstance,
                consumersPerTopic,
                MaterializeTelemetryAsync,
                stoppingToken));

        var alertTasks = Enumerable.Range(1, consumersPerTopic)
            .Select(consumerInstance => ConsumeTopicAsync<FleetAlert>(
                _configuration["Kafka:AlertsTopic"] ?? "rover.alerts",
                "alerts",
                consumerInstance,
                consumersPerTopic,
                MaterializeAlertAsync,
                stoppingToken));

        var tasks = telemetryTasks.Concat(alertTasks);

        return Task.WhenAll(tasks);
    }

    private async Task ConsumeTopicAsync<TEvent>(
        string topic,
        string consumerName,
        int consumerInstance,
        int consumerCount,
        Func<TEvent, IDatabase, Task> materialize,
        CancellationToken stoppingToken)
    {
        var clientId = _configuration["Kafka:ClientId"] ?? "fleet-state-materialization";
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = _configuration["Kafka:GroupId"] ?? "fleet-state-materialization",
            ClientId = $"{clientId}-{consumerName}-{consumerInstance}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Assigned {PartitionCount} partitions to {ConsumerName} consumer {ConsumerInstance}/{ConsumerCount}: {Partitions}",
                    partitions.Count,
                    consumerName,
                    consumerInstance,
                    consumerCount,
                    string.Join(", ", partitions.Select(partition => partition.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Revoked {PartitionCount} partitions from {ConsumerName} consumer {ConsumerInstance}/{ConsumerCount}.",
                    partitions.Count,
                    consumerName,
                    consumerInstance,
                    consumerCount);
            })
            .Build();
        var db = _redis.GetDatabase();

        consumer.Subscribe(topic);
        _logger.LogInformation(
            "Consuming {ConsumerName} from Kafka topic {Topic} with consumer {ConsumerInstance}/{ConsumerCount}.",
            consumerName,
            topic,
            consumerInstance,
            consumerCount);

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
        var stateKey = $"{LatestTelemetryPrefix}{evt.RoverId}:latest";
        var eventTimeUtc = ToUtc(evt.EventTime);

        while (true)
        {
            var currentValue = await db.StringGetAsync(stateKey);
            if (currentValue.HasValue && TryGetLastSeenUtc(currentValue, out var lastSeenUtc) && eventTimeUtc <= lastSeenUtc)
            {
                _logger.LogDebug(
                    "Skipping out-of-order telemetry event {EventId} for rover {RoverId}. EventTime={EventTime:o}, LastSeen={LastSeen:o}.",
                    evt.EventId,
                    evt.RoverId,
                    eventTimeUtc,
                    lastSeenUtc);
                return;
            }

            var state = new RoverFleetState(
                evt.RoverId,
                evt.Latitude,
                evt.Longitude,
                evt.HeadingDegrees,
                evt.SpeedMetersPerSecond,
                evt.BatteryPercent,
                evt.AirQualityIndex,
                eventTimeUtc,
                GetStatus(evt),
                GetActiveAlerts(evt),
                evt.Sequence,
                evt.EventId);

            var json = JsonSerializer.Serialize(state, JsonOptions);
            var transaction = db.CreateTransaction();

            if (currentValue.HasValue)
            {
                transaction.AddCondition(Condition.StringEqual(stateKey, currentValue));
            }
            else
            {
                transaction.AddCondition(Condition.KeyNotExists(stateKey));
            }

            _ = transaction.StringSetAsync(stateKey, json, _redisKeyTtl);
            _ = transaction.SetAddAsync(ActiveRoversKey, evt.RoverId);
            _ = transaction.KeyExpireAsync(ActiveRoversKey, _redisKeyTtl);

            if (await transaction.ExecuteAsync())
            {
                return;
            }
        }
    }

    private static bool TryGetLastSeenUtc(RedisValue value, out DateTime lastSeenUtc)
    {
        try
        {
            var currentState = JsonSerializer.Deserialize<RoverFleetState>(value!, JsonOptions);
            if (currentState is not null && currentState.LastSeenUtc > DateTime.UnixEpoch)
            {
                lastSeenUtc = ToUtc(currentState.LastSeenUtc);
                return true;
            }
        }
        catch (JsonException)
        {
        }

        lastSeenUtc = default;
        return false;
    }

    private static DateTime ToUtc(DateTime value) =>
        value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value.ToUniversalTime();

    private static string GetStatus(CleanRoverTelemetryEvent evt)
    {
        if (!evt.IsAlive || evt.BatteryPercent <= 0 || string.Equals(evt.EventType, "rover_died", StringComparison.OrdinalIgnoreCase))
        {
            return "Dead";
        }

        if (evt.BatteryPercent <= 20 || evt.AirQualityIndex >= 100)
        {
            return "Warning";
        }

        return "Healthy";
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

    private static IReadOnlyList<string> GetActiveAlerts(CleanRoverTelemetryEvent evt)
    {
        var alerts = new List<string>();

        if (!evt.IsAlive || evt.BatteryPercent <= 0 || string.Equals(evt.EventType, "rover_died", StringComparison.OrdinalIgnoreCase))
        {
            alerts.Add("DEAD_ROVER");
        }

        if (evt.BatteryPercent <= 20)
        {
            alerts.Add("LOW_BATTERY");
        }

        if (evt.AirQualityIndex >= 100)
        {
            alerts.Add("HIGH_AIR_POLLUTION");
        }

        return alerts;
    }
}
