using System.Text.Json;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var redisOptions = ConfigurationOptions.Parse(builder.Configuration["Redis:Configuration"] ?? "localhost:6379");
    redisOptions.AbortOnConnectFail = false;
    return ConnectionMultiplexer.Connect(redisOptions);
});

var app = builder.Build();
var staleAfter = TimeSpan.FromSeconds(builder.Configuration.GetValue("Fleet:StaleAfterSeconds", 120));

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/fleet/state", async (IConnectionMultiplexer redis) =>
{
    var db = redis.GetDatabase();
    var roverIds = await db.SetMembersAsync(RedisKeys.ActiveRovers);

    if (roverIds.Length == 0)
    {
        return Results.Ok(Array.Empty<RoverFleetState>());
    }

    var keys = roverIds
        .Where(id => id.HasValue)
        .Select(id => (RedisKey)$"{RedisKeys.LatestTelemetryPrefix}{id}:latest")
        .ToArray();

    var values = await db.StringGetAsync(keys);
    var states = values
        .Where(value => value.HasValue)
        .Select(value => JsonSerializer.Deserialize<RoverFleetState>(value!, RedisJson.Options))
        .OfType<RoverFleetState>()
        .Where(FleetStateValidation.IsCurrentShape)
        .Select(state => Freshness.Apply(state, staleAfter))
        .OrderBy(state => state.RoverId)
        .ToArray();

    return Results.Ok(states);
});

app.MapGet("/api/alerts/latest", async (IConnectionMultiplexer redis, int? take) =>
{
    var count = Math.Clamp(take ?? 20, 1, 100);
    var db = redis.GetDatabase();
    var values = await db.ListRangeAsync(RedisKeys.LatestAlerts, 0, count - 1);

    var alerts = values
        .Where(value => value.HasValue)
        .Select(value => JsonSerializer.Deserialize<FleetAlert>(value!, RedisJson.Options))
        .OfType<FleetAlert>()
        .OrderByDescending(alert => alert.EventTime)
        .ToArray();

    return Results.Ok(alerts);
});

app.Run();

static class RedisKeys
{
    public const string LatestTelemetryPrefix = "fleet:rover:";
    public const string ActiveRovers = "fleet:rovers:active";
    public const string LatestAlerts = "fleet:alerts:latest";
}

static class RedisJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}

static class FleetStateValidation
{
    public static bool IsCurrentShape(RoverFleetState state) =>
        !string.IsNullOrWhiteSpace(state.RoverId) &&
        !string.IsNullOrWhiteSpace(state.LastEventId) &&
        state.LastSeenUtc > DateTime.UnixEpoch &&
        state.Lat is >= -90 and <= 90 &&
        state.Lon is >= -180 and <= 180;
}

static class Freshness
{
    public static RoverFleetState Apply(RoverFleetState state, TimeSpan staleAfter)
    {
        var activeAlerts = state.ActiveAlerts ?? Array.Empty<string>();
        var lastSeen = state.LastSeenUtc.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(state.LastSeenUtc, DateTimeKind.Utc)
            : state.LastSeenUtc.ToUniversalTime();

        if (DateTime.UtcNow - lastSeen <= staleAfter || state.Status == "Dead")
        {
            return state with { LastSeenUtc = lastSeen, ActiveAlerts = activeAlerts };
        }

        var alerts = activeAlerts.Contains("STALE_ROVER")
            ? activeAlerts
            : activeAlerts.Concat(["STALE_ROVER"]).ToArray();

        return state with
        {
            LastSeenUtc = lastSeen,
            Status = "Stale",
            ActiveAlerts = alerts
        };
    }
}
