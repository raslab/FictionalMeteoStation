using System.Text.Json;
using Confluent.Kafka;

public sealed class RoverEmitterWorker : BackgroundService
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<RoverEmitterWorker> _logger;
    private readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);
    private readonly IReadOnlyList<RoverState> _rovers;
    private readonly RoverMotionModel _motion = new();
    private readonly AirQualityField _air = new();
    private readonly string _topic;
    private readonly TimeSpan _roversReportingLoopTimeSec;

    public RoverEmitterWorker(
        IProducer<string, string> producer,
        IConfiguration configuration,
        ILogger<RoverEmitterWorker> logger)
    {
        _producer = producer;
        _logger = logger;
        _topic = configuration["Kafka:Topic"] ?? "rover.telemetry.raw";
        _roversReportingLoopTimeSec = TimeSpan.FromSeconds(Math.Max(0.1d, configuration.GetValue("Simulation:RoversReportingLoopTimeSec", 1.0d)));

        var originLat = configuration.GetValue("Simulation:OriginLat", 52.2297);
        var originLon = configuration.GetValue("Simulation:OriginLon", 21.0122);
        var roverCount = Math.Max(1, configuration.GetValue("Simulation:InitialRoverCount", 8));

        _rovers = Enumerable.Range(1, roverCount)
            .Select(index => new RoverState
            {
                RoverId = $"rover-{index}",
                Lat = originLat + (Random.Shared.NextDouble() - 0.5) * 0.02,
                Lon = originLon + (Random.Shared.NextDouble() - 0.5) * 0.02,
                HeadingDegrees = Random.Shared.NextDouble() * 360,
                SpeedMetersPerSecond = 1.0 + Random.Shared.NextDouble() * 3.0,
                BatteryPercent = 75 + Random.Shared.NextDouble() * 25
            })
            .ToArray();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting rover telemetry emitter with {RoverCount} rovers.", _rovers.Count);

        while (!stoppingToken.IsCancellationRequested)
        {
            var tasks = _rovers.Select(rover=> {
                _motion.Advance(rover, _roversReportingLoopTimeSec);

                var rawAir = _air.Sample(rover.Lat, rover.Lon, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                var evt = new RoverTelemetryEvent(
                    Guid.NewGuid(),
                    rover.RoverId,
                    "meteo-station-a",
                    DateTime.UtcNow,
                    rover.Lat,
                    rover.Lon,
                    rover.HeadingDegrees,
                    rover.SpeedMetersPerSecond,
                    rover.BatteryPercent,
                    _air.ToAqi(rawAir),
                    rawAir,
                    rover.IsAlive,
                    rover.IsAlive ? "telemetry" : "rover_died",
                    rover.Sequence,
                    $"trace-{rover.RoverId}-{rover.Sequence}");

                var payload = JsonSerializer.Serialize(evt, _json);
                return _producer.ProduceAsync(_topic, new Message<string, string>
                {
                    Key = evt.RoverId,
                    Value = payload
                }, stoppingToken);
            });
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error while emitirng rovers batch.");
            }


            await Task.Delay(_roversReportingLoopTimeSec, stoppingToken);
        }
    }
}
