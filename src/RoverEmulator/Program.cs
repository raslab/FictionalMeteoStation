using System.Text.Json;

var roverId = Environment.GetEnvironmentVariable("ROVER_ID") ?? "rover-1";

while (true)
{
    var telemetry = new
    {
        eventId = Guid.NewGuid().ToString("N"),
        roverId,
        eventTime = DateTimeOffset.UtcNow,
        latitude = 52.5200 + Random.Shared.NextDouble() * 0.001,
        longitude = 13.4050 + Random.Shared.NextDouble() * 0.001,
        pm25 = 10 + Random.Shared.NextDouble() * 40,
        batteryPercent = 60 + Random.Shared.NextDouble() * 40
    };

    Console.WriteLine(JsonSerializer.Serialize(telemetry));
    await Task.Delay(1000);
}
