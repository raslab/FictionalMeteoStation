public sealed class RoverMotionModel
{
    private readonly Random _random = new(42);

    public void Advance(RoverState rover, TimeSpan delta)
    {
        if (!rover.IsAlive) return;

        rover.HeadingDegrees = Normalize(rover.HeadingDegrees + _random.NextDouble() * 20 - 10);
        rover.SpeedMetersPerSecond = Math.Clamp(
            rover.SpeedMetersPerSecond + (_random.NextDouble() * 0.8 - 0.4),
            0.5,
            7.0);

        var distanceMeters = rover.SpeedMetersPerSecond * delta.TotalSeconds;
        var headingRadians = Math.PI * rover.HeadingDegrees / 180.0;
        rover.Lat += (distanceMeters * Math.Cos(headingRadians)) / 111_320d;
        rover.Lon += (distanceMeters * Math.Sin(headingRadians)) /
                     (111_320d * Math.Cos(rover.Lat * Math.PI / 180.0));

        rover.BatteryPercent = Math.Max(0, rover.BatteryPercent - delta.TotalSeconds * 0.03);
        rover.Sequence++;
    }

    private static double Normalize(double degrees)
        => (degrees % 360 + 360) % 360;
}
