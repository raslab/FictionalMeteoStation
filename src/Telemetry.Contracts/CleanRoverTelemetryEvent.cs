public sealed record CleanRoverTelemetryEvent(
    string EventId,
    string RoverId,
    DateTime EventTime,
    double Latitude,
    double Longitude,
    double HeadingDegrees,
    double SpeedMetersPerSecond,
    int AirQualityIndex,
    double AirQualityRaw,
    double BatteryPercent,
    bool IsAlive,
    string EventType,
    int Sequence);
