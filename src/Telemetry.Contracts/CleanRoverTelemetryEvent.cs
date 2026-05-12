public sealed record CleanRoverTelemetryEvent(
    string EventId,
    string RoverId,
    DateTime EventTime,
    double Latitude,
    double Longitude,
    int AirQualityIndex,
    double AirQualityRaw,
    double BatteryPercent);
