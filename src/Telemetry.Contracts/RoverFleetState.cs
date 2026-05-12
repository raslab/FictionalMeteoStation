public sealed record RoverFleetState(
    string RoverId,
    double Lat,
    double Lon,
    double HeadingDegrees,
    double SpeedMetersPerSecond,
    double BatteryPercent,
    int AirQualityIndex,
    DateTime LastSeenUtc,
    string Status,
    IReadOnlyList<string> ActiveAlerts,
    int ProcessedSequence,
    string LastEventId);
