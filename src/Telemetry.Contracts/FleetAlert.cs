public sealed record FleetAlert(
    string AlertId,
    string RoverId,
    string AlertType,
    string Severity,
    string Message,
    DateTime DetectedAtUtc,
    string SourceEventId);