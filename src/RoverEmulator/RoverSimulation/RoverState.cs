public sealed class RoverState
{
    public string RoverId { get; init; } = default!;
    public double Lat { get; set; }
    public double Lon { get; set; }
    public double HeadingDegrees { get; set; }
    public double SpeedMetersPerSecond { get; set; }
    public double BatteryPercent { get; set; }
    public bool IsAlive => BatteryPercent > 0;
    public int Sequence { get; set; }
}
