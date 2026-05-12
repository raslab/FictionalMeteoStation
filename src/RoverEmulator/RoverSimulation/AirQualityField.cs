public sealed class AirQualityField
{
    public double Sample(double lat, double lon, double timeSeconds)
    {
        var x = (lat - 52.2297) * 28.0;
        var y = (lon - 21.0122) * 28.0;
        var t = timeSeconds * 0.0008;

        var broadGradient = 0.52 + 0.24 * Math.Sin(x * 0.8 + y * 0.35 + t);
        var crossBreeze = 0.16 * Math.Sin(x * -0.25 + y * 0.9 - t * 0.7);
        var localPocket = 0.08 * Math.Cos(x * 1.2 - y * 0.55 + t * 0.4);

        return Math.Clamp(broadGradient + crossBreeze + localPocket, 0.0, 1.0);
    }

    public int ToAqi(double raw)
        => (int)Math.Round(70 + raw * 50);
}
