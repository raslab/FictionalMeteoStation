public sealed class AirQualityField
{
    public double Sample(double lat, double lon, double timeSeconds)
    {
        var x = lat * 40.0;
        var y = lon * 40.0;
        var t = timeSeconds * 0.02;

        var v00 = HashNoise(Math.Floor(x), Math.Floor(y), t);
        var v10 = HashNoise(Math.Floor(x) + 1, Math.Floor(y), t);
        var v01 = HashNoise(Math.Floor(x), Math.Floor(y) + 1, t);
        var v11 = HashNoise(Math.Floor(x) + 1, Math.Floor(y) + 1, t);

        var fx = SmoothStep(x - Math.Floor(x));
        var fy = SmoothStep(y - Math.Floor(y));

        var ix0 = Lerp(v00, v10, fx);
        var ix1 = Lerp(v01, v11, fx);
        var raw = Lerp(ix0, ix1, fy);

        return raw;
    }

    public int ToAqi(double raw)
        => (int)Math.Round(25 + raw * 175);

    private static double HashNoise(double x, double y, double t)
    {
        var n = Math.Sin(x * 127.1 + y * 311.7 + t * 17.3) * 43758.5453;
        return n - Math.Floor(n);
    }

    private static double SmoothStep(double value)
        => value * value * (3 - 2 * value);

    private static double Lerp(double a, double b, double amount)
        => a + (b - a) * amount;
}