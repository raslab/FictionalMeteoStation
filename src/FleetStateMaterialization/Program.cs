using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var redisOptions = ConfigurationOptions.Parse(builder.Configuration["Redis:Configuration"] ?? "localhost:6379");
    redisOptions.AbortOnConnectFail = false;
    return ConnectionMultiplexer.Connect(redisOptions);
});
builder.Services.AddHostedService<FleetStateMaterializer>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
