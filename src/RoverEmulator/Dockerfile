FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY Telemetry.Contracts/Telemetry.Contracts.csproj Telemetry.Contracts/
COPY RoverEmulator/RoverEmulator.csproj RoverEmulator/
RUN dotnet restore RoverEmulator/RoverEmulator.csproj

COPY Telemetry.Contracts/ Telemetry.Contracts/
COPY RoverEmulator/ RoverEmulator/
RUN dotnet publish RoverEmulator/RoverEmulator.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "RoverEmulator.dll"]
