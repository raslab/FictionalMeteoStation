FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY Telemetry.Contracts/Telemetry.Contracts.csproj Telemetry.Contracts/
COPY FleetStateMaterialization/FleetStateMaterialization.csproj FleetStateMaterialization/
RUN dotnet restore FleetStateMaterialization/FleetStateMaterialization.csproj

COPY Telemetry.Contracts/ Telemetry.Contracts/
COPY FleetStateMaterialization/ FleetStateMaterialization/
RUN dotnet publish FleetStateMaterialization/FleetStateMaterialization.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "FleetStateMaterialization.dll"]
