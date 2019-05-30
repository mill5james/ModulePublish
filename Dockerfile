FROM microsoft/dotnet:2.2-sdk AS build
ARG CONFIG=Release
WORKDIR /src

COPY [".", "."]
RUN dotnet publish --configuration ${CONFIG} --output /app

FROM microsoft/dotnet:2.2-runtime as final
WORKDIR /app
COPY --from=build ["/app", "."]