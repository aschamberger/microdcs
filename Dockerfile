# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# MicroDCS container image: Native AOT publish into a chiseled runtime-deps image (non-root, no
# shell, no package manager). Images are pinned by digest; Dependabot (docker) proposes updates.
#
#   docker build -t microdcs .                    # final image (target runtime)
#   docker run --rm microdcs                       # runs `microdcs smoke` (S02, D-14)
#   docker build --target tests -t microdcs-tests .   # integration test runner for compose.test.yaml
#
# The AOT SDK image carries clang and zlib for the native link; its SDK matches global.json.

FROM mcr.microsoft.com/dotnet/sdk:11.0-resolute-aot@sha256:f3264ae0bf11e2062b9650e0822d20c999320ac1685fe3acf4b7c17d0881af85 AS restore
ENV CI=true \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
WORKDIR /src
# Restore inputs first so the package layer is cached until a project or lock file changes.
COPY --parents global.json nuget.config Directory.Build.props Directory.Packages.props MicroDCS.slnx \
     src/*/*.csproj src/*/packages.lock.json \
     tests/*/*.csproj tests/*/packages.lock.json \
     bench/*/*.csproj bench/*/packages.lock.json \
     tools/*/*.csproj tools/*/packages.lock.json \
     ./
RUN dotnet restore MicroDCS.slnx
COPY . .

FROM restore AS publish
ARG TARGETARCH
RUN rid="linux-$([ "${TARGETARCH:-amd64}" = arm64 ] && echo arm64 || echo x64)" \
 && scripts/aot-publish src/MicroDCS.App "$rid" /out --no-restore

# Integration test runner (D-15). Runs inside compose against the services there.
FROM restore AS tests
RUN dotnet build tests/MicroDCS.IntegrationTests -c Release --no-restore
ENTRYPOINT ["dotnet", "test", "--project", "tests/MicroDCS.IntegrationTests", "-c", "Release", "--no-build", \
            "--fail-skips", "on", "--results-directory", "/results", "--report-xunit-trx", \
            "--filter-not-trait", "Category=Image"]

# Final stage, last in the file so it is the default target.
FROM mcr.microsoft.com/dotnet/runtime-deps:11.0-resolute-chiseled@sha256:24534e1f18024d3f8026061537fa21ba424f46cc0cdab4297c5528937b1ae960 AS runtime
# Links the GHCR package to the repository (S22).
LABEL org.opencontainers.image.source=https://github.com/aschamberger/microdcs \
      org.opencontainers.image.licenses=MIT
WORKDIR /app
COPY --from=publish --chown=root:root --chmod=0555 /out/microdcs /app/microdcs
USER $APP_UID
ENTRYPOINT ["/app/microdcs"]
CMD ["smoke"]
