# eclipse-temurin publishes official multi-arch manifests (Stage 5 DECISIONS.md) — built
# with --platform linux/arm64 explicitly for the Oracle Ampere target, verified against
# the actual image manifest after building, not just trusted from the flag.

FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw -q dependency:go-offline
COPY src ./src
RUN ./mvnw -q package -DskipTests

# eclipse-temurin:25-jre ships neither wget nor curl (verified directly: `which
# wget`/`which curl` both exit 1), and the runtime stage below has no javac at all (it's
# a -jre, not a -jdk) — compiled here, in the builder stage that already has javac,
# rather than add a package to the runtime image just for one HTTP check. See
# healthcheck/HealthCheckCli.java and DECISIONS.md.
COPY healthcheck/HealthCheckCli.java ./healthcheck/
RUN javac -d /healthcheck-classes healthcheck/HealthCheckCli.java

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app
COPY --from=builder /app/target/consumer-*.jar app.jar
COPY --from=builder /healthcheck-classes /healthcheck-classes

# Actuator's own /actuator/health, not a liveness-file-style check — see
# ProcessingHealthIndicator (reflects actual processing, not just "is the JVM alive")
# and DECISIONS.md for the stated limit of what it does and doesn't catch.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["java", "-cp", "/healthcheck-classes", "HealthCheckCli"]

ENTRYPOINT ["java", "-jar", "app.jar"]
