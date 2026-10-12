import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

// Run by Docker's HEALTHCHECK, compiled and invoked standalone — NOT part of the
// Spring Boot app.jar. See consumer/healthcheck/HealthCheckCli.java and DECISIONS.md
// for why (Spring Boot's repackaged-jar layout, and eclipse-temurin:25-jre shipping
// neither wget nor curl).
public final class HealthCheckCli {

  private HealthCheckCli() {}

  public static void main(String[] args) {
    // 8082: must match application.yml's own MANAGEMENT_PORT default exactly — neither
    // compose.prod.yaml nor this fallback ever has the other to cross-check against at
    // runtime, so a mismatch here would silently check the wrong port and report this
    // container unhealthy forever whenever MANAGEMENT_PORT isn't explicitly passed in.
    int port = Integer.parseInt(System.getenv().getOrDefault("MANAGEMENT_PORT", "8082"));
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + "/actuator/health"))
        .timeout(Duration.ofSeconds(3))
        .GET()
        .build();
    try {
      HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
      System.exit(response.statusCode() == 200 ? 0 : 1);
    } catch (Exception e) {
      System.err.println("healthcheck request failed: " + e);
      System.exit(1);
    }
  }
}
