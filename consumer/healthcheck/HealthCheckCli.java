import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

// Run by Docker's HEALTHCHECK, compiled and invoked standalone — NOT part of the
// Spring Boot app.jar. A Spring Boot repackaged jar nests its classes under
// BOOT-INF/classes/ and its dependencies under BOOT-INF/lib/*.jar, which only Spring
// Boot's own JarLauncher knows how to read; a plain `java -cp app.jar <Class>` can't
// reach a class inside that structure at all. Rather than fight the fat-jar layout for
// a single dependency-free HTTP check, this lives outside src/main/java entirely,
// compiled with a bare `javac` in the Dockerfile and run with a plain `-cp`. It also
// sidesteps the earlier wget/curl problem: eclipse-temurin:25-jre ships neither
// (verified directly), but the JRE's own java.net.http.HttpClient is always there.
// See DECISIONS.md.
public final class HealthCheckCli {

  private HealthCheckCli() {}

  public static void main(String[] args) {
    int port = Integer.parseInt(System.getenv().getOrDefault("MANAGEMENT_PORT", "8081"));
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
