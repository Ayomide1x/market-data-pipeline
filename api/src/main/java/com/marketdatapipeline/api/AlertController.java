package com.marketdatapipeline.api;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/alerts")
public class AlertController {

  private final AlertService alertService;

  public AlertController(AlertService alertService) {
    this.alertService = alertService;
  }

  // getRemoteAddr() here, not a manual X-Forwarded-For parse: with
  // server.forward-headers-strategy=native, Tomcat's own RemoteIpValve already does the
  // "only trust the header if the direct peer is an internal/trusted address" check and
  // substitutes it into getRemoteAddr() itself — see DECISIONS.md for why hand-rolling
  // this in application code was rejected.
  @PostMapping
  public ResponseEntity<AlertView> create(
      @RequestBody CreateAlertRequest request, HttpServletRequest httpRequest) {
    AlertView created =
        alertService.create(
            request.symbol(), request.direction(), request.threshold(), httpRequest.getRemoteAddr());
    return ResponseEntity.status(HttpStatus.CREATED).body(created);
  }

  @GetMapping
  public List<AlertView> list(@RequestParam String symbol) {
    return alertService.list(symbol);
  }

  @DeleteMapping("/{id}")
  public ResponseEntity<Void> delete(@PathVariable String id) {
    alertService.delete(id);
    return ResponseEntity.noContent().build();
  }
}
