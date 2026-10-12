package com.marketdatapipeline.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

// Anyone who finds the bare api domain (a shared link, a browser history entry) should
// land on the actual demo, not Spring's default Whitelabel error page. Redirects to the
// same origin CORS already trusts, so there's no new config surface for this.
@RestController
public class RootController {

  private final String corsOrigin;

  public RootController(@Value("${app.cors.origin}") String corsOrigin) {
    this.corsOrigin = corsOrigin;
  }

  @GetMapping("/")
  public RedirectView root() {
    return new RedirectView(corsOrigin);
  }
}
