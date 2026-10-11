package com.marketdatapipeline.api;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/symbols")
public class SymbolController {

  private final SnapshotService snapshotService;
  private final List<String> configuredSymbols;

  public SymbolController(SnapshotService snapshotService, List<String> configuredSymbols) {
    this.snapshotService = snapshotService;
    this.configuredSymbols = configuredSymbols;
  }

  // One entry per configured symbol, not per Redis key — a symbol that hasn't traded yet
  // still gets a row (hasData: false), since that's an expected state, not an error. See
  // DECISIONS.md.
  @GetMapping
  public List<SymbolSnapshot> all() {
    return configuredSymbols.stream().map(snapshotService::snapshotFor).toList();
  }

  @GetMapping("/{symbol}")
  public SymbolSnapshot one(@PathVariable String symbol) {
    if (!configuredSymbols.contains(symbol)) {
      throw new UnknownSymbolException(symbol);
    }
    return snapshotService.snapshotFor(symbol);
  }
}
