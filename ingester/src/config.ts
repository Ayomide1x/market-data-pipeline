export interface Config {
  symbols: string[];
  kafkaBootstrapServers: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const symbolsRaw = env.SYMBOLS;
  const symbols = (symbolsRaw ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (symbols.length === 0) {
    throw new Error("SYMBOLS environment variable is required (comma-separated, e.g. BTC-USD,ETH-USD)");
  }

  const kafkaBootstrapServers = env.KAFKA_BOOTSTRAP_SERVERS;
  if (!kafkaBootstrapServers) {
    throw new Error("KAFKA_BOOTSTRAP_SERVERS environment variable is required");
  }

  return { symbols, kafkaBootstrapServers };
}
