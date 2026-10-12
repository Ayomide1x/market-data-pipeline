# node:24-slim, not alpine: @confluentinc/kafka-javascript is a native librdkafka
# addon; glibc avoids the musl prebuilt-binary/build-tooling risk named in DECISIONS.md
# back in Stage 1. Built with --platform linux/arm64 explicitly for the Oracle Ampere
# target (Stage 5) — see DECISIONS.md for why that flag is explicit rather than relied
# on implicitly, and for the manifest inspection that verified it actually works.

FROM node:24-slim AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build

FROM node:24-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/dist ./dist

# Liveness, not "is Node running" (checking that from inside a container whose only
# process IS Node tells Docker nothing it doesn't already know — see DECISIONS.md).
# start-period gives the WS connection time to establish before the first real check.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["node", "dist/healthcheck.js"]

CMD ["node", "dist/index.js"]
