.PHONY: reset verify-stage3 verify-stage4 verify-stage5 verify-stage6 verify-stage7 verify-stage8 verify-stage9 verify-ema \
        prod-up prod-local-up prod-down prod-ps prod-logs

# Destructive: wipes Kafka/Redis volumes and brings the stack back up fresh. See
# scripts/reset-dev.sh and DECISIONS.md for why this exists.
reset:
	./scripts/reset-dev.sh

PROD_COMPOSE := docker compose -f compose.yaml -f compose.prod.yaml
# PLACEHOLDER values satisfy compose.prod.yaml's mandatory SYMBOLS/CORS_ORIGIN
# interpolation, which fires on every subcommand (down, ps, logs — not just up), before
# Compose even knows which subcommand is running. A failed down/ps/logs because a real
# value wasn't exported left prod containers running and still writing to Redis, which
# then broke `make reset`'s own empty-Redis check — a real incident, not hypothetical.
# prod-up intentionally does NOT set these: it must still fail loudly if the caller
# hasn't exported real values, preserving the one guarantee that actually matters ("a
# real deploy must not start without a real CORS origin"). down/ps/logs don't start
# anything and never consume these values for real work, so a placeholder is harmless
# for them specifically. See DECISIONS.md.
PROD_PLACEHOLDER := SYMBOLS=placeholder CORS_ORIGIN=http://placeholder.invalid

# Requires SYMBOLS and CORS_ORIGIN exported with real values — fails with compose.prod.yaml's
# own ":?" message otherwise, by design.
prod-up:
	$(PROD_COMPOSE) up -d --build

# Same real-value requirement as prod-up — this brings up the identical prod config plus
# compose.prod.local.yaml's loopback-only port publish on the api, so "run prod locally
# first" is something you can point a browser at. Never applied on the real VM.
prod-local-up:
	docker compose -f compose.yaml -f compose.prod.yaml -f compose.prod.local.yaml up -d --build

# Placeholder values, deliberately: tears down by project name regardless of which file
# combination (prod-up or prod-local-up) brought the stack up.
prod-down:
	$(PROD_PLACEHOLDER) $(PROD_COMPOSE) down

prod-ps:
	$(PROD_PLACEHOLDER) $(PROD_COMPOSE) ps

prod-logs:
	$(PROD_PLACEHOLDER) $(PROD_COMPOSE) logs -f

verify-stage3:
	./scripts/verify-stage3.sh

verify-stage4:
	./scripts/verify-stage4.sh

verify-stage5:
	./scripts/verify-stage5.sh

verify-stage6:
	./scripts/verify-stage6.sh

verify-stage7:
	./scripts/verify-stage7.sh

verify-stage8:
	./scripts/verify-stage8.sh

verify-stage9:
	./scripts/verify-stage9.sh

verify-ema:
	./tools/verify-ema
