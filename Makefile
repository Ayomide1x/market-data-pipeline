.PHONY: reset verify-stage3 verify-ema

# Destructive: wipes Kafka/Redis volumes and brings the stack back up fresh. See
# scripts/reset-dev.sh and DECISIONS.md for why this exists.
reset:
	./scripts/reset-dev.sh

verify-stage3:
	./scripts/verify-stage3.sh

verify-ema:
	./tools/verify-ema
