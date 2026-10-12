.PHONY: reset verify-stage3 verify-stage4 verify-stage5 verify-stage6 verify-stage7 verify-stage8 verify-stage9 verify-ema

# Destructive: wipes Kafka/Redis volumes and brings the stack back up fresh. See
# scripts/reset-dev.sh and DECISIONS.md for why this exists.
reset:
	./scripts/reset-dev.sh

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
