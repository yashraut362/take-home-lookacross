PYTHON ?= python3

seed:
	$(PYTHON) seed.py

worker:
	$(PYTHON) worker.py

# Dev only: wipe our table and the provider's in-memory state before a fresh trial run.
reset:
	docker compose exec -T postgres psql -U dispatch -d dispatch -q -c "DROP TABLE IF EXISTS actions, provider_state;"
	docker compose restart provider

.PHONY: seed worker reset
