.PHONY: run clean

run:
	@if [ -z "$(DATA_PATH)" ]; then \
		echo "Erreur : Spécifiez le chemin avec DATA_PATH=/chemin/vers/csv"; \
		exit 1; \
	fi
	DATA_PATH=$(DATA_PATH) docker compose up --build

clean:
	docker compose down -v