.PHONY: seed reproduce test integration clean

PYTHON ?= python3

seed:
	python scripts/bootstrap_seed_data.py

reproduce:
	$(PYTHON) -B scripts/reproduce_public_baseline.py

test:
	$(PYTHON) -B -m pytest -q

integration:
	$(PYTHON) -B -m pytest -q -m integration

clean:
	rm -f data/processed/*.csv
	rm -f results/tables/*.csv
	rm -f results/figures/*.png
