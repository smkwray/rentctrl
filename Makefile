.PHONY: seed reproduce test integration clean

seed:
	python scripts/bootstrap_seed_data.py

reproduce:
	[local-path] -B scripts/reproduce_public_baseline.py

test:
	[local-path] -B -m pytest -q

integration:
	[local-path] -B -m pytest -q -m integration

clean:
	rm -f data/processed/*.csv
	rm -f results/tables/*.csv
	rm -f results/figures/*.png
