.PHONY: seed test clean

seed:
	python scripts/bootstrap_seed_data.py

test:
	pytest -q

clean:
	rm -f data/processed/*.csv
	rm -f results/tables/*.csv
	rm -f results/figures/*.png
