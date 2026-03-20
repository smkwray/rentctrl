.PHONY: seed reproduce test integration clean

seed:
	python scripts/bootstrap_seed_data.py

reproduce:
	/Users/shanewray/venvs/rentctrl/bin/python -B scripts/reproduce_public_baseline.py

test:
	/Users/shanewray/venvs/rentctrl/bin/python -B -m pytest -q

integration:
	/Users/shanewray/venvs/rentctrl/bin/python -B -m pytest -q -m integration

clean:
	rm -f data/processed/*.csv
	rm -f results/tables/*.csv
	rm -f results/figures/*.png
