VENV = venv
PYTHON = $(VENV)/bin/python
PYTEST = $(VENV)/bin/pytest

run: 
	PYTHONPATH=. $(PYTHON) src/main.py

test:
	PYTHONPATH=. $(PYTEST) ./tests --maxfail=1 --disable-warnings -q