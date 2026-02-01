VENV = venv
PYTHON = $(VENV)/bin/python
PYTEST = $(VENV)/bin/pytest

test:
	PYTHONPATH=. $(PYTEST) ./tests --maxfail=1 --disable-warnings -q