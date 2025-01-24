run:
	uv run python3 main.py
test:
	uv run coverage run -m pytest
lint:
	uv run ruff check
format:
	uv run ruff format
mypy:
	uv run mypy --strict .
cov-report:
	uv run coverage report
cov-report-html:
	uv run coverage html
cov-report-html-open:
	uv run open tests/coverage-report/index.html

