.ONESHELL:

.default: run

run:
	uv run src/run.py --country-code NIU

build:
	docker build -t climate .

docker:
	docker build -t climate .
	docker run -it --rm climate uv run run.py --tile-id 50,41 --year 2024 --version 0.0.1

fmt:
	black src/

upgrade:
	uv sync -U --prerelease=allow

