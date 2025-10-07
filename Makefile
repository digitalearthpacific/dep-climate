.ONESHELL:

.default: run

run:
	uv run src/run.py --country-code NIU

build:
	docker build -t climate .

docker:
	docker build -t climate .
	docker run -it --rm climate uv run run.py --country-code NIU

fmt:
	black src/

upgrade:
	uv sync -U --prerelease=allow

