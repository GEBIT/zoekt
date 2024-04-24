build: build-zoekt build-gebit-indexserver

build-zoekt:
	@docker build . -t docker-registry.local.gebit.de:5000/gebit-build/zoekt:latest

build-gebit-indexserver:
	@docker build . -f Dockerfile.gebit-indexserver -t docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest

push:
	@docker push docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest
