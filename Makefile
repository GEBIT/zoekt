build: build-zoekt build-gebit-indexserver

build-zoekt:
	@docker build . -t zoekt

build-gebit-indexserver:
	@docker build . -f Dockerfile.gebit-indexserver -t docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest

push:
	@docker push docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest
