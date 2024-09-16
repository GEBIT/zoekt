build: build-zoekt build-gebit-indexserver build-gebit-webserver

push: push-gebit-indexserver push-gebit-webserver

build-zoekt:
	@docker build . -t zoekt

build-gebit-indexserver:
	@docker build . -f Dockerfile.gebit-indexserver -t docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest

build-gebit-webserver:
	@docker build . -f Dockerfile.gebit-webserver -t docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-webserver:latest

push-gebit-indexserver:
	@docker push docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-indexserver:latest

push-gebit-webserver:
	@docker push docker-registry.local.gebit.de:5000/gebit-build/zoekt-gebit-webserver:latest
