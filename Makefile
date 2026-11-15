IMG ?= ghcr.io/xxl-job/xxl-job-operator:latest
SIDECAR_IMG ?= ghcr.io/xxl-job/xxl-job-operator-sidecar:latest

.PHONY: all
all: build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: fmt vet
	go test -v -race ./...

.PHONY: build
build: fmt vet
	go build -o bin/manager ./cmd/main.go
	go build -o bin/sidecar ./cmd/sidecar/main.go

.PHONY: run
run: fmt vet
	go run ./cmd/main.go

.PHONY: install
install:
	kubectl apply -f config/crd/bases/

.PHONY: uninstall
uninstall:
	kubectl delete -f config/crd/bases/ --ignore-not-found=true

.PHONY: deploy
deploy: install
	kubectl apply -f config/manager/manager.yaml

.PHONY: deploy-webhook
deploy-webhook:
	kubectl apply -f config/webhook/manifests.yaml

.PHONY: undeploy
undeploy:
	kubectl delete -f config/webhook/manifests.yaml --ignore-not-found=true
	kubectl delete -f config/manager/manager.yaml --ignore-not-found=true

.PHONY: docker-build
docker-build:
	docker build --target manager -t ${IMG} .
	docker build --target sidecar -t ${SIDECAR_IMG} .
