.PHONY: build proto proto-lint test test-coverage coverage-html vet check hooks xray-smoke

VERSION ?= dev

build: ## Собрать исполняемый node-agent с указанной VERSION.
	mkdir -p bin
	go build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/node-agent ./cmd/node-agent

proto: ## Сгенерировать Go-код из локальной копии protobuf-контракта.
	go tool buf generate

proto-lint: ## Проверить protobuf-файлы с помощью buf lint.
	go tool buf lint

test: ## Запустить все тесты с детектором гонок.
	go test -race ./...

test-coverage: ## Запустить тесты с детектором гонок и собрать профиль покрытия.
	scripts/coverage.sh coverage.out

coverage-html: test-coverage ## Сформировать HTML-отчёт о покрытии.
	go tool cover -html=coverage.out -o coverage.html

vet: ## Запустить стандартный статический анализатор Go.
	go vet ./...

check: vet proto-lint test ## Выполнить все обязательные локальные проверки.

hooks: ## Установить git hooks через pre-commit.
	pre-commit install

xray-smoke: ## Проверить адаптер на реальном Xray 26.3.27 через Docker.
	go test -tags=xray_smoke -run TestXrayAPISmoke -count=1 ./internal/xray
