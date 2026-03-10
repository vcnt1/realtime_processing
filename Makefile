NETWORK_NAME := kafka_net
COMPOSE := docker compose
ARTIFACTS_MOUNT := $(CURDIR)/trabalho/data:/data

.PHONY: network build infra seed app up down ps logs logs-bootstrap clean reset

network:
	@docker network inspect $(NETWORK_NAME) >/dev/null 2>&1 || docker network create $(NETWORK_NAME)

build: network
	@$(COMPOSE) build bf-topic-init bf-seed-bootstrap bf-generator bf-dashboard bf-processor-spark

infra: network
	@$(COMPOSE) up -d zookeeper kafka kafka-2 kafka-3 minio

seed: infra
	@$(COMPOSE) up --build bf-topic-init bf-seed-bootstrap

app: seed
	@$(COMPOSE) up -d --build bf-generator bf-processor-spark bf-dashboard

up: build app

down:
	@$(COMPOSE) down

ps:
	@$(COMPOSE) ps

logs:
	@$(COMPOSE) logs -f --tail=100 bf-topic-init bf-seed-bootstrap bf-generator bf-processor-spark bf-dashboard

logs-bootstrap:
	@$(COMPOSE) logs --tail=100 bf-topic-init bf-seed-bootstrap

clean: down
	@docker run --rm -v "$(ARTIFACTS_MOUNT)" alpine:3.20 sh -c 'rm -rf /data/out'

reset: down
	@$(COMPOSE) down -v
	@docker run --rm -v "$(ARTIFACTS_MOUNT)" alpine:3.20 sh -c 'rm -rf /data/out'