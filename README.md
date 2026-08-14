# NodeAgent
Агент на Go, обслуживающий xray на серверах VPN-флота

## Конфигурация

Процесс читает конфигурацию только из переменных окружения. Infrastructure
заранее создаёт каталог состояния с правами `0700`, TLS-файлы и конфигурацию
Xray; агент не создаёт эти ресурсы автоматически.

| переменная | обязательна | значение по умолчанию |
|---|---:|---|
| `SPIRIT_NODE_ID` | да | — |
| `SPIRIT_GRPC_LISTEN` | да | —; конкретный management IP и порт |
| `SPIRIT_HTTP_LISTEN` | нет | `127.0.0.1:9090`; только loopback |
| `SPIRIT_GRPC_TLS_CERT_FILE` | да | — |
| `SPIRIT_GRPC_TLS_KEY_FILE` | да | — |
| `SPIRIT_GRPC_TLS_CLIENT_CA_FILE` | да | — |
| `SPIRIT_GRPC_ALLOWED_CLIENT_IDENTITIES` | да | —; DNS/URI SAN через запятую |
| `SPIRIT_XRAY_INBOUND_TAG` | да | — |
| `SPIRIT_XRAY_LOCAL_OUTBOUND_TAG` | да | — |
| `SPIRIT_XRAY_API_ADDRESS` | нет | `127.0.0.1:10085` |
| `SPIRIT_XRAY_FALLBACK_OUTBOUND_TAG` | нет | `block` |
| `SPIRIT_STATE_DB_PATH` | нет | `/var/lib/spirit-agent/state.db` |
| `SPIRIT_MAX_INVENTORY_USERS` | нет | `2000` |
| `SPIRIT_SHUTDOWN_TIMEOUT` | нет | `10s` |
| `SPIRIT_LOG_LEVEL` | нет | `info` |

Служебный HTTP-сервер отдаёт liveness по `/health/live`, readiness по
`/health/ready` и метрики Prometheus по `/metrics`. Пути `/healthz` и `/readyz`
доступны как короткие алиасы. Endpoint метрик раскрывает состояние ноды и поэтому
намеренно доступен только через loopback listener.

Сборка:

```bash
make build VERSION=dev
```

## Деплой

Node-agent поставляется как один публичный OCI-образ. Один и тот же образ
используется в `develop` и `prod`: окружение, адреса, TLS-материалы и параметры
Xray передаются только через инфраструктурную конфигурацию и не встраиваются в
образ.

Целевой процесс поставки:

1. Коммит в `develop` проходит CI и публикуется с неизменяемым тегом
   `develop-<git-sha>`.
2. Infrastructure-репозиторий закрепляет образ по `sha256` digest и разворачивает
   его в `develop`.
3. После проверки release из `main` добавляет semver-тег к уже проверенному
   digest, не пересобирая образ.
4. Infrastructure-репозиторий переводит `prod` на тот же digest и управляет
   стратегией rollout и rollback.

Движущиеся теги можно использовать для навигации по registry, но не для деплоя.
Публичный образ не требует registry credentials на VPN-нодах. Секреты, включая
TLS-ключи, в образ не входят.

Инфраструктура при запуске агента должна:

- создать каталог состояния с правами `0700` и сохранить его между обновлениями;
- передать TLS-файлы и конфигурацию через read-only mounts или эквивалентный
  механизм;
- предоставить доступ к локальному Xray API на `127.0.0.1:10085`; при
  контейнерном запуске для этого нужен общий network namespace или host network;
- открыть gRPC listener только в management-сети, а служебный HTTP listener
  оставить на loopback;
- проверять `/health/live` и `/health/ready` во время rollout;
- завершать агент штатно перед согласованным резервным копированием SQLite.

Сейчас репозиторий содержит проверки CI, но ещё не содержит `Dockerfile` и
workflow публикации OCI-образа. До их добавления описанная схема является целевой,
а не полностью автоматизированной.

Если новая версия не добавляет SQL-миграцию, разрешён rollback на предыдущий
образ. Версия с новой миграцией считается rollback barrier, пока отдельно не
доказана её совместимость с предыдущим агентом. Удалять SQLite при rollback
нельзя: в базе может находиться неподтверждённый usage outbox.

Локальные проверки и git hooks:

```bash
make check
make test-coverage
make hooks
```

Подробные правила продвижения между `develop` и `prod`, а также политика
rollback для SQLite описаны в [документе о поставке](docs/DEPLOYMENT.md).
