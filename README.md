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

Локальные проверки и git hooks:

```bash
make check
make test-coverage
make hooks
```

Правила продвижения между `develop` и `prod`, а также политика rollback для
SQLite описаны в [документе о поставке](docs/DEPLOYMENT.md).
