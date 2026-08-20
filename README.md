# NodeAgent
Агент на Go, обслуживающий xray на серверах VPN-флота

## Конфигурация

Процесс читает настройки только из переменных окружения. Infrastructure заранее
создаёт каталог состояния с правами `0700`, TLS-файлы и базовую конфигурацию
Xray. Агент атомарно поддерживает в ней принадлежащие ему VLESS clients и
routing rules; файл и его каталог должны быть доступны ему для записи.

| переменная | обязательна | значение по умолчанию |
|---|---:|---|
| `SPIRIT_NODE_ID` | да | — |
| `SPIRIT_GRPC_LISTEN` | да | —; конкретный management IP и порт |
| `SPIRIT_HTTP_LISTEN` | нет | `127.0.0.1:9090`; конкретный IP и порт |
| `SPIRIT_GRPC_TLS_CERT_FILE` | да | — |
| `SPIRIT_GRPC_TLS_KEY_FILE` | да | — |
| `SPIRIT_GRPC_TLS_CLIENT_CA_FILE` | да | — |
| `SPIRIT_GRPC_ALLOWED_CLIENT_IDENTITIES` | да | —; DNS/URI SAN через запятую |
| `SPIRIT_XRAY_INBOUND_TAG` | да | — |
| `SPIRIT_XRAY_CONFIG_PATH` | нет | `/opt/vpn/xray/config.json` |
| `SPIRIT_XRAY_LOCAL_OUTBOUND_TAG` | да | — |
| `SPIRIT_XRAY_API_ADDRESS` | нет | `127.0.0.1:10085` |
| `SPIRIT_XRAY_FALLBACK_OUTBOUND_TAG` | нет | `block` |
| `SPIRIT_STATE_DB_PATH` | нет | `/var/lib/spirit-agent/state.db` |
| `SPIRIT_MAX_INVENTORY_USERS` | нет | `2000` |
| `SPIRIT_SHUTDOWN_TIMEOUT` | нет | `10s` |
| `SPIRIT_LOG_LEVEL` | нет | `info` |

Служебный HTTP-сервер отдаёт liveness по `/health/live`, readiness по
`/health/ready` и метрики Prometheus по `/metrics`. Пути `/healthz` и `/readyz`
доступны как короткие алиасы. У этого сервера нет ни TLS, ни проверки
вызывающего, а endpoint метрик раскрывает состояние ноды, поэтому адрес обязан
быть конкретным: wildcard и пустой хост отвергаются. Какую сеть открыть, решает
развёртывание. По умолчанию это loopback; management-адрес допустим и нужен,
когда метрики и health собирает Prometheus управляющего контура.

Сборка:

```bash
make build VERSION=dev
make image VERSION=dev IMAGE=node-agent:dev
```

## Деплой

Node-agent поставляется как один публичный OCI-образ. Один и тот же образ
используется в `develop` и `prod`: окружение, адреса, TLS-материалы и параметры
Xray передаются только через инфраструктурную конфигурацию и не встраиваются в
образ.

Целевой процесс поставки:

1. Коммит в `main` проходит CI и публикуется с неизменяемым тегом
   `sha-<git-sha>`.
2. CI сообщает digest в Infrastructure-репозиторий; тот закрепляет образ в своём
   desired state отдельным коммитом и разворачивает его в `develop`.
3. После проверки тег `vX.Y.Z` ставится на коммит, доступный из `main`. CI
   добавляет semver-тег к уже проверенному digest, не пересобирая образ; если
   соответствующего образа нет, release завершается ошибкой.
4. Infrastructure-репозиторий переводит `prod` на тот же digest и управляет
   стратегией rollout и rollback.

Движущиеся теги можно использовать для навигации по registry, но не для деплоя.
Публичный образ не требует registry credentials на VPN-нодах. Секреты, включая
TLS-ключи, в образ не входят.

Инфраструктура при запуске агента должна:

- создать каталог состояния с правами `0700`, владельцем UID/GID `65532` и
  сохранить его между обновлениями; образ запускает агент от distroless-пользователя
  `nonroot` с этим UID/GID;
- передать TLS-файлы read-only, а каталог конфигурации Xray — read-write для
  UID/GID `65532` (нужны создание временного файла и атомарный rename);
- предоставить доступ к локальному Xray API на `127.0.0.1:10085`; при
  контейнерном запуске для этого нужен общий network namespace или host network;
- открыть gRPC listener только в management-сети; служебный HTTP listener
  открывать там же, если его собирает Prometheus, и оставлять на loopback,
  если нет — публичного адреса он не должен получать ни в каком случае;
- проверять `/health/live` и `/health/ready` во время rollout;
- завершать агент штатно перед согласованным резервным копированием SQLite.

Основной CI собирает multi-platform образ для `linux/amd64` и `linux/arm64` после
успешных тестов. Образы публикуются в GitHub Container Registry по адресу
`ghcr.io/<organization>/node-agent`. Для первой публикации владелец организации
должен один раз сделать пакет публичным в настройках GHCR; последующие версии
публикуются с помощью стандартного `GITHUB_TOKEN`.

Точный `image@sha256:...` выводится в summary workflow. Именно эту ссылку следует
переносить в infrastructure-репозиторий.

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
