# Node agent v1 — техническое задание

Статус: **нормативный документ для реализации**.

Агент разрабатывается в отдельном репозитории. Здесь описано то, что от него
требуется, чтобы работать с этим backend и с Xray на ноде.

## Источники

Иерархия при расхождениях — сверху вниз:

1. `proto/spiritvpn/nodeagent/v1/node_agent.proto` — провод. Байт-в-байт копия
   `infra_v1/contracts/nodeagent/v1/node_agent.proto`, владелец — инфраструктурный
   репозиторий. Побеждает текст любой спеки.
2. `internal/nodeagent/` этого репозитория — реализованный клиент. Всё, что здесь
   помечено ссылкой на файл, проверяемо чтением кода и уже работает именно так.
3. `dev/BACKEND_DOMAIN_AGREEMENTS.md` §7, §9, §10, §12, §16 — семантика. Файл
   удалён из рабочего дерева; читается через
   `git show caa4a87^:dev/BACKEND_DOMAIN_AGREEMENTS.md`.
4. `README.md` — устройство backend как смежной системы.

`dev/NODE_AGENT_CONTRACT.md` описывает отброшенную архитектуру и помечен
`superseded`. Ссылаться на него нельзя.

## 1. Назначение и границы

Агент — единственный, кто трогает runtime state Xray на ноде. Backend хранит
желаемое состояние в PostgreSQL и доводит его до агента по gRPC; напрямую к Xray
он не обращается никогда.

Агенту принадлежат:

* runtime users клиентского inbound входной ноды и их персональные routing rules;
* файловые entries этих users/rules и завершающий default-deny;
* локальная durable база: снапшот backend-owned набора и спул трафика;
* сбор per-email счётчиков StatsService, включая их сброс.

Агенту не принадлежат остальные части конфигурации Xray (inbound, outbound,
транспорт между нодами, инфраструктурные credentials — это infrastructure),
состав флота, сроки доступа и квоты (backend), решения об удалении пользователей
(backend, §6).

Единица управления — один `accounting_id` на одной ноде. Сущности «устройство» в
контракте нет.

## 2. Провод

`NodeAgentService` из `node_agent.proto`. Агент — сервер, backend — клиент.

| RPC | вызывает | в v1 |
|---|---|---|
| `EnsureUserPresent` | `dispatch` | завести или починить одного пользователя |
| `EnsureUserAbsent` | `dispatch` | снять одного пользователя |
| `GetNodeState` | `usage`, `reconcile` | забрать трафик; отдельно — инвентарь |
| `ReconcileUsers` | `reconcile` | заменить состав пользователей целиком |
| `Health` | — | backend его не вызывает |

`Health` реализовать всё равно: он нужен операторам и smoke-тестам деплоя.
Готовность ноды backend определяет по исходу остальных вызовов
([README.md:52](../README.md:52)).

### 2.1 Транспорт и идентичность

Mutual TLS поверх management WireGuard overlay. Порт агента открыт файрволом
только для backend.

Backend держит по одному переиспользуемому HTTP/2 channel на ноду, ключ кеша —
`node_id|address|tls_server_name|certificate_identity`
([client.go:36](../internal/nodeagent/client.go:36)). Смена endpoint в манифесте
даёт новый ключ и новое соединение, старое закрывается.

Требования к агенту:

* принимать только клиентский сертификат backend;
* предъявлять серверный сертификат, идентичность которого совпадает с
  `certificate_identity` из манифеста ноды — иначе backend классифицирует вызов
  как `IDENTITY_MISMATCH`, поднимает alert и прекращает hot-retry
  ([outcome.go](../internal/nodeagent/outcome.go));
* возвращать в `GetNodeStateResponse.node_id` свой сконфигурированный `node_id`.
  Backend сверяет его с тем, к кому подключался, и расхождение трактует как
  «соединение ведёт не туда» ([usage.go:140](../internal/nodeagent/usage.go:140),
  [inventory.go:85](../internal/nodeagent/inventory.go:85));
* никогда не принимать `node_id` из тела запроса как авторизационный вход. Нода
  определяется mTLS-идентичностью.

### 2.2 Дедлайны

Backend отменяет вызов по своему таймеру, и агент обязан укладываться:

| RPC | дедлайн backend | источник |
|---|---|---|
| `EnsureUserPresent`, `EnsureUserAbsent` | 5 с | [client.go:19](../internal/nodeagent/client.go:19) |
| `GetNodeState` | 30 с | [usage.go:15](../internal/nodeagent/usage.go:15) |
| `ReconcileUsers` | 60 с | [reconcile.go:16](../internal/nodeagent/reconcile.go:16) |

60 секунд на reconcile рассчитаны на полный набор ноды с `AddUser` и `AddRule` на
каждого пользователя.

### 2.3 Исходы операций

Мутирующие RPC отвечают `OperationResult` с `ApplyStatus`. Backend читает два
канала независимо — gRPC-код и статус в теле — и сводит их в судьбу операции
([outcome.go](../internal/nodeagent/outcome.go)):

`APPLY_STATUS_APPLIED`
: состояние изменено и проверено в Xray.

`APPLY_STATUS_ALREADY_APPLIED`
: Xray уже соответствовал запросу. Успех.

`APPLY_STATUS_RETRYABLE_ERROR`
: временный отказ. Backend сохраняет операцию и повторяет с тем же
  `operation_id`. Backoff 1 с → 5 мин, число попыток не ограничено, пока desired
  state актуален ([domain/dispatch.go:41](../internal/domain/dispatch.go:41)).

`APPLY_STATUS_PERMANENT_ERROR`
: вход недопустим. Автоповтор прекращается навсегда, операция остаётся в
  терминальном отказе, поднимается alert. Операторского API для её оживления в v1
  нет — чинится изменением desired state либо `ReconcileUsers`.

gRPC OK с `RETRYABLE_ERROR` внутри — штатный ответ «сейчас не могу», а не успех.
Разница между retryable и permanent решает, будет ли пользователь заведён вообще:
ошибочный permanent тихо оставляет клиента без доступа.

Транспортные коды: `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED` — retryable;
`INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `UNAUTHENTICATED`,
`PERMISSION_DENIED`, `UNIMPLEMENTED` — permanent.

Диагностика в `message` санитизируется и ограничивается по длине. Ни `client_uuid`,
ни содержимое локальной базы, ни данные о посещениях в неё попадать не должны:
backend кладёт этот текст в `agent_operations.last_error` и в логи.

### 2.4 Идемпотентность

Мутирующие вызовы несут `operation_id`. Повтор идёт с тем же значением.

Агент ведёт durable журнал операций с каноническим digest запроса. Повтор
завершённой операции возвращает сохранённый результат, не выполняя мутацию
второй раз. Тот же `operation_id` с другим методом или payload — `PERMANENT_ERROR`
без изменения состояния.

Канонизация семантическая, а не по байтам protobuf: в `ReconcileUsers.users`
проверяются дубли `accounting_id`, набор сортируется по `accounting_id`, и уже
это хешируется.

Backend повторяет актуальную операцию без ограничения по числу попыток, поэтому
в v1 завершённые записи журнала не удаляются автоматически. Ввод retention
потребует отдельного ограниченного горизонта повторов в backend либо протокола,
по которому backend подтверждает, что `operation_id` больше не будет повторён.

## 3. Конфигурация

Infrastructure владеет статическими секциями, outbound и базовым шаблоном.
Агент атомарно поддерживает в том же файле только clients namespace `u.*`,
персональные rules `spirit-agent:user:*` и завершающий default-deny.

| параметр | назначение |
|---|---|
| `node_id` | идентичность ноды в ответах |
| адрес и порт gRPC | слушатель в management-сети |
| TLS: сертификат, ключ, CA | mTLS с backend |
| адрес Xray API | по умолчанию `127.0.0.1:10085` |
| путь writable Xray config | по умолчанию `/opt/vpn/xray/config.json` |
| тег клиентского inbound | куда ставятся пользователи |
| тег локального выхода | Xray outbound для FREEDOM; на проводе ему соответствует пустой `egress_key` |
| путь локальной базы | по умолчанию `/var/lib/spirit-agent/state.db` |
| интервалы сбора и self-heal | см. §11 |

Каталог `0700`, база `0600`. В ней лежат открытые `client_uuid` — это принятый
компромисс ради доступности (§5), но файл секретный.

Требования к инфраструктуре, без которых агент нерабочий:

* на ноде включены `HandlerService`, `StatsService` и **`RoutingService`**;
* соответствия `egress_tag → outbound` созданы статически заранее; персональные
  правила создаёт агент;
* default-deny `inboundTag → block` находится последним; агент переносит
  существующий catch-all в конец либо создаёт его;
* runtime-правила добавляются через `RoutingService.AddRule(should_append=false)`
  и потому не оказываются после default-deny;
* агент создаёт персональное правило и для FREEDOM, направляя его в
  сконфигурированный локальный outbound. Это сохраняет безопасный fallback в
  `block`, но позволяет валидному локальному доступу выйти через FREEDOM;
* Xray API доступен только с loopback, удалённый `:10085` закрыт;
* форма поставки агента (systemd-юнит либо контейнер) обеспечивает ему доступ к
  loopback Xray, владение каталогом состояния и write/rename в каталоге
  `config.json`.

## 4. Управление пользователями

### 4.1 Модель

Backend передаёт `User`:

```protobuf
message User {
  string accounting_id   = 1;
  string credential_uuid = 2;
  string flow            = 3;
  string egress_key      = 4;
}
```

`accounting_id` — стабильный псевдоним, он же Xray `email` и ключ учёта трафика.
Формат backend: `u.` плюс 20 символов из `[a-z2-7]`, всего 22
([crypto/accounting.go:8-27](../internal/crypto/accounting.go:8)). Префикс `u.` —
backend namespace: по нему агент отличает свои пользователи от инфраструктурных
`svc-*`. Значение глобально уникально, не переиспользуется и не содержит ничего
пользовательского.

`credential_uuid` — секретный VLESS UUID. Отличается от `accounting_id` и
ротируется независимо: смена credential сохраняет `accounting_id`, историю
трафика и владение квотой.

`flow` — пустая строка либо `xtls-rprx-vision`.

`egress_key` — логический выход этого пользователя. Пустое значение означает
FREEDOM; агент отображает его в сконфигурированный тег локального Xray outbound.
Непустое значение — `egress_tag` связи из манифеста, дословно. Тег локального
outbound — деталь конфигурации ноды и на провод вместо пустого значения не
выставляется.

Уникальность `accounting_id` обязательна: `RemoveUser` матчит по email, и
StatsService считает трафик per-email.

### 4.2 EnsureUserPresent

Приводит Xray к состоянию «этот пользователь существует с точно такими
параметрами»:

1. проверить журнал операций по `operation_id`;
2. валидировать `accounting_id`, UUID, flow и принадлежность к namespace `u.`;
3. взять локальный лок мутаций и сброса StatsService;
4. при замене credential — снять и durable записать в спул остаток трафика
   прежнего значения;
5. записать желаемого пользователя в локальный снапшот **до** обращения к Xray;
6. атомарно записать полный desired-набор clients и rules в `config.json`;
7. `HandlerService.AddUser` (email = `accounting_id`, id = `credential_uuid`,
   flow);
8. создать `RoutingService.AddRule(should_append=false)` с детерминированным
   `rule_tag = "spirit-agent:user:" + accounting_id` вида
   `user:[accounting_id] → outbound(resolved_egress_tag)`, где пустой
   `egress_key` разрешается в сконфигурированный локальный outbound;
9. проверить фактический результат в Xray;
10. записать исход операции и ответить.

Персональные runtime-правила добавляются в начало, но фильтр `user` не даёт им
перехватить служебный API-трафик. В файле они находятся после статических
API/security rules и перед последним default-deny. Агент проверяет выбор выхода
через `RoutingService.TestRoute` для `user=accounting_id`; одного успешного
ответа `AddRule` недостаточно.

Существующий `accounting_id` с другим `credential_uuid` или другим `egress_key`
заменяется: пользователь и его правило приводятся к запрошенным значениям.
Совпадающее состояние возвращает `ALREADY_APPLIED`.

Падение агента после шага 5 не теряет изменение: локальный self-heal (§5)
дозавершает его при старте.

### 4.3 EnsureUserAbsent

`credential_uuid` здесь не передаётся вовсе — удаление матчится по
`accounting_id` ([client.go:172-176](../internal/nodeagent/client.go:172)).

Под тем же локальным локом:

1. проверить журнал операций;
2. снять и durable записать в спул остаток трафика пользователя;
3. записать `desired_present=false` в снапшот (локальный tombstone);
4. `HandlerService.RemoveUser` по email;
5. `RoutingService.RemoveRule` по детерминированному `rule_tag` персонального
   правила;
6. проверить отсутствие;
7. записать исход и ответить.

Порядок обязателен: счётчики снимаются до удаления, иначе трафик пользователя
теряется целиком. Падение между шагами 3 и 5 дозавершается self-heal.

Успешная проверка гарантирует отказ **новым** подключениям. Разрыв уже
установленного соединения в v1 не гарантируется; если это понадобится заявить как
жёсткое enforcement квоты, поведение закреплённой сборки Xray нужно доказать
E2E-тестом.

Инфраструктурные (`svc-*`) и неизвестные пользователи не трогаются никогда, ни
здесь, ни в reconcile.

## 5. Локальный self-heal

Runtime API сам по себе не персистит users и rules. Поэтому до live-мутации агент
записывает полный desired-набор в `config.json`. После рестарта Xray клиенты и
маршруты загружаются непосредственно из файла; self-heal остаётся независимой
проверкой и исправляет расхождения.

Агент держит durable снапшот backend-owned набора —
`{accounting_id, credential_uuid, flow, egress_key}` — и обновляет его на каждый
`EnsureUserPresent`, `EnsureUserAbsent` и `ReconcileUsers`.

Рестарт Xray определяется по сбросу `uptime_seconds` либо по переходу
`reachable` после недоступности. **Не по пустому списку пользователей**: пустая
и целая нода отличается от сброшенной именно по uptime.

Периодический self-heal приводит каждую запись с `desired_present=true` к точным
credential, flow и egress. Неизвестные runtime users он не удаляет; удаления
разрешены только durable tombstone либо полным авторитетным ReconcileUsers.
Флаг `applied` не исключает пользователя из этой проверки: он описывает результат
предыдущего применения, а не фактическое наличие state в текущем процессе Xray.

Отдельный recovery прерванных durable intent дозавершает только то действие,
которое уже было записано до падения: present-intent добавляет/заменяет
пользователя и правило, absent-intent снимает их. Это не удаление, выведенное из
отставшего снапшота: tombstone мог появиться только из аутентифицированного
`EnsureUserAbsent` либо полного `ReconcileUsers`. При `needs_bootstrap=true`
recovery удалений запрещён.

Self-heal и recovery работают при недоступном backend и ничего у него не
запрашивают.

Ошибки пользовательских Xray-операций накапливаются, а не схлопываются в одну
причину. Каждая содержит безопасный `accounting_id` и имя RPC (`AddUser`,
`RemoveUser`, `AddRule`, `RemoveRule` или `TestRoute`); credential в ошибку не
попадает.

Сверка запускается при старте агента, раз в 30 секунд и немедленно после
обнаружения рестарта. Недоступность Xray повторяется с экспоненциальным backoff
от 1 до 30 секунд, а не превращается в одноразовый отказ.

## 6. Authoritative reconcile

Единственный источник удалений, выведенных из расхождения полного набора, и
правки неизвестного дрейфа. Адресный `EnsureUserAbsent` и recovery уже записанного
absent-intent также могут удалять пользователя. Backend вызывает reconcile при
обнаружении расхождения, по таймеру и после рестарта агента.

```protobuf
message ReconcileUsersRequest {
  string operation_id = 1;
  bool   complete     = 2;   // backend всегда шлёт true
  repeated User users = 3;
}
```

Ревизии на проводе нет. Backend держит `desired_revision` у себя и проверяет
актуальность guarded-UPDATE после ответа
([reconcile.go:33-45](../internal/nodeagent/reconcile.go:33)).

`complete=true` — утверждение, что набор авторитетный и не усечён по дороге.
Пустой `users` легален и означает, что backend-owned пользователей на ноде нет.

Поведение агента:

* на время reconcile сериализовать его с `EnsureUserPresent/Absent`: входящие
  мутирующие RPC отвечают `UNAVAILABLE`, backend их повторит;
* валидировать **весь** набор до применения; невалидный запрос отклоняется
  целиком, Xray не меняется;
* перед массовым удалением сделать best-effort флаш спула; ошибка флаша не
  задерживает reconcile;
* в пределах namespace `u.`: добавить отсутствующих, заменить тех, у кого
  разошёлся `credential_uuid`, `flow` или `egress_key`, удалить backend-owned
  пользователей, которых в наборе нет;
* обновить локальный снапшот;
* ответить счётчиками `added`, `replaced`, `removed`, `unchanged`.

Счётчики не декоративные: ненулевой `removed` на живой ноде — это тот самый
дрейф, ради которого reconcile и существует, и backend выносит его в метрики.

Ошибка Xray посреди применения оставляет частично приведённое состояние. Это
допустимо: следующий reconcile безопасно продолжит с того же desired state.

## 7. Учёт трафика

Сбор ведёт агент по своему таймеру, независимо от доступности backend.

### 7.1 Сбор

Каждые 15 секунд со случайной фазой:

```text
QueryStats(pattern="user>>>", reset=true)
```

Один bulk-запрос на весь набор — закреплённая сборка Xray возвращает все
подходящие `Stat` в одном ответе и пагинации не имеет. Двух RPC на пользователя
быть не должно.

Агент — **единственный владелец сброса** счётчиков. Ненулевые дельты пишутся в
спул под новым `sequence`.

Финальный сбор одного пользователя (§4.3) идёт по полному префиксу ключа
`user>>><accounting_id>>>traffic>>>` с разбором точных uplink/downlink ключей,
без поиска по подстроке.

Сброс, замена credential и удаление пользователя сериализуются одним локальным
локом, иначе bulk-сброс и финальный сбор разойдутся на одних и тех же байтах.

### 7.2 Спул

```text
spool_id — случайный UUID на время жизни локального спула
sequence — монотонно растёт внутри spool_id
```

Пара `(spool_id, sequence)` — ключ идемпотентности на стороне backend. Новая
локальная база создаёт новый `spool_id`.

Жёсткие требования, проверяемые backend при разборе ответа
([usage.go:163-172](../internal/nodeagent/usage.go:163)):

* `spool_id` непуст;
* `sequence` начинается с **единицы**. Ноль отвергается как дефект версии агента
  — весь ответ не разбирается, поднимается alert.

Фраза `starts sequence from zero` в комментарии `UsageCursor` означает начальное
значение внутреннего счётчика до первой вставки. На провод batch с нулевым
`sequence` не выходит никогда: операция выдачи номера сначала увеличивает
счётчик, поэтому первый сохранённый и выданный batch имеет `sequence=1`.

Один результат сброса Xray становится одним или несколькими batch по 5 000 items
максимум. Все чанки одного сброса вставляются в локальную базу **одной
транзакцией** до того, как разрешён следующий сброс; их sequence непрерывны.
`accounting_id` уникален внутри batch.

`collected_at_unix_ms` — момент сбора на ноде. Backend сопоставляет по нему item с
периодом квоты, поэтому это не время получения ответа.

В batch запрещены: customer ID, source IP, destination, DNS, SNI, URL, содержимое
пакетов. Только `accounting_id` и агрегированные uplink/downlink.

### 7.3 Выдача и подтверждение

`GetNodeState` возвращает неподтверждённые batch по возрастанию `sequence`,
не больше `max_usage_batches`. Backend просит по 16 за опрос
([pull_usage.go:20](../internal/app/pull_usage.go:20)).

Агент удаляет batch **только** после подтверждения по совпадающему `spool_id`.
Подтверждение чужого спула игнорируется. Подтверждение выше максимального
выданного sequence игнорируется и поднимает alert.

Отдельно: при сверке инвентаря (§8) backend вызывает тот же `GetNodeState` с
**пустым** `acknowledged_usage_through` и приехавшие batch игнорирует
([inventory.go:54-67](../internal/nodeagent/inventory.go:54)). Агент обязан
трактовать пустой курсор как «не подтверждено ничего» и отдать те же batch
следующему опросу usage-воркера. Трактовка пустого курсора как разрешения на
очистку теряет неучтённый трафик.

Если спул больше не может безопасно принять результат сброса, агент прекращает
новые reset-запросы и поднимает критический alert. Неподтверждённые batch не
выбрасываются никогда.

Неустранимое окно потерь между сбросом счётчиков в памяти Xray и коммитом в
локальную базу остаётся, плюс до одного интервала сбора при аварийном падении
процесса Xray. Этого достаточно для контроля квот и недостаточно для точного
финансового учёта байтов.

## 8. Инвентарь

Отдельного RPC под инвентарь в контракте нет: backend зовёт `GetNodeState` с
`include_users=true`.

Ответ обязан содержать:

* `users` — фактическое состояние, наблюдённое **непосредственно в Xray**, а не
  копию снапшота;
* `users_complete=true` только если список полон и не усечён;
* `users_observed_at_unix_ms` — момент наблюдения на ноде;
* `backend_managed` на каждой записи — принадлежность к namespace `u.`;
* `egress_key` в каждом `ActualUser.user`.

`credential_uuid` и `flow` читаются через `HandlerService.GetInboundUsers`.
Фактический выход проверяется через `RoutingService.TestRoute(user=accounting_id)`;
наличие ожидаемого `rule_tag` дополнительно проверяется через `ListRule`. Если
Xray выбрал сконфигурированный локальный outbound, в ответе канонически уходит
пустой `egress_key`. Любой другой выбранный outbound, включая fallback `block`,
уходит своим фактическим тегом и создаёт `mismatch` с desired FREEDOM.

Backend отвергает наблюдение целиком, если `users_complete=false`,
`users_observed_at` нулевой, наблюдение старше 10 минут либо более чем на 10
минут помечено временем из будущего
([app/inventory.go:64+](../internal/app/inventory.go:64),
[worker.go:87-104](../cmd/spiritvpnd/worker.go:87)). Отвергнутое наблюдение не
даёт вывода «расхождений нет» и не авторизует удаления.

Ключ сравнения — `accounting_id` → `(credential_uuid, flow, egress_key)`.
Пустой `egress_key` корректен только когда фактический `TestRoute` выбрал
сконфигурированный локальный outbound. Для BRIDGE пустое значение либо любой
другой фактический тег создаёт `mismatch`.

Если полный инвентарь не помещается в сконфигурированный лимит ответа, RPC
завершается `RESOURCE_EXHAUSTED`. Помечать усечённый список полным запрещено.

## 9. Health

`Health.ready=true` означает одновременно: локальная база доступна на запись,
Xray достижим, bootstrap завершён, сбор трафика безопасен. От того, опрашивал ли
backend ноду недавно, не зависит.

`needs_bootstrap=true` выставляется, когда локальная база новая или повреждена. В
этом состоянии агент **не удаляет** пользователей ни при каких условиях, пока
backend не пришлёт полный набор через `ReconcileUsers`. Флаг едет в
`GetNodeStateResponse` и разбирается backend
([usage.go:57](../internal/nodeagent/usage.go:57)).

`XrayState.uptime_seconds` — источник детекта рестарта для self-heal (§5) и
liveness ноды для backend.

## 10. Локальная база

SQLite в режиме WAL. Логический состав:

```text
agent_meta(usage_spool_id, initialized, last_xray_audit_at)

managed_users(accounting_id, credential_uuid, flow, egress_key,
              desired_present, applied, updated_at)

operations(operation_id, operation_type, request_digest, status, result,
           created_at, updated_at)

usage_batches(spool_id, sequence, collected_at, payload, acknowledged)
```

База — recovery-кеш и outbox, а не вторая бизнес-база. Резервного копирования не
требует: набор пользователей восстанавливается из backend. Неподтверждённый
трафик при утрате диска теряется — принятая погрешность v1.

Схемой владеет агент и мигрирует её сам. Инфраструктура создаёт только каталоги
и конфигурацию.

## 11. Интервалы

| что | значение |
|---|---|
| сбор трафика из Xray | 15 с со случайной фазой |
| сверка снапшота с Xray | при старте агента, каждые 30 с и сразу после рестарта Xray |
| размер batch | ≤ 5 000 items |
| batch в одном ответе | по запросу backend, сейчас 16 |

Со стороны backend: `GetNodeState` каждые 15 с с jitter, инвентарь запрашивается
отдельным вызовом reconcile-воркера.

Масштаб: до 100 нод, до 50 000 customer в парке. Число управляемых пользователей
на ноде равно `C * (1 + R)`, где `R` — число входящих BRIDGE-связей; расчётный
потолок реализации — 2000 пользователей и столько же routing rules на ноду.

## 12. Наблюдаемость

Экспорт Prometheus, минимальный набор:

```text
spirit_agent_up
spirit_agent_xray_up
spirit_agent_usage_last_collection_timestamp_seconds
spirit_agent_usage_outbox_batches
spirit_agent_usage_outbox_bytes
spirit_agent_last_backend_poll_timestamp_seconds
spirit_agent_local_reconcile_errors_total
spirit_agent_needs_bootstrap
spirit_agent_desired_users
spirit_agent_desired_routing_rules
spirit_agent_applied_users
spirit_agent_applied_routing_rules
spirit_agent_state_drift
spirit_agent_last_successful_reconcile_timestamp_seconds
```

Alert'ы: устаревший сбор, растущий или переполненный спул, недоступность Xray,
застрявший bootstrap, повторяющиеся отказы self-heal.

Поскольку владельцем сброса счётчиков становится агент, любой внешний экспортёр
не может считать сырые счётчики Xray монотонными. Операционные дашборды по
пользователям строятся на накопленных значениях backend либо на монотонной серии,
которую экспортирует сам агент.

В логи не попадают: `credential_uuid`, TLS-ключи, payload операций, содержимое
локальной базы, source IP, destination.

## 13. Отказы

| ситуация | требуемое поведение |
|---|---|
| backend недоступен | состояние Xray сохраняется, трафик копится в спуле |
| агент недоступен | существующий трафик идёт, операции backend ждут |
| Xray перезапущен | users/rules загружаются из config.json; self-heal сверяет их без backend |
| ответ на RPC потерян | backend повторяет тот же `operation_id`, результат берётся из журнала |
| падение в середине добавления | снапшот записан до Xray, self-heal дозавершает |
| падение в середине удаления | tombstone записан до Xray, self-heal дозавершает |
| локальная база утрачена | `needs_bootstrap=true`, удаления запрещены |
| спул переполнен | новые сбросы прекращаются, alert; подтверждённое не выбрасывается |
| batch прислан повторно | backend дедуплицирует по `(spool_id, sequence, accounting_id)` |
| нода недоступна долго | операция сохраняется, backoff, alert |

## 14. Вне скоупа v1

**Подсистема activity.** Поля `ActivityBatch`, `SourceActivity`, `ActivityState` в
контракте присутствуют и остаются зарезервированными, но агент v1 их не заполняет
и access-log Xray не читает вовсе. Потребителя нет:
[internal/nodeagent/usage.go:46](../internal/nodeagent/usage.go:46) явно не
переносит `activity_batches` в состояние ноды. Возврат к теме потребует работы в
агенте и backend, но не изменения `.proto`.

Также не входят: точный финансовый учёт байтов при аварийном падении Xray;
гарантия разрыва уже установленного соединения; определение физических устройств;
высокая доступность одной входной ноды; брокер сообщений, long-poll и
версионированный протокол desired-списка.

## 15. Definition of done

Функционально:

* [ ] mTLS-сервер, сверка идентичности backend, `node_id` из конфигурации в ответах;
* [ ] `EnsureUserPresent` и `EnsureUserAbsent` с проверкой фактического результата
      в Xray, включая создание и удаление персонального routing rule для
      FREEDOM и BRIDGE;
* [ ] журнал операций с digest, повтор возвращает сохранённый результат, тот же
      `operation_id` с другим payload — `PERMANENT_ERROR`;
* [ ] `ReconcileUsers`: полная валидация до применения, сериализация с Ensure,
      удаления только в namespace `u.`, корректные счётчики;
* [ ] сбор трафика одним bulk-запросом с `reset=true`, спул с sequence от единицы,
      выдача и удаление только по подтверждению;
* [ ] `GetNodeState` с инвентарём: `users_complete`, время наблюдения,
      `backend_managed`, `egress_key`;
* [ ] сходящийся self-heal при старте, по таймеру и после рестарта Xray;
* [ ] `needs_bootstrap` и запрет удалений в этом состоянии;
* [ ] `Health`, метрики §12.

Тесты:

* [ ] повтор `operation_id` не выполняет мутацию дважды; тот же ID с другим
      payload не меняет Xray;
* [ ] `sequence` первого batch равен единице (backend отвергает ноль);
* [ ] пустой `acknowledged_usage_through` не удаляет ничего, и те же batch
      приезжают следующим опросом;
* [ ] подтверждение чужого `spool_id` игнорируется; подтверждение выше выданного
      sequence игнорируется и алертит;
* [ ] после рестарта Xray пользователи и правила уже загружены из файла при
      выключенном backend;
* [ ] отставший снапшот не удаляет пользователя, отсутствующего в нём;
* [ ] `ReconcileUsers` с пустым набором снимает всех backend-owned и не трогает
      `svc-*`;
* [ ] мутирующий RPC во время reconcile получает `UNAVAILABLE`;
* [ ] падение между записью снапшота и вызовом Xray дозавершается при старте — по
      одному тесту на добавление и на удаление;
* [ ] финальный сбор трафика перед удалением не теряет байты и не считает их
      дважды с bulk-сбросом;
* [ ] инвентарь при усечении отвечает `RESOURCE_EXHAUSTED`, а не
      `users_complete=false` с частичным списком.

Обязательные smoke/E2E на закреплённом образе Xray `26.3.27`:

* [ ] `RoutingService` присутствует в reflection и принимает `AddRule`,
      `RemoveRule`, `ListRule`, `TestRoute`;
* [ ] entry-конфигурация содержит персональные rules перед завершающим catch-all
      `inboundTag → block`;
* [ ] до добавления персонального правила `TestRoute` неизвестного
      `user=u.*` выбирает `block`;
* [ ] FREEDOM-пользователь получает правило с `should_append=false`, а
      `TestRoute(user=accounting_id)` выбирает настроенный `direct`;
* [ ] BRIDGE-пользователь получает правило с `should_append=false`, а `TestRoute`
      выбирает переданный `egress_key`;
* [ ] последовательное добавление нескольких персональных правил не меняет
      работу ранее добавленных правил и статических API/security rules;
* [ ] `ListRule` возвращает детерминированный `rule_tag` и нужный outbound;
      `RemoveRule(rule_tag)` удаляет только это правило, после чего тот же
      `TestRoute` снова выбирает `block`;
* [ ] реальный VLESS-трафик FREEDOM выходит через локальный FREEDOM outbound, а
      BRIDGE-трафик — через тестовый bridge outbound; одной проверки
      `TestRoute` для допуска схемы недостаточно;
* [ ] после рестарта Xray без участия backend пользователь продолжает работать:
      clients и rules уже присутствуют в `config.json`, self-heal подтверждает
      отсутствие расхождения.

E2E, до заявления жёсткого enforcement квоты:

* [ ] поведение закреплённой сборки Xray при удалении пользователя с активным
      соединением.

## 16. Открытые вопросы

1. Форма поставки агента: systemd-юнит на хосте либо контейнер. Влияет на доступ
   к loopback Xray и владение каталогом состояния.
2. Проверка клиентской mTLS-идентичности: выделенный CA только для backend либо
   отдельный allowlist SAN/SPIFFE identity в конфигурации агента.
3. Предел локального usage-спула и резерв свободного места, после которого агент
   прекращает `reset=true`: нужны конкретные значения для `Health.ready` и
   критического alert.
