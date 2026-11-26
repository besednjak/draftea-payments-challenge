# draftea-task-2-implementation

---

## [User]
vamos a laburar juntos como pares en la parte 2 de este challenge tecnico de backend senior en go te pido que actues como un senior backend engineer en go enfocado en codigo limpio principios solid y arquitectura hexagonal

te dejo adjunto el pdf del challenge para que tengas los requisitos originales ya terminamos toda la parte 1 de arquitectura y estamos parados en la rama feat/core-implementation donde tenes toda la docu actualizada en los 6 archivos de la carpeta docs/ el readme y la estructura de carpetas base ya creada

primero lee el pdf y los docs de docs/ para empaparte bien de lo que diseñamos la idea es armar un monolito modular en go con un alcance realista para 4 horas que cubra bien todo lo imprescindible event store con optimistic locking read models event bus en memoria particionado por clave saga orquestada mock de pasarela con backoff jitter y circuit breaker observabilidad con slog metricas y health checks y tests para los 5 escenarios incluyendo concurrencia con go test -race

no escribas codigo todavia para arrancar proponeme brevemente el plan de paquetes e interfaces siguiendo nuestra estructura actual y una vez que me cierre empezamos implementando solo internal/domain y internal/application/ports manteniendo cero dependencias externas en el dominio y montos en int64

## [Assistant]
Ya leí el PDF y los 6 documentos en [`docs/`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/docs), además del [`README.md`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md) y [`go.mod`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/go.mod). El diseño de la Parte 1 está muy sólido y acotado; baja limpio a un monolito modular hexagonal en Go.

Acá va el plan breve de **paquetes e interfaces** respetando la estructura actual, con cero dependencias externas en el dominio (solo stdlib) y montos en `int64` (centavos):

---

### 1. Capa de Dominio (`internal/domain`) — *cero dependencias externas*

`wallet` y `payment` **no se importan entre sí**; ambos dependen únicamente del kernel compartido `events`.

- **`internal/domain/events`**
  - `Money` (`Amount int64`, `Currency string`) con validaciones (`> 0`, moneda ISO-4217 no vacía, `SameCurrency`).
  - `Envelope` estándar (`EventID`, `Type`, `SchemaVersion`, `AggregateType`, `AggregateID`, `AggregateVersion`, `OccurredAt`, `CorrelationID`, `CausationID`, `Payload json.RawMessage`) + helpers tipados `NewEnvelope` y `DecodePayload[T]`.
  - Constantes de topics (`payments.events.v1`, `wallets.commands.v1`, `wallets.events.v1`, `gateway.commands.v1`, `gateway.events.v1` + helper `DLQTopic`), tipos de comandos/eventos y structs de payload del catálogo en [`03-event-specification.md`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/docs/03-event-specification.md).
- **`internal/domain/wallet`**
  - Aggregate `Wallet`: `id`, `currency`, `balance int64`, `held int64`, `reservations map[string]int64`, `refunds map[string]struct{}`, `version int64`, eventos pendientes.
  - Operaciones: `Create`, `Deposit`, `Reserve` (emite `FundsReserved` o `FundsReservationRejected` sin romper invariantes), `Capture`, `Release`, `Refund`.
  - Rehidratación: `Apply`, `Rehydrate(snapshot, history)` y `Snapshot()` (para snapshots cada N eventos).
  - Invariantes: `Available() = balance - held >= 0`, misma moneda, idempotencia por `payment_id`/`refund_id`.
- **`internal/domain/payment`**
  - Aggregate `Payment` + enum `Status` (`PENDING`, `FUNDS_RESERVED`, `REJECTED`, `SUBMITTED`, `GATEWAY_UNKNOWN`, `MANUAL_REVIEW`, `COMPLETED`, `FAILED`, `REFUNDED`).
  - Máquina de estados explícita (`Initiate`, `MarkFundsReserved`, `MarkRejected`, `MarkSubmitted`, `MarkGatewayUnknown`, `MarkManualReview`, `MarkCompleted`, `MarkFailed`, `RecordRefund`) con validación estricta de transiciones e invariante `refundedTotal + amount <= capturedAmount`.
  - Rehidratación: `Apply` y `Rehydrate(history)`.

---

### 2. Capa de Aplicación (`internal/application`)

- **`internal/application/ports`** *(interfaces segregadas por ISP, contratos que implementa `infrastructure`)*
  - **Persistencia Event Sourcing:**
    - `EventStore`: `Append(ctx, streamID, expectedVersion, evts...) error` (devuelve `ErrVersionConflict`), `LoadStream(ctx, streamID, fromVersion)`, `LoadAll(ctx, fromSeq)` (para rebuild/replay).
    - `SnapshotStore` (segregada de `EventStore` porque solo `Wallet` usa snapshots): `SaveSnapshot`, `LoadSnapshot`.
    - `InboxStore` (idempotencia de consumidores): `TryMarkProcessed(ctx, consumer, eventID) (bool, error)`.
    - `IdempotencyStore` (nivel API): `GetOrReserve` / `Complete` por `(userID, key, requestHash)`.
  - **Mensajería:**
    - `EventPublisher`: `Publish(ctx, topic, partitionKey, env) error`.
    - `EventSubscriber`: `Subscribe(topic, group, handler) error`.
    - `DLQReader`: inspección de mensajes en DLQ para alertas/tests.
  - **Integración externa:**
    - `PaymentGateway`: `SubmitPayment`, `CheckStatus`, `RefundPayment` (devolviendo resultados tipados: aceptado/aprobado, rechazado, timeout/error transitorio).
  - **Read Models (CQRS):**
    - `WalletBalanceReadModel` (`Upsert` sincrónico *read-your-writes* + `Get`).
    - `WalletTransactionReadModel` (`Append` asincrónico + `ListByWallet` + `Reset`).
    - `PaymentViewReadModel` (`Upsert` + `Get` + `ListNonTerminal` para recovery de sagas + `Reset`).
  - **Observabilidad:**
    - `MetricsRecorder` y `HealthChecker` (`Name()`, `Check(ctx) error`).
- **`internal/application/saga`** *(siguiente iteración)*
  - `PaymentOrchestrator`: inicia pagos (con espera acotada de hasta 2s para rechazo inmediato `INSUFFICIENT_FUNDS`), reacciona a `wallets.events.v1` y `gateway.events.v1`, emite comandos, gestiona conciliación (`CheckPaymentStatus` → `MANUAL_REVIEW`), reembolsos y `RecoverActiveSagas(ctx)` tras reinicio.
  - `WalletService` (handler de `wallets.commands.v1`): carga aggregate desde snapshot + stream, aplica comando con retry ante `ErrVersionConflict` + jitter, persiste snapshot periódico, actualiza `WalletBalanceReadModel` y publica evento.

---

### 3. Infraestructura, Observabilidad y Demo *(iteraciones posteriores)*

- **`internal/infrastructure/eventstore`**: `InMemoryEventStore` thread-safe (`sync.RWMutex`), validación atómica de `(stream_id, version)`, secuencia global para replay y `InMemorySnapshotStore`.
- **`internal/infrastructure/eventbus`**: bus particionado por clave (`FNV-1a % partitions`) con un worker por partición (garantiza orden FIFO por `wallet_id` / `payment_id`), reintentos con backoff, captura a DLQ y shutdown ordenado.
- **`internal/infrastructure/readmodel`**: implementaciones en memoria de `WalletBalance`, `WalletTransaction` y `PaymentView`, con método `RebuildFrom(ctx, eventStore)` para el escenario 5.
- **`internal/infrastructure/gateway`**: `MockGateway` configurable (happy path sync/webhook, rechazo, timeouts intermitentes o totales, estado de conciliación, verificación HMAC), `CircuitBreaker` (closed/open/half-open), política de retry (backoff exponencial + full jitter) y `GatewayAdapter` que consume `gateway.commands.v1` y publica `gateway.events.v1`.
- **`internal/observability`**: logger `log/slog` JSON con `correlation_id`/`payment_id`/`wallet_id`, colector de métricas coreografiado (suscripto a `*.events.v1`) + métricas operativas, y registro de health checks (`/healthz`, `/readyz`).
- **`cmd/demo`**: composición completa (wiring por DI) ejecutando los 5 escenarios, acompañado de tests unitarios y de integración con `go test -race` cubriendo los 5 flujos.

---

### Dos detalles antes de arrancar con `domain` + `ports`

1. **`WalletService` (handler de comandos de Wallet):** ¿preferís mantener estrictamente las carpetas actuales (poniéndolo en `internal/application/saga`, o bien renombrando/agregando solo `internal/application/wallet`)? Para este alcance, tenerlo en `internal/application/wallet` separa limpiamente `Wallet` de la saga de `Payment`, pero también podemos respetar exactamente el árbol actual si querés que coincida 1:1 con el [`README.md`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md).
2. **IDs en el dominio sin dependencias externas:** para `event_id` podemos usar un generador liviano basado en `crypto/rand` (formato UUIDv4 en `events`) o inyectar un `IDGenerator` por puerto y mantener los aggregates deterministas para los tests. Yo iría por un `crypto/rand` en stdlib con opción de clock/id inyectable si hace falta.

Decime si te cierra este diseño y esas dos definiciones, y arrancamos implementando **solo `internal/domain` y `internal/application/ports`** (con sus tests unitarios de dominio).

## [User]
me cierra el plan, te confirmo los dos puntos
1 si creemos internal/application/wallet para separar bien el servicio de wallet de la saga de payment despues actualizamos esa linea en el readme
2 si vamos con crypto/rand en stdlib para mantener cero dependencias externas y con opcion de inyectar clock o id para los tests

avanza e implementa solo internal/domain y internal/application/ports junto con los unit tests de dominio y corre go test ./... para verificar que todo compila y pasa limpio

## [Assistant]
Implementados `internal/domain` y `internal/application/ports` junto con los unit tests de dominio.

### Qué se creó

- **`internal/domain/events`** *(solo stdlib)*
  - [money.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/money.go): `Money` con `Amount int64` + validación/normalización ISO-4217.
  - [envelope.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/envelope.go): `Envelope`, `NewUUID` con `crypto/rand`, hooks inyectables `Clock`/`IDGenerator`, `NewEnvelope`, `DecodePayload[T]` y `StreamID`.
  - [catalog.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/catalog.go): topics (`*.v1` + `DLQTopic`), constantes de eventos/comandos/razones y todos los payloads tipados del catálogo.
  - [events_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/events_test.go): tests de `Money`, UUIDv4, roundtrip de `Envelope` y hooks deterministas.

- **`internal/domain/wallet`** *(solo importa `domain/events`)*
  - [errors.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/errors.go) y [wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go): aggregate `Wallet` con `Create`, `Deposit`, `Reserve`, `Capture`, `Release`, `Refund`, invariante `Available() = Balance - Held >= 0`, idempotencia por `payment_id`/`refund_id`/`reference_id`, `BaseVersion()` para optimistic locking, `TakeSnapshot()` (con copia profunda) y `Rehydrate(snap, history)`.
  - [wallet_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet_test.go): cobertura de ciclo reserva/captura/liberación, rechazos por saldo insuficiente y moneda, reembolsos, idempotencia y rehidratación con y sin snapshots.

- **`internal/domain/payment`** *(solo importa `domain/events`)*
  - [errors.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/errors.go) y [payment.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment.go): aggregate `Payment` con la máquina de estados completa (`PENDING` → `FUNDS_RESERVED` → `SUBMITTED` → `GATEWAY_UNKNOWN` → `MANUAL_REVIEW` → `COMPLETED` / `FAILED` / `REJECTED` / `REFUNDED`), reembolsos parciales y totales con invariante `refundedTotal + amount <= capturedAmount`, idempotencia por `refund_id` y `Rehydrate`.
  - [payment_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment_test.go): validaciones, transiciones válidas e inválidas, conciliación/`MANUAL_REVIEW`, reembolsos parciales/totales y replay.

- **`internal/application/ports`** *(interfaces segregadas por ISP)*
  - [errors.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/errors.go), [eventstore.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/eventstore.go), [eventbus.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/eventbus.go), [idempotency.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/idempotency.go), [gateway.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/gateway.go), [readmodels.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/readmodels.go), [observability.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/observability.go).

### Verificación

`go vet ./... && go test -v -race -cover ./...` pasó limpio sin race conditions y con cero dependencias externas:
- `internal/domain/events`: **PASS** (`85.0%` coverage)
- `internal/domain/payment`: **PASS** (`83.3%` coverage)
- `internal/domain/wallet`: **PASS** (`82.8%` coverage)
- `internal/application/ports`: compila limpio (solo depende de stdlib + `domain/events`)

¿Seguimos ahora con la capa de infraestructura (`eventstore`, `eventbus`, `readmodel`, `gateway`) y `observability`, o preferís implementar primero `internal/application/wallet` y `internal/application/saga`?

## [User]
ya commitee este bloque
sigamos ahora con la capa de infraestructura eventstore eventbus readmodel gateway y observability asi ya nos quedan listos y probados los adaptadores en memoria el optimistic locking el particionado con dlq y el circuit breaker antes de conectar wallet y saga
agregale unit tests a las piezas criticas de infra sobre todo optimistic locking concurrente en el eventstore orden por particion y dlq en el bus y los estados del circuit breaker y corre go test -race ./... para verificar

## [Assistant]
Implementada y probada toda la capa de infraestructura y observabilidad, lista para conectar `wallet` y `saga`.

### Qué se creó

- **`internal/infrastructure/eventstore`**
  - [memory.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/memory.go): `InMemoryEventStore` (append-only, optimistic locking atómico sobre `(streamID, version)`, secuencia global `GlobalSeq` para replay, clonación defensiva de payloads y `HealthChecker`), `InMemorySnapshotStore`, `InMemoryInboxStore` y `InMemoryIdempotencyStore` con expiración por TTL.
  - [memory_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/memory_test.go): concurrencia optimista (20 goroutines compitiendo → 1 ganador y 19 `ErrVersionConflict`), loop concurrente de recarga y retry de 25 escritores sin saltos de versión, snapshots, inbox e idempotencia.

- **`internal/infrastructure/eventbus`**
  - [partitioned.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned.go): `PartitionedBus` con N particiones por `FNV-1a(partitionKey)` y colas FIFO sin deadlock ante publicaciones recursivas sobre la misma partición, reintentos con backoff, soporte de errores `NonRetryable` (*poison pill* directo a DLQ), captura a DLQ con metadata completa, `Drain(ctx)` y `Close(ctx)`.
  - [partitioned_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned_test.go): orden estricto FIFO por clave (10 claves × 50 mensajes concurrentes), recuperación en reintentos, ruteo a DLQ (por agotamiento y por *poison pill*) y publicación encadenada en una sola partición sin deadlock.

- **`internal/infrastructure/readmodel`**
  - [memory.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/memory.go): `WalletBalanceStore`, `WalletTransactionStore` (con deduplicación por `event_id`), `PaymentViewStore` (con `ListNonTerminal` para recovery de sagas) y `Projector` con `RebuildAll(ctx, store)` para reconstruir todos los read models desde el Event Store.
  - [memory_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/memory_test.go): reconstrucción completa de proyecciones y descubrimiento de sagas activas.

- **`internal/infrastructure/gateway`**
  - [circuitbreaker.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/circuitbreaker.go): `CircuitBreaker` (`CLOSED` / `OPEN` / `HALF_OPEN`) con ventana deslizante (>50% fallos en 20 llamadas), 5 timeouts consecutivos, cooldown de 30s y 3 probes exitosos para cerrar.
  - [retry.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/retry.go): backoff exponencial con *full jitter* (`random(0, min(cap, base * 2^attempt))`).
  - [mock.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/mock.go): `MockGateway` configurable (aprobación sincrónica/asincrónica, rechazos, timeouts transitorios o totales, `CheckStatus` para conciliación, reembolsos y firma/verificación HMAC-SHA256).
  - [adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go): ACL que consume `gateway.commands.v1`, aplica bulkhead + circuit breaker + retry, recibe webhooks HMAC deduplicados y garantiza la regla de oro (solo emite `GatewaySubmissionNotSent` si ningún intento salió por red; si el breaker se abre tras un timeout enviado, emite `GatewayOutcomeUnknown`).
  - [gateway_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/gateway_test.go): transiciones completas del circuit breaker, cotas de *full jitter*, no-envío con circuito abierto, timeouts a mitad de retry y webhooks HMAC.

- **`internal/observability`**
  - [logger.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/observability/logger.go), [metrics.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/observability/metrics.go), [health.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/observability/health.go) y [observability_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/observability/observability_test.go): logging estructurado JSON con `slog` y correlación por contexto, colector coreografiado de métricas de negocio y operativas, y registro `/healthz` + `/readyz`.

### Verificación

`go vet ./... && go test -v -race -cover ./...` pasó completo y sin race conditions:
- `internal/infrastructure/eventstore`: **PASS** (`83.9%`)
- `internal/infrastructure/eventbus`: **PASS** (`86.7%`)
- `internal/infrastructure/readmodel`: **PASS** (`63.3%`)
- `internal/infrastructure/gateway`: **PASS** (`59.3%`)
- `internal/observability`: **PASS** (`77.4%`)

Cuando quieras, seguimos con el último bloque fuerte: `internal/application/wallet`, `internal/application/saga`, los tests integrales de los **5 escenarios** (incluyendo concurrencia y recuperación tras reinicio), `cmd/demo` y el ajuste final del [`README.md`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md).

## [User]
ya commitee infra y observability

vamos ahora con el ultimo bloque de implementacion
1 internal/application/wallet con el manejo de comandos de billetera optimistic locking con retry y actualizacion del read model de saldo
2 internal/application/saga con el orquestador de pagos espera acotada de 2s para rechazo inmediato conciliacion ante timeout reembolsos y recuperacion de sagas activas
3 tests de integracion cubriendo los 5 escenarios del enunciado ruta feliz saldo insuficiente timeout de pasarela con conciliacion y compensacion pagos concurrentes probando que no hay doble gasto y recuperacion tras reinicio reconstruyendo read models y retomando sagas
4 cmd/demo con un main limpio que conecte todo por inyeccion de dependencias y corra una demo legible de los escenarios mostrando tambien metricas y health check
5 actualizar el readme con la carpeta internal/application/wallet la version real de go que tenemos en go.mod y los comandos finales verificados

corre al final go vet ./... go test -race -cover ./... y go run ./cmd/demo para validar que todo funciona punta a punta

## [Assistant]
Listo el bloque final: aplicación (`wallet` + `saga`), tests de integración de los 5 escenarios, `cmd/demo` y actualización del [`README.md`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md).

### Qué se implementó

1. **`internal/application/wallet`**
   - [service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go): manejo de `wallets.commands.v1` (`ReserveFunds`, `CaptureFunds`, `ReleaseFunds`, `RefundFunds`), `CreateWallet`, `Deposit` y `GetBalance`, con retry con jitter ante `ErrVersionConflict`, snapshots periódicos, deduplicación por Inbox y actualización sincrónica (*read-your-writes*) de `WalletBalanceReadModel`.
   - [service_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service_test.go): prueba *read-your-writes*, snapshots periódicos + rehidratación y contención concurrente directa con optimistic locking.

2. **`internal/application/saga`**
   - [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): `PaymentOrchestrator` con `InitiatePayment` (idempotencia por `Idempotency-Key` + espera acotada de 2s para devolver `422` inmediato o `202` con `available_balance`), handlers para `wallets.events.v1` y `gateway.events.v1`, conciliación (`TriggerReconciliation`, `ExpireReconciliationWindow`, `ResolveManualReview`), reembolsos parciales/totales (`RequestRefund`) y `RecoverActiveSagas` desde el Event Store.
   - [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go): suite integral de los **5 escenarios**:
     - **Escenario 1:** ruta feliz asincrónica (`ACCEPTED` → webhook HMAC `APPROVED` → captura → `COMPLETED`), cache de `Idempotency-Key` y reembolso parcial + total (`REFUNDED`).
     - **Escenario 2:** saldo insuficiente con respuesta inmediata `422 INSUFFICIENT_FUNDS` y 0 llamadas a la pasarela.
     - **Escenario 3:** timeout de pasarela (3 intentos con backoff + jitter) → `GATEWAY_UNKNOWN` con fondos retenidos → conciliación aprobada (`COMPLETED`), conciliación `NOT_FOUND` (compensación `ReleaseFunds` → `FAILED`), vencimiento a `MANUAL_REVIEW` y circuit breaker abierto con liberación inmediata segura (`CIRCUIT_OPEN`).
     - **Escenario 4:** pagos concurrentes (`$100` vs 2×`$80` → 1 `COMPLETED`, 1 `REJECTED`, `$20` restantes) + test de contención directa sobre `WalletService` probando el optimistic locking bajo `-race`.
     - **Escenario 5:** simulación de caída y reinicio sobre un nuevo proceso leyendo solo de `EventStore` + `SnapshotStore` → `RebuildAll` de read models + `RecoverActiveSagas` completando y compensando sagas pendientes.

3. **`cmd/demo` y documentación**
   - [main.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/cmd/demo/main.go): composición por inyección de dependencias de los 4 módulos y ejecución secuencial de los 5 escenarios con salida de `/healthz`, `/readyz` y métricas (soporta `DEMO_LOGS=1` para ver los logs JSON de `slog`).
   - [README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md): actualizada la estructura con `internal/application/wallet`, versión `Go 1.25.2+`, estado de Parte 2 y comandos finales verificados.

### Verificación

`go vet ./... && go test -race -cover ./... && go run ./cmd/demo` pasó limpio (0 race conditions, 0 dependencias externas):

- `internal/domain/events`: **PASS** (`85.0%`)
- `internal/domain/payment`: **PASS** (`83.3%`)
- `internal/domain/wallet`: **PASS** (`82.8%`)
- `internal/application/wallet`: **PASS** (`58.3%`)
- `internal/application/saga`: **PASS** (`71.3%`)
- `internal/infrastructure/eventstore`: **PASS** (`83.9%`)
- `internal/infrastructure/eventbus`: **PASS** (`86.7%`)
- `internal/infrastructure/readmodel`: **PASS** (`63.3%`)
- `internal/infrastructure/gateway`: **PASS** (`59.3%`)
- `internal/observability`: **PASS** (`77.4%`)
- `go run ./cmd/demo`: ejecutó los 5 escenarios completos + `/healthz` + `/readyz` + métricas finales.

¡Ya queda listo para commitear y mergear! Recordá guardar el historial de esta conversación en `ai-history/` como pide el enunciado del challenge.

## [User]
abramos una pr hacia main, utiliza gh, escribe el titulo y descripcion en español para ser consistentes con primera pr que hice

## [Assistant]
Listo, PR creada hacia `main` manteniendo el mismo formato y estilo en español que la PR #1:

- **PR #2:** https://github.com/besednjak/draftea-payments-challenge/pull/2
- **Título:** `feat: implementación del monolito modular orientado a eventos en Go (Parte 2)`
- **Ramas:** `feat/core-implementation` → `main` (incluyendo los 3 commits de dominio/puertos, infraestructura/observabilidad y aplicación/escenarios/demo).

## [User]
estuve revisando todo el codigo linea por linea antes de pasar a la review final y encontre varios bugs reales ademas de cosas de organizacion nombramiento y tests que quiero dejar prolijas ahora

aplica estas correcciones manteniendo la misma arquitectura y sin sumar dependencias externas

bugs reales a corregir
1 inbox y webhooks hoy en wallet/service.go saga/orchestrator.go y gateway/adapter.go llamamos a TryMarkProcessed antes de ejecutar la logica como no hay rollback en memoria si falla un intento y el bus reintenta el segundo intento se descarta como duplicado arreglalo para que solo quede marcado si el handler termina sin error o se revierta ante fallo lo mismo con seenWebhooks en HandleWebhook y agrega un test que pruebe que un error transitorio se reintenta sin quedar bloqueado por el inbox
2 en eventbus/partitioned.go deliverWithRetry hay un break dentro de un select dentro de un for al cancelarse runCtx eso solo sale del select y sigue reintentando corregilo para cortar los reintentos inmediatamente y evita que Drain deje goroutines colgadas si vence el contexto
3 en saga/orchestrator.go toPaymentView no preserva CreatedAt y lo pisa con fecha cero al sincronizar la vista corregilo y agrega jitter al reintentar conflictos de version en mutatePayment igual que en wallet
4 conecta las piezas sueltas usa observability.WithCorrelation y LoggerFromContext en el flujo real en vez de dejarlos sin usar y envuelve los errores de DecodePayload como no reintentables para que los poison pills vayan directo a DLQ sin gastar 3 intentos

organizacion nombres y comentarios
5 dividi eventstore/memory.go en archivos por store eventstore.go snapshot.go inbox.go idempotency.go y readmodel/memory.go en balance.go transaction.go payment.go projector.go
6 elimina duplicacion crea un helper privado publishCommand en saga/orchestrator.go para unificar los 7 metodos emit y un helper en readmodel/projector.go para no repetir 5 veces el bloque de movimiento de billetera
7 limpia cmd/demo/main.go separa los 5 escenarios en funciones runScenario1 a runScenario5 y usa un helper must en vez de ignorar errores con guion bajo
8 mejora el nombramiento reemplaza abreviaturas confusas y variables de una letra como a b c d pv1 bal1 se cp whBody snaps idemp por nombres descriptivos dejando solo convenciones idiomaticas de go como receivers cortos ctx y err
9 limpia comentarios borra separadores ascii tipo // --- y comentarios obvios que repiten lo que ya dice el codigo deja solo godoc conciso en tipos o funciones exportadas y comentarios que expliquen un por que no obvio

estandarizacion de tests
10 saca la logica condicional de los tests como el switch dentro de TestPartitionedBusRetriesAndDLQ separandolo en subtests t.Run independientes dividi los tests que mezclan muchos componentes como TestEventStoreQueriesAndAuxStores y los escenarios muy largos en subtests claros usa formato consistente arrange act assert y helpers de setup que validen errores en vez de ignorarlos

al terminar corre go vet ./... go test -race -cover ./... y go run ./cmd/demo para validar que todo pasa limpio

## [Assistant]
Aplicadas las 10 correcciones sin sumar dependencias externas y manteniendo la arquitectura hexagonal intacta.

### Bugs corregidos

1. **Rollback de Inbox y `seenWebhooks` ante fallo:**
   - Se agregó `UnmarkProcessed(ctx, consumer, eventID)` en [ports/idempotency.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/idempotency.go) e [inbox.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/inbox.go).
   - [wallet/service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go), [saga/orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go) y [gateway/adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go) revierten la marca de inbox vía `defer` cuando el handler retorna error, y `HandleWebhook` revierte `seenWebhooks[providerEventID]` si falla la publicación.
   - Cubierto con tests dedicados de reintento ante fallo transitorio en `wallet`, `saga` y `gateway`.

2. **Cancelación de reintentos y `Drain` sin leaks en `PartitionedBus`:**
   - En [partitioned.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned.go), `deliverWithRetry` usa un `break retryLoop` etiquetado para cortar inmediatamente al cancelarse `runCtx` (y `Close` cancela y espera la salida limpia de workers).
   - `Drain(ctx)` ahora espera sobre un canal `idleCh` cerrado/reemplazado bajo `inFlightMu`, eliminando la goroutine colgada ante `ctx.Done()`.

3. **`CreatedAt` preservado y jitter en `mutatePayment`:**
   - El aggregate [payment.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment.go) persiste `createdAt` desde `EventPaymentInitiated` (expuesto vía `CreatedAt()`) y `toPaymentView` lo preserva en todas las transiciones.
   - `mutatePayment` en [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go) aplica backoff con jitter entre reintentos de `ErrVersionConflict`.

4. **Correlación de logs y *poison pills* directo a DLQ:**
   - `observability.WithCorrelation` y `observability.LoggerFromContext` conectados en `wallet`, `saga`, `gateway` y `eventbus`.
   - `ports.NonRetryable` / `ports.IsNonRetryable` en [ports/errors.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/errors.go) envolviendo todos los errores de `events.DecodePayload` en `wallet`, `saga`, `gateway`, `readmodel` y `observability` para rutear mensajes corruptos a DLQ en el primer intento.

### Organización, nombres, comentarios y tests

5. **División de stores y proyecciones:**
   - `internal/infrastructure/eventstore/` dividido en [eventstore.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/eventstore.go), [snapshot.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/snapshot.go), [inbox.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/inbox.go) e [idempotency.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/idempotency.go).
   - `internal/infrastructure/readmodel/` dividido en [balance.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/balance.go), [transaction.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/transaction.go), [payment.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/payment.go) y [projector.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/projector.go).
6. **Eliminación de duplicación:** helper `publishCommand` unificando los 7 envíos en [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go) y `applyWalletMovement` en [projector.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/projector.go).
7. **Demo limpia:** [cmd/demo/main.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/cmd/demo/main.go) modularizado en `runScenario1HappyPath` a `runScenario5SystemRecovery` + `printObservabilitySummary`, usando helpers `must`/`mustValue` sin silenciar errores.
8. **Nombres y comentarios:** reemplazadas abreviaturas crípticas (`d`, `se`, `cp`, `pv1`, `bal1`, `snaps`, `idemp`, `whBody`, etc.) y removidos separadores ASCII y comentarios redundantes.
9. **Tests estandarizados:** sin `switch` ni lógica condicional dentro de handlers de test, divididos en `t.Run` independientes ([eventstore_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventstore/eventstore_test.go), [partitioned_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned_test.go), [readmodel_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/readmodel/readmodel_test.go), [gateway_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/gateway_test.go), [service_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service_test.go), [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go)) con helpers que validan cada error.

### Verificación final

`go vet ./... && go test -count=1 -race -cover ./... && go run ./cmd/demo` pasó limpio (0 warnings, 0 data races):
- `internal/domain/events`: **85.0%** | `internal/domain/payment`: **83.5%** | `internal/domain/wallet`: **82.8%**
- `internal/application/saga`: **73.7%** | `internal/application/wallet`: **67.3%**
- `internal/infrastructure/eventbus`: **89.9%** | `internal/infrastructure/eventstore`: **84.0%** | `internal/infrastructure/readmodel`: **65.3%** | `internal/infrastructure/gateway`: **63.8%**
- `internal/observability`: **77.4%**
- `go run ./cmd/demo`: los 5 escenarios, `/healthz`, `/readyz` y métricas ejecutados correctamente.

## [User]
todo casi listo encontre solo dos detalles chicos en los cambios nuevos antes de commitear

1 en TestWalletServiceInboxRollbackAllowsBusRetry y TestSagaInboxRollbackAllowsBusRetry como el error transitorio se inyecta en el publisher despues de store.Append el intento 1 ya persistio el cambio y el intento 2 termina en no-op por idempotencia sin publicar el evento o comando saliente de hecho flakyPublisher.published queda en 0 cambia esos dos tests para inyectar el fallo transitorio en un EventStore wrapper que falle el primer Append y valida en el assert que el evento o comando saliente se publico correctamente en el reintento
2 en internal/infrastructure/eventbus/partitioned.go dentro de deliverWithRetry cuando dispara b.runCtx.Done() hace return en vez de break retryLoop para no mandar a la DLQ mensajes interrumpidos por un shutdown del bus

corre go test -race ./... para confirmar

## [Assistant]
Listos ambos ajustes y verificados:

1. **Fallo transitorio inyectado en `EventStore.Append` + validación del mensaje saliente en reintento:**
   - [wallet/service_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service_test.go): `TestWalletServiceInboxRollbackAllowsBusRetry` usa `failingAppendStore` para fallar el primer `Append` y verifica explícitamente que el reintento publica `FundsReserved` en `recordingPublisher`.
   - [saga/scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go): `TestSagaInboxRollbackAllowsBusRetry` usa `failingAppendStore` para fallar el primer `Append` y verifica mediante un suscriptor en `gateway.commands.v1` que el reintento emite exactamente 1 comando `SubmitPayment`.

2. **Shutdown limpio sin contaminar DLQ:**
   - [partitioned.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned.go): `deliverWithRetry` retorna inmediatamente (`return`) cuando dispara `<-b.runCtx.Done()`, evitando enviar a la DLQ mensajes interrumpidos por cierre del bus.
   - [partitioned_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned_test.go): `TestPartitionedBusCloseCancelsPendingRetryImmediately` ahora verifica además `len(bus.DeadLetters(...)) == 0`.

`go test -count=1 -race ./...` pasó en todos los paquetes sin race conditions.

## [User]
pase el codigo por una review independiente y salieron varios bugs reales de consistencia financiera concurrencia y limites hexagonales que quiero corregir ahora con cambios quirurgicos y sin meter sobreingenieria ni dependencias externas

aplica exactamente estos puntos

1 reembolsos parciales concurrentes C1 hoy RequestRefund valida contra una lectura puntual sin reservar el monto en vuelo y Wallet.Refund no valida contra lo capturado asi que dos reembolsos parciales concurrentes pueden duplicar plata arreglalo en dos lados en Wallet.Refund exigiendo que paymentID exista en captured y trackeando refundedByPayment en Wallet y Snapshot para garantizar refundedByPayment + amount <= captured ajustando wallet_test.go y en Payment reservando el monto de reembolso en vuelo con optimistic locking al iniciar RequestRefund y liberandolo si llega GatewayRefundDeclined
2 fallo entre Append y Publish C2 sin armar un worker complejo de outbox en memoria aplica la solucion simple cuando un reintento de comando en WalletService o un evento en PaymentOrchestrator detecte que el efecto ya esta persistido en el stream no hagas un no-op silencioso re-sincroniza la vista y republica el evento o comando saliente para que un fallo en Publish o un RecoverActiveSagas sobre PENDING con reserva ya guardada nunca deje una saga trabada
3 resiliencia en reembolsos M1 en GatewayAdapter.handleRefundPayment aplica bulkhead circuit breaker y el mismo loop de reintentos con RefundID como clave de idempotencia distinguiendo timeout o error transitorio de un rechazo explicito del proveedor Approved == false con err == nil para no convertir un timeout en rechazo definitivo
4 cierre limpio en PartitionedBus M2 en PartitionedBus vincula el contexto de los mensajes al ciclo de vida del bus runCtx manteniendo los valores de contexto para que Close(ctx) corte handlers o reintentos en vuelo y respete el deadline de ctx sin bloquearse indefinidamente
5 idempotency key trabada ante cancelacion M3 en InitiatePayment usa context.WithoutCancel(ctx) al llamar idempotency.Complete o Delete despues de iniciar el pago para que si el cliente corta el request durante la espera de 2s la clave no quede clavada 24h en IN_PROGRESS
6 eliminar mapa volatil approvedRefs M4 transporta ProviderRef en CaptureFundsCommand y FundsCapturedPayload y borra el mapa approvedRefs y su mutex en PaymentOrchestrator para que no se pierda ante reinicios
7 deduplicacion en Metrics M5 deduplica eventos por event_id en observability.Collector.HandleEvent para que las re-entregas at-least-once no inflen contadores ni volumen
8 limites hexagonales DIP e ISP M6 move WithCorrelation y LoggerFromContext de internal/observability a internal/application/ports para que application no importe observability y deja en ports.MetricsRecorder solo los metodos que realmente se usan desde afuera de Collector
9 hardening menor y docs B1 B2 B4 agrega guardia IsTerminal() en EventGatewayPaymentApproved marca errores permanentes de dominio como ports.NonRetryable en los handlers loguea con Warn los errores de syncPaymentView en vez de descartarlos con guion bajo evita perder el reloj monotonico al medir duracion en adapter.go valida overflow int64 en Wallet.Deposit y Wallet.Refund baja go.mod a go 1.22 y actualiza docs/02-service-design.md agregando internal/application/wallet y corrigiendo GatewayClient por PaymentGateway y el README.md
10 tests faltantes B3 agrega tests concretos para a dos RequestRefund parciales concurrentes que no puedan superar lo capturado b fallo transitorio en Publisher.Publish despues de EventStore.Append mas recovery en Escenario 5 de un pago en PENDING con reserva ya persistida en Wallet c vencimiento de SyncReserveWait devolviendo 202 PENDING cancelacion de ctx en InitiatePayment sin trabar Idempotency-Key y casos de rechazo o error en handleRefundPayment y handleCheckStatus

al terminar corre go vet ./... go test -race -cover ./... y go run ./cmd/demo para validar que todo pasa limpio

## [Assistant]
Aplicados los 10 puntos de la review y validados de punta a punta.

### Resumen de correcciones

1. **Reembolsos parciales concurrentes (C1):**
   - [wallet/wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go) y [wallet/errors.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/errors.go): `Wallet.Refund` exige que `paymentID` exista en `captured` (`ErrPaymentNotCaptured`) y trackea `refundedByPayment` (también en `Snapshot`) garantizando `refundedByPayment + amount <= captured` (`ErrRefundExceedsCaptured`).
   - [payment/payment.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment.go) y [saga/orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): `RequestRefund` reserva el monto en vuelo vía `EventPaymentRefundRequested` con optimistic locking y `EventGatewayRefundDeclined` lo libera con `EventPaymentRefundDeclined`.

2. **Fallo entre `Append` y `Publish` sin no-op silencioso (C2):**
   - [wallet/service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go) y [saga/orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): ante un reintento o recovery donde el efecto ya existe en el stream (`len(uncommitted) == 0`), se re-sincroniza la vista y se republica el evento/comando persistido (`republishPersistedEvent` / `republishPersistedPaymentEvent` + `SubmitPayment`).

3. **Resiliencia en reembolsos (M1):**
   - [gateway/adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go): `handleRefundPayment` aplica bulkhead, circuit breaker y reintentos con *full jitter* usando `RefundID` como clave de idempotencia; un rechazo explícito (`Approved == false, err == nil`) emite `GatewayRefundDeclined` sin abrir el circuito, mientras que un timeout/error transitorio reintenta y retorna error sin emitir un rechazo falso.

4. **Cierre limpio de `PartitionedBus` (M2):**
   - [eventbus/partitioned.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned.go): `lifecycleContext` enlaza el contexto de cada mensaje a `b.runCtx` conservando valores del caller; `Close(ctx)` cancela `runCtx` y retorna `ctx.Err()` respetando el deadline sin bloquearse indefinidamente.

5. **Idempotency-Key segura ante cancelación del cliente (M3):**
   - [saga/orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): `idempotency.Complete` y `Delete` usan `context.WithoutCancel(ctx)` para evitar que una desconexión durante los 2s de espera trabe la clave en `IN_PROGRESS`.

6. **Eliminado mapa volátil `approvedRefs` (M4):**
   - [events/catalog.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/catalog.go), [wallet/wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go) y [saga/orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): `ProviderRef` viaja en `CaptureFundsCommand` y `FundsCapturedPayload`.

7. **Deduplicación en `Metrics` (M5):**
   - [observability/metrics.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/observability/metrics.go): `Collector.HandleEvent` deduplica por `env.EventID` para que re-entregas *at-least-once* no inflen contadores ni volumen.

8. **Límites hexagonales DIP e ISP (M6):**
   - [ports/observability.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/ports/observability.go): `WithCorrelation` y `LoggerFromContext` movidos a `ports`, `MetricsRecorder` reducido a los 4 métodos usados externamente, y verificado con `go list` que ningún paquete de `internal/application` importa `internal/observability` ni `internal/infrastructure`.

9. **Hardening menor y docs (B1, B2, B4):**
   - Guardia `IsTerminal()` en `EventGatewayPaymentApproved`, errores permanentes de dominio envueltos con `ports.NonRetryable`, logs `Warn` para errores de `syncPaymentView`, medición monotónica (`time.Now()` / `time.Since`) en `adapter.go`, protección contra overflow `int64` en `Wallet.Deposit` y `Wallet.Refund`, [`go.mod`](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/go.mod) en `go 1.22`, y actualizados [02-service-design.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/docs/02-service-design.md) y [README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md).

10. **Tests agregados (B3):**
    - Reembolsos parciales concurrentes y liberación tras rechazo de pasarela en [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go) y [payment_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment_test.go).
    - Fallo transitorio en `Publish` tras `Append` en [wallet/service_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service_test.go) y [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go), más recovery de `PENDING` con reserva ya persistida en Escenario 5.
    - Expiración de `SyncReserveWait` (`202 PENDING`), cancelación de `ctx` en `InitiatePayment` sin trabar `Idempotency-Key`, y todos los casos de `handleRefundPayment`, `handleCheckStatus` y `Close(ctx)` en [gateway_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/gateway_test.go) y [partitioned_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/eventbus/partitioned_test.go).

### Verificación

`go vet ./... && go test -count=1 -race -cover ./... && go run ./cmd/demo` pasó limpio (0 warnings, 0 data races):
- `internal/domain/events`: **85.0%** | `internal/domain/wallet`: **83.3%** | `internal/domain/payment`: **81.7%**
- `internal/application/saga`: **74.4%** | `internal/application/wallet`: **63.4%**
- `internal/infrastructure/eventbus`: **88.7%** | `internal/infrastructure/eventstore`: **84.0%** | `internal/infrastructure/gateway`: **79.5%** | `internal/infrastructure/readmodel`: **65.3%**
- `internal/observability`: **75.8%**
- `go run ./cmd/demo`: los 5 escenarios, `/healthz`, `/readyz` y métricas ejecutados correctamente.

## [User]
retoque documental para dejar todo 1 a 1 antes de mergear
1 agrega las dos filas de PaymentRefundRequested y PaymentRefundDeclined en la tabla de eventos de Payment seccion 3.3 de docs/03-event-specification.md
2 actualiza la seccion Uso de IA al final de README.md para aclarar que el historial en ai-history/ cubre diseño arquitectonico implementacion en go y revision final

## [Assistant]
Listos ambos retoques documentales:

1. [03-event-specification.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/docs/03-event-specification.md#L98-L102): agregadas las filas `PaymentRefundRequested` y `PaymentRefundDeclined` en la tabla de eventos de `Payment` (sección 3.3).
2. [README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md#L90-L92): actualizada la sección **Uso de IA** indicando que el historial en `ai-history/` cubre el diseño arquitectónico, la implementación en Go y la revisión final.

## [User]
hicimos una segunda review cruzada con otro modelo y salto un set chico de bugs reales confirmados con PoC mas un tema de gofmt y claridad en el readme apliquemos solo estos fixes quirurgicos sin tocar la arquitectura general ni hacer refactors esteticos

1 C1 proteccion contra NotSent tardio en PaymentOrchestrator.HandleGatewayEvent para EventGatewaySubmissionNotSent libera fondos unicamente si loaded.Status() == payment.StatusFundsReserved en cualquier otro estado ignoralo ademas en gateway.Adapter registra los payment_id que ya enviaron al menos un intento en un mapa protegido con mutex attemptedPayments para que un reintento posterior con breaker abierto o bulkhead lleno nunca emita GatewaySubmissionNotSent sino GatewayOutcomeUnknown
2 C2 proteccion en handleRefundPayment en gateway.Adapter registra los refund_id intentados en attemptedRefunds y si el breaker esta abierto el bulkhead saturado o ya hubo un intento previo devolve un error reintentable en lugar de emitir GatewayRefundDeclined solo emite GatewayRefundDeclined ante un rechazo explicito del proveedor result.Approved == false con err == nil
3 M1 evitar reserva huerfana en Wallet registra los payment_id rechazados en rejectedPayments incluido en Snapshot para que un ReserveFunds tardio o duplicado vuelva a emitir el rechazo y en PaymentOrchestrator.HandleWalletEvent si llega EventFundsReserved para un pago que ya esta en REJECTED o FAILED emite ReleaseFunds como compensacion defensiva
4 M2 Idempotency-Key en InitiatePayment si store.Append ya persistio PaymentInitiated pero despues falla publishPaymentEvents o publishCommand completa igualmente la clave de idempotencia con status PENDING y HTTPStatus 202 usando context.WithoutCancel(ctx) antes de retornar el error para que no quede trabada 24h en IN_PROGRESS
5 tests de regresion agrega 3 tests concisos cubriendo exactamente C1 pago SUBMITTED que recibe SubmitPayment duplicado con breaker abierto no libera fondos C2 reembolso con timeout que en retry encuentra breaker abierto no libera pendingRefunds ni emite Declined y M1 ReserveFunds duplicado tras rechazo no deja fondos retenidos huerfanos
6 M7 corre gofmt -w . y verifica que gofmt -l . salga vacio
7 M8 README.md agrega una seccion corta y transparente llamada Implementado en Parte 2 vs Diseño de Produccion aclarando las simplificaciones conscientes del alcance de 4 horas sin servidor HTTP publish-after-append con republish-on-retry en lugar de outbox worker SQL inbox en memoria con rollback conciliacion disparada explicitamente sin ticker de fondo particiones compartidas en el bus en memoria y recovery enfocado en pagos mas una tabla breve mapeando los 5 escenarios del enunciado a los tests de scenarios_test.go

al terminar corre gofmt -l . go vet ./... go test -race -count=3 ./... y go run ./cmd/demo para validar que todo pasa limpio

## [Assistant]
Primero leo el README actual para ubicar bien la nueva sección. En paralelo aplico los fixes de código, que no dependen del README.

Fixes de código aplicados. Ahora agrego la sección del README y corro gofmt y toda la validación.

Apliqué los 7 puntos sin tocar la arquitectura, y toda la validación pasa limpia: `gofmt -l .` sale vacío, `go vet` sin warnings, `go test -race -count=3 ./...` pasa en todos los paquetes y `go run ./cmd/demo` corre los 5 escenarios.

**Bugs corregidos:**

- **C1, NotSent tardío** ([orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go), [adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go)):
  - La saga libera fondos ante `GatewaySubmissionNotSent` solo si el pago está en `FUNDS_RESERVED`; en cualquier otro estado lo ignora.
  - El adapter guarda en `attemptedPayments` (con mutex) los pagos que ya se enviaron al menos una vez. Si un reintento choca con el breaker abierto o el bulkhead lleno, emite `GatewayOutcomeUnknown` en vez de `NotSent`.
- **C2, reembolsos** ([adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go)):
  - Agregué `attemptedRefunds`. Con breaker abierto o bulkhead saturado, `handleRefundPayment` devuelve un error reintentable.
  - `GatewayRefundDeclined` sale solo ante un rechazo explícito del proveedor (`Approved == false` con `err == nil`).
- **M1, reserva huérfana:**
  - [wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go) registra `rejectedPayments`, también en el `Snapshot`. Un `ReserveFunds` tardío o duplicado ya no vuelve a evaluar contra un saldo nuevo; [service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go) republica el rechazo original.
  - La saga emite `ReleaseFunds` como compensación si llega `FundsReserved` para un pago `REJECTED` o `FAILED`.
- **M2, Idempotency-Key:** una vez guardado `PaymentInitiated`, si después falla `publishPaymentEvents` o `publishCommand`, la clave se completa igual como `202 PENDING` con `context.WithoutCancel` antes de devolver el error. M2 no tiene test propio porque pediste exactamente 3.

**Tests:**

- Los 3 de regresión están al final de [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go):
  - `TestRegressionDuplicateSubmitWithOpenBreakerDoesNotReleaseSubmittedFunds` (C1)
  - `TestRegressionRefundTimeoutThenOpenBreakerKeepsPendingRefund` (C2)
  - `TestRegressionLateReserveAfterRejectionLeavesNoOrphanHold` (M1), con dos subtests: el duplicado tras un rechazo y la compensación defensiva.
- **Cambio de comportamiento a revisar:** actualicé un subtest de [gateway_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/gateway_test.go). Antes esperaba que un reembolso con breaker abierto terminara en `GatewayRefundDeclined`; ahora espera un error reintentable con `ErrCircuitOpen`.

**gofmt y README:**

- Corrí `gofmt -w .`.
- [README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md) tiene la nueva sección "Implementado en Parte 2 vs Diseño de Producción", con las simplificaciones que pediste y la tabla que mapea los 5 escenarios a sus tests.

## [User]
la segunda pasada de verificacion confirmo que C1 C2 M1 y M2 quedaron cerrados sin regresiones ni races pero detecto un caso residual de C1 tras reinicio y tres aclaraciones menores para el readme apliquemos este micro-patch quirurgico

1 residual C1 tras reinicio agrega el campo Resubmission bool con tag json:"resubmission,omitempty" en events.SubmitPaymentCommand setea Resubmission en true cuando RecoverActiveSagas re-emite SubmitPayment para un pago en FUNDS_RESERVED y en gateway.Adapter trata cmd.Resubmission || paymentWasAttempted como posiblemente enviado para que si encuentra el breaker abierto o el bulkhead saturado nunca emita GatewaySubmissionNotSent sino GatewayOutcomeUnknown dejando que la conciliacion resuelva
2 detalle menor de M2 en InitiatePayment si store.Append ya persistio PaymentInitiated y despues falla publishPaymentEvents o publishCommand ademas de completar la Idempotency-Key con 202 PENDING loguea un Warn y devolve result, nil en vez de error para que el primer caller reciba el mismo 202 PENDING que un reintento ya que el pago existe y la recovery lo retoma
3 test de regresion agrega un test en scenarios_test.go que reproduzca exactamente ese escenario residual pago en FUNDS_RESERVED tras reinicio donde RecoverActiveSagas re-emite SubmitPayment con el breaker abierto verificando que termina en GATEWAY_UNKNOWN con los fondos retenidos y no en FAILED
4 readme agrega estas tres aclaraciones cortas a una nota bajo la tabla Decisiones tecnicas clave indicando que refleja el diseño objetivo y remitiendo a la seccion Implementado vs Diseño para lo simulado b aclarar explicitamente en M3 que los reembolsos inciertos quedan reservados en DLQ para redrive manual y que RecoverActiveSagas no retoma reembolsos c agregar una fila aclarando que attemptedPayments attemptedRefunds y seenWebhooks del adapter viven en memoria y en produccion se persisten en gateway_attempts y gateway_webhooks

al terminar corre gofmt -l . go vet ./... go test -race -count=3 ./... y go run ./cmd/demo para validar que todo pasa limpio

## [Assistant]
Aplico los cuatro puntos en paralelo y después valido todo en un solo comando.

Apliqué los 4 puntos del micro-patch y todo pasa limpio: `gofmt -l .` sale vacío, `go vet` sin warnings, `go test -race -count=3 ./...` pasa en todos los paquetes y `go run ./cmd/demo` corre los 5 escenarios. El test nuevo pasa también corrido aislado.

1. **C1 tras reinicio:**
   - `SubmitPaymentCommand` en [catalog.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/catalog.go) tiene el campo nuevo `Resubmission bool` (`json:"resubmission,omitempty"`).
   - `RecoverActiveSagas` en [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go) lo pone en `true` al re-emitir `SubmitPayment` para un pago en `FUNDS_RESERVED`.
   - En [adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go), si el envío pudo haber salido (`cmd.Resubmission || paymentWasAttempted`) y encuentra el breaker abierto o el bulkhead lleno, emite `GatewayOutcomeUnknown` y nunca `GatewaySubmissionNotSent`.

2. **M2:** si `PaymentInitiated` ya está guardado y después falla `publishPaymentEvents` o `publishCommand`, `InitiatePayment` loguea un `Warn`, completa la Idempotency-Key con `202 PENDING` y devuelve `result, nil`. Así el primer caller recibe lo mismo que un reintento. Este cambio no tiene test propio.

3. **Test de regresión:** agregué `TestRegressionRecoveryResubmitWithOpenBreakerEndsGatewayUnknown` en [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go). Simula la caída con un pago en `FUNDS_RESERVED` y levanta un adapter nuevo, sin memoria de intentos, con el breaker abierto. Comprueba que:
   - no hay llamada a la pasarela,
   - el pago termina en `GATEWAY_UNKNOWN` y no en `FAILED`,
   - los 6000 quedan retenidos.

4. **README** ([README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md)):
   - Nota bajo "Decisiones técnicas clave": la tabla describe el diseño objetivo, con link a la sección "Implementado en Parte 2 vs Diseño de Producción".
   - Fila sobre reembolsos inciertos: el monto queda reservado y el comando va a la DLQ para redrive manual; `RecoverActiveSagas` no retoma reembolsos.
   - Fila sobre `attemptedPayments`, `attemptedRefunds` y `seenWebhooks`: viven en memoria y en producción irían a `gateway_attempts` y `gateway_webhooks`.
   - Además completé la fila de recovery con el uso de `Resubmission=true`.

## [User]
pasamos una auditoria final de negocio solid y casos borde y detecto un ultimo set chico de fixes quirurgicos de muy alto impacto vamos a aplicar solo estos y cerramos el codigo definitivamente sin hacer refactors estructurales

1 N1 regla de oro en redelivery en PaymentOrchestrator.HandleWalletEvent caso EventFundsReserved setea Resubmission: !transitioned en SubmitPaymentCommand para que un redelivery post-restart nunca pueda disparar NotSent
2 N2 y N3 evidencia contradictoria en HandleGatewayEvent si llega EventGatewayPaymentApproved para un pago en StatusFailed o EventGatewayPaymentDeclined para un pago en StatusCompleted devolve ports.NonRetryable(...) para que caiga en DLQ con alerta en vez de ignorarlo con return nil ademas en EventGatewayPaymentDeclined si loaded.ProviderRef() != "" y payload.Reason == string(ports.GatewayOutcomeNotFound) no liberes fondos pasalo o mantenelo en GATEWAY_UNKNOWN para revision manual
3 N4 ownership de billetera agrega UserID string en ReserveFundsCommand y pasalo desde PaymentOrchestrator en Wallet.Reserve valida que si userID != "" && userID != w.userID emita FundsReservationRejected con reason UNAUTHORIZED_WALLET
4 N5 N6 N8 E4 y E5 endurecimiento de entrada a en InitiatePayment exige que al menos req.IdempotencyKey != "" o req.PaymentID != "" b en gateway.Adapter.HandleWebhook hacelo fail-closed si a.webhookSecret == "" retorna error en vez de saltear HMAC y ajusta cualquier test que no pasara secret c marca ports.ErrStreamNotFound como non-retryable en wrapPermanentDomainError de wallet service d en hashInitiateRequest hashea un struct con json.Marshal y currency normalizada con TrimSpace en vez de concatenar con pipe e usa strings.TrimSpace al validar IDs requeridos en Payment.Initiate y Wallet.Create
5 E1 E2 E3 y E7 bordes de concurrencia y recovery a en InitiatePayment usa cleanupCtx para publishPaymentEvents y publishCommand posteriores al Append b en RecoverActiveSagas si falla un stream acumula el error con errors.Join y segui recuperando el resto de las sagas c en CircuitBreaker ejecuta cfg.OnStateChange fuera de cb.mu para evitar deadlocks o IO bajo lock d en HandleGatewayEvent caso EventGatewayOutcomeUnknown agrega payment.StatusManualReview a la guarda para ignorar eventos tardios
6 T1 y T2 red de seguridad en tests en newTestSystem de scenarios_test.go agrega un t.Cleanup que verifique 1 que las DLQ de todos los topics esten vacias salvo que el test marque un flag expectDLQ y 2 el invariante global del ledger reconstruido desde store.LoadAll balance == depositos - capturas + reembolsos held == reservas activas available >= 0 y consistencia entre pagos COMPLETED/FAILED y Wallet agrega tambien tests de regresion cortos para N1 redelivery de FundsReserved con breaker abierto N2+N3 NOT_FOUND con provider_ref y webhook APPROVED tardio sobre FAILED y N4 intento de pago de otro usuario sobre billetera ajena
7 README.md suma en la tabla Implementado en Parte 2 vs Diseño de Produccion las notas breves de trade-offs conscientes shared kernel en domain/events breaker unico en la simulacion vs por proveedor en produccion carrera Declined vs Approved resuelta en Wallet sin estado intermedio ReleaseRequested y mapas de dedupe en memoria sin cota

al terminar corre gofmt -w . && gofmt -l . && go vet ./... && go test -race -count=3 ./... && go run ./cmd/demo para verificar que todo compila y pasa limpio

## [Assistant]
Aplicados los 7 fixes finales de la auditoría y validado todo punta a punta sin cambios estructurales ni dependencias externas.

### Resumen de cambios

1. **N1 — Regla de oro en redelivery:**
   - [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): `HandleWalletEvent` (`EventFundsReserved`) setea `Resubmission: !transitioned` en `SubmitPaymentCommand`, evitando que un redelivery post-restart con breaker abierto o bulkhead lleno pueda emitir `GatewaySubmissionNotSent`.

2. **N2 y N3 — Evidencia contradictoria y `NOT_FOUND` con `provider_ref`:**
   - [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go):
     - `EventGatewayPaymentApproved` sobre un pago `FAILED` y `EventGatewayPaymentDeclined` sobre un pago `COMPLETED`/`REFUNDED` retornan `ports.NonRetryable(...)` directo a DLQ con alerta en vez de ignorarse silenciosamente.
     - `EventGatewayPaymentDeclined` con `Reason == NOT_FOUND` cuando `loaded.ProviderRef() != ""` nunca libera fondos: pasa o mantiene el pago en `GATEWAY_UNKNOWN` para conciliación/revisión manual.

3. **N4 — Ownership de billetera:**
   - [catalog.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/events/catalog.go), [wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go), [wallet/service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go), [orchestrator.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/orchestrator.go): agregado `UserID` en `ReserveFundsCommand` y `ReasonUnauthorizedWallet`; `Wallet.Reserve` valida `userID != "" && userID != w.userID` y emite `FundsReservationRejected (UNAUTHORIZED_WALLET)`.

4. **N5, N6, N8, E4 y E5 — Endurecimiento de entradas:**
   - `InitiatePayment` exige `IdempotencyKey != ""` o `PaymentID != ""` (con `strings.TrimSpace`).
   - [adapter.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/adapter.go): `HandleWebhook` es *fail-closed* y devuelve `ErrWebhookSecretNotConfigured` si `webhookSecret` está vacío.
   - [wallet/service.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/wallet/service.go): `ports.ErrStreamNotFound` marcado como `NonRetryable`.
   - `hashInitiateRequest` hashea un struct canónico serializado con `json.Marshal` y moneda normalizada con `TrimSpace`.
   - [payment.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/payment/payment.go) y [wallet.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/domain/wallet/wallet.go): `strings.TrimSpace` en la validación de IDs requeridos.

5. **E1, E2, E3 y E7 — Concurrencia y recovery:**
   - `InitiatePayment` usa `cleanupCtx` (`context.WithoutCancel`) para `publishPaymentEvents` y `publishCommand` tras `Append`.
   - `RecoverActiveSagas` acumula errores por stream con `errors.Join` y continúa recuperando el resto de las sagas.
   - [circuitbreaker.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/circuitbreaker.go): `OnStateChange` se invoca fuera de `cb.mu`.
   - `EventGatewayOutcomeUnknown` incluye `payment.StatusManualReview` en la guarda para ignorar eventos tardíos.

6. **T1 y T2 — Red de seguridad y tests de regresión:**
   - [scenarios_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/application/saga/scenarios_test.go): `t.Cleanup` en `newTestSystem` valida DLQ vacías en todos los topics (salvo `expectDLQ = true`) e invariante contable global del ledger reconstruido desde `store.LoadAll` + consistencia cruzada `Payment` ↔ `Wallet`.
   - Agregados `TestRegressionRedeliveryFundsReservedWithOpenBreakerEndsGatewayUnknown` (N1), `TestRegressionContradictoryGatewayEventsAndNotFoundWithProviderRef` (N2+N3), `TestRegressionUnauthorizedWalletOwnershipRejected` (N4) y el caso *fail-closed* sin secret en [gateway_test.go](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/internal/infrastructure/gateway/gateway_test.go).

7. **README:**
   - [README.md](file:///Users/besednjak/Desktop/repos/draftea-payments-challenge/README.md): agregadas en **Implementado en Parte 2 vs Diseño de Producción** las notas sobre *shared kernel* en `domain/events`, circuit breaker único en la simulación, resolución atómica en `Wallet` sin estado `ReleaseRequested` y mapas de deduplicación en memoria sin cota.

### Verificación

`gofmt -w . && gofmt -l . && go vet ./... && go test -race -count=3 ./... && go run ./cmd/demo` pasó limpio (0 archivos sin formatear, 0 warnings, 0 data races en las 3 corridas completas y demo ejecutando los 5 escenarios).
