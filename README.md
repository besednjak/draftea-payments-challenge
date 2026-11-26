# Draftea Payments Challenge — Sistema de Pagos Orientado a Eventos

Diseño e implementación de un sistema de pagos orientado a eventos donde los usuarios pagan servicios usando el saldo de su billetera y la liquidación al proveedor se resuelve de forma asincrónica a través de una pasarela externa, con manejo integral de errores y compensaciones.

| Parte | Estado | Entregables |
|---|---|---|
| **Parte 1: Diseño de arquitectura** | ✅ Completa | Documentación en [`docs/`](docs/) |
| **Parte 2: Implementación** | ✅ Completa | Monolito modular en Go (`cmd/`, `internal/`) |

---

## Documentación de Arquitectura (Parte 1)

Organizada siguiendo los 6 entregables del enunciado:

| # | Documento | Contenido |
|---|---|---|
| 1 | [01-system-architecture.md](docs/01-system-architecture.md) | Supuestos, diagrama de arquitectura de alto nivel, flujo de datos, integraciones externas y los **5 diagramas de secuencia** de los escenarios requeridos |
| 2 | [02-service-design.md](docs/02-service-design.md) | Límites y responsabilidades de los 4 servicios, dependencias, contrato HTTP, consistencia *read-your-writes* y mapeo a la Parte 2 |
| 3 | [03-event-specification.md](docs/03-event-specification.md) | Convenciones, envelope, topics, catálogo de comandos y eventos, **máquina de estados de la saga**, idempotencia y Event Sourcing |
| 4 | [04-tech-stack.md](docs/04-tech-stack.md) | Justificación de Kafka vs. RabbitMQ, PostgreSQL, TimescaleDB, Redis opcional, Go y observabilidad |
| 5 | [05-error-handling.md](docs/05-error-handling.md) | Clasificación de errores, backoff exponencial con jitter, DLQ, circuit breaker, compensaciones y ventana de conciliación |
| 6 | [06-scalability.md](docs/06-scalability.md) | Escalamiento horizontal, balanceo de carga, particionamiento de bases, caché, cuellos de botella y evolución desde monolito modular |

---

## Decisiones técnicas clave

| Decisión | Justificación |
|---|---|
| **4 servicios:** Payment, Wallet, Gateway Adapter, Metrics | Límites claros sin sobre-ingeniería; el Event Store de `Wallet` cumple el rol de ledger contable |
| **Saga orquestada** en `Payment` + **coreografía** para `Metrics` | Estado explícito, compensaciones centralizadas y un dueño claro de los timeouts, manteniendo la analítica desacoplada |
| **Reservar → cobrar / liberar** (`FundsReserved` → `FundsCaptured` / `FundsReleased`) | El dinero en tránsito queda visible y la compensación es una liberación limpia; `FundsRefunded` queda reservado para reembolsos reales posteriores |
| **Timeout de pasarela = estado incierto (`GATEWAY_UNKNOWN`)** | Nunca se liberan fondos sin evidencia negativa; se concilia hasta 24h (configurable por proveedor) antes de pasar a `MANUAL_REVIEW` |
| **Event Sourcing + CQRS** en `Wallet` y `Payment` | Auditoría completa, reconstrucción del estado por replay y vista de saldo actualizada en la misma transacción (*read-your-writes*) |
| **Optimistic locking** (`UNIQUE(stream_id, version)`) | Garantiza saldo no negativo ante pagos concurrentes sin bloqueos distribuidos |
| **Transactional Outbox con polling** | Evita el problema de *dual-write* sin sumar infraestructura de CDC desde el día uno (evolucionable a Debezium) |
| **Kafka + PostgreSQL** | Orden garantizado por clave (`wallet_id` / `payment_id`), replay de eventos y transacciones ACID |
| **At-least-once + idempotencia en 3 niveles** | Clave en API (`Idempotency-Key`), Inbox en consumidores (`event_id`) y clave ante la pasarela (`payment_id`) |
| **Espera de hasta 2s en la API** | Permite devolver `422` inmediato por saldo insuficiente, o `202 Accepted` si demora más |

> **Nota:** esta tabla refleja el **diseño objetivo de producción**. Lo que la Parte 2 simula o simplifica (HTTP, Outbox, Kafka, PostgreSQL) está detallado en [Implementado en Parte 2 vs Diseño de Producción](#implementado-en-parte-2-vs-diseño-de-producción).

---

## Estructura del proyecto (Parte 2)

```
.
├── cmd/
│   └── demo/                  # Composición y ejecución de los escenarios
├── internal/
│   ├── domain/                # Aggregates y contratos (sin dependencias externas)
│   │   ├── events/            # Envelope y definiciones de eventos/comandos
│   │   ├── payment/           # Aggregate Payment y máquina de estados
│   │   └── wallet/            # Aggregate Wallet e invariantes de saldo
│   ├── application/
│   │   ├── ports/             # Interfaces segregadas (EventStore, EventBus, Gateway, ReadModels)
│   │   ├── wallet/            # Servicio de Wallet (comandos, optimistic locking + retry, snapshots)
│   │   └── saga/              # Orquestador de la saga de pagos, conciliación y recovery
│   ├── infrastructure/        # Implementaciones en memoria y mocks
│   │   ├── eventstore/        # Event Store con optimistic locking y snapshots
│   │   ├── eventbus/          # Bus en memoria particionado por clave
│   │   ├── readmodel/         # Proyecciones
│   │   └── gateway/           # Mock de pasarela + circuit breaker
│   └── observability/         # Logging estructurado, métricas y health checks
├── docs/                      # Documentación de arquitectura (Parte 1)
└── ai-history/                # Historial de conversación con IA
```

---

## Cómo ejecutar

**Requisitos:** Go 1.22+ (sin dependencias externas; solo librería estándar).

```bash
# Análisis estático
go vet ./...

# Ejecutar pruebas unitarias e integración con detector de race conditions y cobertura
go test -race -cover ./...

# Ejecutar demostración de los 5 escenarios, métricas y health checks
go run ./cmd/demo

# Opcional: habilitar logs estructurados JSON (slog) durante la demo
DEMO_LOGS=1 go run ./cmd/demo
```

---

## Implementado en Parte 2 vs Diseño de Producción

La Parte 2 se acotó conscientemente al alcance de ~4 horas. Las invariantes financieras (saldo no negativo, nunca liberar sin evidencia negativa, reembolsos acotados a lo capturado) se respetan igual, pero algunas piezas de infraestructura se simplificaron respecto del diseño de [`docs/`](docs/):

| Diseño de producción (Parte 1) | Implementación en Parte 2 |
|---|---|
| API HTTP (`POST /v1/payments`, `GET /healthz`, etc.) | Sin servidor HTTP: los casos de uso se invocan directamente desde `cmd/demo` y los tests (`HTTPStatus` modela la respuesta `202`/`422`) |
| Transactional Outbox con worker de polling | *Publish-after-append* con **republish-on-retry**: si `Publish` falla después de `Append`, el reintento detecta el efecto ya persistido y republica el evento/comando |
| Inbox `processed_messages` en PostgreSQL (misma transacción) | Inbox en memoria con **rollback** (`UnmarkProcessed`) si el handler falla |
| Conciliación periódica (`1m, 5m, 15m…` hasta 24h) | Conciliación disparada explícitamente (`TriggerReconciliation`, `ExpireReconciliationWindow`), sin ticker de fondo |
| Kafka con particiones por topic | Bus en memoria con **particiones compartidas** entre topics (mismo orden FIFO por clave) |
| Recuperación de todos los consumidores por offset | Recovery enfocado en pagos: `RebuildAll` de read models + `RecoverActiveSagas` sobre streams `payment-*` (re-emite `SubmitPayment` con `Resubmission=true`, que nunca se reporta como `NotSent`) |
| Conciliación de reembolsos inciertos con el proveedor | Un reembolso con timeout o breaker abierto mantiene el monto reservado en `Payment` y su comando queda en la DLQ para *redrive* manual; `RecoverActiveSagas` **no** retoma reembolsos |
| Tablas `gateway_attempts` / `gateway_webhooks` en PostgreSQL | `attemptedPayments`, `attemptedRefunds` y `seenWebhooks` del Gateway Adapter viven en memoria y se pierden al reiniciar (por eso la recovery marca `Resubmission=true`) |
| Contratos/schemas versionados por servicio | *Shared kernel* en `internal/domain/events` para evitar mapeo repetitivo en el alcance de 4 horas (`wallet` y `payment` siguen sin importarse entre sí) |
| Un circuit breaker por proveedor externo | Instancia única de `CircuitBreaker` en la simulación (el diseño soporta un mapa por `provider` detrás del mismo puerto) |
| Estado intermedio `ReleaseRequested` ante carrera `Declined` vs `Approved` | La carrera se resuelve atómicamente en el aggregate `Wallet` (una reserva capturada no puede liberarse y viceversa), evitando un estado extra |
| Tablas de deduplicación con TTL/retención | Mapas en memoria sin cota (`InboxStore`, `seenEvents`, `seenWebhooks`, `attempted*`) suficientes para tests y demo |

### Escenarios del enunciado → tests

Todos en [`internal/application/saga/scenarios_test.go`](internal/application/saga/scenarios_test.go):

| # | Escenario | Test |
|---|---|---|
| 1 | Ruta feliz (+ reembolsos) | `TestScenario1_HappyPathWebhookAndRefunds` |
| 2 | Saldo insuficiente | `TestScenario2_InsufficientFundsImmediateRejection` |
| 3 | Timeout de pasarela (conciliación, compensación, circuit breaker) | `TestScenario3_GatewayTimeoutReconciliationAndCircuitBreaker` |
| 4 | Pagos concurrentes sin doble gasto | `TestScenario4_ConcurrentPaymentsNoDoubleSpend` |
| 5 | Recuperación tras reinicio | `TestScenario5_SystemRecoveryRebuildAndResumeActiveSagas` |

---

## Uso de IA

Tanto el diseño arquitectónico como la implementación en Go y la revisión final del código se trabajaron iterativamente con asistentes de IA, debatiendo trade-offs y validando invariantes antes de consolidar cada bloque. Siguiendo las indicaciones del enunciado, el historial completo de conversación (diseño, implementación y revisión) se guarda en [`ai-history/`](ai-history/).
