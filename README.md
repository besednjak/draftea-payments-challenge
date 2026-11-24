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

## Uso de IA

El diseño arquitectónico se trabajó iterativamente con un asistente de IA, debatiendo trade-offs antes de documentar. Siguiendo las indicaciones del enunciado, el historial de conversación se guarda en [`ai-history/`](ai-history/).
