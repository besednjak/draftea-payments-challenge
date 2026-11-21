# 04 — Recomendación de Stack Tecnológico

## 1. Corredor de mensajes: Apache Kafka

| Criterio | Apache Kafka (**Elegido**) | RabbitMQ |
|---|---|---|
| **Orden de eventos** | Garantizado por partición usando clave (`wallet_id` / `payment_id`) | Se pierde cuando múltiples consumidores compiten por la misma cola |
| **Replay de eventos** | Nativo gracias a la retención de log | No es nativo (mensajes desaparecen al ser consumidos) |
| **Throughput y escalabilidad** | Muy alto; escala linealmente con particiones y consumer groups | Menor techo para alto volumen de eventos |
| **Reintentos con delay y DLQ** | Se implementan con topics de reintento y DLQ | Soportados de forma nativa (DLX + TTL) |
| **Operación** | Mayor complejidad (se mitiga usando servicios gestionados: AWS MSK / Confluent) | Más simple de operar |

**Justificación:** en un sistema de pagos, el **orden por billetera** (para serializar operaciones sobre un mismo saldo) y la capacidad de **reprocesar eventos** (para reconstruir métricas o vistas) pesan más que la simplicidad de colas tradicionales. Se utiliza un único broker (Kafka) para no introducir la complejidad operativa de mantener dos sistemas de mensajería.

**Configuración recomendada para flujos financieros:** `acks=all`, `replication.factor=3`, `min.insync.replicas=2`, productor idempotente y commit manual de offsets después de persistir en base de datos.

---

## 2. Bases de datos

### Event Store, Outbox y Read Models: PostgreSQL

Se utiliza **PostgreSQL** con bases/schemas lógicos separados por módulo (`wallet`, `payment`, `gateway`).

**Por qué PostgreSQL:**
- **ACID y concurrencia optimista:** la restricción `UNIQUE (stream_id, version)` garantiza consistencia fuerte al escribir en el Event Store sin bloqueos distribuidos.
- **Transactional Outbox e Inbox atómicos:** permite guardar el evento, el mensaje saliente (`outbox`), el registro de deduplicación (`processed_messages`) y la vista de saldo (`wallet_balances`) en una **única transacción local**.
- **Simplicidad operativa:** tecnología madura, conocida por cualquier equipo backend y con soporte nativo de JSONB y particionamiento.

**Alternativas descartadas:**
- *EventStoreDB:* excelente para Event Sourcing puro, pero suma una base especializada que el equipo tiene que operar aparte de los Read Models.
- *DynamoDB:* buena opción a escala masiva con escrituras condicionales, pero acopla el diseño a un proveedor cloud y limita consultas analíticas.

#### Esquema conceptual principal

| Tabla | Propósito | Campos clave |
|---|---|---|
| `events` | Event Store (*append-only*) | `global_seq`, `event_id`, `stream_id`, `version`, `type`, `payload (jsonb)`, `occurred_at` — `UNIQUE(stream_id, version)` |
| `snapshots` | Acelerar rehidratación de `Wallet` | `stream_id`, `version`, `state (jsonb)` |
| `outbox` | Publicación confiable a Kafka | `id`, `topic`, `partition_key`, `payload`, `published_at` |
| `processed_messages` | Inbox de idempotencia | `consumer`, `event_id`, `processed_at` |
| `wallet_balances` | Read model sincrónico de saldo | `wallet_id`, `balance`, `held`, `available`, `version` |
| `wallet_transactions` | Historial asincrónico de movimientos | `event_id`, `wallet_id`, `payment_id`, `type`, `amount`, `occurred_at` |
| `payment_views` | Read model del estado de pagos | `payment_id`, `user_id`, `status`, `amount`, `refunded_total`, `version` |
| `idempotency_keys` | Deduplicación de API HTTP | `user_id`, `idempotency_key`, `request_hash`, `response`, `expires_at` |
| `gateway_attempts` / `gateway_webhooks` | Auditoría y deduplicación en Gateway Adapter | `payment_id`, `provider_event_id`, `outcome`, `raw_payload` |

### Métricas de negocio: TimescaleDB

- Extensión de series de tiempo sobre PostgreSQL: mantiene el stack homogéneo, permite agregaciones por ventana temporal (tasas de éxito por minuto/hora, latencia p95/p99, volumen por proveedor) y escala bien para el volumen esperado.
- Si el volumen analítico creciera a escala masiva, el consumidor de `Metrics` puede migrar su destino a **ClickHouse** sin afectar el resto del sistema.

---

## 3. Caché: Redis (opcional)

- **Dónde aporta valor:**
  - Resolución rápida de `Idempotency-Key` en la API (evitando golpear PostgreSQL en reintentos inmediatos).
  - *Rate limiting* por usuario o IP en el borde de la API.
  - Caché de lecturas frecuentes de `GET /v1/payments/{id}` para pagos ya finalizados (inmutables).
- **Dónde NO se usa:** **nunca** como fuente de verdad para decidir si un usuario tiene saldo suficiente. Toda validación de fondos ocurre contra el aggregate de `Wallet` en PostgreSQL.

---

## 4. Lenguaje y observabilidad

| Componente | Tecnología | Rol |
|---|---|---|
| **Lenguaje** | **Go** | Concurrencia nativa liviana, excelente performance, tipado fuerte y ecosistema sólido para Kafka y PostgreSQL |
| **Logging** | `log/slog` (JSON estructurado) | Correlación por `payment_id` (`correlation_id`), `wallet_id` y `event_type` |
| **Métricas operativas** | Prometheus + Grafana | Latencia, errores, consumer lag, tamaño de DLQ y estado del circuit breaker |
| **Trazabilidad** | OpenTelemetry | Propagación de contexto de traza a través de headers HTTP y Kafka |
| **Health checks** | `/healthz` y `/readyz` | Liveness del proceso y verificación de dependencias (DB, broker) |
