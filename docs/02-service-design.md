# 02 — Diseño de Servicios

## 1. Límites y responsabilidades

El sistema se divide en **4 servicios** con límites claros de dominio. En la Parte 2 se implementan como módulos independientes dentro de un monolito modular en Go; en producción pueden desplegarse como servicios separados sin cambiar los contratos porque solo se comunican mediante comandos y eventos.

### 1.1 Payment Service

| Aspecto | Definición |
|---|---|
| **Responsabilidad** | Gestionar el ciclo de vida del pago y de los reembolsos. Actúa como **orquestador de la saga**: decide el siguiente paso en función del estado actual y los eventos recibidos. |
| **Estado propio** | Stream de eventos `payment-{payment_id}` (Event Sourcing), tabla de idempotencia de API (`idempotency_keys`) y read model `payment_views`. |
| **Invariantes** | Solo ejecuta transiciones válidas de estado; la suma de reembolsos nunca puede superar el monto capturado (`refunded_total <= captured_amount`). |
| **Publica** | Eventos en `payments.events.v1`; comandos en `wallets.commands.v1` y `gateway.commands.v1`. |
| **Suscribe** | `wallets.events.v1` y `gateway.events.v1`. |
| **No hace** | No calcula ni modifica saldos; no realiza llamadas HTTP a la pasarela externa. |

### 1.2 Wallet Service

| Aspecto | Definición |
|---|---|
| **Responsabilidad** | Fuente de verdad del dinero del usuario. Acredita fondos, reserva saldo, captura pagos confirmados, libera reservas y acredita reembolsos. |
| **Estado propio** | Stream de eventos `wallet-{wallet_id}` (Event Sourcing, que cumple el rol de **ledger** inmutable), snapshots periódicos y read models (`wallet_balances`, `wallet_transactions`). |
| **Invariantes** | `available = balance - held >= 0` en todo momento; una única reserva por `payment_id`; solo se captura o libera una reserva activa. |
| **Publica** | Eventos en `wallets.events.v1`. |
| **Suscribe** | `wallets.commands.v1`. |
| **No hace** | No conoce la pasarela externa ni la lógica de orquestación del pago. |

### 1.3 Gateway Adapter

| Aspecto | Definición |
|---|---|
| **Responsabilidad** | *Anti-Corruption Layer* frente al proveedor externo. Traduce comandos internos a llamadas HTTPS, recibe y valida webhooks (firma HMAC + deduplicación), y protege el sistema con reintentos técnicos, circuit breaker y bulkhead. |
| **Estado propio** | Tablas relacionales simples (`gateway_attempts`, `gateway_webhooks`) + `outbox`. **No usa Event Sourcing**: es infraestructura sin reglas de negocio ni invariantes propias; sus tablas de intentos y webhooks ya registran un historial *append-only*. |
| **Publica** | Eventos normalizados en `gateway.events.v1`. |
| **Suscribe** | `gateway.commands.v1` y webhooks HTTP entrantes del proveedor. |
| **No hace** | No decide liberar ni capturar dinero; solo reporta el resultado técnico del proveedor. |

### 1.4 Metrics Service

| Aspecto | Definición |
|---|---|
| **Responsabilidad** | Analítica en tiempo real: tasas de éxito y rechazo, latencia de punta a punta, volumen procesado, rechazos por saldo insuficiente y tasa de reembolsos. |
| **Estado propio** | Series de tiempo agregadas en TimescaleDB. |
| **Publica** | Ninguno (es un consumidor terminal). Expone consultas para dashboards. |
| **Suscribe** | `payments.events.v1`, `wallets.events.v1`, `gateway.events.v1` (coreografiado). |
| **No hace** | No interviene ni bloquea el flujo de pagos. |

### Qué deliberadamente NO se separó como servicio

- **Orquestador de saga separado:** vive dentro de `Payment`, evitando un salto de red y duplicación de estado.
- **Ledger independiente:** el Event Store inmutable de `Wallet` ya cumple el rol de libro mayor contable.
- **Servicio de usuarios / notificaciones / API Gateway:** son infraestructura o dominios adyacentes fuera del alcance del challenge.

---

## 2. Dependencias y patrones de comunicación

| Origen | Destino | Patrón | Motivo |
|---|---|---|---|
| Cliente → API HTTP | `Payment` | HTTP Sincrónico (`POST /v1/payments`) | Recepción de solicitud con `Idempotency-Key` y respuesta inmediata (`422` o `202`) |
| Cliente → API HTTP | `Wallet` / `Payment` | HTTP Sincrónico (`GET`) | Consultas sobre Read Models (CQRS) |
| `Payment` → `Wallet` | Comandos | Asincrónico (Kafka `wallets.commands.v1`) | Desacoplamiento temporal y serialización por `wallet_id` |
| `Payment` → `Gateway Adapter` | Comandos | Asincrónico (Kafka `gateway.commands.v1`) | Aísla al dominio de la latencia y fallas de la pasarela |
| `Wallet` / `Gateway Adapter` → `Payment` | Eventos | Asincrónico (Kafka `*.events.v1`) | Respuestas de la saga con entrega *at-least-once* |
| Todos → `Metrics` | Eventos (Pub/Sub) | Asincrónico coreografiado | Analítica desacoplada sin impacto en el camino crítico |
| `Gateway Adapter` ↔ Pasarela | HTTPS + Webhooks | Request/Reply + Callback asincrónico | Integración con proveedor externo |

---

## 3. Contrato HTTP principal y consistencia de lectura

| Endpoint | Descripción | Respuestas clave |
|---|---|---|
| `POST /v1/payments` (`Idempotency-Key`) | Inicia el pago de un servicio | `202` con `payment_id`, `status` y `available_balance`; `422` si no hay saldo o si la clave se reusó con otro body; `409` si el mismo request está en curso |
| `GET /v1/payments/{id}` | Estado actual del pago y monto reembolsado | `200` / `404` |
| `POST /v1/payments/{id}/refunds` (`Idempotency-Key`) | Solicita un reembolso total o parcial | `202` / `422` (excede lo capturado) / `409` (estado no reembolsable) |
| `GET /v1/wallets/{id}/balance` | Consulta `balance`, `held` y `available` | `200` |
| `GET /v1/wallets/{id}/transactions` | Historial de movimientos | `200` |
| `POST /v1/webhooks/{provider}` | Recepción de webhooks de la pasarela | `200` / `401` (firma inválida) / `5xx` (fallo temporal para que el proveedor reintente) |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | Observabilidad y operación | `200` / `503` |

### Rechazo inmediato por saldo insuficiente (espera acotada de 2s)

Al recibir `POST /v1/payments`, la API persiste `PaymentInitiated`, despacha `ReserveFunds` y espera hasta **2 segundos** el resultado de la reserva:
- Si llega `FundsReservationRejected` → responde `422 INSUFFICIENT_FUNDS` de inmediato.
- Si llega `FundsReserved` → responde `202 Accepted` incluyendo el nuevo `available_balance`.
- Si pasan los 2 segundos sin respuesta → responde `202 Accepted` con estado `PENDING` y el cliente consulta luego por `GET /v1/payments/{id}`.

### Read-your-writes en CQRS

Si una proyección se actualiza de forma asincrónica, un usuario que paga y consulta su saldo 50ms después podría ver dinero viejo. Se resuelve en dos capas simples:
1. **Respuesta enriquecida:** `POST /v1/payments` devuelve directamente el `available_balance` calculado tras la reserva.
2. **Proyección de saldo en la misma transacción:** `wallet_balances` se actualiza en la misma transacción que guarda el evento en `Wallet`. Sigue siendo CQRS (el modelo de lectura está separado del de escritura), pero garantiza consistencia inmediata para el saldo propio. El historial (`wallet_transactions`) y las métricas continúan siendo asincrónicos.

---

## 4. Mapeo a la Parte 2 (Monolito Modular en Go)

La estructura del repositorio refleja estos mismos límites con separación estricta por capas:

- `internal/domain/` (`wallet`, `payment`, `events`): aggregates, reglas de negocio y contratos de eventos. **Sin dependencias externas**; `wallet` y `payment` no se importan entre sí.
- `internal/application/` (`ports`, `saga`): interfaces (`EventStore`, `EventBus`, `GatewayClient`, `ReadModels`) y el orquestador de la saga.
- `internal/infrastructure/` (`eventstore`, `eventbus`, `readmodel`, `gateway`): implementaciones en memoria y mock configurable de la pasarela (con latencia, timeouts y circuit breaker).
- `internal/observability/`: logging estructurado (`slog`), métricas y health checks.
- `cmd/demo/`: composición del sistema y ejecución de los escenarios.
