# 01 — Arquitectura del Sistema

## 1. Resumen y supuestos

El sistema permite a los usuarios pagar servicios usando el saldo de su billetera. El cobro se liquida al proveedor del servicio a través de una pasarela externa con respuesta asincrónica, recolectando métricas en tiempo real y garantizando consistencia financiera ante fallas o concurrencia.

### Supuestos principales

1. **Rol de la pasarela externa:** la billetera **financia** el pago y la pasarela **liquida** al proveedor del servicio. La carga de saldo queda fuera de alcance (se contempla un evento `FundsDeposited` para fondear billeteras).
2. **Moneda y montos:** una billetera por usuario y moneda. Todos los montos se expresan como enteros `int64` en unidades mínimas (centavos) más código ISO-4217 (`"ARS"`, `"USD"`), nunca en punto flotante.
3. **Contrato con la pasarela:** soporta una clave de idempotencia por request (`payment_id`), consulta de estado para conciliación y **webhooks firmados con HMAC** para autenticar las respuestas asincrónicas.
4. **Reembolsos:** se soportan reembolsos totales o parciales después de completado un pago.
5. **Alcance de la Parte 2:** se implementa como **monolito modular en Go**, con los módulos desacoplados detrás de interfaces y el broker/base/pasarela simulados en memoria.

---

## 2. Arquitectura de alto nivel

La arquitectura se organiza en **4 servicios** con responsabilidades claras, coordinados a través de Kafka y con almacenamiento lógico independiente por servicio:

```mermaid
flowchart LR
    C["Cliente / App"] --> API["API HTTP + Idempotency-Key"]

    subgraph core["Plataforma de Pagos"]
        API --> PAY["Payment Service (Saga Orquestada)"]
        API --> WAL["Wallet Service (Event Sourcing + CQRS)"]
        PAY --> PGP[("PostgreSQL: Payment Event Store + Outbox")]
        WAL --> PGW[("PostgreSQL: Wallet Event Store + Outbox + Read Models")]
        GWA["Gateway Adapter (ACL + Circuit Breaker)"] --> PGG[("PostgreSQL: Attempts + Webhooks + Outbox")]
        MET["Metrics Service"] --> TS[("TimescaleDB: Métricas de negocio")]
    end

    KAFKA{{"Apache Kafka"}}
    EXT["Pasarela de Pagos Externa"]
    RD[("Redis (Opcional): Idempotencia / Rate Limit")]

    API -.-> RD
    PGP -->|"Outbox Polling"| KAFKA
    PGW -->|"Outbox Polling"| KAFKA
    PGG -->|"Outbox Polling"| KAFKA

    KAFKA -->|"wallets.commands"| WAL
    KAFKA -->|"gateway.commands"| GWA
    KAFKA -->|"wallets.events / gateway.events"| PAY
    KAFKA -->|"todos los eventos de dominio"| MET

    GWA -->|"HTTPS + Idempotency Key"| EXT
    EXT -->|"Webhooks firmados (HMAC)"| GWA
```

### Flujo de datos y patrones de integración

- **Camino de escritura (comandos):** los servicios escriben sus eventos y los mensajes salientes en su propia base PostgreSQL dentro de la **misma transacción** (patrón *Transactional Outbox*). Un worker de polling publica los mensajes pendientes en Kafka, evitando el problema de escritura doble (*dual-write*).
- **Camino de lectura (CQRS):** las consultas de saldo leen un read model actualizado en la misma transacción de escritura para evitar que el usuario vea saldo desactualizado tras pagar (*read-your-writes*). El historial de movimientos y las métricas se actualizan de forma asincrónica desde Kafka.
- **Coordinación híbrida:** `Payment` **orquesta** el flujo financiero (emite comandos a `Wallet` y `Gateway Adapter` y reacciona a sus respuestas). `Metrics` consume los eventos de forma **coreografiada**, sin acoplarse al flujo del pago.

### Puntos de integración externa

| Punto de integración | Dirección | Mecanismo | Protecciones |
|---|---|---|---|
| **Envío de pago** | Gateway Adapter → Pasarela | HTTPS `POST /payments` | `payment_id` como clave de idempotencia, timeout, reintentos con backoff + jitter, circuit breaker |
| **Consulta de estado** | Gateway Adapter → Pasarela | HTTPS `GET /payments/{id}` | Usado durante la conciliación cuando no llega respuesta |
| **Reembolso** | Gateway Adapter → Pasarela | HTTPS `POST /refunds` | `refund_id` como clave de idempotencia |
| **Webhooks de resultado** | Pasarela → Gateway Adapter | HTTPS `POST /v1/webhooks/{provider}` | Validación de firma HMAC, deduplicación por `provider_event_id` y persistencia antes de responder `200` |

---

## 3. Escenarios de flujo de pago

### 3.1 Ruta feliz

Flujo completo desde la solicitud del usuario hasta la liquidación confirmada y la captura definitiva de fondos.

```mermaid
sequenceDiagram
    autonumber
    actor U as Usuario
    participant API as API HTTP
    participant P as Payment (Saga)
    participant K as Kafka
    participant W as Wallet
    participant G as Gateway Adapter
    participant X as Pasarela Externa
    participant M as Metrics

    U->>API: POST /v1/payments (Idempotency-Key)
    API->>P: Iniciar pago
    P->>P: Guarda PaymentInitiated + Outbox(ReserveFunds)
    P-->>K: wallets.commands: ReserveFunds (key: wallet_id)
    K-->>W: ReserveFunds
    W->>W: Valida available >= amount, guarda FundsReserved
    W-->>K: wallets.events: FundsReserved
    K-->>P: FundsReserved
    P->>P: Transiciona a FUNDS_RESERVED + Outbox(SubmitPayment)
    API-->>U: 202 Accepted (payment_id, available_balance)
    P-->>K: gateway.commands: SubmitPayment (key: payment_id)
    K-->>G: SubmitPayment
    G->>X: POST /payments (idempotency_key: payment_id)
    X-->>G: 202 Accepted (provider_ref)
    G-->>K: gateway.events: GatewaySubmissionAccepted
    K-->>P: GatewaySubmissionAccepted (estado: SUBMITTED)
    Note over X,G: Procesamiento asincrónico en la pasarela
    X->>G: Webhook payment.approved (firma HMAC)
    G->>G: Verifica HMAC y deduplica evento
    G-->>K: gateway.events: GatewayPaymentApproved
    K-->>P: GatewayPaymentApproved
    P-->>K: wallets.commands: CaptureFunds
    K-->>W: CaptureFunds
    W->>W: Descuenta balance y held, guarda FundsCaptured
    W-->>K: wallets.events: FundsCaptured
    K-->>P: FundsCaptured -> PaymentCompleted (estado: COMPLETED)
    K-->>M: Consume eventos de dominio en tiempo real
```

---

### 3.2 Saldo insuficiente (rechazo inmediato)

La validación se hace siempre sobre el aggregate de `Wallet` reconstruido desde sus eventos (nunca sobre una caché). La API espera hasta **2 segundos** el resultado de la reserva para responder `422` de forma inmediata; si la operación tarda más, responde `202` y el cliente consulta el estado luego.

```mermaid
sequenceDiagram
    autonumber
    actor U as Usuario
    participant API as API HTTP
    participant P as Payment (Saga)
    participant W as Wallet

    U->>API: POST /v1/payments
    API->>P: Iniciar pago
    P->>P: Guarda PaymentInitiated + Outbox(ReserveFunds)
    P-->>W: ReserveFunds
    W->>W: Rehidrata saldo: available < amount
    W->>W: Guarda FundsReservationRejected (INSUFFICIENT_FUNDS)
    W-->>P: FundsReservationRejected
    P->>P: Guarda PaymentRejected (estado: REJECTED)
    alt Respuesta recibida dentro de 2s
        API-->>U: 422 Unprocessable Entity (INSUFFICIENT_FUNDS)
    else Supera 2s de espera
        API-->>U: 202 Accepted (status: PENDING)
        Note over U,API: El cliente consulta GET /v1/payments/{id} y ve REJECTED
    end
    Note over P,W: No se contacta a la pasarela ni hace falta compensar
```

---

### 3.3 Tiempo de espera de pasarela externa (reintento, fallback y conciliación)

Un timeout de la pasarela **no es un rechazo**: el proveedor pudo haber procesado el cobro. Nunca se liberan los fondos sin evidencia negativa. Solo cuando el circuit breaker está abierto (certeza de que el request no salió) se falla y libera de forma segura.

```mermaid
sequenceDiagram
    autonumber
    participant P as Payment (Saga)
    participant G as Gateway Adapter
    participant X as Pasarela Externa
    participant W as Wallet

    P-->>G: SubmitPayment (payment_id)
    alt Circuit breaker abierto (no-envío seguro)
        G-->>P: GatewaySubmissionNotSent (CIRCUIT_OPEN)
        P-->>W: ReleaseFunds
        W-->>P: FundsReleased
        P->>P: Guarda PaymentFailed (estado: FAILED)
    else Request enviado sin respuesta
        loop Hasta 3 intentos (backoff exponencial + jitter)
            G->>X: POST /payments (misma idempotency_key)
            X--xG: Timeout
        end
        G-->>P: GatewayOutcomeUnknown
        P->>P: Estado GATEWAY_UNKNOWN (inicia ventana de conciliación)
        loop Consultas periódicas (hasta 24h configurables)
            P-->>G: CheckPaymentStatus
            G->>X: GET /payments/{payment_id}
            alt Proveedor confirma aprobación
                G-->>P: GatewayPaymentApproved
                P-->>W: CaptureFunds -> PaymentCompleted
            else Proveedor confirma rechazo o inexistencia
                G-->>P: GatewayPaymentDeclined
                P-->>W: ReleaseFunds -> PaymentFailed
            else Sigue pendiente o sin respuesta
                G-->>P: GatewayStatusPending
            end
        end
        Note over P,W: Si vencen las 24h sin evidencia -> MANUAL_REVIEW (fondos retenidos + alerta)
    end
```

---

### 3.4 Pagos concurrentes (condiciones de carrera)

Ejemplo: billetera con saldo `100` recibe dos pagos simultáneos de `80` (A y B). El sistema protege el saldo con tres capas: idempotencia en la API (doble click), particionado por `wallet_id` en Kafka (procesamiento en serie) y **optimistic locking** en PostgreSQL como red de seguridad definitiva.

```mermaid
sequenceDiagram
    autonumber
    participant A as Request Pago A (80)
    participant B as Request Pago B (80)
    participant P as Payment
    participant K as Kafka (Partición wallet_id)
    participant W as Wallet
    participant DB as Event Store (Postgres)

    Note over W: Saldo disponible = 100 (version 7)
    A->>P: POST /v1/payments (80)
    B->>P: POST /v1/payments (80)
    P-->>K: ReserveFunds A (key: wallet_id)
    P-->>K: ReserveFunds B (key: wallet_id)
    K-->>W: ReserveFunds A
    W->>DB: INSERT FundsReserved (expected_version = 7)
    DB-->>W: OK -> version 8 (available = 20)
    K-->>W: ReserveFunds B (en serie)
    W->>W: Rehidrata version 8 -> available (20) < 80
    W->>DB: INSERT FundsReservationRejected (expected_version = 8)
    DB-->>W: OK -> version 9
    Note over W,DB: Si dos instancias compiten en un rebalanceo, el segundo INSERT falla por UNIQUE(stream_id, version), recarga y rechaza por saldo.
```

---

### 3.5 Recuperación del sistema (reinicio y reconstrucción del estado)

Ante un reinicio abrupto del proceso, el estado no depende de la memoria: los aggregates se reconstruyen desde el Event Store (último snapshot + eventos posteriores), el Outbox retoma publicaciones pendientes y las sagas activas continúan desde su último estado persistido.

```mermaid
sequenceDiagram
    autonumber
    participant SYS as Arranque del Servicio
    participant DB as PostgreSQL
    participant RL as Outbox Relay
    participant K as Kafka
    participant PR as Proyecciones
    participant S as Recuperación de Sagas

    SYS->>DB: Health check y verificación de conexión
    SYS->>RL: Inicia polling de Outbox
    RL->>DB: SELECT mensajes WHERE published_at IS NULL
    RL-->>K: Publica eventos y comandos pendientes
    SYS->>PR: Reanuda desde último checkpoint persistido
    SYS->>K: Consumidores retoman desde último offset confirmado
    Note over K,DB: Mensajes re-entregados se descartan vía Inbox (processed_messages)
    SYS->>S: Busca pagos en estados no finales
    S->>DB: Rehidrata cada pago desde su stream de eventos
    alt Estado PENDING o FUNDS_RESERVED
        S-->>K: Re-emite comando pendiente (idempotente)
    else Estado SUBMITTED o GATEWAY_UNKNOWN
        S->>S: Retoma seguimiento de timeout / conciliación
    end
    Note over PR,DB: Si un Read Model se corrompe, se reconstruye reproduciendo eventos desde el Event Store.
```
