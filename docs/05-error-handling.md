# 05 — Estrategia de Manejo de Errores

## 1. Clasificación de fallas

Cada error se clasifica antes de decidir si reintentar, compensar o derivar a una DLQ:

| Categoría | Ejemplos | Acción |
|---|---|---|
| **Transitorio (infraestructura)** | Timeout de red, HTTP `5xx` o `429`, corte momentáneo de DB, conflicto de versión optimista | Reintentar con backoff exponencial y jitter |
| **De negocio (permanente)** | Saldo insuficiente, validación inválida, rechazo `4xx` de la pasarela | No reintentar; emitir evento de rechazo y compensar si corresponde |
| **Resultado incierto** | Timeout **después** de enviar el pago a la pasarela | Pasar a `GATEWAY_UNKNOWN` e iniciar conciliación; **no liberar fondos** |
| **Mensaje corrupto (*poison pill*)** | Error de deserialización JSON, schema desconocido | Enviar directo a la DLQ sin reintentos y disparar alerta |

---

## 2. Reintentos con backoff exponencial y jitter

Para evitar que múltiples instancias reintenten al unísono contra un servicio degradado (*thundering herd*), todos los reintentos aplican **backoff exponencial con full jitter**:

`delay = random(0, min(cap, base * 2^intento))`

| Capa | Política por defecto |
|---|---|
| **Llamada HTTP a pasarela** | Base `500ms`, tope `10s`, hasta **3 intentos** (timeout de `10s` por intento), siempre con el mismo `payment_id` como clave de idempotencia |
| **Conflicto de concurrencia (Event Store)** | Recargar el aggregate, reevaluar invariantes y reintentar con jitter de `10–50ms` (hasta 5 intentos) |
| **Consumidores de Kafka** | 3 reintentos rápidos en proceso (`100ms → 1s`); si persiste, escalar a topics de reintento (`retry.30s` → `retry.5m` → `retry.30m`) y finalmente a la DLQ |
| **Proyecciones de lectura** | Reintento bloqueante sobre la partición con backoff (sin enviar a topics de retry) para preservar el orden exacto de versiones |

---

## 3. Dead Letter Queue (DLQ)

Cuando un mensaje agota todos sus reintentos o resulta no procesable, se mueve al topic `<topic>.dlq` preservando en sus headers el contexto del fallo:

- **Metadata adjunta:** topic/partición/offset original, tipo y detalle del error, cantidad de intentos y timestamp del primer fallo.
- **Alertas:** cualquier mensaje (`> 0`) en la DLQ de topics financieros (`wallets.*`, `gateway.*`) dispara alerta inmediata al equipo. En `Metrics` la severidad es menor.
- **Retención y redrive:** retención de 30 días. Una vez corregida la causa raíz, una herramienta de *redrive* reinyecta los mensajes al topic original de forma segura gracias a la idempotencia de los consumidores.

---

## 4. Circuit Breaker y Bulkhead (Gateway Adapter)

El `Gateway Adapter` mantiene un **circuit breaker por proveedor** para proteger al sistema ante caídas prolongadas:

- **Cerrado → Abierto:** cuando más del 50% de las últimas 20 llamadas falla, o tras 5 timeouts consecutivos.
- **Abierto (30s):** rechaza llamadas inmediatamente sin contactar al proveedor. Como existe **certeza de que el pago no se envió**, emite `GatewaySubmissionNotSent` y la saga puede liberar la reserva de forma segura sin esperar conciliación.
- **Semi-abierto:** deja pasar 3 llamadas de prueba; si tienen éxito vuelve a cerrarse, si fallan vuelve a abrirse.
- **Bulkhead:** limita la concurrencia máxima de llamadas simultáneas hacia cada proveedor (ej. 50 requests en vuelo) para que una pasarela lenta no agote los recursos del resto de la plataforma.

---

## 5. Transacciones compensatorias

El **punto de no retorno** de la saga es la confirmación del proveedor:

- **Antes de la aprobación:** se compensa hacia atrás liberando la reserva en `Wallet`.
- **Después de la aprobación:** solo se avanza hacia adelante; `CaptureFunds` no puede fallar por reglas de negocio porque los fondos ya están retenidos, así que se reintenta hasta confirmarse.

| Escenario de falla | Compensación |
|---|---|
| Saldo insuficiente en `Wallet` | Ninguna (no se reservó saldo ni se llamó a la pasarela) |
| Pasarela rechaza el pago (`GatewayPaymentDeclined`) | `ReleaseFunds` → `FundsReleased` → `PaymentFailed` |
| Circuit breaker abierto (no-envío seguro) | `ReleaseFunds` → `FundsReleased` → `PaymentFailed` |
| Conciliación confirma rechazo o pago inexistente | `ReleaseFunds` → `FundsReleased` → `PaymentFailed` |
| Reembolso posterior a `COMPLETED` | `RefundPayment` (pasarela) → `RefundFunds` (`Wallet`) → `FundsRefunded` |

Todas las compensaciones son **idempotentes**: ejecutar dos veces `ReleaseFunds` para el mismo `payment_id` no altera el saldo una segunda vez.

---

## 6. Ventana de conciliación para estados inciertos (`GATEWAY_UNKNOWN`)

**Regla de oro:** nunca se libera saldo por falta de respuesta; solo se libera con **evidencia negativa** del proveedor. Si liberáramos al primer timeout y la pasarela sí procesó el pago, el usuario podría gastar dos veces ese dinero y la plataforma asumiría la pérdida.

La política es **configurable por proveedor** según su SLA:

| Fase | Comportamiento | Valores por defecto |
|---|---|---|
| **1. Reintentos de envío** | Reenvío con la misma `idempotency_key` (`payment_id`). Si un proveedor no soporta idempotencia, no se reenvía y se pasa directo a fase 2. | 3 intentos, timeout `10s`, backoff + jitter (~1 min) |
| **2. Conciliación activa** | El pago entra en `GATEWAY_UNKNOWN`. Un proceso periódico consulta `GET /payments/{id}` con frecuencia decreciente. Si llega un webhook en cualquier momento, resuelve el pago inmediatamente. | Consultas a `1m, 5m, 15m, 30m` y luego cada `1h`, hasta **24 horas** |
| **3. Revisión manual** | Si vencen las 24h sin evidencia concluyente, el pago pasa a `MANUAL_REVIEW`. **Los fondos permanecen retenidos** y se alerta a operaciones para resolver con el archivo de liquidación diario del proveedor (T+1). | Alerta operativa + resolución manual (`APPROVED` o `DECLINED`) |

> **Opción de negocio (desactivada por defecto):** la configuración por proveedor puede incluir un umbral `auto_release_below_amount` para liberar automáticamente pagos de monto bajo tras vencer la ventana, aceptando el riesgo acotado a cambio de no retener saldo del usuario. Por defecto está desactivada para priorizar cero pérdida financiera.
