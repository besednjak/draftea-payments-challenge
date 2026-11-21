# 06 — Plan de Escalabilidad

## 1. Supuesto de carga y dimensionamiento

Como referencia para dimensionar el diseño, consideramos **200 pagos/s sostenidos** con picos de **1.000 pagos/s** (por ejemplo, momentos previos a eventos masivos).

- Cada pago exitoso genera ~8 eventos en los Event Stores (`Payment` + `Wallet`) y ~12 mensajes en Kafka entre eventos y comandos.
- En pico (1.000 pagos/s), esto representa ~8.000 inserciones/s de eventos repartidas entre los módulos, un volumen que PostgreSQL bien afinado soporta cómodamente antes de requerir sharding.

---

## 2. Escalamiento horizontal y balanceo de carga

| Capa | Estrategia |
|---|---|
| **API HTTP** | Instancias sin estado (*stateless*) detrás de un balanceador L7 (round-robin + health checks en `/readyz`). Auto-escalado por CPU y requests por segundo. |
| **Consumidores de Kafka** | Auto-escalado basado en el **consumer lag** de cada consumer group, hasta un máximo igual a la cantidad de particiones del topic. |
| **Kafka** | Topics creados con margen de particiones desde el inicio (ej. 48 particiones) para evitar reparticionar en producción, ya que cambiar el número de particiones altera el mapeo de claves y puede romper el orden temporalmente. |
| **Gateway Adapter** | Escala de forma independiente porque su carga es *I/O-bound* (espera de red hacia proveedores externos), controlada con bulkhead y rate limits acordados con cada pasarela. |

---

## 3. Escalamiento y particionamiento de bases de datos

1. **Separación lógica → separación física:** desde el inicio, `Wallet`, `Payment` y `Gateway Adapter` tienen schemas independientes. El primer paso ante crecimiento es mover cada módulo a su propia instancia física de PostgreSQL sin modificar el código de dominio.
2. **Réplicas de lectura:** las consultas no críticas (`GET /transactions`, consultas de backoffice y reportes) se derivan a réplicas de lectura. `wallet_balances` se lee del primario para mantener consistencia inmediata (*read-your-writes*).
3. **Connection pooling:** uso de PgBouncer delante de PostgreSQL para absorber cientos de conexiones de instancias Go sin saturar la base.
4. **Particionamiento del Event Store:** particionamiento por rango de fecha (`occurred_at`, mensual) para mantener índices pequeños y permitir archivar eventos antiguos (ya cubiertos por snapshots) en almacenamiento frío manteniendo la retención legal.
5. **Sharding por `wallet_id` (escala masiva):** si el volumen de escritura supera la capacidad de un único primario, `Wallet` se shardea por `wallet_id`. Como cada billetera es un aggregate independiente, ninguna operación requiere transacciones entre shards.

---

## 4. Estrategia de caché

| Dato | ¿Se cachea? | Estrategia |
|---|---|---|
| **Respuestas de `Idempotency-Key`** | Sí (Redis, TTL 24h) | Acelera la detección de reintentos y doble click sin consultar PostgreSQL |
| **Rate limiting de API** | Sí (Redis) | Contadores con ventana deslizante por usuario |
| **Estado de pagos finalizados** (`COMPLETED`, `FAILED`, `REJECTED`) | Sí (Redis, TTL corto) | Son inmutables (salvo reembolso, que invalida la clave) |
| **Saldo disponible para validar pagos** | **No** | Siempre se evalúa sobre el aggregate de `Wallet` para evitar condiciones de carrera y saldo negativo |

---

## 5. Análisis de cuellos de botella

| Cuello de botella potencial | Señal de alerta | Mitigación |
|---|---|---|
| **Escrituras en el Event Store** | Latencia p99 de commit en aumento | Escritura en lotes donde aplique → separar módulos en instancias físicas → particionamiento → sharding por `wallet_id` |
| **Latencia / rate limit de la pasarela externa** | Incremento de `GATEWAY_UNKNOWN` y aperturas del circuit breaker | Procesamiento asincrónico, bulkhead, negociación de cuotas o soporte multi-proveedor |
| **Throughput del Outbox Relay (polling)** | Crecimiento sostenido de mensajes pendientes en `outbox` | Lectura en lotes más grandes → workers particionados → migración transparente a CDC (Debezium) |
| **Lag en proyecciones / Metrics** | Retraso visible en historial o dashboards | Escalar consumidores por partición y pre-agregar métricas en ventanas de 1 minuto |
| **Billetera "caliente" (ej. cuenta concentradora)** | Conflictos frecuentes de optimistic locking en un solo `stream_id` | Procesamiento secuencial por partición y, a gran escala, subdivisión en subcuentas |

---

## 6. Camino evolutivo desde el monolito modular

La arquitectura está pensada para crecer de forma incremental, evitando microservicios prematuros:

1. **Etapa 1 (Inicio / Parte 2):** monolito modular en Go con límites estrictos entre `Payment`, `Wallet`, `Gateway Adapter` y `Metrics`.
2. **Etapa 2 (Aislamiento externo):** extraer `Gateway Adapter` como servicio independiente (distinto perfil de fallas y escala *I/O-bound*).
3. **Etapa 3 (Carga analítica):** extraer `Metrics` con su base TimescaleDB dedicada.
4. **Etapa 4 (Alta escala transaccional):** separar `Wallet` y `Payment` con instancias físicas independientes y sharding por `wallet_id`.
