# draftea-task-3-code-review-opus

---

## [User]
actua como principal backend engineer en go haciendo un code review final e imparcial de este challenge tecnico antes de entregarlo

te dejo adjunto el pdf con los requisitos originales recorre el repositorio completo docs/ internal/ cmd/demo go.mod y README.md sin modificar archivos todavia y armame un reporte directo con hallazgos concretos clasificados por severidad critica media y baja

evalua especialmente
1 cumplimiento de todos los requisitos funcionales y no funcionales del enunciado y consistencia entre docs/ y el codigo en go
2 posibles bugs de concurrencia race conditions deadlocks goroutines sin cerrar o casos borde financieros donde se pueda perder o duplicar dinero
3 calidad de codigo en go arquitectura hexagonal principios solid legibilidad y diseño de tests
4 detalles de entrega README

## [Assistant]
No lo entregaría todavía: encontré 2 bugs donde se pierde dinero y falta un entregable que pide el enunciado (el historial de IA de la Parte 2).

`go vet` está limpio, `go test -race -count=5` pasa estable y la demo corre los 5 escenarios. `gofmt` falla en 3 archivos.

No toqué el repo. Para confirmar los bugs de dinero escribí 3 tests que reproducen cada caso y los corrí sobre una copia en `/tmp`: los 3 se confirman.

---

### Reporte completo de Code Review

**Alcance:** `docs/`, `internal/`, `cmd/demo`, `go.mod`, `README.md`, `ai-history/` contra el PDF de requisitos. No se modificó nada del repo. Las PoC se corrieron en una copia aislada en `/tmp/review-poc`.

**Baseline verificado**

| Check | Resultado |
|---|---|
| `go vet ./...` | ✅ limpio |
| `go test -race -count=5 ./internal/...` | ✅ estable, sin races ni flakiness |
| Cobertura | saga 74% · wallet app 63% · readmodel 65% · domain 81–85% · bus 89% |
| `go run ./cmd/demo` | ✅ corre los 5 escenarios + observabilidad |
| `gofmt -l .` | ❌ 3 archivos sin formatear |
| PoC de bugs financieros | ❌ **3 de 3 confirmados** |

> **Veredicto:** la base es sólida: dominio puro, optimistic locking bien hecho, bus sin goroutines colgadas y tests con `-race` en verde. Igual **no lo entregaría todavía**. Hay 2 bugs que rompen tu propia "regla de oro" (liberar o declinar sin evidencia negativa) y falta un entregable explícito del enunciado (el historial de IA de la Parte 2). Calculo ~3–4 h para cerrar los críticos y los "media" que más se ven.

---

### 🔴 CRÍTICA

#### C1. `GatewaySubmissionNotSent` libera fondos de un pago que la pasarela ya aceptó (pérdida de dinero)

- **Dónde:** `internal/infrastructure/gateway/adapter.go` (L207–231) y `internal/application/saga/orchestrator.go` (L481–508)
- **Causa raíz (doble):**
  1. `sentAnyAttempt` es una variable local de **cada invocación** del handler y no se lleva por `payment_id`. Un segundo `SubmitPayment` para el mismo pago puede venir de la recovery de `FUNDS_RESERVED`, de un redelivery at-least-once o de un retry del bus si falló el `emit`. Si ese segundo intento encuentra el breaker abierto o el bulkhead saturado, emite `NotSent` aunque el primer intento sí haya salido.
  2. La saga solo descarta `NotSent` cuando el pago es terminal o `COMPLETED`. Libera desde **`SUBMITTED` y `GATEWAY_UNKNOWN`**, que es justo lo que `docs/05-error-handling.md §6` dice que nunca debe pasar.
- **PoC confirmada:** pago `SUBMITTED` → breaker OPEN → llega un `SubmitPayment` duplicado → `NotSent` → `ReleaseFunds` → `FAILED`. Después llega el webhook `APPROVED` y se ignora. Resultado final: `status=FAILED balance=10000 held=0` con el proveedor habiendo aprobado. La plataforma pierde el monto.
- **Fix:**
  - (a) En la saga, liberar por `NotSent` **solo si `status == FUNDS_RESERVED`**.
  - (b) En el adapter, registrar los intentos por `payment_id` (es el `gateway_attempts` que ya está en `docs/04-tech-stack.md`) y no emitir `NotSent` nunca si hubo un intento previo para ese pago.
  - (c) Agregar un test de regresión.

#### C2. Un reembolso con resultado incierto termina marcado como `GatewayRefundDeclined` por un retry del bus

- **Dónde:** `internal/infrastructure/gateway/adapter.go` (L402–416) junto con el retry de `internal/infrastructure/eventbus/partitioned.go` (L357–404)
- **Secuencia:**
  1. Los timeouts hacen que el handler devuelva error. Está bien, "no declinar ante incertidumbre".
  2. El bus reintenta el comando.
  3. Entre medio el breaker se abrió.
  4. La nueva invocación arranca con `sentAnyAttempt=false` y emite `RefundDeclined(CIRCUIT_OPEN)`.
  5. La saga hace `DeclineRefund` y libera el monto que tenía reservado en vuelo.
- **PoC confirmada (determinística con la config por defecto):** `refund calls over the wire=5 pendingRefund=0 refundable=5000 breaker=OPEN`.
- **Impacto:** el proveedor puede haber ejecutado el reembolso y el usuario nunca recibe el crédito. Además, como el saldo reembolsable se restaura, un segundo pedido de reembolso puede **duplicarse en el proveedor**. `BULKHEAD_SATURATED → RefundDeclined` tiene el mismo problema: la saturación es un límite local, no una respuesta del proveedor.
- **Fix:** guardar los intentos por `refund_id`. Si el breaker está abierto o el bulkhead lleno, reintentar más tarde (error retryable) en vez de declinar. Si el resultado es incierto, mantener el refund pendiente y conciliarlo con una consulta de estado del refund.

#### C3. Falta el historial de IA de la Parte 2 (entregable explícito del enunciado)

- El enunciado dice: *"Si usas IA (…), incluir historial de conversación"*.
- `ai-history/` solo tiene `draftea-task-1-architecture.md`.
- El commit `4187244` dice *"add part 2 AI history"* pero solo toca `README.md` y `docs/03`.
- `README.md` (L93) afirma que está *"el historial completo (diseño, implementación y revisión)"*. Hoy esa afirmación es falsa, y un evaluador lo nota en segundos.

---

### 🟡 MEDIA

#### M1. Reserva huérfana: una billetera queda con fondos retenidos que nadie libera

- **Dónde:** `internal/domain/wallet/wallet.go` (L198–226) y `internal/application/saga/orchestrator.go` (L320–335)
- **Causa:**
  - `Reserve` no recuerda los rechazos. Si después del rechazo llega un `ReserveFunds` tardío o duplicado (por ejemplo, la recovery re-emite para `PENDING` mientras el rechazo está en vuelo) y el saldo ya alcanza, reserva.
  - La saga ve `FundsReserved` para un pago `REJECTED`, no hace nada y no compensa.
- **PoC confirmada:** `status=REJECTED balance=15000 held=8000`. Esos fondos quedan congelados sin ninguna alerta.
- **Fix:** que la billetera registre los `payment_id` rechazados y, ante un comando repetido, vuelva a publicar el rechazo. Que la saga emita `ReleaseFunds` cuando llega `FundsReserved` para un pago que ya no está en `PENDING`.

#### M2. La `Idempotency-Key` queda trabada en `IN_PROGRESS` durante 24 h

- **Dónde:** `internal/application/saga/orchestrator.go` (L206–230)
- Si falla `publishPaymentEvents` o `publishCommand(ReserveFunds)` después de un `Append` exitoso, se devuelve el error sin llamar a `Complete` ni a `Delete`.
- Cada reintento del cliente recibe `ErrIdempotencyInProgress` (409) durante 24 h, mientras el pago queda en `PENDING` hasta que corra la recovery.
- **Fix:** completar la clave con `{status: PENDING}`. El pago ya existe y la recovery lo retoma.

#### M3. Los reembolsos en vuelo no se recuperan tras un reinicio

- `IsActiveSaga` (`internal/domain/payment/payment.go` L71–78) excluye `COMPLETED`. Un pago `COMPLETED` con `pendingRefunds` nunca vuelve a emitir `RefundPayment`.
- Cuando el adapter agota los retries por timeout, el mensaje termina en la DLQ y el monto queda reservado para siempre. No hay conciliación de reembolsos.

#### M4. Lo que dicen los docs y el README no coincide con el código en Outbox e Inbox

- El README y `docs/03-event-specification.md §6` venden el *Transactional Outbox* como decisión clave.
- El código hace **publish-after-append** (dual write), mitigado con "republish on retry". Es razonable, pero no está declarado.
- El Inbox marca antes de procesar y desmarca si falla, en lugar de ir "en la misma transacción". Ante un crash entre el mark y el procesamiento, el mensaje se pierde.
- Hay que declararlo como simplificación explícita en el README (o en `02 §4`). Si no, el evaluador lo va a leer como inconsistencia.

#### M5. La conciliación y los timeouts que describen los docs no están automatizados

- `docs/05-error-handling.md §6` describe consultas a 1m/5m/15m/30m/1h, un timeout de webhook (`SUBMITTED→GATEWAY_UNKNOWN`) y la expiración a las 24 h hacia `MANUAL_REVIEW`.
- El código solo expone `TriggerReconciliation` y `ExpireReconciliationWindow`, que se invocan a mano. No hay scheduler ni política configurable por proveedor.
- Alcanza con un reconciler mínimo (un ticker con política inyectable) o con documentarlo como fuera de alcance.

#### M6. El bus en memoria no es fiel al modelo de Kafka

- **Dónde:** `internal/infrastructure/eventbus/partitioned.go` (L307–355)
- Las particiones se **comparten entre todos los topics y consumer groups**. Un `SubmitPayment` con reintentos (3×10 s + backoff con la config por defecto) bloquea los `ReserveFunds` y las proyecciones que caen en la misma partición. Es head-of-line blocking entre dominios.
- Como la concurrencia está acotada por la cantidad de particiones (8–16), el bulkhead de 50 nunca llega a saturarse: es código muerto en la práctica.
- El backoff del bus no tiene jitter, aunque `05 §2` dice que todos los reintentos aplican full jitter.

#### M7. `gofmt` falla en 3 archivos

- `internal/domain/payment/payment.go`, `internal/application/wallet/service_test.go` y `internal/observability/observability_test.go`.
- El fix es trivial, pero es lo primero que corre un reviewer de Go. Sumar `gofmt -l .` al README o a un `Makefile`.

#### M8. Al README le falta contexto de lo implementado

- La tabla "Decisiones técnicas clave" describe el **diseño objetivo** (Kafka, Postgres, Outbox, API con 2 s / 422 / 202) y no lo que efectivamente se implementó.
- Falta:
  - una sección **"Implementado vs. simulado / fuera de alcance"**: sin capa HTTP, todo en memoria, outbox simplificado, conciliación manual, breaker único;
  - un **mapeo escenario → test → output de la demo**, por ejemplo `Scenario1..5` en `internal/application/saga/scenarios_test.go`;
  - la estrategia de testing y la cobertura;
  - un output de ejemplo de la demo;
  - un diagrama de componentes de la Parte 2 (bus → handlers → stores).
- El contrato HTTP de `docs/02-service-design.md §3` (409, 422, `/healthz`, `/metrics`) no existe como endpoints. Hay que aclarar que el registry y el collector son el "backend" de esos endpoints.

---

### 🟢 BAJA

**Arquitectura hexagonal y SOLID**
- **El transporte se filtra a la capa de aplicación.** `InitiatePaymentResult.HTTPStatus` y el `import "net/http"` en `internal/application/saga/orchestrator.go` (L13). Mejor devolver un resultado de dominio (`Reserved`/`Rejected`/`Pending`) y mapearlo en un adapter HTTP.
- **El orquestador tiene 1111 líneas** y dos `switch` gigantes con el mismo boilerplate (decode, correlation, mutate, republish). Se puede pasar a una tabla de handlers por tipo de evento (OCP/SRP). `publishCommand` recibe 9 parámetros posicionales y 6 son `string`, así que intercambiar `correlationID` con `causationID` es fácil. Conviene un struct.
- **Duplicación.** `wrapPermanentDomainError` y el loop de optimistic retry están duplicados entre `internal/application/wallet/service.go` y la saga. `newTestSystem` duplica casi línea por línea `newDemoPlatform`. Se puede extraer un composition root, por ejemplo `internal/bootstrap`.
- **El paquete `domain/events` tiene conceptos de infraestructura** (nombres de topics de Kafka, `DLQTopic`).
- **`observability.Collector` mezcla dos roles:** port de métricas operativas y "Metrics Service" de dominio, que importa `domain/payment`.

**Correctitud y robustez**
- **El webhook falla en abierto:** si `WebhookSecret == ""` se salta la verificación HMAC (`internal/infrastructure/gateway/adapter.go` L138). Debería fallar cerrado.
- **El breaker es único para todos los proveedores**, aunque los docs dicen "por proveedor". `handleCheckStatus` no pasa ni por el breaker ni por el bulkhead.
- **`OnStateChange` se ejecuta con `cb.mu` tomado.** Si el callback llama a `State()`, hay deadlock.
- **`RecoverActiveSagas` escanea todos los streams (O(N))**, mientras `PaymentViewStore.ListNonTerminal` existe y nunca se usa. Además usa el literal `"recovery"` como `causation_id`, que no es un `event_id`.
- **`RequestRefund` ignora en silencio** un `refundID` repetido con un monto distinto. Debería devolver un conflicto.
- **`ResolveManualReview` no persiste la decisión** (quién y por qué) en el stream del pago, y se pierde auditoría.
- **El aggregate `Wallet` acumula mapas que solo crecen** (`captured`, `released`, `deposits`, `refunds`). El snapshot crece en O(historia).
- **`partitionFor`:** `int(Sum32()) % n` puede dar un índice negativo en plataformas de 32 bits.

**Observabilidad**
- `RecordGatewayAttempt` descarta la latencia (`_ time.Duration`).
- `paymentStartedAt` y `seenEvents` no tienen límite.
- La demo imprime métricas de la instancia 1, así que el pago recuperado no aparece.
- `must` → `os.Exit` se saltea los `defer Close`.

**Tests**
- Falta un test de **invariante financiero** (conservación del dinero: `balance = depósitos − capturas + reembolsos` y `held = Σ reservas activas`) sobre secuencias aleatorias o duplicadas de comandos. Habría detectado C1 y M1.
- Hay un `time.Sleep` en `internal/infrastructure/eventbus/partitioned_test.go` (L254).
- No hay tests para el orden "webhook antes de `Accepted`" ni para la recovery con refunds pendientes.
- El jitter de optimistic lock es de 2–15 ms; los docs dicen 10–50 ms.

---

### Matriz de cumplimiento del enunciado

| Requisito | Estado | Nota |
|---|---|---|
| P1 · Diagramas (alto nivel, flujos, datos, integraciones) | ✅ | `docs/01`, los 5 escenarios con sequence diagrams |
| P1 · Diseño de servicios / eventos / stack / errores / escalabilidad | ✅ | Completo y coherente. Se desvía en C1, M4 y M5 |
| P1 · 5 escenarios (feliz, saldo, timeout, concurrencia, recovery) | ✅ | Documentados y demostrados |
| P2 · Estructura, patrones, tests representativos | ✅ | Capas claras, `-race` en verde |
| P2 · Event store y read models | ✅ | En memoria, con OCC, snapshots, rebuild y proyecciones con guard de versión |
| P2 · Plan de escalabilidad | ✅ | `docs/06` |
| P2 · Observabilidad (logging, métricas, health) | ⚠️ | Existe como librería; no hay endpoints `/healthz` `/readyz` `/metrics` |
| P2 · Mocks para lo externo | ✅ | `MockGateway` configurable + HMAC |
| Historial de IA | ❌ | Falta la Parte 2 (C3) |
| README con decisiones y cómo correr los tests | ⚠️ | Cómo correr: OK. Falta "implementado vs. simulado" (M8) |

### Lo que está bien (para sostenerlo en la entrevista)

- **Dominio puro y sin dependencias cruzadas.** `wallet` y `payment` no se importan entre sí. `Apply` está separado de los comandos y los aggregates son idempotentes por `payment_id` y `refund_id`.
- **Optimistic locking correcto,** con `BaseVersion()` y retry con jitter. El escenario de pagos concurrentes es sólido.
- **El timeout de envío respeta la regla de oro:** si un intento ya salió y después se abre el breaker, emite `OutcomeUnknown` y no `NotSent`.
- **Bus bien hecho:** sin deadlock en publish recursivo (cond-var FIFO), `Drain` sin goroutines filtradas, `Close` con deadline que cancela retries, DLQ con metadata.
- **Republish-on-retry** cubre el hueco de "append OK, publish falla" sin perder eventos.

### Orden de trabajo sugerido antes de entregar

1. C1 (guard en la saga + registro de intentos por pago) y C2, más los 3 tests de regresión.
2. C3: subir el historial de la Parte 2 y corregir la afirmación del README.
3. M7 `gofmt`, M8 README (implementado vs. simulado + mapeo de escenarios a tests).
4. M1, M2: fixes chicos de alto valor.
5. M3–M6: corregirlos o documentarlos como limitaciones conscientes.

¿Querés que empiece por los dos bugs de dinero?

## [User]
ya implementamos los fixes quirurgicos de tu reporte C1 C2 M1 M2 M7 gofmt y M8 en el readme junto con los 3 tests de regresion y dejamos M3 M4 M5 M6 documentados explicitamente como simplificaciones conscientes del alcance de 4 horas en el readme los archivos de ai-history de parte 2 los exportamos apenas cerremos esta verificacion para incluir tambien esta charla

hace una segunda pasada sin modificar archivos todavia y verifica
1 si tus 3 PoCs financieras de C1 C2 y M1 quedaron completamente cerradas y si M2 ya no traba la idempotency key
2 si alguno de los fixes metio alguna regresion race condition o nuevo caso borde financiero
3 corre gofmt -l . go vet ./... go test -race -count=3 ./... y go run ./cmd/demo
4 revisa si la nueva seccion del readme deja claro que esta implementado vs simulado y dame un veredicto directo de si queda listo para exportar ai-history y mergear a main

## [Assistant]
No está listo para mergear todavía, pero falta poco. Las 3 PoCs financieras y M2 quedaron cerradas y no encontré regresiones ni race conditions. Sí queda un caso residual de C1 que pierde dinero tras un reinicio, y con un fix chico se resuelve.

No modifiqué el repo. Las PoCs las corrí sobre una copia de tu working tree en `/tmp/review-poc2`.

### 1. PoCs y M2

| Hallazgo | Antes | Ahora | Estado |
|---|---|---|---|
| C1: `NotSent` libera un pago ya aceptado | `FAILED`, balance 10000 (pérdida) | `COMPLETED`, balance 5000, held 0 | ✅ cerrado |
| C2: refund marcado como rechazado tras un retry del bus | pendiente 0 (liberado) | pendiente 5000, refund en DLQ, sin `RefundDeclined` | ✅ cerrado |
| M1: reserva huérfana | held 8000 | held 0, se vuelve a publicar el rechazo original | ✅ cerrado |
| M2: `Idempotency-Key` trabada | `ErrIdempotencyInProgress` por 24 h | el retry devuelve el 202 `PENDING` cacheado | ✅ cerrado |

Corrí cada PoC 3 veces con `-race`. Tus 3 tests de regresión cubren lo mismo y además la compensación defensiva de M1.

### 2. Regresiones y casos borde nuevos

**🔴 Residual de C1 tras un reinicio (confirmado).** El registro de qué pagos ya se intentaron enviar (`attemptedPayments`) vive en la memoria del adapter. En un reinicio se pierde, aunque el event store sobrevive; es justo el modelo de recovery del escenario 5.

La secuencia que reproduje:
1. El `SubmitPayment` sale hacia la pasarela.
2. El proceso cae antes de procesar `Accepted`, así que el pago queda en `FUNDS_RESERVED`.
3. Al reiniciar, la recovery re-emite el `SubmitPayment` y el breaker está abierto.
4. El adapter, sin memoria del intento anterior, emite `NotSent`, y la saga libera porque el pago sigue en `FUNDS_RESERVED`.

Resultado: `status=FAILED balance=10000` con el proveedor aprobado.

Fix sugerido (~15 min):
- Agregar `Resubmission bool` a `SubmitPaymentCommand` y ponerlo en `true` en `RecoverActiveSagas` para `FUNDS_RESERVED`.
- En el adapter, tratar `cmd.Resubmission || paymentWasAttempted(...)` como "posiblemente enviado": nunca emitir `NotSent`, emitir `OutcomeUnknown` y dejar que la conciliación resuelva.

**🟢 Detalles menores, no bloquean:**
- **M2:** el primer llamador recibe un error mientras el reintento recibe 202. Como el pago ya quedó guardado, conviene devolver `result, nil` también en el primer llamado.
- **C1:** un submit duplicado con el breaker abierto baja el pago de `SUBMITTED` a `GATEWAY_UNKNOWN` con razón `GATEWAY_TIMEOUT`. Es conservador y seguro, pero la razón es engañosa; usaría algo como `RESUBMIT_BLOCKED`.
- **C2:** durante una caída, cada refund termina en la DLQ en milisegundos, porque los reintentos del bus duran 1–50 ms y el breaker queda abierto 30 s. Es seguro, pero requiere redrive manual. Conviene sumarlo al README junto con M3.
- Los mapas `attemptedPayments` y `attemptedRefunds` crecen sin límite. Para la simulación no importa.
- No hay race conditions ni deadlocks nuevos: los mapas están protegidos por `attemptMu` y los mensajes de un mismo `payment_id` se procesan en orden en su partición.

### 3. Checks

| Comando | Resultado |
|---|---|
| `gofmt -l .` | ✅ vacío |
| `go vet ./...` | ✅ |
| `go test -race -count=3 ./...` | ✅ todos los paquetes |
| `go run ./cmd/demo` | ✅ 5 escenarios, exit 0, sin mensajes en DLQ |

### 4. README

La sección nueva "Implementado en Parte 2 vs Diseño de Producción" está bien: cubre HTTP, Outbox, Inbox, conciliación, particiones del bus y recovery, más el mapeo de cada escenario a su test. Le faltan tres cosas:
- **Una aclaración bajo "Decisiones técnicas clave"**, algo como *"diseño objetivo; ver 'Implementado vs Diseño' para lo simulado"*. Hoy esa tabla sigue diciendo "Transactional Outbox" y "Kafka + PostgreSQL" sin matiz.
- **M3 explícito:** los reembolsos inciertos quedan reservados en la DLQ y requieren redrive manual, y la recovery no retoma reembolsos.
- **Una fila más:** el registro de intentos y la deduplicación de webhooks del adapter viven en memoria y no sobreviven un reinicio. En producción irían en `gateway_attempts` / `gateway_webhooks`.

### Veredicto

Antes de exportar `ai-history` y mergear a `main`, haría tres cosas:
1. Aplicar el fix de `Resubmission` y sumar el test residual como regresión. Debería pasar.
2. Agregar las tres aclaraciones al README.
3. Exportar `ai-history` asegurándote de incluir la Parte 2 completa (implementación + estas dos revisiones), porque el README ya afirma que está.

Si preferís no tocar código, la alternativa mínima es documentar el residual en la tabla de simplificaciones. No lo recomiendo: es una pérdida de dinero dentro del propio modelo de reinicio de la demo, y el fix es chico.

## [User]
ya aplicamos el micro-patch que propusiste el flag Resubmission en SubmitPaymentCommand para cubrir el residual de C1 tras reinicio el ajuste de M2 devolviendo 202 PENDING si falla el publish post-append el test de regresion correspondiente y las 3 aclaraciones en el readme

hace una verificacion rapida final sin modificar archivos
1 volve a correr tu PoC del residual de C1 tras reinicio y confirma que ahora termina en GATEWAY_UNKNOWN con los fondos retenidos sin perdida de dinero y que M2 devuelve el mismo 202 PENDING tanto en el primer llamado como en el retry
2 corre gofmt -l . go vet ./... go test -race -count=3 ./... y go run ./cmd/demo para validar que no hubo regresiones ni races
3 chequea que las 3 aclaraciones del readme hayan quedado claras y consistentes con el codigo
4 dame un veredicto final directo de si ya esta listo para exportar los historiales de parte 2 en ai-history/ y mergear a main sin abrir nuevos refactors de severidad baja

## [Assistant]
Sí, ya está listo: podés exportar los historiales de la Parte 2 y mergear a `main`. No queda nada crítico ni de severidad media abierto, y no abriría más refactors de severidad baja.

No modifiqué el repo. Corrí todo sobre una copia del commit `a72a062`, con todos los cambios ya commiteados.

### 1. PoCs (cada una 3 veces con `-race`)

| Caso | Resultado |
|---|---|
| **Residual de C1 tras reinicio** | ✅ Con el breaker abierto, la recovery deja el pago en `status=GATEWAY_UNKNOWN balance=10000 held=4000` y no llama a la pasarela. Cuando llega el webhook `APPROVED`, captura: `COMPLETED balance=6000 held=0`. No se pierde dinero. |
| **M2** | ✅ El primer llamado devuelve `202 PENDING` con `err=nil`, y el retry devuelve el mismo `202 PENDING` desde la caché de idempotencia. |
| C1 original / C2 / M1 | ✅ Siguen cerrados, sin regresión. |

La PoC del residual al principio no pasó, pero el error estaba en mi test, no en tu código. Primero comparaba el estado después del webhook, y además leía el saldo del read model, que en esa instancia nueva no se había reconstruido. La corregí para mirar el estado intermedio sobre el aggregate.

El flag `Resubmission` tiene un costo aceptable: si una reemisión de la recovery nunca había salido y el breaker está abierto, el pago va a `GATEWAY_UNKNOWN` y los fondos quedan retenidos hasta la conciliación, en vez de liberarse de inmediato. Es el lado seguro de la regla de oro.

### 2. Checks

| Comando | Resultado |
|---|---|
| `gofmt -l .` | ✅ vacío |
| `go vet ./...` | ✅ |
| `go test -race -count=3 ./...` | ✅ todos los paquetes |
| `go run ./cmd/demo` | ✅ 5 escenarios, exit 0, sin mensajes en DLQ |

### 3. Aclaraciones del README

Las tres son claras y coinciden con el código:
- **Nota bajo "Decisiones técnicas clave":** aclara que esa tabla es el diseño objetivo y enlaza a la sección de lo implementado.
- **Reembolsos inciertos:** el monto queda reservado y el comando en la DLQ para redrive manual, y la recovery no retoma reembolsos. Coincide con lo que mostró la PoC de C2 (`pending=5000`, un mensaje en DLQ).
- **Estado del adapter en memoria:** `attemptedPayments`, `attemptedRefunds` y `seenWebhooks` se pierden al reiniciar, y por eso la recovery manda `Resubmission=true`. Coincide con `orchestrator.go`, donde la recovery reemite los pagos en `FUNDS_RESERVED` con ese flag.

### 4. Antes de mergear

La sección "Uso de IA" del README dice que el historial completo está en `ai-history/`, y hoy ahí solo está `draftea-task-1-architecture.md`. Exportá la Parte 2 (implementación más las tres pasadas de revisión, incluida esta) antes del merge, para que el README no afirme algo que todavía no está.
