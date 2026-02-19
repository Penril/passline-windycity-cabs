# WindyCity Cabs – Solución End-to-End de Ingeniería de Datos

Este proyecto implementa una solución analítica completa sobre el dataset de Chicago Taxi Trips, diseñada para soportar decisiones estratégicas tanto para las áreas de Finanzas como de Operaciones.

La solución cubre:

- Ingesta incremental desde la API pública Socrata
- Modelado analítico en base relacional (GCP/Cloud SQL - MySQL)
- Transformaciones y agrupamientos de los datos
- Construcción de 4 dashboards en Looker Studio integrados con Cloud SQL
- Controles básicos de calidad y consistencia

---

# 1. Objetivos de la solución

El producto de datos fue diseñado para responder las siguientes preguntas clave para el negocio:

1. ¿Cómo evoluciona el ingreso/revenue total en el tiempo?
2. ¿Existe concentración de ingresos en algunas pocas compañías?
3. ¿Cuáles son las horas y días de mayor demanda? ¿Existe efectos de alto tráfico?
4. ¿Qué zonas generan mayor volumen y mayor ticket promedio?
5. ¿Existen viajes premium o anomalías de alto valor?
6. ¿Cómo se comportan los distintos medios de pago?

---

# 2. Arquitectura de la Solución

Socrata API
- Ingesta incremental con watermark  
- `stg_trips` (capa raw normalizada)  
- `fact_trips` (tabla analítica principal)  
- Tablas agregadas (`daily_kpis`, `hourly_kpis`, `zone_kpis`, `payment_kpis`)  
- Dashboards en Looker Studio

## Principios de diseño

- Ingesta idempotente
- Separación clara entre staging y capa analítica
- Agregaciones pre-calculadas para optimizar el uso de herramientas de BI
- Modelo con granuralidad explícita
- Reprocesamiento seguro

---

# 3. Modelo de Datos

## Granularidad

`fact_trips`  
1 row = 1 viaje

## Clave primaria

`trip_id`

## Estrategia de Ingesta Incremental

- Watermark basado en `trip_start_timestamp`
- Paginación por offset desde la API
- Upsert con `ON DUPLICATE KEY UPDATE`
- Normalización de NaN antes de insertar
- Tipificación de columnas

Esto permite:

- Re-ejecución sin duplicación
- Consistencia del revenue
- Escalabilidad para futuras cargas

---

# 4. Tablas Principales

## stg_trips
Capa de staging con datos normalizados provenientes de la API.

## fact_trips
Tabla analítica consolidada que incluye:

- Revenue total (`trip_total`)
- Componentes del revenue (`fare`, `tips`, `tolls`, `extras`)
- Duración del viaje
- Velocidad promedio (MPH)
- Zona pickup y dropoff
- Medio de pago
- Compañía

Todos los montos están expresados en USD (dataset oficial de Chicago Taxi Trips).

## daily_kpis
Métricas financieras agregadas por día:

- revenue_total
- trips
- avg_revenue_per_trip
- avg_duration

## hourly_kpis
Demanda y eficiencia por hora del día:

- trips
- revenue_total
- avg_speed_mph
- avg_trip_duration

## zone_kpis
Agregación por zona (`community_area`) y tipo de zona:

- zone_type = pickup / dropoff
- trips
- revenue_total

## payment_kpis
Distribución de revenue y volumen por medio de pago.

---

# 5. Métricas Clave

A continuación se definen las métricas implementadas:

## 1. Revenue Total
Definición: Ingresos totales generados por los viajes.  
Fórmula: `SUM(trip_total)`  
Decisión: Evaluar desempeño financiero global.

## 2. Viajes Totales
Definición: Cantidad total de viajes realizados.  
Fórmula: `COUNT(trip_id)`  
Decisión: Medir nivel de demanda.

## 3. Ingreso Promedio por Viaje
Definición: Ticket promedio.  
Fórmula: `SUM(trip_total) / COUNT(trip_id)`  
Decisión: Evaluar pricing y mix de viajes.

## 4. Propinas Totales
Definición: Total de tips generadas.  
Fórmula: `SUM(tips)`  
Decisión: Proxy de calidad de servicio y comportamiento del cliente.

## 5. % Tip Rate
Definición: Proporción de propinas respecto al revenue total.  
Fórmula: `SUM(tips) / SUM(trip_total)`  
Decisión: Análisis de comportamiento del consumidor.

## 6. P99 Trip Total
Definición: Percentil 99 del `trip_total`.  
Decisión: Detectar viajes premium o posibles anomalías.

## 7. Duración Promedio de Viaje
Definición: Tiempo promedio en minutos.  
Fórmula: `AVG(trip_seconds) / 60`  
Decisión: Evaluar eficiencia operativa.

## 8. Velocidad Promedio (MPH)
Definición: Distancia recorrida dividida por tiempo total.  
Fórmula: `SUM(trip_miles) / SUM(trip_seconds / 3600)`  
Decisión: Analizar impacto de tráfico.

## 9. Revenue por Milla
Definición: Ingreso promedio por distancia recorrida.  
Fórmula: `SUM(trip_total) / SUM(trip_miles)`  
Decisión: Optimización de pricing.

## 10. Concentración de Revenue
Definición: Participación de mercado de las principales compañías.  
Decisión: Evaluar riesgo de dependencia y concentración.

---

# 6. Dashboards Implementados

## Dirección / Finanzas #1 – Vista Ejecutiva

Audiencia: Dirección / CFO  

Incluye:
- Evolución de revenue
- Evolución de viajes
- Mix de medios de pago
- Ticket promedio
- P99 análisis

Decisiones habilitadas:
- Seguimiento financiero general
- Identificación de anomalías
- Análisis de precios

---

## Dirección / Finanzas #2 – Análisis por Compañías

Audiencia: Finanzas / Estrategia  

Incluye:
- Top revenue por compañía
- Participación de mercado
- Ticket promedio por compañía
- % propina por compañía (evaluacion de servicio por el cliente)
- Comparación de eficiencia operativa (en velocidad promedio)

Decisiones habilitadas:
- Benchmark competitivo
- Análisis de concentración de mercado (riesgo)
- Evaluación de performance individual

---

## Operación #1 – Demanda y Tiempo

Audiencia: Operaciones  

Incluye:
- Viajes por hora del día
- Revenue por hora del día
- Velocidad vs duración
- Revenue por día de semana

Decisiones habilitadas:
- Planificación de turnos
- Optimización de flota
- Identificación de horas peak

---

## Operación #2 – Distribución Geográfica

Audiencia: Operaciones / Expansión  

Incluye:
- Top zonas de pickup
- Top zonas de dropoff
- Ingreso promedio por zona
- Distribución geográfica de demanda

Decisiones habilitadas:
- Priorización de zonas
- Estrategia de cobertura
- Identificación de zonas premium

---

# 7. Controles de Calidad

- Normalización de tipos antes de inserción
- Manejo explícito de NaN
- Upsert para evitar duplicación
- Separación staging vs analítica
- Agregaciones desacopladas del fact

---

# 8. Consideraciones Técnicas

- Alimentación de BI desde tablas agregadas para mejorar performance
- Modularización del código en `ingest`, `transform` y `db`
- Separación clara entre lógica de extracción y lógica analítica
- Diseño orientado a re-ejecución segura

---

# 9. Decisiones de diseño y trade-offs

Durante el desarrollo del proyecto se tomaron las siguientes decisiones técnicas y de modelado:

## 1. Uso de modelo en estrella simplificado
Se construyó una tabla granular transaccional (`fact_trips`) y tablas transformadas agrupadas (`daily_kpis`, `hourly_kpis`, `zone_kpis`) en lugar de exponer directamente la tabla transaccional.

**Trade-off:**  
Se pierde algo de flexibilidad analítica en favor de:
- Mejor desempeño en BI
- Menor complejidad en Looker Studio
- Mayor claridad en la definición de métricas

## 2. Ingesta incremental con watermark
Se implementó un mecanismo de ingestión incremental basado en fecha máxima procesada.

**Ventajas:**
- Evita reprocesar todo el histórico
- Reduce costos de cómputo al permitir el procesamiento por lotes
- Permite escalabilidad

## 3. Manejo explicito de valores atípicos
Se incorporó el análisis de P99 para detectar outliers en `trip_total`.

Esto permite:
- Monitorear anomalías
- Detectar posibles errores o eventos extraordinarios
- Detectar viajes premium

**Desventajas:**
- Apunta mas directamente a datos anómalos o cobros excesivos puntuales, mas que a un grupo de valores altos.

---

# 10. Uso de IA y Prompts Relevantes Utilizados

Durante el desarrollo de este proyecto se utilizaron dos herramientas de IA, estas fueron ChatGPT y Gemini. Principalmente se usaron como apoyo técnico en tareas específicas de implementación y validación. Algunos ejemplos de prompts utilizados fueron:

1. "¿Ayudame implementar una estrategia de ingesta incremental idempotente con watermark en Python utilizando MySQL y lógica de UPSERT? Estoy implementando un proceso de ingestión incremental usando un campo timestamp como watermark. Qué problemas debo considerar si existen datos con problemas y cómo podría mitigarlos sin reprocesar toda la historia?"

2. "Necesito que me ayudes a implementar una buena practica para manejar valores NaN con Python y pandas antes de insertar datos en MySQL usando SQLAlchemy"

3. "Necesito que me ayudes a implementar una herramienta o forma utilizando percentiles para la detección de valores atípicos en MySQL para que luego sean mostrados en un Dashboard?"

4. "Generame una imagen incluyendo una paleta de colores y estilos para construir un dashboard en looker studio, esta imagen debe poder ser importada en looker para construir un panel corporativo para una empresa de taxis en Chicago. Debes elegir con cuidado los colores para que hagan un buen contraste y representen la industria."

5. "Al calcular ingresos totales en un dataset de taxis cuáles son las consideraciones para evitar dobles conteos cuando se analizan tanto por zona de pickup como por zona de dropoff y qué técnicas simples puedo usar para detectar valores anómalos en el monto total del viaje, es el percentil 99 una buena opción?"

La IA fue utilizada como soporte técnico para validar enfoques y optimizar soluciones, mientras que el modelado de datos, la definición de métricas y las decisiones de arquitectura fueron diseñadas y razonadas manualmente.

