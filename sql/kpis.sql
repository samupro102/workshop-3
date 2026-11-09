-- Workshop 3: vista y consultas de los KPIs del dashboard (PostgreSQL)

-- Vista plana que une la tabla de hechos con sus dimensiones. Power BI se conecta a esta vista.
CREATE OR REPLACE VIEW vw_predictions_detail AS
SELECT
    f.prediction_id,
    f.raw_event_id,
    c.country_name,
    f.year,
    f.actual_score,
    f.predicted_score,
    f.prediction_error,                  -- predicho - real (con signo)
    ABS(f.prediction_error) AS abs_error,
    d.full_date             AS event_date,
    f.event_time,
    f.prediction_timestamp
FROM fact_predictions f
JOIN dim_country c ON c.country_id = f.country_id
JOIN dim_date    d ON d.date_id    = f.date_id;

-- KPI 1: error promedio de prediccion
SELECT
    COUNT(*)                                          AS total_predicciones,
    ROUND(AVG(abs_error), 3)                          AS error_absoluto_promedio,
    ROUND(AVG(prediction_error), 3)                   AS sesgo_promedio,
    ROUND(SQRT(AVG(prediction_error ^ 2)), 3)         AS rmse
FROM vw_predictions_detail;

-- KPI 2: predicciones por pais
SELECT
    country_name,
    COUNT(*)                 AS predicciones,
    ROUND(AVG(abs_error), 3) AS error_absoluto_promedio
FROM vw_predictions_detail
GROUP BY country_name
ORDER BY predicciones DESC, country_name;

-- KPI 3: predicho vs real (una fila por prediccion, para el grafico de dispersion)
SELECT
    country_name,
    year,
    actual_score,
    predicted_score,
    prediction_error
FROM vw_predictions_detail
ORDER BY year, country_name;

-- KPI 3 (resumen): correlacion entre el puntaje real y el predicho
SELECT ROUND(CORR(actual_score, predicted_score)::numeric, 3) AS correlacion_real_vs_predicho
FROM vw_predictions_detail;

-- KPI 4: tendencia por anio del dato historico
SELECT
    year,
    COUNT(*)                       AS predicciones,
    ROUND(AVG(actual_score), 3)    AS puntaje_real_promedio,
    ROUND(AVG(predicted_score), 3) AS puntaje_predicho_promedio,
    ROUND(AVG(abs_error), 3)       AS error_absoluto_promedio
FROM vw_predictions_detail
GROUP BY year
ORDER BY year;