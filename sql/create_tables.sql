-- Workshop 3: esquema de la base de datos de predicciones (PostgreSQL)

-- Tabla cruda: guarda cada mensaje de Kafka tal como llegó, con su estado de procesamiento.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    kafka_topic       TEXT      NOT NULL,
    kafka_partition   INTEGER   NOT NULL,
    kafka_offset      BIGINT    NOT NULL,
    kafka_key         TEXT,
    raw_payload       TEXT      NOT NULL,
    event_id          TEXT,
    processing_status TEXT      NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN
            ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    error_message     TEXT,
    received_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

-- Dimensión país
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- Dimensión fecha (se construye a partir de event_time). date_id = AAAAMMDD
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INTEGER PRIMARY KEY,
    full_date DATE    NOT NULL UNIQUE,
    year      INTEGER NOT NULL,
    month     INTEGER NOT NULL,
    day       INTEGER NOT NULL
);

-- Tabla de hechos: una fila por predicción, enlazada al evento crudo que la originó.
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT  NOT NULL UNIQUE REFERENCES raw_happiness_events (raw_event_id),
    country_id           INTEGER NOT NULL REFERENCES dim_country (country_id),
    date_id              INTEGER NOT NULL REFERENCES dim_date (date_id),
    year                 INTEGER NOT NULL,            -- año del dato histórico
    actual_score         NUMERIC(6, 3) NOT NULL,
    predicted_score      NUMERIC(6, 3) NOT NULL,
    prediction_error     NUMERIC(6, 3) NOT NULL,      -- predicho - real (con signo)
    event_time           TIMESTAMP NOT NULL,
    prediction_timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);