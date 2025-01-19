-- Создание схемы logs для хранения логов
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Создание таблицы LOGS.DATA_LOAD_LOG для логирования процессов загрузки данных
CREATE TABLE IF NOT EXISTS LOGS.DATA_LOAD_LOG (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    status VARCHAR(20),
    message text,
	rows_loaded INTEGER
);