import json
import logging

from pathlib import Path
from typing import Any

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def load_json_config(config_path: str = "dag_config.json") -> list[dict[str, Any]]:
    """
    Загружает конфигурацию DAG'ов из JSON файла.

    @param config_path: Путь к JSON файлу с конфигурацией
    @return: Список конфигураций DAG'ов
    """
    config_file = Path(__file__).parent / config_path

    with Path.open(config_file, "r", encoding="utf-8") as file:
        config = json.load(file)

    return config["dags"]


def create_print_task(message: str) -> callable:
    """
    Создает функцию для печати сообщения в логи.

    @param message: Сообщение для вывода
    @return: Функция для PythonOperator
    """

    def print_message_task(**context) -> None:
        logging.info(f"DAG Message: {message}")
        logging.info(f"DAG ID: {context['dag'].dag_id}")
        logging.info(f"Execution Date: {context['execution_date']}")
        logging.info(f"Task Instance: {context['task_instance']}")

        # Печатаем весь контекст как в оригинальном примере
        for context_key, context_value in context.items():
            logging.info(
                f"key_name – {context_key} | value_name – {context_value} | type_value_name – {type(context_value)}"
            )

    return print_message_task


def create_dag_from_config(dag_config: dict[str, Any]) -> DAG:
    """
    Создает DAG на основе конфигурации.

    @param dag_config: Словарь с конфигурацией DAG
    @return: Объект DAG
    """

    # Парсим дату начала
    start_date = pendulum.parse(dag_config["start_date"]).replace(tzinfo=pendulum.timezone("Europe/Moscow"))

    # Настройки по умолчанию
    default_args = {
        "owner": dag_config["owner"],
        "start_date": start_date,
        "retries": dag_config.get("retries", 3),
        "depends_on_past": True,
        "retry_delay": pendulum.duration(seconds=1),
    }

    dag = DAG(
        dag_id=dag_config["dag_id"],
        schedule_interval=dag_config["schedule_interval"],
        default_args=default_args,
        catchup=dag_config.get("catchup", False),
        tags=dag_config.get("tags", []),
        description=dag_config.get("description", ""),
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
    )

    # Создаем задачи
    with dag:
        start = EmptyOperator(
            task_id="start",
        )

        print_message_task = PythonOperator(
            task_id="print_message_task",
            python_callable=create_print_task(dag_config.get("print_message", "Default message")),
        )

        end = EmptyOperator(
            task_id="end",
        )

        # Определяем зависимости
        start >> print_message_task >> end

    return dag


# Загружаем конфигурацию и создаем DAG'и
try:
    dags_config = load_json_config()

    # Создаем DAG'и и добавляем их в глобальную область видимости
    for config in dags_config:
        dag_id = config["dag_id"]
        globals()[dag_id] = create_dag_from_config(config)

except Exception:
    logging.exception("Error loading JSON config")
