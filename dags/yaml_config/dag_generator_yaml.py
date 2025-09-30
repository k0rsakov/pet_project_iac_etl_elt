import logging

from pathlib import Path
from typing import Any

import pendulum
import yaml

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def load_dag_config() -> dict[str, Any]:
    """
    Загружает конфигурацию DAG из YAML файла.

    Returns:
        Dict[str, Any]: Конфигурация DAG'ов
    """
    config_path = Path(__file__).parent.parent.parent / "dag_config.yaml"

    try:
        with config_path.open("r", encoding="utf-8") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.exception("Файл конфигурации dag_config.yaml не найден")
        return {"dags": []}


def create_print_task(message: str) -> callable:
    """
    Создает функцию для печати сообщения и контекста.

    Args:
        message: Сообщение для вывода

    Returns:
        callable: Функция для выполнения в PythonOperator
    """

    def print_message_and_context(**context) -> None:
        """
        Печатает сообщение и контекст DAG.

        Args:
            **context: Контекст DAG
        """
        logging.info(f"Сообщение: {message}")

        for key, value in context.items():
            logging.info(f"key_name – {key} | value_name – {value} | type_value_name – {type(value)}")

    return print_message_and_context


def create_dag_from_config(dag_config: dict[str, Any]) -> DAG:
    """
    Создает DAG на основе конфигурации.

    Args:
        dag_config: Конфигурация DAG

    Returns:
        DAG: Созданный DAG
    """
    try:
        # Парсинг даты начала
        start_date_str = dag_config.get("start_date", "2023-01-01")
        start_date = pendulum.parse(start_date_str).replace(tzinfo=pendulum.timezone("Europe/Moscow"))

        # Настройки по умолчанию
        default_args = {
            "owner": dag_config.get("owner", "airflow"),
            "start_date": start_date,
            "catchup": False,
            "retries": dag_config.get("retries", 1),
            "depends_on_past": True,
            "retry_delay": pendulum.duration(seconds=1),
        }

        # Создание DAG
        dag = DAG(
            dag_id=dag_config["dag_id"],
            schedule_interval=dag_config.get("schedule_interval", "0 0 * * *"),
            default_args=default_args,
            tags=dag_config.get("tags", ["generated"]),
            description=f"Generated DAG from YAML config: {dag_config['dag_id']}",
            concurrency=1,
            max_active_tasks=1,
            max_active_runs=1,
        )

        with dag:
            start = EmptyOperator(task_id="start")

            print_message_task = PythonOperator(
                task_id="print_message_task",
                python_callable=create_print_task(dag_config.get("print_message", "Default message")),
            )

            end = EmptyOperator(task_id="end")

            start >> print_message_task >> end

        return dag  # noqa: TRY300

    except Exception:
        logging.exception(f"Ошибка при создании DAG {dag_config.get('dag_id', 'unknown')}")
        raise


# Загрузка конфигурации и создание DAG'ов
config = load_dag_config()

for dag_config in config.get("dags", []):
    try:
        dag_id = dag_config["dag_id"]
        globals()[dag_id] = create_dag_from_config(dag_config)
        logging.info(f"DAG {dag_id} успешно создан из YAML конфигурации")
    except Exception:
        logging.exception("Не удалось создать DAG из конфигурации")
