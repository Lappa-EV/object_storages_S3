import os
import asyncio
import logging

from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from contextlib import asynccontextmanager  # Для создания асинхронного контекстного менеджера
from aiobotocore.session import get_session  # Асинхронная версия boto3
from botocore.exceptions import ClientError  # Ошибки при обращении к API

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s3_client.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("S3")

load_dotenv()  # Для считывания файла .env с ключами

CONFIG = {
    "key_id": os.getenv("KEY_ID"),
    "secret": os.getenv("SECRET"),
    "endpoint": os.getenv("ENDPOINT"),
    "container": os.getenv("CONTAINER"),
}


def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


class AsyncObjectStorage:
    # Определяем конструктор с конфигурацией пользователя и контейнера
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        if not all([key_id, secret, endpoint, container]):
            raise ValueError("Missing required configuration parameters")

        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "verify": False,
        }
        self._bucket = container
        self._session = get_session()

    # Создаем клиента S3 и передаем значения.
    # Соединение всегда закрывается как только функция в контекстмом менеджере отработает
    @asynccontextmanager
    async def _connect(self):
        """Асинхронный контекстный менеджер для установления соединения с ОХ."""
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str, target_name: Optional[str] = None) -> None:
        """Загружает файл из локальной файловой системы в бакет ОХ.
                Args:
                    local_source: Путь к локальному файлу.
                    target_name: Имя файла в объектном хранилище (если не указано, используется имя локального файла).
        """
        file_ref = Path(local_source)

        if target_name is None:
            target_name = file_ref.name  #
            logger.warning(f"Target name is none")

        try:
            async with self._connect() as remote:  # Устанавливаем соединение
                with file_ref.open("rb") as binary_data:  # Открываем файл
                    await remote.put_object(  # Отправляем файл
                        # указываем необходимые данные (bucket, имя ввиде ID  и то как передаем сам файл)
                        Bucket=self._bucket,  # Имя контейнера
                        Key=target_name,  # Имя файла в объектном хранилище
                        Body=binary_data  # Тело файла (бинарные данные)
                    )
                logger.info(f"Sent: {target_name}")

        except ClientError as error:
            logger.error(f"Failed to send {target_name}: {error}")

    async def fetch_file(self, remote_name: str, local_target: Optional[str] = None):
        """ Загружает файл из ОХ в локальную файловую систему.
                Args:
                    remote_name: Имя файла в объектном хранилище.
                    local_target: Путь для сохранения локального файла (если не указано, используется имя файла из хранилища).
        """
        try:
            async with self._connect() as remote:
                response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
                body = await response["Body"].read()

                if local_target is None:
                    local_target = Path(remote_name).name
                    logger.info(f"Local target not specified, using: {local_target}")

                with open(local_target, "wb") as out:
                    out.write(body)
                logger.info(f"Retrieved: {remote_name}")

        except ClientError as error:
            logger.error(f"Could not retrieve {remote_name}: {error}")

    # Удаляем указанный файл из бакета
    async def remove_file(self, remote_name: str):
        """ Удаляет файл из объектного хранилища.
                Args:
                    remote_name: Имя файла в объектном хранилище.
        """
        try:
            async with self._connect() as remote:
                await remote.delete_object(Bucket=self._bucket, Key=remote_name)
                logger.info(f"Removed: {remote_name}")

        except ClientError as error:
            logger.error(f"Failed to remove {remote_name}: {error}")

    async def get_bucket_size(self) -> int:
        """ Вычисляет общий размер всех объектов в контейнере (bucket).
                Returns:
                    Общий размер контейнера в байтах.
        """
        total_size = 0

        try:
            async with self._connect() as remote:
                paginator = remote.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=self._bucket):
                    contents = page.get("Contents", [])
                    logger.info(f"Found {len(contents)} objects in page")

                    for obj in contents:
                        logger.info(f"Object {obj['Key']} - Size: {obj['Size']} bytes")
                        total_size += obj["Size"]

            logger.info(f"Total size calculated: {total_size} bytes")
            return total_size

        except ClientError as e:
            logger.error(f"Error calculating bucket size: {e}")
            return 0

    async def list_files(self) -> list[str]:
        """Возвращает список объектов в бакете."""
        try:
            async with self._connect() as remote:
                paginator = remote.get_paginator("list_objects_v2")
                file_list = []
                async for page in paginator.paginate(Bucket=self._bucket):
                    for obj in page.get("Contents", []):
                        file_list.append(obj["Key"])
            logger.info(f"List of files: {file_list}")
            return file_list
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            return []

    async def file_exists(self, remote_name: str) -> bool:
        """Проверяет, существует ли файл с определенным именем в бакете."""
        try:
            async with self._connect() as remote:
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
            logger.info(f"File '{remote_name}' exists.")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.info(f"File '{remote_name}' does not exist.")
                return False
            else:
                logger.error(f"Error checking if file '{remote_name}' exists: {e}")
                return False

async def demo():
    """ Демонстрационная функция для работы с AsyncObjectStorage."""
    storage = AsyncObjectStorage(
        key_id=CONFIG["key_id"],
        secret=CONFIG["secret"],
        endpoint=CONFIG["endpoint"],
        container=CONFIG["container"]
    )

    #  Получаем общий размер всех объектов в контейнере(bucket).
    size = await storage.get_bucket_size()
    logger.info(f"Bucket size: {format_size(size)}")

    await storage.send_file("./test_file.csv", "uploaded_demo_test.csv")

    size = await storage.get_bucket_size()
    logger.info(f"Bucket size: {format_size(size)}")

    await storage.fetch_file("uploaded_demo_test.csv") # загружаем файл test_file.csv в бакет под именем uploaded_demo_test.csv

    #  await storage.remove_file("uploaded_demo_test.csv") # удаление файла из ОХ

    final_size = await storage.get_bucket_size()
    logger.info(f"Final bucket size: {format_size(final_size)}")

    # Демонстрация новых методов
    # Получаем список файлов
    files = await storage.list_files()
    logger.info(f"Files in bucket: {', '.join(files)}")

    # Проверяем есть ли файл с указанным именем
    exists = await storage.file_exists("uploaded_demo_test.csv")
    logger.info(f"File 'uploaded_demo_test.csv' exists: {exists}")

    exists = await storage.file_exists("non_existent_file.csv")
    logger.info(f"File 'non_existent_file.csv' exists: {exists}")

# Запуск асинхронной функции demo
if __name__ == "__main__":
    asyncio.run(demo())
