import aiohttp
import asyncio
import pandas as pd
from aiofiles import open as aioopen
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
import argparse
import os
import math

# Константы API и файла
API_URL = "http://5.159.103.79:4000/api/v1/logs"
DEFAULT_OUTPUT_CSV = "customs_data.csv"
RATE_LIMIT_DELAY = 180  # Задержка в секундах
REQUEST_DELAY = 1  # Задержка в секундах между запросами
MAX_REQUEST_DELAY = 10  # Максимальная задержка в секундах между запросами
MAX_CONCURRENT_REQUESTS = 50  # Максимальное количество одновременных запросов
HEADERS = {'accept': 'application/json'}
SAVE_BATCH_SIZE = 1000  # Сколько записей сохранять за один раз, прежде чем записать в файл
RETRY_DELAY = 300  # 5 минут задержки, если произошла ошибка, не 429
MAX_RETRIES = 10  # Максимальное количество попыток при ошибках

ITERATION_FILE = "iteration.txt"

async def fetch_total_entries_and_per_page(session: aiohttp.ClientSession) -> Tuple[int, int]:
    try:
        async with session.get(API_URL, headers=HEADERS, params={'page': 1}) as response:
            if response.status != 200:
                response.raise_for_status()
            data = await response.json()
            total_entries = data['totalEntries']
            per_page = len(data['items'])
            return total_entries, per_page
    except Exception as e:
        print(f"Failed to fetch total entries and per_page: {str(e)}")
        raise e


async def fetch_page(session: aiohttp.ClientSession, page: int, request_delay: float) -> Tuple[int, Optional[List[Dict[str, Any]]]]:
    await asyncio.sleep(request_delay)
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with session.get(API_URL, headers=HEADERS, params={'page': page}) as response:
                if response.status == 429:
                    print("Rate limit exceeded. Waiting for retry...")
                    for remaining in range(RATE_LIMIT_DELAY, 0, -1):
                        print(f"Retrying in {remaining} seconds...", end='\r')
                        await asyncio.sleep(1)
                    print(" " * 30, end='\r')
                    continue
                if response.status != 200:
                    response.raise_for_status()
                data = await response.json()
                return page, data.get('items', [])
        except Exception as e:
            print(f"Failed to fetch page {page}: {str(e)}. Retrying in {RETRY_DELAY} seconds.")
            retries += 1
            await asyncio.sleep(RETRY_DELAY)
    return page, None


async def fetch_all_pages(session: aiohttp.ClientSession, request_delay: float, queue: asyncio.Queue) -> List[Tuple[int, Optional[List[Dict[str, Any]]]]]:
    tasks = []
    while len(tasks) < MAX_CONCURRENT_REQUESTS and not queue.empty():
        page = await queue.get()
        tasks.append(fetch_page(session, page, request_delay))
    return await asyncio.gather(*tasks)


async def save_data_to_file(f, data: List[Dict[str, Any]], header_written: bool):
    df = pd.DataFrame(data)
    buffer = StringIO()
    df.to_csv(buffer, sep='\t', index=False, header=not header_written)
    await f.write(buffer.getvalue())


def load_last_iteration() -> int:
    if os.path.exists(ITERATION_FILE):
        with open(ITERATION_FILE, 'r') as file:
            last_page = file.read().strip()
            if last_page.isdigit():
                return int(last_page)
    return 1


def save_last_iteration(page: int) -> None:
    with open(ITERATION_FILE, 'w') as file:
        file.write(str(page))


async def process_pages(session: aiohttp.ClientSession, output_file: str, request_delay: float, queue: asyncio.Queue):
    all_data = []

    async with aioopen(output_file, 'a') as f:
        header_written = False

        while not queue.empty():
            tasks = await fetch_all_pages(session, request_delay, queue)
            for page_num, data in tasks:
                if data is None:  # Если данные отсутствуют, считаем, что достигли конца
                    continue

                all_data.extend(data)

                if len(all_data) >= SAVE_BATCH_SIZE:
                    await save_data_to_file(f, all_data, header_written)
                    all_data = []
                    header_written = True

                # Сохранение текущей страницы в случае прерывания
                save_last_iteration(page_num)

        # Записываем оставшиеся данные
        if all_data:
            await save_data_to_file(f, all_data, header_written)

async def main(output_file: str) -> None:
    print("Fetching data from API...")
    request_delay = REQUEST_DELAY

    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        total_entries, per_page = await fetch_total_entries_and_per_page(session)
        total_pages = math.ceil(total_entries / per_page)

        start_page = load_last_iteration()

        queue = asyncio.Queue()

        for page in range(start_page, total_pages + 1):
            await queue.put(page)

        workers = [asyncio.create_task(process_pages(session, output_file, request_delay, queue))
                   for _ in range(MAX_CONCURRENT_REQUESTS)]
        await queue.join()

        for worker in workers:
            worker.cancel()

    print(f"Data saved to {output_file}")

    # Удаление файла итерации при успешном завершении
    if os.path.exists(ITERATION_FILE):
        os.remove(ITERATION_FILE)


def run() -> None:
    parser = argparse.ArgumentParser(description="Fetch customs data and save to CSV.")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_CSV, help="Output CSV file path")
    args = parser.parse_args()
    asyncio.run(main(args.output))


if __name__ == "__main__":
    run()