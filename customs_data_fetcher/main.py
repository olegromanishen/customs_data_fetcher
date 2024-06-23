import aiohttp
import asyncio
import aiofiles
import pandas as pd
from aiofiles import open as aioopen
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
import argparse
import time

# Константы API и файла
API_URL = "http://5.159.103.79:4000/api/v1/logs"
DEFAULT_OUTPUT_CSV = "customs_data.csv"
HEADERS = {'accept': 'application/json'}
SAVE_BATCH_SIZE = 1000  # Сколько записей сохранять за один раз, прежде чем записать в файл
INITIAL_CONCURRENT_REQUESTS = 10  # Начальное количество одновременных запросов
MAX_CONCURRENT_REQUESTS = 200  # Максимальное количество одновременных запросов
RATE_LIMIT_WINDOW_SEC = 180  # Задержка в секундах при rate limit


async def fetch_page(
        session: aiohttp.ClientSession, page: int, request_delay: float
) -> Tuple[int, Optional[List[Dict[str, Any]]], bool]:
    await asyncio.sleep(request_delay)
    try:
        async with session.get(API_URL, headers=HEADERS,
                               params={'page': page}) as response:
            if response.status == 429:
                return page, None, False
            if response.status != 200:
                response.raise_for_status()
            data = await response.json()
            return page, data.get('items', []), True
    except Exception as e:
        print(f"Failed to fetch page {page}: {str(e)}")
        return page, None, True


async def fetch_all_pages(
    session: aiohttp.ClientSession, start_page: int, end_page: int,
    request_delay: float, concurrent_requests: int
) -> List[Tuple[int, Optional[List[Dict[str, Any]]], bool]]:
    tasks = [
        fetch_page(session, page, request_delay)
        for page in range(start_page, end_page + 1)
    ]
    semaphore = asyncio.Semaphore(concurrent_requests)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*[sem_task(task) for task in tasks])


async def fetch_and_save_data(output_file: str) -> None:
    concurrent_requests = INITIAL_CONCURRENT_REQUESTS
    request_delay = 0
    page = 1
    successful_requests = 0
    total_requests = 0
    start_time = time.time()

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            limit_per_host=MAX_CONCURRENT_REQUESTS)) as session:
        async with aioopen(output_file, 'w') as f:
            header_written = False
            all_data = []
            rate_limit_reached = False
            rate_limit_interval = 60  # Начальный интервал проверки лимита (60 секунд)

            while True:
                tasks = await fetch_all_pages(session, page,
                                              page + concurrent_requests - 1,
                                              request_delay,
                                              concurrent_requests)
                batch_successful_requests = sum(1 for task in tasks if task[2])
                total_requests += len(tasks)
                successful_requests += batch_successful_requests

                run_time = time.time() - start_time

                for task in tasks:
                    page_num, data, is_successful = task
                    if not is_successful:
                        rate_limit_reached = True
                        break
                    all_data.extend(data)
                    page += 1

                if rate_limit_reached:
                    print("Rate limit reached. Adjusting request rate...")
                    current_rps = total_requests / run_time  # Текущая скорость запросов в секунду
                    target_rps = total_requests / rate_limit_interval  # Целевая скорость запросов
                    request_delay = min(
                        (1 / target_rps), RATE_LIMIT_WINDOW_SEC /
                        total_requests)  # Новое значение задержки запроса

                    if rate_limit_interval == 60:
                        rate_limit_interval = 180  # Увеличиваем интервал до 3 минут
                    else:
                        # Если достигли лимита даже с интервалом в 3 минуты, уменьшить количество одновременных запросов
                        concurrent_requests = max(INITIAL_CONCURRENT_REQUESTS,
                                                  concurrent_requests - 10)

                    rate_limit_reached = False
                    successful_requests = 0
                    total_requests = 0
                    start_time = time.time()
                else:
                    if batch_successful_requests == concurrent_requests and concurrent_requests < MAX_CONCURRENT_REQUESTS:
                        concurrent_requests += 10  # Увеличиваем количество одновременных запросов
                    else:
                        concurrent_requests = max(INITIAL_CONCURRENT_REQUESTS,
                                                  concurrent_requests - 1)

                if not all_data:
                    break

                if len(all_data) >= SAVE_BATCH_SIZE:
                    df = pd.DataFrame(all_data)
                    buffer = StringIO()
                    df.to_csv(buffer,
                              sep='\t',
                              index=False,
                              header=not header_written)
                    await f.write(buffer.getvalue())

                    all_data = []  # Сброс буфера
                    if not header_written:
                        header_written = True

            # Запись оставшихся данных в файл
            if all_data:
                df = pd.DataFrame(all_data)
                buffer = StringIO()
                df.to_csv(buffer,
                          sep='\t',
                          index=False,
                          header=not header_written)
                await f.write(buffer.getvalue())


async def main(output_file: str) -> None:
    print("Fetching data from API...")
    await fetch_and_save_data(output_file)
    print(f"Data saved to {output_file}")


def run() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch customs data and save to CSV.")
    parser.add_argument("--output",
                        type=str,
                        default=DEFAULT_OUTPUT_CSV,
                        help="Output CSV file path")
    args = parser.parse_args()
    asyncio.run(main(args.output))


if __name__ == "__main__":
    run()
