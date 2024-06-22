import aiohttp
import asyncio
import pandas as pd
from aiofiles import open as aioopen
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
import argparse

# Константы API и файла
API_URL = "http://5.159.103.79:4000/api/v1/logs"
DEFAULT_OUTPUT_CSV = "customs_data.csv"
RATE_LIMIT_DELAY = 180  # Задержка в секундах
INITIAL_REQUEST_DELAY = 0.1  # Начальная задержка в секундах между запросами
MAX_REQUEST_DELAY = 10  # Максимальная задержка в секундах между запросами
MAX_CONCURRENT_REQUESTS = 5  # Максимальное количество одновременных запросов
HEADERS = {'accept': 'application/json'}


async def fetch_page(
        session: aiohttp.ClientSession, page: int,
        request_delay: float) -> Tuple[int, Optional[List[Dict[str, Any]]]]:
    await asyncio.sleep(request_delay)
    try:
        async with session.get(API_URL, headers=HEADERS,
                               params={'page': page}) as response:
            if response.status == 429:
                print("Rate limit exceeded. Waiting for retry...")
                for remaining in range(RATE_LIMIT_DELAY, 0, -1):
                    print(f"Retrying in {remaining} seconds...", end='\r')
                    await asyncio.sleep(1)
                print(" " * 30, end='\r')  # Clear the line after countdown
                return await fetch_page(session, page, request_delay)
            if response.status != 200:
                response.raise_for_status()
            data = await response.json()
            return page, data.get('items', [])
    except Exception as e:
        print(f"Failed to fetch page {page}: {str(e)}")
        return page, None


async def fetch_all_pages(
        session: aiohttp.ClientSession, start_page: int, end_page: int,
        request_delay: float
) -> List[Tuple[int, Optional[List[Dict[str, Any]]]]]:
    tasks = [
        fetch_page(session, page, request_delay)
        for page in range(start_page, end_page + 1)
    ]
    return await asyncio.gather(*tasks)


async def fetch_and_save_data(output_file: str) -> None:
    request_delay = INITIAL_REQUEST_DELAY
    page = 1

    async with aiohttp.ClientSession() as session:
        async with aioopen(output_file, 'w') as f:
            header_written = False
            while True:
                tasks = await fetch_all_pages(
                    session, page, page + MAX_CONCURRENT_REQUESTS - 1,
                    request_delay)
                all_data = []
                for task in tasks:
                    page_num, data = task
                    if data is None:
                        break
                    all_data.extend(data)
                    page += 1

                if not all_data:
                    break

                df = pd.DataFrame(all_data)
                buffer = StringIO()
                df.to_csv(buffer,
                          sep='\t',
                          index=False,
                          header=not header_written)
                await f.write(buffer.getvalue())
                header_written = True

                # Динамическая настройка задержки
                if request_delay < MAX_REQUEST_DELAY:
                    request_delay *= 1.1  # Увеличиваем задержку на 10%


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
