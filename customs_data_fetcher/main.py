import aiohttp
import asyncio
import pandas as pd
from aiofiles import open as aioopen
from io import StringIO
from typing import Any, Dict, List, Optional
import argparse

# Константы API и файла
API_URL = "http://5.159.103.79:4000/api/v1/logs"
DEFAULT_OUTPUT_CSV = "customs_data.csv"
INITIAL_REQUEST_DELAY = 1  # Начальная задержка в секундах между запросами
RATE_LIMIT_DELAY = 180  # Задержка в секундах при превышении лимита
MAX_REQUEST_DELAY = 10  # Максимальная задержка в секундах между запросами
HEADERS = {'accept': 'application/json'}


async def fetch_page(session: aiohttp.ClientSession, page: int) -> Optional[List[Dict[str, Any]]]:
    try:
        async with session.get(API_URL, headers=HEADERS, params={'page': page}) as response:
            if response.status == 429:
                print("Rate limit exceeded. Waiting for retry...")
                for remaining in range(RATE_LIMIT_DELAY, 0, -1):
                    print(f"Retrying in {remaining} seconds...", end='\r')
                    await asyncio.sleep(1)
                print(" " * 30, end='\r')
                return await fetch_page(session, page)
            if response.status != 200:
                response.raise_for_status()
            data = await response.json()
            return data.get('items', [])
    except Exception as e:
        print(f"Failed to fetch page {page}: {str(e)}")
        return None


async def fetch_and_save_data(output_file: str) -> None:
    page: int = 1
    request_delay = INITIAL_REQUEST_DELAY

    async with aiohttp.ClientSession() as session:
        async with aioopen(output_file, 'w') as f:
            header_written = False
            while True:
                data = await fetch_page(session, page)
                if not data:
                    break
                df = pd.DataFrame(data)

                buffer = StringIO()
                df.to_csv(buffer, sep='\t', index=False, header=not header_written)

                await f.write(buffer.getvalue())

                if not header_written:
                    header_written = True
                page += 1

                # Добавление задержки между запросами
                await asyncio.sleep(request_delay)

                # Динамическая настройка задержки
                if request_delay < MAX_REQUEST_DELAY:
                    request_delay *= 1.1  # Увеличиваем задержку на 10%


async def main(output_file: str) -> None:
    print("Fetching data from API...")
    await fetch_and_save_data(output_file)
    print(f"Data saved to {output_file}")


def run() -> None:
    parser = argparse.ArgumentParser(description="Fetch customs data and save to CSV.")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_CSV, help="Output CSV file path")
    args = parser.parse_args()

    asyncio.run(main(args.output))


if __name__ == "__main__":
    run()