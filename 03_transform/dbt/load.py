import asyncio
from pathlib import Path
import httpx

DATA_DIR = Path(__file__).parent / "data"

async def download_files(base_url, start, end):
    """Download files start..end concurrently (max 4 at once) and save each to data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(4)

        async def fetch(i):
            """Fetch one file, write it to disk, and print a status line."""
            async with semaphore:
                url = base_url.format(i=f"{i:02d}")
                response = await client.get(url)
                path = DATA_DIR / url.rsplit("/", 1)[-1]
                path.write_bytes(response.content)
                print(f"Fetched {path.name} ({len(response.content) / 1e6:.1f} MB)")
                return path

        tasks = [fetch(i) for i in range(start, end + 1)]
        return await asyncio.gather(*tasks)

# Usage
paths = asyncio.run(download_files(
    "https://s3.amazonaws.com/uvasds-data/taxi/yellow_tripdata_2025-{i}.parquet",
    start=1,
    end=12
))
print(f"Done: {len(paths)} files saved to {DATA_DIR}")
