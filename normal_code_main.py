import asyncio
import pandas as pd
import os
import json
import logging
import time
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
from typing import Dict, List, Tuple
from pathlib import Path
from googleapiclient.errors import HttpError

class NormalMainScraper:
    def __init__(self, automotives_data: Dict[str, List[Tuple[str, int]]]):
        self.automotives_data = automotives_data
        self.chunk_size = 2  # Number of automotives processed per chunk
        self.max_concurrent_links = 2  # Max links processed simultaneously
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.upload_retry_delay = 15  # Retry delay in seconds
        self.page_delay = 3  # Delay between page requests
        self.chunk_delay = 10  # Delay between chunks
        self.drive_saver = None

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    async def scrape_automotive(self, automotive_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        self.logger.info(f"Starting to scrape {automotive_name}")
        car_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)
                    scraper = DetailsScraping(url)
                    try:
                        cars = await scraper.get_car_details()
                        for car in cars:
                            if car.get("date_published", "").split()[0] == yesterday:
                                car_data.append(car)
                        await asyncio.sleep(self.page_delay)
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue

        return car_data

    async def save_to_excel(self, automotive_name: str, car_data: List[Dict]) -> str:
        if not car_data:
            self.logger.info(f"No data to save for {automotive_name}, skipping Excel file creation.")
            return None

        excel_file = Path(f"{automotive_name}.xlsx")
        try:
            df = pd.DataFrame(car_data)
            df.to_excel(excel_file, index=False)
            self.logger.info(f"Successfully saved data for {automotive_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    def upload_files_with_retry(self, files: List[str]):
        for file in files:
            attempt = 0
            while attempt < self.upload_retries:
                try:
                    self.logger.info(f"Attempting to upload file: {file}")
                    self.drive_saver.save_files([file])
                    self.logger.info(f"File {file} uploaded successfully.")
                    break
                except HttpError as e:
                    if e.resp.status == 404:
                        self.logger.error(f"Parent folder not found for file {file}. Skipping upload.")
                        break
                    else:
                        self.logger.warning(f"Upload failed for {file}, attempt {attempt + 1} of {self.upload_retries}. Error: {e}")
                        attempt += 1
                        time.sleep(self.upload_retry_delay)
                except Exception as e:
                    self.logger.error(f"Unexpected error while uploading {file}: {e}")
                    break
            else:
                self.logger.error(f"Max retries reached. Could not upload {file}.")

    async def scrape_all_automotives(self):
        self.temp_dir.mkdir(exist_ok=True)

        try:
            credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("HIERARCHIAL_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            self.drive_saver = SavingOnDrive(credentials_dict)
            self.drive_saver.authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        automotive_chunks = [
            list(self.automotives_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.automotives_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        for chunk_index, chunk in enumerate(automotive_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(automotive_chunks)}")

            tasks = []
            for automotive_name, urls in chunk:
                task = asyncio.create_task(self.scrape_automotive(automotive_name, urls, semaphore))
                tasks.append((automotive_name, task))
                await asyncio.sleep(2)

            pending_uploads = []
            for automotive_name, task in tasks:
                try:
                    car_data = await task
                    if car_data:
                        excel_file = await self.save_to_excel(automotive_name, car_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {automotive_name}: {e}")

            if pending_uploads:
                self.upload_files_with_retry(pending_uploads)
                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            if chunk_index < len(automotive_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

if __name__ == "__main__":
    automotives_data = {
        "سيارات كلاسيكية": [("https://www.q84sale.com/ar/automotive/classic-cars/{}", 3)],
        "سيارات سكراب": [("https://www.q84sale.com/ar/automotive/junk-cars/{}", 3)],
        "مطلوب و نشترى سيارات": [("https://www.q84sale.com/ar/automotive/wanted-cars/{}", 4)],
        "قطع غيار": [("https://www.q84sale.com/ar/automotive/spare-parts-3406/{}", 3)],
        "قوارب و جت سكى": [("https://www.q84sale.com/ar/automotive/watercraft/{}", 3)],
        "اكسسوارات المركبات": [("https://www.q84sale.com/ar/automotive/automotive-accessories/{}", 6)],
        "مركبات و معدات": [("https://www.q84sale.com/ar/automotive/cmvs/{}", 2)],
        "تأجير": [("https://www.q84sale.com/ar/automotive/rentals/{}", 1)],
        "عربات الطعام": [("https://www.q84sale.com/ar/automotive/food-trucks/{}", 1)],
    }

    async def main():
        scraper = NormalMainScraper(automotives_data)
        await scraper.scrape_all_automotives()

    # Run everything in the async event loop
    asyncio.run(main())

