import asyncio
import pandas as pd
import os
import json
import logging
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
from typing import Dict, List, Tuple
from pathlib import Path


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

    def setup_logging(self):
        """Initialize logging configuration."""
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
        """Scrape data for a single automotive category."""
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
                        await asyncio.sleep(self.page_delay)  # Delay between page requests
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")

        return car_data

    async def save_to_excel(self, automotive_name: str, car_data: List[Dict]) -> str:
        """Save data to an Excel file."""
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

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism and folder management."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # Ensure a parent folder exists
            parent_folder = "Scraper Data"
            parent_folder_id = drive_saver.create_folder(parent_folder)

            # Ensure a subfolder for yesterday's date exists
            folder_id = drive_saver.create_folder(yesterday, parent_folder_id)

            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            drive_saver.save_files([file], folder_id=folder_id)
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} to Google Drive folder '{yesterday}'")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            await asyncio.sleep(self.upload_retry_delay)
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")

        except Exception as e:
            self.logger.error(f"Error managing Google Drive folder for {yesterday}: {e}")

        return uploaded_files

    async def scrape_all_automotives(self):
        """Scrape all automotives in chunks."""
        self.temp_dir.mkdir(exist_ok=True)

        # Setup Google Drive
        try:
            credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("HIERARCHIAL_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split automotives into chunks
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
                await asyncio.sleep(2)  # Delay between task creation

            pending_uploads = []
            for automotive_name, task in tasks:
                car_data = await task
                if car_data:
                    excel_file = await self.save_to_excel(automotive_name, car_data)
                    if excel_file:
                        pending_uploads.append(excel_file)

            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                # Clean up uploaded files
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
