# Required imports
import asyncio
import pandas as pd
import os
import json
import logging
import time
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping  # Custom scraper class to get car details from a page
from SavingOnDrive import SavingOnDrive  # Custom class for saving files to Google Drive
from typing import Dict, List, Tuple
from pathlib import Path
from googleapiclient.errors import HttpError  # To handle Google Drive errors

# Main class for scraping automotive data
class NormalMainScraper:
    def __init__(self, automotives_data: Dict[str, List[Tuple[str, int]]]):
        # Dictionary of automotive categories and their corresponding URL templates and page counts
        self.automotives_data = automotives_data
        self.chunk_size = 2  # Number of automotive categories to process at once
        self.max_concurrent_links = 2  # Max number of concurrent link scraping tasks
        self.logger = logging.getLogger(__name__)  # Logger for debugging/info
        self.setup_logging()  # Set up log formatting and file output
        self.temp_dir = Path("temp_files")  # Temporary directory for storing Excel files
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3  # Number of times to retry upload if it fails
        self.upload_retry_delay = 15  # Wait time between upload retries
        self.page_delay = 3  # Delay between scraping pages to reduce server load
        self.chunk_delay = 10  # Delay between chunks of scraping jobs
        self.drive_saver = None  # Placeholder for the Google Drive saving object

    # Configures logging to both console and a file
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # Output to console
                logging.FileHandler('scraper.log')  # Output to log file
            ]
        )
        self.logger.setLevel(logging.INFO)

    # Scrapes all pages of a specific automotive category
    async def scrape_automotive(self, automotive_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        self.logger.info(f"Starting to scrape {automotive_name}")
        car_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Get yesterday's date as string

        async with semaphore:  # Limit concurrency using semaphore
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)  # Format URL with page number
                    scraper = DetailsScraping(url)  # Initialize scraper
                    try:
                        cars = await scraper.get_car_details()  # Get list of car details from page
                        for car in cars:
                            if car.get("date_published", "").split()[0] == yesterday:
                                car_data.append(car)  # Only add cars published yesterday
                        await asyncio.sleep(self.page_delay)  # Delay before next page
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")  # Log and skip on error
                        continue

        return car_data

    # Saves list of car data into an Excel file
    async def save_to_excel(self, automotive_name: str, car_data: List[Dict]) -> str:
        if not car_data:
            self.logger.info(f"No data to save for {automotive_name}, skipping Excel file creation.")
            return None

        excel_file = Path(f"{automotive_name}.xlsx")  # File path
        try:
            df = pd.DataFrame(car_data)  # Convert list to DataFrame
            df.to_excel(excel_file, index=False)  # Save to Excel
            self.logger.info(f"Successfully saved data for {automotive_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    # Tries uploading files to Google Drive with retries
    def upload_files_with_retry(self, files: List[str]):
        for file in files:
            attempt = 0
            while attempt < self.upload_retries:
                try:
                    self.logger.info(f"Attempting to upload file: {file}")
                    self.drive_saver.save_files([file])  # Upload file
                    self.logger.info(f"File {file} uploaded successfully.")
                    break
                except HttpError as e:
                    if e.resp.status == 404:
                        self.logger.error(f"Parent folder not found for file {file}. Skipping upload.")
                        break
                    else:
                        self.logger.warning(f"Upload failed for {file}, attempt {attempt + 1} of {self.upload_retries}. Error: {e}")
                        attempt += 1
                        time.sleep(self.upload_retry_delay)  # Wait before retry
                except Exception as e:
                    self.logger.error(f"Unexpected error while uploading {file}: {e}")
                    break
            else:
                self.logger.error(f"Max retries reached. Could not upload {file}.")

    # Main orchestrator function to scrape all automotives
    async def scrape_all_automotives(self):
        self.temp_dir.mkdir(exist_ok=True)

        try:
            credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')  # Read credentials from env variable
            if not credentials_json:
                raise EnvironmentError("HIERARCHIAL_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            self.drive_saver = SavingOnDrive(credentials_dict)  # Initialize Google Drive uploader
            self.drive_saver.authenticate()  # Authenticate
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Break data into smaller chunks
        automotive_chunks = [
            list(self.automotives_data.items())[i:i + self.chunk_size]
            for i in range(0, len(self.automotives_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)  # Limit concurrency

        for chunk_index, chunk in enumerate(automotive_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(automotive_chunks)}")

            tasks = []
            for automotive_name, urls in chunk:
                task = asyncio.create_task(self.scrape_automotive(automotive_name, urls, semaphore))  # Create async task
                tasks.append((automotive_name, task))
                await asyncio.sleep(2)  # Stagger task start slightly

            pending_uploads = []
            for automotive_name, task in tasks:
                try:
                    car_data = await task  # Wait for scrape result
                    if car_data:
                        excel_file = await self.save_to_excel(automotive_name, car_data)  # Save if non-empty
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {automotive_name}: {e}")

            # Upload and clean up
            if pending_uploads:
                self.upload_files_with_retry(pending_uploads)
                for file in pending_uploads:
                    try:
                        os.remove(file)  # Delete local file
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            # Wait before next chunk
            if chunk_index < len(automotive_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)

# Sample data to scrape: category -> (URL template, number of pages)
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

    # Async main function to start scraping
    async def main():
        scraper = NormalMainScraper(automotives_data)
        await scraper.scrape_all_automotives()

    # Run the whole scraper
    asyncio.run(main())
