import asyncio
import nest_asyncio
import re
import json
from playwright.async_api import async_playwright  # Playwright async API for browser automation
from DetailsScraper import DetailsScraping  # Custom scraper for extracting car details
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Define the CarScraper class
class CarScraper:
    def __init__(self, url):
        self.url = url  # URL of the main page containing all car brands
        self.data = []  # List to store scraped brand and car data

    # Asynchronous method to scrape brands and their associated car details
    async def scrape_brands_and_types(self):
        # Start Playwright context
        async with async_playwright() as p:
            # Launch headless Chromium browser
            browser = await p.chromium.launch(headless=True)
            # Open a new page
            page = await browser.new_page()
            # Navigate to the given URL
            await page.goto(self.url)

            # Select all brand elements (anchor tags inside the brand container)
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            # If no brands are found, log it and return empty data
            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            # Loop over each brand element
            for element in brand_elements:
                # Extract the title of the brand (e.g., Toyota, BMW)
                title = await element.get_attribute('title')
                # Extract the relative or absolute link to the brand page
                brand_link = await element.get_attribute('href')

                if brand_link:
                    # Construct base URL (protocol + domain) from the input URL
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    # Construct full link: prepend base_url if it's a relative link
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link

                    # Print the complete brand link for debugging
                    print(f"Full brand link: {full_brand_link}")

                    # Open a new browser page to access brand-specific cars
                    new_page = await browser.new_page()
                    await new_page.goto(full_brand_link)

                    # Create an instance of the DetailsScraping class to extract car info
                    details_scraper = DetailsScraping(full_brand_link)
                    # Get detailed car data from the brand page
                    car_details = await details_scraper.get_car_details()
                    # Close the temporary brand page
                    await new_page.close()

                    # Store the extracted information in the data list
                    self.data.append({
                        'brand_title': title,  # Brand name
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # Link template for pagination
                        'available_cars': car_details,  # List of car details scraped
                    })

                    # Log the brand and link found
                    print(f"Found brand: {title}, Link: {full_brand_link}")

            # Close the browser after scraping is done
            await browser.close()
        
        # Return the collected data
        return self.data
