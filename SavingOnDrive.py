import os
import json
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError

# This class handles authentication and uploading files to multiple folders in Google Drive.
class SavingOnDrive:
    def __init__(self, credentials_dict):
        # Initializes with the credentials dictionary and sets up necessary variables
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Permission scope to access Google Drive
        self.service = None  # Will hold the authenticated service client

        # List of Google Drive parent folder IDs to upload into
        self.parent_folder_ids = [
            '1PBrE4Qfage1WgcS_rRjNpO7hW50emOaT',  # Confirmed working folder
            '1mLRdYvZb56LS10M0hjpzYTVrQiyWGN7m'   # Folder with known upload issues
        ]
        self.setup_logging()  # Initialize logging configuration

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),                 # Print logs to console
                logging.FileHandler("drive_upload.log")  # Save logs to file
            ]
        )

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            # Authenticate using the provided service account credentials
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build a Google Drive API client
            self.service = build('drive', 'v3', credentials=creds)
            logging.info("Successfully authenticated with Google Drive.")
        except Exception as e:
            # Log and raise any errors encountered during authentication
            logging.error(f"Authentication error: {e}")
            raise

    def get_or_create_folder(self, folder_name, parent_folder_id):
        """Retrieve folder ID if it exists, otherwise create a new folder."""
        try:
            # Query to check if folder already exists under the given parent
            query = (f"name='{folder_name}' and "
                     f"'{parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            folders = results.get('files', [])
            if folders:
                # Return the ID of the first matching folder
                return folders[0]['id']
            
            # Folder does not exist, so create a new one
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            # Create the folder and return its ID
            folder = self.service.files().create(body=folder_metadata, fields='id').execute()
            logging.info(f"Created new folder: {folder_name} (ID: {folder['id']})")
            return folder['id']
        except HttpError as e:
            # Specific handling if parent folder doesn't exist
            if e.resp.status == 404:
                logging.error(f"Parent folder not found (ID: {parent_folder_id}). Skipping this folder.")
                return None
            # General error logging
            logging.error(f"Error getting/creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            if not folder_id:
                # Skip if folder ID is not valid
                logging.error(f"Invalid folder ID for file {file_name}. Skipping upload.")
                return None

            # Prepare file metadata for upload
            file_metadata = {
                'name': os.path.basename(file_name),  # Use only the file name, not full path
                'parents': [folder_id]                # Upload to the specified folder
            }

            # Wrap the file for upload
            media = MediaFileUpload(file_name, resumable=True)

            # Upload the file and retrieve its ID
            uploaded_file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            logging.info(f"Uploaded {file_name} to Google Drive (File ID: {uploaded_file['id']})")
            return uploaded_file['id']
        except Exception as e:
            # Log any upload failure
            logging.error(f"Error uploading {file_name}: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in all valid parent folders."""
        try:
            # Determine yesterday's date to name subfolders
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            # Iterate over each parent folder
            for parent_folder_id in self.parent_folder_ids:
                # Create or get the folder for yesterday under each parent
                folder_id = self.get_or_create_folder(yesterday, parent_folder_id)

                if not folder_id:
                    # Skip this parent folder if subfolder creation failed
                    logging.error(f"Skipping uploads to {parent_folder_id} because folder ID retrieval failed.")
                    continue

                # Upload all specified files into the folder
                for file_name in files:
                    self.upload_file(file_name, folder_id)
            
            logging.info(f"All files uploaded successfully to valid parent folders.")
        except Exception as e:
            # Log any fatal error during the process
            logging.error(f"Error saving files: {e}")
            raise
