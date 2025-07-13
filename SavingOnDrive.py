import os
import json
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.parent_folder_ids = [  # Updated Parent Folders
            '1qnzV3sACpyrADIpRKVQuzAcirvXvjqo1',  # Confirmed working
            '10nEH6A4H7gvkGBE-81OVXHuGALsw7vab'   # Target folder with upload issues
        ]
        self.setup_logging()

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(), logging.FileHandler("drive_upload.log")]
        )

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds)
            logging.info("Successfully authenticated with Google Drive.")
        except Exception as e:
            logging.error(f"Authentication error: {e}")
            raise

    def get_or_create_folder(self, folder_name, parent_folder_id):
        """Retrieve folder ID if exists, otherwise create a new folder."""
        try:
            query = (f"name='{folder_name}' and "
                     f"'{parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            folders = results.get('files', [])
            if folders:
                return folders[0]['id']
            
            # If folder doesn't exist, create it
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.service.files().create(body=folder_metadata, fields='id').execute()
            logging.info(f"Created new folder: {folder_name} (ID: {folder['id']})")
            return folder['id']
        except HttpError as e:
            if e.resp.status == 404:
                logging.error(f"Parent folder not found (ID: {parent_folder_id}). Skipping this folder.")
                return None
            logging.error(f"Error getting/creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            if not folder_id:
                logging.error(f"Invalid folder ID for file {file_name}. Skipping upload.")
                return None

            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_name, resumable=True)
            uploaded_file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logging.info(f"Uploaded {file_name} to Google Drive (File ID: {uploaded_file['id']})")
            return uploaded_file['id']
        except Exception as e:
            logging.error(f"Error uploading {file_name}: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in all valid parent folders."""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            for parent_folder_id in self.parent_folder_ids:
                folder_id = self.get_or_create_folder(yesterday, parent_folder_id)

                if not folder_id:
                    logging.error(f"Skipping uploads to {parent_folder_id} because folder ID retrieval failed.")
                    continue

                for file_name in files:
                    self.upload_file(file_name, folder_id)
            
            logging.info(f"All files uploaded successfully to valid parent folders.")
        except Exception as e:
            logging.error(f"Error saving files: {e}")
            raise
