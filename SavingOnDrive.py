import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name, parent_id=None):
        """Get folder ID by name and optional parent ID."""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id)'
            ).execute()
            
            files = results.get('files', [])
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name, parent_folder_id=None):
        """Create a new folder in Google Drive."""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]

            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            return folder.get('id')
        except Exception as e:
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_name, resumable=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            return file.get('id')
        except Exception as e:
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files, folder_id=None):
        """Save multiple files to Google Drive."""
        try:
            if folder_id:
                # If folder_id is provided, use it directly
                for file_name in files:
                    self.upload_file(file_name, folder_id)
            else:
                # Original behavior with hardcoded parent folder
                parent_folder_id = '1wwVdI2kT2k_j_pScF13PDhm2hd9EvjRN'
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                folder_id = self.create_folder(yesterday, parent_folder_id)
                
                for file_name in files:
                    self.upload_file(file_name, folder_id)
            print(f"Files uploaded successfully to Google Drive.")
        except Exception as e:
            print(f"Error saving files: {e}")
            raise
