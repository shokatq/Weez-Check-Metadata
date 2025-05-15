from flask import Flask, request, jsonify
from azure.storage.blob import BlobServiceClient, ContentSettings
import os

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

# Azure Storage Configuration
AZURE_STORAGE_CONNECTION_STRING_2 = os.getenv('AZURE_STORAGE_CONNECTION_STRING_1')
CONTAINER_NAME = 'weez-user-data'
METADATA_CONTAINER_NAME = 'weez-files-metadata'

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING_1'))
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
metadata_blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_METADATA_STORAGE_CONNECTION_STRING'))
metadata_container_client = metadata_blob_service_client.get_container_client(METADATA_CONTAINER_NAME)

# Function to check if metadata exists in Azure Blob Storage
def check_metadata_exists(file_name, user_id):
    blob_client = metadata_container_client.get_blob_client(f"{user_id}/{file_name}.json")
    return blob_client.exists()

# Function to upload the file to Blob Storage if metadata does not exist
def upload_file_to_blob(file, file_name, user_id):
    try:
        # Initialize the blob client
        blob_client = container_client.get_blob_client(f"{user_id}/{file_name}")

        # Upload file to Azure Blob Storage
        blob_client.upload_blob(
            file,
            overwrite=True,
            blob_type="BlockBlob",
            content_settings=ContentSettings(content_type="application/octet-stream")
        )

        print(f"File {file_name} uploaded successfully for user {user_id}.")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

# API Endpoint to check if metadata exists and upload if it doesn’t
@app.route('/check-metadata', methods=['POST'])
def check_metadata():
    try:
        # Extract form data
        user_id = request.form.get('userID')
        file_name = request.form.get('fileName')
        file = request.files.get('file')

        if not user_id or not file_name or not file:
            return jsonify({'error': 'userID, fileName, and file are required'}), 400

        # Check for existing metadata
        exists = check_metadata_exists(file_name, user_id)

        if exists:
            return jsonify({'exists': True}), 200
        else:
            # If metadata doesn't exist, upload the file
            upload_successful = upload_file_to_blob(file, file_name, user_id)
            if upload_successful:
                return jsonify({'exists': False, 'message': 'File uploaded successfully.'}), 201
            else:
                return jsonify({'exists': False, 'error': 'File upload failed.'}), 500
    except Exception as e:
        print('Error checking metadata existence or uploading file:', e)
        return jsonify({'error': 'Unable to check metadata existence or upload file.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000, threaded=True)
