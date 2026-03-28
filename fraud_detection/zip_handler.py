import os
import zipfile
import uuid
from django.conf import settings
from django.core.files.storage import default_storage

def handle_uploaded_zip(uploaded_file):
    unique_id = str(uuid.uuid4())

    # Save ZIP inside MEDIA_ROOT
    zip_name = f"uploads/{unique_id}.zip"
    zip_path = default_storage.save(zip_name, uploaded_file)

    full_zip_path = os.path.join(settings.MEDIA_ROOT, zip_name)

    # Extract inside MEDIA_ROOT
    extract_dir = os.path.join(settings.MEDIA_ROOT, f"extracted/{unique_id}/")
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(full_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    return extract_dir