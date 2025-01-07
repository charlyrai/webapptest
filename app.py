from fastapi import FastAPI, Form, UploadFile, File, HTTPException
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from fastapi.responses import StreamingResponse
from typing import List
import os
from io import BytesIO

app = FastAPI()

# Helper function to authenticate and get BlobServiceClient
def get_blob_service_client(tenant_id: str, client_id: str, client_secret: str, storage_account_name: str):
    try:
        # Authenticate with Azure using service principal credentials
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        # Create BlobServiceClient using the storage account name and credentials
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        return blob_service_client
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/containers")
async def list_containers(
    tenant_id: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    storage_account_name: str = Form(...)
) -> List[str]:
    try:
        # Get BlobServiceClient
        blob_service_client = get_blob_service_client(tenant_id, client_id, client_secret, storage_account_name)
        # List all containers
        containers = blob_service_client.list_containers()
        container_names = [container.name for container in containers]
        return container_names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/blobs")
async def list_blobs(
    tenant_id: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    storage_account_name: str = Form(...),
    container_name: str = Form(...)
) -> List[str]:
    try:
        # Get BlobServiceClient
        blob_service_client = get_blob_service_client(tenant_id, client_id, client_secret, storage_account_name)
        # Get ContainerClient for specific container
        container_client = blob_service_client.get_container_client(container_name)
        # List all blobs in the container
        blobs = container_client.list_blobs()
        blob_names = [blob.name for blob in blobs]
        return blob_names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload")
async def upload_blob(
    tenant_id: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    storage_account_name: str = Form(...),
    container_name: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        # Get BlobServiceClient
        blob_service_client = get_blob_service_client(tenant_id, client_id, client_secret, storage_account_name)
        # Get BlobClient for the specific blob to upload
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file.filename)

        # Read file content
        file_content = await file.read()
        # Upload the blob
        blob_client.upload_blob(file_content, overwrite=True)

        return {"message": f"File '{file.filename}' uploaded successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/download")
async def download_blob(
    tenant_id: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    storage_account_name: str = Form(...),
    container_name: str = Form(...),
    blob_name: str = Form(...)
):
    try:
        # Get BlobServiceClient
        blob_service_client = get_blob_service_client(tenant_id, client_id, client_secret, storage_account_name)
        # Get BlobClient for the specific blob to download
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download the blob
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()

        return StreamingResponse(BytesIO(blob_data), media_type="application/octet-stream", headers={"Content-Disposition": f"attachment; filename={blob_name}"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
