from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import os

def main():
    # User login using DefaultAzureCredential
    print("Welcome to Azure Storage Manager")

    try:
        # Authenticate using Azure AD (DefaultAzureCredential)
        credential = DefaultAzureCredential()

        # Let user input their storage account name
        storage_account_name = input("Enter your Azure Storage Account name: ")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )

        print("\nAzure Storage Account connected successfully.")

        # List all containers in the storage account
        print("\nListing Containers:")
        containers = blob_service_client.list_containers()
        container_names = [container['name'] for container in containers]

        for container_name in container_names:
            print(f"- {container_name}")

        # Select a container to perform operations
        selected_container_name = input("\nEnter the container name to work with: ")
        if selected_container_name not in container_names:
            print("Invalid container name.")
            return

        container_client = blob_service_client.get_container_client(selected_container_name)

        # List blobs in the container
        print(f"\nListing blobs in container '{selected_container_name}':")
        blob_list = container_client.list_blobs()

        for blob in blob_list:
            print(f"- {blob.name}")

        # Perform upload operation
        upload_file_path = input("\nEnter the path of the file to upload: ")
        if os.path.exists(upload_file_path):
            blob_name = os.path.basename(upload_file_path)
            blob_client = container_client.get_blob_client(blob_name)

            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            print(f"File '{blob_name}' uploaded to container '{selected_container_name}'.")
        else:
            print("File does not exist.")

        # Perform download operation
        download_blob_name = input("\nEnter the name of the blob to download: ")
        blob_client = container_client.get_blob_client(download_blob_name)

        if blob_client.exists():
            download_file_path = os.path.join(os.getcwd(), download_blob_name)
            with open(download_file_path, "wb") as file:
                file.write(blob_client.download_blob().readall())
            print(f"Blob '{download_blob_name}' downloaded to '{download_file_path}'.")
        else:
            print("Blob does not exist.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
