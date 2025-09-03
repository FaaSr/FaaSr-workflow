from pathlib import Path

from FaaSr_py.config.debug_config import global_config


def default_func():
    # Create test file
    test_file = Path("/tmp") / "test_upload.txt"
    test_file.write_text("This is a test file for FaaSr.\nLine 2 of the file.")

    # Upload file using faasr_put_file
    faasr_put_file(  # noqa: F821
        local_file="test_upload.txt",
        remote_file="uploaded.txt",
        local_folder="/tmp",
        remote_folder="test-folder",
        server_name="local",
    )

    # Print uploaded file content from the local "bucket"
    uploaded_path = (
        Path(global_config.LOCAL_FILE_SYSTEM_DIR) / "test-folder" / "uploaded.txt"
    )
    if uploaded_path.exists():
        print("\nUploaded file contents:")
        print("-" * 30)
        print(uploaded_path.read_text())
        print("-" * 30)
    else:
        print("Uploaded file not found.")

    # Delete file using faasr_delete_file
    faasr_delete_file(  # noqa: F821
        remote_file="uploaded.txt", remote_folder="test-folder", server_name="local"
    )

    # Confirm deletion
    if not uploaded_path.exists():
        print("File successfully deleted.")
    else:
        print("File still exists after deletion.")
