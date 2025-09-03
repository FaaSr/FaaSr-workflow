import datetime
import random
import string
from pathlib import Path

NUM_FILES = 5
FILE_SIZE_MB = 10
BYTES_PER_FILE = FILE_SIZE_MB * 1024 * 1024
LOCAL_FOLDER = "/tmp/faasr_benchmark"
REMOTE_FOLDER = "benchmark_uploads"


Path(LOCAL_FOLDER).mkdir(parents=True, exist_ok=True)


def generate_file(filepath: Path, size_bytes: int):
    with filepath.open("w") as f:
        f.write(
            "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
        )


def benchmark_faasr_put_file():
    start_time = datetime.datetime.now()
    successes = 0

    for i in range(NUM_FILES):
        filename = f"file_{i}.txt"
        local_path = Path(LOCAL_FOLDER) / filename
        remote_path = f"{filename}"

        generate_file(local_path, BYTES_PER_FILE)

        try:
            faasr_put_file(  # noqa: F821
                local_file=filename,
                remote_file=remote_path,
                local_folder=LOCAL_FOLDER,
                remote_folder=REMOTE_FOLDER,
            )
            print(f"[✓] Uploaded {filename}")
            successes += 1
        except Exception as e:
            print(f"[x] Failed to upload {filename}: {e}")

    end_time = datetime.datetime.now()
    total_time = (end_time - start_time).total_seconds()
    avg_time = total_time / NUM_FILES

    print("\n--- Benchmark Results ---")
    print(f"Uploaded {successes}/{NUM_FILES} files")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average time per file: {avg_time:.2f} seconds")
