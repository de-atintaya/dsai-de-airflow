import os
import re

# Define the data directory
DATA_DIR = "/home/s.lopezmedina/proyectos_maestria_git/dsai-de-airflow/data"

def rename_parquet_files():
    """
    Renames parquet files in the data directory by replacing colons in filenames
    (from ISO datetime) with hyphens to make them Windows-compatible.
    """
    count = 0
    # List all files in the directory
    for filename in os.listdir(DATA_DIR):
        # We only care about parquet files
        if not filename.endswith(".parquet"):
            continue

        # Check if the filename contains the character ':' which is forbidden in Windows
        if ":" in filename:
            # Replace ':' with '-' to make it safe
            new_filename = filename.replace(":", "-")
            
            old_path = os.path.join(DATA_DIR, filename)
            new_path = os.path.join(DATA_DIR, new_filename)
            
            try:
                os.rename(old_path, new_path)
                print(f"Renamed: {filename} -> {new_filename}")
                count += 1
            except OSError as e:
                print(f"Error renaming {filename}: {e}")

    print(f"Total files renamed: {count}")

if __name__ == "__main__":
    rename_parquet_files()
