# %%
import argparse
import gzip
import pickle
import os
import pyarrow.plasma as plasma
from lazy_core import gen_plasma_functions, get_plasma_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# %%
store = 10

# %%
def dump_store_to_pickle_threaded(store: str, output_dir: str, max_workers: int = 4, dry_run: bool = False, zip: bool = False):
    # Create a subfolder for the store
    store_output_dir = os.path.join(output_dir, f"store_{store}")
    os.makedirs(store_output_dir, exist_ok=True)

    # Connect to the Plasma store
    client = get_plasma_client(db=store)
    object_ids = client.list()
    print(f"[{store}] Found {len(object_ids)} objects in the Plasma store.")

    if dry_run:
        print(f"[{store}] Dry run: {len(object_ids)} objects would be saved to {store_output_dir}.")
        return

    # Metadata dictionary and lock for thread safety
    metadata = {}
    metadata_lock = Lock()

    def export_single_object_with_zip(client, obj_id, dump_dir, metadata_dict, metadata_lock, zip):
        try:
            obj = client.get(obj_id)
            obj_id_str = obj_id.binary().hex()
            filename = f"{dump_dir}/object_{obj_id_str}.pkl.gz" if zip else f"{dump_dir}/object_{obj_id_str}.pkl"

            with (gzip.open(filename, "wb") if zip else open(filename, "wb")) as f:
                pickle.dump(obj, f)

            with metadata_lock:
                metadata_dict[obj_id_str] = {
                    'filename': filename,
                    'type': str(type(obj)),
                    'size': len(pickle.dumps(obj))  # Approximate size
                }

            return f"Successfully exported {obj_id_str}"
        except Exception as e:
            return f"Failed to export {obj_id.binary().hex()}: {str(e)}"

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_objid = {
            executor.submit(export_single_object_with_zip, client, obj_id, store_output_dir, metadata, metadata_lock, zip): obj_id
            for obj_id in object_ids
        }

        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_objid):
            result = future.result()
            completed += 1
            print(f"[{completed}/{len(object_ids)}] {result}")

    # Save metadata
    metadata_file = os.path.join(store_output_dir, "metadata.pkl")
    with open(metadata_file, 'wb') as f:
        pickle.dump(metadata, f)

    print(f"[{store}] Export complete! All objects saved to {store_output_dir}/")

# %%
def load_store_from_pickle_threaded(store: str, input_dir: str, max_workers: int = 4, dry_run: bool = False):
    # Locate the subfolder for the store
    store_input_dir = os.path.join(input_dir, f"store_{store}")
    if not os.path.exists(store_input_dir):
        print(f"[{store}] No directory found for this store in {input_dir}. Skipping.")
        return

    # Connect to the Plasma store
    client = get_plasma_client(db=store)

    # List all pickle and gzip files in the store's input directory, excluding metadata.pkl
    pickle_files = [f for f in os.listdir(store_input_dir) if (f.endswith(".pkl") or f.endswith(".pkl.gz")) and f != "metadata.pkl"]
    print(f"[{store}] Found {len(pickle_files)} files in the input directory.")

    if dry_run:
        print(f"[{store}] Dry run: {len(pickle_files)} objects would be loaded into the Plasma store.")
        return

    def load_single_object(client, file_path):
        """Load a single object into the Plasma store"""
        try:
            # Handle gzip-compressed files
            if file_path.endswith(".gz"):
                with gzip.open(file_path, "rb") as f:
                    obj = pickle.load(f)
            else:
                with open(file_path, "rb") as f:
                    obj = pickle.load(f)

            # Generate a unique object ID based on the file name
            obj_id = plasma.ObjectID(bytes.fromhex(os.path.basename(file_path).split("_")[1].split(".")[0]))

            # Put the object into the Plasma store
            client.put(obj, obj_id)

            return f"Successfully loaded {file_path}"
        except Exception as e:
            return f"Failed to load {file_path}: {str(e)}"

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(load_single_object, client, os.path.join(store_input_dir, file)): file
            for file in pickle_files
        }

        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_file):
            result = future.result()
            completed += 1
            print(f"[{completed}/{len(pickle_files)}] {result}")

    print(f"[{store}] Load complete! All objects loaded into the Plasma store.")

# %%
def main():
    parser = argparse.ArgumentParser(description="A tool for dumping and loading Plasma store objects to/from pickle files.")
    parser.add_argument("--mode", choices=["dump", "load"], required=True, help="Operation mode")
    parser.add_argument("--stores", nargs="+", required=True, help="List of plasma store names")
    parser.add_argument("--dir", required=True, help="Directory to save/load .pkl.gz files")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without saving/loading data")
    parser.add_argument("--zip", action="store_true", help="Enable zipping of the pickle files during dumping")
    parser.add_argument("--max-workers", type=int, default=4, help="Number of threads for multi-threaded operations")
    args = parser.parse_args()

    os.makedirs(args.dir, exist_ok=True)

    for store in args.stores:
        if args.mode == "dump":
            dump_store_to_pickle_threaded(store, args.dir, max_workers=args.max_workers, dry_run=args.dry_run, zip=args.zip)
        else:
            load_store_from_pickle_threaded(store, args.dir, max_workers=args.max_workers, dry_run=args.dry_run)

# %%
if __name__ == "__main__":
    main()