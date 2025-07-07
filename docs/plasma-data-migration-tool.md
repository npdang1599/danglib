# Plasma Store Data Migration Utility

This utility provides tools for dumping and loading objects from Plasma stores to/from pickle files. It supports multi-threaded operations and optional gzip compression for efficient data handling.

## Features
- **Dump Plasma Store Objects**: Export objects from a Plasma store to pickle files.
- **Load Plasma Store Objects**: Import objects from pickle files back into a Plasma store.
- **Multi-threaded Operations**: Perform dumping and loading using multiple threads for improved performance.
- **Gzip Compression**: Optionally compress pickle files during dumping.
- **Store-specific Subfolders**: Organize objects by Plasma store in subfolders.

## Usage

### Command-line Arguments

```bash
python data-migration.py --mode <dump|load> --stores <store1 store2 ...> --dir <output_dir> [options]
```

### Required Arguments
- `--mode`: Operation mode. Choose `dump` to export objects or `load` to import objects.
- `--stores`: List of Plasma store names to process.
- `--dir`: Directory to save/load pickle files.

### Optional Arguments
- `--dry-run`: Perform a dry run without saving/loading data.
- `--zip`: Enable gzip compression for pickle files during dumping.
- `--max-workers`: Number of threads for multi-threaded operations (default: 4).

### Examples

#### Dump Objects from Plasma Stores
```bash
python data-migration.py --mode dump --stores 1 2 3 --dir /path/to/output --zip --max-workers 8
```
This command dumps objects from Plasma stores `1`, `2`, and `3` into `/path/to/output`, compressing the files with gzip and using 8 threads.

#### Load Objects into Plasma Stores
```bash
python data-migration.py --mode load --stores 1 2 3 --dir /path/to/output --max-workers 4
```
This command loads objects from `/path/to/output` back into Plasma stores `1`, `2`, and `3` using 4 threads.

#### Perform a Dry Run
```bash
python data-migration.py --mode dump --stores 1 2 3 --dir /path/to/output --dry-run
```
This command performs a dry run, showing what would be dumped without actually saving any files.

## Notes
- Each Plasma store's objects are saved in a subfolder named `store_<store_name>` within the specified output directory.
- The `metadata.pkl` file in each subfolder contains metadata about the dumped objects and is ignored during loading.

## Requirements
- Python 3.10 or higher
- `pyarrow` library for Plasma store operations

## License
This utility is provided under the MIT License.
