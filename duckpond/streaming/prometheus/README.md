# Prometheus Protocol Buffer Files

This directory contains the Protocol Buffer definitions and generated Python code for the Prometheus Remote Write protocol.

## Files

- `types.proto` - Core Prometheus data types (TimeSeries, Label, Sample, etc.)
- `remote.proto` - Remote Write/Read protocol messages (imports types.proto)
- `types_pb2.py` - Generated Python code from types.proto
- `remote_pb2.py` - Generated Python code from remote.proto

## Important: Dependency Resolution

The `remote.proto` file depends on `types.proto` via an import statement. This means that when `remote_pb2.py` is loaded, it needs `types_pb2.py` to already be registered in the protobuf descriptor pool.

**Manual Fix Applied:** The generated `remote_pb2.py` file has been manually modified to include:

```python
from duckpond.streaming.prometheus import types_pb2 as types__pb2
```

This import ensures that `types.proto` is loaded into the descriptor pool before `remote.proto` attempts to reference its types.

## Regenerating Protocol Buffer Files

If you need to regenerate the Python code from the `.proto` files, use the following commands:

```bash
# Navigate to the prometheus directory
cd duckpond/streaming/prometheus

# Generate types_pb2.py first (no dependencies)
python -m grpc_tools.protoc \
    --python_out=. \
    --proto_path=. \
    types.proto

# Generate remote_pb2.py (depends on types.proto)
python -m grpc_tools.protoc \
    --python_out=. \
    --proto_path=. \
    remote.proto
```

**After regenerating `remote_pb2.py`, you MUST manually add the import:**

Add this line after the `_sym_db = _symbol_database.Default()` line:

```python
from duckpond.streaming.prometheus import types_pb2 as types__pb2
```

Without this import, you will get the error:
```
✗ Server error: Couldn't build proto file into descriptor pool: Depends on file 'types.proto', but it has not been loaded
```

## Why This Is Necessary

The Protocol Buffer compiler (`protoc`) doesn't always generate the correct Python imports for cross-file dependencies, especially when using `--python_out` without additional plugin options. The generated code references types from `types.proto` but doesn't explicitly import the corresponding Python module.

This is a known issue with protobuf code generation. The manual import ensures the dependency is properly resolved at runtime.

## Dependencies

These proto files are from the official Prometheus remote write specification:
- Repository: https://github.com/prometheus/prometheus
- License: Apache License 2.0
- Path: prompb/remote.proto and prompb/types.proto

## Usage in DuckPond

The generated protobuf classes are used by:
- `protocol.py` - Handles Prometheus Remote Write protocol (snappy decompression, protobuf decoding)
- `parser.py` - Parses Prometheus protobuf messages into Python data structures
- `converter.py` - Converts Prometheus data to Apache Arrow format
- `ingestor.py` - Streaming ingestion of Prometheus metrics into DuckPond

The API endpoint for Prometheus remote write is:
```
POST /api/v1/stream/prometheus/{dataset_name}
```
