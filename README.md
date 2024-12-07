# DynamicAnnotationDB

[![Actions Status](https://github.com/seung-lab/DynamicAnnotationDB/workflows/DynamicAnnotationDB/badge.svg)](https://github.com/seung-lab/DynamicAnnotationDB/actions)
[![PyPI version](https://badge.fury.io/py/DynamicAnnotationDB.svg)](https://badge.fury.io/py/DynamicAnnotationDB)
[![Documentation Status](https://readthedocs.org/projects/dynamicannotationdb/badge/?version=latest)](https://dynamicannotationdb.readthedocs.io/en/latest/?badge=latest)

DynamicAnnotationDB (DADB) is an interface layer for creating, modifying and interacting with PostgreSQL databases used to store spatial point annotations and segmentation data defined by a chunkedgraph.

## Features

- Create and manage PostgreSQL databases for annotation storage
- Handle spatial point annotations with PostGIS integration
- Support for segmentation data linked to annotations
- Dynamic schema generation and validation
- CRUD operations for annotations and segmentation data
- Reference annotation support with table linking
- Built-in versioning and tracking of modifications

## Installation

You can install DynamicAnnotationDB using pip:

```bash
pip install DynamicAnnotationDB
```

Or install from source:

```bash
git clone https://github.com/CAVEconnectome/DynamicAnnotationDB
cd DynamicAnnotationDB
pip install -e .
```

## Quick Start

Here's a basic example of using DynamicAnnotationDB:

```python
from dynamicannotationdb import DynamicAnnotationInterface

# Connect to database
sql_uri = "postgresql://postgres:postgres@localhost:5432"
aligned_volume = "my_annotations"
interface = DynamicAnnotationInterface(sql_uri, aligned_volume)

# Create a new annotation table
table_name = "synapse_annotations"
interface.annotation.create_table(
    table_name=table_name,
    schema_type="synapse",
    description="Synapse annotations for dataset X",
    user_id="user@example.com",
    voxel_resolution_x=4.0,
    voxel_resolution_y=4.0,
    voxel_resolution_z=40.0
)

# Insert annotations
annotations = [{
    "pre_pt": {"position": [121, 123, 1232]},
    "ctr_pt": {"position": [128, 143, 1232]},
    "post_pt": {"position": [235, 187, 1232]},
    "size": 1
}]

annotation_ids = interface.annotation.insert_annotations(table_name, annotations)
```

## Key Components

The interface consists of several key components:

- **Annotation Client**: Handle CRUD operations for annotation data
- **Segmentation Client**: Manage segmentation data linked to annotations
- **Schema Client**: Generate and validate dynamic schemas
- **Database Client**: Core database operations and metadata management

## Documentation

Full documentation is available at [dynamicannotationdb.readthedocs.io](https://dynamicannotationdb.readthedocs.io/).

## Features in Detail

### Dynamic Schema Support

DADB uses EMAnnotationSchemas to define table structures:

```python
# Create a table with a specific schema
interface.annotation.create_table(
    table_name="my_table",
    schema_type="synapse",
    description="Description",
    user_id="user@example.com",
    voxel_resolution_x=4.0,
    voxel_resolution_y=4.0,
    voxel_resolution_z=40.0
)
```

### Segmentation Support

Link segmentation data to annotations:

```python
# Create a segmentation table
seg_table = interface.segmentation.create_segmentation_table(
    table_name="my_table",
    schema_type="synapse", 
    segmentation_source="seg_source"
)

# Insert linked data
interface.segmentation.insert_linked_annotations(
    table_name="my_table",
    pcg_table_name="seg_source",
    annotations=[...]
)
```

### Reference Tables

Create tables that reference other annotations:

```python
# Create a reference table
interface.annotation.create_table(
    table_name="reference_table",
    schema_type="reference_type",
    description="Reference annotations",
    user_id="user@example.com",
    voxel_resolution_x=4.0,
    voxel_resolution_y=4.0,
    voxel_resolution_z=40.0,
    table_metadata={
        "reference_table": "target_table",
        "track_target_id_updates": True
    }
)
```

## Development

### Requirements

- Python 3.7+
- PostgreSQL with PostGIS extension
- Docker (optional, for testing)

### Testing

Run tests with pytest:

```bash
# Install test requirements
pip install -r test_requirements.txt

# Run tests (requires a PostgreSQL with PostGIS extension running)
pytest

# Run tests with a temporary Docker PostgreSQL instance (preferred local testing method)
pytest --docker=true
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support, please open an issue on the [GitHub repository](https://github.com/seung-lab/DynamicAnnotationDB/issues).