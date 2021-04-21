User Guide
==========

Introduction
------------
The Dynamic Annotation DB (DADB) is an interface layer used to create, modify and interact
with a PostgreSQL database used to store spatial point annotations and segmentation data
defined by a chunkedgraph.

DADB has three classes used for interfacing. The base interface class is used for 
creating the database with the required schemas as well as general utility methods. The 
annotation client and materialization client inherent the base interface class and have extended
functionality for querying data.

The Materialization Engine uses the DADB to create versioned materializations. The Annotation Engine also
uses DADB to allow clients to insert, modify and query annotations in the Materialization Database.


Installation
------------

The DADB interface layer can be installed locally or via pip.

To install locally:

.. code-block:: bash

    $ git clone https://github.com/seung-lab/DynamicAnnotationDB
    $ cd DynamicAnnotationDB
    $ setup.py install

Alternatively one can install DADB via pip:

.. code-block:: bash

    $ pip install DynamicAnnotationDB


Base Interface Layer
--------------------
The base interface layer contains various methods to create/select and modify PostgreSQL databases.


**Getting started**

Assuming a PostgreSQL database is running the DADB will add the required schemas necessary to create 
a Materialization Database. 

The Materialization Database name is defined as an 'aligned_volume'.

To setup a new database instantiate a new client:

.. code-block:: python

    from dynamicannotationdb.interface import DynamicAnnotationInterface

    # database uri
    sql_uri = "postgresql://postgres:postgres@localhost:5432"

    # aligned volume will be the name of the new database
    aligned_volume = "example_database"

    # connection to the database
    interface = DynamicAnnotationInterface(sql_uri)

    # create a new database if not exists, else it will connect to the existing database
    new_db_sql_uri = interface.create_or_select_database(aligned_volume, sql_uri)


A database named "example_database" will now be created. It features the following changes:

* PostGIS extension will be installed.
* Annotation and Segmentation Metadata tables will be created. 

The database will be ready for creating annotation and segmentation tables.
To create a new annotation table we will need to define a schema. The schemas are defined
in the EM Annotation Schemas library. In this example we will use the 'synapse' schema.

.. code-block:: python

    # define the table name
    table_name = "example_annotation_table"

    # define schema type
    schema_type = "synapse"

    # create a table description which will be inserted into the Annotation Metadata table 
    description = "This is an example annotation table"

    # define a user that created the table for reference and collaboration
    user_id = "foo@bar.org"

    # create a new annotation table 
    table = interface.create_annotation_table(table_name, schema_type, description, user_id)

    print(table)
    "example_annotation_table"


Annotation Interface Layer
--------------------------
The annotation layer provides CRUD operations on annotation tables. In addition it supports creating,
selecting and deleting annotation tables.

Let's add an annotation to the database:

.. warning::

    Currently there is a limit of 10,000 annotations that can be inserted into the database at a time.
    For inserting large datasets it is recommend to use the Materialization Engine Bulk Upload API.

.. code-block:: python

    from dynamicannotationdb.annotation_client import DynamicAnnotationClient

    # database uri
    sql_uri = "postgresql://postgres:postgres@localhost:5432"

    # aligned volume (the name of the database)
    aligned_volume = "example_database"

    # create a client instance
    annotation_client = DynamicAnnotationClient(aligned_volume, sql_uri)

    # get tables in the database
    tables = annotation_client.get_valid_table_names()
    
    # define an annotation, can be list of dicts
    # ids can be either automatically inserted by the database or define manually
    example_synapse_data = [
        {
            "pre_pt": {"position": [121, 123, 1232]},
            "ctr_pt": {"position": [128, 143, 1232]},
            "post_pt": {"position": [235, 187, 1232]},
            "size": 1,
        },
        {
            "id": 2,
            "pre_pt": {"position": [321, 525, 5232]},
            "ctr_pt": {"position": [343, 522, 5233]},
            "post_pt": {"position": [333, 595, 5233]},
            "size": 1,
        }
    ]

    table_name = tables[0] # example_annotation_table 

    # insert data, returns True if successful
    is_committed = annotation_client.insert_annotations(table_name, example_synapse_data)

.. note::
    Inserting annotations without specifying Ids will have auto-incrementing Ids created 
    by the database. Generally one should either only predefine the ids or have the database
    define them.


Materialization Interface Layer
-------------------------------