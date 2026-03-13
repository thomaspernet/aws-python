# AWS Python Utilities (`awsPy`)

Reusable Python library providing wrapper functions for AWS services (S3, Glue, Athena). Used as a shared dependency across multiple data engineering projects.

## Overview

`awsPy` is a custom Python package that simplifies interaction with AWS services for data engineering workflows. It provides high-level abstractions for common operations like uploading data to S3, managing Glue catalogs, running Athena queries, and handling AWS authentication.

### Key Modules

- **`aws_s3`** — S3 file upload/download, listing, and management
- **`aws_glue`** — Glue catalog operations (create/update tables, manage schemas)
- **`aws_athena`** — Athena query execution and result retrieval
- **`aws_authorization`** — AWS credential management and session handling

## Tech Stack

- **Python** — Boto3 (AWS SDK)
- **AWS Services** — S3, Glue, Athena
- **Package Distribution** — pip-installable Python package

## Usage

```python
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector

# Connect to AWS
con = aws_connector.aws_instantiate(credential='path/to/creds.csv', region='eu-west-2')
client = con.client_boto()

# S3 operations
s3 = service_s3.connect_S3(client=client, bucket='datalake-london')

# Glue operations
glue = service_glue.connect_glue(client=client)
```

## Author

**Thomas Pernet-Coudrier**
