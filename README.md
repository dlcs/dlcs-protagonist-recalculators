# dlcs-protagonist-recalculators

This repository contains a set of functions designed to help recalculate tables in the
database that have gone out of sync.  These are as follows:

## Entity Counter Recalculator

This resets `customer-images` and `space-images` within the `EntityCounter` table to match details from the `Images`
table. This is completed by performing an `UPSERT` on the database.
It passes details from these changes into cloudwatch metrics for the following details:

- Difference between `EntityCounter` and `Images` for customer images and space images
- Deletions required to match the `EntityCounter` table to `Images` for customer images and space images

## Customer Storage Recalculator

Resets `CustomerStorage` table by summing `ImageStorage` rows.

## Local development

### Environment variables

There are several environment variables that need to be set. The list of these variables has an example file at the
root of this project called `.env-dist`.  You need to copy and set the variables in this file into a `.env` file.
There is also a powershell script to pull these variables into the console called `SetEnvFile.ps1`.  This is required
to be set before the scripts can be run.

### Prerequisites

Prerequisites for the project can be installed from `pip` using the following command:

```powershell
pip install -r requirements.txt
```

### Updating requirements

In order to avoid encoding issues in `requirements.txt` when running a `pip freeze` in powershell requires
that the file format be set to `UTF-8`. This can be done with the following command:

```powershell
pip freeze -l | Out-File -Encoding UTF8 requirements.txt
```

### Running

The scripts can be run directly with python or via the built docker container

#### Running directly

First, the environment variables need to be set, as above.

You can run directly using the below commands

```powershell
python <file location>
```

An example of this is below, if running from the root of this project:

```powershell
python .\entity-counter-recalculator\main.py
```

#### Running via docker

The docker container can be built with the following commands:

```powershell
# EntityCounter
docker build -f .\src\EntityCounterDockerfile -t dlcs-entity-counter-recalculator:local .\src

# CustomerStorage
docker build -f .\src\CustomerStorageDockerfile -t dlcs-customer-storage-recalculator:local .\src
```

then run with this command:

```powershell
# EntityCounter
docker run -it --env-file .env dlcs-entity-counter-recalculator:local

# CustomerStorage
docker run -it --env-file .env dlcs-customer-storage-recalculator:local
```
