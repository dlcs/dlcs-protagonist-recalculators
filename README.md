# dlcs-protagonist-recalculators

This repository contains a set of lambda functions designed to help recalculate tables in the
database that have gone out of sync.  These are as follows:

## Entity Counter Recalculator

This resets `customer-images` and `space-images` within the `EntityCounter` table to match details from the `Images`
table. This is completed by performing an `UPSERT` on the database.
It passes details from these changes into cloudwatch metrics for the following details:

- difference between `EntityCounter` and `Images` for customer images and space images
- deletions required to match the `EntityCounter` table to `Images` for customer images and space images


## Local development

### environment variables

There are several environment variables that need to be set.  The list of these variables has an example file at the
root of this project called `.env-dist`.  You need to copy and set the variables in this file into a `.env` file.
There is also a powershell script to pull these variables into the console called `SetEnvFile.ps1`.  This is required
To be set before the lambda functions can be run.

### prerequisites

prerequisites for the project can be installed from `pip` using the following command:

```powershell
pip install -r requirements.txt
```

### running

The lambda functions can be run using `python-lambda-local`, which should have been installed from `requirements.txt`
and can run the lambda function using the below command:

```powershell
python-lambda-local -f <function name> <lambda location> <event json location>
```

An example of this is below, if running from the root of this project:

```powershell
python-lambda-local -f lambda_handler .\entity-counter-recalculator\main.py .\entity-counter-recalculator\event.json
```
