# datamart-scripts

This repository contains Python data integration, ETL, reporting, and analytics code primarily involving Toast, Restaurant365 (R365), PostgreSQL, pandas, and SQLAlchemy.

## Runtime and Stack

* Python 3.14
* PostgreSQL
* pandas
* SQLAlchemy
* psycopg2
* Toast APIs
* Restaurant365 APIs
* Scripts are generally executed as modules from the repository root:

  `python -m src.<module>`

Follow the existing virtual environment and project configuration rather than creating a new environment.

## General Development Rules

* Inspect existing modules and shared utilities before creating new implementations.
* Prefer extending shared database/API utilities over duplicating logic across scripts.
* Preserve existing command-line arguments unless explicitly asked to change them.
* Do not rename database columns, CLI arguments, tables, schemas, or API fields without checking their consumers.
* Keep API retrieval, transformation, and database-write concerns reasonably separated.
* Prefer explicit DataFrame transformations that make expected columns and types obvious.

## Debugging

When investigating failures:

1. Identify the actual failing data or code path.
2. Inspect upstream data before patching the failing statement.
3. Check whether empty API responses, empty DataFrames, missing columns, null values, or changed API response structures are involved.
4. Avoid fixes that merely suppress an exception while leaving invalid data downstream.

When useful, add concise diagnostic output temporarily, but remove excessive debugging output once the issue is understood.

## pandas

* Expect API responses to occasionally return no rows.
* Code should handle empty DataFrames without crashing.
* Do not assume a column exists if an empty API response can prevent it from being created.
* Drop invalid rows intentionally rather than allowing malformed records to propagate.
* Be explicit about null handling before database writes.
* Convert pandas-specific missing values such as `pd.NA` to database-compatible values when necessary.
* Avoid chained assignment.
* Avoid deprecated pandas behavior and resolve warnings when practical rather than suppressing them.

## PostgreSQL

PostgreSQL is the authoritative analytical/database layer for this project.

* Prefer parameterized SQL.
* Qualify database objects with their schema when practical.
* Use transactions for multi-step database changes.
* Roll back transactions on failure.
* Do not silently swallow database exceptions.
* Do not use `if_exists="replace"` on established production tables unless explicitly requested.
* Do not automatically drop, truncate, or recreate established tables.
* Do not execute destructive SQL without explicit instruction.
* Preserve existing primary keys, indexes, constraints, and column types unless the task specifically concerns them.

For data synchronization, prefer inserts/upserts that update existing records rather than replacing entire tables.

### R365 Data

* R365 source records generally use the R365 `id` as the primary key.
* Repeated API synchronization should update existing records and insert new records.
* Do not truncate and reload an R365 table merely because the API was queried again.
* Preserve fields supplied by the API unless there is an intentional normalization rule.

## Toast Data

* Toast identifiers such as GUIDs should remain stable identifiers where available.
* Do not assume menu-item names are globally unique.
* Be aware that the same conceptual menu item may have different names or concept prefixes across locations.
* Business-date logic may differ from ordinary midnight-to-midnight calendar dates; preserve the project's established Toast business-date handling.
* Handle locations with no sales or no returned API records without treating that condition as an application failure.

## API Code

* Reuse existing API clients and authentication handling.
* Do not print authentication headers, bearer tokens, security IDs, tenant IDs, or credentials.
* Respect API pagination.
* Do not assume a single API page contains all records.
* Preserve server-provided identifiers.
* Treat API response fields as potentially nullable unless documentation or existing validation proves otherwise.

## Database Writes

Before changing database-write behavior:

* Find existing shared database utilities.
* Determine the table's primary key or conflict key.
* Determine whether the intended operation is append, insert/update, full refresh, or snapshot.
* Do not convert an incremental table into a full-refresh table without explicit instruction.

When implementing synchronization, report useful counts when practical, including:

* rows fetched
* rows inserted
* rows updated/upserted
* rows skipped or rejected

## Schema and Reporting

The repository contains operational source data as well as company-defined reporting structures.

Do not assume API source schemas and reporting schemas serve the same purpose.

When changing a table, view, or shared field:

* Search for downstream consumers.
* Check Python code, SQL views, reports, and other references before renaming or removing it.
* Prefer backward-compatible changes where practical.

## External Effects

Routine read-only inspection and local static analysis are allowed.

Before running commands that could materially modify production data, schemas, external systems, or large datasets, describe the operation first unless the user has explicitly requested that operation.

Never execute:

* destructive SQL
* schema drops
* mass deletes
* credential changes
* production deployment actions

unless explicitly requested.

## Completion Checklist

Before considering a code change complete:

1. Review the files that were changed.
2. Run the most relevant safe verification command available.
3. Check for tracebacks, warnings, and obvious regressions.
4. Review `git diff`.
5. Confirm that unrelated code was not modified.
6. Summarize what changed and note anything that could not be verified.

