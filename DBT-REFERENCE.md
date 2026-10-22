# `dbt` in Prefect Flows

1. Install and import

    a. Create a new virtual environment for your project using `uv` or `pipenv`.
    b. Next, install `dbt-core`, `dbt-duckdb` and `prefect-dbt`.

2. Initialize your `dbt` project by hand

    Use `dbt init` within the root of your repo to create a new `dbt` project. It will prompt you for a project name, which should be `package_health` This will look something like this:

    ```
    package_health/
    ├── README.md
    ├── .gitignore
    ├── dbt_project.yml
    ├── analyses/
    ├── macros/
    ├── models/
    │   └── example/
    │       ├── my_first_dbt_model.sql
    │       ├── my_second_dbt_model.sql
    │       └── schema.yml
    ├── seeds/
    ├── snapshots/
    └── tests/
    ```

    `dbt_project.yml` is the main configuration file. It sets the project name, version, which profile to use, the paths to each folder, and default model configurations such as materialization.

    `models/example/` holds two sample models and a schema.yml. The second model selects from the first using `ref()`, and the schema.yml adds descriptions and a couple of generic tests (unique, not_null). Most people delete this folder once they start real work.

    The other directories (analyses, macros, seeds, snapshots, tests) start out empty except for a .gitkeep file, so Git tracks them. .gitignore already excludes target/, dbt_packages/, and logs/.

    `profiles.yml` would normally hold your database connection details and be kept outside of the repository. **In this case, you should create this file in the DBT folder you just created**. These settings are a good place to start:

    ```
    package_health:
        target: dev
        outputs:
            dev:
            type: duckdb
            path: package_health.duckdb
            threads: 8
    ```

    **You may delete** some folders unnecessary to this project:
    - analyses
    - macros
    - seeds
    - snapshots

3. Loading the raw data from the GitHub API will occur as a separate @task within the Prefect flow, so `dbt` does not need to stage the initial data - it will find the data in `package_health.duckdb`.
4. Develop your mart(s) as necessary to build various dimensions around package health. This should include new, calculated columns, aggregate results, etc.
5. Develop tests as necessary to verify you are getting values within an expected range, testing for outliers, `NULL` values, etc.
6. Implement idempotency by dropping tables before creating or inserting into them.
7. Test your entire `dbt` build and test process by hand to validate success and meaningful results.
8. When ready to execute within the full workflow, call `dbt build` within a Prefect `@task` in your `@flow`. This can be done with:

    ```
    from pathlib import Path
    from prefect import flow, task
    from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

    # Resolve relative to this file
    DBT_DIR = Path(__file__).parent / "package_health"

    @task(log_prints=True)
    def dbt_build():
        runner = PrefectDbtRunner(
            settings=PrefectDbtSettings(
                project_dir=str(DBT_DIR),
                profiles_dir=str(DBT_DIR),
            )
        )
        return runner.invoke(["build"])
    ```
