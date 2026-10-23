# `dbt` in Prefect Flows

1. Install and import

    a. Create a new virtual environment for your project using `uv` or `pipenv`.
    
    b. Next, install `dbt-core`, `dbt-duckdb` and `prefect-dbt`.

2. A `dbt` project has already been initiated in this repository.

    This normally produces a project structure like this:

    ```
    <REPO-ROOT-DIR>/
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

    `profiles.yml` would normally hold sensitive database connection details and be kept outside of the repository in `~/.dbt/profiles.yml`. **In this case, this file can live directly in the repository**. These settings are a good place to start:

    ```
    package_health:
        target: dev
        outputs:
            dev:
                type: duckdb
                path: packages.duckdb
                threads: 8
    ```

3. Loading the raw data from the GitHub API will occur as a separate @task within the Prefect flow, so `dbt` does not need to stage the initial data - it will find the data in `packages.duckdb`.
4. Develop your mart(s) as necessary to build various dimensions around package health. This should include new, calculated columns, aggregate results, etc. A layout matching the suggested models in the README (Task 3):

    ```
    models/
    ├── staging/
    │   ├── sources.yml              # raw_commits, raw_pull_requests, raw_pr_reviews, raw_releases
    │   ├── stg_commits.sql
    │   ├── stg_pull_requests.sql
    │   ├── stg_pr_reviews.sql
    │   └── stg_releases.sql
    └── marts/
        ├── schema.yml               # descriptions + generic tests
        ├── fct_pull_requests.sql
        ├── fct_releases.sql
        ├── mart_repo_monthly.sql
        └── mart_health_inputs.sql
    tests/
    ├── assert_pr_closed_after_created.sql
    └── assert_review_after_pr_created.sql
    ```

    Reference raw tables with `{{ source('raw', 'raw_commits') }}` and other models with `{{ ref('stg_commits') }}`. A singular test is a `SELECT` that returns the offending rows; it passes when it returns none.
5. Develop tests as necessary to verify you are getting values within an expected range, testing for outliers, `NULL` values, etc.
6. Implement idempotency by dropping tables before creating or inserting into them in a new run.
7. Test your entire `dbt` build and test process by hand to validate success and meaningful results to be sure that task is ready to run as part of the larger workflow.
8. When ready to execute within the full workflow, call `dbt build` within a Prefect `@task` in your `@flow`. This can be done using the `PrefectDbtRunner`:

    ```
    from pathlib import Path
    from prefect import flow, task
    from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

    # Resolve relative to this file
    DBT_DIR = Path(__file__).parent

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

