# Data Project 2 // Open Source Health: GitHub Data Pipeline

Released: Oct 23, 2026

## Overview

Students will build one Prefect flow that pulls GitHub activity for five data science packages and ranks them from healthiest to least healthy development community. The project is worth 100 points: 92 core and 8 extended.

The flow must run end to end from a single entry point and contain six tasks: (1) ingest from the GitHub API, (2) land raw data, (3) transform and test with `dbt`, (4) reconcile and summarize, (5) model and rank, (6) plot. Each task is graded on its own, and each is designed to require real problem solving rather than a single library call.

Analysis window: **2023-01-01 through 2025-12-31** (36 months). A fixed window keeps results reproducible and comparable across projects.

## Package selection

Students will choose one of three preassigned sets of packages. Fixed sets equalize the API workload, guarantee a spread of community sizes so the ranking has something to find, and let you grade results against a known reference run.

Each set mixes two very large projects, two mid-size projects, and one smaller or maintainer-concentrated project.

| Set | Large (heavy pagination) | Mid-size | Smaller / concentrated |
| --- | --- | --- | --- |
| A | numpy/numpy, scikit-learn/scikit-learn | dask/dask, mwaskom/seaborn | vega/altair |
| B | pandas-dev/pandas, matplotlib/matplotlib | statsmodels/statsmodels, dmlc/xgboost | networkx/networkx |
| C | scipy/scipy, pola-rs/polars | plotly/plotly.py, microsoft/LightGBM | pydata/xarray |

A student may swap one package within the same size tier with instructor approval. Avoid monorepos such as apache/arrow and very large repos such as pytorch/pytorch; they distort comparisons and can exhaust a day of API quota.

## General requirements — 14 points

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Submission | All code, README, and plots are committed by the deadline in a private fork of the source repository. | 1 |
| Single Prefect flow | One `@flow` orchestrates exactly six `@task` functions in order; running one command executes the whole pipeline, including dbt build. | 3 |
| Execution | The flow runs to completion from a clean clone with only a token set; no unhandled exceptions. | 2 |
| Secrets | The GitHub token is read from an environment variable or Prefect Secret block; no token appears anywhere in the repository or its history. | 1 |
| Retries and error handling | Tasks use Prefect retries with backoff where failure is transient; HTTP and parsing errors are caught and reported with context. | 2 |
| Logging | Each task logs through the Prefect logger and to its own log file in the project directory (six logs; dbt's own log may serve for Task 3). | 2 |
| Code quality | Docstrings on every function; type hints; files organized into folders (flows, tasks, dbt, plots). | 1 |
| Reproducibility | Dependencies pinned (requirements.txt or pyproject); README explains setup, run command, and expected runtime. | 1 |
| No cruft | The repository contains no database files, log files, raw API dumps, or caches. It contains no `venv` folder or virtualenv artifacts apart from a `pyproject.yaml` (uv) or `Pipfile*` (pipenv) | 2 |

## Task 1: Ingestion — 20 points

The task pulls every required dimension for all five repositories within the analysis window, across every page, while carefully working around GitHub's ratelimits.

[**GitHub API Documentation**](https://docs.github.com/en/rest?apiVersion=2026-03-10)
- [Commits](https://docs.github.com/en/rest/commits/commits?apiVersion=2026-03-10)
- [Issues](https://docs.github.com/en/rest/issues/issues?apiVersion=2026-03-10)
- [Pull Requests (a special type of issue)](https://docs.github.com/en/rest/issues/issues?apiVersion=2026-03-10)
- [Releases](https://docs.github.com/en/rest/releases/releases?apiVersion=2026-03-10)
- [Contributors](https://docs.github.com/en/rest/repos/repos?apiVersion=2026-03-10#list-repository-contributors)
- etc ...

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Required endpoints | Collects commits, issues, pull requests, releases, contributors, etc. for all five repos. Issues and PRs are separated (the issues endpoint also returns PRs). | 3 |
| Detail calls | Fetches per-item data for every merged PR in the window: reviews and first-response time. This multiplies call volume and is what pushes large repos past the hourly quota. | 3 |
| Pagination | Follows the `Link` header (`rel="next"`) with `per_page=100` until exhausted; no hard-coded page counts. Record counts are logged per endpoint per repo. | 3 |
| Primary rate limit | Reads `X-RateLimit-Remaining` and `X-RateLimit-Reset` on every response and sleeps until reset before the budget hits zero. The 5,000 requests/hour authenticated limit is fixed and cannot be raised. | 3 |
| Other HTTP errors | Handles 403/429 and other errors with standard Prefect `retries` and `retry_delay` flags in the appropriate task(s). | 3 |
| Resumability | A run interrupted mid-ingestion resumes from a checkpoint (last page or `since` timestamp) instead of starting over. Demonstrated in the README with log excerpts. | 3 |


## Task 2: Raw ingestion — 8 points

Data lands in a local DuckDB database as raw JSON responses; `dbt` builds everything downstream in Task 3.

> NOTE: Setting up `dbt` (below) will assume the DB file is in a specific location; your other Prefect tasks should work with that.

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Raw layer | Each API response is stored as JSON with repo, endpoint, page, and fetch timestamp, so every dbt model can be rebuilt without calling the API again. | 3 |
| Idempotent loads | Re-running the flow produces identical raw row counts; loads upsert on repo, endpoint, and natural key, never blind inserts. | 3 |
| Incremental loads | A second run fetches only records updated since the last successful run, using stored checkmarks. | 2 |

## Task 3: `dbt` transformations and tests — 14 points

A `dbt` project in the repository turns raw JSON into tested staging models and marts with precalculated measures. The Prefect flow runs `dbt build` and fails if any model or test fails. Little cleaning is needed; the work is in the derived measures.

A simple `dbt` Reference is available [here](DBT-REFERENCE.md).

Suggested models (students may add more):

| Model | Grain | Precalculated columns |
| --- | --- | --- |
| `stg_*` | One per raw endpoint | Typed columns, UTC timestamps, deduplicated on natural key, `is_bot` flag |
| `fct_issues` | One row per issue | Hours to first response, hours to close, open at window end, stale (no activity in 180+ days) |
| `fct_pull_requests` | One row per PR | Hours to first review, hours to merge, merged flag, lines changed, first-time contributor flag |
| `fct_releases` | One row per release | Days since previous release, major/minor/patch from tag, prerelease flag |
| `dim_contributors` | One row per author per repo | First and last contribution date, active quarters, retained 6 months after first contribution |
| `mart_repo_monthly` | Repo × month | Issues opened and closed, close ratio, median response and merge hours, commits, active contributors |
| `mart_health_inputs` | One row per repo | Every metric Task 5 scores, already size-normalized |

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Staging models | One staging model per raw endpoint with typed columns, deduplication, and bot flagging; the bot rule is documented. | 3 |
| Fact and dimension marts | `fct_issues`, `fct_pull_requests`, `fct_releases`, and `dim_contributors` built with the precalculated durations and flags above. | 3 |
| Aggregate marts | `mart_repo_monthly` and `mart_health_inputs` feed Tasks 4–6 directly; no downstream task recomputes durations in Python. | 2 |
| Open items | Issues and PRs still open at window end are kept and marked open, never dropped or given a fake close date. The README explains how medians treat them. | 1 |
| Generic tests | `unique` and `not_null` on every primary key, `relationships` from facts to repos and contributors, `accepted_values` on state columns. | 2 |
| Singular tests | Custom SQL tests assert no issue closed before it opened, no PR merged before it was created, and every timestamp falls inside the window. Results print to screen and log. | 2 |
| Documentation | `schema.yml` describes every mart and column; raw tables are declared as dbt sources. | 1 |

## Task 4: Reconciliation, outliers, and statistics — 10 points

This task reads the `dbt` marts, checks them against GitHub's own totals, and summarizes them before modeling; it fails the flow when the data is incomplete.

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Reconciliation checks | Mart counts are compared against independent API totals (for example, the repo object's open-issue count vs. open rows in `fct_issues`). Mismatches over a stated tolerance fail the task. | 3 |
| Outlier detection | A stated method (IQR, z-score on log scale, or median absolute deviation) flags extreme values in time to close, PR size, and weekly commits. Flagged records are listed and the treatment (keep, cap, or exclude) is justified. | 4 |
| Descriptive statistics | Per repo: median and 90th percentile time to first response, time to close, and time to merge; commits per week; releases per year; active contributors per quarter. Printed to screen and log. | 3 |

Medians and percentiles are required because these durations are heavily right-skewed; a mean alone earns no credit.

## Task 5: Health modeling and ranking — 16 points

Students build a composite health score from at least six metrics spanning at least four dimensions, then rank the five repos and show the ranking holds up.

| Dimension | Example metrics |
| --- | --- |
| Activity | Commits per week; trend slope over the window |
| Responsiveness | Median time to first response on issues; median time to first PR review |
| Throughput | Issues closed ÷ issues opened per month; PR merge rate; median time to merge |
| Backlog | Open-issue growth rate; share of open issues with no activity in 180+ days |
| Release cadence | Releases per year; regularity (coefficient of variation of days between releases) |
| Contributor base | Active contributors per quarter; bus factor (fewest people making 50% of commits); new-contributor retention after 6 months |
| Community health | Community profile files (CONTRIBUTING, CODE\_OF\_CONDUCT); `good first issue` usage; first-time contributor PRs merged |

Stars and forks measure popularity, not health, and do not count toward the six metrics.

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Metric design | Six or more metrics across four or more dimensions, read from `mart_health_inputs`, each with a written definition and a stated direction (higher or lower is healthier). | 3 |
| Size normalization | Metrics are normalized so large projects do not win by volume alone (per-contributor rates, ratios, or ranks rather than raw counts). | 3 |
| Scoring model | Metrics are standardized (z-score, min-max, or rank) and combined with weights the student justifies in writing. | 3 |
| Trend component | At least one metric captures direction of change (for example, a regression slope over 36 months), not only a level. | 2 |
| Sensitivity analysis | The ranking is recomputed under at least two alternative weightings, or one metric left out at a time. The README states whether the top and bottom positions hold. | 3 |
| Ranking output | A ranked table with score, rank, and per-dimension subscores prints to screen and log and is written back to the database. | 2 |

## Task 6: Multi-dimensional plots — 10 points

The flow saves at least three plots as PNG to a `plots/` directory, committed to the repository. Each plot shows three or more dimensions at once.

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| Dimension profile | A radar chart or small-multiples bar chart showing all five repos across every scored dimension. | 3 |
| Trends over time | Monthly small multiples from `mart_repo_monthly` (one panel per metric, one line per repo) covering the full 36-month window. | 3 |
| Relationship plot | A scatter or bubble chart encoding at least three variables (for example, x = time to first response, y = close ratio, size = active contributors, color = repo). | 2 |
| Presentation | Titles, labeled axes with units, a consistent color per repo across all plots, and a one-paragraph interpretation of each plot in the README. | 2 |

## Extended process — 8 points

These items separate strong submissions from complete ones.

| Criterion | Full credit requires | Pts |
| --- | --- | --- |
| GraphQL ingestion | Rewrite PR and review ingestion against the GitHub GraphQL API with cursor pagination, tracking the 5,000-point hourly budget from the `rateLimit` field. Log request counts and runtime for REST vs. GraphQL. | 3 |
| Concurrent fetching | Use Prefect task mapping or async requests to fetch repos in parallel while a shared limiter keeps total usage under the quota. | 3 |
| Scheduled deployment | A Prefect deployment with a weekly schedule runs the incremental flow; the README shows the deployment command and one scheduled run's log. [See note](DEPLOYMENTS.md). | 2 |

## Point summary

| Section | Points | Score |
| --- | --- | --- |
| General requirements | 14 |  |
| Task 1: Ingestion | 20 |  |
| Task 2: Raw landing | 8 |  |
| Task 3: dbt transformations and tests | 14 |  |
| Task 4: Reconciliation, outliers, and statistics | 10 |  |
| Task 5: Health modeling and ranking | 16 |  |
| Task 6: Multi-dimensional plots | 10 |  |
| Extended process | 8 |  |
| **Total** | **100** |  |

