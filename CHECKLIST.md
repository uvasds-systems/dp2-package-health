# Submission Checklist

Due: Nov 02, 2026. See the [README](README.md) for full grading criteria.

## Submit in Canvas

- [ ] Package set (A, B, or C): `____`
- [ ] Repository URL: `https://github.com/________/________`
- [ ] Set up your oral exam time slot 

## Repository setup

- [ ] Created from the template (not forked) and set to **Private**
- [ ] `nmagee`, `elaineliu05`, and `tcusick8` added as collaborators
- [ ] Dependencies pinned in `requirements.txt`, `Pipfile`, or `pyproject.toml`
- [ ] `.gitignore` excludes all appropriate files and directories.
- [ ] No database files, logs, raw API dumps, caches, or binaries committed
- [ ] No GitHub token anywhere in the repository or its history

## The flow (`main.py`)

- [ ] One `@flow` runs all six tasks in order and completes from a clean clone with only a GitHub token set
- [ ] Token read from an environment variable or Prefect Secret block
- [ ] Each task logs to the Prefect logger and to its own log file; dbt's log is saved
- [ ] **Task 1 · Extract:** commits, pull requests, PR reviews, and releases for all three repos, written to `data/raw/` one file per page
- [ ] **Task 2 · Load:** one `raw_*` table per endpoint, upserted on `(repo, id)`, plus `_load_state` and the `_LOADED` marker
- [ ] **Task 3 · dbt:** `dbt build` runs inside the flow and fails it on any model or test failure
- [ ] **Task 4 · Validation (Python, no dbt):** completeness checks, outlier detection, and descriptive statistics printed and logged
- [ ] **Task 5 · Model:** ranked table with score, rank, and per-dimension subscores printed, logged, and written to the database
- [ ] **Task 6 · Plot:** at least three PNGs saved to `plots/`

## dbt project

- [ ] Staging models for all four raw tables, with `is_bot` flag
- [ ] `fct_pull_requests`, `fct_releases`, `mart_repo_monthly`, `mart_health_inputs`
- [ ] Raw tables declared as sources; `schema.yml` describes every mart and column
- [ ] Generic tests: `unique`/`not_null` on primary keys, `relationships` from reviews to PRs, `accepted_values` on state columns
- [ ] Singular tests: no PR closed or merged before creation; no review before its PR was created

## Plots (committed in `plots/`)

- [ ] Dimension profile showing all three repos, ordered by final rank with rank and score labeled
- [ ] Monthly trends: small multiples from `mart_repo_monthly`, one line per repo
- [ ] Relationship plot encoding at least three variables
- [ ] Titles, labeled axes with units, and a consistent color per repo across all plots

## Your README

- [ ] Setup steps, run command, and expected runtime
- [ ] Log excerpts from an interrupted run and its resumed run
- [ ] Row counts from running Task 2 twice on the same files
- [ ] Request counts for a full run and an incremental run
- [ ] The bot rule you used
- [ ] How medians treat PRs still open at run time
- [ ] Outlier cutoffs, the records flagged, and whether you kept, capped, or dropped them, and why
- [ ] Each health metric's definition and direction (higher or lower is healthier); at least four metrics across three dimensions
- [ ] Your weights and the reasoning behind them
- [ ] A one-paragraph interpretation of each plot

## Extended items (if attempted)

- [ ] **GraphQL:** PR and review ingestion uses GraphQL with cursor pagination; README logs request counts and runtime for REST vs. GraphQL
- [ ] **Concurrent fetching:** repos are fetched in parallel under a shared rate limiter; README says where this happens in the code
- [ ] **Scheduled deployment:** monthly Prefect deployment (the 28th); `DEPLOYMENTS.md` shows the deployment command and the log of one manually triggered run
- [ ] **API odometer:** total requests per flow run is logged, and your count is: ___________

## After submission

- [ ] Attend your 5-minute oral interview, ready to explain any part of your code
