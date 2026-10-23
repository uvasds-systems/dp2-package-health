# Extra Commentary / Suggestions

1. **Update DuckDB CLI** - IMPORTANT! In the last few days, DuckDB has released a new version, 2.0. (Great news!) However, out of the gate you might find that your python using the `duckdb` package has created a new v2.0 database file that your local `duckdb` command-line tool cannot open. To fix this, simply reinstall the DuckDB command-line tool the same way you installed it, which is probably:

    ```
    curl https://install.duckdb.org | bash
    ```
   
3. **Test with Smaller Sets** - In order to get cooking with your code faster, my suggestion is that you pull your API queries from a much later start date, like 01/01/2026 instead of 01/01/2024 so that you're dealing with a lot less files to pull. You can also develop your code against only ONE package repo instead of all three. Once your code is in a good working state, you can delete everything in `data/raw`, and delete your local `packages.duckdb` file, and run the whole thing fresh with the full date range and full set of (3) packages.
4. **Resuming / Restarting Logic** - In the [README - Task 1](README.md#task-1-extract-from-github--16-points) I mention using Task 2 to create a file `data/raw/_LOADED` when it has imported everything it needs. And Task 2 creates a separate table named `_load_state` that tracks the latest updates it has for each of the four (4) API endpoints. **These are strategies** to record what has been downloaded so far, and to indicate when Task 1 should pull a few extra pages of data in subsequent runs. You are free to rework/rename/reconfigure these methods however you want to, so long as you maintain the principle: **Once you have downloaded a data file, your flow should not need to pull it down again**.
5. **Stopwatch** - If you want to time each run in your terminal, just use the `time` command first: `time python3 main.py` which will return the time elapsed for the module to run in its entirety. (Neal here: I just ran tasks 1 and 2 and together they took 1hr33min to run, but 32mins were sleeping waiting for my API limit to reset.)
6. **Adding Tasks** - If you see a need for additional @tasks in your @flow (think: subtasks that serve a primary task), go for it! There is no strict count of tasks.
