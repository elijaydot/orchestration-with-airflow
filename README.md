# Orchestration with Airflow

## Engineering evolution report and Git history audit

This repository is a learning project that evolves a sales/orders ELT workflow from manually executed Python scripts to explicit Apache Airflow task graphs, reusable helpers, SQL transformations, XCom communication, conditional execution, and SMTP reporting.

The business workflow is to download a published spreadsheet, load raw orders into PostgreSQL, clean and calculate sales data, and produce a stakeholder summary. Its history illustrates both the benefits of orchestration and the operational problems that remain when scheduling, configuration, transactions, storage, and deployment are not treated consistently.

> **Audit baseline:** `origin/main` at [`02988ab`](https://github.com/elijaydot/orchestration-with-airflow/commit/02988aba46888c163220891bb0dfa373cadb9359), authored October 30, 2026 at 12:21:53 +02:00. The remote was fetched before analysis; local `HEAD` matched that revision. This README is a subsequent documentation change, not part of that baseline.
>
> **Evidence boundary:** This is a source-history analysis, not a claim that the workflows have successfully run. No database queries, spreadsheet downloads, Docker deployment, or email deliveries were performed for this report.

## Contents

- [Audit method and scope](#audit-method-and-scope)
- [Architecture and repository map](#architecture-and-repository-map)
- [Complete chronological commit ledger](#complete-chronological-commit-ledger)
- [Chronological engineering narrative](#chronological-engineering-narrative)
- [File-level implementation reference](#file-level-implementation-reference)
- [DAG catalog and orchestration semantics](#dag-catalog-and-orchestration-semantics)
- [Configuration and onboarding](#configuration-and-onboarding)
- [Feature provenance](#feature-provenance)
- [Repository evolution summary](#repository-evolution-summary)

## Audit method and scope

### What was reviewed

The audit examined all **22 commits reachable from `origin/main`**, including **19 non-merge commits and three merge commits**. Of these, **18 are on main's first-parent chain**; four additional commits entered through upstream merges. Every non-merge patch was inspected, along with each merge's change relative to its first parent and the current implementation.

The primary evidence was:

```powershell
git fetch origin
git log origin/main --reverse --date=iso-strict --format="%H %P %aI %s"
git log origin/main --first-parent --reverse --format="%h %aI %s"
git log origin/main --reverse --topo-order --no-merges -p
git diff <merge>^1 <merge> --name-status
git diff <merge>^1 <merge>
git ls-tree -r --name-only origin/main
```

The narrative below follows commit author timestamps, with original UTC offsets retained in the ledger. Integration notes distinguish an upstream feature's authoring date from the commit that brought it onto main. This avoids mistaking an upstream development commit for a direct main-branch change.

### What Git cannot establish

- **A commit is not a push.** One push can contain multiple commits; Git objects do not preserve historical push batches or push timestamps. No push-event archive was available, so phases are commit-based, not invented push numbers.
- Reachability establishes that a commit is in the audited main history. It does not prove which branch name existed when an early commit was originally authored.
- Commit messages and patches establish what changed. Explanations of business motivation, design trade-offs, and avoided alternatives are **engineering interpretations** unless explicitly supported by comments or existing documentation.
- A declared task graph establishes intended sequencing, not successful execution, delivery, throughput, or measured reliability.
- Other branches, upstream-only commits not reachable from main, and local agent checkpoint refs are outside the audit. The remote's locally advertised default-branch pointer was `master`; the requested target, `origin/main`, was selected explicitly.
- No standalone file deletion was found. There are five file renames; these are identified as renames rather than counted as unrelated deletions and additions.

### How to read each phase

Each phase identifies its commit, issue/root cause, solution, exact file inventory, and before/after impact. The [file-level reference](#file-level-implementation-reference) explains the functions and major code blocks shared across phases; the [DAG catalog](#dag-catalog-and-orchestration-semantics) consolidates scheduling and execution semantics.

“Outcome” means the capability or limitation visible in the source unless runtime evidence is explicitly stated. No numerical operational improvement is claimed without measurements.

## Architecture and repository map

### Current business-data path

```text
Published spreadsheet CSV
          |
          v
raw_sales.csv in /opt/airflow/dags/data
          |
          v
PostgreSQL raw_sales
          |
          +--> pandas transform in orders
          |
          +--> SQL transform in orders_sql / orders_email
                        |
                        v
                  PostgreSQL sales
                        |
                        +--> report.txt in orders / orders_sql
                        |
                        +--> XCom report --> SMTP in orders_email
```

The business database is separate from the Compose PostgreSQL service used for Airflow metadata. Some business operations use an Airflow Connection; others use the environment-derived SQLAlchemy URL. These must resolve to the same intended business database.

### Current tracked surfaces

| Surface | Files | Role |
|---|---|---|
| Learning-stage instructions | [manual](fundamentals/stage0-manual/README.md), [single script](fundamentals/stage1-one-big-script/README.md), [cron](fundamentals/stage2-cron/README.md), [custom orchestrator](fundamentals/stage3-custom-orchestrator/README.md) | Explain increasing levels of orchestration; the custom-orchestrator stage is documentation only. |
| Standalone pipeline | [configuration](fundamentals/steps/config.py), [extract](fundamentals/steps/extract.py), [load](fundamentals/steps/load.py), [transform](fundamentals/steps/transform.py), [notify](fundamentals/steps/notify.py) | Original ELT/reporting implementation. |
| Script coordination | [runner](fundamentals/stage1-one-big-script/run_pipeline.py), [crontab](fundamentals/stage2-cron/crontab.txt), [log placeholder](fundamentals/stage2-cron/logfile.log), [requirements](fundamentals/requirements.txt) | Sequential execution, scheduling examples, and Python dependencies. |
| Airflow examples | [first DAG](airflow/dags/00_first-dag.py), [Python orders](airflow/dags/01_orders.py), [SQL orders](airflow/dags/02_orders-sql.py), [TaskFlow](airflow/dags/03_taskflow.py), [email orders](airflow/dags/04_orders-email.py) | Five DAG definitions, from operator basics to conditional reporting. |
| Airflow business logic | [configuration](airflow/dags/include/config.py), [shared helpers](airflow/dags/include/orders_util.py), [email helpers](airflow/dags/include/orders_email.py), [SQL](airflow/dags/sql/orders.sql) | Connections/configuration, extraction/loading, transformation, and reporting. |
| Runtime | [Compose](airflow/docker-compose.yaml), [Dockerfile](airflow/Dockerfile) | Local Celery-based Airflow stack and a separate custom-image recipe. |
| Non-workflow example | [OOP example](airflow/dags/oop.py) | Small `Dog` class demonstration; not a DAG. |
| Developer configuration | [ignore rules](.gitignore), [VS Code settings](.vscode/settings.json), [workspace](orchestration-with-airflow.code-workspace) | Generated-file exclusions and editor setup. |

## Complete chronological commit ledger

“Direct” means present on main's first-parent chain. “Via” identifies where an upstream development commit entered main. Dates are author dates, not push dates.

| Phase | Commit | Author timestamp | Subject | Main relationship |
|---|---|---|---|---|
| 1 | [`00694fc`](https://github.com/elijaydot/orchestration-with-airflow/commit/00694fc) | 2026-10-14 00:01:52 +01:00 | Added stage zero | Direct, root |
| 2 | [`1456771`](https://github.com/elijaydot/orchestration-with-airflow/commit/1456771) | 2026-10-14 23:40:14 +01:00 | Added extract and load steps | Direct |
| 3 | [`53cbd0f`](https://github.com/elijaydot/orchestration-with-airflow/commit/53cbd0f) | 2026-10-14 23:41:11 +01:00 | Added transform step | Direct |
| 4 | [`87d1416`](https://github.com/elijaydot/orchestration-with-airflow/commit/87d1416) | 2026-10-19 01:58:18 +01:00 | Added transform step | Direct |
| 5 | [`beedce7`](https://github.com/elijaydot/orchestration-with-airflow/commit/beedce7) | 2026-10-19 02:05:08 +01:00 | Removed exception handling | Direct |
| 6 | [`e339efe`](https://github.com/elijaydot/orchestration-with-airflow/commit/e339efe) | 2026-10-19 02:08:55 +01:00 | Added notify step | Direct |
| 7 | [`17e85df`](https://github.com/elijaydot/orchestration-with-airflow/commit/17e85df) | 2026-10-19 02:20:51 +01:00 | Added stage 1 | Direct |
| 8 | [`10d3828`](https://github.com/elijaydot/orchestration-with-airflow/commit/10d3828) | 2026-10-19 02:26:15 +01:00 | Added stage 2 and 3 | Direct |
| 9 | [`2d4b7e2`](https://github.com/elijaydot/orchestration-with-airflow/commit/2d4b7e2) | 2026-10-19 02:28:00 +01:00 | Added stage 2 and 3 | Direct |
| 10 | [`026e57c`](https://github.com/elijaydot/orchestration-with-airflow/commit/026e57c) | 2026-10-19 02:45:42 +01:00 | Added cron paths | Direct |
| 11 | [`ef0b002`](https://github.com/elijaydot/orchestration-with-airflow/commit/ef0b002) | 2026-10-21 22:14:24 +01:00 | Added first DAG | Direct |
| 12 | [`a105c1b`](https://github.com/elijaydot/orchestration-with-airflow/commit/a105c1b) | 2026-10-23 22:28:40 +01:00 | Convert ELT steps into DAG | Via phase 15 |
| 13 | [`d25e600`](https://github.com/elijaydot/orchestration-with-airflow/commit/d25e600) | 2026-10-25 00:05:53 +01:00 | Did separation of concerns and improved DAG implementation | Via phase 15 |
| 14 | [`fd724d6`](https://github.com/elijaydot/orchestration-with-airflow/commit/fd724d6) | 2026-10-26 12:59:10 +02:00 | my first dag | Direct |
| 15 | [`ec0f6d0`](https://github.com/elijaydot/orchestration-with-airflow/commit/ec0f6d0) | 2026-10-26 13:42:50 +02:00 | Merge remote-tracking branch 'upstream/master' | Direct merge |
| 16 | [`d92963d`](https://github.com/elijaydot/orchestration-with-airflow/commit/d92963d) | 2026-10-26 21:30:22 Z | Added taskflow, context and xcom | Via phase 18 |
| 17 | [`35d14eb`](https://github.com/elijaydot/orchestration-with-airflow/commit/35d14eb) | 2026-10-27 11:32:58 +02:00 | Fix orders output directory and database URL construction | Direct |
| 18 | [`ad5081b`](https://github.com/elijaydot/orchestration-with-airflow/commit/ad5081b) | 2026-10-27 11:42:31 +02:00 | Merge remote-tracking branch 'upstream/master' | Direct merge |
| 19 | [`26d0847`](https://github.com/elijaydot/orchestration-with-airflow/commit/26d0847) | 2026-10-27 14:22:28 +02:00 | seperation of concerns and DAG improvement | Direct |
| 20 | [`696a2d0`](https://github.com/elijaydot/orchestration-with-airflow/commit/696a2d0) | 2026-10-28 21:24:31 Z | Added branching and Variable | Via phase 21 |
| 21 | [`c82c3f2`](https://github.com/elijaydot/orchestration-with-airflow/commit/c82c3f2) | 2026-10-30 07:52:36 +02:00 | Merge remote-tracking branch 'upstream/master' | Direct merge |
| 22 | [`02988ab`](https://github.com/elijaydot/orchestration-with-airflow/commit/02988ab) | 2026-10-30 12:21:53 +02:00 | Added branching and Variable with reporting sent via email using SMTP connection | Direct, audit tip |

## Chronological engineering narrative

### Phase 1 - Manual orchestration baseline

**Commit:** `00694fc` - October 14.

**Summary / issue identified:** The first commit describes an engineer acting as the orchestrator and manually invoking extraction, loading, and transformation.

**Root cause:** There is no executable coordinator, dependency graph, scheduler, or durable record of step completion.

**Solution and rationale:** [The stage-zero README](fundamentals/stage0-manual/README.md) records three Python commands. This establishes the intended ELT order without adding scheduler infrastructure. At this revision, the referenced scripts do not yet exist.

**Files changed:** Created: [stage-zero README](fundamentals/stage0-manual/README.md). Modified: none. Deleted: none.

**Before -> after / outcome:** An empty repository becomes a written workflow contract, not a runnable pipeline. The commands use `steps/...` while claiming execution from the repository root; the eventual scripts live beneath `fundamentals`. That documentation mismatch remains relevant to onboarding.

### Phase 2 - Implementing extraction and raw loading

**Commit:** `1456771` - October 14.

**Summary / issue identified:** The documented workflow needs actual data ingestion.

**Root cause:** No code retrieves the spreadsheet or persists a raw database table.

**Solution and implementation:**

- [Configuration](fundamentals/steps/config.py) calls `load_dotenv()`, reads `DB_USER` and `DB_PASS`, defines a fixed PostgreSQL endpoint/database, and interpolates a SQLAlchemy URL.
- [`extract()`](fundamentals/steps/extract.py) reads a published CSV through pandas and writes `data/raw_sales.csv` without the DataFrame index.
- [`load()`](fundamentals/steps/load.py) reads that artifact, creates an engine, and uses `to_sql(..., if_exists='replace')` to recreate `raw_sales`.
- Both scripts have `__main__` guards, allowing direct execution. Loading initially catches every `Exception` and prints a failure message without re-raising.

**Files changed:** Created: [config](fundamentals/steps/config.py), [extract](fundamentals/steps/extract.py), [load](fundamentals/steps/load.py). Modified: none. Deleted: none.

**Before -> after / outcome:** Written steps become executable ingestion functions. A CSV separates extraction from loading and permits inspection/reuse instead of fetching the remote source again inside the loader. It also introduces a working-directory dependency and requires an existing `data` directory. Broad exception handling can make a failed load look like a successful process exit.

### Phase 3 - Producing transformed sales data

**Commit:** `53cbd0f` - October 14.

**Summary / issue identified:** Raw rows alone do not provide a cleaned sales dataset or calculated sales value.

**Root cause:** The ingestion layer has no normalization or derived business fields.

**Solution and implementation:** [The transform script](fundamentals/steps/transform.py) creates an engine, reads `raw_sales`, fills missing values with the literal `"Unkmown"`, drops duplicate rows, parses `order_date`, calculates `total_sale = quantity * price`, and replaces `sales`. Its initial broad exception handler prints an error without propagating it.

**Files changed:** Created: [transform](fundamentals/steps/transform.py). Modified: none. Deleted: none.

**Before -> after / outcome:** The intended manual sequence can now produce an analytical table. Pandas keeps transformation logic accessible in Python instead of introducing a SQL task at this stage. Blanket missing-value substitution is not a schema-aware quality policy: numeric/date columns can become invalid, and the placeholder is misspelled. No validation assertions or quarantine path are added.

### Phase 4 - Centralizing the source configuration

**Commit:** `87d1416` - October 19.

**Summary / issue identified:** Despite the repeated “Added transform step” message, this patch changes configuration and extraction, not transformation.

**Root cause:** The spreadsheet URL is embedded directly in the extraction module.

**Solution and implementation:** `DATA_SOURCE` moves into [config](fundamentals/steps/config.py); [extract](fundamentals/steps/extract.py) imports the module and calls `pd.read_csv(config.DATA_SOURCE)`.

**Files changed:** Created: none. Modified: [config](fundamentals/steps/config.py), [extract](fundamentals/steps/extract.py). Deleted: none.

**Before -> after / outcome:** Source selection is separated from ingestion behavior. Future source updates can occur in configuration without editing the extraction function. The source remains a fixed constant rather than a runtime parameter.

### Phase 5 - Restoring failure propagation

**Commit:** `beedce7` - October 19.

**Summary / issue identified:** Database or transformation failures can be printed and then hidden from a caller.

**Root cause:** Broad `try/except Exception` blocks swallow exceptions.

**Solution and implementation:** [Load](fundamentals/steps/load.py) and [transform](fundamentals/steps/transform.py) remove those blocks while retaining connection context managers. Exceptions now escape the functions; the final success print is reached only if the protected operations finish.

**Files changed:** Created: none. Modified: [load](fundamentals/steps/load.py), [transform](fundamentals/steps/transform.py). Deleted: none.

**Before -> after / outcome:** Later sequential coordinators can stop on failure rather than continue after a misleading success-shaped return. This is failure visibility, not failure recovery: no retries, rollback design, or alert callback is introduced. Using `connect()` still does not explicitly establish a commit-on-success write transaction.

### Phase 6 - Generating a local business report

**Commit:** `e339efe` - October 19.

**Summary / issue identified:** Cleaned data is not yet summarized for a business consumer.

**Root cause:** The pipeline ends at the `sales` table.

**Solution and implementation:** [`notify()`](fundamentals/steps/notify.py) reads all of `sales`, calculates revenue, row count, total quantity, highest-revenue customer, and highest-revenue product, then writes `data/report.txt`. Customer/product ranking uses grouped `total_sale` sums followed by `idxmax()`.

**Files changed:** Created: [notify](fundamentals/steps/notify.py). Modified: none. Deleted: none.

**Before -> after / outcome:** The pipeline gains an inspectable text summary. Despite the function name, it does **not** send an email or otherwise distribute the report. It uses today's wall-clock date and all table rows, not a date-filtered daily window. Empty datasets are not guarded before ranking; the revenue line is incorrectly labeled “Top restaurants.”

### Phase 7 - One-command sequential coordination

**Commit:** `17e85df` - October 19.

**Summary / issue identified:** Engineers must remember and invoke four dependent operations in order.

**Root cause:** Individual scripts have no common entry point.

**Solution and implementation:** [The runner](fundamentals/stage1-one-big-script/run_pipeline.py) derives the `fundamentals` directory from `__file__`, inserts its `steps` directory into `sys.path`, imports four functions, and calls `extract() -> load() -> transform() -> notify()` inside `main()`. Start/completion prints delimit a run.

**Files changed:** Created: [ignore rules](.gitignore), [stage-one README](fundamentals/stage1-one-big-script/README.md), [runner](fundamentals/stage1-one-big-script/run_pipeline.py). Modified: none. Deleted: none.

**Before -> after / outcome:** Four invocations become one, and uncaught exceptions interrupt downstream work. This avoids independent manual commands but provides only process-local sequencing: no persisted task state, selective retry, schedule, or execution UI. The README's underscore-based command does not match the tracked hyphenated directory. Ignoring `*.env` begins excluding environment files; it is not evidence that every configuration input is validated.

### Phase 8 - Cron examples and a custom-orchestrator placeholder

**Commit:** `10d3828` - October 19.

**Summary / issue identified:** A single command still requires a person to initiate execution.

**Root cause:** No scheduler invokes the workflow automatically.

**Solution and implementation:**

- [Requirements](fundamentals/requirements.txt) pins pandas, SQLAlchemy, psycopg2-binary, and python-dotenv. These are standalone-script requirements, not a complete Airflow runtime specification.
- [Cron documentation](fundamentals/stage2-cron/README.md) illustrates staggered hourly invocation and stdout/stderr redirection.
- [The crontab](fundamentals/stage2-cron/crontab.txt) adds four minute offsets; only the extraction entry has concrete machine-specific interpreter/script paths. The other three remain placeholders.
- [The empty log file](fundamentals/stage2-cron/logfile.log) reserves an output destination; it contains no execution evidence.
- [Stage three](fundamentals/stage3-custom-orchestrator/README.md) states the intention to build an in-house orchestrator, but adds no implementation.

**Files changed:** Created: [requirements](fundamentals/requirements.txt), [cron README](fundamentals/stage2-cron/README.md), [crontab](fundamentals/stage2-cron/crontab.txt), [log placeholder](fundamentals/stage2-cron/logfile.log), [stage-three README](fundamentals/stage3-custom-orchestrator/README.md). Modified: none. Deleted: none.

**Before -> after / outcome:** Time-based execution is demonstrated, not delivered as a portable complete installation. A five/six/seven/eight-minute schedule is not a dependency graph: a slow or failed extract cannot prevent a separately scheduled load. The generic README example uses different minute offsets. Neither example adds locks, overlap prevention, retries, or state recovery.

### Phase 9 - Report formatting annotations

**Commit:** `2d4b7e2` - October 19.

**Summary / issue identified:** Long report-formatting lines need lint suppression.

**Root cause:** Two f-string lines exceed the intended line-length rule.

**Solution:** [Notify](fundamentals/steps/notify.py) adds `# noqa E501` annotations.

**Files changed:** Created: none. Modified: [notify](fundamentals/steps/notify.py). Deleted: none.

**Before -> after / outcome:** Runtime behavior is unchanged. The subject repeats “Added stage 2 and 3,” but the patch introduces neither another stage nor a new orchestration feature.

### Phase 10 - Correcting one cron entry and repository hygiene

**Commit:** `026e57c` - October 19.

**Summary / issue identified:** The extraction cron entry points into a different project layout.

**Root cause:** Absolute paths reflect an earlier local directory structure.

**Solution:** [The crontab](fundamentals/stage2-cron/crontab.txt) updates the interpreter, extraction script, and log destinations to the orchestration repository layout. [Ignore rules](.gitignore) add `venv/` and `__pycache__/`.

**Files changed:** Created: none. Modified: [ignore rules](.gitignore), [crontab](fundamentals/stage2-cron/crontab.txt). Deleted: none.

**Before -> after / outcome:** One developer's extraction entry better matches their checkout and generated Python files are excluded. The other three cron commands remain placeholders. No `cd` is added, so the script's relative `data/...` path still depends on cron's working directory.

### Phase 11 - First Airflow DAG and local cluster definition

**Commit:** `ef0b002` - October 21.

**Summary / issue identified:** Script/cron orchestration lacks explicit task-state tracking and success-dependent execution.

**Root cause:** Time offsets and a single Python process cannot model independently observable tasks.

**Solution and implementation:**

- Historical `airflow/dags/first-dag.py`, now [the first-DAG example](airflow/dags/00_first-dag.py), defines `first_dag` using `DAG`, `BashOperator`, and `PythonOperator`. A shell echo precedes `message()` through `first_task >> second_task`.
- [Compose](airflow/docker-compose.yaml) introduces a CeleryExecutor architecture: PostgreSQL metadata, Redis broker, API server, scheduler, DAG processor, worker, triggerer, initialization service, optional CLI, and optional Flower.
- Shared YAML anchors distribute Airflow environment and mounts. Health checks and service dependencies control startup ordering. Initialization performs configuration/migration/user setup through the image entrypoint and prepares mounted directories.
- [The OOP example](airflow/dags/oop.py) defines `Dog.name`, creates two instances, and prints one instance's inherited class attribute. It provides no orchestration capability.
- [Ignore rules](.gitignore) add generated `config/`, `logs/`, `plugins/`, and `data/` directories.

**Files changed:** Created: historical `airflow/dags/first-dag.py` ([current path](airflow/dags/00_first-dag.py)), [OOP example](airflow/dags/oop.py), [Compose](airflow/docker-compose.yaml). Modified: [ignore rules](.gitignore). Deleted: none.

**Before -> after / outcome:** The project gains a declarative, manually triggered two-task workflow and local execution infrastructure. `schedule=None` means no periodic scheduling. This initial Compose revision selects `build: .` but has no tracked Dockerfile, leaving a clean-checkout build gap. Its own comments explicitly identify it as a local-development configuration, not production infrastructure.

### Phase 12 - Moving the business pipeline into a DAG

**Commit:** `a105c1b` - October 23; integrated into main in phase 15.

**Summary / issue identified:** Airflow currently demonstrates operators but does not orchestrate the business ELT workflow.

**Root cause:** The sales functions exist only in the fundamentals scripts.

**Solution and implementation:** Historical `airflow/dags/orders.py`, now [the Python orders DAG](airflow/dags/01_orders.py), defines `orders` and embeds extraction, loading, transformation, and local report generation as four Python callables. Four `PythonOperator` tasks form `extract_orders >> load_orders >> transform_orders >> notify_orders`. Historical `airflow/dags/config.py` copies the data-source/database configuration into the DAG area.

**Files changed:** Created: historical `airflow/dags/config.py` ([later configuration path](airflow/dags/include/config.py)), historical `airflow/dags/orders.py` ([current DAG](airflow/dags/01_orders.py)). Modified: none. Deleted: none.

**Before -> after / outcome:** The business operations become separately visible task instances instead of one process. A failure blocks ordinary downstream execution, allowing task-level diagnosis and manual reruns. The intermediate directory becomes `/opt/airflow/dags/data`, matching container execution, but is not created by extraction. Code is duplicated inside the DAG and still uses replace-table operations, all-row reporting, and `connect()` for writes. No automatic retry or schedule is added.

### Phase 13 - Shared helpers, SQL execution, and a provider image

**Commit:** `d25e600` - October 25; integrated into main in phase 15.

**Summary / issue identified:** Business logic embedded inside a DAG is hard to reuse, and transformation can be demonstrated directly in the database.

**Root cause:** Orchestration definitions, configuration, and data-processing functions are mixed.

**Solution and implementation:**

- Configuration moves from historical `airflow/dags/config.py` to [the include configuration](airflow/dags/include/config.py), adding `DATA_PATH`.
- [Shared helpers](airflow/dags/include/orders_util.py) extract the four business functions into an importable module. [The Python DAG](airflow/dags/01_orders.py) retains operator definitions and dependency wiring but imports those functions.
- Historical `airflow/dags/orders-sql.py`, now [the SQL DAG](airflow/dags/02_orders-sql.py), defines `orders_sql`. Extraction/loading/reporting remain Python tasks; `SQLExecuteQueryOperator` runs inline `DROP TABLE` and `CREATE TABLE AS SELECT DISTINCT` statements.
- SQL casts dates, substitutes missing text with `COALESCE`, and calculates `quantity * price` inside PostgreSQL. This avoids moving the full raw table into pandas for this transformation variant.
- [The Dockerfile](airflow/Dockerfile) extends `apache/airflow:3.3.2` and installs `apache-airflow-providers-postgres`, providing a recipe for provider-enabled execution.

**Files changed:** Created: [Dockerfile](airflow/Dockerfile), [helpers](airflow/dags/include/orders_util.py), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)). Modified: historical Python DAG ([current path](airflow/dags/01_orders.py)). Renamed and modified: historical `airflow/dags/config.py` -> [include config](airflow/dags/include/config.py). Deleted: none beyond the rename's old path.

**Before -> after / outcome:** Reusable processing and thin DAG definitions improve maintainability; two DAG variants illustrate Python versus database-side transformation. The SQL connection ID is initially `supabse_conn` (exact historical spelling). SQL also leaves the `COALESCE(region, 'Unknown')` expression without a `region` alias, so it does not explicitly preserve that output-column name. The provider install is unpinned.

### Phase 14 - Making the local Airflow stack more deliberate

**Commit:** `fd724d6` - October 26, before the upstream ELT merge.

**Summary / issue identified:** The local stack needs explicit secrets, an available image, and resource/startup settings suitable for a small workstation.

**Root cause:** Initial Compose uses a missing custom build, permissive defaults, and relatively aggressive checks.

**Solution and implementation in [Compose](airflow/docker-compose.yaml):**

- Replace `build: .` with the official `apache/airflow:3.3.2` image, overridable through `AIRFLOW_IMAGE_NAME`.
- Require nonempty `FERNET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET` during Compose interpolation rather than silently accepting missing/default values.
- Disable bundled example DAGs and retain paused-at-creation behavior.
- Publish the API/UI only at `127.0.0.1:8081`; container-to-container execution API calls continue using port 8080.
- Set execution API timeout to 60 seconds and Celery worker concurrency to two.
- Change scheduler checking to its HTTP health endpoint. Add longer startup periods; use `exec` and longer intervals/timeouts for several process-based checks.
- Change restart policies from `always` to `no`, making service exits explicit rather than automatically recovering them.
- Add editor settings and a single-folder workspace.

**Files changed:** Created: [VS Code settings](.vscode/settings.json), [workspace](orchestration-with-airflow.code-workspace). Modified: [ignore rules](.gitignore), [Compose](airflow/docker-compose.yaml). Deleted: none.

**Before -> after / outcome:** Local initialization becomes more constrained and less exposed, with lower task concurrency and more startup tolerance. These are configuration changes, not measured performance gains. Disabling restarts trades automatic recovery for explicit local control. The workspace file is both added and ignored in this commit; ignore rules do not untrack an already tracked file. The editor setting only disables an R-language-server installation prompt.

### Phase 15 - Integrating the ELT and refactoring branches into main

**Commit:** `ec0f6d0` - October 26.

**Summary / issue identified:** Main has the local Airflow setup but not the business DAGs developed upstream.

**Root cause:** The development histories diverged after the first Airflow example.

**Solution:** Merge first parent `fd724d6` with second parent `d25e600`, importing phases 12-13.

**Files changed relative to main's first parent:** Created: [Dockerfile](airflow/Dockerfile), [include config](airflow/dags/include/config.py), [shared helpers](airflow/dags/include/orders_util.py), historical Python DAG ([current path](airflow/dags/01_orders.py)), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)). Modified: none. Deleted: none.

**Before -> after / outcome:** Main now contains three DAGs and the reusable implementation. The upstream config relocation is not a rename relative to main's first parent because main never contained its old path.

**Integration limitation:** Main retains phase 14's `image:` selection. Merely adding the Dockerfile does not make Compose build or use it. The comment that no Dockerfile/extra dependencies exist becomes stale after integration. Provider availability in the selected runtime must therefore be verified separately.

### Phase 16 - External SQL, TaskFlow context, and an incomplete Hook migration

**Commit:** `d92963d` - October 26; integrated into main in phase 18.

**Summary / issue identified:** Inline SQL clutters DAG code; the project also needs examples of TaskFlow/context communication and managed database access.

**Root cause:** Transformation text lives in the operator declaration, and the loader manually constructs its engine.

**Solution and implementation:**

- [SQL](airflow/dags/sql/orders.sql) receives the existing transformation. [The SQL DAG](airflow/dags/02_orders-sql.py) now passes `sql="sql/orders.sql"` rather than embedding statements.
- Historical `airflow/dags/taskflow.py`, now [the TaskFlow example](airflow/dags/03_taskflow.py), defines decorated `dag1` with two `@task` functions. `task1` pushes `"This is an xcom"` under `var_id`; `task2` pulls that key from `task1` and prints it.
- [Shared loading](airflow/dags/include/orders_util.py) starts using `PostgresHook(postgres_conn_id="supabse_conn")`, but assigns `.get_sqlalchemy_engine` **without calling it**.

**Files changed:** Created: [SQL](airflow/dags/sql/orders.sql), historical TaskFlow DAG ([current path](airflow/dags/03_taskflow.py)). Modified: [shared helpers](airflow/dags/include/orders_util.py), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)). Deleted: none.

**Before -> after / outcome:** SQL becomes independently editable and the project gains a focused metadata-sharing example. The Hook migration introduces a concrete implementation defect: `engine` is a method reference rather than the result of `get_sqlalchemy_engine()`, yet the next block expects `engine.connect()`. This is not a completed reliability improvement. The TaskFlow example uses explicit XCom operations rather than return-value/XComArg data passing.

### Phase 17 - Output-directory and URL-construction fixes on main

**Commit:** `35d14eb` - October 27.

**Summary / issue identified:** Fresh extraction cannot assume an ignored output directory exists; interpolated URLs can misinterpret special characters in credentials.

**Root cause:** pandas creates the CSV but not its parent directory, while URL string interpolation treats credentials as URL syntax.

**Solution and implementation:**

- [Shared extraction](airflow/dags/include/orders_util.py) adds `Path(config.DATA_PATH).mkdir(parents=True, exist_ok=True)` before writing.
- [Airflow config](airflow/dags/include/config.py) replaces raw URL interpolation with `sqlalchemy.engine.URL.create(...)`, passing connection fields separately.
- Both [Airflow config](airflow/dags/include/config.py) and [fundamentals config](fundamentals/steps/config.py) change the fixed pooler host and port, from the earlier endpoint on 6543 to the current endpoint on 5432. The patch proves an endpoint change, not its external service rationale or connectivity.
- [The SQL DAG](airflow/dags/02_orders-sql.py) adopts `Superbase_Postgres_Conn`.

**Files changed:** Created: none. Modified: [Airflow config](airflow/dags/include/config.py), [shared helpers](airflow/dags/include/orders_util.py), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)), [fundamentals config](fundamentals/steps/config.py). Deleted: none.

**Before -> after / outcome:** Shared extraction becomes fresh-directory tolerant and Airflow's direct engine configuration handles credentials structurally. Fundamentals still uses an interpolated URL; this is not a repository-wide URL fix. No environment validation or connection provisioning is added.

### Phase 18 - Integrating TaskFlow while regressing a connection name

**Commit:** `ad5081b` - October 27.

**Summary / issue identified:** Main needs the upstream SQL/TaskFlow work, but also has local runtime fixes to preserve.

**Root cause:** Both histories changed shared loading and the SQL DAG.

**Solution:** Merge first parent `35d14eb` with second parent `d92963d`.

**Files changed relative to main's first parent:** Created: [SQL](airflow/dags/sql/orders.sql), historical TaskFlow DAG ([current path](airflow/dags/03_taskflow.py)). Modified: [shared helpers](airflow/dags/include/orders_util.py), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)). Deleted: none.

**Before -> after / outcome:** Main gains external SQL and its fourth DAG. The directory fix survives. However, the merged loader contains the uncalled engine method from phase 16, and the SQL operator changes back from `Superbase_Postgres_Conn` to `supabse_conn`. This merge temporarily reverses part of phase 17's configuration alignment. The next commit repairs these concrete problems.

### Phase 19 - Correct engine invocation and transactional shared loading

**Commit:** `26d0847` - October 27.

**Summary / issue identified:** Shared loading cannot use a method reference as an engine, and business-database identifiers are inconsistent.

**Root cause:** The Hook migration omitted `()` and the merge restored the old connection name. Writes also lack an explicit commit-on-success context.

**Solution and implementation:**

- [Shared `load()`](airflow/dags/include/orders_util.py) calls `PostgresHook(...).get_sqlalchemy_engine()` using `Superbase_Postgres_Conn`.
- It uses `with engine.begin() as connection:` around `to_sql`, committing on successful exit and rolling back that transaction on an exception.
- [The SQL DAG](airflow/dags/02_orders-sql.py) restores the same connection ID.

**Files changed:** Created: none. Modified: [shared helpers](airflow/dags/include/orders_util.py), historical SQL DAG ([current path](airflow/dags/02_orders-sql.py)). Deleted: none.

**Before -> after / outcome:** The shared raw loader now obtains an actual engine and explicitly manages the write transaction. Both `orders` and `orders_sql` benefit because they import this loader. This is not end-to-end atomicity: extraction, raw replacement, transformation, and report generation remain separate tasks; the pandas transform still uses `connect()`.

### Phase 20 - Conditional email reporting and numbered examples

**Commit:** `696a2d0` - October 28; integrated into main in phase 21.

**Summary / issue identified:** Local text files do not distribute reports, and a no-row extraction needs a different path from ordinary processing.

**Root cause:** Earlier DAGs always proceed through loading/transformation/report generation after successful extraction, without an explicit empty-data decision or email delivery.

**Solution and implementation:**

- [The email DAG](airflow/dags/04_orders-email.py) introduces `orders_email`, `BranchPythonOperator`, and SMTP `EmailOperator`.
- [Email extraction](airflow/dags/include/orders_email.py) returns `len(df)`. `PythonOperator` can store that returned count in the `return_value` XCom.
- `check_data(ti)` pulls the count and returns either `load_orders` or `alert_no_record`, matching downstream task IDs.
- For positive counts, the workflow loads, transforms with the shared SQL file, generates a report, and emails it. For zero counts, it sends a no-report notice and skips the processing branch.
- `notify(ti)` calculates the existing metrics and pushes the report text with XCom key `report`; it no longer writes a report file.
- Email HTML pulls that report, replaces newlines with HTML breaks, and renders `{{ ds }}` and the `environment` Airflow Variable. The direct Python `Variable.get(...)` example is commented out.
- Four existing DAG files receive numeric prefixes. Their DAG IDs, tasks, start dates, and schedules do not change.
- On the upstream branch, [shared helpers](airflow/dags/include/orders_util.py) also add the missing engine-method parentheses. That patch is relative to upstream, not a new improvement over main's already-fixed loader.

**Files changed:** Created: [email DAG](airflow/dags/04_orders-email.py), [email helpers](airflow/dags/include/orders_email.py). Modified: [shared helpers](airflow/dags/include/orders_util.py). Renamed without content changes:

| Historical path | New/current path |
|---|---|
| `airflow/dags/first-dag.py` | [00_first-dag.py](airflow/dags/00_first-dag.py) |
| `airflow/dags/orders.py` | [01_orders.py](airflow/dags/01_orders.py) |
| `airflow/dags/orders-sql.py` | [02_orders-sql.py](airflow/dags/02_orders-sql.py) |
| `airflow/dags/taskflow.py` | [03_taskflow.py](airflow/dags/03_taskflow.py) |

**Deleted:** None beyond the old rename paths.

**Why a separate DAG?** The patch preserves the earlier teaching examples and adds a distinct workflow with branching/email behavior. This supports side-by-side learning without changing their task graphs. That is a structural inference; no design record explicitly states the author's reasoning. It is not a downstream reporting-only DAG: it repeats extraction, loading, and transformation instead of waiting for another DAG's completion.

**Before -> after / outcome:** Reporting gains a distribution mechanism and a no-data notification path. This is a no-row availability check, not schema validation, anomaly detection, or general failure alerting. The new helper duplicates logic and omits main's directory-creation and `engine.begin()` fixes. It initially uses `supabse_conn` and `email_conn`. Actual SMTP delivery is not established by the patch.

### Phase 21 - Integrating branching and email without losing the shared-loader fixes

**Commit:** `c82c3f2` - October 30.

**Summary / issue identified:** The upstream reporting feature must coexist with main's repaired shared loader.

**Root cause:** Upstream and main evolved the loader differently.

**Solution:** Merge first parent `26d0847` with second parent `696a2d0`.

**Files changed relative to main's first parent:** Created: [email DAG](airflow/dags/04_orders-email.py), [email helpers](airflow/dags/include/orders_email.py). Renamed: the four DAG paths listed in phase 20. Modified beyond renames: none. Deleted: none beyond old rename paths.

**Before -> after / outcome:** Main now contains all five DAGs. A first-parent comparison shows **no net change to the shared helper**: its directory creation, correct Hook call, current connection name, and `begin()` transaction survive. The newly introduced email helper still has its own older implementation, so preserving shared fixes does not fix that new path.

### Phase 22 - Local connection and SMTP routing alignment

**Commit:** `02988ab` - October 30, audit tip.

**Summary / issue identified:** The imported email workflow references upstream connection names and recipient/signature settings rather than the local deployment's intended routing.

**Root cause:** The feature arrived with upstream-specific database and SMTP identifiers.

**Solution and implementation:**

- [The email DAG](airflow/dags/04_orders-email.py) changes SQL execution to `Superbase_Postgres_Conn`.
- [Email loading](airflow/dags/include/orders_email.py) changes its Hook to the same identifier.
- Both email tasks change SMTP connection ID from `email_conn` to `orders_email_conn`.
- Both recipient lists and sender-signature text are updated. The actual addresses are intentionally not repeated in this document.

**Files changed:** Created: none. Modified: [email DAG](airflow/dags/04_orders-email.py), [email helpers](airflow/dags/include/orders_email.py). Deleted: none.

**Before -> after / outcome:** The email workflow's raw load and SQL transformation align with the configured connection name used by the shared pipeline. Report/no-data messages target the newly specified recipients through the local SMTP identifier.

**Important distinction:** The commit message describes branching, Variables, and SMTP reporting, but those mechanisms first appeared in `696a2d0` and entered main through `c82c3f2`. This final patch configures them; it does not introduce their task graph, add a schedule, create Airflow Connections, or prove delivery.

## File-level implementation reference

This section explains the business and technical responsibilities of every current file. Historical modifications and original paths are traced in the phases above. Line references describe the audited source, not a guarantee of stable numbering after future edits.

### Standalone configuration and processing

#### [fundamentals/steps/config.py](fundamentals/steps/config.py)

**Purpose:** Keep the original scripts' database/source settings together. Business need: point all standalone steps at the same sales source and destination.

**Important blocks:**

1. Imports and `load_dotenv()` load environment-file values into process configuration.
2. `DB_USER`/`DB_PASS` come from environment variables; host, port, and database are fixed constants.
3. `DATABASE_URL` interpolates those fields into a `postgresql+psycopg2` string.
4. `DATA_SOURCE` stores the published CSV URL, centralized in phase 4.

There are no classes, required-setting guards, or connection checks. Unlike the Airflow config after phase 17, this module does not use `URL.create`. A credential containing reserved URL characters remains a parsing concern. Importing the module loads settings but does not open a database connection.

#### [fundamentals/steps/extract.py](fundamentals/steps/extract.py)

**Purpose:** Materialize the source as a local artifact so loading can be a separate operation.

`extract()` prints progress, calls `pd.read_csv(config.DATA_SOURCE)`, and writes `data/raw_sales.csv` with `index=False`. The `__main__` block calls it when executed directly. Exceptions propagate. The directory is not created, the output is overwritten, and neither a timeout/retry policy nor a schema check is specified by this code.

#### [fundamentals/steps/load.py](fundamentals/steps/load.py)

**Purpose:** Make extracted rows queryable in the raw database layer.

`load()` reads the CSV, builds an engine from configuration, enters `engine.connect()`, and replaces `raw_sales` through pandas. It prints the returned row-count value afterward. Replacement is full refresh, not incremental ingestion or an upsert. Since phase 5, errors escape instead of being swallowed. The connection context does not express an explicit write commit; this deserves runtime verification under the actual dependency versions.

#### [fundamentals/steps/transform.py](fundamentals/steps/transform.py)

**Purpose:** Produce the cleaned `sales` table used by reporting.

The major sequence is: engine creation -> `SELECT * FROM raw_sales` -> blanket null fill -> duplicate removal -> date parsing -> revenue calculation -> `to_sql('sales', ..., if_exists='replace')`. The read uses the engine while a separate connection context is open; the write uses that context's connection.

The literal `"Unkmown"` is not a typed default for every column. Invalid dates/numeric values can fail processing; no bad-record handling exists. Whole-row duplicate removal is not a business-key uniqueness guarantee. The algorithm loads the complete table into memory. After phase 5 there is no exception suppression, but there is also no retry policy.

#### [fundamentals/steps/notify.py](fundamentals/steps/notify.py)

**Purpose:** Turn sales into a human-readable operational/business summary.

After reading `sales`, it calculates:

- `total_revenue`: sum of `quantity * price`;
- `order_count`: DataFrame row count, not necessarily distinct order IDs;
- `total_quantity`: sum of quantities;
- `top_customer` and `top_product`: names with the greatest grouped `total_sale`.

It formats a wall-clock-dated message and overwrites `data/report.txt`. The file write occurs after the database context. `idxmax()` is unguarded for an empty grouping; no tie policy is explicit. The “Top restaurants” text contains revenue rather than a restaurant ranking. Phase 9 adds lint annotations only. This file never gains SMTP support.

### Fundamentals coordination, documentation, and dependencies

#### [fundamentals/stage1-one-big-script/run_pipeline.py](fundamentals/stage1-one-big-script/run_pipeline.py)

**Purpose:** Provide one entry point and enforce in-process ordering.

`ROOT = Path(__file__).resolve().parent.parent` anchors module discovery to the checkout; `sys.path.insert(...)` makes the step modules importable. `main()` calls four functions sequentially and prints completion only if all return. The commented earlier imports show an alternative package-style import attempt, not active code.

This solves import discovery, not artifact path discovery: the functions still resolve `data/...` against the working directory. Failure interrupts the process but no step completion is persisted, so rerunning starts with extraction again.

#### Stage READMEs

- [Stage zero](fundamentals/stage0-manual/README.md): manual three-step commands; originally predates the scripts and omits the later notify step.
- [Stage one](fundamentals/stage1-one-big-script/README.md): motivates one-command execution but its command spelling does not match the tracked folder.
- [Stage two](fundamentals/stage2-cron/README.md): shows cron fields, independent invocations, and combined log redirection; it does not implement success dependencies.
- [Stage three](fundamentals/stage3-custom-orchestrator/README.md): a two-line concept placeholder, with no scheduler class, state machine, or executable custom orchestrator.

These are educational documentation artifacts rather than additional orchestration engines. Their inaccurate paths are historical limitations, not corrected code in this documentation-only change.

#### [fundamentals/stage2-cron/crontab.txt](fundamentals/stage2-cron/crontab.txt) and [logfile.log](fundamentals/stage2-cron/logfile.log)

The crontab has four hourly minute offsets. Only extraction is a concrete command; the remaining entries are templates. `>>` appends stdout and `2>&1` includes stderr in the same log. Absolute paths tie the example to one machine, while the script's output remains relative to the process working directory. The tracked log is empty. There is no log rotation, structured run ID, or record of successful scheduled execution.

#### [fundamentals/requirements.txt](fundamentals/requirements.txt)

The manifest pins pandas `3.0.6`, SQLAlchemy `2.1.4`, psycopg2-binary `2.9.13`, and python-dotenv `1.2.4`, as committed in phase 8. Their roles are DataFrame/CSV processing, SQL engine/table integration, PostgreSQL driver access, and environment loading. Package resolution and compatibility were not tested in this audit. This manifest neither installs Airflow nor declares all providers used by its DAGs.

### Airflow configuration and shared functions

#### [airflow/dags/include/config.py](airflow/dags/include/config.py)

**Purpose:** Supply Airflow callables with the business source, output directory, and direct database engine configuration.

The environment-loading block mirrors fundamentals. Since phase 17, `URL.create` receives the driver and individual connection components, avoiding manual URL credential interpolation. The source URL remains fixed, and `DATA_PATH` is `/opt/airflow/dags/data`.

This module is not an Airflow Connection or Variable definition. Import-time configuration is separate from the Hook's runtime `Superbase_Postgres_Conn`, and missing credential values are not explicitly rejected. Airflow metadata-database settings in Compose do not configure this business connection.

#### [airflow/dags/include/orders_util.py](airflow/dags/include/orders_util.py)

**Purpose:** Reuse the same Python business functions in the `orders` and `orders_sql` DAGs.

| Function | Audited lines | Important logic and rationale |
|---|---|---|
| `extract()` | 9-15 | Create the output directory idempotently; download the configured CSV; overwrite `raw_sales.csv`; print progress. The directory creation is the phase-17 fresh-checkout fix. |
| `load()` | 18-24 | Read the CSV; obtain a SQLAlchemy engine through `PostgresHook` using `Superbase_Postgres_Conn`; replace `raw_sales` inside `engine.begin()`. The phase-19 change provides an actual engine and a commit-on-success write boundary. |
| `transform()` | 27-44 | Use the direct configured URL, read `raw_sales`, apply the same pandas cleaning/derived-value steps as fundamentals, and replace `sales` through `connect()`. This has not inherited the raw loader's explicit transaction fix. |
| `notify()` | 47-72 | Read `sales` through the direct URL, calculate all-row summary metrics, and write `report.txt`. It retains the inaccurate revenue label and no empty-data guard. |

There are no classes or custom operators. Exceptions propagate to whichever Airflow operator invokes the function. `orders_sql` imports extraction, loading, and notification but deliberately replaces `transform()` with database-side SQL. Both DAGs still share output filenames and database tables.

#### [airflow/dags/include/orders_email.py](airflow/dags/include/orders_email.py)

**Purpose:** Support conditional processing and pass a summary into the email task.

| Function | Audited lines | Important logic and rationale |
|---|---|---|
| `extract()` | 8-15 | Read the source, write the same shared CSV, return `len(df)` for the branch decision. Unlike the shared extractor, it does not create the parent directory. |
| `check_data(ti)` | 18-23 | Pull `return_value` from `extract_orders`; return `load_orders` if the count is positive, otherwise `alert_no_record`. The return values are task IDs, not booleans. |
| `load()` | 26-33 | Use `Superbase_Postgres_Conn` after phase 22, obtain an engine correctly, and replace `raw_sales`. It uses `connect()`, not the transactional shared loader. |
| `notify(ti)` | 36-62 | Read `sales` through the direct URL, calculate the summary, label revenue “Top Revenue,” and push text to XCom key `report`. The commented `return message` is inactive. |

Only a row count and summary string are passed through XCom; the full DataFrame remains in CSV/PostgreSQL. This avoids using Airflow metadata storage for bulk data.

The count check distinguishes a successfully parsed CSV with zero rows from one with rows. A network/parser failure raises before branching; a missing/invalid XCom is not explicitly handled. The helper does not implement schema checks, date filtering, failure recovery, or deduplicated email delivery.

#### [airflow/dags/sql/orders.sql](airflow/dags/sql/orders.sql)

**Purpose:** Perform the SQL DAGs' transformation in PostgreSQL.

1. `DROP TABLE IF EXISTS sales` removes the prior output.
2. `CREATE TABLE sales AS` builds a replacement from the current `raw_sales`.
3. `SELECT DISTINCT` removes identical projected rows.
4. `CAST(order_date AS TIMESTAMP)` converts the date representation.
5. `COALESCE` supplies `"Unknown"` for missing customer/product/region text.
6. `quantity * price AS total_sale` computes the derived business measure.

The region expression lacks `AS region`. The SQL is not a perfect semantic match for pandas: it fills selected text fields rather than every field, and deduplication occurs on projected values after conversion. No constraints, indexes, business-key uniqueness checks, incremental watermark, or date-window predicate are added. Operator/provider transaction behavior must be checked before claiming atomic table replacement. A successful task is not evidence that its data met a defined quality contract.

### Runtime and developer surfaces

#### [airflow/docker-compose.yaml](airflow/docker-compose.yaml)

**Business purpose:** Supply a local environment in which engineers can inspect and run task-based orchestration.

**Major configuration blocks:**

- `x-airflow-common` centralizes the selected image, environment file, executor/auth settings, mounts, container user, and healthy Redis/PostgreSQL prerequisites.
- The metadata database/result backend support Airflow state and Celery results; Redis brokers queued work. These are separate concerns from the external sales database.
- API server, scheduler, DAG processor, worker, and triggerer are distinct services. No sensor or deferrable task is defined by this project's DAGs despite the triggerer service being available.
- The scheduler queues eligible tasks; Celery workers execute them. Worker concurrency is set to two, a local capacity limit rather than a measured scaling result.
- Health checks inspect API/scheduler responsiveness or process status. They do not validate business-row correctness or email delivery.
- `airflow-init` checks available resources, creates mounted directories, initializes configuration, invokes Airflow's entrypoint setup, and adjusts ownership. Its resource warnings are recommendations, not guaranteed capacity.
- CLI and Flower are optional profiles. Flower exposes Celery-level monitoring if enabled; it is not a committed business-monitoring DAG.
- DAGs, logs, generated configuration, and plugins are bind-mounted. PostgreSQL metadata uses a named volume.

**Operational qualifications:** Compose requires an environment file and two nonempty encryption/signing settings, pauses new DAGs, disables examples, publishes the UI at localhost port 8081, and specifies no automatic service restarts. The selected official image does not automatically incorporate the tracked Dockerfile. No custom business monitoring dashboard, backup policy, production deployment automation, or checked-in `airflow.cfg` is present.

#### [airflow/Dockerfile](airflow/Dockerfile)

Three lines select `apache/airflow:3.3.2` and install the PostgreSQL provider. This is a custom-image recipe, not proof that the current Compose services run that image. It does not explicitly install the SMTP provider or the fundamentals requirements, and the PostgreSQL provider version is not pinned. The available packages in the actual selected image must be checked rather than assumed.

#### [airflow/dags/oop.py](airflow/dags/oop.py)

`Dog` has a class attribute `name`; two objects are created and one name is printed at module level. There is no DAG object, task dependency, business requirement, or scheduler capability here. It is an educational example placed in the DAG directory, not an orchestration phase of its own.

#### [.gitignore](.gitignore), [.vscode/settings.json](.vscode/settings.json), and [workspace](orchestration-with-airflow.code-workspace)

Ignore rules evolve from environment files to virtual environments, caches, Airflow runtime directories, generated data, and the workspace filename. They reduce accidental versioning of local/generated material but do not validate secrets or remove already tracked files.

The VS Code setting suppresses an R-language-server installation prompt; it is unrelated to Python/Airflow correctness. The workspace opens the repository root as its sole folder. Neither defines debug tasks, interpreter selection, or orchestration configuration.

## DAG catalog and orchestration semantics

### 1. `first_dag` - Operator and dependency basics

**Source:** [00_first-dag.py](airflow/dags/00_first-dag.py). Introduced in `ef0b002`; renamed in `696a2d0` and integrated as a rename in `c82c3f2`.

```text
first_task [BashOperator] --> second_task [PythonOperator: message]
```

The DAG context sets `dag_id="first_dag"`, start date October 21, 2026, and `schedule=None`. The first task echoes a message; `message()` prints the second. The explicit `>>` dependency replaces time-offset assumptions with upstream-success ordering. It validates the teaching pattern but does not ingest business data.

### 2. `orders` - Python ELT with local reporting

**Source:** [01_orders.py](airflow/dags/01_orders.py). Introduced in `a105c1b`, refactored in `d25e600`, first integrated into main in `ec0f6d0`.

```text
extract_orders --> load_orders --> transform_orders --> notify_orders
```

The import block loads four shared functions. The DAG context sets `orders`, start date October 23, 2026, and no schedule. Each `PythonOperator` binds one task ID to its callable. The final chain is the complete workflow definition.

This separates failure visibility into four task instances and keeps business code out of the DAG definition. It still uses a shared CSV/table namespace and pandas transformation; reporting writes a file rather than sending email.

### 3. `orders_sql` - Database-side transformation

**Source:** [02_orders-sql.py](airflow/dags/02_orders-sql.py). Introduced in `d25e600`, first integrated in `ec0f6d0`; external SQL added through `ad5081b`; current connection alignment restored in `26d0847`.

```text
extract_orders [Python]
    --> load_orders [Python]
    --> transform_orders [SQLExecuteQueryOperator]
    --> notify_orders [Python]
```

The context defines `orders_sql`, start date October 23, and no schedule. The SQL operator points to the DAG-relative `sql/orders.sql` template and `Superbase_Postgres_Conn`. Python loading uses the same connection ID, while notification uses the direct URL.

Database-side processing avoids transferring the raw table into Python for transformation, but extraction/loading/reporting still use pandas and whole-data operations. Externalizing SQL improves separation of concerns, not a measured end-to-end scaling guarantee.

### 4. `dag1` - TaskFlow, runtime context, and XCom

**Source:** [03_taskflow.py](airflow/dags/03_taskflow.py). Introduced in `d92963d`, first integrated in `ad5081b`.

```text
task1 [push var_id] --> task2 [pull var_id and print]
```

`@dag(start_date=datetime(2026, 10, 26), schedule=None)` wraps `dag1`; nested `@task()` functions access the task instance through `**context`. Calling `dag1()` constructs/registers the graph. The name-derived DAG ID is `dag1`, not the filename.

This teaches explicit metadata transfer and ordering. The dependency is created with `>>`; no function return value is used to infer a data dependency. It is not connected to the business DAGs.

### 5. `orders_email` - Branching and stakeholder delivery

**Source:** [04_orders-email.py](airflow/dags/04_orders-email.py). Introduced in `696a2d0`, first integrated in `c82c3f2`, locally configured in `02988ab`.

```text
extract_orders
      |
      v
check_extract [BranchPythonOperator]
      |
      +-- positive count --> load_orders --> transform_orders
      |                                         |
      |                                         v
      |                                  generate_report --> send_report
      |
      +-- zero count ----> alert_no_record
```

**Major source blocks:**

- Imports select email-specific helpers, SQL execution, branching, and SMTP operators.
- The DAG context defines `orders_email`, October 23 start date, and `schedule=None`.
- `extract_orders` produces the count XCom; `check_extract` chooses a downstream task ID.
- The success branch uses `Superbase_Postgres_Conn` for raw loading and SQL transformation.
- `generate_report` pushes `report`; `send_report` reads it through `ti.xcom_pull(...)` in a Jinja HTML template.
- Both email tasks use `orders_email_conn`. Their subjects include `{{ ds }}` and bodies render `var.value.get("environment")`. The imported `Variable` symbol and commented direct lookup do not actively retrieve configuration in Python.
- Final dependency statements connect the branch alternatives and the positive-count sequence. The commented linear chain is not active.

**Branch behavior:** There is no convergence/join task. The unselected direct downstream branch is skipped, and ordinary downstream success requirements prevent its processing chain from running. There is no custom trigger rule added to run both outcomes.

**Operational impact:** The report can leave the local filesystem and reach stakeholders, while successful zero-row extraction can produce a specific notice. A task failure is different: it does not automatically execute `alert_no_record`. Neither email is a generic exception alert or delivery audit.

### Shared scheduling, reliability, and isolation rules

| Concept | Audited behavior |
|---|---|
| Scheduling | All five DAGs use `schedule=None`. Start dates define eligibility, not daily execution. “Daily” in email/report text does not create a schedule. |
| Triggering | Manual or external triggering is required; no trigger-DAG operator, asset dependency, sensor, or cross-DAG completion dependency is defined. |
| Pause behavior | Compose pauses new DAGs at creation. |
| Dependencies | Explicit `>>` edges model within-DAG order; numeric filename prefixes do not order DAG execution. |
| Retries and recovery | No DAG/task-specific retries, retry delays, failure callbacks, timeouts, SLAs, or custom recovery workflow are declared. Runtime defaults may apply and were not inspected. Manual clearing/reruns are possible platform operations, not a committed recovery policy. |
| Logging | Scripts/callables print progress; Airflow supplies task log context. Cron appends combined stdout/stderr. No structured application logger or retention/rotation policy is introduced. |
| Quality controls | Null substitution, deduplication, date conversion, and email row-count branching exist; assertions, freshness checks, reconciliations, and schema enforcement do not. |
| Run isolation | The three business DAGs share `raw_sales.csv`, `raw_sales`, and `sales`; two share `report.txt`. No run-specific partition or explicit one-active-run limit is declared. Concurrent triggers can interfere. |
| Distributed execution | The CSV uses a host bind mount under the DAG directory. It supports the described single-host stack, not automatically a multi-host worker fleet without shared storage. |
| Report scope | Queries read all `sales` rows; wall-clock `date.today()` differs from Airflow's logical `ds` for backfills/manual runs. No true daily-window filter exists. |

## Configuration and onboarding

This section describes the audited source's requirements, not a certified clean-checkout installation recipe.

### Required inputs

| Input | Consumer | Requirement or caution |
|---|---|---|
| `FERNET_KEY` | Compose / Airflow | Must be nonempty for Compose interpolation; use a valid Airflow encryption key kept outside Git. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Compose / Airflow services | Required nonempty shared signing value; do not commit it. |
| `DB_USER`, `DB_PASS` | Both business config modules | Used for direct business-database engine creation, not the Compose metadata database. |
| `Superbase_Postgres_Conn` | Business loaders and SQL operators | Provision an Airflow PostgreSQL Connection with this exact case-sensitive identifier and intended database details. No provisioning code is tracked. |
| `orders_email_conn` | Both SMTP tasks | Provision SMTP settings/credentials and transport options appropriate to the provider. No SMTP account is created by the DAG. |
| `environment` | Email Jinja templates | Set an Airflow Variable to label the environment; no active Python default or provisioning is committed. |
| Runtime packages | DAG imports | Verify pandas, SQLAlchemy, dotenv, psycopg2, and the standard/common-SQL/PostgreSQL/SMTP providers in the selected image. Imports alone do not prove installation. |
| Published CSV schema | Extraction/transform/reporting | Expected fields include `order_id`, `order_date`, `customer_name`, `product`, `quantity`, `price`, and `region`. Validate permissions, types, and availability before live use. |

Keep the direct URL and Airflow Connection pointed at the same business dataset. Otherwise the loader/SQL task can update one database while pandas transformation or reporting reads another.

### Standalone script path guidance

The original stage documentation contains outdated paths. From the repository root, an engineer evaluating the fundamentals can use the actual layout below after configuring dependencies/database access:

```powershell
Set-Location .\fundamentals
New-Item -ItemType Directory -Force .\data
python .\stage1-one-big-script\run_pipeline.py
```

This runs full-refresh database writes and creates/overwrites the report. Review the destination first; the command is not read-only. Running from `fundamentals` aligns the scripts' relative artifact paths. The requirements are recorded in [the fundamentals manifest](fundamentals/requirements.txt), but installation/version compatibility was not tested for this documentation change.

### Airflow preparation checklist

1. Review [Compose](airflow/docker-compose.yaml) and configure its untracked environment file beside it, or use `ENV_FILE_PATH`.
2. Decide whether to use the official selected image or a deliberately built custom image. The tracked [Dockerfile](airflow/Dockerfile) is not selected by the current Compose configuration.
3. Verify all required provider/import dependencies and validate the Compose configuration before initialization.
4. Provision the business PostgreSQL and SMTP Connections and the `environment` Variable. Avoid real stakeholder recipients during development.
5. Ensure the output directory exists and is writable before running `orders_email`; its extractor does not inherit the shared helper's automatic creation.
6. Initialize and inspect the local stack according to the selected Airflow image's requirements; the configured UI endpoint is `http://localhost:8081`.
7. Check DAG import errors, inspect task graphs, and deliberately trigger one business DAG at a time against a safe database.
8. Verify committed rows after loading/transformation and inspect branch choices, report content, and SMTP delivery. The source history alone does not establish those outcomes.

The cron example requires a Unix-like cron environment and machine-specific path replacement. It is not a native Windows scheduling configuration.

## Feature provenance

| Capability | First implementation | First inclusion in main | Subsequent change / qualification |
|---|---|---|---|
| Manual ELT instructions | `00694fc` | Same ancestry commit | Scripts arrive later. |
| CSV extraction / raw load | `1456771` | Same ancestry commit | Source centralized in `87d1416`. |
| Pandas transformation | `53cbd0f` | Same ancestry commit | Airflow version authored in `a105c1b`. |
| Propagated script failures | `beedce7` | Same ancestry commit | No retry/recovery policy added. |
| Local report file | `e339efe` | Same ancestry commit | Not email; all-row wall-clock summary. |
| Single-command pipeline | `17e85df` | Same ancestry commit | Sequential process, not durable task state. |
| Cron scheduling examples | `10d3828` | Same ancestry commit | Paths partly corrected in `026e57c`; three placeholders remain. |
| Airflow infrastructure and first DAG | `ef0b002` | Same commit | Local Compose behavior refined in `fd724d6`. |
| Business `orders` DAG | `a105c1b` | `ec0f6d0` | Helpers extracted in `d25e600`. |
| `orders_sql` / SQL operator | `d25e600` | `ec0f6d0` | External SQL authored in `d92963d`, integrated in `ad5081b`. |
| PostgreSQL provider image recipe | `d25e600` | `ec0f6d0` | Current Compose does not select the build. |
| Required Compose secrets / localhost UI | `fd724d6` | Same commit | Local-development scope, not production hardening. |
| Directory-safe shared extraction | `35d14eb` | Same commit | Not copied into email extraction. |
| Structured URL construction | `35d14eb` | Same commit | Airflow config only. |
| TaskFlow / explicit XCom demo | `d92963d` | `ad5081b` | Standalone `dag1`, no business-data dependency. |
| Hook-based shared loader | `d92963d` | `ad5081b` | Initially incomplete; corrected and transactional in `26d0847`. |
| Branching on extracted row count | `696a2d0` | `c82c3f2` | Only empty/nonempty; not a general quality gate. |
| Report XCom / SMTP / environment label | `696a2d0` | `c82c3f2` | Local connection/recipient settings aligned in `02988ab`. |
| Numbered DAG filenames | `696a2d0` | `c82c3f2` | Renames do not change DAG IDs or execution order. |
| Dedicated monitoring/failure-recovery DAG | Not introduced | Not present | Infrastructure health checks and no-data email are narrower capabilities. |

## Repository Evolution Summary

### Architecture evolution

1. **Human-controlled commands:** The initial contract relies on an engineer for ordering and initiation.
2. **Standalone data functions:** Extraction, raw loading, transformation, and reporting become executable with a local CSV/database handoff.
3. **One-process coordination:** A runner guarantees call order but cannot persist task-level state or selectively recover.
4. **Clock-based scheduling examples:** Cron removes some initiation work but substitutes elapsed time for successful dependencies. The custom-orchestrator stage remains conceptual.
5. **Explicit task orchestration:** Airflow introduces observable task instances, dependency edges, and a local distributed-executor architecture.
6. **Separation of concerns:** Helpers, configuration, and later external SQL reduce DAG-definition clutter; database-side transformation becomes an alternative to pandas.
7. **Configuration and write correctness:** Directory creation, structured URLs, correct Hook invocation, and a transaction boundary address practical execution defects.
8. **Conditional distribution:** A fifth DAG routes empty/nonempty input and distributes a summary through SMTP/XCom, while preserving earlier examples.

This is not a purely linear replacement architecture. Old stages and DAG variants remain together for learning, sharing resources and sometimes duplicating behavior. The newest example is not automatically the most robust implementation in every detail.

### Orchestration maturity journey

| Dimension | Early state | Audited state | Remaining maturity gap |
|---|---|---|---|
| Initiation | Manual commands | Cron examples and manually/external-triggered DAGs | No periodic business-DAG schedule is declared. |
| Dependency control | Human memory / minute offsets | Explicit Airflow edges and branch task IDs | No cross-DAG coordination or run-isolated shared outputs. |
| Failure visibility | Broad catches initially hide failures | Exceptions generally propagate into task failures | No explicit automated recovery/failure-notification policy. |
| Modularity | Independent scripts, then embedded DAG functions | Shared helper/config/SQL modules | Email helper duplicates and diverges from shared fixes. |
| Persistence correctness | Replace writes under `connect()` | Shared raw loader uses `begin()` | Email loader and pandas transform need transaction verification. |
| Reporting | Local text output | SMTP summary and no-row email path | No verified delivery, date-window contract, or idempotency. |
| Observability | Prints and empty cron log | Airflow task logs/state, service health checks, optional Flower | No business metrics, freshness alerts, or delivery audit. |
| Deployment | No runtime definition | Local Celery/Redis/PostgreSQL Compose stack | Image/dependency alignment and production operations remain unresolved. |

### Key lessons learned

- **A scheduler is not a dependency engine.** Cron offsets cannot guarantee extraction has completed or succeeded before loading.
- **Failing visibly is necessary.** Removing swallowed exceptions makes downstream coordination meaningful, but does not replace transaction management or recovery.
- **A working function is not a deployable workflow.** Output directories, runtime dependencies, credentials, Connections, and environment Variables are part of the execution contract.
- **Merges can regress correctness.** `ad5081b` reintroduced an old connection ID and imported an incomplete Hook call; `26d0847` repaired them.
- **Preserving a fix is not propagating it.** `c82c3f2` retained the shared loader fixes, but the new email helper still omitted directory creation and explicit write transactions.
- **“Daily” is a business contract, not a label.** All-row queries, `date.today()`, `{{ ds }}`, and `schedule=None` do not together implement date-windowed daily reporting.
- **Separate examples can collide operationally.** Distinct DAG IDs do not isolate a shared CSV or replace-table database outputs.
- **Commit messages require patch verification.** Several subjects overstate or misidentify their actual diff; the final commit configures reporting introduced earlier.

### Major milestones

- **October 14:** Manual instructions, extraction/loading, and pandas transformation establish the ELT foundation.
- **October 19:** Configuration centralization, propagated failures, local reports, a one-command runner, and cron examples expand the fundamentals.
- **October 21:** First Airflow DAG and a local Celery-based runtime enter the history.
- **October 23-25:** Upstream development adds business DAGs, reusable helpers, SQL transformation, and a provider-image recipe.
- **October 26:** Main integrates business DAGs; upstream adds TaskFlow/XCom/external SQL.
- **October 27:** Main fixes directories/URL construction, integrates TaskFlow, then repairs the Hook call, transaction boundary, and connection naming.
- **October 28:** Upstream authors branching and email reporting and numbers existing DAG files.
- **October 30:** Main integrates reporting and aligns local PostgreSQL/SMTP routing.

### Current state assessment

| Area | Assessment | Evidence and consequence |
|---|---|---|
| Reliability | Improved but uneven | Explicit dependencies and propagated exceptions exist; only the shared raw loader has an explicit `begin()` write boundary. Email extraction assumes the output directory already exists. |
| Maintainability | Stronger educational structure, partial duplication | Thin DAGs/shared SQL help, but fundamentals, shared helpers, and email helpers repeat logic and have diverged. |
| Scalability | Local executor architecture, not demonstrated business scale | Celery is configured with concurrency two; full pandas reads, shared CSV storage, and replace-table writes limit safe parallelism. |
| Monitoring | Platform-level foundations | Health checks, task logs/state, and optional Flower exist. No committed business-monitoring or freshness workflow exists. |
| Reporting | Richer delivery capability, not a validated reporting service | Text metrics, XCom HTML rendering, SMTP, and no-row routing exist. No date filtering, verified delivery, or duplicate-send protection is shown. |
| Operational readiness | Learning/local-development system | Required runtime provisioning, package/image verification, transaction consistency, isolation, retries, and production controls remain. |

### Prioritized follow-up work

These are recommendations derived from the audited source, **not changes already implemented by this README**:

1. Align the selected runtime image with all DAG dependencies and validate clean imports.
2. Consolidate extraction/loading helpers so the email workflow inherits directory creation, consistent connection handling, and commit-on-success writes.
3. Verify and standardize transformation/load transactions, then test committed data from a separate connection.
4. Eliminate shared-run collisions through deliberate concurrency limits or run-partitioned artifacts/tables before parallel execution.
5. Define reporting windows and use logical dates consistently; add a schedule only when that business contract is clear.
6. Add focused tests for nonempty input, empty input, invalid schema/dates, unavailable source/database, branch skipping, and SMTP failure.
7. Add explicit retry/timeout/failure-alert policies and decide how to avoid duplicate reports during reruns.
8. Replace placeholder cron commands and stale instructional paths if those historical stages are intended to remain runnable.
9. Define production deployment, credential management, log retention, backup/recovery, and monitoring requirements independently of the local Compose example.

### Final conclusion

The repository's main engineering progression is from **remembered order**, to **coded sequence**, to **clock-based examples**, to **explicit task graphs**, and finally to **conditional report distribution**. Its most consequential correctness changes are not just new DAGs: they include propagating failures, creating ignored artifact directories, constructing database URLs safely, invoking Hooks correctly, and committing writes deliberately.

The audited history supports a useful and increasingly modular orchestration teaching framework. It does not yet support an evidence-based claim of production reliability, automated daily operation, scalable concurrent runs, or verified stakeholder delivery.
