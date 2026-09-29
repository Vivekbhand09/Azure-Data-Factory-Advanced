<div align="center">

![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0062AD?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure SQL Database](https://img.shields.io/badge/Azure_SQL_Database-0078D4?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![Azure Logic Apps](https://img.shields.io/badge/Azure_Logic_Apps-0062AD?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white)
![JSON](https://img.shields.io/badge/JSON-000000?style=for-the-badge&logo=json&logoColor=white)
![REST API](https://img.shields.io/badge/REST_API-02569B?style=for-the-badge)
![Gmail](https://img.shields.io/badge/Gmail_Alerts-D14836?style=for-the-badge&logo=gmail&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Data_Engineering-FF6F00?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Learning_Project-blueviolet?style=for-the-badge)

</div>

# 🏭 Azure Data Factory: Advanced Real-World Projects

Five end-to-end pipelines I built in **Azure Data Factory (ADF)** while learning data engineering. Each project has a short **theory** section, the **pipeline flow**, exactly how I **implemented** it (activities, settings and expressions), and the **limitations** I found with ideas for a production version.

> 💡 **About this repo:** This is a learning project. It goes beyond basic copy pipelines into incremental loading, alerting, scheduling, API ingestion, routing and dynamic mapping.

---

## 📑 Table of Contents

1. [Tools and Concepts Used](#-tools-and-concepts-used)
2. [Project 1: Incremental Load with Backdated Refresh](#-project-1-incremental-load-with-backdated-refresh)
3. [Project 2: Failure Alerts with Logic Apps and Triggers](#-project-2-failure-alerts-with-logic-apps-and-triggers)
4. [Project 3: REST API Ingestion with Pagination](#-project-3-rest-api-ingestion-with-pagination)
5. [Project 4: Router Pipeline with Validation and Switch](#-project-4-router-pipeline-with-validation-and-switch)
6. [Project 5: Dynamic Column Mapping with One Copy Activity](#-project-5-dynamic-column-mapping-with-one-copy-activity)
7. [Summary of All Projects](#-summary-of-all-projects)
8. [Key Takeaways](#-key-takeaways)

---

## 🧰 Tools and Concepts Used

**☁️ Services:** Azure Data Factory, Azure Data Lake Storage Gen2 (ADLS Gen2), Azure SQL Database, Azure Logic Apps, Gmail (alerts), PokeAPI (public REST API)

| Concept | What it does (in simple words) |
|---|---|
| 🔌 **Linked Service** | The connection to an external system (URL, credentials, authentication) |
| 📂 **Dataset** | Points to specific data (file, folder, table or API endpoint) inside a linked service |
| 🎛️ **Parameterised dataset** | A dataset with parameters (container, folder, file) so one dataset can be reused everywhere |
| 🔎 **Lookup** | Reads a small amount of data (max 5,000 rows / about 4 MB), for example a watermark |
| 📜 **Script** | Runs SQL (SELECT, DML, DDL) and returns result sets |
| 🔀 **If Condition** | True / False branching |
| 🚦 **Switch** | Many-way branching on a string expression (like `switch/case`) |
| 🔁 **ForEach** | Loops over a list, once per item |
| 📋 **Copy Activity** | Moves data from a source to a sink |
| 🔎 **Get Metadata** | Reads file or folder information (child items, size, last modified) |
| ⏳ **Validation** | Waits until a file or folder is ready (exists, minimum size, has children) |
| 🌐 **Web Activity** | Calls a REST endpoint from a pipeline (small responses only) |
| ▶️ **Execute Pipeline** | Calls one pipeline from another (parent and child) |
| ⏰ **Trigger** | Starts a pipeline automatically (Schedule, Tumbling Window, Storage Events, Custom Events) |
| 🧮 **Expressions** | Dynamic logic such as `@greater()`, `@if()`, `@equals()`, `@split()`, `@pipeline().RunId` |

### 🧬 Common building blocks used across projects

| Dataset | Type | Purpose |
|---|---|---|
| `ds_csv_dynamic` | ADLS Gen2 + CSV, parameters `p_container`, `p_folder`, `p_files` | One reusable dataset for the router and dynamic mapping projects |
| `ds_cdc` | ADLS Gen2 + JSON | Reads and writes the watermark file |
| `ds_database` | Azure SQL | Source for the incremental load |
| `ds_parquet` | ADLS Gen2 + Parquet | Sink for the incremental load |
| `ds_API` / `ds_API_json` | REST (source) / JSON (sink) | Source and sink of the API ingestion |

---

## 📥 Project 1: Incremental Load with Backdated Refresh

### 🎯 Goal
Copy only **new or changed rows** from Azure SQL DB to the Data Lake as **Parquet**, instead of copying the whole table on every run. Also support a **manual reload from an older date** (backdated refresh).

### 📖 Theory
**Watermark-based incremental loading** (also called the CDC-timestamp method): store how far you have already loaded, and on the next run load only rows newer than that value. ADF pipelines have no memory between runs, so the watermark must live outside the pipeline, for example in a control table or a small file. I used a file.

Azure setup (all inside one Resource Group):

| Resource | Role |
|---|---|
| 🗄️ Storage Account (ADLS Gen2) | Holds the watermark file and the Parquet output |
| 🛢️ Azure SQL Database | Source system with the `Orders` table |
| 🏭 Azure Data Factory | Orchestration and data movement (stores no data itself) |

### 🔀 Pipeline flow

```
[Lookup: last_cdc]        -> reads old timestamp from cdc.json
        |
        v
[Script: Total_Results]   -> counts rows newer than that timestamp
        |
        v
[If Condition: If New Rows]  -> count > 0 ?
        |
        |-- TRUE:
        |     [Copy: Load]          -> SQL -> Parquet (only new rows)
        |           |
        |           v
        |     [Script: MaxCDC]      -> gets MAX(last_updated)
        |           |
        |           v
        |     [Copy: change_cdc]    -> writes new timestamp into cdc.json
        |
        |-- FALSE: empty, do nothing
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 0 | **Source table + watermark file** | Table `source.Orders` with `last_updated DATETIME DEFAULT GETDATE()` (10 rows dated 20 to 25 Sept 2025). File `source/monitor/cdc.json` containing `{"cdc_timestamp": "1900-01-01"}` so the first run is a full load |
| 1 | **Linked services** | `ls_datalake` (ADLS Gen2) and `ls_database` (Azure SQL) |
| 2 | **Pipeline parameters** | `schema` (for example `source`), `table` (for example `Orders`), `backdate` (empty by default) |
| 3 | **last_cdc** (Lookup) | Dataset `ds_cdc`, reads the last timestamp |
| 4 | **Total_Results** (Script) | Counts rows newer than the watermark (query below) |
| 5 | **If New Rows** (If Condition) | True: Load, MaxCDC, change_cdc. False: empty |
| 6 | **Load** (Copy, inside True) | Source `ds_database` with a query, sink `ds_parquet` (`sink/Orders/`) |
| 7 | **MaxCDC** (Script, inside True) | `SELECT MAX(last_updated) AS cdc_timestamp FROM @{schema}.@{table}` |
| 8 | **change_cdc** (Copy, inside True) | Source `ds_emptyjson` (a file containing `{}`) plus an **Additional column** `cdc_timestamp`, sink `ds_cdc` (overwrites `cdc.json`) |

**Reading the watermark**

```
@activity('last_cdc').output.value[0].cdc_timestamp
```

`output.value` is an array, so `[0]` is needed. If "First row only" is checked, use `output.firstRow.cdc_timestamp` instead.

**Total_Results script**

```sql
SELECT COUNT(*) AS total_count
FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
WHERE last_updated > '@{activity('last_cdc').output.value[0].cdc_timestamp}'
```

**If Condition expression**

```
@greater(activity('Total_Results').output.resultSets[0].rows[0].total_count, 0)
```

**Load source query (this is the incremental part)**

```sql
SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
WHERE last_updated > '@{activity('last_cdc').output.value[0].cdc_timestamp}'
```

**change_cdc additional column value**

```
@activity('MaxCDC').output.resultSets[0].rows[0].cdc_timestamp
```

- `empty.json` must contain **one row** (for example `{}`). If the file has zero records, Copy reads nothing and the additional column has nothing to attach to.
- The watermark only moves **after** a successful load. If Load fails, the watermark stays old and the next run retries the same rows.

### ⏪ Backdated Refresh
Re-load data from an **older date** than the current watermark, manually and on demand.

**When it is needed:** late-corrected source data, pipeline downtime, a transformation bug that needs reprocessing, late-arriving data, or a new consumer that needs history from a specific date.

**Implementation:** I added the `backdate` parameter and changed only two places, the `Total_Results` script and the `Load` query:

```
where last_updated > '@{if(empty(pipeline().parameters.backdate),
                            activity('last_cdc').output.value[0].cdc_timestamp,
                            pipeline().parameters.backdate)}'
```

- `backdate` empty: use the watermark from `cdc.json` (normal run)
- `backdate` has a value (for example `2025-09-22`): use that date instead
- After a backdated run, MaxCDC and change_cdc set the watermark to the latest date again, so normal runs continue smoothly

### 🧪 The 3 scenarios I tested

| Scenario | What happens |
|---|---|
| New data in source | Count > 0, Load runs, watermark updated |
| No new data | Count = 0, False branch, nothing copied, watermark unchanged |
| Backdate given | Query uses the backdate, old rows re-copied, watermark set to the latest |

---

## 🚨 Project 2: Failure Alerts with Logic Apps and Triggers

### 🎯 Goal
Run the incremental pipeline **automatically on a schedule** and get an **email whenever it fails**.

### 📖 Theory
Production pipelines run unattended, usually at night. If one fails and nobody knows, data goes stale and reports break. The technique used here is a **parent (wrapper) pipeline** that calls the child pipeline with **Execute Pipeline**. On failure, a **Web activity** calls a **Logic App** over HTTP, and the Logic App sends the email.

| Alert method | How it works |
|---|---|
| 🧩 Logic Apps + Web activity (used here) | Pipeline calls a Logic App that sends email, Teams or Slack messages. Fully custom content |
| 📈 Azure Monitor alert rules | Built-in alerts on ADF metrics such as "Failed pipeline runs". No pipeline changes |
| ⚡ Azure Functions / webhooks | Same idea as Logic Apps but with code |

### 🔀 Pipeline flow

```
[Schedule Trigger]  -> fires at the set time
        |
        v
[Pipeline: Scheduled]  (parent)
        |
        v
[Execute Pipeline]  -> runs "Incremental ingestion" (child)
        |
        |-- On Success: nothing extra
        |
        |-- On Failure:
              [Web activity] -> POST to Logic App URL
                    |  body: pipeline_name + pipeline_runId
                    v
              [Logic App: When an HTTP request is received]
                    |
                    v
              [Send an email (V2)] -> Gmail alert
```

### 🔧 Implementation

| Step | Component | Settings |
|---|---|---|
| 1 | **Logic App** (Consumption) | Created in the same resource group. Trigger: *When an HTTP request is received*. Action: *Send an email (V2)* using a Gmail connection |
| 2 | **Copy the HTTP POST URL** | Generated only **after saving** the Logic App. Contains a secret `sig=` key |
| 3 | **Parent pipeline `Scheduled`** | One **Execute Pipeline** activity pointing to the incremental pipeline, **Wait on completion = checked**, passing `schema`, `table` and an empty `backdate` |
| 4 | **Web activity** | Connected with the **On failure (red)** line. URL = Logic App URL, Method = `POST`, header `Content-Type: application/json`, Authentication = None (the key is in the URL) |
| 5 | **Schedule trigger** | Attached to the parent pipeline, then **published** and **started** |

**Web activity body**

```json
{
  "pipeline_name": "@{pipeline().Pipeline}",
  "pipeline_runId": "@{pipeline().RunId}"
}
```

**Email template inside the Logic App**

```
Subject: ADF Pipeline Failed: @{triggerBody()?['pipeline_name']}

Hello,
The pipeline "@{triggerBody()?['pipeline_name']}" has failed.
Run ID: @{triggerBody()?['pipeline_runId']}
Please check the Monitor tab in Azure Data Factory.
```

To make the two fields selectable as dynamic content, paste a sample JSON into **Use sample payload to generate schema** in the trigger.

### 🔗 Dependency conditions

| Condition | Next activity runs when |
|---|---|
| ✅ On success (green) | The previous activity succeeded |
| ❌ On failure (red) | The previous activity failed |
| 🔵 On completion (blue) | The previous activity finished, success or failure |
| ⚪ On skip (grey) | The previous activity was skipped |

ADF has no try/catch block. **On-failure paths are how error handling is done.**

### ⏰ Triggers in ADF

| Trigger | When it fires | Typical use |
|---|---|---|
| Schedule | At set times or on a repeat | Nightly incremental loads (**used here**) |
| Tumbling Window | Fixed, non-overlapping time slices, each tracked | Time-slice processing, backfill |
| Storage Events | When a blob is created or deleted (via Event Grid) | Process a file as soon as it lands |
| Custom Events | When an event is published to an Event Grid topic | Event-driven architectures |
| Manual | Only when clicked or called by API / SDK | Testing, one-off runs |

**Schedule vs Tumbling Window**

| Feature | Schedule | Tumbling Window |
|---|---|---|
| Trigger to pipeline | Many-to-many | One-to-one |
| Time slices | No window concept | Each run has a window start and end |
| Backfill of past periods | Not supported | Supported |
| Retry policy | Not built in | Built in |
| Dependencies and concurrency control | No | Yes |

**System variables used:** `@pipeline().Pipeline` (pipeline name), `@pipeline().RunId` (unique run ID, used to find the failed run in Monitor). Others worth knowing: `DataFactory`, `TriggerName`, `TriggerType`, `TriggerTime`, `GroupId`.

### 🧪 How I tested the alert
1. Break the child pipeline on purpose (for example a wrong table name)
2. Run the parent with Debug or Trigger now
3. Confirm the Execute Pipeline activity is red and the Web activity is green (HTTP 200)
4. Check the Logic App run history and the Gmail inbox (also spam)
5. Fix the issue and confirm **no** email arrives on a successful run


---

## 🌐 Project 3: REST API Ingestion with Pagination

### 🎯 Goal
Pull data from a public REST API (**PokeAPI**) into the Data Lake as raw JSON, and get **all records, not only the first 20**.

### 📖 Theory
Many sources (SaaS tools, public data, internal apps) expose data only through APIs. APIs return data in small **pages**, so without pagination handling the pipeline succeeds but silently loads incomplete data.

PokeAPI response shape:

```json
{
  "count": 1302,
  "next": "https://pokeapi.co/api/v2/pokemon?offset=20&limit=20",
  "previous": null,
  "results": [
    { "name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/" },
    { "name": "ivysaur",   "url": "https://pokeapi.co/api/v2/pokemon/2/" }
  ]
}
```

| Field | Meaning |
|---|---|
| `count` | Total records available |
| `next` | URL of the next page (becomes `null` on the last page) |
| `results` | Records on this page (20 by default) |

The `next` field is exactly what the pagination rule uses.

### 🔀 Pipeline flow

```
[Web activity]  -> GET https://pokeapi.co/api/v2/pokemon  (test the API call)
        |
        v  (On success)
[Copy activity]
    Source: ds_API (REST dataset via ls_REST)
            + Pagination rule: AbsoluteUrl -> Body -> $.next
    Sink:   JSON dataset in ADLS Gen2
```

### 🔧 Implementation

| Step | Component | Settings |
|---|---|---|
| 1 | **Web activity** | URL `https://pokeapi.co/api/v2/pokemon`, Method `GET`. A quick test that the API is reachable. Output readable via `@activity('Web1').output.count` |
| 2 | **On success line** | Copy runs only if the API call worked |
| 3 | **Linked service `ls_REST`** | Type REST, Base URL `https://pokeapi.co/api/v2/`, Authentication **Anonymous** |
| 4 | **Dataset `ds_API`** (REST) | Relative URL `pokemon`. Base URL + Relative URL = full URL, so one linked service serves many endpoints |
| 5 | **Sink dataset `ds_API_json`** | JSON in ADLS Gen2 (raw / bronze layer, source data stored unchanged) |
| 6 | **Copy activity** | Source `ds_API`, method `GET`, **Pagination rule** below |

**The pagination rule (Copy activity, Source tab)**

| Field | Value |
|---|---|
| Name | `AbsoluteUrl` |
| Value | `Body` with JSONPath `$.next` |

How it works: Copy calls `/pokemon` and gets page 1, reads `$.next` (the URL of page 2), calls it, and repeats. On the last page `next` is `null`, so Copy stops automatically.

> ⚠️ Choosing **Body** alone is not enough. The value must include the JSONPath `$.next`, otherwise the rule cannot find the next URL and only page 1 is loaded.

**Optional: one row per record.** By default each page is one JSON object with the records nested inside `results`. In the **Mapping** tab, set **Collection reference** to `$['results']` so ADF flattens the array and writes one row per Pokémon. Very useful when the sink is a table or Parquet.

### 📌 Pagination styles ADF supports

| Style | ADF pagination rule |
|---|---|
| Next-page URL in the body (PokeAPI) | `AbsoluteUrl` → Body → `$.next` |
| Next-page URL in a header | `AbsoluteUrl` → Headers → header name |
| Offset / limit | `QueryParameters.offset` |
| Page number | `QueryParameters.page` |
| Cursor / token | `QueryParameters.token` or `Headers.token` with a JSONPath |
| Stop condition | `EndCondition: <JSONPath>` |
| Safety cap or testing | `MaxRequestNumber` |

### 🆚 Web vs Copy vs Lookup for APIs

| Activity | Best for | Limits |
|---|---|---|
| Web | Small calls: trigger something, get a token or a status | About 4 MB, no pagination |
| Copy (REST source) | Moving large or paginated API data into storage | Supports pagination, headers, request interval |
| Lookup (REST dataset) | Reading a small response as config | 5,000 rows / about 4 MB |

### 🚧 Limitations and how to improve
- **Silent data loss** if pagination is missing. Always compare the API `count` with the number of records written to the sink.
- **Rate limiting:** use the *Request interval* setting, retries with delay, and expect HTTP 429 errors.
- **Authentication:** PokeAPI is anonymous, but real APIs need Basic, Service Principal, Managed Identity or OAuth2. Keep secrets in **Key Vault**, never hard-coded.
- **Incremental loads from APIs:** if the API supports a filter like `updated_since`, reuse the watermark idea from Project 1.
- **Retry policy:** set retry count and interval on the Copy activity.
- **Schema drift:** storing raw JSON first keeps the pipeline safe when the API changes.
- **Private APIs** need a Self-hosted Integration Runtime. Public APIs use the default Azure IR.
- **Use `MaxRequestNumber` while testing** so you do not pull thousands of pages while debugging.

---

## 🚦 Project 4: Router Pipeline with Validation and Switch

### 🎯 Goal
Wait for a **trigger (flag) file** (`locations.csv`), then read all files in `source/files` and **route each file to its own Copy activity** based on its name. Finally, **delete the trigger file** so the next run starts clean.

### 📖 Theory
- **Validation activity:** makes the pipeline wait (polling) until a file or folder meets a condition. It fails when the timeout is reached. This supports the *flag file* pattern, where the source system drops a small file to say "all my data is ready".
- **Switch activity:** like `switch/case`. It evaluates a string expression, runs the matching case, and runs the Default case if nothing matches.

| | Validation | Get Metadata | Storage Event Trigger |
|---|---|---|---|
| Purpose | Wait until ready | Read info once | Start a pipeline on a blob event |
| Waits / polls? | Yes | No | Event-driven |
| Runs inside a pipeline? | Yes | Yes | No, it starts the pipeline |
| If item missing | Fails after timeout | Fails immediately | n/a |

| | If Condition | Switch |
|---|---|---|
| Branches | 2 (True / False) | Many cases + Default |
| Input | Boolean | String |
| Best for | Yes / No decisions | Routing among several known values |

### 🔀 Pipeline flow

```
Validation -> Get Metadata -> ForEach -> (after loop) Delete locations.csv
                                 |
                                 +-- Switch (@split(item().name, '.')[0])
                                        |-- customers -> Copy
                                        |-- drivers   -> Copy
                                        |-- trips     -> Copy
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 1 | **Parameterised dataset `ds_csv_dynamic`** | Parameters `p_container`, `p_folder`, `p_files`, wired in through `@dataset().p_container`, `@dataset().p_folder`, `@dataset().p_files`. Reused by Validation and every Copy source and sink |
| 2 | **Validation** | Dataset `ds_csv_dynamic`, `p_container = source`, `p_folder = trigger`, `p_files = locations.csv`. Set a realistic **Timeout** and **Sleep** (polling interval) |
| 3 | **Get Metadata** | Folder `source/files`, field list: **Child items** |
| 4 | **ForEach** | Items: `@activity('Get Metadata').output.childItems` |
| 5 | **Switch** (inside ForEach) | Expression `@split(item().name, '.')[0]`, one case per file name |
| 6 | **Copy** (inside each case) | Source `source/files/@item().name`, sink container `sink`, folder `@item().name` |
| 7 | **Delete** (after the loop) | Deletes `locations.csv` so the next run starts clean |

**How the Switch expression works**

| Part | Meaning |
|---|---|
| `item().name` | Current file, for example `customers.csv` |
| `split(..., '.')` | Splits at the dot into `['customers', 'csv']` |
| `[0]` | Takes the first element: `customers` |

**Cases and Copy settings**

| Case value | File it catches | Action |
|---|---|---|
| `customers` | `customers.csv` | Copy |
| `drivers` | `drivers.csv` | Copy |
| `trips` | `trips.csv` | Copy |

| | Source | Sink |
|---|---|---|
| Dataset | `ds_csv_dynamic` | `ds_csv_dynamic` |
| `p_container` | `source` | `sink` |
| `p_folder` | `files` | `@item().name` |
| `p_files` | `@item().name` | `@item().name` |

---

## 🧩 Project 5: Dynamic Column Mapping with One Copy Activity

### 🎯 Goal
Copy 3 CSV files (`customers`, `drivers`, `trips`) from `source/files` to `sink/files` using **one Copy activity**, where each file automatically gets **its own column mapping (schema)**.

### 📖 Theory
**Mapping** tells the Copy activity which source column goes to which sink column, and with what data type.

| Mapping type | Description |
|---|---|
| Auto (default) | ADF matches columns by name |
| Explicit (manual) | You define each source-to-sink pair, types, renames and skipped columns |
| **Dynamic** | The mapping is supplied **at runtime** through an expression or parameter |

Behind the scenes, mapping is stored as a **`TabularTranslator`** JSON object:

```json
{
  "type": "TabularTranslator",
  "mappings": [
    {
      "source": { "name": "customer_id", "type": "String", "physicalType": "String" },
      "sink":   { "name": "customer_id", "type": "String", "physicalType": "String" }
    }
  ],
  "typeConversion": true,
  "typeConversionSettings": { "allowDataTruncation": true, "treatBooleanAsNumber": false }
}
```

Because it is just an object, it can be passed dynamically instead of being hard-coded in the Copy activity.

| Approach | Structure |
|---|---|
| ❌ Without dynamic mapping | ForEach → Switch → 3 separate Copy activities (harder to maintain, grows with every new file) |
| ✅ With dynamic mapping | ForEach → **ONE Copy** → mapping picked by an expression |

### 🔀 Pipeline flow

```
Get Metadata -> ForEach (childItems) -> ONE Copy Activity
  (source/files)                          |-- Source: ds_csv_dynamic (source/files/@item().name)
                                          |-- Sink:   ds_csv_dynamic (sink/files/@item().name)
                                          |-- Mapping: chosen dynamically per file
```

### 🔧 Implementation

| Step | What I did |
|---|---|
| 1 | **Get Metadata**: dataset `ds_meta` (folder `source/files`, file left blank), field list **Child items** |
| 2 | **ForEach**: Items `@activity('Get Metadata').output.childItems`. `@item().name` gives the current file |
| 3 | **Parameterised dataset** `ds_csv_dynamic` with `p_container`, `p_folder`, `p_files` |
| 4 | Created **3 Object pipeline parameters**: `customers_schema`, `drivers_schema`, `trips_schema` |
| 5 | Got each mapping JSON by using a temporary Copy activity on that file, **Import schemas** in the Mapping tab, opened **Code view**, copied the `TabularTranslator` block, and pasted it as the parameter's default value |
| 6 | **One Copy activity** inside ForEach with source and sink on `ds_csv_dynamic`, and the dynamic mapping below |

**Dynamic mapping expression**

```
@if(
  equals(item().name, 'customers.csv'),
  pipeline().parameters.customers_schema,
  if(
    equals(item().name, 'drivers.csv'),
    pipeline().parameters.drivers_schema,
    pipeline().parameters.trips_schema
  )
)
```

| Iteration | File | Mapping used |
|---|---|---|
| 1 | `customers.csv` | `customers_schema` |
| 2 | `drivers.csv` | `drivers_schema` |
| 3 | `trips.csv` | `trips_schema` (the else branch) |

### 🆚 Dynamic mapping vs Switch + multiple Copy

| Feature | Switch + 3 Copy | Dynamic mapping + 1 Copy |
|---|---|---|
| Number of Copy activities | 3 (grows with files) | 1 |
| Maintenance | Higher | Lower |
| Best when | Files need **different logic** | Files need the **same logic**, different schema |

**Rule of thumb:** if only the *mapping* differs, use dynamic mapping. If the *processing steps* differ, use Switch (Project 4).

```json
{
  "customers": { "type": "TabularTranslator", "mappings": [ ... ] },
  "drivers":   { "type": "TabularTranslator", "mappings": [ ... ] },
  "trips":     { "type": "TabularTranslator", "mappings": [ ... ] }
}
```

```
@pipeline().parameters.schemas[split(item().name, '.')[0]]
```

  For a fully **metadata-driven** design, store the mappings in a config file or table and read them with a **Lookup** activity.

---

## 📊 Summary of All Projects

| # | Project | Question it answers | Key technique |
|---|---|---|---|
| 1 | 📥 Incremental Load + Backdate | How do I load only new or changed rows, and reload history safely? | Watermark in `cdc.json`, Lookup + Script + If + Copy, `backdate` parameter |
| 2 | 🚨 Failure Alerts + Triggers | How do I run on a schedule and get told when it fails? | Parent pipeline, On-failure path, Web activity → Logic App → Gmail, Schedule trigger |
| 3 | 🌐 REST API Pagination | How do I get all records from an API, not just page 1? | REST linked service and dataset, `AbsoluteUrl` → Body → `$.next` |
| 4 | 🚦 Router Pipeline | How do I wait for a flag file and route each file to its own logic? | Validation + Get Metadata + ForEach + Switch |
| 5 | 🧩 Dynamic Mapping | How do I map columns per file with a single Copy? | Object parameters + `if(equals())` in the Mapping tab |

---

## 🧠 Key Takeaways

- 💧 **Pipelines have no memory.** Watermarks must be stored outside ADF (a file or control table), and only advanced after a successful load.
- ✅ **A pipeline that "succeeds" can still be wrong.** Missing pagination, zero files or a missing Default case all fail silently, so add count checks and Default branches.
- 🚨 **Errors are handled with paths, not try/catch.** Use On-failure lines, Fail activities and Azure Monitor as a safety net.
- 🎛️ **Parameterise everything.** One dataset, one Copy activity and one pipeline can serve many files, tables and schemas.
- 🚦 **Pick the right tool:** Validation to wait, Switch to route, If for yes/no, ForEach to loop, dynamic mapping when only the schema differs.
- 🔐 **Protect secrets.** Logic App URLs and API keys belong in Azure Key Vault, not in plain text or on GitHub.
- ⏱️ **Time zones and triggers:** schedules default to UTC, and triggers only run when published and started.
- 🔁 **Design for reruns.** Make sinks idempotent so backdated loads and retries do not create duplicates.
- 🚧 **Know the limitations.** Understanding what a pattern does not handle is as important as building it.

---
