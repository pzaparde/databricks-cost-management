# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC # Cost Management Accelerator - one-click install (no bundle / CLI needed)
# MAGIC
# MAGIC Use this when you cloned the repo into a **Git folder** and just want it running.
# MAGIC Every widget has a default, so you can just **Run all** (catalog = workspace default catalog, schema = `cost_management`, history from 2025-01-01, all workspaces, auto-picked SQL warehouse). Change a widget only to override.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC | --- | --- |
# MAGIC | setup | Creates the schema if missing, then landing volume, config tables, (optional) sample cloud data - runs `src/setup/run_setup` |
# MAGIC | pipeline | Creates or updates the Lakeflow pipeline from `src/pipeline/transformations` |
# MAGIC | run | Starts the first pipeline update and waits (first run loads history from `history_start_date`) |
# MAGIC | dashboard | Imports `src/dashboards/*.lvdash.json` pointed at your catalog/schema and publishes it |
# MAGIC | job | (optional, `create_job = true`) daily job: setup -> pipeline refresh |
# MAGIC
# MAGIC Re-running is safe: existing pipeline / dashboard / job with the same name are updated, not duplicated.
# MAGIC Set `dry_run = true` to validate everything without creating or changing anything.
# MAGIC
# MAGIC **Prerequisites:** the catalog exists (the schema is created if missing - needs `CREATE SCHEMA` on the catalog); you can read `system.billing.*` and `system.access.workspaces_latest`; serverless jobs/pipelines are enabled; you can use a SQL warehouse. The preflight report checks all of this first.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "", "1. Catalog (blank = workspace default UC catalog)")
dbutils.widgets.text("schema", "cost_management", "2. Schema (created if missing)")
dbutils.widgets.text("warehouse_id", "", "3. SQL warehouse ID (blank = auto-pick)")
dbutils.widgets.dropdown("load_sample_data", "false", ["false", "true"], "4. Load sample cloud data")
dbutils.widgets.text("history_start_date", "2025-01-01", "5. History start date")
dbutils.widgets.text("workspace_filter", "ALL", "6. Workspaces: ALL (every workspace in the account) or comma-separated IDs")
dbutils.widgets.dropdown("pricing_mode", "account_with_list_fallback", ["account_with_list_fallback", "list"], "7. Pricing mode")
dbutils.widgets.text("name_prefix", "", "8. Name prefix (optional)")
dbutils.widgets.dropdown("create_job", "false", ["false", "true"], "9. Create daily job")
dbutils.widgets.text("steps", "setup,pipeline,run,dashboard,job", "Steps to run")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run (validate only)")

P = {k: dbutils.widgets.get(k).strip() for k in [
    "catalog", "schema", "warehouse_id", "load_sample_data", "history_start_date",
    "workspace_filter", "pricing_mode", "name_prefix", "create_job", "steps", "dry_run"]}

# Advanced settings (same defaults as databricks.yml)
ADV = {
    "landing_volume": "cloud_cost_landing",
    "lookback_days": "7",
    "list_price_discount_pct": "0",
    "additional_discount_pct": "0",
    "control_total_tolerance_pct": "0.5",
    "currency": "USD",
    "force_full_reload": "false",
    "schedule_cron": "0 0 6 * * ?",
    "timezone": "UTC",
}

# No input is mandatory: blank catalog = the workspace's default Unity Catalog catalog,
# schema defaults to cost_management (created by setup if missing).
_LEGACY = ("hive_metastore", "spark_catalog")
if not P["catalog"]:
    from databricks.sdk import WorkspaceClient as _WC
    _w0, _cands = _WC(), []
    try:
        _cands.append(_w0.metastores.current().default_catalog_name)   # workspace default catalog
    except Exception:
        pass
    try:
        _cands.append(spark.sql("SELECT current_catalog()").first()[0])  # session default
    except Exception:
        pass
    P["catalog"] = next((c for c in _cands if c and c.lower() not in _LEGACY), "")
    if not P["catalog"]:
        _skip = set(_LEGACY) | {"system", "samples", "__databricks_internal"}
        _uc = sorted(c.name for c in _w0.catalogs.list() if c.name not in _skip)[:25]
        raise ValueError("This workspace's default catalog is the legacy hive_metastore, so a Unity Catalog "
                         f"catalog cannot be picked automatically. Set the 'catalog' widget to one of: {_uc}")
    print(f"catalog not set - using the workspace default catalog: {P['catalog']}")
if P["catalog"].lower() in _LEGACY:
    raise ValueError("hive_metastore is not supported - set the 'catalog' widget to a Unity Catalog catalog.")
if not P["schema"]:
    P["schema"] = "cost_management"

STEPS = {s.strip() for s in P["steps"].split(",") if s.strip()}
DRY_RUN = P["dry_run"] == "true"
PREFIX = P["name_prefix"]
PIPELINE_NAME = f"{PREFIX}cost-management-accelerator"
DASHBOARD_NAME = f"{PREFIX}Cost Management Accelerator"
JOB_NAME = f"{PREFIX}cost-management-accelerator-refresh"
print(f"steps={sorted(STEPS)} dry_run={DRY_RUN}")

# COMMAND ----------

# DBTITLE 1,Resolve repo paths and clients
import os, json, glob, time, inspect
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
ME = w.current_user.me().user_name

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = os.path.dirname(_nb if _nb.startswith("/Workspace") else "/Workspace" + _nb)

SETUP_NB = f"{REPO_ROOT}/src/setup/run_setup"
PIPELINE_ROOT = f"{REPO_ROOT}/src/pipeline"
TRANSFORMS = f"{PIPELINE_ROOT}/transformations"
DASH_FILES = sorted(glob.glob(f"{REPO_ROOT}/src/dashboards/*.lvdash.json"))
DASH_PARENT = f"/Workspace/Users/{ME}"

for label, p in [("setup notebook", SETUP_NB), ("transformations", TRANSFORMS)]:
    ok = os.path.exists(p) or os.path.exists(p + ".ipynb") or os.path.exists(p + ".py")
    print(f"{'OK ' if ok else 'MISSING'} {label}: {p}")
    if not ok:
        raise FileNotFoundError(p)
if len(DASH_FILES) != 1:
    raise FileNotFoundError(f"Expected exactly one .lvdash.json in src/dashboards, found {DASH_FILES}")
print("OK  dashboard file:", DASH_FILES[0])
print("Running as:", ME, "| repo root:", REPO_ROOT)
RESULT = {}

# COMMAND ----------

# DBTITLE 1,Preflight report (checks everything, then stops if anything is missing)
# Runs EVERY check (does not stop at the first problem) and prints one report:
#   PASS  = ready   |  WARN = works, with a limitation   |  FAIL = must be fixed before install
# Nothing is created here (the pipeline check uses the API's dry-run validation).
from databricks.sdk.service import pipelines as pl

CHECKS = []
def check(area, item, status, detail="", fix=""):
    CHECKS.append((area, item, status, str(detail)[:110], fix))

def _err(e):
    return str(e).splitlines()[0][:110]

CAT, SCH = P["catalog"], P["schema"]
FULL = f"{CAT}.{SCH}"

# 1. Unity Catalog
try:
    check("Unity Catalog", "metastore attached", "PASS", spark.sql("SELECT current_metastore()").first()[0])
except Exception as e:
    check("Unity Catalog", "metastore attached", "FAIL", _err(e), "Attach the workspace to a Unity Catalog metastore.")

def _effective(sec_type, name):
    try:
        r = w.grants.get_effective(securable_type=sec_type, full_name=name, principal=ME)
    except TypeError:
        from databricks.sdk.service.catalog import SecurableType
        r = w.grants.get_effective(securable_type=SecurableType(sec_type.upper()), full_name=name, principal=ME)
    out = set()
    for pa in r.privilege_assignments or []:
        for p in pa.privileges or []:
            out.add(getattr(p.privilege, "value", str(p.privilege)))
    return out

# 2. Target catalog (must exist) / schema (created by the setup step if missing)
cat_obj = sch_obj = None
try:
    cat_obj = w.catalogs.get(CAT)
    check("Target", f"catalog {CAT}", "PASS", f"owner {cat_obj.owner}")
except Exception as e:
    check("Target", f"catalog {CAT}", "FAIL", _err(e),
          "Pick a catalog this workspace can see (catalogs can be bound to specific workspaces).")
SCHEMA_MISSING = False
if cat_obj:
    try:
        sch_obj = w.schemas.get(FULL)
        check("Target", f"schema {FULL}", "PASS", f"owner {sch_obj.owner}")
    except Exception as e:
        SCHEMA_MISSING = True
        try:
            cat_privs = _effective("catalog", CAT)
        except Exception:
            cat_privs = set()
        can_create = cat_obj.owner == ME or "ALL_PRIVILEGES" in cat_privs or {"USE_CATALOG", "CREATE_SCHEMA"} <= cat_privs
        if "setup" not in STEPS:
            check("Target", f"schema {FULL}", "FAIL", "does not exist", "Include the 'setup' step - it creates the schema.")
        elif can_create:
            check("Target", f"schema {FULL}", "WARN", "does not exist - the setup step will create it")
        else:
            check("Target", f"schema {FULL}", "FAIL", "does not exist and you cannot create it",
                  f"GRANT USE CATALOG, CREATE SCHEMA ON CATALOG {CAT} TO `{ME}` - or create the schema yourself.")

# 3. Privileges on the target (owners and ALL PRIVILEGES count; a schema created by setup is owned by you)

if sch_obj:
    is_owner = ME in (sch_obj.owner, cat_obj.owner)
    try:
        privs = _effective("schema", FULL) | _effective("catalog", CAT)
        need = ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "CREATE_MATERIALIZED_VIEW", "CREATE_VOLUME"]
        miss = [] if (is_owner or "ALL_PRIVILEGES" in privs) else [n for n in need if n not in privs]
        if miss:
            check("Privileges", f"on {FULL}", "FAIL", f"missing {miss}",
                  f"GRANT {', '.join(m.replace('_', ' ') for m in miss)} ON SCHEMA {FULL} TO `{ME}` "
                  "(ignore if you get them through ownership by one of your groups).")
        else:
            check("Privileges", f"on {FULL}", "PASS", "owner" if is_owner else "all required privileges")
    except Exception as e:
        check("Privileges", f"on {FULL}", "WARN", f"could not verify: {_err(e)}", "Install will fail later if privileges are missing.")

# 4. System tables (account-wide billing data -> all workspaces are visible)
for t, required in [("system.billing.usage", True), ("system.billing.list_prices", True),
                    ("system.access.workspaces_latest", True), ("system.billing.account_prices", False)]:
    try:
        spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
        check("System tables", t, "PASS")
    except Exception as e:
        check("System tables", t, "FAIL" if required else "WARN", _err(e),
              "Account admin: enable the system schema and GRANT USE SCHEMA + SELECT to you." if required
              else "Optional - list prices will be used instead of negotiated prices.")
try:
    n_ws = spark.sql("SELECT count(DISTINCT workspace_id) FROM system.billing.usage WHERE usage_date >= current_date() - 30").first()[0]
    scope = "ALL" if P["workspace_filter"].upper() == "ALL" else f"{len(P['workspace_filter'].split(','))} selected"
    check("System tables", "workspaces with billing (30d)", "PASS", f"{n_ws} in account; reporting scope = {scope}")
except Exception:
    pass

# 5. SQL warehouse (auto-picked when blank: serverless + running preferred)
try:
    whs = list(w.warehouses.list())
    if P["warehouse_id"]:
        wh = next((x for x in whs if x.id == P["warehouse_id"]), None)
        if wh:
            check("SQL warehouse", wh.name, "PASS", f"id {wh.id}, {wh.state.value if wh.state else ''}")
        else:
            check("SQL warehouse", P["warehouse_id"], "FAIL", "not found or no access", "Leave blank to auto-pick, or ask for CAN USE.")
    elif whs:
        rank = lambda x: (not bool(x.enable_serverless_compute), (x.state.value if x.state else "") != "RUNNING", x.name or "")
        wh = sorted(whs, key=rank)[0]
        P["warehouse_id"] = wh.id
        check("SQL warehouse", wh.name, "PASS", f"auto-selected id {wh.id} "
              f"({'serverless' if wh.enable_serverless_compute else 'classic'}, {wh.state.value if wh.state else ''}) out of {len(whs)}")
    else:
        check("SQL warehouse", "any", "FAIL", "no warehouse visible to you", "Create a SQL warehouse or ask for CAN USE on one.")
except Exception as e:
    check("SQL warehouse", "list", "FAIL", _err(e), "Ask for access to a SQL warehouse.")

# 6. Serverless pipelines (validated by the API without creating anything)
if cat_obj:
    try:
        _p = inspect.signature(w.pipelines.create).parameters
        _spec = dict(name=f"{PIPELINE_NAME}-preflight", catalog=CAT, serverless=True, dry_run=True,
                     libraries=[pl.PipelineLibrary(file=pl.FileLibrary(path=f)) for f in sorted(glob.glob(f"{TRANSFORMS}/*.py"))])
        _spec["schema" if "schema" in _p else "target"] = SCH
        w.pipelines.create(**_spec)
        check("Compute", "serverless pipeline", "PASS", "definition accepted by the API (dry run)")
    except Exception as e:
        check("Compute", "serverless pipeline", "FAIL", _err(e), "Enable serverless compute for pipelines in the workspace.")

# 7. Existing accelerator tables owned by a different pipeline
if sch_obj and spark.catalog.tableExists(f"{FULL}.bronze_billing_usage"):
    owner_id = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {FULL}.bronze_billing_usage").collect()}.get("pipelines.pipelineId")
    mine = [p.pipeline_id for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'") if p.name == PIPELINE_NAME]
    if owner_id and owner_id not in mine:
        # Only blocks installs that (re)create the pipeline; e.g. steps=dashboard is fine.
        check("Target", "existing pipeline tables", "FAIL" if "pipeline" in STEPS else "WARN", f"owned by pipeline {owner_id}",
              "Use an empty schema, or rename that pipeline to " + PIPELINE_NAME + " to adopt it.")
    else:
        check("Target", "existing pipeline tables", "PASS", "will be updated in place")
elif sch_obj or SCHEMA_MISSING:
    check("Target", "existing pipeline tables", "PASS", "none - fresh install")

# 8. Identity directory (for user / service principal names)
try:
    next(iter(w.users.list(attributes="userName", count=1)), None)
    check("Identity", "workspace SCIM read", "PASS")
except Exception as e:
    check("Identity", "workspace SCIM read", "WARN", _err(e), "Identity names will show as raw IDs.")

# ---- Report ---------------------------------------------------------------
w_area = max(len(c[0]) for c in CHECKS); w_item = max(len(c[1]) for c in CHECKS)
print(f"{'AREA':{w_area}}  {'CHECK':{w_item}}  STATUS  DETAIL")
for a, i, s, d, f in CHECKS:
    print(f"{a:{w_area}}  {i:{w_item}}  {s:6}  {d}")
    if f and s != "PASS":
        print(f"{'':{w_area}}  {'':{w_item}}  -> fix: {f}")
fails = [c for c in CHECKS if c[2] == "FAIL"]
RESULT["preflight"] = {s: sum(1 for c in CHECKS if c[2] == s) for s in ("PASS", "WARN", "FAIL")}
RESULT["warehouse_id"] = P["warehouse_id"]
print("\nPreflight:", RESULT["preflight"])
if fails:
    raise RuntimeError(f"{len(fails)} preflight check(s) failed - see the report above: "
                       + "; ".join(f"{c[0]}/{c[1]}" for c in fails))

# COMMAND ----------

# DBTITLE 1,Step 1 - setup (preflight, config tables, volume, sample data)
if "setup" not in STEPS:
    print("setup skipped")
elif DRY_RUN:
    print("dry run: setup notebook not executed (the preflight report above covers its checks)")
else:
    dbutils.notebook.run(SETUP_NB, 3600, {
        "catalog": P["catalog"], "schema": P["schema"],
        "landing_volume": ADV["landing_volume"], "load_sample_data": P["load_sample_data"],
    })
    print("setup completed")

# COMMAND ----------

# DBTITLE 1,Step 2 - create or update the pipeline
from databricks.sdk.service import pipelines as pl

configuration = {
    "catalog": P["catalog"], "schema": P["schema"], "landing_volume": ADV["landing_volume"],
    "pricing_mode": P["pricing_mode"], "list_price_discount_pct": ADV["list_price_discount_pct"],
    "additional_discount_pct": ADV["additional_discount_pct"], "history_start_date": P["history_start_date"],
    "lookback_days": ADV["lookback_days"], "workspace_filter": P["workspace_filter"],
    "control_total_tolerance_pct": ADV["control_total_tolerance_pct"], "currency": ADV["currency"],
    "force_full_reload": ADV["force_full_reload"],
}

# Prefer a folder glob (new files picked up automatically); fall back to per-file libraries on older SDKs.
if hasattr(pl, "PathPattern") and "glob" in inspect.signature(pl.PipelineLibrary).parameters:
    libraries = [pl.PipelineLibrary(glob=pl.PathPattern(include=f"{TRANSFORMS}/**"))]
else:
    libraries = [pl.PipelineLibrary(file=pl.FileLibrary(path=f))
                 for f in sorted(glob.glob(f"{TRANSFORMS}/*.py"))]

_create_params = inspect.signature(w.pipelines.create).parameters
spec = dict(name=PIPELINE_NAME, catalog=P["catalog"], serverless=True, photon=True,
            channel="CURRENT", continuous=False, development=False,
            libraries=libraries, configuration=configuration)
spec["schema" if "schema" in _create_params else "target"] = P["schema"]
if "root_path" in _create_params:
    spec["root_path"] = PIPELINE_ROOT

existing = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'") if p.name == PIPELINE_NAME]
PIPELINE_ID = existing[0].pipeline_id if existing else None

# Safety: the bronze/silver/gold tables can only be owned by ONE pipeline.
owner = None
bronze = f"{P['catalog']}.{P['schema']}.bronze_billing_usage"
if spark.catalog.tableExists(bronze):
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {bronze}").collect()}
    owner = props.get("pipelines.pipelineId")
conflict = owner is not None and owner != PIPELINE_ID

if "pipeline" not in STEPS:
    print("pipeline step skipped | existing:", PIPELINE_ID)
elif conflict:
    msg = (f"Tables in {P['catalog']}.{P['schema']} are owned by another pipeline ({owner}). "
           f"Use a different schema, or rename that pipeline to '{PIPELINE_NAME}' to adopt it.")
    if DRY_RUN:
        print("WARNING (would stop here):", msg)
        RESULT["pipeline_conflict"] = msg
    else:
        raise RuntimeError(msg)
if "pipeline" in STEPS and (DRY_RUN or not conflict):
    if DRY_RUN:
        r = w.pipelines.create(**spec, dry_run=True)
        print("dry run: pipeline spec validated by the API ->", r.effective_settings.name if r.effective_settings else r)
        RESULT["pipeline_dry_run"] = f"validated ({len(libraries)} library entries, {'glob' if libraries[0].glob else 'files'})"
    elif PIPELINE_ID:
        w.pipelines.update(pipeline_id=PIPELINE_ID, **spec)
        print("updated pipeline", PIPELINE_ID)
    else:
        PIPELINE_ID = w.pipelines.create(**spec).pipeline_id
        print("created pipeline", PIPELINE_ID)
RESULT["pipeline_id"] = PIPELINE_ID

# COMMAND ----------

# DBTITLE 1,Step 3 - run the pipeline (first run loads history)
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}

if "run" not in STEPS or DRY_RUN or not PIPELINE_ID:
    print("run skipped", "(dry run)" if DRY_RUN else "")
else:
    update_id = w.pipelines.start_update(pipeline_id=PIPELINE_ID).update_id
    print(f"started update {update_id} - first run can take 10-30 min depending on history size")
    deadline = time.time() + 2 * 3600
    state = None
    while time.time() < deadline:
        state = w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update_id).update.state.value
        if state in TERMINAL:
            break
        time.sleep(30)
    print("pipeline update state:", state)
    RESULT["pipeline_update"] = state
    if state != "COMPLETED":
        raise RuntimeError(f"Pipeline update {update_id} ended in {state} - open the pipeline to see the error.")
    for t in ["gold_dbx_cost_daily", "gold_total_cost_monthly"]:
        print(t, spark.table(f"{P['catalog']}.{P['schema']}.{t}").count(), "rows")

# COMMAND ----------

# DBTITLE 1,Step 4 - import and publish the dashboard
from databricks.sdk.service.workspace import ObjectType
# Uses the Lakeview REST API directly so it works with every databricks-sdk version.

src = json.load(open(DASH_FILES[0]))
# Point every dataset at the target catalog/schema (queries use unqualified table names).
for ds in src["datasets"]:
    ds["catalog"], ds["schema"] = P["catalog"], P["schema"]
serialized = json.dumps(src)
print(f"dashboard JSON: {len(src['datasets'])} datasets, {len(src['pages'])} pages -> {P['catalog']}.{P['schema']}")

def _find_dashboard():
    for o in w.workspace.list(DASH_PARENT):
        if o.object_type == ObjectType.DASHBOARD and os.path.basename(o.path) == f"{DASHBOARD_NAME}.lvdash.json":
            return o.resource_id
    return None

def _upsert(dashboard_id):
    body = {"display_name": DASHBOARD_NAME, "serialized_dashboard": serialized, "warehouse_id": P["warehouse_id"]}
    if dashboard_id:
        return w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dashboard_id}", body=body)["dashboard_id"]
    return w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body={**body, "parent_path": DASH_PARENT})["dashboard_id"]

if "dashboard" not in STEPS:
    print("dashboard skipped")
elif DRY_RUN:
    RESULT["dashboard_dry_run"] = f"would {'update' if _find_dashboard() else 'create'} '{DASHBOARD_NAME}' in {DASH_PARENT}"
    print("dry run:", RESULT["dashboard_dry_run"])
else:
    DASHBOARD_ID = _upsert(_find_dashboard())
    w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{DASHBOARD_ID}/published",
                    body={"embed_credentials": True, "warehouse_id": P["warehouse_id"]})
    host = w.config.host.rstrip("/")
    RESULT["dashboard_id"] = DASHBOARD_ID
    RESULT["dashboard_url"] = f"{host}/dashboardsv3/{DASHBOARD_ID}/published"
    print("published:", RESULT["dashboard_url"])

# COMMAND ----------

# DBTITLE 1,Step 5 - daily job (optional)
from databricks.sdk.service import jobs

if "job" not in STEPS or P["create_job"] != "true":
    print("job skipped (create_job = false)")
else:
    settings = dict(
        name=JOB_NAME,
        max_concurrent_runs=1,
        schedule=jobs.CronSchedule(quartz_cron_expression=ADV["schedule_cron"], timezone_id=ADV["timezone"],
                                   pause_status=jobs.PauseStatus.UNPAUSED),
        email_notifications=jobs.JobEmailNotifications(on_failure=[ME]),
        tasks=[
            jobs.Task(task_key="setup", notebook_task=jobs.NotebookTask(
                notebook_path=SETUP_NB.replace("/Workspace", "", 1),
                base_parameters={"catalog": P["catalog"], "schema": P["schema"],
                                 "landing_volume": ADV["landing_volume"], "load_sample_data": P["load_sample_data"]})),
            jobs.Task(task_key="refresh_pipeline", depends_on=[jobs.TaskDependency(task_key="setup")],
                      pipeline_task=jobs.PipelineTask(pipeline_id=PIPELINE_ID or "<pipeline_id>", full_refresh=False)),
        ],
    )
    existing_jobs = [j for j in w.jobs.list(name=JOB_NAME)]
    if DRY_RUN:
        RESULT["job_dry_run"] = f"would {'reset' if existing_jobs else 'create'} job '{JOB_NAME}' ({len(settings['tasks'])} tasks)"
        print("dry run:", RESULT["job_dry_run"])
        print(json.dumps(jobs.JobSettings(**settings).as_dict(), indent=1)[:1500])
    elif existing_jobs:
        w.jobs.reset(job_id=existing_jobs[0].job_id, new_settings=jobs.JobSettings(**settings))
        RESULT["job_id"] = existing_jobs[0].job_id
        print("updated job", RESULT["job_id"])
    else:
        RESULT["job_id"] = w.jobs.create(**settings).job_id
        print("created job", RESULT["job_id"])

# COMMAND ----------

# DBTITLE 1,Summary
RESULT["target"] = f"{P['catalog']}.{P['schema']}"
RESULT["dry_run"] = DRY_RUN
print("=" * 60)
print("Cost Management Accelerator install", "(DRY RUN)" if DRY_RUN else "")
print("target   :", f"{P['catalog']}.{P['schema']}")
for k, v in RESULT.items():
    print(f"{k:16}: {v}")
print("next     : edit config_workspace_env (fix UNASSIGNED), config_tag_mapping, config_budget as needed.")
print("=" * 60)
# Returns the result when this notebook is called from another notebook / job (no effect in Run all).
dbutils.notebook.exit(json.dumps(RESULT, default=str))

# COMMAND ----------


