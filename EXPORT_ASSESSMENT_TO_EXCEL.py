# Databricks notebook source
# MAGIC %md
# MAGIC ##### Exporter of UCX assessment results
# MAGIC ##### Instructions:
# MAGIC 1. Execute using an all-purpose cluster with Databricks Runtime 14 or higher.
# MAGIC 1. Hit **Run all** button and wait for completion.
# MAGIC 1. Go to the bottom of the notebook and click the Download UCX Results button.
# MAGIC
# MAGIC ##### Important:
# MAGIC Please note that this is only meant to serve as example code.
# MAGIC
# MAGIC Example code developed by **Databricks Shared Technical Services team**.

# COMMAND ----------

# DBTITLE 1,Installing Packages
# MAGIC %pip install /Workspace/Applications/ucx/wheels/databricks_labs_ucx-0.57.0-py3-none-any.whl -qqq
# MAGIC %pip install xlsxwriter -qqq
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Libraries Import and Setting UCX
import os
import logging
import threading
import shutil
from pathlib import Path
from threading import Lock
from functools import partial

import pandas as pd
import xlsxwriter

from databricks.sdk.config import with_user_agent_extra
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.dashboards import Dashboards
from databricks.labs.lsql.lakeview.model import Dataset
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext

# ctx
install_logger()
with_user_agent_extra("cmd", "export-assessment")
named_parameters = dict(config="/Workspace/Applications/ucx/config.yml")
ctx = RuntimeContext(named_parameters)
lock = Lock()

# COMMAND ----------

# DBTITLE 1,Assessment Export
FILE_NAME = "ucx_assessment_main.xlsx"
UCX_PATH = Path(f"/Workspace{ctx.installation.install_folder()}")
DOWNLOAD_PATH = Path("/dbfs/FileStore/excel-export/")


def _cleanup() -> None:
    '''Move the temporary results file to the download path and clean up the temp directory.'''
    shutil.move(
        UCX_PATH / "tmp" / FILE_NAME,
        DOWNLOAD_PATH / FILE_NAME,
    )
    shutil.rmtree(UCX_PATH / "tmp/")


def _prepare_directories() -> None:
    '''Ensure that the necessary directories exist.'''
    os.makedirs(UCX_PATH / "tmp/", exist_ok=True)
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

def _process_id_columns(df):
    id_columns = [col for col in df.columns if 'id' in col.lower()]

    if id_columns:
        for col in id_columns:
            df[col] = "'" + df[col].astype(str)
    return df

def _to_excel(dataset: Dataset, writer: ...) -> None:
    '''Execute a SQL query and write the result to an Excel sheet.'''
    worksheet_name = dataset.display_name[:31]
    df = spark.sql(dataset.query).toPandas()
    df = _process_id_columns(df)
    with lock:
        df.to_excel(writer, sheet_name=worksheet_name, index=False)


def _render_export() -> None:
    '''Render an HTML link for downloading the results.'''
    html_content = '''
    <style>@font-face{font-family:'DM Sans';src:url(https://cdn.bfldr.com/9AYANS2F/at/p9qfs3vgsvnp5c7txz583vgs/dm-sans-regular.ttf?auto=webp&format=ttf) format('truetype');font-weight:400;font-style:normal}body{font-family:'DM Sans',Arial,sans-serif}.export-container{text-align:center;margin-top:20px}.export-container h2{color:#1B3139;font-size:24px;margin-bottom:20px}.export-container a{display:inline-block;padding:12px 25px;background-color:#1B3139;color:#fff;text-decoration:none;border-radius:4px;font-size:18px;font-weight:500;transition:background-color 0.3s ease,transform:translateY(-2px) ease}.export-container a:hover{background-color:#FF3621;transform:translateY(-2px)}</style>
    <div class="export-container"><h2>Export Results</h2><a href='https://adb-522548490841242.2.azuredatabricks.net/files/excel-export/ucx_assessment_main.xlsx?o=522548490841242' target='_blank' download>Download Results</a></div>

    '''
    displayHTML(html_content)


def export_results() -> None:
    '''Main method to export results to an Excel file.'''
    _prepare_directories()

    assessment_dashboard = next(UCX_PATH.glob("dashboards/*Assessment (Main)*"))
    dashboard_datasets = Dashboards(ctx.workspace_client).get_dashboard(assessment_dashboard).datasets

    try:
        target = UCX_PATH / "tmp/ucx_assessment_main.xlsx"
        with pd.ExcelWriter(target, engine="xlsxwriter") as writer:
            tasks = []
            for dataset in dashboard_datasets:
                tasks.append(partial(_to_excel, dataset, writer))
                Threads.strict("exporting", tasks)
        _cleanup()
        _render_export()
    except Exception as e:
        print(f"Error exporting results ", e)

# COMMAND ----------

# DBTITLE 1,Data Export
export_results()
