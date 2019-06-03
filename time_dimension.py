import pandas as pd
import json
import os
pd.options.mode.chained_assignment = None

from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector

from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector


def next_month(date):
    year = int(date[:4])
    month = int(date[5:7])
    if month < 12:
        return str(year) + "-" + str(month + 1).zfill(2)
    elif month == 12:
        return str(year + 1) + "-01"


class CreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("CREATION STEP")
        df = pd.DataFrame({"date": pd.date_range("1997-01", next_month(params["last-month"]), freq="M")})
        df["time_id"] = df.index + 1
        df["year_id"] = df.date.dt.year
        df["quarter_id"] = df.date.dt.year.astype(str) + "-Q" + df.date.dt.quarter.astype(str)
        df["month_id"] = df.date.dt.year.astype(str) + "-" + df.date.dt.month.astype(str).str.zfill(2)
        df["month_name"] = df["year_id"].astype("str") + " " + df.date.dt.month_name()
        df = df.drop(columns="date")

        return df


class SaveStep(PipelineStep):
    def run_step(self, prev, params):
        print("SAVE STEP")
        df = prev
        if os.path.isdir("./dimension_tables"):
            df.to_csv("./dimension_tables/time_table.csv", index=False)
        else:
            os.mkdir("dimension_tables")
            df.to_csv("./dimension_tables/time_table.csv", index=False)
        return df


class TimePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'time-table-pipeline'

    @staticmethod
    def name():
        return 'Time Table Pipeline'

    @staticmethod
    def description():
        return 'Creates and ingests the time table'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Last Month", name="last-month", dtype=str),
            Parameter(label="Database Connector", name="db-connector", dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        db_connector = grab_connector(__file__, params.get("db-connector"))

        step1 = CreationStep()
        step2 = SaveStep()
        step3 = LoadStep("time_table", db_connector, if_exists="replace", pk = ["time_id"])

        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2).next(step3)

        return pipeline.run_pipeline()


def run_time(params, **kwargs):
    pipeline = TimePipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_time({
        "last-month": "2019-04",
        "db-connector": "clickhouse-local"
        })