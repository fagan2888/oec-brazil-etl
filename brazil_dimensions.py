import pandas as pd
import json
import os

from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector

from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector


class TimeCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("TIME DIMENSION")
        df = pd.read_csv("./resources/shared_time.csv")
        df.to_csv("./dimension_tables/time_table.csv", index=False)
        return df


class ProductCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("PRODUCT DIMENSION")
        df = pd.read_csv("./resources/shared_hs92.csv")
        df = df.drop(columns=["hs6","hs6_name","hs6_id"])
        df = df.drop_duplicates(subset="hs4_id").reset_index(drop=True)
        df.to_csv("./dimension_tables/product_table.csv", index=False)
        return df


class CountryCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("COUNTRY DIMENSION")
        df = pd.read_csv("./resources/shared_countries.csv")
        df.to_csv("./dimension_tables/country_table.csv", index=False)
        return df


class StateCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("STATE DIMENSION")
        df = pd.read_csv("./resources/UF.csv", sep=";", encoding="latin-1")
        df["NO_REGIAO"] = df["NO_REGIAO"].str.title()
        df["SG_UF"] = df["SG_UF"].str.lower()
        df = df.rename(columns={"CO_UF":"state_id","SG_UF":"state_code","NO_UF":"state_name","NO_REGIAO":"region_name"})
        return df


class MunicipalityCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("MUNICIPALITY DIMENSION")
        df = pd.read_csv("./resources/UF_MUN.csv", sep=";", encoding="latin-1")
        df = df.rename(columns={"CO_MUN_GEO":"municipality_id","NO_MUN_MIN":"municipality_name","SG_UF":"state_code"})
        df = df.drop(columns="NO_MUN")
        df["state_code"] = df["state_code"].str.lower()
        df.to_csv("./dimension_tables/municipality_table.csv",index=False)
        return df


class FlowCreationStep(PipelineStep):
    def run_step(self, prev, params):
        print("FLOW DIMENSION")
        df = pd.DataFrame({"flow_id":[1,2], "flow_name":["Exports","Imports"]})
        df.to_csv("./dimension_tables/flow_table.csv", index=False)
        return df


class DimensionsPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'dimensions-pipeline'

    @staticmethod
    def name():
        return 'Dimensions ETL Pipeline for Brazil trade data'

    @staticmethod
    def description():
        return 'Create dimension tables for Brazil trade data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Database connector", name="db", dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        db_connector = grab_connector(__file__, params.get("db"))

        step1 = TimeCreationStep()
        step2 = LoadStep("dim_shared_time", db_connector, if_exists="replace", pk=["time_id"])
        step3 = ProductCreationStep()
        step4 = LoadStep("dim_shared_products", db_connector, if_exists="replace", pk=["hs4_id"])
        step5 = CountryCreationStep()
        step6 = LoadStep("dim_shared_countries", db_connector, if_exists="replace", pk=["id"])
        step7 = StateCreationStep()
        step8 = LoadStep("dim_shared_states", db_connector, if_exists="replace", pk=["state_id"])
        step9 = MunicipalityCreationStep()
        step10 = LoadStep("dim_shared_municipalities", db_connector, if_exists="replace", pk=["municipality_id"])
        step11 = FlowCreationStep()
        step12 = LoadStep("dim_shared_flow", db_connector, if_exists="replace", pk=["flow_id"])

        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2).next(step3).next(step4).next(step5).next(step6).next(step7).next(step8).next(step9).next(step10).next(step11).next(step12)

        return pipeline.run_pipeline()


def run_dimensions(params, **kwargs):
    pipeline = DimensionsPipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_dimensions({
        "db": "clickhouse-local"
        })
