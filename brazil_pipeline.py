import pandas as pd
import json
import os
import requests

from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector

from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        print("DOWNLOAD STEP")
        filename = "{}_{}_MUN.csv".format(params.get("flow"),params.get("year"))
        if filename not in os.listdir("./data_temp"):
            r = requests.get("http://www.mdic.gov.br/balanca/bd/comexstat-bd/mun/{}_{}_MUN.csv".format(params.get("flow"),params.get("year")))
            with open("./data_temp/"+filename,"w") as file:
                file.write(r.text)
        df = pd.read_csv("./data_temp/"+filename, sep=";")

        return df


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        print("TRANSFORM STEP")
        df = prev

        df["time_id"] = (df["CO_ANO"].astype("str") + df["CO_MES"].astype("str").str.zfill(2)).astype("int")

        p = pd.read_csv("./resources/shared_hs92.csv")
        p_dict = {k:v for (k,v) in zip(p["hs4"],p["hs4_id"])}

        df["hs4_id"] = df["SH4"].map(p_dict).fillna(33391).astype("int")

        g1 = pd.read_csv("./resources/PAIS.csv", sep=";", encoding="latin-1")
        g1_dict = {k:v for (k,v) in zip(g1["CO_PAIS"],g1["CO_PAIS_ISOA3"])}

        df["country_id"] = df["CO_PAIS"].map(g1_dict).str.lower()

        g2 = pd.read_csv("./resources/shared_countries.csv")
        g2_dict = {k:v for (k,v) in zip(g2["iso3"],g2["id_num"])}

        df["country_id"] = df["country_id"].map(g2_dict)

        s = pd.read_csv("./resources/UF.csv", sep=";", encoding="latin-1")
        s_dict = {k:v for (k,v) in zip(s["SG_UF"],s["CO_UF"])}

        df["state_id"] = df["SG_UF_MUN"].map(s_dict)

        df["municipality_id"] = df["CO_MUN"]
        df["liquid_kg"] = df["KG_LIQUIDO"]
        df["fob"] = df["VL_FOB"]

        f_dict = {"EXP": 1, "IMP": 2}
        df["flow_id"] = f_dict[params["flow"]]

        df = df[["time_id","flow_id","hs4_id","country_id","state_id","municipality_id","liquid_kg","fob"]]
        
        print(df.dtypes,"\n")
        print(df.isnull().any())
        #df.time_id = df.time_id.astype("int")
        return df


class BrazilPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'brazil-etl-pipeline'

    @staticmethod
    def name():
        return 'Brazil ETL Pipeline'

    @staticmethod
    def description():
        return 'Processes Brazil data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source", dtype=str, source=Connector),
            Parameter(label="Flow", name="flow", dtype=str),
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Database connector", name="db", dtype=str, source=Connector)
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = grab_connector(__file__, params.get("source"))
        db_connector = grab_connector(__file__, params.get("db"))

        step1 = DownloadStep(connector=source_connector)
        step2 = TransformStep()
        step3 = LoadStep("trade_i_mdic_m_hs", db_connector, if_exists="append", pk=["time_id"])

        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2).next(step3)

        return pipeline.run_pipeline()


def run_brazil(params, **kwargs):
    pipeline = BrazilPipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_brazil({
        "source": "http-local",
        "flow": "EXP",
        "year": "2019",
        "db": "clickhouse-local"
        })
