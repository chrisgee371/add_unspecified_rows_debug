from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            var_catalog_name: str=None,
            var_gold_schema: str=None,
            var_task_name: str=None,
            var_bronze_schema: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(var_catalog_name, var_gold_schema, var_task_name, var_bronze_schema)

    def update(
            self,
            var_catalog_name: str="chris_demos",
            var_gold_schema: str="demos",
            var_task_name: str="gold_anomalies",
            var_bronze_schema: str="demos",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.var_catalog_name = var_catalog_name
        self.var_gold_schema = var_gold_schema
        self.var_task_name = var_task_name
        self.var_bronze_schema = var_bronze_schema
        pass
