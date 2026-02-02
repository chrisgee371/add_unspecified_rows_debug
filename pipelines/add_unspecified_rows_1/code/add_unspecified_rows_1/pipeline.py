from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from add_unspecified_rows_1.config.ConfigStore import *
from add_unspecified_rows_1.functions import *
from prophecy.utils import *
from add_unspecified_rows_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Copper_Delta_Table = Copper_Delta_Table(spark)
    df_Add_Unspecified_Rows_1 = Add_Unspecified_Rows_1(spark, df_Copper_Delta_Table)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("add_unspecified_rows_1").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/add_unspecified_rows_1")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/add_unspecified_rows_1", config = Config)(pipeline)

if __name__ == "__main__":
    main()
