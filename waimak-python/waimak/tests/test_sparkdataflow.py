from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from waimak.sparkdataflow import SparkDataFlow


class TestSparkDataFlow(TestCase):
    def setUp(self):
        self.spark_session = SparkSession.builder.master("local").config("spark.jars.packages",
                                                                         "com.coxautodata:waimak-core_2.11:2.7").getOrCreate()

    def tearDown(self):
        self.spark_session.stop()

    def test_execute_empty_flow(self):
        actions = SparkDataFlow(self.spark_session).execute()
        self.assertEqual(len(actions), 0)

    def test_execute_flow_show(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        test_list = [['Bob', 33], ['James', 34]]

        df = self.spark_session.createDataFrame(test_list, schema=schema)
        flow = SparkDataFlow(self.spark_session)
        flow.add_input("in", df)
        flow.show("in")
        actions = flow.execute()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].description(), 'Action: show Inputs: [in] Outputs: []')

    def test_execute_flow_transform(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        test_list = [['Bob', 33], ['James', 34]]

        df = self.spark_session.createDataFrame(test_list, schema=schema)
        flow = SparkDataFlow(self.spark_session)
        flow.add_input("in", df)
        flow.transform("in", "out", lambda df: df.groupBy("name").count())
        flow.show("out")
        actions = flow.execute()
        self.assertEqual(len(actions), 2)
        #self.assertEqual(actions[0].description(), 'Action: show Inputs: [in] Outputs: []')
