from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

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

    def test_execute_flow_alias(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        test_list = [['Bob', 33], ['James', 34]]
        df = self.spark_session.createDataFrame(test_list, schema=schema)
        flow = SparkDataFlow(self.spark_session)
        flow.add_input("in", df)
        flow.alias("in", "out")
        actions = flow.execute()
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].description(), 'Action: alias Inputs: [in] Outputs: [out]')
        out = flow.get_inputs().get_dataframe("out")
        TestRow = Row('name', 'age')
        self.assertItemsEqual(out.collect(), [TestRow('Bob', 33), TestRow('James', 34)])

    def test_execute_flow_debug_as_table(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        test_list = [['Bob', 33], ['James', 34]]
        df = self.spark_session.createDataFrame(test_list, schema=schema)
        flow = SparkDataFlow(self.spark_session)
        flow.add_input("in", df)
        flow.alias('in', 'table_in')
        flow.debug_as_table("table_in")
        actions = flow.execute()
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[1].description(), 'Action: debugAsTable Inputs: [table_in] Outputs: []')
        in_table = self.spark_session.sql("select * from table_in")
        TestRow = Row('name', 'age')
        self.assertItemsEqual(in_table.collect(), [TestRow('Bob', 33), TestRow('James', 34)])

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

    def test_execute_flow_transform1(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        test_list = [['Bob', 33], ['James', 34]]
        df = self.spark_session.createDataFrame(test_list, schema=schema)
        flow = SparkDataFlow(self.spark_session)
        flow.add_input("in", df)
        flow.transform("in", "out", lambda in_df: in_df.groupBy("name").count())
        flow.show("out")
        actions = flow.execute()
        self.assertEqual(len(actions), 2)
        out = flow.get_inputs().get_dataframe("out")
        TestRow = Row('name', 'count')
        self.assertItemsEqual(out.collect(), [TestRow('Bob', 1), TestRow('James', 1)])
