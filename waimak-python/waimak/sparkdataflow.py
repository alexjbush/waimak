from pyspark.sql import DataFrame
from pyspark.sql.context import SQLContext
from pyspark.java_gateway import ensure_callback_server_started
from pyspark.sql.types import StructType

from waimak.dataflowaction import DataFlowAction
from waimak.dataflowentities import DataFlowEntities
from waimak.scala_function import Function1


class SparkDataFlow:
    def __init__(self, spark_session, _jsdf=None):
        self._jvm = spark_session._jvm
        self._gateway = spark_session.sparkContext._gateway
        ensure_callback_server_started(self._gateway)
        self.spark_session = spark_session
        self.sql_context = SQLContext(spark_session.sparkContext, spark_session)
        if _jsdf is None:
            self._jsdf = self._jvm.com.coxautodata.waimak.dataflow.Waimak.sparkFlow(self.spark_session._jsparkSession)
        else:
            self._jsdf = _jsdf
        # Hack to get hold of DataFrame ClassTag object
        empty_jdf = self.sql_context.createDataFrame(self.spark_session.sparkContext.emptyRDD(),
                                                     StructType([]))._jdf
        self._jdf_classtag = self.__get_classtag_from_jobject(empty_jdf)

    def __get_classtag_from_jobject(self, jobject):
        return getattr(getattr(self._jvm.scala.reflect, 'ClassTag$'), 'MODULE$').apply(jobject.getClass())

    def __get_extension_object(self):
        return getattr(getattr(self._jvm.com.coxautodata.waimak.dataflow.spark, 'package$'),
                       'MODULE$').SparkDataFlowExtension(
            self._jsdf)

    def get_inputs(self):
        return DataFlowEntities(self._jsdf.inputs(), self._jdf_classtag, self.sql_context)

    def add_input(self, label, df):
        self._jsdf = self._jsdf.addInput(label, self._jvm.scala.Some(df._jdf))

    def execute(self, error_on_unexecuted_actions=True):
        res = self._jsdf.execute(error_on_unexecuted_actions)
        self._jsdf = res._2()
        if res._1().length() == 0:
            jactions = []
        else:
            jactions = self._jvm.scala.collection.JavaConversions.seqAsJavaList(res._1())
        return [DataFlowAction(a) for a in jactions]

    def alias(self, input_label, output_label):
        self._jsdf = self.__get_extension_object().alias(input_label, output_label)

    def debug_as_table(self, *labels):
        jlabels = self._jvm.PythonUtils.toSeq([l for l in labels])
        self._jsdf = self.__get_extension_object().debugAsTable(jlabels)

    def open(self, label, open_func):
        self._jsdf = self._jsdf.open(label, Function1(lambda _sfc: open_func(self.spark_session)._jdf))

    # |  open(String, Function1) : SparkDataFlow
    # |
    # |  open(String, Option, Option, Seq, Function1) : SparkDataFlow
    # |
    # |  open(String, Function1, Map) : SparkDataFlow
    # |
    # |  openCSV(String, Option, Option, Map, Seq) : SparkDataFlow
    # |
    # |  openFileCSV(String, String, Map) : SparkDataFlow
    # |
    # |  openFileParquet(String, String, Map) : SparkDataFlow
    # |
    # |  openParquet(String, Option, Option, Map, Seq) : SparkDataFlow
    # |
    # |  openTable(String, Option, Seq) : SparkDataFlow
    # |
    # |  partitionSort(String, String, Seq, Seq) : SparkDataFlow
    # |
    # |  printSchema(String) : SparkDataFlow
    # |
    def show(self, label):
        self._jsdf = self.__get_extension_object().show(label)

    # |
    # |  sql(String, Seq, String, String, Seq) : SparkDataFlow
    # |
    # |  transform(String, String, String, String, String, String, String, String, String, String, String, Function10) : SparkDataFlow
    # |
    # |  transform(String, String, String, String, String, String, String, String, String, String, Function9) : SparkDataFlow
    # |
    # |  transform(String, String, String, String, String, String, String, String, String, Function8) : SparkDataFlow
    # |
    # |  transform(String, String, String, String, String, String, String, String, Function7) : SparkDataFlowExtension
    # |  transform(String, String, String, String, String, String, String, String, String, String, String, String, Function11) : SparkDataFlow
    # |
    # |  transform(String, String, String, String, String, String, String, String, String, String, String, String, String, Function12) : SparkDataFlow
    # |

    def transform(self, input1, output1, trans_func):
        def lambda_function(_jdf1):
            df = DataFrame(_jdf1, self.sql_context)
            return trans_func(df)._jdf

        self._jsdf = self.__get_extension_object().transform(input1, output1, Function1(lambda_function))

# |
# |  transform(String, String, String, Function2) : SparkDataFlow
# |
# |  transform(String, String, String, String, Function3) : SparkDataFlow
# |
# |  transform(String, String, String, String, String, Function4) : SparkDataFlow
# |
# |  transform(String, String, String, String, String, String, Function5) : SparkDataFlow
# |
# |  transform(String, String, String, String, String, String, String, Function6) : SparkDataFlow
# |
# |  typedTransform(String, String, Function1) : SparkDataFlow
# |
# |  unitTransform(String, Function1, String) : SparkDataFlow
# |
# |  write(String, Function1, Function1) : SparkDataFlow
# |
# |  writeAsNamedFiles(String, String, int, String, String, Map) : SparkDataFlow
# |
# |  writeCSV(String, Map, boolean, Option, Seq) : SparkDataFlow
# |
# |  writeHiveManagedTable(String, boolean, Seq) : SparkDataFlow
# |
# |  writeParquet(String, boolean, Seq) : SparkDataFlow
# |
# |  writePartitionedCSV(String, boolean, Map, String, Seq) : SparkDataFlow
# |
# |  writePartitionedParquet(String, int, String) : SparkDataFlow
# |
# |  writePartitionedParquet(String, boolean, String, Seq) : SparkDataFlow
