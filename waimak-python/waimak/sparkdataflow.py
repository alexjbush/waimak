from waimak.dataflowaction import DataFlowAction


class SparkDataFlow:
    def __init__(self, spark_session, _jsdf=None):
        self._jvm = spark_session._jvm
        self._gateway = spark_session.sparkContext._gateway
        self.spark_session = spark_session
        if _jsdf is None:
            self._jsdf = self._jvm.com.coxautodata.waimak.dataflow.Waimak.sparkFlow(self.spark_session._jsparkSession)
        else:
            self._jsdf = _jsdf

    def __get_extension_object(self):
        return getattr(getattr(self._jvm.com.coxautodata.waimak.dataflow.spark, 'package$'),
                       'MODULE$').SparkDataFlowExtension(
            self._jsdf)

    def add_input(self, label, df):
        self._jsdf = self._jsdf.addInput(label, self._jvm.scala.Some(df._jdf))

    def alias(self, input_label, output_label):
        self._jsdf = self.__get_extension_object().alias(input_label, output_label)

    def show(self, label):
        self._jsdf = self.__get_extension_object().show(label)

    def execute(self, error_on_unexecuted_actions=True):
        res = self._jsdf.execute(error_on_unexecuted_actions)
        self._jsdf = res._2()
        if res._1().length() == 0:
            jactions = []
        else:
            jactions = self._jvm.scala.collection.JavaConversions.seqAsJavaList(res._1())
        return [DataFlowAction(a) for a in jactions]
