from pyspark.sql import DataFrame


class DataFlowEntities:

    def __init__(self, _jdfe, _jdf_classtag, sql_context):
        self._jdfe = _jdfe
        self._jdf_classtag = _jdf_classtag
        self.sql_context = sql_context

    def get_dataframe(self, label):
        return DataFrame(self._jdfe.get(label, self._jdf_classtag), self.sql_context)
