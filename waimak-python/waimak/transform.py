from pyspark.sql import DataFrame


class Transform1:
    def __init__(self, gateway, sql_context, lambda_function):
        self.gateway = gateway
        self.sql_context = sql_context
        self.lambda_function = lambda_function

    def apply(self, _jdf1):
        df1 = DataFrame(_jdf1, self.sql_context)
        return self.lambda_function(df1)._jdf

    class Java:
        implements = ["scala.Function1"]
