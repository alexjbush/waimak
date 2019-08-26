class Function1:
    def __init__(self, lambda_function):
        self.lambda_function = lambda_function

    def apply(self, arg1):
        return self.lambda_function(arg1)

    class Java:
        implements = ["scala.Function1"]