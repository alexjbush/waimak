class DataFlowAction:
    def __init__(self, _jdfa):
        self._jdfa = _jdfa

    def log_label(self):
        return self._jdfa.logLabel()

    def description(self):
        return self._jdfa.description()

    def action_name(self):
        return self._jdfa.actionName()
