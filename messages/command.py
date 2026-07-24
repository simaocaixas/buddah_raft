class Command():

    def __init__(self, operation: str, value: int):
        self._operation = operation
        self._value = value
    
    @staticmethod
    def from_dict(raw):
        if not isinstance(raw, dict): return raw
        return Command(raw['operation'], raw['value'])

    def to_dict(self):
        return {
            'operation': self._operation,
            'value': self._value,
        }