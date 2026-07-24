from messages.command import Command

class LogEntry():

    def __init__(self, term: int, commands: list[Command]) -> None:
        self._term = term
        self._commands = commands

    @staticmethod
    def from_dict(raw: dict) -> 'LogEntry':
        if not isinstance(raw, dict): return raw
        commands = [Command.from_dict(c) for c in raw['commands']]
        return LogEntry(raw['term'], commands)

    def to_dict(self):
        return {
            'term': self._term,
            'commands': [c.to_dict() for c in self._commands]
        }