from enum import Enum


class Operation(Enum):
    READ = "READ"
    WRITE = "WRITE"
    COMMIT = "COMMIT"

    @classmethod
    def from_str(cls, op_str):
        operations_map = {"R": cls.READ, "W": cls.WRITE, "C": cls.COMMIT}
        return operations_map.get(op_str, None)
    
    def to_string(_, op):
        reverse_map = {"READ": "R", "WRITE": "W", "COMMIT": "C"}
        return reverse_map.get(op, None)


class Command:
    def __init__(self, operation, target_object, transaction_id):
        self.operation = operation
        self.target_object = target_object
        self.transaction_id = transaction_id

    # Corrigindo a função __str__ para uma saída legível
    def __str__(self):
        if self.operation == Operation.COMMIT:
            operation_str = self.operation.to_string(self.operation.value)
            return f"{operation_str}{self.transaction_id}"
        operation_str = self.operation.to_string(self.operation.value)
        return f"{operation_str}{self.transaction_id}({self.target_object})"

    # Corrigindo a função __repr__ para garantir uma saída legível durante a depuração
    def __repr__(self):
        if self.operation == Operation.COMMIT:
            return f"Commit of transaction {self.transaction_id}"
        return f"{self.operation.value} on {self.target_object} by transaction {self.transaction_id}"


class Lock:
    def __init__(self, command, is_intentional=False):
        self.operation = command.operation
        self.target_object = command.target_object
        self.transaction_id = command.transaction_id
        self.is_intentional = is_intentional

    def create_intentional_copy(self):
        new_command = Command(self.operation, self.target_object, self.transaction_id)
        return Lock(new_command, is_intentional=True)
