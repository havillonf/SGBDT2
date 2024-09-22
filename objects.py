from enum import Enum

from operation import Lock


class ObjectType(Enum):
    DATABASE = "DATABASE"
    TABLESPACE = "TABLESPACE"
    TABLE = "TABLE"
    PAGE = "PAGE"
    ROW = "ROW"


class DBObject:
    def __init__(self, parent=None, name="", obj_type=None):
        self.parent = parent
        self.name = name
        self.obj_type = obj_type
        self.children = []
        self.locks = []
        if parent:
            parent.children.append(self)

    def add_lock(self, lock, propagate_to_parent=True):
        if self.parent and propagate_to_parent:
            parent_lock = lock.create_intentional_copy()
            self.parent.add_lock(parent_lock)
        for child in self.children:
            child.add_lock(lock, propagate_to_parent=False)
        self.locks.append(lock)

    def check_lock(self, allowed_operations, current_transaction):
        for lock in self.locks:
            if (
                lock.operation in allowed_operations
                and lock.transaction_id != current_transaction
            ):
                return lock
        return None

    def has_lock_in_transactions(self, allowed_operations, transaction_ids):
        return any(
            lock.operation in allowed_operations
            and lock.transaction_id in transaction_ids
            for lock in self.locks
        )

    def get_all_locks(self):
        all_locks = self.locks.copy()
        for child in self.children:
            all_locks.extend(child.get_all_locks())
        return all_locks

    def check_locks_for_transactions(self, allowed_operations, transaction_ids):
        all_locks = self.get_all_locks()
        return next(
            (
                lock
                for lock in all_locks
                if lock.operation in allowed_operations
                and lock.transaction_id in transaction_ids
            ),
            None,
        )

    # Método para remover todos os bloqueios relacionados a uma transação específica
    def remove_locks_for_transaction(self, transaction_id):
        # Remove os bloqueios do objeto atual
        self.locks = [
            lock for lock in self.locks if lock.transaction_id != transaction_id
        ]
        # Propaga a remoção para todos os filhos
        for child in self.children:
            child.remove_locks_for_transaction(transaction_id)

    def find_child(self, object_name):
        return next(
            (child for child in self.children if child.name == object_name), None
        )

    def find_recursive(self, object_name):
        """
        Busca recursiva pelo objeto na hierarquia com base em partes do nome.
        """
        parts = object_name.split("_")
        current_object = self

        # Percorre cada parte e tenta localizar o objeto
        for part in parts:
            found_object = current_object.find_child(part)
            if found_object:
                current_object = found_object
            else:
                return None  # Retorna None se o objeto não for encontrado
        return current_object

    def __str__(self):
        return f"{self.obj_type.value} {self.name}"

