import transactionParser as tp
from objects import DBObject, ObjectType
from operation import Command, Lock, Operation

# Cria a estrutura do banco de dados como um objeto de tipo DATABASE
db = DBObject(None, "DB", ObjectType.DATABASE)

# Recebe a string do escalonamento (schedule) como entrada do usuário
scheduler_string = input("Insira a string do schedule: \n")
final_schedule = []

# Usa o transactionParser para converter o escalonamento em comandos
scheduler = tp.parse_schedule(db, scheduler_string)

# Inicialização de variáveis globais para gerenciar transações, espera e detecção de deadlocks
waiting = []  # Lista de comandos em espera
transactions = []  # Lista de transações ativas
deadlock_graph = {}  # Grafo de dependências para detecção de deadlocks


# Verifica se o objeto já tem algum bloqueio que impede a operação
def verify_lock(command, operations):
    return command.target_object.check_lock(operations, command.transaction_id)


# Verifica se um objeto específico tem um bloqueio que impede a operação
def verify_lock_for_object(db_object, operations, transaction_id):
    # Garante que db_object seja do tipo DBObject
    if isinstance(db_object, DBObject):
        return db_object.check_lock(operations, transaction_id)
    return None  # Retorna None se o objeto não for um DBObject


# Verifica se o objeto tem bloqueios de transações específicas
def verify_lock_for_transaction_with_object(obj, operations, transaction_ids):
    return obj.has_lock_in_transactions(operations, transaction_ids)


# Verifica se a transação atual tem algum bloqueio pendente
def verify_lock_for_transaction(operations, transaction_ids):
    return (
        db.get_all_locks()
    )  # Garantimos que aqui seja retornado um objeto ou lista de objetos


# Realiza o commit da transação, removendo os bloqueios
def schedule_commit(command):
    print("Escalonando commit " + repr(command))
    final_schedule.append(str(command))
    db.remove_locks_for_transaction(
        command.transaction_id
    )  # Remove os bloqueios da transação
    del deadlock_graph[
        command.transaction_id
    ]  # Remove a transação do grafo de deadlock
    # Remove a transação das listas de espera em caso de deadlock
    for deadlock_list in deadlock_graph.values():
        while command.transaction_id in deadlock_list:
            deadlock_list.remove(command.transaction_id)


# Tenta escalonar um comando para execução
def try_schedule_command(command):
    # Garante que a transação tenha uma entrada no grafo de deadlock
    if command.transaction_id not in deadlock_graph:
        deadlock_graph[command.transaction_id] = []  # Inicializa a transação no grafo

    if command.operation == Operation.WRITE:
        # Verifica se há bloqueios de escrita ou commit
        has_lock = verify_lock(command, [Operation.WRITE, Operation.COMMIT])
        if has_lock:
            deadlock_graph[command.transaction_id].append(
                has_lock.transaction_id
            )  # Adiciona ao grafo de deadlock
            return False  # Não pode escalonar
        else:
            command.target_object.add_lock(
                Lock(command)
            )  # Adiciona o bloqueio de escrita
            print("Escalonando em outra versão " + repr(command))
            final_schedule.append(str(command))
    elif command.operation == Operation.READ:
        # Verifica se há bloqueios de commit
        has_lock = verify_lock(command, [Operation.COMMIT])
        if has_lock:
            deadlock_graph[command.transaction_id].append(
                has_lock.transaction_id
            )  # Adiciona ao grafo de deadlock
            return False  # Não pode escalonar
        else:
            command.target_object.add_lock(
                Lock(command)
            )  # Adiciona o bloqueio de leitura
            # Verifica se a transação já tem bloqueios de escrita
            if verify_lock_for_transaction_with_object(
                command.target_object, [Operation.WRITE], [command.transaction_id]
            ):
                print("Escalonando em outra versão " + repr(command))
                final_schedule.append(str(command))
            else:
                print("Escalonando " + repr(command))
                final_schedule.append(str(command))
    else:
        # Processa comandos de commit, verificando se existem bloqueios pendentes
        current_object = verify_lock_for_transaction(
            [Operation.WRITE], [command.transaction_id]
        )
        while current_object is not None and isinstance(current_object, DBObject):
            lock = verify_lock_for_object(
                current_object,  # Garante que este objeto seja um DBObject
                [Operation.READ, Operation.WRITE, Operation.COMMIT],
                command.transaction_id,
            )
            if lock:
                deadlock_graph[command.transaction_id].append(
                    lock.transaction_id
                )  # Adiciona ao grafo de deadlock
                return False  # Não pode escalonar
            current_object = verify_lock_for_transaction(
                [Operation.WRITE], [command.transaction_id]
            )
        schedule_commit(command)  # Executa o commit
    return True  # Escalonamento bem-sucedido


# Verifica se há comandos de commit pendentes
def has_waiting_commands(waiting, command):
    if command.operation == Operation.COMMIT:
        return any(c.transaction_id == command.transaction_id for c in waiting)
    return False


# Transfere comandos da lista de espera para a fila de execução
def transfer_waiting_to_scheduler():
    global scheduler
    global waiting
    scheduler = waiting + scheduler
    waiting = []


# Função recursiva que verifica se há um caminho de deadlock no grafo
def find_way_to_deadlock(deadlock_graph, from_transaction, to_transaction):
    if to_transaction in deadlock_graph[from_transaction]:
        return True
    for transaction in deadlock_graph[from_transaction]:
        if find_way_to_deadlock(deadlock_graph, transaction, to_transaction):
            return True
    return False


# Verifica se há um deadlock e, se houver, remove a transação que causou o deadlock
def has_deadlock(scheduler, waiting, deadlock_graph, command):
    has_deadlock = False
    for deadlock in deadlock_graph[command.transaction_id]:
        if find_way_to_deadlock(deadlock_graph, deadlock, command.transaction_id):
            has_deadlock = True
            print(f"Deadlock encontrado, abortando transação {command.transaction_id}")
            # Remove todos os comandos pendentes da transação abortada
            waiting[:] = [
                c for c in waiting if c.transaction_id != command.transaction_id
            ]
            scheduler[:] = [
                c for c in scheduler if c.transaction_id != command.transaction_id
            ]
            db.remove_locks_for_transaction(
                command.transaction_id
            )  # Remove os bloqueios
            del deadlock_graph[command.transaction_id]  # Remove a transação do grafo
            # Limpa a transação das dependências de outras transações
            for deadlock_list in deadlock_graph.values():
                while command.transaction_id in deadlock_list:
                    deadlock_list.remove(command.transaction_id)
            transfer_waiting_to_scheduler()  # Move comandos de volta para o scheduler
    return has_deadlock


# Adiciona um comando à lista de espera
def add_command_to_waiting_list(command):
    global waiting
    print(f"Comando {repr(command)} adicionado à lista de espera")
    waiting.append(command)


# Loop principal de execução do scheduler
while scheduler:
    command = scheduler.pop(0)  # Remove o primeiro comando do scheduler
    if command.transaction_id not in transactions:
        transactions.append(
            command.transaction_id
        )  # Adiciona a transação à lista de ativas
        deadlock_graph[
            command.transaction_id
        ] = []  # Cria uma entrada no grafo de deadlocks

    # Verifica se há comandos pendentes para a transação
    if has_waiting_commands(waiting, command):
        add_command_to_waiting_list(command)
        continue

    # Tenta escalonar o comando, verifica por deadlocks se não conseguir
    if not try_schedule_command(command):
        if not has_deadlock(scheduler, waiting, deadlock_graph, command):
            add_command_to_waiting_list(command)
    elif command.operation == Operation.COMMIT:
        transfer_waiting_to_scheduler()  # Após commit, transfere comandos da lista de espera


print("\nSaída final:")

for i in range (0, len(final_schedule)):
    if(i == len(final_schedule)-1):
        print(final_schedule[i])
    else:
        print(final_schedule[i], end="-")