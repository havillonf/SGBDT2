from objects import DB, ObjectType
from operation import Command, Operation


# Função para analisar e criar os objetos dentro do banco de dados
def parse_objects(database, object_strings):
    # Itera sobre cada string que representa um objeto com prefixos de granularidade
    for object_string in object_strings:
        # Divide a string pelo tipo de objeto e nome do objeto
        obj_type_str, obj_name = object_string.split("_", 1)

        # Mapeia o prefixo para o tipo de objeto
        if obj_type_str == "TS":
            current_type = ObjectType.TABLESPACE
        elif obj_type_str == "T":
            current_type = ObjectType.TABLE
        elif obj_type_str == "P":
            current_type = ObjectType.PAGE
        elif obj_type_str == "R":
            current_type = ObjectType.ROW
        else:
            raise ValueError(f"Tipo de objeto desconhecido: {obj_type_str}")

        current_object = database  # Começa no banco de dados como ponto inicial

        # Verifica se o objeto com o nome correto já existe
        found_object = current_object.find_child(obj_name)

        if found_object:
            # Se o objeto já existe, avança para o próximo nível
            current_object = found_object
        else:
            # Se o objeto não existe, cria um novo objeto com o nome, sem prefixo
            new_object = DB(current_object, obj_name, current_type)
            current_object = new_object


# Função para analisar o escalonamento e retornar os comandos estruturados
def parse_schedule(database, schedule):
    commands = []  # Lista que armazenará os comandos gerados
    object_names = []  # Lista temporária para armazenar os nomes dos objetos
    operations = schedule.split("-")  # Divide o escalonamento pelas operações

    # Primeira iteração: coleta os nomes dos objetos
    for operation in operations:
        if operation[0] != "C":  # Ignora operações de commit nesta passagem
            start = operation.index("(") + 1
            end = operation.index(")")
            object_names.append(operation[start:end])  # Extrai o nome do objeto

    # Cria ou encontra objetos com base nos nomes extraídos
    parse_objects(database, object_names)

    # Segunda iteração: cria os comandos de transações
    object_index = 0  # Índice para rastrear a posição em object_names
    for operation in operations:
        op_type = operation[0]  # Tipo da operação (R, W, C)
        transaction_id = int(operation[1])  # ID da transação (um número)

        if op_type == "C":
            # Se for um commit, cria um comando sem objeto associado
            command = Command(Operation.from_str(op_type), None, transaction_id)
        else:
            # Para operações de leitura/escrita, encontra o objeto correto
            object_name = object_names[object_index]  # Nome do objeto
            object_index += 1  # Incrementa o índice para acompanhar o próximo objeto

            # Remove o prefixo ao buscar o objeto
            obj_name = object_name.split("_", 1)[
                1
            ]  # Remove o prefixo, busca pelo nome do objeto
            target_object = database.find_recursive(obj_name)  # Busca o objeto no banco

            if target_object is None:
                raise ValueError(f"Objeto {obj_name} não encontrado no banco de dados")

            command = Command(
                Operation.from_str(op_type), target_object, transaction_id
            )

        # Adiciona o comando à lista de comandos
        commands.append(command)

    return commands  # Retorna todos os comandos gerados
