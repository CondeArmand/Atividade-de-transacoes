import threading
import time

import psycopg2


def run_transaction(thread_name, transaction_type):
    conn = psycopg2.connect(
        host="localhost",
        database="company",
        user="postgres",
        password="montepicos"
    )

    cursor = conn.cursor()
    try:
        conn.autocommit = False

        if transaction_type == "1":
            # Operações de leitura na tabela "employee"
            cursor.execute("BEGIN")
            cursor.execute("SELECT * FROM employee where ssn = '123456789'")
            rows = cursor.fetchall()
            print(f"{thread_name}: Realizou uma leitura da tabela 'employee'")
            for row in rows:
                print(row)

            time.sleep(5)  # Simulação de processamento

            cursor.execute("SELECT * FROM employee where ssn = '123456789'")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        elif transaction_type == "2":
            # Operações de escrita na tabela "employee"

            cursor.execute("BEGIN")
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

            # Exemplo de atualização de salário para os funcionários
            cursor.execute("UPDATE employee SET salary = salary + 1000 WHERE ssn = '123456789'")
            print(f"{thread_name}: Realizou uma atualização na tabela 'employee'")
            time.sleep(10)  # Simulação de processamento
        else:
            raise ValueError("Tipo de transação inválido.")

        # time.sleep(2)  # Simulação de processamento

        conn.commit()
        print(f"{thread_name}: Transação concluída com sucesso\n")
    except Exception as e:
        conn.rollback()
        print(f"{thread_name}: Erro durante a transação:", e)
    finally:
        cursor.close()
        conn.close()


# Criação das threads concorrentes
num_threads = 5
threads = []

for i in range(num_threads):
    thread_name = f"Thread-{i + 1}"
    thread = threading.Thread(target=run_transaction, args=(thread_name, "1"))
    threads.append(thread)
    thread.start()

for i in range(num_threads):
    thread_name = f"Thread-{i + 1}"
    thread = threading.Thread(target=run_transaction, args=(thread_name, "2"))
    threads.append(thread)
    thread.start()

# Aguardar a conclusão de todas as threads
for thread in threads:
    thread.join()

print("Programa principal concluído.")
