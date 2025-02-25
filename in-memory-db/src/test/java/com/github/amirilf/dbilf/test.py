import socket
import time
import threading

HOST = "localhost"
PORT = 9090

def send_command(sock, command):
    """Send a command to the database and return the response.
    This function reads until it sees the prompt "dbilf> " again.
    """
    sock.sendall((command + "\n").encode())
    response = b""
    while True:
        data = sock.recv(4096)
        if not data:
            break
        response += data
        if b"dbilf> " in data:
            break
    # Remove prompt from response
    return response.decode().replace("dbilf> ", "").strip()

def test_create_table():
    with socket.create_connection((HOST, PORT)) as sock:
        # Read initial welcome prompt
        send_command(sock, "")
        cmd = "CREATE TABLE employees (id LONG, name VARCHAR 100, age INTEGER, salary INTEGER)"
        print("[TEST] Create Table: " + send_command(sock, cmd))
        time.sleep(0.5)

def test_insert_select():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        cmd = "INSERT INTO employees (name, age, salary) VALUES ('Alice', 30, 50000)"
        print("[TEST] Insert: " + send_command(sock, cmd))
        cmd = "SELECT * FROM employees"
        print("[TEST] Select All:\n" + send_command(sock, cmd))
        time.sleep(0.5)

def test_update():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        # Assume a row with id=1 exists.
        cmd = "UPDATE employees SET salary=60000 WHERE id = 1"
        print("[TEST] Update: " + send_command(sock, cmd))
        cmd = "SELECT * FROM employees WHERE id = 1"
        print("[TEST] Select Updated:\n" + send_command(sock, cmd))
        time.sleep(0.5)

def test_delete_non_pk():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        # Insert two rows with the same name for testing deletion by non-PK column.
        cmd1 = "INSERT INTO employees (name, age, salary) VALUES ('Bob', 40, 55000)"
        cmd2 = "INSERT INTO employees (name, age, salary) VALUES ('Bob', 45, 57000)"
        print("[TEST] Insert Bob1: " + send_command(sock, cmd1))
        print("[TEST] Insert Bob2: " + send_command(sock, cmd2))
        # Delete by non-PK field (name)
        cmd = "DELETE FROM employees WHERE name = 'Bob'"
        print("[TEST] Delete by Name: " + send_command(sock, cmd))
        # Check if any row with name Bob remains.
        cmd = "SELECT * FROM employees WHERE name = 'Bob'"
        print("[TEST] Select Bob:\n" + send_command(sock, cmd))
        time.sleep(0.5)

def test_create_drop_index():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        cmd = "CREATE INDEX ON employees (salary) UNIQUE"
        print("[TEST] Create Index: " + send_command(sock, cmd))
        cmd = "DROP INDEX ON employees (salary)"
        print("[TEST] Drop Index: " + send_command(sock, cmd))
        time.sleep(0.5)

def test_transaction_commit():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        print("[TEST] Begin Transaction: " + send_command(sock, "BEGIN"))
        cmd = "INSERT INTO employees (name, age, salary) VALUES ('Charlie', 35, 58000)"
        print("[TEST] Insert in Tx: " + send_command(sock, cmd))
        print("[TEST] Commit Transaction: " + send_command(sock, "COMMIT"))
        cmd = "SELECT * FROM employees WHERE name = 'Charlie'"
        print("[TEST] Select Charlie:\n" + send_command(sock, cmd))
        time.sleep(0.5)

def test_transaction_rollback():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        print("[TEST] Begin Transaction: " + send_command(sock, "BEGIN"))
        cmd = "INSERT INTO employees (name, age, salary) VALUES ('David', 50, 62000)"
        print("[TEST] Insert in Tx: " + send_command(sock, cmd))
        print("[TEST] Rollback Transaction: " + send_command(sock, "ROLLBACK"))
        cmd = "SELECT * FROM employees WHERE name = 'David'"
        print("[TEST] Select David:\n" + send_command(sock, cmd))
        time.sleep(0.5)

def bulk_insert(start, count):
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            send_command(sock, "")
            for i in range(start, start + count):
                name = f"Employee{i}"
                age = 20 + (i % 50)
                salary = 40000 + (i % 20001)
                cmd = f"INSERT INTO employees (name, age, salary) VALUES ('{name}', {age}, {salary})"
                send_command(sock, cmd)
                if (i - start + 1) % 1000 == 0:
                    print(f"[BULK] Thread starting at {start}: Inserted {i - start + 1} rows")
    except Exception as e:
        print("[BULK] Error:", e)

def test_bulk_insertion(total_rows=10000, thread_count=4):
    print(f"[TEST] Starting bulk insertion of {total_rows} rows using {thread_count} threads...")
    rows_per_thread = total_rows // thread_count
    threads = []
    for i in range(thread_count):
        start = 10000 + i * rows_per_thread
        t = threading.Thread(target=bulk_insert, args=(start, rows_per_thread))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print("[TEST] Bulk insertion completed.")

def test_drop_table():
    with socket.create_connection((HOST, PORT)) as sock:
        send_command(sock, "")
        cmd = "DROP TABLE employees"
        print("[TEST] Drop Table: " + send_command(sock, cmd))
        time.sleep(0.5)

def main():
    print("[INFO] Waiting 2 seconds for the DB server to start...")
    time.sleep(2)
    
    print("[INFO] Running tests sequentially...")
    test_create_table()
    test_insert_select()
    test_update()
    test_delete_non_pk()
    test_create_drop_index()
    test_transaction_commit()
    test_transaction_rollback()
    test_bulk_insertion(total_rows=5000, thread_count=2)
    test_drop_table()
    
    print("[INFO] All tests completed.")

if __name__ == "__main__":
    main()
