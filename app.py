import sqlite3

def main():
    # Підключення до бази даних SQLite (файл "example.db" буде створено, якщо не існує)
    connection = sqlite3.connect("example.db")
    cursor = connection.cursor()
    
    # Створення таблиці 'users', якщо вона ще не існує
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );
    ''')
    
    # Вставка прикладових даних, якщо таблиця порожня
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute('INSERT INTO users (name) VALUES (?)', ("John Doe",))
        connection.commit()
    
    # Витяг та вивід всіх записів з таблиці 'users'
    cursor.execute('SELECT * FROM users')
    rows = cursor.fetchall()
    print("Користувачі:")
    for row in rows:
        print(row)
    
    # Закриття з'єднання з базою даних
    connection.close()

if __name__ == "__main__":
    main()