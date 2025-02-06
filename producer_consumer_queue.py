import threading
import time
import queue
import random
import sqlite3

# Queue to exchange messages
message_queue = queue.Queue()

# List of 100 random names for producers to use
names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
         "Karl", "Laura", "Mallory", "Nathan", "Oscar", "Peggy", "Quentin", "Ruth", "Steve", "Trudy",
         "Uma", "Victor", "Walter", "Xander", "Yvonne", "Zack", "Adam", "Bella", "Chris", "Diana",
         "Eli", "Fiona", "Gina", "Hank", "Ivy", "Jack", "Kathy", "Liam", "Mona", "Nate",
         "Owen", "Paula", "Quinn", "Rachel", "Sam", "Tina", "Ursula", "Vince", "Wendy", "Xena",
         "Yara", "Zane", "Aaron", "Bianca", "Caleb", "Dana", "Eric", "Frida", "Gabe", "Holly",
         "Isaac", "Jill", "Kevin", "Lori", "Mike", "Nora", "Omar", "Penny", "Quincy", "Rita",
         "Scott", "Tara", "Uri", "Vera", "Wayne", "Ximena", "Yosef", "Zara", "Alex", "Britney",
         "Carl", "Derek", "Ella", "Freddie", "Gwen", "Harold", "Irene", "Josh", "Kim", "Leo"]

# Producer function
def producer(producer_id):
    while True:
        name = random.choice(names)
        age = random.randint(18, 101)
        message = f"{name}, {age}"
        message_queue.put(message)
        print(f"Producer {producer_id} produced: {message}")
        time.sleep(random.randint(1, 5))

# Consumer function
def consumer(consumer_id):
    while True:
        message = message_queue.get()
        #print(f"Consumer {consumer_id} consumed: {message}")

        # Add new record to the database 
        with sqlite3.connect('messages.db') as conn:
            cursor = conn.cursor()
            name, age = message.split(", ")
            cursor.execute('INSERT INTO messages (name, age) VALUES (?, ?)', (name, int(age)))
            conn.commit()
        time.sleep(1)

# Viewer function
def viewer():
    while True:
        with sqlite3.connect('messages.db') as conn:
            cursor = conn.cursor()

            # Check if the database is empty
            cursor.execute('SELECT COUNT(*) FROM messages')
            count = cursor.fetchone()[0]
            if count == 0:
                print("Viewer: Database is empty. Sleeping for 10 seconds.")
                time.sleep(10)
                continue

            cursor.execute('SELECT COUNT(*), AVG(age) FROM messages')
            count, avg_age = cursor.fetchone()

            cursor.execute('SELECT name FROM messages')
            names = [row[0] for row in cursor.fetchall()][0:count]

            print(f"Viewer: Number of people = {count}, Average age = {avg_age:.2f}")
            print(f"Names: {', '.join(names)}")

        time.sleep(random.randint(5, 10))

def create_db():
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS messages''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS messages
                      (id INTEGER PRIMARY KEY AUTOINCREMENT,
                       name TEXT,
                       age INTEGER)''')
    conn.commit()
    conn.close()

def main():
    create_db()
    
    producer_threads = [threading.Thread(target=producer, args=(i,)) for i in range(6)]
    consumer_threads = [threading.Thread(target=consumer, args=(i,)) for i in range(4)]
    viewer_thread = threading.Thread(target=viewer)

    for pt in producer_threads:
        pt.start()
    for ct in consumer_threads:
        ct.start()

    viewer_thread.start()

    for pt in producer_threads:
        pt.join()
    for ct in consumer_threads:
        ct.join()

    viewer_thread.join()

if __name__ == "__main__":
    main()
