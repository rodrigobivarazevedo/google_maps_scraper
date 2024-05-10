import sqlite3

def create_database():
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('farms.db')
        cursor = conn.cursor()

        # Create Farm table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Farms (
                farm_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                website TEXT,
                main_category TEXT,
                categories TEXT,
                phone TEXT,
                address TEXT,
                coordinates TEXT,
                link TEXT,
                latitude REAL,
                longitude REAL,
                animal_type TEXT,
                geometry TEXT,
                country TEXT,
                state TEXT,
                department TEXT
            )
        ''')

        # Commit changes and close connection
        conn.commit()
        conn.close()

        print("Database created successfully.")

    except sqlite3.Error as e:
        print("Error while connecting to SQLite", e)

# Call the function to create the database
create_database()
