import csv
import sqlite3

def insert_data_from_csv(filename):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('farms.db')
        cursor = conn.cursor()

        # Read data from CSV file and insert into Farms table
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  # Skip header row
            for row in csvreader:
                cursor.execute('''
                    INSERT INTO Farms (name, website, main_category, categories, phone, address, coordinates, link, latitude, longitude, animal_type, geometry, country, state, department)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)

        # Commit changes and close connection
        conn.commit()
        conn.close()

        print("Data inserted successfully.")

    except sqlite3.Error as e:
        print("Error while inserting data into SQLite", e)

# Call the function to insert data from CSV
#insert_data_from_csv('Bourgogne-Franche-Comté_cleaned.csv')
