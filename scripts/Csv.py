import pandas as pd
import sqlite3
import os

# --- Configuration ---
db_file_path = "../data/cleaned_churn.db"
table_name = "ChurnData"
# Set the path for the new CSV file you will create
csv_output_path = "../data/cleaned_churn_for_tableau.csv"


def export_cleaned_data_to_csv():
    """
    Connects to the SQLite database and exports the cleaned data to a CSV file.
    """
    try:
        print(f"Connecting to database '{db_file_path}'...")
        conn = sqlite3.connect(db_file_path)

        # Use a SQL query to read the entire ChurnData table into a Pandas DataFrame.
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)

        print(f"Successfully read {len(df)} rows from the '{table_name}' table.")

        # --- Export to CSV ---
        print(f"\nExporting data to CSV file at '{csv_output_path}'...")
        
        # Use the to_csv() method to write the DataFrame to a CSV file.
        # index=False ensures that Pandas' internal index is not written as a column.
        df.to_csv(csv_output_path, index=False)
        
        print("Data successfully exported to CSV.")

    except sqlite3.OperationalError as e:
        print(f"Error: Could not connect to the database. Please make sure '{db_file_path}' exists.")
        print(f"Detailed error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")


# Run the function
if __name__ == "__main__":
    export_cleaned_data_to_csv()
