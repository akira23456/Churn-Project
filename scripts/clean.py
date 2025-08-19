import pandas as pd
import sqlite3
import os

# --- Configuration ---
# The path to the CSV file is now relative to the 'scripts' directory.
# We go up one level (..) to the root, then into the 'data' directory.
csv_file_path = "../data/customer_churn_dataset-training-master.csv"
# The database file will also be created inside the 'data' directory.
db_file_path = "../data/cleaned_churn.db"
# Set the name of the table you want to create in the database
table_name = "ChurnData"


def clean_and_load_data():
    """
    Cleans the raw customer churn data using Pandas and loads it into a SQLite database.
    """
    try:
        print(f"Loading data from '{csv_file_path}' into a Pandas DataFrame...")
        
        # Load the CSV file. We use low_memory=False to handle the large file and
        # prevent Pandas from inferring mixed data types, which can cause issues.
        df = pd.read_csv(csv_file_path, low_memory=False)

        print("Initial DataFrame Info:")
        df.info()

        # --- Step 1: Data Cleaning ---
        print("\n--- Cleaning the data ---")
        
        # Check for and remove rows with any missing values.
        # This is a good first step to handle the incomplete rows that can cause import errors.
        # We'll use a copy to avoid a SettingWithCopyWarning.
        df_cleaned = df.dropna().copy()
        
        print(f"Removed {len(df) - len(df_cleaned)} rows with missing values.")
        
        # --- Step 2: Data Type Conversion ---
        # Explicitly convert columns to the correct data types.
        # This prevents the "datatype mismatch" error during the SQL import.
        # We'll use nullable integer types where appropriate.

        # Convert numeric columns to appropriate integer or float types.
        numeric_cols_to_convert = [
            'CustomerID', 'Age', 'Tenure', 'Usage Frequency', 
            'Support Calls', 'Payment Delay', 'Last Interaction', 'Churn'
        ]
        
        for col in numeric_cols_to_convert:
            # Use pd.to_numeric with errors='coerce' to turn non-numeric values into NaN,
            # then convert to the desired type. We drop the NaNs first, so this is safe.
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').astype('Int64')

        # Convert 'Total Spend' to float, as it has decimal values.
        df_cleaned['Total Spend'] = pd.to_numeric(df_cleaned['Total Spend'], errors='coerce').astype('float64')

        print("\nCleaned DataFrame Info after type conversion:")
        df_cleaned.info()
        
        # --- Step 3: Load Data into SQLite ---
        print("\n--- Loading cleaned data into SQLite database ---")

        # Create a connection to the SQLite database. If the file doesn't exist, it will be created.
        conn = sqlite3.connect(db_file_path)

        # Use the to_sql() method to write the DataFrame to the database.
        # if_exists='replace' will drop the table if it already exists and create a new one.
        # index=False ensures that Pandas' internal index is not written as a column.
        df_cleaned.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"Successfully loaded {len(df_cleaned)} rows into the '{table_name}' table.")

        # --- Step 4: Verification ---
        print("\n--- Verifying the data load ---")
        # Query the database to confirm the data was loaded correctly.
        query = f"SELECT * FROM {table_name} LIMIT 5;"
        df_from_db = pd.read_sql(query, conn)
        print("First 5 rows from the new database table:")
        print(df_from_db)

    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found. Please ensure it is in the correct directory relative to this script.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Ensure the database connection is closed properly.
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")


# Run the function
if __name__ == "__main__":
    clean_and_load_data()
