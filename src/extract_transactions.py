from utils import connect_to_mysql, insert_data_to_table
import pandas as pd
import time

"""
Program Algorithm:
get_statement -> ask user for statements, validate files
clean_upload_data_toserver
    |- bank_extraction -> gets bank name)
    |- prepare_dateframe -> it cleans, arranges and returns dataframe for further processing
    |- clean_data -> cleans transaction detail
    |- insert_data_to_server -> insert cleaned data to tables in server
"""

def get_statement(st):
    # Add a flag to track the processing status
    if 'processing' not in st.session_state:
        st.session_state.processing = False
    # login state
    if not st.session_state.login_status:
        return False
    
    st.subheader("Extracting And Cleaning Transaction Data")
    st.info("""Please upload statements for Scotia debit, credit & Amex credit card only.\n\n
File name format: bank_card_till_date_month(3 letters)_year.csv\n\n
eg.amex_credit_till_15_oct_2024.csv\n\nFile column format: date, transaction detail, amount""")
    
    statement = st.file_uploader(label="Upload statements", type=['.csv'], accept_multiple_files=False)
        
    # Proceed to extraction
    if statement:
        if any(statement.name.lower().startswith(prefix) for prefix in ['scotia_credit','scotia_debit','amex_credit']):
            st.toast(f"File '{statement.name}' uploaded successfully!")
            if st.button(label="Clean And Store Statements", disabled=st.session_state.processing):
                st.session_state.processing = True
                clean_upload_data_toserver(st=st, statement=statement)
        else:
            st.warning("Invalid name format for the file")


# split file name and get name of bank
def bank_extraction(statement):    
    filename = statement.name.lower()
    if "amex" in filename:
        return "American Express"
    elif "scotia_credit" in filename:
        return "Scotia Credit"
    elif "scotia_debit" in filename:
        return "Scotia Debit"
    return None


def clean_upload_data_toserver(st, statement):
    progress = st.progress(0)
    log_placeholder = st.empty()
    # Step 1: Read the file (log progress)
    bank_name = bank_extraction(statement)
    
    df = prepare_dataframe(statement=statement, bank_name=bank_name, progress=progress, log_placeholder=log_placeholder)
    
    # Perform data cleaning and transformations
    log_placeholder.text("Step-5: Cleaning Transaction Details")
    df_cleaned = clean_data(df)
    time.sleep(1)
    progress.progress(90)
    
    # Connect to MySQL and insert cleaned data
    log_placeholder.text("Step-6: Inserting Data to Server")
    insert_data_to_server(df_cleaned, bank_name)
    time.sleep(1)
    progress.progress(100)
    log_placeholder.text("Data Extraction, Cleaning and Uploading process completed!")
    
    # Re-enable components after processing
    st.session_state.processing = False  # Set processing flag to False

    # Display the cleaned data
    st.write(df_cleaned)
    
    
def prepare_dataframe(statement, bank_name, progress, log_placeholder):
    
    # Step 1: Load data from the uploaded file into a DataFrame
    log_placeholder.text("Step 1: Reading the file...")
    if bank_name in ['American Express', 'Scotia Credit']:
        df = pd.read_csv(statement, header=None, names=['transactdate', 'transactdetail', 'amount'])
    if bank_name == 'Scotia Debit':
        df = pd.read_csv(statement, header=None, names=['transactdate', 'amount', 'transactdetail', 'longdetail'])
    time.sleep(1)
    progress.progress(10)
    
    # Step 2: Clean the data (you can customize this part)
    log_placeholder.text("Step 2: Cleaning the data")
    df.dropna(inplace=True)  # Remove rows with missing values
    df.columns = df.columns.str.strip()  # Strip leading/trailing whitespace from column names
    time.sleep(1.5)
    progress.progress(25)
    
    # Step 3: Generate a unique ID for each row (optional if your table auto-generates IDs)
    log_placeholder.text("Step 3: Inserting id's and formatting date")
    df['id'] = range(1, len(df) + 1)
    # Ensure the date column is in the correct format (if needed)
    df['transactdate'] = pd.to_datetime(df['transactdate'], errors='coerce')  # Handle date parsing issues
    df['transactdate'] = df['transactdate'].dt.strftime('%d-%m-%Y')  # Convert to desired format
    time.sleep(1)
    progress.progress(40)
    
    # Step 4: Determine 'PurchaseType' based on the 'amount' column
    log_placeholder.text("Step 4: Determining type of purchase")
    # TODO: create for scot debit ,cred
    if bank_name == 'American Express':
        df['purchasetype'] = df['amount'].apply(lambda x: 'Credit' if x < 0 else 'Debit')
    if bank_name in ['Scotia Credit', 'Scotia Debit']:
        df['purchasetype'] = df['amount'].apply(lambda x: 'Credit' if x > 0 else 'Debit')
    df['amount'] = df['amount'].abs() # changing amount to positive values 
    time.sleep(2)
    progress.progress(55)
    
    # Step 5: Select and rearrange the columns to match the database table schema
    log_placeholder.text("Step 4: Rearranging final Dataframe")
    if bank_name in ['American Express', 'Scotia Credit']:
        df_final = df[['id', 'purchasetype', 'transactdetail', 'transactdate', 'amount']]
    if bank_name == 'Scotia Debit':
        df_final = df[['id', 'purchasetype', 'transactdetail', 'longdetail', 'transactdate', 'amount']]
    time.sleep(0.5)
    progress.progress(70)

    return df_final

def clean_data(df):
    df.dropna(inplace=True) # replace null values
    df.columns = df.columns.str.strip()  # Remove leading/trailing whitespace from columns
    # df.
    return df

def insert_data_to_server(df_cleaned, bank_name):
    pass