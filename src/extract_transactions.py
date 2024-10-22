from dotenv import load_dotenv
import os
from utils import connect_to_mysql, insert_data_to_table, schema_template
import pandas as pd
from time import sleep
from dateutil import parser

load_dotenv(override=True)

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
            st.button(label="Clean And Store Statements",on_click=clean_upload_data_toserver, args=(st,statement))
        else:
            st.warning("Invalid name format for the file")


# split file name and get name of bank
def bank_extraction(statement):    
    filename = statement.name.lower()
    if "amex" in filename:
        return "amex_green"
    elif "scotia_credit" in filename:
        return "scotia_visa_credit"
    elif "scotia_debit" in filename:
        return "scotia_visa_debit"
    return None


def clean_upload_data_toserver(st, statement):
    try:
        progress = st.progress(0)
        log_placeholder = st.empty()
        # Step 1: Read the file (log progress)
        bank_name = bank_extraction(statement)
        
        df = prepare_dataframe(st=st, statement=statement, bank_name=bank_name, progress=progress, log_placeholder=log_placeholder)
        
        # Perform data cleaning and transformations
        log_placeholder.text("Step 5: Cleaning Transaction Details")
        df_cleaned = clean_data(df)
        sleep(1)
        progress.progress(90)
        
        # Connect to MySQL and insert cleaned data
        log_placeholder.text("Step 6: Inserting Data to Server")
        
        isinserted = insert_data_to_server(log_placeholder, df_cleaned, bank_name)
        sleep(1)
        progress.progress(100)
        if isinserted:
            log_placeholder.text("Data Extracted, Cleaned And Stored To Database Successfully!")
            st.success("Data Processing Successful!")
        else:
            st.error("Data Processing Failed!")
    except Exception as e:
        st.error(f"Data Processing Error! {e}")    
    
def prepare_dataframe(st, statement, bank_name, progress, log_placeholder):
    
    try:
        # Step 1: Load data from the uploaded file into a DataFrame
        log_placeholder.text("Step 1: Reading the file...")
        if bank_name in ['amex_green', 'scotia_visa_credit']:
            df = pd.read_csv(statement, header=None, names=['transactdate', 'transactdetail', 'amount'])
        if bank_name == 'scotia_visa_debit':
            df = pd.read_csv(statement, header=None, names=['transactdate', 'amount', 'transactdetail', 'longdetail'])
        sleep(1)
        progress.progress(10)
        
        # Ensure the date column is in the correct format (if needed)
        df['transactdate'] = df['transactdate'].apply(lambda x: parser.parse(x).strftime('%Y-%m-%d') if pd.notnull(x) else None)  # Convert to desired format
        sleep(0.8)
        progress.progress(25)
        
        # applying index for dataframe
        # df['id'] = range(1, len(df) + 1)
        
        # Step 2: Determine 'PurchaseType' based on the 'amount' column
        log_placeholder.text("Step 2: Determining type of purchase")
        if bank_name == 'amex_green':
            df['purchasetype'] = df['amount'].apply(lambda x: 'Credit' if x < 0 else 'Debit')
        if bank_name in ['scotia_visa_credit', 'scotia_visa_debit']:
            df['purchasetype'] = df['amount'].apply(lambda x: 'Credit' if x > 0 else 'Debit')
        df['amount'] = df['amount'].abs() # changing amount to positive values 
        sleep(0.75)
        progress.progress(40)
        
        # Step 3: Select and rearrange the columns to match the database table schema
        log_placeholder.text("Step 3: Rearranging final Dataframe")
        if bank_name in ['amex_green', 'scotia_visa_credit']:
            df_final = df[['purchasetype', 'transactdetail', 'transactdate', 'amount']]
        if bank_name == 'scotia_visa_debit':
            df_final = df[['purchasetype', 'transactdetail', 'longdetail', 'transactdate', 'amount']]
        sleep(0.5)
        progress.progress(55)

        # Step 4: Clean the data (you can customize this part)
        log_placeholder.text("Step 4: Cleaning the data")
        df_final.dropna(inplace=True)  # Remove rows with missing values
        df_final.columns = df_final.columns.str.strip()  # Strip leading/trailing whitespace from column names
        sleep(1.5)
        progress.progress(70)
        return df_final
    except Exception as e:
        st.error(e)

def clean_data(df):
    if "longdetail" in df:
        # replace deposit, withdrawal to interact etransfer
        df['transactdetail'] = df['transactdetail'].replace({'deposit': 'Interac e-transfer', 'withdrawal': 'Interac e-transfer'}, regex=True)
        
        replacements = ['pos purchase', 'bill payment', 'miscellaneous payment','rent', 'loans', 
        'investment', 'deposit', 'withdrawal', 'transfer']
        
        # strip and convert to lower case
        df['transactdetail'] = df['transactdetail'].str.strip().str.lower()
        df.loc[df['transactdetail'].isin(replacements), 'transactdetail'] = df['longdetail']
        
        # if long detail == none, payroll deposit then keep as it is
        df['transactdetail'] = df.apply(lambda row: 'payroll deposit' if pd.isnull(row['longdetail']) and 'payroll deposit' in row['transactdetail'] else row['transactdetail'], axis=1)
        
        # remove longdetail value 
        df.drop('longdetail', axis=1, inplace=True)
    else:
        # renaming inter transfer
        df.loc[df['transactdetail'].str.contains('FROM - *'), 'transactdetail'] = "Internal Transfer"

    df['transactdetail'] = df['transactdetail'].str.replace(r'apos|free', '', regex=True)
    # remove special symbols and numbers
    df['transactdetail'] = df['transactdetail'].str.split('  ').str[0].str.replace(r'[^a-zA-Z\s]', '', regex=True).str.lower()
    return df


def insert_data_to_server(log_placeholder, df_cleaned, bank_name) -> bool:
    conn, cur = connect_to_mysql(host=os.environ['HOST'], user=os.environ['SERVER_USERNAME'],port=int(os.environ['PORT']), database=os.environ['DATABASE'])
    log_placeholder.text("Connected to Database")
    sleep(0.5)
    
    # get tables and select appropriate one
    cur.execute("SHOW TABLES;")
    tables = cur.fetchall()
    selected_table = None
    for table in tables:
        if bank_name in table[0]:
            selected_table = table[0]
            break
    
    col_names, col_dtype, total_fields = schema_template(df_cleaned)
    log_placeholder.text("Scheme identified! Inserting Data...")
    sleep(1)
    
    try:    
        rows_inserted = insert_data_to_table(conn=conn, cursor=cur, database=os.environ['DATABASE'], table=selected_table, total_field=total_fields, col_names=col_names, dataframe=df_cleaned)
    except Exception as e:
        print(e)
    
    cur.close()
    conn.close()
    
    if df_cleaned.shape[0] == rows_inserted:    
        log_placeholder.text(f"{rows_inserted} rows inserted successfully")
        sleep(1)
        return True
    else:
        log_placeholder.warning(f"Duplicates Transaction Record Found! {rows_inserted}/{df_cleaned.shape[0]} rows inserted") 
        return False       
