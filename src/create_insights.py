import os
import pandas as pd
import streamlit as st
from utils import connect_to_mysql


class Generate_insights():
    def __init__(self) -> None:
        self.start_date = None
        self.end_date = None
        self.selected_bank = None
        self.table = None
        self.__validated = False
        self.conn = None
        self.cur = None
        self.df = None
        self.get_date_range_and_bank()
        self.create_panel()
    
    def get_date_range_and_bank(self):
        bank_options = ["American Express", "Scotia Bank Debit", "Scotia Bank Credit"]
        with st.empty().container(border=True):
            st.subheader("Select Date Range To Create Insights")
            self.start_date = st.date_input(label="Start Date").strftime("%Y-%m-%d")
            self.end_date = st.date_input(label="End Date").strftime("%Y-%m-%d")
            self.selected_bank = st.selectbox(label="Select Bank", options=bank_options)

            if st.button(label="Generate"):
                if self.start_date >= self.end_date:
                    st.toast("Start date should be less than end date!")
                    self.__validated = False
                else:
                    self.__validated = True
                        
            bank_table_mapping = {
                "American Express": "amex_green",
                "Scotia Bank Debit": "scotia_visa_debit",
                "Scotia Bank Credit": "scotia_visa_credit"
            }
            self.table = bank_table_mapping.get(self.selected_bank)
        return
        
    def create_panel(self):
        if not self.__validated:
            return
        
        with st.spinner("Getting data..."):
            if not self.get_data_to_dataframe():
                st.error("No Transactions Found!")
                return

            # Graph Container
            with st.container(border=True):
                st.title("Graphical Insights")
                # TODO: create piechart that shows total income, spent :::> next to it shows bank name, Total income(total credit), total spent(total debit)
                # TODO: create barchart for all of the transactions grouped by itself and show on daily bases (x-axis date, y-axis amount -> label store name and total spent)
                # add all of these charts to cols and max 3 charts on each row and add next chart below dynamically 
                
                        
            with st.sidebar:
            # TODO: Arrange data to table in sidebar
            # TODO: On table show bank name, start and end date below, total spent in tabular form like store grouped by name and total amount beside it
            # example: col1 store name, col2 total amount 
                st.subheader("Categories")
                st.table()
    
    # TODO: Get data for the selected date and bank
    def get_data_to_dataframe(self):
        self.conn, self.cur = connect_to_mysql(host=os.environ['HOST'], user=os.environ['SERVER_USERNAME'], port=int(os.environ['PORT']), database=os.environ['DATABASE'])
        query = f"SELECT purchasetype, transactdetail, transactdate, amount FROM {self.table} WHERE transactdate BETWEEN '{self.start_date}' AND '{self.end_date}';"
        self.df = pd.read_sql(query, self.conn) # loading data to dataframe

        if self.df.empty: # verify if dataframe is empty or not
            return False
        self.conn.close() # close connection
        return True
