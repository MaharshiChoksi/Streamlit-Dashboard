import pandas as pd
import streamlit as st
from utils import connect_to_mysql
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
import time

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
        self.__validated = False
        self.get_date_range_and_bank()
        self.create_graphs()
    
    def get_date_range_and_bank(self):
        bank_options = ["American Express", "Scotia Bank Debit", "Scotia Bank Credit"]
        with st.container(border=True):
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
        
    def create_graphs(self):
        if not self.__validated:
            return
        
        with st.spinner("Getting data & Plotting Charts..."):
            time.sleep(5)
            if not self.get_data_to_dataframe(): # getting data from server and loading to dataframe
                st.error("No Transactions Found!")
                return

            # Graph Container
            with st.container():
                st.title("Graphical Insights")
 
            with st.container(border=True):
                # TODO: create piechart(from plotly) that shows total income, spent :::> next to it shows bank name, Total income(total credit), total spent(total debit)
                col1= st.columns(1)[0]
                with col1:
                    st.subheader("Grouped Categories")
                    grouped_category_data = self.df.groupby('transactdetail')['amount'].sum().reset_index()
                    grouped_category_data['amount'] = grouped_category_data['amount'].apply(lambda x: f"{x:.2f}")
                    st.scatter_chart(grouped_category_data, x='transactdetail', y="amount")
                               

            # TODO: create barchart for all of the transactions grouped by itself and show on daily bases (x-axis date, y-axis amount -> label store name and total spent)
            # lets say there are n number of different stores where purchases were made and we need to create chart dynamically
            # add all of these charts to cols and max 3 charts on each row and add next chart below dynamically 
            
            
            st.title("Daily-Transactions")
            with st.container(border=True):
                self.df['transactdate'] = pd.to_datetime(self.df['transactdate'])
                stores = self.df['transactdetail'].unique()
                
                # Loop through each store and plot charts in rows of 3 columns
                cols_per_row = 3  # Number of charts per row
                col_idx = 0  # To track column position
                
                for i, store in enumerate(stores):
                    if col_idx == 0:  # Create new row if at the start of the row
                        cols = st.columns(cols_per_row)
                    
                    store_data = self.df[self.df['transactdetail'] == store].set_index('transactdate')
                    
                        # Display the chart in the appropriate column
                    with cols[col_idx]:
                        st.write(f'### {store.capitalize()}')
                        st.bar_chart(store_data[['amount']])
                    
                    # Update column index, and reset to 0 after 3 columns
                    col_idx = (col_idx + 1) % cols_per_row
                 
                        
            with st.sidebar:
            # TODO: Arrange data to table in sidebar
            # TODO: On table show bank name, start and end date below, total spent in tabular form like store grouped by name and total amount beside it
            # example: col1 store name, col2 total amount 
                st.title("**Transactions Breakdown**")
                # grouping data in dataframe
                grouped_df_in_out = self.df.groupby('purchasetype')['amount'].sum().reset_index()
                grouped_df_in_out.columns = ['purchasetype', 'amount']
                
                total_credited = grouped_df_in_out[grouped_df_in_out['purchasetype'] == 'Credit']['amount'].values[0]
                total_debited = grouped_df_in_out[grouped_df_in_out['purchasetype'] == 'Debit']['amount'].values[0]
                
                # Display bank details
                st.write(f"**Bank:** {self.selected_bank}")                
                st.write(f"**Transactions From:** {self.start_date}\n**Transactions Till:** {self.end_date}")
                
                st.markdown(
                    f"<div style='margin-bottom: 20px;'>"
                    f"<strong>Amount Credited To Account:</strong> <span style='color:green;'><strong>${total_credited}</strong></span><br>"
                    f"<strong>Amount Debited From Account:</strong> <span style='color:red;'><strong>${total_debited}</strong></span></div>",
                    unsafe_allow_html=True
                )

                st.write(grouped_category_data.to_html(index=False), unsafe_allow_html=True)
        
    # TODO: Get data for the selected date and bank
    def get_data_to_dataframe(self):
        self.conn, self.cur = connect_to_mysql(host=st.secrets['HOST'], user=st.secrets['SERVER_USERNAME'], port=int(st.secrets['PORT']), database=st.secrets['DATABASE'])
        query = f"SELECT purchasetype, transactdetail, transactdate, amount FROM {self.table} WHERE transactdate BETWEEN '{self.start_date}' AND '{self.end_date}';"
        self.df = pd.read_sql(query, self.conn) # loading data to dataframe

        if self.df.empty: # verify if dataframe is empty or not
            return False
        self.conn.close() # close connection
        return True
