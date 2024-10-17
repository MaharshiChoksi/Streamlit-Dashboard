from dotenv import load_dotenv
import streamlit as st
from utils import connect_to_mysql, get_data_from_table, insert_data_to_table
from extract_transactions import get_statement
import os

load_dotenv(override=True)
st.set_page_config(page_title="Bank Statement Analytical Dashboard" )

# Initialize session state variables
if 'login_status' not in st.session_state:
    st.session_state.login_status = False


class App():
    def __init__(self) -> None:
        self.user_authentication()
    
    @staticmethod
    def __credentials_check(cusername: str, cpassword: str) -> bool:
        if cusername == os.environ['CLIENT_USERNAME'] and cpassword == os.environ['CLIENT_PASSWORD']:
            st.session_state.login_status = True
            return True
        return False

    def user_authentication(self):
        if not st.session_state.login_status:
            st.subheader("Client Authentication")
            client_username = st.text_input("Username")
            client_password = st.text_input("Password", type="password")

            if st.button("Login"):
                # validate login
                if self.__credentials_check(client_username, client_password):
                    st.rerun()  # Rerun the script immediately after login
                else:
                    st.error("Authorization Failed")
        else:
            options = ("Select an option...", "Upload New Data", "Generate Previous Insights")
            selection = st.selectbox("Select an option...", options=options, index=0)
            
            if selection == options[1]:
                get_statement(st)
            # TODO-1: Create new function to generate previous data insights
            elif selection == options[2]:
                ("""add function which will ask user to select date range and extract data from database and generate insights""")
    
    
    
    def __clear_envs(self):
        print("Getting all Environmental variables")
        keys=list(os.environ.keys())
        print("Clearing all envs")
        for key in keys:
            os.environ.pop(key, None)
        print("All envs cleaned")

if __name__ == "__main__":
    instant = App()
    # instant._App__clear_envs()  # clear all envs at end of program