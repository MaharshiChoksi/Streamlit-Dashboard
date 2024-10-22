import streamlit as st
from extract_transactions import get_statement
from create_insights import Generate_insights

st.set_page_config(page_title="Bank Statement Analytical Dashboard", layout="wide", initial_sidebar_state="auto")

# Initialize session state variables
if 'login_status' not in st.session_state:
    st.session_state.login_status = False


class App():
    # initialize class object
    def __init__(self) -> None:
        self.user_authentication()
    
    # checking user input to credentials
    @staticmethod
    def __credentials_check(cusername: str, cpassword: str) -> bool:
        if cusername == st.secrets['CLIENT_USERNAME'] and cpassword == st.secrets['CLIENT_PASSWORD']:
            st.session_state.login_status = True
            return True
        return False

    # authenticate user's credentials and login
    def user_authentication(self):
        if not st.session_state.login_status:
            with st.container(border=True):
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
            with st.empty().container(border=True):
                options = ("Select an option...", "Upload New Data", "Generate Previous Insights")
                selection = st.selectbox("Select an option...", options=options, index=0)
                
            if selection == options[1]:
                with st.empty().container(border=True):        
                    get_statement(st)
            elif selection == options[2]:
                Generate_insights()

# initializing the instance of object if called directly from main file
if __name__ == "__main__":
    instant = App()