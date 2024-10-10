import streamlit as st
import os
from dotenv import load_dotenv

# Set the page layout to wide mode
st.set_page_config(layout="wide")

# Load environment variables from a .env file
load_dotenv()

DB_NAME = os.getenv('DB_NAME')

# Custom CSS for minimal width sidebar
sidebar_css = """
    <style>
    /* Style the sidebar background */
    [data-testid="stSidebar"] {
        background-color: #1560BD;
        width: 120px;  /* Minimal width */
    }

    /* Force the sidebar to be smaller */
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
        width: 120px;  /* Minimal width */
    }
    
    /* Style for the sidebar buttons */
    .sidebar-btn {
        display: block;
        width: 100%;
        padding: 10px;
        margin: 10px 0;
        background-color: #1E90FF;
        color: white;
        font-size: 14px;
        border: none;
        text-align: center;
        border-radius: 5px;
        cursor: pointer;
        transition: background-color 0.3s;
    }

    /* Hover effect for buttons */
    .sidebar-btn:hover {
        background-color: #104e8b;
    }

    /* Align the text */
    h2 {
        color: white;
        text-align: center;
        font-size: 14px;
        margin-bottom: 10px;
    }
    </style>
"""

# Inject custom CSS
st.markdown(sidebar_css, unsafe_allow_html=True)

# Sidebar layout with minimal width
with st.sidebar:
    st.markdown("<h2>Menu</h2>", unsafe_allow_html=True)  # Optional title in the sidebar
    
    # Define buttons for sidebar
    if st.button("Dashboard", key="dashboard"):
        st.session_state.active_page = "Dashboard"

    if st.button("Users", key="users"):
        st.session_state.active_page = "Users"
    
    if st.button("Settings", key="settings"):
        st.session_state.active_page = "Settings"
    
    if st.button("Reports", key="reports"):
        st.session_state.active_page = "Reports"

# Handling button click logic
if 'active_page' not in st.session_state:
    st.session_state.active_page = "Dashboard"  # Default active page

# Main content based on active page
if st.session_state.active_page == "Dashboard":
    st.title("Welcome to the Dashboard!")
    st.write("This is the dashboard view.")

elif st.session_state.active_page == "Users":
    st.title("User Management")
    st.write("Manage users from this view.")

elif st.session_state.active_page == "Settings":
    st.title("Settings")
    st.write("Here you can configure the app settings.")

elif st.session_state.active_page == "Reports":
    st.title("Reports")
    st.write("Generate reports from this view.")