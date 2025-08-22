import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import hashlib
from google.oauth2 import service_account


# Authentication credentials (same as tgr.py)
CREDENTIALS = {
    "admin": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",  # admin
    "user": "04f8996da763b7a969b1028ee3007569eaf3a635486ddab211d512c85b9df8fb",  # user
    "dina.teilab@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mai.sobhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mostafa.sayed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.hassan@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.youssef@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.nagy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "adel.abuelella@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ammar.abdelbaset@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "youssef.mohamed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "abdallah.hazem@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.abdelgalil@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohanad.elgarhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
}


def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in CREDENTIALS and \
                hashlib.sha256(st.session_state["password"].encode()).hexdigest() == CREDENTIALS[
            st.session_state["username"]]:
            st.session_state["password_correct"] = True
            st.session_state["current_user"] = st.session_state["username"]  # Store the username
            del st.session_state["password"]  # Don't store the password
            del st.session_state["username"]  # Don't store the username
        else:
            st.session_state["password_correct"] = False
            if "username" in st.session_state:
                del st.session_state["username"]
               
    # Return True if the password is validated
    if st.session_state.get("password_correct", False):
        return True

    # Show input fields for username and password
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password")
    st.button("Login", on_click=password_entered)

    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")

    return False


def get_bigquery_client():
    """Get BigQuery client using credentials."""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["service_account"]
        )
    except (KeyError, FileNotFoundError):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                'service_account.json'
            )
        except FileNotFoundError:
            st.error(
                "No credentials found. Please configure either Streamlit secrets or provide a service_account.json file.")
            return None

    return bigquery.Client(credentials=credentials)


def run_car_position_query(client):
    """Run the car position query and return results."""
    query = """
    SELECT
        A.`event_date`,
        A.`carName`,
        A.`position` AS position_mode,
        dim_car.retail_current_status,
        make,
        model,
        year,
        kilometers,
        sylndr_selling_price,
        CASE WHEN sylndr_selling_price Between 0 AND 800000 THEN 'A.0-800k'
            WHEN sylndr_selling_price Between 800001 AND 1100000 THEN 'B.800k-1.1M'
            WHEN sylndr_selling_price Between 1100001 AND 1600000 THEN 'C.1.1M-1.6M'
            WHEN sylndr_selling_price > 1600001 THEN 'D.1.6M-2.1M+'
            ELSE 'NULL'
            END AS price_range,
        CASE WHEN kilometers Between 0 AND 30000 THEN 'A.0-30k'
            WHEN kilometers Between 30001 AND 60000 THEN 'B.30K-60K'
            WHEN kilometers Between 60001 AND 90000 THEN 'C.60K-90K'
            WHEN kilometers Between 90001 AND 120000 THEN 'D.90K-120K'
            ELSE 'E.120K+'
            END AS KM_Range,
        CASE
            WHEN year Between 2010 AND 2016 THEN 'A.2010-2016'
            WHEN year Between 2017 AND 2019 THEN 'B.2017-2019'
            WHEN year Between 2020 AND 2021 THEN 'C.2020-2021'
            ELSE 'D.2022-2023' END AS year_range,
        car_profile_sessions AS Sessions,
        actual_booking_all AS Bookings
    FROM (
        SELECT
            `event_date`,
            `carName`,
            `position`,
            ROW_NUMBER() OVER(PARTITION BY `event_date`, `carName`ORDER BY COUNT(*) DESC) AS rn
        FROM `google_analytics`.`action_click_on_car_card`
        WHERE event_date = current_date()
        AND appliedFilter = 'false'
        AND appliedSort = 'false'
        AND sourceScreen = "Cars Listing"
        GROUP BY `event_date`, `carName`,`position`
        QUALIFY rn = 1
    ) AS A
    LEFT JOIN reporting.retail_funnel on retail_funnel.carName = A.carName AND retail_funnel.event_date = A.event_date
    LEFT JOIN gold.dim_car on dim_car.car_name = A.carName
    ORDER BY A.position DESC
    """

    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()


def format_car_names_for_copy(car_names):
    """Format car names as comma-separated string for easy copying."""
    return ",".join(car_names)


def main():
    st.set_page_config(
        page_title="Car Position Query Tool",
        page_icon="🚗",
        layout="wide"
    )

    st.title("🚗 Car Position Query Tool")
    st.markdown("*Get all unreserved cars with bottom 10 (by position) listed first*")

    if not check_password():
        return

    # Track page view
    if "current_user" in st.session_state:
        pass  # User tracking would go here

    # Get BigQuery client
    client = get_bigquery_client()
    if not client:
        return

    st.markdown("---")

    # Main query section
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Query Controls")

        if st.button("🔍 Run Query", type="primary", use_container_width=True):
            with st.spinner("Running query..."):
                # Track query execution
                if "current_user" in st.session_state:
                    pass  # Query tracking would go here

                df = run_car_position_query(client)

                if not df.empty:
                    st.session_state['query_results'] = df
                    st.success(f"Query completed! Found {len(df)} cars.")
                else:
                    st.warning("No results found.")

    with col2:
        st.subheader("Instructions")
        st.info("""
        **This tool will:**
        1. Run the car position query for today's data
        2. Order results by position (descending - bottom cars first)
        3. Get ALL unreserved cars from the query
        4. Reorder so bottom 10 cars appear first in the list
        5. Provide all car names in copy-ready format: C-12345,C-67890,...
        """)

    # Results section
    if 'query_results' in st.session_state and not st.session_state['query_results'].empty:
        df = st.session_state['query_results']


        # Get all unreserved cars
        all_unreserved_cars = df[
            df['retail_current_status'] != 'Reserved'] if 'retail_current_status' in df.columns else pd.DataFrame()

        if not all_unreserved_cars.empty:
            # Get bottom 10 unreserved cars (first 10 from the DESC ordered data)
            bottom_10_unreserved = all_unreserved_cars.head(10)['carName'].tolist()

            # Get remaining unreserved cars (everything after the first 10)
            remaining_unreserved = all_unreserved_cars.iloc[10:]['carName'].tolist()

            # Combine: bottom 10 first, then the rest
            reordered_car_names = bottom_10_unreserved + remaining_unreserved
            formatted_names = format_car_names_for_copy(reordered_car_names)

            st.subheader("🔓 All Unreserved Cars (Bottom 10 First)")

            st.subheader("📋 Car Names (Ready to Copy)")

            # Display in a large, easy-to-select text area
            st.text_area(
                "Select All & Copy:",
                formatted_names,
                height=200,
                help="Click in the box, select all (Ctrl+A), then copy (Ctrl+C)"
            )

            # Also show as code block for backup
            st.code(formatted_names, language="text")

            st.success(f"Found {len(reordered_car_names)} total unreserved cars (bottom 10 listed first)")
            st.info(f"Bottom 10 cars: {len(bottom_10_unreserved)} | Remaining cars: {len(remaining_unreserved)}")

            # Track copy action
            if "current_user" in st.session_state:
                pass  # Analytics tracking would go here
        else:
            st.warning("No unreserved cars found.")

        # Show all results in expandable section
        with st.expander("📈 View All Query Results", expanded=False):
            
            st.dataframe(
                df,
                column_config={
                    "event_date": "Date",
                    "carName": "Car Name",
                    "position_mode": "Position",
                    "retail_current_status": "Status",
                    "make": "Make",
                    "model": "Model",
                    "year": "Year",
                    "kilometers": st.column_config.NumberColumn("KM", format="%d km"),
                    "sylndr_selling_price": st.column_config.NumberColumn("Price", format="EGP %d"),
                    "Sessions": "Sessions",
                    "Bookings": "Bookings"
                },
                use_container_width=True
            )


if __name__ == "__main__":
    main()
