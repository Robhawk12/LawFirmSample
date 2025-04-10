import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from st_aggrid import AgGrid, GridOptionsBuilder
import os
import tempfile
from data_processor import DataProcessor
from data_analyzer import DataAnalyzer
from visualization import create_visualizations
from query_engine import process_natural_language_query
from sample_data import load_sample_data
from utils import filter_dataframe
from database import ArbitrationDatabase

# Page configuration
st.set_page_config(
    page_title="Arbitration Data Visualization Dashboard",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for data storage
if 'data' not in st.session_state:
    st.session_state.data = None
if 'filtered_data' not in st.session_state:
    st.session_state.filtered_data = None
if 'metrics' not in st.session_state:
    st.session_state.metrics = {}

def main():
    """Main function to run the Streamlit app."""
    
    # Header with navigation
    st.title("⚖️ Law Firm Data Dashboard")
    
    # Sidebar for filtering
    with st.sidebar:
        # Automatically load sample data if no data is loaded
        if st.session_state.data is None:
            load_sample_data_to_state()
        
        # Only show filters if data is loaded
        if st.session_state.data is not None:
            st.header("Filters")
            
            # Date range filter (if dates are available)
            if 'Date_Filed' in st.session_state.data.columns:
                import datetime
                
                # Clean date column first - drop NaN and convert to datetime
                date_series = st.session_state.data['Date_Filed'].dropna()
                
                # Skip if the column is empty after dropping NaN
                if len(date_series) > 0:
                    try:
                        # Try to find min and max dates - handle mixed types
                        valid_dates = []
                        for d in date_series:
                            try:
                                # Convert various date formats to datetime.date
                                if isinstance(d, datetime.date):
                                    valid_dates.append(d)
                                elif isinstance(d, datetime.datetime):
                                    valid_dates.append(d.date())
                                elif isinstance(d, str):
                                    try:
                                        parsed_date = pd.to_datetime(d).date()
                                        valid_dates.append(parsed_date)
                                    except:
                                        pass  # Skip unparseable strings
                            except:
                                pass  # Skip any problematic values
                        
                        # Use the min and max of valid dates if available
                        if valid_dates:
                            min_date = min(valid_dates)
                            max_date = max(valid_dates)
                            
                            date_range = st.date_input(
                                "Date Range",
                                [min_date, max_date],
                                min_value=min_date,
                                max_value=max_date
                            )
                        else:
                            # Fallback to default range if no valid dates found
                            today = datetime.date.today()
                            default_start = today - datetime.timedelta(days=365)  # 1 year ago
                            default_end = today
                            date_range = st.date_input(
                                "Date Range",
                                [default_start, default_end]
                            )
                    except Exception as e:
                        # Log the error and use default date range as fallback
                        print(f"Error processing dates: {e}")
                        today = datetime.date.today()
                        default_start = today - datetime.timedelta(days=365)  # 1 year ago
                        default_end = today
                        date_range = st.date_input(
                            "Date Range",
                            [default_start, default_end]
                        )
                else:
                    # No dates available, use default range
                    today = datetime.date.today()
                    default_start = today - datetime.timedelta(days=365)  # 1 year ago
                    default_end = today
                    date_range = st.date_input(
                        "Date Range",
                        [default_start, default_end]
                    )
            
            # Helper function to safely sort options with None values and mixed types
            def safe_sort(options):
                # Filter out None values
                filtered_options = [opt for opt in options if opt is not None]
                
                # Convert all values to strings for consistent sorting
                return sorted(filtered_options, key=lambda x: str(x))
            
            # Arbitrator filter
            arbitrator_options = ['All'] + safe_sort(st.session_state.data['Arbitrator_Name'].unique().tolist())
            selected_arbitrator = st.selectbox("Arbitrator", arbitrator_options)
            
            # Respondent filter
            respondent_options = ['All'] + safe_sort(st.session_state.data['Respondent_Name'].unique().tolist())
            selected_respondent = st.selectbox("Respondent (Company)", respondent_options)
            
            # Consumer Attorney filter
            attorney_options = ['All'] + safe_sort(st.session_state.data['Consumer_Attorney'].unique().tolist())
            selected_attorney = st.selectbox("Consumer Attorney", attorney_options)
            
            # Forum filter
            forum_options = ['All'] + safe_sort(st.session_state.data['Forum'].unique().tolist())
            selected_forum = st.selectbox("Arbitration Forum", forum_options)
            
            # Disposition Type filter
            disposition_options = ['All'] + safe_sort(st.session_state.data['Disposition_Type'].unique().tolist())
            selected_disposition = st.selectbox("Type of Disposition", disposition_options)
            
            # Apply filters button
            if st.button("Apply Filters"):
                with st.spinner("Applying filters..."):
                    # Apply filters to the data
                    filtered_data = filter_dataframe(
                        st.session_state.data,
                        arbitrator=selected_arbitrator if selected_arbitrator != 'All' else None,
                        respondent=selected_respondent if selected_respondent != 'All' else None,
                        attorney=selected_attorney if selected_attorney != 'All' else None,
                        forum=selected_forum if selected_forum != 'All' else None,
                        disposition=selected_disposition if selected_disposition != 'All' else None,
                        date_range=date_range if 'Date_Filed' in st.session_state.data.columns else None
                    )
                    
                    st.session_state.filtered_data = filtered_data
                    
                    # Recalculate metrics for filtered data
                    analyzer = DataAnalyzer(filtered_data)
                    st.session_state.metrics = analyzer.calculate_metrics()
                    
                    st.success("Filters applied!")
            
            # Reset filters button
            if st.button("Reset Filters"):
                st.session_state.filtered_data = st.session_state.data
                
                # Recalculate metrics for all data
                analyzer = DataAnalyzer(st.session_state.data)
                st.session_state.metrics = analyzer.calculate_metrics()
                
                st.success("Filters reset!")
    
    # Main content area
    if st.session_state.data is None:
        st.info("Loading sample data...")
        load_sample_data_to_state()
    else:
        # Tab-based interface
        tab1, tab2, tab3 = st.tabs(["Dashboard", "Data Explorer", "AI Query"])
        
        with tab1:
            # Dashboard tab with visualizations
            
            # Key metrics cards
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Disputes",
                    st.session_state.metrics.get('total_disputes', 0)
                )
            
            with col2:
                st.metric(
                    "Total Claim Amount",
                    f"${st.session_state.metrics.get('total_claim_amount', 0):,.2f}"
                )
            
            with col3:
                st.metric(
                    "Avg Claim Amount",
                    f"${st.session_state.metrics.get('avg_claim_amount', 0):,.2f}"
                )
            
            with col4:
                st.metric(
                    "Avg Consumer Claimant",
                    f"${st.session_state.metrics.get('avg_consumer_claimant', 0):,.2f}"
                )
            
            # Create visualizations
            create_visualizations(st.session_state.filtered_data)
        
        with tab2:
            # Data Explorer tab with filterable table
            st.header("Data Explorer")
            
            # Allow searching in the data table
            search_term = st.text_input("Search in data", "")
            
            # Display data table with search functionality
            if search_term:
                search_results = st.session_state.filtered_data[
                    st.session_state.filtered_data.apply(
                        lambda row: search_term.lower() in str(row).lower(), axis=1
                    )
                ]
                display_data = search_results
            else:
                display_data = st.session_state.filtered_data
            
            # Configure the data grid
            gb = GridOptionsBuilder.from_dataframe(display_data)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            gb.configure_side_bar()
            gb.configure_selection('multiple', use_checkbox=True)
            grid_options = gb.build()
            
            # Display the data grid
            grid_response = AgGrid(
                display_data,
                gridOptions=grid_options,
                data_return_mode='AS_INPUT',
                update_mode='MODEL_CHANGED',
                fit_columns_on_grid_load=False,
                enable_enterprise_modules=True,
                height=500,
                width='100%'
            )
            
            # Add export button
            if st.button("Export Selected Data to CSV"):
                selected_rows = grid_response['selected_rows']
                if selected_rows:
                    selected_df = pd.DataFrame(selected_rows)
                    csv = selected_df.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        csv,
                        "arbitration_data_export.csv",
                        "text/csv",
                        key='download-csv'
                    )
                else:
                    st.warning("No rows selected for export.")
        
        with tab3:
            # AI Query tab
            st.header("AI-Powered Query Interface")
            st.info("This feature allows you to ask natural language questions about the arbitration data.")
            
            query = st.text_area("Enter your query:", height=100, placeholder="Example: How many arbitrations has Arbitrator John Smith had?")
            
            if st.button("Process Query"):
                if query:
                    with st.spinner("Processing your query..."):
                        # Process the natural language query
                        response = process_natural_language_query(query, st.session_state.filtered_data)
                        st.markdown("### Response")
                        st.markdown(response)
                else:
                    st.warning("Please enter a query.")
            
            # Sample queries
            st.markdown("### Sample Queries")
            st.markdown("""
            - How many arbitrations has Arbitrator John L. Smith had?
            - How many times has Arbitrator Jane D. Brown ruled for the complainant?
            - What was the average award given by Arbitrator Robert T. Williams?
            - List the names of all arbitrations handled by Arbitrator Maria A. Johnson.
            - How many times has Arbitrator David M. Taylor ruled for the consumer against AT&T?
            """)

def load_sample_data_to_state():
    """Helper function to load sample data into session state"""
    with st.spinner("Loading sample data..."):
        # Load sample data
        sample_data = load_sample_data()
        
        # Store in session state
        st.session_state.data = sample_data
        st.session_state.filtered_data = sample_data
        
        # Calculate metrics
        analyzer = DataAnalyzer(sample_data)
        st.session_state.metrics = analyzer.calculate_metrics()

if __name__ == "__main__":
    main()