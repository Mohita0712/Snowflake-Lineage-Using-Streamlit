# Snowflake Data Lineage Streamlit App
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import streamlit.components.v1 as components
from pyvis.network import Network
import tempfile
import os
import json
from io import BytesIO

# Initialize session
session = get_active_session()

# =============================================================================
# UTILITY FUNCTIONS FOR DATABASE/SCHEMA/OBJECT OPERATIONS
# =============================================================================

def get_all_databases(session):
    """Get all available databases"""
    try:
        query = "SHOW DATABASES"
        databases_df = session.sql(query).to_pandas()
        
        # Check for different possible column names (including quoted versions)
        possible_columns = ['"name"', 'name', 'NAME', '"NAME"', 'database_name', 'DATABASE_NAME', 
                          '"database_name"', '"DATABASE_NAME"']
        
        for col in possible_columns:
            if col in databases_df.columns:
                return sorted(databases_df[col].tolist())
        
        # If none of the expected columns exist, show what columns are available
        st.error(f"Unexpected column names in SHOW DATABASES result: {list(databases_df.columns)}")
        # Try to use the first column as database name
        if len(databases_df.columns) > 0:
            return sorted(databases_df.iloc[:, 0].tolist())
        return []
    except Exception as e:
        st.error(f"Error getting databases: {str(e)}")
        return []

def get_all_schemas(session, database):
    """Get all schemas in a database"""
    try:
        query = f"SHOW SCHEMAS IN DATABASE {database}"
        schemas_df = session.sql(query).to_pandas()
        
        # Check for different possible column names (including quoted versions)
        possible_columns = ['"name"', 'name', 'NAME', '"NAME"', 'schema_name', 'SCHEMA_NAME', 
                          '"schema_name"', '"SCHEMA_NAME"']
        
        for col in possible_columns:
            if col in schemas_df.columns:
                return sorted(schemas_df[col].tolist())
        
        # If none of the expected columns exist, show what columns are available
        st.error(f"Unexpected column names in SHOW SCHEMAS result: {list(schemas_df.columns)}")
        # Try to use the first column as schema name
        if len(schemas_df.columns) > 0:
            return sorted(schemas_df.iloc[:, 0].tolist())
        return []
    except Exception as e:
        st.error(f"Error getting schemas from {database}: {str(e)}")
        return []

def get_objects(session, database_name: str, schema_name: str, object_type: str = "ALL"):
    """Get list of tables, views, or all objects in a schema"""
    try:
        if object_type == "TABLE":
            query = f"""
            SELECT TABLE_NAME, 'TABLE' as OBJECT_TYPE
            FROM {database_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
        elif object_type == "VIEW":
            query = f"""
            SELECT TABLE_NAME, 'VIEW' as OBJECT_TYPE
            FROM {database_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_TYPE = 'VIEW'
            ORDER BY TABLE_NAME
            """
        else:  # ALL
            query = f"""
            SELECT TABLE_NAME, TABLE_TYPE as OBJECT_TYPE
            FROM {database_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' 
            AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY TABLE_NAME
            """
        
        result_df = session.sql(query).to_pandas()
        if not result_df.empty:
            # Map BASE TABLE to TABLE for consistency
            result_df['OBJECT_TYPE'] = result_df['OBJECT_TYPE'].replace('BASE TABLE', 'TABLE')
            return result_df
        return pd.DataFrame(columns=['TABLE_NAME', 'OBJECT_TYPE'])
    except Exception as e:
        st.error(f"Error fetching objects: {e}")
        return pd.DataFrame(columns=['TABLE_NAME', 'OBJECT_TYPE'])

def get_object_type(session, db_name: str, schema_name: str, object_name: str):
    """Determines if the given object is a TABLE or a VIEW"""
    try:
        table_type_query = f"""
        SELECT TABLE_TYPE
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND TABLE_NAME = '{object_name}'
        """
        table_result_df = session.sql(table_type_query).to_pandas()

        if not table_result_df.empty:
            table_type = table_result_df['TABLE_TYPE'].iloc[0]
            if table_type == 'BASE TABLE':
                return 'TABLE'
            elif table_type == 'VIEW':
                return 'VIEW'
        return None
    except Exception as e:
        st.warning(f"Could not determine object type: {e}")
        return None

# =============================================================================
# LINEAGE ANALYSIS FUNCTIONS
# =============================================================================

def get_lineage_for_single_object(session, object_full_name: str, max_distance: int):
    """Retrieves lineage for a single Snowflake object using SNOWFLAKE.CORE.GET_LINEAGE"""
    parts = object_full_name.strip().split('.')
    
    # Parse the object name
    if len(parts) == 3:
        db_name, schema_name, obj_name = parts[0].upper(), parts[1].upper(), parts[2].upper()
    elif len(parts) == 2:
        schema_name, obj_name = parts[0].upper(), parts[1].upper()
        current_db = session.get_current_database()
        if current_db:
            db_name = current_db.upper()
        else:
            st.error("Could not determine current database. Please provide full 'DATABASE.SCHEMA.OBJECT_NAME'.")
            return None, None, None
    else:
        st.error("Please enter object name in 'SCHEMA.OBJECT_NAME' or 'DATABASE.SCHEMA.OBJECT_NAME' format.")
        return None, None, None

    # Determine object type
    object_type = get_object_type(session, db_name, schema_name, obj_name)
    if object_type is None:
        st.error(f"Could not determine if '{object_full_name}' is a TABLE or VIEW, or object does not exist.")
        return None, None, None

    fully_qualified_object_name = f"{db_name}.{schema_name}.{obj_name}"

    # Query for UPSTREAM dependencies
    upstream_query = f"""
    SELECT
        DISTANCE,
        SOURCE_OBJECT_DOMAIN,
        SOURCE_OBJECT_DATABASE,
        SOURCE_OBJECT_SCHEMA,
        SOURCE_OBJECT_NAME,
        TARGET_OBJECT_DOMAIN,
        TARGET_OBJECT_DATABASE,
        TARGET_OBJECT_SCHEMA,
        TARGET_OBJECT_NAME
    FROM TABLE (SNOWFLAKE.CORE.GET_LINEAGE(
        '{fully_qualified_object_name}',
        '{object_type}',
        'UPSTREAM',
        {max_distance}
    ))
    ORDER BY DISTANCE, SOURCE_OBJECT_NAME
    """

    # Query for DOWNSTREAM dependencies
    downstream_query = f"""
    SELECT
        DISTANCE,
        SOURCE_OBJECT_DOMAIN,
        SOURCE_OBJECT_DATABASE,
        SOURCE_OBJECT_SCHEMA,
        SOURCE_OBJECT_NAME,
        TARGET_OBJECT_DOMAIN,
        TARGET_OBJECT_DATABASE,
        TARGET_OBJECT_SCHEMA,
        TARGET_OBJECT_NAME
    FROM TABLE (SNOWFLAKE.CORE.GET_LINEAGE(
        '{fully_qualified_object_name}',
        '{object_type}',
        'DOWNSTREAM',
        {max_distance}
    ))
    ORDER BY DISTANCE, TARGET_OBJECT_NAME
    """

    try:
        upstream_df = session.sql(upstream_query).to_pandas()
        downstream_df = session.sql(downstream_query).to_pandas()
        return upstream_df, downstream_df, object_type
    except Exception as e:
        st.error(f"Error fetching lineage: {e}")
        st.info("Ensure you have ACCOUNTADMIN role or necessary privileges for SNOWFLAKE.CORE functions.")
        return None, None, None

def get_lineage_for_multiple_objects(session, object_names: list, max_distance: int):
    """Retrieves lineage for multiple Snowflake objects and consolidates results"""
    all_lineage_data = []
    failed_objects = []
    processed_count = 0
    total_count = len(object_names)

    # Create progress tracking
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0)
        status_text = st.empty()

    for obj_name in object_names:
        processed_count += 1
        obj_name = obj_name.strip().upper()
        
        # Update progress
        progress_bar.progress(processed_count / total_count)
        status_text.text(f"Processing lineage for: {obj_name} ({processed_count}/{total_count})")
        
        parts = obj_name.split('.')
        
        if len(parts) == 3:
            db_name, schema_name, current_obj_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            schema_name, current_obj_name = parts[0], parts[1]
            current_db = session.get_current_database()
            if current_db:
                db_name = current_db.upper()
            else:
                st.warning(f"Skipping '{obj_name}': Could not determine current database.")
                failed_objects.append(obj_name)
                continue
        else:
            st.warning(f"Skipping '{obj_name}': Invalid format.")
            failed_objects.append(obj_name)
            continue

        object_type = get_object_type(session, db_name, schema_name, current_obj_name)
        if object_type is None:
            st.warning(f"Skipping '{obj_name}': Could not determine object type.")
            failed_objects.append(obj_name)
            continue

        fully_qualified_object_name = f"{db_name}.{schema_name}.{current_obj_name}"

        # Combined query for both upstream and downstream
        combined_query = f"""
        SELECT
            '{obj_name}' AS QUERIED_OBJECT,
            'UPSTREAM' AS LINEAGE_DIRECTION,
            DISTANCE,
            SOURCE_OBJECT_DOMAIN,
            SOURCE_OBJECT_DATABASE,
            SOURCE_OBJECT_SCHEMA,
            SOURCE_OBJECT_NAME,
            TARGET_OBJECT_DOMAIN,
            TARGET_OBJECT_DATABASE,
            TARGET_OBJECT_SCHEMA,
            TARGET_OBJECT_NAME
        FROM TABLE (SNOWFLAKE.CORE.GET_LINEAGE(
            '{fully_qualified_object_name}',
            '{object_type}',
            'UPSTREAM',
            {max_distance}
        ))
        UNION ALL
        SELECT
            '{obj_name}' AS QUERIED_OBJECT,
            'DOWNSTREAM' AS LINEAGE_DIRECTION,
            DISTANCE,
            SOURCE_OBJECT_DOMAIN,
            SOURCE_OBJECT_DATABASE,
            SOURCE_OBJECT_SCHEMA,
            SOURCE_OBJECT_NAME,
            TARGET_OBJECT_DOMAIN,
            TARGET_OBJECT_DATABASE,
            TARGET_OBJECT_SCHEMA,
            TARGET_OBJECT_NAME
        FROM TABLE (SNOWFLAKE.CORE.GET_LINEAGE(
            '{fully_qualified_object_name}',
            '{object_type}',
            'DOWNSTREAM',
            {max_distance}
        ))
        ORDER BY LINEAGE_DIRECTION, DISTANCE
        """

        try:
            lineage_df = session.sql(combined_query).to_pandas()
            if not lineage_df.empty:
                all_lineage_data.append(lineage_df)
        except Exception as e:
            st.warning(f"Error fetching lineage for '{obj_name}': {e}")
            failed_objects.append(obj_name)

    # Clear progress
    progress_container.empty()

    if all_lineage_data:
        combined_df = pd.concat(all_lineage_data, ignore_index=True)
    else:
        combined_df = pd.DataFrame()

    return combined_df, failed_objects

# =============================================================================
# VISUALIZATION FUNCTIONS
# =============================================================================



def display_lineage_summary(upstream_df: pd.DataFrame, downstream_df: pd.DataFrame):
    """Display summary statistics for lineage analysis"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🔼 Upstream Objects", len(upstream_df['SOURCE_OBJECT_NAME'].unique()) if not upstream_df.empty else 0)
    
    with col2:
        st.metric("🔽 Downstream Objects", len(downstream_df['TARGET_OBJECT_NAME'].unique()) if not downstream_df.empty else 0)
    
    with col3:
        max_upstream_distance = upstream_df['DISTANCE'].max() if not upstream_df.empty else 0
        st.metric("📏 Max Upstream Distance", max_upstream_distance)
    
    with col4:
        max_downstream_distance = downstream_df['DISTANCE'].max() if not downstream_df.empty else 0
        st.metric("📏 Max Downstream Distance", max_downstream_distance)

# =============================================================================
# STREAMLIT UI CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Snowflake Data Lineage Tool", 
    layout="wide", 
    initial_sidebar_state="expanded",
    page_icon="🔄"
)

# Custom CSS for clean, professional styling
st.markdown("""
<style>
    /* Main container styling */
    .main > div {
        padding-left: 1rem;
        padding-right: 1rem;
        font-size: 0.9rem;
        padding-top: 4rem;
    }
    
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        margin-top: 0rem;
    }
    
    /* Header styling */
    h1 {
        text-align: center !important;
        margin-bottom: 0.5rem !important;
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        background: white !important;
        color: black !important;
        z-index: 999 !important;
        padding: 1rem !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1) !important;
        font-size: 2rem !important;
    }
    
    .subtitle {
        text-align: center !important;
        position: fixed !important;
        top: 4rem !important;
        left: 0 !important;
        right: 0 !important;
        background: white !important;
        color: black !important;
        z-index: 998 !important;
        padding: 0.8rem 1rem !important;
        font-size: 1rem !important;
        font-weight: 300 !important;
    }
    /* Input section styling */
    .input-container {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 3px solid #212529;
        margin-bottom: 1rem;
        min-height: 600px;
        max-height: 600px;
        overflow-y: auto;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .output-container {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 3px solid #212529;
        min-height: 600px;
        max-height: 600px;
        overflow-y: auto;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Navigation header */
    .nav-header {
        background: black;
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        text-align: center;
        font-weight: 500;
        font-size: 1rem;
    }
    
    .section-divider {
        margin: 1.5rem 0;
        border-top: 2px solid #dee2e6;
    }
    
    /* Form elements */
    .stSelectbox label, .stMultiSelect label, .stTextInput label, .stNumberInput label {
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        color: #495057 !important;
        margin-bottom: 0.5rem !important;
    }
    
    .stSelectbox > div > div, .stMultiSelect > div > div, .stTextInput > div > div, .stNumberInput > div > div {
        background-color: white !important;
        border: 2px solid #ced4da !important;
        border-radius: 6px !important;
        min-height: 42px !important;
        transition: border-color 0.2s ease !important;
    }
    
    .stSelectbox > div > div:focus-within, .stMultiSelect > div > div:focus-within, 
    .stTextInput > div > div:focus-within, .stNumberInput > div > div:focus-within {
        border-color: #2a5298 !important;
        box-shadow: 0 0 0 0.2rem rgba(42, 82, 152, 0.25) !important;
    }
    
    /* Button styling */
    .stButton button {
        font-size: 1rem !important;
        font-weight: 600 !important;
        padding: 0.6rem 2rem !important;
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
    }
    
    .stButton button:hover {
        background: linear-gradient(90deg, #2a5298 0%, #1e3c72 100%) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 12px rgba(0,0,0,0.2) !important;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        background: linear-gradient(90deg, #f8f9fa 0%, #e9ecef 100%) !important;
        border-radius: 10px 10px 0 0 !important;
        padding: 0.5rem !important;
        margin-top: 20px;
        margin-bottom: 1rem !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: white !important;
        color: #495057 !important;
        border: 2px solid #dee2e6 !important;
        border-radius: 8px !important;
        margin: 0.25rem !important;
        padding: 0.8rem 1.5rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
    }
    
  
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%) !important;
        color: white !important;
        border-color: #1e3c72 !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
    }
    
    /* Metrics styling */
    .stMetric {
        background: white !important;
        padding: 0.5rem !important;
        border-radius: 8px !important;
        border: 1px solid #dee2e6 !important;
        text-align: center !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
    }
    
    .stMetric > div {
        font-weight: 600 !important;
    }
    
    /* DataFrames */
    .stDataFrame {
        border-radius: 8px !important;
        overflow: hidden !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
    }
    
    /* Info, warning, error boxes */
    .stInfo, .stWarning, .stError, .stSuccess {
        border-radius: 8px !important;
        border: none !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%) !important;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# MAIN APPLICATION UI
# =============================================================================

# Page header
st.markdown("# Snowflake Data Lineage Tool")
st.markdown('<p class="subtitle"></p>', 
           unsafe_allow_html=True)

# Create main tabs
tab1, tab2 = st.tabs(["🎯 Single Object Lineage", "📊 Multi-Object Analysis"])

# =============================================================================
# TAB 1: SINGLE OBJECT LINEAGE
# =============================================================================
with tab1:
    
    # Create layout columns
    col_input, col_output = st.columns([3, 7], gap="large")
    
    with col_input:
        st.markdown('<div class="nav-header">OBJECT SELECTION</div>', unsafe_allow_html=True)
        st.markdown("### 🎯Select Object Details")
        
        
        # Database selection
        available_databases = get_all_databases(session)
        selected_db = st.selectbox("Database Name:", [""] + available_databases, key="single_db")
        
        # Schema selection
        if selected_db:
            available_schemas = get_all_schemas(session, selected_db)
            selected_schema = st.selectbox("Schema Name:", [""] + available_schemas, key="single_schema")
        else:
            selected_schema = ""
            st.selectbox("Schema Name:", ["Select Database First"], disabled=True)
        
        # Object selection
        if selected_db and selected_schema:
            objects_df = get_objects(session, selected_db, selected_schema)
            if not objects_df.empty:
                # Create formatted options
                object_options = [""] + [f"{row['TABLE_NAME']} ({row['OBJECT_TYPE']})" 
                                       for _, row in objects_df.iterrows()]
                selected_object_display = st.selectbox("Select Object Name:", object_options, key="single_object")
                
                if selected_object_display and selected_object_display != "":
                    selected_object = selected_object_display.split(" (")[0]
                else:
                    selected_object = ""
            else:
                st.warning(f"⚠️ No tables or views found in {selected_db}.{selected_schema}")
                selected_object = ""
        else:
            selected_object = ""
            st.selectbox("Object Name:", ["Select Schema First"], disabled=True)
        
        
        # Analysis parameters
        st.markdown("### ⚙️ Analysis Parameters")
        max_distance = st.number_input("Maximum Distance:", min_value=1, max_value=5, value=1 ,
                                     help="How many levels deep to trace lineage")
        
        # Action buttons
        analyze_single = st.button("🚀 Analyze Lineage", type="primary", use_container_width=True, key="analyze_single")
        
        if analyze_single and selected_object:
            object_full_name = f"{selected_db}.{selected_schema}.{selected_object}"
            st.session_state['single_analysis'] = {
                'object_name': object_full_name,
                'max_distance': max_distance
            }

    
    with col_output:
        st.markdown('<div class="nav-header">LINEAGE ANALYSIS RESULTS</div>', unsafe_allow_html=True)
        
        # Check if analysis should run
        if 'single_analysis' in st.session_state and st.session_state['single_analysis'] is not None:
            analysis_params = st.session_state['single_analysis']
            object_name = analysis_params['object_name']
            max_dist = analysis_params['max_distance']
            
            with st.spinner(f"🔄 Analyzing lineage for {object_name}..."):
                upstream_df, downstream_df, object_type = get_lineage_for_single_object(session, object_name, max_dist)
                
                if upstream_df is not None or downstream_df is not None:
                    st.success(f"✅ Successfully analyzed lineage for **{object_name}** ({object_type})")
                    
                    # Display summary metrics
                    display_lineage_summary(
                        upstream_df if upstream_df is not None else pd.DataFrame(), 
                        downstream_df if downstream_df is not None else pd.DataFrame()
                    )
                    
                    # Create tabs for different views
                    tab_upstream, tab_downstream = st.tabs(["🔼 Upstream", "🔽 Downstream"])
                    
                    
                    
                    with tab_upstream:
                        if upstream_df is not None and not upstream_df.empty:
                            st.markdown("### 🔼 Upstream Dependencies")
                            st.dataframe(upstream_df, use_container_width=True, hide_index=True)
                            
                            # Download option
                            csv = upstream_df.to_csv(index=False)
                            st.download_button("📥 Download Upstream Data", csv, f"upstream_lineage_{object_name.replace('.', '_')}.csv", "text/csv")
                        else:
                            st.info("🔍 No upstream dependencies found")
                    
                    with tab_downstream:
                        if downstream_df is not None and not downstream_df.empty:
                            st.markdown("### 🔽 Downstream Dependencies")
                            st.dataframe(downstream_df, use_container_width=True, hide_index=True)
                            
                            # Download option
                            csv = downstream_df.to_csv(index=False)
                            st.download_button("📥 Download Downstream Data", csv, f"downstream_lineage_{object_name.replace('.', '_')}.csv", "text/csv")
                        else:
                            st.info("🔍 No downstream dependencies found")
                else:
                    st.error("❌ Failed to analyze lineage. Please check the object name and your permissions.")
                    
            # Clear the analysis state
            st.session_state['single_analysis'] = None
        else:
            st.info("👈 **Get Started:** Select an object and click 'Analyze Lineage' to view results here.")
        

# =============================================================================
# TAB 2: MULTI-OBJECT ANALYSIS
# =============================================================================
with tab2:
    
    # Create layout columns
    col_input2, col_output2 = st.columns([3, 7], gap="large")
    
    with col_input2:
        st.markdown('<div class="nav-header">MULTI-OBJECT SELECTION</div>', unsafe_allow_html=True)
        
        # Sample file download section
        st.markdown("### 📥 Download Sample Template")
        st.markdown("""
        1. **Download** the template below
        2. **Fill in** your database objects (replace sample data with your actual objects)
        3. **Upload** the completed file to analyze lineage
        """)
        
        # Create sample data
        sample_data = {
            'DATABASE_NAME': [
                'SNOWFLAKE_SAMPLE_DATA', 
                'SNOWFLAKE_SAMPLE_DATA'
            ],
            'SCHEMA_NAME': [
                'TPCH_SF1', 
                'TPCH_SF1'
            ],
            'OBJECT_TYPE': [
                'TABLE', 
                'VIEW'
            ],
            'OBJECT_NAME': [
                'CUSTOMER', 
                'CUSTOMER_VIEW'
            ]
        }
        sample_df = pd.DataFrame(sample_data)
        
      
        # Excel template download
        excel_buffer = BytesIO()
        try:
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                sample_df.to_excel(writer, sheet_name='Lineage_Objects', index=False)
            
            # Get the data AFTER the writer is closed
            excel_buffer.seek(0)
            excel_data = excel_buffer.getvalue()
            
            st.download_button(
                label="📊 Download Excel Template",
                data=excel_data,
                file_name="lineage_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                help="Download Excel template file"
            )
                
        except Exception as e:
            st.error(f"Error creating Excel file: {str(e)}")
            st.info("Please try again or contact support if the issue persists.")
        
        
        uploaded_file = st.file_uploader(
            "📂 Upload CSV/Excel file:", 
            type=['csv', 'xlsx', 'xls'],
            help="File should contain columns: DATABASE_NAME, SCHEMA_NAME, OBJECT_TYPE, OBJECT_NAME"
        )
        
        object_names_from_file = []
        
        if uploaded_file is not None:
            try:
                # Read file based on extension
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:  # Excel files
                    df = pd.read_excel(uploaded_file)
                
                # Validate required columns
                required_columns = ['DATABASE_NAME', 'SCHEMA_NAME', 'OBJECT_NAME']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"❌ Missing required columns: {', '.join(missing_columns)}")
                    st.info("Required columns: DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME")
                else:
                    st.success(f"✅ File uploaded successfully! Found {len(df)} objects")
                   
                    
                    # Create object names list
                    object_names_from_file = []
                    for _, row in df.iterrows():
                        object_name = f"{row['DATABASE_NAME']}.{row['SCHEMA_NAME']}.{row['OBJECT_NAME']}"
                        object_names_from_file.append(object_name)
                    
                    
            except Exception as e:
                st.error(f"❌ Error reading file: {str(e)}")
                st.info("Please ensure your file has the correct format and columns")

        
        # Analysis parameters
        st.markdown("### ⚙️ Analysis Parameters")
        multi_max_distance = st.number_input("Maximum Distance:", min_value=1, max_value=5, value=1, 
                                           help="Maximum lineage distance for each object", key="multi_distance")
        
        # Action button
        analyze_multi = st.button("🚀 Analyze Objects", type="primary", use_container_width=True, key="analyze_multi")
        
        if analyze_multi:
            # Determine which source to use
            if object_names_from_file:
                # Use file upload
                st.session_state['multi_analysis'] = {
                    'object_names': object_names_from_file,
                    'max_distance': multi_max_distance,
                    'source': 'file'
                }
            elif object_list_input.strip():
                # Use manual input
                object_names = [name.strip() for name in object_list_input.strip().split('\n') if name.strip()]
                if object_names:
                    st.session_state['multi_analysis'] = {
                        'object_names': object_names,
                        'max_distance': multi_max_distance,
                        'source': 'manual'
                    }
            else:
                st.warning("⚠️ Please either upload a file or enter object names manually")
        
    
    with col_output2:
        st.markdown('<div class="nav-header">MULTI-OBJECT ANALYSIS RESULTS</div>', unsafe_allow_html=True)
        
        # Check if analysis should run
        if 'multi_analysis' in st.session_state and st.session_state['multi_analysis'] is not None:
            analysis_params = st.session_state['multi_analysis']
            object_names = analysis_params['object_names']
            max_dist = analysis_params['max_distance']
            
            st.markdown(f"**📋 Analysis Summary:** {len(object_names)} objects selected")
            
            with st.spinner(f"🔄 Analyzing lineage for {len(object_names)} objects..."):
                combined_df, failed_objects = get_lineage_for_multiple_objects(session, object_names, max_dist)
                
                if not combined_df.empty or failed_objects:
                    # Success/failure summary
                    success_count = len(object_names) - len(failed_objects)
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("✅ Successful", success_count)
                    with col2:
                        st.metric("❌ Failed", len(failed_objects))
                    with col3:
                        st.metric("📊 Total Lineage Records", len(combined_df))
                    
                    if failed_objects:
                        st.warning(f"⚠️ Failed to process: {', '.join(failed_objects)}")
                    
                    if not combined_df.empty:
                        # Create tabs for different views
                        tab_summary, tab_details = st.tabs(["📈 Summary", "📋 All Records"])
                        
                        with tab_summary:
                            st.markdown("### 📈 Lineage Summary")
                            
                            # Summary statistics
                            upstream_count = len(combined_df[combined_df['LINEAGE_DIRECTION'] == 'UPSTREAM'])
                            downstream_count = len(combined_df[combined_df['LINEAGE_DIRECTION'] == 'DOWNSTREAM'])
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("🔼 Upstream Records", upstream_count)
                            with col2:
                                st.metric("🔽 Downstream Records", downstream_count)
                            with col3:
                                unique_sources = combined_df['SOURCE_OBJECT_NAME'].nunique()
                                st.metric("📤 Unique Sources", unique_sources)
                            with col4:
                                unique_targets = combined_df['TARGET_OBJECT_NAME'].nunique()
                                st.metric("📥 Unique Targets", unique_targets)
                            
                            # Object-level summary
                            st.markdown("#### 🎯 Per-Object Summary")
                            object_summary = combined_df.groupby(['QUERIED_OBJECT', 'LINEAGE_DIRECTION']).size().unstack(fill_value=0)
                            object_summary['Total'] = object_summary.sum(axis=1)
                            st.dataframe(object_summary, use_container_width=True)
                        
                        with tab_details:
                            st.markdown("### 📋 Detailed Lineage Records")
                            st.dataframe(combined_df, use_container_width=True, hide_index=True)
                            
                            from io import BytesIO
                            output = BytesIO()
                            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                combined_df.to_excel(writer, sheet_name='Lineage_Analysis', index=False)
                            excel_data = output.getvalue()
                            st.download_button("📊 Download Excel", excel_data, "multi_object_lineage.xlsx", 
                                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        
                        
                        
                    else:
                        st.warning("⚠️ No lineage data found for the analyzed objects")
                else:
                    st.error("❌ Failed to analyze lineage for any of the specified objects")
            
            # Clear the analysis state
            st.session_state['multi_analysis'] = None
        else:
            st.info("👈 **Get Started:** Enter object names and click 'Analyze Multiple Objects' to view results here.")
        
        st.markdown('</div>', unsafe_allow_html=True)

# =============================================================================
# FOOTER
# =============================================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem; font-size: 0.9rem;'>
    🔄 <strong>Snowflake Data Lineage Tool</strong> | 
    Built with Streamlit and Snowpark | 
    Powered by SNOWFLAKE.CORE.GET_LINEAGE
</div>
""", unsafe_allow_html=True)

# Initialize session state variables if not present
if 'single_analysis' not in st.session_state:
    st.session_state['single_analysis'] = None
if 'multi_analysis' not in st.session_state:
    st.session_state['multi_analysis'] = None
