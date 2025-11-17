# Snowflake Data Lineage Tool

A comprehensive Streamlit application for visualizing and analyzing data lineage in Snowflake environments using the `SNOWFLAKE.CORE.GET_LINEAGE` function.

## Features

### Single Object Lineage
- Analyze upstream and downstream dependencies for individual tables or views
- Interactive network graph visualization
- Detailed tabular reports with download options
- Configurable lineage distance (1-10 levels)

### Multi-Object Analysis
- Batch analysis of multiple objects simultaneously
- Consolidated reporting across objects
- Per-object breakdown and summary statistics
- Progress tracking for large datasets

## Installation & Setup

### Prerequisites
- Snowflake account with appropriate privileges
- Python 3.8 or higher
- ACCOUNTADMIN role (recommended for full lineage access)

### Required Python Packages

```bash
pip install streamlit
pip install snowflake-snowpark-python
pip install pandas
pip install pyvis
```

### Alternative: Install via requirements file

Create a `requirements.txt` file:
```
streamlit>=1.28.0
snowflake-snowpark-python>=1.12.0
pandas>=2.0.0
pyvis>=0.3.2
```

Then install:
```bash
pip install -r requirements.txt
```
### Running the Application

**In Snowflake (Snowsight):**
   - Upload the `snowflake_data_lineage_app.py` to your Snowflake stage
   - Run as a Streamlit app in Snowflake

### Required Snowflake Permissions

The application requires access to:
- `SNOWFLAKE.CORE.GET_LINEAGE()` function
- `INFORMATION_SCHEMA` views
- `SHOW DATABASES` and `SHOW SCHEMAS` commands

### Quick Explorer Analysis
1. Browse to database and schema
2. Select multiple objects using checkboxes
3. Set distance and click "Quick Analyze Selected"

## License

This tool is provided as-is for educational and internal business use. Ensure compliance with your organization's Snowflake licensing and data governance policies.
