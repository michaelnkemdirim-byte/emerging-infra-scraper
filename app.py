import streamlit as st
import pandas as pd
import os
import datetime as dt
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import subprocess
import time

st.set_page_config(
    page_title="Emerging Infrastructure Dashboard",
    page_icon="🏗️",
    layout="wide"
)

# Data file path
DATA_FILE = Path(__file__).parent / "combined_data.csv"
MASTER_SCRAPER = Path(__file__).parent / "master_scraper.py"

# Session state for scraper status
if 'scraper_running' not in st.session_state:
    st.session_state.scraper_running = False
if 'scraper_process' not in st.session_state:
    st.session_state.scraper_process = None
if 'scraper_start_time' not in st.session_state:
    st.session_state.scraper_start_time = None

st.title("🏗️ African Infrastructure News Dashboard")
st.markdown("Infrastructure project news from Burkina Faso, Ethiopia, Ghana, Kenya, Nigeria, Rwanda, South Africa, and Tanzania")

# Scraper control section
st.markdown("---")
scraper_col1, scraper_col2 = st.columns([3, 1])

with scraper_col1:
    # Check if scraper is still running
    if st.session_state.scraper_running and st.session_state.scraper_process:
        poll_result = st.session_state.scraper_process.poll()
        if poll_result is not None:
            # Process completed - clear cache and reload data
            st.session_state.scraper_running = False
            st.session_state.scraper_process = None
            elapsed = time.time() - st.session_state.scraper_start_time
            st.cache_data.clear()  # Clear cache to reload new data
            st.success(f"✅ Scraping completed in {elapsed:.1f} seconds! Showing new data...")
            time.sleep(1)  # Brief pause to show success message
            st.rerun()  # Auto-reload with new data
        else:
            # Still running
            elapsed = time.time() - st.session_state.scraper_start_time
            minutes = int(elapsed // 60)
            seconds = int(elapsed % 60)
            if minutes > 0:
                time_str = f"{minutes}m {seconds}s"
            else:
                time_str = f"{seconds}s"
            st.info(f"⏳ Scraper is running... ({time_str} elapsed)")
            time.sleep(30)  # Check every 30 seconds
            st.rerun()  # Keep checking status
    else:
        if DATA_FILE.exists():
            last_modified = dt.datetime.fromtimestamp(DATA_FILE.stat().st_mtime)
            st.info(f"📅 Data last updated: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.warning("⚠️ No data file found. Run the scraper to collect data.")

with scraper_col2:
    if st.button("🔄 Run Scraper", disabled=st.session_state.scraper_running, use_container_width=True):
        # Start scraper in background
        try:
            process = subprocess.Popen(
                ['python3', str(MASTER_SCRAPER)],
                cwd=str(MASTER_SCRAPER.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True  # Detach from parent process
            )
            st.session_state.scraper_running = True
            st.session_state.scraper_process = process
            st.session_state.scraper_start_time = time.time()
            st.rerun()
        except Exception as e:
            st.error(f"❌ Failed to start scraper: {e}")

# Sidebar
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    This dashboard displays infrastructure news articles scraped from official government portals,
    international development banks, and news sources across 8 African countries.

    **Data Pipeline:**
    - Automated web scraping from 29+ sources
    - French/Amharic to English translation
    - AI-powered categorization (Claude Haiku 3.5)
    - URL + Title deduplication
    - Infrastructure-only filtering

    **Categories:**
    - **Port**: Maritime, shipping, airport infrastructure
    - **Rail**: Railway, metro, train projects
    - **Highway**: Roads, highways, bridges
    - **SEZ**: Special Economic Zones, industrial parks
    - **Smart City**: Digital infrastructure, urban tech
    - **Infrastructure**: General infrastructure projects
    """)


# Load data
@st.cache_data
def load_data():
    """Load combined_data.csv with caching"""
    if not DATA_FILE.exists():
        return None

    df = pd.read_csv(DATA_FILE)

    # Convert date_iso to datetime
    if 'date_iso' in df.columns:
        df['date_iso'] = pd.to_datetime(df['date_iso'], errors='coerce')

    return df

# Auto-start scraper if no data file exists
if not DATA_FILE.exists() and not st.session_state.scraper_running:
    st.warning("⚠️ No data file found. Starting scraper automatically...")
    try:
        process = subprocess.Popen(
            ['python3', str(MASTER_SCRAPER)],
            cwd=str(MASTER_SCRAPER.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        st.session_state.scraper_running = True
        st.session_state.scraper_process = process
        st.session_state.scraper_start_time = time.time()
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"❌ Failed to auto-start scraper: {e}")
        st.info("💡 Please run manually: python master_scraper.py")
        st.stop()

# Load the data
df = load_data()

# Check if data exists (might be None if scraper is still running)
if df is None or len(df) == 0:
    if st.session_state.scraper_running:
        # Scraper is running, show message and continue checking
        st.info("📊 Data is being scraped. Please wait...")
    else:
        st.error("❌ No data found! Click 'Run Scraper' button above to collect data.")
    st.stop()

# Main content
st.markdown("---")
st.header("📊 Dashboard Overview")

# Summary statistics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Articles", len(df))
with col2:
    st.metric("Countries", df['country'].nunique())
with col3:
    st.metric("Sources", df['source'].nunique())

# Breakdown charts
st.subheader("📈 Data Breakdown")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("**By Country**")
    country_counts = df['country'].value_counts().reset_index()
    country_counts.columns = ['country', 'count']

    # Interactive horizontal bar chart
    fig_country = px.bar(
        country_counts,
        y='country',
        x='count',
        orientation='h',
        labels={'count': 'Number of Articles', 'country': 'Country'},
        color='count',
        color_continuous_scale='Blues',
        height=400
    )
    fig_country.update_layout(
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'},
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_country, use_container_width=True)

with chart_col2:
    st.markdown("**By Category**")
    category_counts = df['category'].value_counts().reset_index()
    category_counts.columns = ['category', 'count']

    # Interactive pie chart
    fig_category = px.pie(
        category_counts,
        values='count',
        names='category',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3,
        height=400
    )
    fig_category.update_traces(textposition='inside', textinfo='percent+label')
    fig_category.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.02)
    )
    st.plotly_chart(fig_category, use_container_width=True)

# Timeline chart
st.markdown("**Articles Over Time**")
if 'date_iso' in df.columns:
    # Group by date
    df_timeline = df.copy()
    df_timeline['date'] = pd.to_datetime(df_timeline['date_iso'], errors='coerce')
    df_timeline = df_timeline.dropna(subset=['date'])

    # Count articles per day
    timeline_counts = df_timeline.groupby(df_timeline['date'].dt.date).size().reset_index()
    timeline_counts.columns = ['date', 'count']

    # Interactive line chart
    fig_timeline = px.line(
        timeline_counts,
        x='date',
        y='count',
        labels={'count': 'Number of Articles', 'date': 'Date'},
        markers=True,
        height=300
    )
    fig_timeline.update_traces(line_color='#1f77b4', line_width=2, marker=dict(size=6))
    fig_timeline.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_title='',
        yaxis_title='Articles Published',
        hovermode='x unified'
    )
    st.plotly_chart(fig_timeline, use_container_width=True)
else:
    st.warning("No date information available for timeline chart")

# Filterable table
st.markdown("---")
st.subheader("🔍 Filterable Data Table")

# Filters
filter_col1, filter_col2 = st.columns(2)

with filter_col1:
    country_filter = st.multiselect(
        "Filter by Country",
        options=sorted(df['country'].unique()),
        default=[]
    )

with filter_col2:
    category_filter = st.multiselect(
        "Filter by Category",
        options=sorted(df['category'].unique()),
        default=[]
    )

# Apply filters
filtered_df = df.copy()

if country_filter:
    filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]

if category_filter:
    filtered_df = filtered_df[filtered_df['category'].isin(category_filter)]

# Search box
search_term = st.text_input("🔎 Search in titles and summaries", "")
if search_term:
    mask = (
        filtered_df['title'].str.contains(search_term, case=False, na=False) |
        filtered_df['summary'].str.contains(search_term, case=False, na=False)
    )
    filtered_df = filtered_df[mask]

st.info(f"Showing {len(filtered_df)} of {len(df)} total records")

# Display table
st.dataframe(
    filtered_df,
    use_container_width=True,
    height=400,
    column_config={
        "url": st.column_config.LinkColumn("URL"),
        "date_iso": st.column_config.DateColumn("Date"),
    }
)

# Export section
st.markdown("---")
st.subheader("💾 Export Data")

export_col1, export_col2 = st.columns(2)

with export_col1:
    # CSV export (full dataset)
    csv_data = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Data (CSV)",
        data=csv_data,
        file_name=f"infrastructure_data_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )

with export_col2:
    # Excel export
    try:
        from io import BytesIO
        import openpyxl

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            filtered_df.to_excel(writer, index=False, sheet_name='Infrastructure Data')

        excel_data = buffer.getvalue()

        st.download_button(
            label="📊 Download Filtered Data (Excel)",
            data=excel_data,
            file_name=f"infrastructure_data_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    except ImportError:
        st.warning("Install openpyxl for Excel export: `pip install openpyxl`")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 0.9em;'>
        Made with Streamlit | Data from official government portals, AfDB, World Bank, and news sources
    </div>
    """,
    unsafe_allow_html=True
)
