# African Infrastructure News Scraper

A production-ready web scraping system that collects infrastructure project news from **8 African countries** across **44+ sources**, with automatic translation, AI-powered categorization, and an interactive Streamlit dashboard.

## Features

- **8 Countries**: Burkina Faso, Ethiopia, Ghana, Kenya, Nigeria, Rwanda, South Africa, Tanzania
- **44+ Sources**: Government portals, development banks (AfDB, World Bank), news outlets
- **Auto Translation**: French/Amharic → English
- **AI Categorization**: Claude Sonnet 4.5 categorizes into Port, Rail, Highway, SEZ, Smart City, Economic, Energy, Technology, Infrastructure
- **7-Day Filter**: Only collects articles from the last 7 days for recent, actionable insights
- **Smart Deduplication**: URL + Title hash prevents duplicates
- **Headless Browser**: Patchright/Playwright for JS-heavy sites
- **Dashboard**: One-click scraping, filters, charts, CSV/Excel export
- **Streamlit Cloud Ready**: Auto-install Chromium on deployment

---

## Installation

### Windows Installation

#### Prerequisites
1. **Python 3.11 or higher**
   - Download from [python.org](https://www.python.org/downloads/)
   - During installation, **check "Add Python to PATH"**
   - Verify installation:
     ```cmd
     python --version
     ```

#### Step 1: Download the Project

1. Extract the project folder to `C:\Users\YourName\Documents\emerging_infra_scraper`
2. Open Command Prompt in that folder (right-click folder → "Open in Terminal")

#### Step 2: Install Dependencies

```cmd
pip install --upgrade pip
pip install -r requirements.txt
```

#### Step 3: Install Chromium Browser

```cmd
python -m patchright install chromium
```

#### Step 4: Configure API Key

1. Get your Anthropic API key from [console.anthropic.com](https://console.anthropic.com/)

2. Create `secrets.toml` file in the project folder:
   ```cmd
   notepad secrets.toml
   ```

3. Add this content:
   ```toml
   anthropicAPI = "sk-ant-api03-YOUR_KEY_HERE"
   ```

4. Save and close Notepad

#### Step 5: Run

**Windows:**
- Scrape: Double-click `Startscrape.bat`
- Dashboard: Double-click `StartDashboard.bat`

**Command Line:**
```cmd
python master_scraper.py    # Scrape all sources
streamlit run app.py         # Launch dashboard
```

---

## Project Structure

```
emerging_infra_scraper/
├── master_scraper.py              # Main orchestrator
├── app.py                         # Streamlit dashboard
├── requirements.txt               # Dependencies
├── secrets.toml                   # API keys (create manually)
├── Startscrape.bat                # Windows: Run scraper
├── StartDashboard.bat             # Windows: Launch dashboard
├── contryModules/                 # Country scrapers (8 countries, 44 sources)
│   ├── BurkinaFaso/
│   ├── Ethiopia/
│   ├── Ghana/
│   ├── Kenya/
│   ├── Nigeria/
│   ├── Rwanda/
│   ├── South_Africa/
│   └── Tanzania/
├── combined_data.csv              # Output (auto-generated)
└── README.md
```

---

## Usage

```bash
# Scrape all sources (translation + AI categorization)
python master_scraper.py

# Launch dashboard
streamlit run app.py
```

### Dashboard Features
- **Manual scraping**: Click "🔄 Run Scraper" button to start (5-15 min, auto-refresh when done)
- **7-day window**: Automatically displays only articles from last 7 days
- **Filters**: Filter by country/category, search keywords in titles/summaries
- **Visualizations**: Interactive charts for country distribution, categories, timeline
- **Export**: Download filtered data as CSV or Excel

---

## Data Schema

| Column | Type | Example |
|--------|------|---------|
| `country` | String | `Kenya` |
| `source` | String | `kenha.co.ke` |
| `title` | String | `Nairobi-Mombasa Highway Upgrade` |
| `date_iso` | YYYY-MM-DD | `2025-03-15` |
| `summary` | String | `The 440km expressway connecting...` |
| `url` | URL | `https://kenha.co.ke/article/123` |
| `category` | Port/Rail/Highway/SEZ/Smart City/Economic/Energy/Technology/Infrastructure | `Highway` |

Output: `combined_data.csv`

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "python is not recognized" (Windows) | Reinstall Python with "Add to PATH" checked |
| "ModuleNotFoundError" | `pip install -r requirements.txt` |
| "Browser not installed" | `python -m patchright install chromium` |
| "API key not found" | Check `secrets.toml` exists, key starts with `sk-ant-api03-` |
| No data scraped | Check internet, review `master_scraper.py` logs |
| Translation not working | Requires internet for Google Translate API |
| Streamlit Cloud deployment | Chromium auto-installs on first run (2-5 min)

---

## Best Practices

- **Run once daily**: System collects last 7 days of data, updated daily is sufficient
- **Export regularly**: Archive `combined_data.csv` to track historical trends
- **Auto-deduplication**: URL + Title hash prevents duplicates automatically
- **Data freshness**: Dashboard displays only articles from last 7 days

---

## Files

**Required:**
- `secrets.toml` - API keys (create manually)
- `requirements.txt` - Dependencies

**Auto-generated:**
- `combined_data.csv` - Final output

**Documentation:**
- `README.md` - This file
