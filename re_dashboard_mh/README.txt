Quick start (local)

1) Start DB + API:
   cd ~/Documents/GRID_INDIA/re_dashboard_mh_project/re_dashboard_mh
   ls -l docker-compose.yml
   docker-compose up -d

2) Ingest:
   - Put MH_sites.csv next to ingest script (or set SITES_CSV env)
   - Put MH.zip into ./incoming (or set WATCH_DIR env)

   cd ingest
   pip install -r requirements.txt
   export DATABASE_URL="postgresql+psycopg2://reuser:repass@localhost:5432/redb"
   export SITES_CSV="../MH_sites.csv"
   export WATCH_DIR="../incoming"
   python ingest_mh_zip.py

3) Dashboard:
   cd dashboard
   pip install -r requirements.txt
   export API_BASE="http://localhost:8000"
   export SHAPEFILE_PATH="../data/MAHARASHTA_State.shp"
   streamlit run app.py
