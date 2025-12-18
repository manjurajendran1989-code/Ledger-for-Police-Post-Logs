import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date
import altair as alt
from mysql.connector import pooling

# 1. DATABASE CONNECTION SETUP
dbconfig = {
    "host": "localhost",
    "user": "root", 
    "password": "Krithik@1229",
    "database": "police_checkpost"
}   

# Initialize the pool
@st.cache_resource
def init_pool():
    return pooling.MySQLConnectionPool(pool_name="stpool", pool_size=5, **dbconfig)

connection_pool = init_pool()

def run_query(sql: str, params: tuple = ()):
    conn = connection_pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows)
    finally:
        conn.close() # Always return connection to pool

# Set page config
st.set_page_config(
    page_title="Checkpost Dashboard",
    page_icon="🚔",
    layout="wide"
)

# Custom CSS for aesthetics
st.markdown("""
<style>
    .main-header { font-size: 2.5em; color: #e377c2; text-align: center; margin-bottom: 20px; }
    .metric-card {
        background-image: linear-gradient(90deg, #0066cc, #66ffff);
        padding: 15px; border-radius: 10px; text-align: center; color: white;
    }
</style>
""", unsafe_allow_html=True)

# 2. HEADER METRICS
try:
    total_stops = run_query("SELECT COUNT(*) as count FROM checkpost_stops")['count'][0]
    total_arrests = run_query("SELECT COUNT(*) as count FROM checkpost_stops WHERE is_arrested = 1")['count'][0]
    avg_age = run_query("SELECT AVG(driver_age) as avg_age FROM checkpost_stops")['avg_age'][0]
except:
    total_stops, total_arrests, avg_age = 0, 0, 0

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f'<div class="metric-card"><h3>Total Stops</h3><h2>{total_stops:,}</h2></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><h3>Total Arrests</h3><h2>{total_arrests:,}</h2></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><h3>Avg Driver Age</h3><h2>{avg_age:.1f}</h2></div>', unsafe_allow_html=True)

st.markdown('<h1 class="main-header">🚔 Police Checkpost Dashboard</h1>', unsafe_allow_html=True)

# SIDEBAR NAVIGATION
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Project Introduction", "Data Visualisation", "SQL Queries" , "Creator Info"])

# PAGE LOGIC
if page == "Project Introduction":
    st.title("Traffic Police Checkpost Data Analysis") 
    st.markdown("""
    This application analyzes traffic stop data collected at police checkposts. 
    **Key Features:**
    - 📊 **Data Dashboard:** View processed records.
    - 🔍 **Search Analysis:** Insights into search outcomes.
    - 🚔 **Violation Tracking:** Categorization of traffic offenses.
    """)

elif page == "Data Visualisation":
    st.title("📊 Check Post Data Visualizer")
    
    with st.sidebar:
        st.header("Filters")
        start_date = st.date_input("Start date", value=date(2005, 1, 1))
        end_date = st.date_input("End date", value=date.today())
        
        gender = st.selectbox("Driver gender", options=["All", "M", "F"]) 
        violation_list = ["All"] + run_query("SELECT DISTINCT violation FROM checkpost_stops")['violation'].tolist()
        violation = st.selectbox("Violations", options=violation_list)
        search_flag = st.selectbox("Search conducted", options=["All", "True", "False"])
        country_list = ["All"] + run_query("SELECT DISTINCT country_name FROM checkpost_stops")['country_name'].tolist()
        country = st.selectbox("Country", options=country_list)
        run = st.button("Run Filtered Query")

    query = "SELECT * FROM checkpost_stops WHERE stop_date BETWEEN %s AND %s"
    params = [start_date, end_date]
    
    if gender != "All":
        query += " AND driver_gender = %s"
        params.append(gender)
    if violation != "All":
        query += " AND violation = %s"
        params.append(violation)
    if search_flag != "All":
        query += " AND search_conducted = %s"
        params.append(1 if search_flag == "True" else 0)
    if country != "All":
        query += " AND country_name = %s"
        params.append(country)

    query += " ORDER BY stop_date DESC limit 200"
    df = run_query(query, tuple(params)) if run else run_query("SELECT * FROM checkpost_stops ORDER BY stop_date DESC LIMIT 200")

    st.subheader(f"Results ({len(df)} records found)")
    st.dataframe(df, use_container_width=True)

    if not df.empty:
        st.divider()
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('count()', title='Number of Stops'),
            y=alt.Y('violation:N', sort='-x'),
            color='driver_gender:N'
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

elif page == "SQL Queries":
    st.title("Run Custom SQL Queries")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🚗 Vehicle", "🧑 Demographic", "⏱ Time", "⚖️ Violation", "🌍 Location", "🔬 Complex"])

    with tab1:
        st.subheader("🚗 Vehicle-Based Analytics 🚗")
        q_v = st.selectbox("Choose query:", ["Top 10 vehicles in drug-related stops", "Vehicles most frequently searched"], key="v_q")
       
        if q_v == "Top 10 vehicles in drug-related stops":
            sql = "SELECT vehicle_number, COUNT(*) AS drug_stop_count FROM checkpost_stops WHERE drugs_related_stop = 1 GROUP BY vehicle_number ORDER BY drug_stop_count DESC LIMIT 10"
            st.table(run_query(sql))
            
            
        elif q_v == "Vehicles most frequently searched":
            sql = "SELECT vehicle_number, COUNT(*) AS searches FROM checkpost_stops WHERE search_conducted = 1 GROUP BY vehicle_number ORDER BY searches DESC LIMIT 20"
            st.table(run_query(sql))
           

    with tab2:
        st.subheader(" 🧑 Demographic-Based Analytics 🧑")
        q_d = st.selectbox("Choose query:", ["Age group with highest arrest rate","Gender distribution per country", "Race + Gender with highest search rate"], key="d_q")
     
        if q_d == "Age group with highest arrest rate":
            sql = """SELECT CASE 
                    WHEN driver_age < 20 THEN 'Under 20' WHEN driver_age BETWEEN 20 AND 29 THEN '20-29'
                    WHEN driver_age BETWEEN 30 AND 39 THEN '30-39' ELSE '40+' END AS age_group,
                    COUNT(*) as total_stops, ROUND(AVG(is_arrested)*100, 2) as arrest_rate FROM checkpost_stops GROUP BY age_group ORDER BY arrest_rate DESC"""
        #   bar chart
            df_age = run_query(sql)
            st.bar_chart(df_age, x="age_group", y="arrest_rate")
            st.table(df_age)
            
        elif q_d == "Gender distribution per country":
             sql = "SELECT country_name, driver_gender, COUNT(*) as total_stops FROM checkpost_stops GROUP BY country_name, driver_gender ORDER BY country_name"     
             st.table(run_query(sql))

        elif q_d == "Race + Gender with highest search rate":
            sql = "SELECT driver_race, driver_gender, COUNT(*) as total_stops, ROUND(AVG(search_conducted)*100, 2) as search_rate FROM checkpost_stops GROUP BY driver_race, driver_gender ORDER BY search_rate DESC"
            st.table(run_query(sql))
    
    with tab3:
        st.subheader("⏱ Time-Based Analytics ⏱")
        q_t = st.selectbox("Choose query:", ["Hour of day with most stops","Average stop duration per violation","Are night stops more likely to lead to arrests?"], key="t_q")
        
        if q_t == "Hour of day with most stops":
            sql = "SELECT DATE_FORMAT(stop_time, '%l %p') AS hour_display, COUNT(*) AS stop_count FROM checkpost_stops WHERE stop_time IS NOT NULL GROUP BY hour_display ORDER BY stop_count DESC"
            st.table(run_query(sql))
            
        elif q_t == "Average stop duration per violation":
            sql = """SELECT violation, AVG( CASE  
                    WHEN stop_duration = '0-15 Min' THEN 7.5
                    WHEN stop_duration = '16-30 Min' THEN 23.0
                    WHEN stop_duration = '30+ Min' THEN 45.0
                    ELSE NULL END) AS avg_duration_minutes
                    FROM checkpost_stops GROUP BY violation ORDER BY avg_duration_minutes DESC"""
            st.table(run_query(sql))
        elif q_t == "Are night stops more likely to lead to arrests?":
            
            sql = """SELECT CASE WHEN 
                    HOUR(stop_time) >= 6 AND HOUR(stop_time) < 18 THEN 'Daytime (6AM-6PM)'
                    ELSE 'Nighttime (6PM-6AM)' END AS period,
                    COUNT(*) AS total_stops,
                    SUM(is_arrested) AS arrest_count,
                    ROUND((SUM(is_arrested) / COUNT(*)) * 100, 2) AS arrest_rate
                    FROM checkpost_stops GROUP BY period"""
            st.table(run_query(sql))
            df2 = run_query(sql)    
            st.bar_chart(df2, x="period", y="arrest_rate")
            
    with tab4:
        
        st.subheader("⚖️ Violation-Based Analytics")
        q_vio = st.selectbox( "Choose query:", ["Violations linked to searches/arrests", "Violations common among <25 drivers", "Violations with almost no searches/arrests"], key="vio_q")

        if q_vio == "Violations linked to searches/arrests":
            sql = """  SELECT violation,COUNT(*) AS total_stops, SUM(CASE WHEN search_conducted = 1 OR is_arrested = 1 THEN 1 ELSE 0 END) AS search_arrest_count,
                       ROUND(SUM(CASE WHEN search_conducted = 1 OR is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS risk_rate
                       FROM checkpost_stops
                       WHERE violation IS NOT NULL
                       GROUP BY violation
                       ORDER BY risk_rate DESC;
                    """
            st.table(run_query(sql))
                      
        elif q_vio == "Violations common among <25 drivers":
             sql = """ SELECT violation, COUNT(*) AS frequency
                      FROM checkpost_stops
                      WHERE driver_age < 25 AND violation IS NOT NULL
                     GROUP BY violation
                     ORDER BY frequency DESC;
                    """
             df_young = run_query(sql)
             st.write("### Frequency of Violations (Drivers < 25)")
             st.bar_chart(df_young, x="violation", y="frequency")
            
        elif q_vio == "Violations with almost no searches/arrests":
            sql = """ SELECT violation, COUNT(*) AS total_stops,
                      SUM(CASE WHEN search_conducted = TRUE OR is_arrested = TRUE THEN 1 ELSE 0 END) AS incident_count,
                      ROUND(SUM(CASE WHEN search_conducted = TRUE OR is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS incident_rate
                      FROM checkpost_stops WHERE violation IS NOT NULL GROUP BY violation HAVING total_stops >= 50 ORDER BY incident_rate ASC;
                  """
        df2 = run_query(sql)
        st.table(df2)
                
    with tab5:
        
        st.subheader("Location-Based Analytics")
        q_loc = st.selectbox("Choose query:", [
        "Countries with highest drug-related stops",
        "Arrest rate by country and violation",
        "Countries with most searches"
        ], key="location_q")

        if q_loc == "Countries with highest drug-related stops":
            sql = """ SELECT country_name, COUNT(*) AS total_stops, SUM(drugs_related_stop = 1) AS drug_stops,ROUND(100.0 * SUM(drugs_related_stop = 1) / NULLIF(COUNT(*),0),2) AS drug_rate
                   FROM checkpost_stops
                   GROUP BY country_name
                   ORDER BY drug_rate DESC;
                  """
            df2 = run_query(sql)
            st.subheader("🌍 Drug-Related Stop Rates by Country")
            st.table(df2)

        elif q_loc == "Arrest rate by country and violation":
             sql = """ SELECT country_name,violation,COUNT(*) AS total_stops, SUM(is_arrested) AS arrest_count, ROUND((SUM(is_arrested) / COUNT(*)) * 100, 2) AS arrest_rate
                  FROM checkpost_stops
                  WHERE country_name IS NOT NULL AND violation IS NOT NULL
                  GROUP BY country_name, violation
                  HAVING total_stops >= 50
                  ORDER BY arrest_rate DESC;
            """
             df2 = run_query(sql)
             st.subheader("⚖️ Arrest Rate by Country & Violation")
             st.table(df2)

        elif q_loc == "Countries with most searches":
             sql = """  SELECT  country_name, COUNT(*) AS search_count
                    FROM checkpost_stops
                    WHERE search_conducted = TRUE AND country_name IS NOT NULL
                    GROUP BY country_name
                    ORDER BY search_count desc;
            """
             df2 = run_query(sql)
             st.subheader("🔍 Search Volume by Country")
             st.table(df2)
    
    with tab6:    
            st.subheader("Complex Analytics")
            q_comp = st.selectbox("Choose query:", [ "Yearly stops & arrests by country", "Violation trends by age & race",
                               "Time period analysis (Year/Month/Hour)", "High search/arrest rate violations",
                                "Driver demographics by country","Top 5 violations by arrest rate" ], key="complex_q")

    if q_comp == "Yearly stops & arrests by country":
            sql = """ SELECT stop_year, country_name, total_stops, arrest_count, RANK() OVER (PARTITION BY stop_year ORDER BY arrest_count DESC) as arrest_rank
                            FROM ( SELECT YEAR(stop_date) AS stop_year, country_name, COUNT(*) AS total_stops, SUM(is_arrested) AS arrest_count
                            FROM checkpost_stops
                            WHERE stop_date IS NOT NULL AND country_name IS NOT NULL
                            GROUP BY YEAR(stop_date), country_name
                        ) AS yearly_aggregates ORDER BY stop_year DESC, arrest_rank ASC;
               """
            df2 = run_query(sql)
            st.subheader("📅 Yearly Stops & Arrests by Country")
            st.table(df2)

    elif q_comp == "Violation trends by age & race":
        
            sql = """ SELECT driver_age, driver_race, violation, COUNT(*) AS count
                          FROM checkpost_stops
                          WHERE driver_age IS NOT NULL
                         GROUP BY driver_age, driver_race, violation
                         ORDER BY count DESC LIMIT 100;
                       """
            df2 = run_query(sql)
            st.subheader("📈 Violation Trends (Age × Race)")
            st.table(df2)

    elif q_comp == "Time period analysis (Year/Month/Hour)":
            sql = """ SELECT YEAR(stop_date) AS year, MONTHNAME(stop_date) AS month_name,COUNT(*) AS total_stops,ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY YEAR(stop_date)), 1) AS pct_of_year
                        FROM checkpost_stops
                        WHERE stop_date IS NOT NULL
                        GROUP BY YEAR(stop_date), MONTHNAME(stop_date), MONTH(stop_date)
                        ORDER BY YEAR(stop_date) DESC, MONTH(stop_date) ASC;
                   """
            df2 = run_query(sql)
            st.subheader("⏱ Time Period Analysis")
            st.table(df2)
        
    elif q_comp == "High search/arrest rate violations":
            sql = """ SELECT violation, COUNT(*) AS total_stops,SUM(search_conducted = 1) AS searches,SUM(is_arrested = 1) AS arrests,
                    ROUND(100.0 * SUM(search_conducted = 1)/COUNT(*),2) AS search_rate,ROUND(100.0 * SUM(is_arrested = 1)/COUNT(*),2) AS arrest_rate
                    FROM checkpost_stops
                    GROUP BY violation
                    ORDER BY search_rate DESC, arrest_rate DESC;
                """
            df2 = run_query(sql)
            st.subheader("🔥 High Search/Arrest Rate Violations")
            st.table(df2)

    elif q_comp == "Driver demographics by country":
         
            sql = """ SELECT country_name,COUNT(*) AS stops,ROUND(AVG(driver_age),1) AS avg_age, SUM(driver_gender='Male') AS male,
                    SUM(driver_gender='Female') AS female
                    FROM checkpost_stops
                    GROUP BY country_name
                    ORDER BY stops DESC;
                """
            df2 = run_query(sql)
            st.subheader("👥 Driver Demographics by Country")
            st.table(df2)

    elif q_comp == "Top 5 violations by arrest rate":
         
            sql = """ SELECT violation, COUNT(*) AS total_stops,SUM(is_arrested = 1) AS arrests, ROUND(100.0 * SUM(is_arrested = 1)/COUNT(*),2) AS arrest_rate
                   FROM checkpost_stops
                   GROUP BY violation
                   ORDER BY arrest_rate DESC LIMIT 5;
               """
            df2 = run_query(sql)
            st.subheader("🚨 Top 5 High-Arrest Violations")
            st.table(df2)

elif page == "Creator Info":
    st.title("👩‍💻 Creator of this Project")
    st.write("""
    **Developed by:** Manjunathan.R
    **Skills:** Python, SQL, Data Analysis,Streamlit, Pandas
    """
    )
    
    
    
    
    
       
    
       
   