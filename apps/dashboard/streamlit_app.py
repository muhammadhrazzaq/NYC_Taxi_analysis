import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(layout="wide")
st.title("🚕 NYC Taxi Lakehouse")
st.caption("Gold layer · Iceberg on S3 Tables · read live from Snowflake")

@st.cache_data(ttl=600)
def q(sql):
    return session.sql(sql).to_pandas()

# KPIs
kpi = q('''
    SELECT SUM("trips") AS trips, SUM("revenue") AS revenue
    FROM NYC_TAXI."gold"."agg_monthly_kpis"
''')
c1, c2 = st.columns(2)
c1.metric("Total trips", f"{int(kpi.TRIPS[0]):,}")
c2.metric("Total revenue", f"${kpi.REVENUE[0]:,.0f}")

# COVID impact
monthly = q('''
    SELECT TO_DATE("pickup_year" || '-' || "pickup_month" || '-01') AS month,
           "trips" AS trips, "trips_yoy_pct" AS yoy_pct
    FROM NYC_TAXI."gold"."agg_monthly_kpis"
    ORDER BY month
''')
st.subheader("Monthly trips")
st.line_chart(monthly, x="MONTH", y="TRIPS")

left, right = st.columns(2)

# Revenue by borough
with left:
    boro = q('''
        SELECT "pu_borough" AS borough, SUM("revenue") AS revenue
        FROM NYC_TAXI."gold"."agg_daily_borough"
        GROUP BY 1 ORDER BY 2 DESC
    ''')
    st.subheader("Revenue by borough")
    st.bar_chart(boro, x="BOROUGH", y="REVENUE")

# Demand by hour
with right:
    weekend = st.toggle("Weekend")
    hourly = q(f'''
        SELECT "pickup_hour" AS hour, SUM("avg_trips_per_day") AS trips_per_day
        FROM NYC_TAXI."gold"."agg_hourly_zone"
        WHERE "is_weekend" = {str(weekend).upper()}
        GROUP BY 1 ORDER BY 1
    ''')
    st.subheader("Average trips per hour")
    st.bar_chart(hourly, x="HOUR", y="TRIPS_PER_DAY")

# Tips
st.subheader("Tipping by borough and time band (card payments)")
st.dataframe(q('''
    SELECT "pu_borough", "time_band", "avg_tip_pct", "median_tip_pct", "pct_no_tip"
    FROM NYC_TAXI."gold"."agg_tips"
    ORDER BY "avg_tip_pct" DESC
'''), use_container_width=True)