import psycopg
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
import folium
from folium.plugins import HeatMap

# 数据库配置
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "taxi",
    "user": "postgres",
    "password": "123"
}

# MongoDB 配置
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "nyc311"
MONGO_COLLECTION = "requests"

def get_connection():
    """获取数据库连接"""
    return psycopg.connect(**PG_CONFIG)

# ============== Business Dashboard 1: Taxi Company Dashboard ==============

def get_revenue_summary():
    """获取收入总览"""
    query = """
    SELECT 
        COUNT(*) as total_trips,
        SUM(fare_amount) as total_fare,
        SUM(tip_amount) as total_tips,
        SUM(tolls_amount) as total_tolls,
        SUM(total_amount) as total_revenue,
        AVG(trip_distance) as avg_distance,
        AVG(fare_amount) as avg_fare,
        SUM(CASE WHEN payment_type = 1 THEN total_amount ELSE 0 END) as credit_card_revenue,
        SUM(CASE WHEN payment_type = 2 THEN total_amount ELSE 0 END) as cash_revenue
    FROM yellow_taxi_clean
    WHERE total_amount > 0 AND fare_amount > 0;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')[0]

def get_revenue_by_distance():
    """收入与距离关系 - 已移除，改为费用计算器"""
    # 此函数已被 get_fare_estimate 替代
    pass

def get_all_zones():
    """获取所有区域列表（用于下拉选择）"""
    query = """
    SELECT 
        "LocationID" as locationid,
        "Zone" as zone,
        "Borough" as borough
    FROM taxi_zone_lookup
    ORDER BY "Zone";
    """
    try:
        with get_connection() as conn:
            df = pd.read_sql(query, conn)
            return df.to_dict('records')
    except Exception as e:
        print(f"Error in get_all_zones: {e}")
        # 尝试小写列名
        try:
            query_lower = """
            SELECT 
                locationid,
                zone,
                borough
            FROM taxi_zone_lookup
            ORDER BY zone;
            """
            with get_connection() as conn:
                df = pd.read_sql(query_lower, conn)
                return df.to_dict('records')
        except Exception as e2:
            print(f"Error with lowercase columns: {e2}")
            raise

def get_fare_estimate(pickup_zone_id, dropoff_zone_id):
    """根据起点和终点估算费用"""
    query = """
    SELECT 
        COUNT(*) as trip_count,
        AVG(fare_amount) as avg_fare,
        MIN(fare_amount) as min_fare,
        MAX(fare_amount) as max_fare,
        AVG(trip_distance) as avg_distance,
        AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60) as avg_duration_min,
        AVG(total_amount) as avg_total,
        AVG(tip_amount) as avg_tip
    FROM yellow_taxi_clean
    WHERE pulocationid = %s 
        AND dolocationid = %s
        AND fare_amount > 0
        AND total_amount > 0;
    """
    try:
        with get_connection() as conn:
            result = pd.read_sql(query, conn, params=(pickup_zone_id, dropoff_zone_id))
            
            if len(result) > 0 and result['trip_count'].iloc[0] > 0:
                data = result.to_dict('records')[0]
                
                # 获取区域名称 - 尝试大写列名
                try:
                    zone_query = """
                    SELECT "LocationID" as locationid, "Zone" as zone, "Borough" as borough 
                    FROM taxi_zone_lookup 
                    WHERE "LocationID" IN (%s, %s);
                    """
                    zones = pd.read_sql(zone_query, conn, params=(pickup_zone_id, dropoff_zone_id))
                except:
                    # 如果失败，尝试小写
                    zone_query = """
                    SELECT locationid, zone, borough 
                    FROM taxi_zone_lookup 
                    WHERE locationid IN (%s, %s);
                    """
                    zones = pd.read_sql(zone_query, conn, params=(pickup_zone_id, dropoff_zone_id))
                
                pickup_info = zones[zones['locationid'] == pickup_zone_id].iloc[0] if len(zones[zones['locationid'] == pickup_zone_id]) > 0 else None
                dropoff_info = zones[zones['locationid'] == dropoff_zone_id].iloc[0] if len(zones[zones['locationid'] == dropoff_zone_id]) > 0 else None
                
                return {
                    'success': True,
                    'pickup_zone': pickup_info['zone'] if pickup_info is not None else f'Zone {pickup_zone_id}',
                    'pickup_borough': pickup_info['borough'] if pickup_info is not None else 'Unknown',
                    'dropoff_zone': dropoff_info['zone'] if dropoff_info is not None else f'Zone {dropoff_zone_id}',
                    'dropoff_borough': dropoff_info['borough'] if dropoff_info is not None else 'Unknown',
                    'trip_count': int(data['trip_count']),
                    'avg_fare': float(data['avg_fare']),
                    'min_fare': float(data['min_fare']),
                    'max_fare': float(data['max_fare']),
                    'avg_distance': float(data['avg_distance']),
                    'avg_duration_min': float(data['avg_duration_min']) if data['avg_duration_min'] else 0,
                    'avg_total': float(data['avg_total']),
                    'avg_tip': float(data['avg_tip'])
                }
            else:
                return {
                    'success': False,
                    'message': 'No historical data found for this route'
                }
    except Exception as e:
        print(f"Error in get_fare_estimate: {e}")
        return {
            'success': False,
            'message': f'Error: {str(e)}'
        }

def get_payment_breakdown():
    """支付方式分布"""
    query = """
    SELECT 
        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            ELSE 'Other'
        END as payment_method,
        COUNT(*) as trip_count,
        SUM(total_amount) as revenue,
        AVG(tip_amount) as avg_tip
    FROM yellow_taxi_clean
    WHERE total_amount > 0
    GROUP BY payment_type
    ORDER BY revenue DESC;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_top_pickup_zones():
    """最高收入上车区域 Top 10"""
    query = """
    SELECT 
        pulocationid as zone_id,
        COUNT(*) as trip_count,
        SUM(total_amount) as total_revenue,
        AVG(fare_amount) as avg_fare,
        AVG(trip_distance) as avg_distance
    FROM yellow_taxi_clean
    WHERE pulocationid IS NOT NULL AND total_amount > 0
    GROUP BY pulocationid
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_surcharge_analysis():
    """附加费用分析"""
    query = """
    SELECT 
        COUNT(*) as total_trips,
        SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as congestion_trips,
        SUM(congestion_surcharge) as total_congestion,
        SUM(extra) as total_extra,
        SUM(mta_tax) as total_mta_tax,
        SUM(improvement_surcharge) as total_improvement,
        AVG(CASE WHEN congestion_surcharge > 0 THEN congestion_surcharge END) as avg_congestion
    FROM yellow_taxi_clean
    WHERE total_amount > 0;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')[0]

def get_hourly_demand():
    """按小时的需求分析"""
    query = """
    SELECT 
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        COUNT(*) as trip_count,
        SUM(total_amount) as revenue,
        AVG(fare_amount) as avg_fare
    FROM yellow_taxi_clean
    WHERE tpep_pickup_datetime IS NOT NULL AND total_amount > 0
    GROUP BY hour
    ORDER BY hour;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

# ============== Business Dashboard 2: Public Riders Dashboard ==============

def get_busiest_pickup_zones():
    """最繁忙的上车区域"""
    query = """
    SELECT 
        pulocationid as zone_id,
        COUNT(*) as trip_count,
        AVG(fare_amount) as avg_fare,
        AVG(trip_distance) as avg_distance
    FROM yellow_taxi_clean
    WHERE pulocationid IS NOT NULL
    GROUP BY pulocationid
    ORDER BY trip_count DESC
    LIMIT 15;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_popular_routes():
    """最热门路线 Top 10 (起点-终点对)"""
    query = """
    SELECT 
        pulocationid as pickup_zone,
        dolocationid as dropoff_zone,
        COUNT(*) as trip_count,
        AVG(fare_amount) as avg_fare,
        AVG(trip_distance) as avg_distance,
        AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60) as avg_duration_min
    FROM yellow_taxi_clean
    WHERE pulocationid IS NOT NULL 
        AND dolocationid IS NOT NULL
        AND tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
    GROUP BY pulocationid, dolocationid
    ORDER BY trip_count DESC
    LIMIT 10;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_demand_by_hour():
    """各时段需求分布"""
    query = """
    SELECT 
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        COUNT(*) as trip_count,
        AVG(passenger_count) as avg_passengers
    FROM yellow_taxi_clean
    WHERE tpep_pickup_datetime IS NOT NULL
    GROUP BY hour
    ORDER BY hour;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_demand_by_day():
    """各星期几需求分布"""
    query = """
    SELECT 
        EXTRACT(DOW FROM tpep_pickup_datetime) as day_of_week,
        CASE EXTRACT(DOW FROM tpep_pickup_datetime)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END as day_name,
        COUNT(*) as trip_count
    FROM yellow_taxi_clean
    WHERE tpep_pickup_datetime IS NOT NULL
    GROUP BY day_of_week, day_name
    ORDER BY day_of_week;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def get_zone_activity_heatmap():
    """区域活跃度热图数据"""
    query = """
    SELECT 
        pulocationid as zone_id,
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        COUNT(*) as trip_count
    FROM yellow_taxi_clean
    WHERE pulocationid IS NOT NULL 
        AND tpep_pickup_datetime IS NOT NULL
    GROUP BY pulocationid, hour
    HAVING COUNT(*) > 10
    ORDER BY zone_id, hour;
    """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

def estimate_wait_time_by_zone(zone_id=None):
    """估算等待时间（基于区域的出行频率）"""
    if zone_id:
        query = f"""
        SELECT 
            pulocationid as zone_id,
            COUNT(*) as trips_per_hour,
            CASE 
                WHEN COUNT(*) > 100 THEN 'Very Short (< 3 min)'
                WHEN COUNT(*) > 50 THEN 'Short (3-5 min)'
                WHEN COUNT(*) > 20 THEN 'Medium (5-10 min)'
                ELSE 'Long (10+ min)'
            END as estimated_wait
        FROM yellow_taxi_clean
        WHERE pulocationid = {zone_id}
        GROUP BY pulocationid;
        """
    else:
        query = """
        SELECT 
            pulocationid as zone_id,
            COUNT(*) as trips_per_hour,
            CASE 
                WHEN COUNT(*) > 100 THEN 'Very Short'
                WHEN COUNT(*) > 50 THEN 'Short'
                WHEN COUNT(*) > 20 THEN 'Medium'
                ELSE 'Long'
            END as estimated_wait
        FROM yellow_taxi_clean
        WHERE pulocationid IS NOT NULL
        GROUP BY pulocationid
        ORDER BY trips_per_hour DESC
        LIMIT 20;
        """
    with get_connection() as conn:
        return pd.read_sql(query, conn).to_dict('records')

# ============== NYC 311 Complaints Heatmap ==============

def classify_descriptor(desc):
    """分类投诉描述"""
    desc = str(desc).lower()
    if "driver complaint" in desc or "driver report" in desc:
        return "Driver Behavior Issues"
    elif "vehicle complaint" in desc:
        return "Vehicle Issues"
    elif "car service company" in desc:
        return "Company Service Issues"
    else:
        return "Other"

def generate_311_heatmap(limit=200000):
    """生成 NYC 311 投诉热点图（带分类图层）"""
    try:
        # 连接 MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # 获取数据（包含 descriptor）
        cursor = collection.find(
            {}, 
            {"latitude": 1, "longitude": 1, "descriptor": 1, "_id": 0}
        ).limit(limit)
        df = pd.DataFrame(list(cursor))
        
        # 数据清洗
        df = df.dropna(subset=["latitude", "longitude"])
        df = df[(df.latitude > 35) & (df.latitude < 45)]
        df = df[(df.longitude > -80) & (df.longitude < -70)]
        
        # 分类
        df["category"] = df["descriptor"].apply(classify_descriptor)
        
        # 初始化地图
        m = folium.Map(location=[40.7128, -74.0060], zoom_start=11)
        
        # 1. Overall 图层
        HeatMap(
            df[['latitude', 'longitude']].values.tolist(),
            radius=8,
            blur=6,
            name="Overall Hotspot"
        ).add_to(m)
        
        # 2. 大类图层
        for cat in df["category"].unique():
            sub_df = df[df["category"] == cat].dropna(subset=["latitude", "longitude"])
            
            if len(sub_df) == 0:
                continue
            
            HeatMap(
                sub_df[['latitude', 'longitude']].values.tolist(),
                radius=8,
                blur=6,
                name=f"Category: {cat}"
            ).add_to(m)
        
        # 3. 小类 Descriptor 图层
        for desc in df["descriptor"].unique():
            sub_df = df[df["descriptor"] == desc].dropna(subset=["latitude", "longitude"])
            
            if len(sub_df) < 10:
                continue
            
            HeatMap(
                sub_df[['latitude', 'longitude']].values.tolist(),
                radius=8,
                blur=6,
                name=f"Descriptor: {desc}"
            ).add_to(m)
        
        # 图层控制器
        folium.LayerControl(collapsed=False).add_to(m)
        
        # 保存为 HTML
        map_html = m._repr_html_()
        
        return {
            "success": True,
            "total_complaints": len(df),
            "map_html": map_html,
            "categories": df["category"].value_counts().to_dict()
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def get_311_stats():
    """获取 311 投诉统计信息"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        total = collection.count_documents({})
        with_coords = collection.count_documents({
            "latitude": {"$exists": True, "$ne": None},
            "longitude": {"$exists": True, "$ne": None}
        })
        
        return {
            "total_complaints": total,
            "complaints_with_location": with_coords,
            "success": True
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# ============== 测试函数 ==============

if __name__ == "__main__":
    print("Testing Company Dashboard APIs...")
    print("\n1. Revenue Summary:")
    print(get_revenue_summary())
    
    print("\n2. Top Pickup Zones:")
    print(get_top_pickup_zones()[:3])
    
    print("\nTesting Public Dashboard APIs...")
    print("\n3. Busiest Zones:")
    print(get_busiest_pickup_zones()[:3])
    
    print("\n4. Popular Routes:")
    print(get_popular_routes()[:3])
    
    print("\n✅ All APIs working!")