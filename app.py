from flask import Flask, render_template, jsonify
import analysis

app = Flask(__name__)

# ============== 主页路由 ==============

@app.route('/')
def index():
    """主页 - 选择仪表板"""
    return render_template('index.html')

# ============== Company Dashboard 路由 ==============

@app.route('/company')
def company_dashboard():
    """公司运营仪表板"""
    return render_template('company_dashboard.html')

@app.route('/api/company/revenue-summary')
def api_revenue_summary():
    """收入总览 API"""
    try:
        data = analysis.get_revenue_summary()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/revenue-by-distance')
def api_revenue_by_distance():
    """收入与距离关系 API - 已移除"""
    return jsonify({"message": "This endpoint has been replaced by fare calculator"}), 404

@app.route('/api/company/zones')
def api_zones():
    """获取所有区域列表 API"""
    try:
        data = analysis.get_all_zones()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/fare-estimate')
def api_fare_estimate():
    """费用估算 API"""
    try:
        from flask import request
        pickup = request.args.get('pickup', type=int)
        dropoff = request.args.get('dropoff', type=int)
        
        if not pickup or not dropoff:
            return jsonify({"error": "Missing pickup or dropoff zone ID"}), 400
        
        data = analysis.get_fare_estimate(pickup, dropoff)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/payment-breakdown')
def api_payment_breakdown():
    """支付方式分布 API"""
    try:
        data = analysis.get_payment_breakdown()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/top-zones')
def api_top_zones():
    """最高收入区域 API"""
    try:
        data = analysis.get_top_pickup_zones()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/surcharges')
def api_surcharges():
    """附加费用分析 API"""
    try:
        data = analysis.get_surcharge_analysis()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/company/hourly-demand')
def api_hourly_demand():
    """按小时需求 API"""
    try:
        data = analysis.get_hourly_demand()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============== Public Dashboard 路由 ==============

@app.route('/public')
def public_dashboard():
    """公众乘客仪表板"""
    return render_template('public_dashboard.html')

@app.route('/api/public/busiest-zones')
def api_busiest_zones():
    """最繁忙区域 API"""
    try:
        data = analysis.get_busiest_pickup_zones()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/popular-routes')
def api_popular_routes():
    """热门路线 API"""
    try:
        data = analysis.get_popular_routes()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/demand-by-hour')
def api_demand_by_hour():
    """各时段需求 API"""
    try:
        data = analysis.get_demand_by_hour()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/demand-by-day')
def api_demand_by_day():
    """各星期需求 API"""
    try:
        data = analysis.get_demand_by_day()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/wait-times')
def api_wait_times():
    """等待时间估算 API"""
    try:
        data = analysis.estimate_wait_time_by_zone()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/public/zone-activity')
def api_zone_activity():
    """区域活跃度 API"""
    try:
        data = analysis.get_zone_activity_heatmap()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============== NYC 311 Complaints 路由 ==============

@app.route('/api/complaints/heatmap')
def api_complaints_heatmap():
    """生成 311 投诉热点图 API"""
    try:
        data = analysis.generate_311_heatmap()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/complaints/stats')
def api_complaints_stats():
    """311 投诉统计 API"""
    try:
        data = analysis.get_311_stats()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============== 启动应用 ==============

if __name__ == '__main__':
    print("=" * 60)
    print("🚕 NYC Taxi Analytics Dashboard")
    print("=" * 60)
    print("📊 Company Dashboard: http://127.0.0.1:5001/company")
    print("👥 Public Dashboard:  http://127.0.0.1:5001/public")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5001)