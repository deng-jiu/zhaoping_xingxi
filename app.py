import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify, g, send_file
from datetime import datetime
import io
from openpyxl import Workbook

app = Flask(__name__)
DATABASE = 'recruitment.db'
EXCEL_FILE = '2026届_秋招汇总表.xlsx'  # Excel文件路径常量

# 数据库连接函数
def get_db_connection():
    """获取数据库连接，确保在应用上下文中"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

# 关闭数据库连接
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# 执行查询的辅助函数
def query_db(query, args=(), one=False):
    """在应用上下文中执行查询"""
    with app.app_context():
        cur = get_db_connection().execute(query, args)
        rv = [dict((cur.description[idx][0], value) for idx, value in enumerate(row)) for row in cur.fetchall()]
        cur.close()
        return (rv[0] if rv else None) if one else rv

# 执行修改的辅助函数
def modify_db(query, args=()):
    """在应用上下文中执行修改操作"""
    with app.app_context():
        conn = get_db_connection()
        cur = conn.execute(query, args)
        conn.commit()
        cur.close()
        return cur.lastrowid

# 初始化数据库 - 全面检查表结构
def init_db():
    """初始化数据库，确保所有必要列都存在"""
    required_columns = [
        ('serial_number', 'TEXT'),
        ('company_name', 'TEXT'),
        ('batch', 'TEXT'),
        ('company_type', 'TEXT'),
        ('industry', 'TEXT'),
        ('recruitment_target', 'TEXT'),
        ('positions', 'TEXT'),
        ('application_status', 'TEXT'),  # 网申状态
        ('location', 'TEXT'),
        ('update_time', 'TEXT'),
        ('deadline', 'TEXT'),
        ('official_announcement', 'TEXT'),
        ('application_method', 'TEXT'),
        ('referral_code', 'TEXT'),
        ('is_suitable', 'INTEGER DEFAULT 0'),
        ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    ]
    
    with app.app_context():
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies'")
        table_exists = cursor.fetchone() is not None
        
        # 如果表存在，检查所有必要的列
        if table_exists:
            cursor.execute("PRAGMA table_info(companies)")
            existing_columns = [column[1] for column in cursor.fetchall()]
            
            # 检查并添加缺失的列
            for col_name, col_type in required_columns:
                if col_name not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE companies ADD COLUMN {col_name} {col_type}")
                        print(f"已添加缺失的列: {col_name}")
                    except Exception as e:
                        print(f"添加列 {col_name} 时出错: {str(e)}")
        
        # 创建表结构（如果不存在）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serial_number TEXT,  -- 序号
            company_name TEXT,  -- 公司名称
            batch TEXT,         -- 批次
            company_type TEXT,  -- 企业性质
            industry TEXT,      -- 行业大类
            recruitment_target TEXT,  -- 招聘对象
            positions TEXT,     -- 招聘岗位
            application_status TEXT,  -- 网申状态
            location TEXT,      -- 工作地点
            update_time TEXT,   -- 更新时间
            deadline TEXT,      -- 截止时间
            official_announcement TEXT,  -- 官方公告
            application_method TEXT,    -- 投递方式
            referral_code TEXT,         -- 内推码/备注
            is_suitable INTEGER DEFAULT 0,  -- 是否合适（0:未标记,1:合适,2:不合适）
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()

# 从Excel导入数据到数据库 - 优化字段处理
def load_data_to_db(file_path):
    """将Excel数据导入数据库，优化字段处理"""
    try:
        if not os.path.exists(file_path):
            return False, f"文件不存在: {file_path}"
            
        # 读取Excel文件
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            try:
                df = pd.read_excel(file_path, engine='xlrd')
            except Exception as e2:
                return False, f"无法读取Excel文件: {str(e2)}"
        
        # 列名映射
        column_mapping = {
            '序号': 'serial_number',
            '公司名称': 'company_name',
            '批次': 'batch',
            '企业性质': 'company_type',
            '行业大类': 'industry',
            '招聘对象': 'recruitment_target',
            '招聘岗位': 'positions',
            '网申状态': 'application_status',
            '工作地点': 'location',
            '更新时间': 'update_time',
            '截止时间': 'deadline',
            '官方公告': 'official_announcement',
            '投递方式': 'application_method',
            '内推码/备注': 'referral_code'
        }
        
        # 处理列名
        excel_columns = [col.strip() for col in df.columns.str.strip()]
        mapped_columns = {}
        
        for excel_col, db_col in column_mapping.items():
            if excel_col in excel_columns:
                mapped_columns[excel_col] = db_col
            else:
                # 尝试模糊匹配
                found = False
                for ec in excel_columns:
                    if ec.lower() == excel_col.lower():
                        mapped_columns[ec] = db_col
                        found = True
                        break
                if not found:
                    print(f"警告: Excel中未找到'{excel_col}'列，将使用空值代替")
        
        # 重命名列名
        df = df.rename(columns={k: v for k, v in mapped_columns.items()})
        
        with app.app_context():
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM companies')
            
            error_count = 0
            for idx, row in df.iterrows():
                try:
                    # 构建插入数据，确保所有字段都有默认值
                    insert_data = (
                        str(row.get('serial_number', str(idx+1))),  # 序号
                        row.get('company_name', ''),                # 公司名称
                        row.get('batch', ''),                       # 批次
                        row.get('company_type', ''),                # 企业性质
                        row.get('industry', ''),                    # 行业大类
                        row.get('recruitment_target', ''),          # 招聘对象
                        row.get('positions', ''),                   # 招聘岗位
                        row.get('application_status', ''),          # 网申状态
                        row.get('location', ''),                    # 工作地点
                        row.get('update_time', ''),                 # 更新时间
                        row.get('deadline', ''),                    # 截止时间
                        row.get('official_announcement', ''),       # 官方公告
                        row.get('application_method', ''),          # 投递方式
                        row.get('referral_code', '')                # 内推码/备注
                    )
                    
                    cursor.execute('''
                    INSERT INTO companies (
                        serial_number, company_name, batch, company_type, industry,
                        recruitment_target, positions, application_status, location,
                        update_time, deadline, official_announcement,
                        application_method, referral_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', insert_data)
                except Exception as e:
                    error_count += 1
                    if error_count <= 10:  # 只显示前10个错误
                        print(f"导入第{idx+1}行数据时出错: {str(e)}")
                    elif error_count == 11:
                        print("...更多错误已省略...")
            
            conn.commit()
            conn.close()
            
            if error_count > 0:
                return True, f"导入完成，共 {len(df)} 条数据，其中 {error_count} 条数据导入失败"
            return True, f"成功导入 {len(df)} 条数据"
    except Exception as e:
        return False, f"导入失败: {str(e)}"

# 获取所有公司数据
def get_all_companies():
    return query_db('SELECT * FROM companies ORDER BY id')

# 首页路由
@app.route('/')
def index():
    companies = get_all_companies()
    
    # 提取筛选条件的唯一值
    industries = list({c['industry'] for c in companies if c['industry']})
    company_types = list({c['company_type'] for c in companies if c['company_type']})
    locations = list({c['location'] for c in companies if c['location']})
    qiyexingz = list({c['recruitment_target'] for c in companies if c['recruitment_target']})
    jiezhi = list({c['deadline'] for c in companies if c['deadline']})
    
    return render_template('index.html', 
                          companies=companies,
                          industries=industries,
                          company_types=company_types,
                          locations=locations,
                          qiyexingz=qiyexingz,
                          jiezhi=jiezhi)

# API: 获取筛选后的公司
@app.route('/api/companies')
def api_companies():
    industry = request.args.get('industry')
    company_type = request.args.get('type')
    location = request.args.get('location')
    qiyexing = request.args.get('qiyexing')
    jie = request.args.get('jie')
    
    query = 'SELECT * FROM companies WHERE 1=1'
    params = []
    
    if industry and industry != 'all':
        query += ' AND industry = ?'
        params.append(industry)
    if company_type and company_type != 'all':
        query += ' AND company_type = ?'
        params.append(company_type)
    if location and location != 'all':
        query += ' AND location = ?'
        params.append(location)
    if qiyexing and qiyexing != 'all':
        query += ' AND recruitment_target = ?'
        params.append(qiyexing)
    if jie and jie != 'all':
        query += ' AND deadline = ?'
        params.append(jie)
    
    query += ' ORDER BY id'
    return jsonify(query_db(query, params))

# API: 获取单个公司详情
@app.route('/api/company/<int:id>')
def api_company(id):
    return jsonify(query_db('SELECT * FROM companies WHERE id = ?', [id], one=True))

# API: 标记公司是否合适
@app.route('/api/mark', methods=['POST'])
def api_mark():
    data = request.get_json()
    company_id = data.get('id')
    suitable = data.get('suitable')
    
    if not company_id:
        return jsonify({'status': 'error', 'message': '缺少公司ID'})
    
    mark_value = 1 if suitable else 2
    modify_db('UPDATE companies SET is_suitable = ? WHERE id = ?', 
             (mark_value, company_id))
    
    return jsonify({'status': 'success', 'message': '标记成功'})

# API: 获取所有合适的公司
@app.route('/api/suitable-companies')
def api_suitable_companies():
    return jsonify(query_db('SELECT * FROM companies WHERE is_suitable = 1 ORDER BY id'))

# API: 导出合适的公司
@app.route('/api/export-suitable')
def api_export_suitable():
    companies = query_db('SELECT * FROM companies WHERE is_suitable = 1 ORDER BY id')
    
    if not companies:
        return jsonify({'status': 'error', 'message': '没有合适的公司可导出'}), 400
    
    # 创建Excel文件
    wb = Workbook()
    ws = wb.active
    ws.title = "合适的公司"
    
    # 写入表头
    headers = ['序号', '公司名称', '批次', '企业性质', '行业大类', 
              '招聘对象', '招聘岗位', '网申状态', '工作地点', '截止时间', '投递方式', '内推码/备注']
    ws.append(headers)
    
    # 写入数据
    for company in companies:
        row = [
            company.get('serial_number', ''),
            company.get('company_name', ''),
            company.get('batch', ''),
            company.get('company_type', ''),
            company.get('industry', ''),
            company.get('recruitment_target', ''),
            company.get('positions', ''),
            company.get('application_status', ''),
            company.get('location', ''),
            company.get('deadline', ''),
            company.get('application_method', ''),
            company.get('referral_code', '')
        ]
        ws.append(row)
    
    # 保存到内存
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    return send_file(output, as_attachment=True, 
                   download_name=f'合适的公司_{datetime.now().strftime("%Y%m%d")}.xlsx',
                   mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# API: 重新加载数据
@app.route('/api/reload-data')
def api_reload_data():
    # 先确保表结构正确
    init_db()
    success, message = load_data_to_db(EXCEL_FILE)
    if success:
        return jsonify({'status': 'success', 'message': message})
    else:
        return jsonify({'status': 'error', 'message': message}), 400

# 初始化应用
def initialize_app():
    """初始化应用程序，确保在应用上下文中执行"""
    with app.app_context():
        # 确保数据库已初始化
        init_db()
        
        # 检查是否需要加载初始数据
        count = query_db('SELECT COUNT(*) as count FROM companies', one=True)
        if count and count['count'] == 0 and os.path.exists(EXCEL_FILE):
            load_data_to_db(EXCEL_FILE)

if __name__ == '__main__':
    # 初始化应用
    initialize_app()
    # 运行应用
    app.run(debug=True)
    