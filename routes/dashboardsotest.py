import re
import pandas as pd
import numpy as np
import traceback
from flask import Blueprint, render_template, jsonify
from auth.decorator import permission_required
from db import engine
from extensions import cache

# Tạo Blueprint
dashboard_so_bp = Blueprint("dashboard_so_bp", __name__, template_folder='../templates')

# ==============================================================================
# 1. CÁC HÀM HỖ TRỢ (HELPER FUNCTIONS)
# ==============================================================================
import pandas as pd
import numpy as np
import re

# --- 1. HÀM LÀM SẠCH SỐ LIỆU (QUAN TRỌNG) ---
def clean_vn_number(val):
    """
    Chuyển đổi chuỗi số Excel Việt Nam sang Float chuẩn của Python.
    Hỗ trợ: 1.200,50 (chuẩn VN) hoặc 1,200.50 (chuẩn EN) hoặc số nguyên.
    """
    if pd.isna(val) or str(val).strip() == '':
        return 0.0
    
    # Nếu đã là số (int/float) thì trả về luôn
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()
    try:
        # Trường hợp 1: Có cả chấm và phẩy (VD: 1.200,50 hoặc 1,200.50)
        # Mẹo: Loại bỏ ký tự không phải số cuối cùng (phần ngàn), thay ký tự phân cách thập phân thành chấm
        if '.' in s and ',' in s:
            if s.rfind('.') < s.rfind(','): # Dạng 1.200,50 (VN)
                s = s.replace('.', '').replace(',', '.')
            else: # Dạng 1,200.50 (EN)
                s = s.replace(',', '')
        
        # Trường hợp 2: Chỉ có dấu phẩy (VD: 1200,5 hoặc 1,200)
        elif ',' in s:
            # Nếu sau dấu phẩy có 3 số (VD: 1,200) -> Khả năng cao là dấu ngàn -> Xóa
            # Nếu sau dấu phẩy có 1-2 số (VD: 50,5) -> Khả năng là thập phân -> Thay bằng chấm
            # Tuy nhiên để an toàn cho HRC (thường không lẻ quá nhỏ), ta ưu tiên replace phẩy thành chấm
            s = s.replace(',', '.')

        # Trường hợp 3: Chỉ có dấu chấm (VD: 1.200 hoặc 1.5)
        elif '.' in s:
             # Logic đếm số: 1.200 -> 1200; 1.5 -> 1.5
             # Với thép HRC, khối lượng thường lớn, nên 1.200 thường là 1200kg/tấn
             parts = s.split('.')
             if len(parts) > 2: # VD: 1.000.000
                 s = s.replace('.', '')
             elif len(parts[1]) == 3: # VD: 1.200 -> 1200
                 s = s.replace('.', '')
             # Còn lại giữ nguyên (1.5 tấn)

        return float(s)
    except:
        return 0.0
import os
# --- 2. HÀM XỬ LÝ CHÍNH ---
def get_future_plans():
    # ĐƯỜNG DẪN FILE (Cần đảm bảo file nằm đúng chỗ)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Nối đường dẫn đó với tên file Excel
    path_hrc = os.path.join(current_dir, 'nhu cau hrc.xlsx') 
    
    try:
        # Đọc file Excel
        # dtype=object để giữ nguyên định dạng chuỗi ban đầu, tránh Pandas tự đoán sai
        df1 = pd.read_excel(path_hrc, sheet_name='HRC1', dtype=object)
        df2 = pd.read_excel(path_hrc, sheet_name='HRC 2', dtype=object)

        print(f"Đã đọc file. HRC1: {len(df1)} dòng, HRC2: {len(df2)} dòng.")

        # --- XỬ LÝ SHEET HRC1 (Đơn vị: TẤN -> Cần nhân 1000) ---
        # Kiểm tra tên cột trong file HRC1.csv bạn gửi: 'Đợt sx1', 'Mác thép', 'Độ dày', 'Khổ rộng', 'Tổng LSX'
        df1 = df1.rename(columns={
            'Đợt sx1': 'month',
            'Mác thép': 'mac_thep',
            'Độ dày': 'chieu_day',
            'Khổ rộng': 'kho_rong',
            'Tổng LSX': 'weight' 
        })
        df1['Factory'] = 'HRC1'
        
        # Làm sạch số và NHÂN 1000 (Tấn -> Kg)
        df1['weight'] = df1['weight'].apply(clean_vn_number) *1000

        # --- XỬ LÝ SHEET HRC 2 (Đơn vị: KG -> Giữ nguyên) ---
        # Kiểm tra tên cột trong file HRC 2.csv: 'Đợt sx2', 'Mác thép', 'Độ dày', 'Khổ rộng', 'Khối lượng (Kg)'
        df2 = df2.rename(columns={
            'Đợt sx2': 'month',
            'Mác thép': 'mac_thep',
            'Độ dày': 'chieu_day',
            'Khổ rộng': 'kho_rong',
            'Khối lượng (Kg)': 'weight'
        })
        df2['Factory'] = 'HRC2'
        
        # Làm sạch số (Giữ nguyên giá trị vì đã là Kg)
        df2['weight'] = df2['weight'].apply(clean_vn_number)*1000

        # --- GỘP DỮ LIỆU ---
        cols_to_keep = ['month', 'Factory', 'mac_thep', 'chieu_day', 'kho_rong', 'weight']
        
        # Chỉ lấy các cột tồn tại thực sự để tránh lỗi
        df1_final = df1[[c for c in cols_to_keep if c in df1.columns]]
        df2_final = df2[[c for c in cols_to_keep if c in df2.columns]]
        
        df_final = pd.concat([df1_final, df2_final], ignore_index=True)

        # --- XỬ LÝ THÔNG TIN BỔ SUNG ---
        # Làm sạch cột tháng (Xóa khoảng trắng thừa)
        df_final['month'] = df_final['month'].astype(str).str.strip()
        
        # Làm sạch chiều dày/khổ rộng (để lọc được trên biểu đồ)
        df_final['chieu_day'] = pd.to_numeric(df_final['chieu_day'], errors='coerce')
        df_final['kho_rong'] = pd.to_numeric(df_final['kho_rong'], errors='coerce')

        # Tạo month_key chuẩn (YYYY-MM) cho Frontend vẽ biểu đồ
        def format_month_key(val):
            if not val or str(val).lower() == 'nan': return None
            val_str = str(val)
            
            # Xử lý dạng "T.12", "T.1", "T12"
            if 'T' in val_str:
                try:
                    # Lấy số sau chữ T/t
                    num_part = re.search(r'\d+', val_str).group()
                    # Giả sử năm là 2025 (theo yêu cầu của bạn)
                    return f"2025-{num_part.zfill(2)}" 
                except:
                    return val_str
            return val_str

        df_final['month_key'] = df_final['month'].apply(format_month_key)

        # Lọc bỏ các dòng không có trọng lượng để giảm tải JSON
        df_final = df_final[df_final['weight'] > 0]

        print("Xử lý xong Future Plans. Dữ liệu mẫu:", df_final[['month_key', 'weight', 'Factory']].head(3).to_dict('records'))

        return df_final.to_dict('records')

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong get_future_plans: {e}")
        print(traceback.format_exc())
        return []
# Bảng tra dung sai chiều dày (Theo ảnh bạn cung cấp)
THICKNESS_STD_MAP = {
    1.5: [0.14, 0.15, 0.16],
    1.8: [0.16, 0.17, 0.18],
    2.0: [0.17, 0.19, 0.20],
    2.3: [0.17, 0.19, 0.20],
    3.0: [0.19, 0.21, 0.22],
    4.0: [0.24, 0.26, 0.28],
    5.0: [0.26, 0.28, 0.29],
    8.0: [0.32, 0.33, 0.34],
    12.0: [0.35, 0.36, 0.37]
}

def parse_material_desc(desc):
    """Phân tích chuỗi mô tả để lấy Dày và Rộng."""
    if not isinstance(desc, str): return None, None
    # Regex: 2.75x121Y hoặc 3.0x1200
    match = re.search(r'(\d+(?:\.\d+)?)\s*x\s*(\d+[A-Z]?)', desc)
    if match:
        try:
            return float(match.group(1)), match.group(2)
        except: pass
    # Fallback: 3x1200
    match = re.search(r'(\d+(?:\.\d+)?)\s*x\s*(\d+)', desc)
    if match:
        try:
            return float(match.group(1)), match.group(2)
        except: pass
    return None, None

def _clean_kho_rong(width_str):
    """Làm sạch khổ rộng: 121X -> 1210."""
    if not isinstance(width_str, str): return None
    width_str = width_str.strip()
    try:
        match_xy = re.search(r'^(\d+)([XY])$', width_str)
        if match_xy: return int(match_xy.group(1)) * 10
        return int(width_str)
    except:
        match_fallback = re.search(r'^(\d+)', width_str)
        if match_fallback: return int(match_fallback.group(1))
        return None

def _group_kho_rong(width_num):
    if pd.isna(width_num): return "Không xác định"
    if 900 <= width_num < 1000: return "900-1000"
    if 1000 <= width_num < 1100: return "1000-1100"
    if 1100 <= width_num < 1200: return "1100-1200"
    if 1200 <= width_num < 1300: return "1200-1300"
    if 1300 <= width_num < 1400: return "1300-1400"
    if 1400 <= width_num < 1500: return "1400-1500"
    if 1500 <= width_num <= 1650: return "1500-1650"
    return "Ngoài khoảng"

def _group_chieu_day(thickness):
    if pd.isna(thickness): return "Không xác định"
    if 1.20 <= thickness < 1.35: return "1.20≤T<1.35"
    if 1.35 <= thickness < 1.50: return "1.35≤T<1.50"
    if 1.50 <= thickness < 1.65: return "1.50≤T<1.65"
    if 1.65 <= thickness < 2.00: return "1.65≤T<2.00"
    if 2.00 <= thickness < 2.15: return "2.00≤T<2.15"
    if 2.15 <= thickness < 2.30: return "2.15≤T<2.30"
    if 2.30 <= thickness < 2.50: return "2.30≤T<2.50"
    if 2.50 <= thickness < 2.75: return "2.50≤T<2.75"
    if 2.75 <= thickness < 3.00: return "2.75≤T<3.00"
    if thickness >= 3.00: return "T>= 3.00"
    return "Ngoài khoảng"

def get_specs_thickness(df_subset, nominal_target):
    """Tính LSL/USL chiều dày dựa vào bảng tra."""
    if df_subset.empty: return None, None, nominal_target
    
    available_nominals = np.array(list(THICKNESS_STD_MAP.keys()))
    closest_nominal = available_nominals[np.abs(available_nominals - nominal_target).argmin()]
    
    avg_width = df_subset['kho_rong_num'].mean()
    tolerances = THICKNESS_STD_MAP[closest_nominal]
    
    if avg_width < 1250: tol = tolerances[0]
    elif avg_width < 1500: tol = tolerances[1]
    else: tol = tolerances[2]
        
    lsl = round(closest_nominal - tol, 3)
    usl = round(closest_nominal + tol, 3)
    return lsl, usl, closest_nominal

def get_specs_width(nominal_width):
    """Tính LSL/USL khổ rộng (Asymmetric: +0/+Max)."""
    lsl = nominal_width
    if 900 <= nominal_width < 1250: usl = nominal_width + 20
    elif nominal_width >= 1250: usl = nominal_width + 30
    else: usl = nominal_width + 20
    return lsl, usl

def generate_cpk_data(data_series, lsl, usl):
    """Tạo dữ liệu đường cong hình chuông (Bell Curve)."""
    data = pd.to_numeric(data_series, errors='coerce').dropna().tolist()
    if len(data) < 2: return None

    mu = np.mean(data)
    sigma = np.std(data)
    if sigma == 0: return None

    # Vẽ rộng hơn LSL/USL một chút để đẹp
    x_min = min(min(data), lsl) * 0.995
    x_max = max(max(data), usl) * 1.005
    x_values = np.linspace(x_min, x_max, 100)
    y_values = (1 / (sigma * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x_values - mu) / sigma) ** 2)

    return {
        "mean": round(mu, 3),
        "std_dev": round(sigma, 4),
        "ucl": round(mu + 3*sigma, 3),
        "lcl": round(mu - 3*sigma, 3),
        "lsl": lsl,
        "usl": usl,
        "line_data": list(zip(np.round(x_values, 2), np.round(y_values, 4))),
        "raw_data": data
    }

# ==============================================================================
# 2. ROUTES
# ==============================================================================

@dashboard_so_bp.route("/dashboard-so")
def dashboard_so():
    return render_template('dashboardso.html')

@dashboard_so_bp.route('/api/warehouse-data')
@cache.cached(timeout=86400)
def get_warehouse_data():
    try:
        print("--> BẮT ĐẦU GỌI API WAREHOUSE-DATA")
        with engine.connect() as conn:
            
            # ==========================================
            # 1. LẤY DỮ LIỆU COILS (KHO + SẢN LƯỢNG)
            # ==========================================
            sql_coils = """
                /* KHỐI 1: Sanluong - Chờ nhập kho */
SELECT DISTINCT
    s.[ID Cuộn Bó], s.[Material Description], s.[Nhóm], s.[Vị trí], s.[Lô phôi],
    s.[Khối lượng], s.[Ngày sản xuất], s.Ca, s.[Order], s.Batch, s.[Mác thép],
    CASE WHEN s.[Tp loại 2] = 1 THEN 1 ELSE 0 END AS TpLoai2,
    N'Chờ nhập kho' AS TrangThai,
    CASE
        WHEN s.[NhaMay] = N'HRC1' THEN DATEADD(DAY, 7, s.[Ngày sản xuất])
        WHEN s.[NhaMay] = N'HRC2' THEN DATEADD(DAY, 8, s.[Ngày sản xuất])
        ELSE DATEADD(DAY, 7, s.[Ngày sản xuất])
    END AS NgayDuKien,
    0 AS [SO Mapping],
    ISNULL(so_detail.[SO_Mapping], 0) AS [SO Mapping dự kiến],
    s.[NhaMay]
FROM sanluong s
LEFT JOIN tbl_SO_Allocation_Detail so_detail ON s.[ID Cuộn Bó] = so_detail.[Roll_ID]
WHERE
    s.[ID Cuộn Bó] IS NOT NULL AND s.[ID Cuộn Bó] <> ''
    AND (s.[Đã nhập kho] = 'No' OR s.[Đã nhập kho] IS NULL) -- Điều kiện chưa nhập
    AND NOT EXISTS (SELECT 1 FROM kho k WHERE k.[ID Cuộn Bó] = s.[ID Cuộn Bó])

UNION ALL

/* KHỐI 2: Kho - Đang tồn kho */
SELECT
    k.[ID Cuộn Bó], k.[Material Description], k.[Nhóm], k.[Vị trí], k.[Lô phôi],
    k.[Khối lượng], k.[Ngày sản xuất], k.Ca, k.[Order], k.Batch, k.[Mác thép],
    CASE WHEN k.[Tp loại 2] = 'X' THEN 1 ELSE 0 END AS TpLoai2,
    CASE
        WHEN k.[SO Mapping] = 0 OR k.[SO Mapping] IS NULL THEN N'Nhập kho chưa mapping'
        ELSE N'Nhập kho đã mapping'
    END AS TrangThai,
    CAST(NULL AS DATE) AS NgayDuKien,
    k.[SO Mapping],
    CASE
        WHEN k.[SO Mapping] = 0 OR k.[SO Mapping] IS NULL THEN ISNULL(so_detail.[SO_Mapping], 0)
        ELSE 0
    END AS [SO Mapping dự kiến],
    CASE
        WHEN k.[Plant] = 1000 THEN N'HRC1'
        WHEN k.[Plant] = 1600 THEN N'HRC2'
        ELSE N'Khác'
    END AS NhaMay
FROM kho k
LEFT JOIN tbl_SO_Allocation_Detail so_detail ON k.[ID Cuộn Bó] = so_detail.[Roll_ID]

UNION ALL

/* KHỐI 3: Sanluong - Đã bán (Các cuộn còn lại) */
SELECT DISTINCT
    s.[ID Cuộn Bó], s.[Material Description], s.[Nhóm], s.[Vị trí], s.[Lô phôi],
    s.[Khối lượng], s.[Ngày sản xuất], s.Ca, s.[Order], s.Batch, s.[Mác thép],
    CASE WHEN s.[Tp loại 2] = 1 THEN 1 ELSE 0 END AS TpLoai2,
    N'Đã bán' AS TrangThai, -- Gán trạng thái Đã bán
    CAST(NULL AS DATE) AS NgayDuKien, -- Đã bán thì không cần ngày dự kiến nhập kho nữa
    0 AS [SO Mapping],
    ISNULL(so_detail.[SO_Mapping], 0) AS [SO Mapping dự kiến],
    s.[NhaMay]
FROM sanluong s
LEFT JOIN tbl_SO_Allocation_Detail so_detail ON s.[ID Cuộn Bó] = so_detail.[Roll_ID]
WHERE
    s.[ID Cuộn Bó] IS NOT NULL AND s.[ID Cuộn Bó] <> ''
    AND s.[Đã nhập kho] = 'Yes' -- Lấy các cuộn ĐÃ nhập kho (ngược với khối 1)
    AND NOT EXISTS (SELECT 1 FROM kho k WHERE k.[ID Cuộn Bó] = s.[ID Cuộn Bó]) -- Nhưng hiện KHÔNG còn trong kho (ngược với khối 2)
            """
            coil_df = pd.read_sql(sql_coils, conn)
            
            # --- XỬ LÝ DỮ LIỆU COIL (AN TOÀN) ---
            if not coil_df.empty:
                if 'Mác thép' in coil_df.columns: coil_df['Mác thép'] = coil_df['Mác thép'].fillna('Không xác định')
                
                coil_df[['chieu_day', 'kho_rong']] = coil_df['Material Description'].apply(lambda x: pd.Series(parse_material_desc(x)))
                
                # Ép kiểu số để tránh lỗi 500 khi tính toán
                coil_df['kho_rong_num'] = pd.to_numeric(coil_df['kho_rong'].apply(_clean_kho_rong), errors='coerce')
                coil_df['chieu_day'] = pd.to_numeric(coil_df['chieu_day'], errors='coerce')
                
                coil_df['kho_rong_group'] = coil_df['kho_rong_num'].apply(_group_kho_rong)
                coil_df['chieu_day_group'] = coil_df['chieu_day'].apply(_group_chieu_day)
            else:
                coil_df['kho_rong_num'] = []
                coil_df['chieu_day'] = []

            # --- Tính Top 5 ---
            if not coil_df.empty:
                top_5_counts = coil_df['Mác thép'].value_counts().nlargest(5)
                top_5_data = [{"name": index, "value": int(value)} for index, value in top_5_counts.items()]
                top_5_names = top_5_counts.index.tolist()
            else:
                top_5_data, top_5_names = [], []

            # ==========================================
            # 2. LẤY DỮ LIỆU SO TỔNG QUAN (CHO MONTHLY & CPK)
            # ==========================================
            sql_so_full = """
                WITH so_processed AS (
                SELECT
                    [Material], [Sales Document], [Material Description],
                    [Shipped Quantity (KG)], [Quantity (KG)], [Document Date],
                    CASE 
                        WHEN [Factory] LIKE N'%Hòa Phát Dung Quất 2%' THEN 'HRC2'
                        ELSE 'HRC1' 
                    END AS [Factory],
                    RTRIM(CASE WHEN RIGHT([Material Description], 3) = ' II' THEN LEFT([Material Description], LEN([Material Description]) - 3) ELSE [Material Description] END) AS CleanDesc
                FROM so
            ),
            so_with_mac_thep AS (
                SELECT *, CASE WHEN CHARINDEX(' ', REVERSE(CleanDesc)) = 0 THEN CleanDesc ELSE REVERSE(LEFT(REVERSE(CleanDesc), CHARINDEX(' ', REVERSE(CleanDesc)) - 1)) END AS [Mác thép]
                FROM so_processed
            ),
            SO_Request_Representative AS (
                 SELECT [SO Mapping], [NHÓM] AS Representative_NHOM, ROW_NUMBER() OVER(PARTITION BY [SO Mapping] ORDER BY [Material description] ASC) as rn
                 FROM dbo.so_request WHERE [NHÓM] IS NOT NULL
            )
            SELECT
                s.*,
                COALESCE(sr_direct.[NHÓM], sr_rep.Representative_NHOM) AS [Nhóm]
            FROM so_with_mac_thep s
            LEFT JOIN dbo.so_request sr_direct ON s.[Sales Document] = sr_direct.[SO Mapping] AND s.[Material Description] = sr_direct.[Material description]
            LEFT JOIN SO_Request_Representative sr_rep ON s.[Sales Document] = sr_rep.[SO Mapping] AND sr_rep.rn = 1
            """
            so_df = pd.read_sql(sql_so_full, conn)
            
            # Xử lý dữ liệu SO cho CPK Overlay
            latest_month_str = "N/A"
            so_current_month = pd.DataFrame()
            
            if not so_df.empty:
                so_df[['chieu_day', 'kho_rong']] = so_df['Material Description'].apply(lambda x: pd.Series(parse_material_desc(x)))
                so_df['kho_rong_num'] = pd.to_numeric(so_df['kho_rong'].apply(_clean_kho_rong), errors='coerce')
                so_df['chieu_day'] = pd.to_numeric(so_df['chieu_day'], errors='coerce')
                
                # Xác định tháng gần nhất từ SO
                if 'Document Date' in so_df.columns:
                    so_df['ValidDate'] = pd.to_datetime(so_df['Document Date'], errors='coerce')
                    so_df['Month'] = so_df['ValidDate'].dt.to_period('M').astype(str)
                    # Loại bỏ NaT/None trước khi tìm max
                    valid_months = so_df['Month'].dropna()
                    if not valid_months.empty:
                        latest_month_str = valid_months.max()
                        so_current_month = so_df[so_df['Month'] == latest_month_str]

            # ======================================================
            # 3. TÍNH TOÁN CPK (TÍCH HỢP SO & TRA BẢNG)
            # ======================================================
            
            # A. CPK KHỔ RỘNG (1200 - 1300)
            cpk_width_data = None
            if not coil_df.empty:
                # Lọc Coil trong khoảng
                w_df = coil_df[(coil_df['kho_rong_num'] >= 1200) & (coil_df['kho_rong_num'] <= 1300)]
                if not w_df.empty:
                    dominant_width = w_df['kho_rong_num'].mode()[0]
                    w_lsl, w_usl = get_specs_width(dominant_width)
                    cpk_width_data = generate_cpk_data(w_df['kho_rong_num'], w_lsl, w_usl)
                    
                    if cpk_width_data:
                        cpk_width_data['title'] = f"Khổ {int(dominant_width)}mm (Tol: +0/+{int(w_usl-w_lsl)})"
                        # Tính Weighted Mean của SO tháng này
                        cpk_width_data['so_mean'] = None
                        cpk_width_data['so_month'] = latest_month_str
                        if not so_current_month.empty:
                            so_w = so_current_month[(so_current_month['kho_rong_num'] >= 1200) & (so_current_month['kho_rong_num'] <= 1300)]
                            if not so_w.empty:
                                total_qty = so_w['Quantity (KG)'].sum()
                                if total_qty > 0:
                                    weighted_mean = (so_w['kho_rong_num'] * so_w['Quantity (KG)']).sum() / total_qty
                                    cpk_width_data['so_mean'] = round(weighted_mean, 2)

            # B. CPK CHIỀU DÀY (2.00 - 2.15)
            cpk_thickness_data = None
            if not coil_df.empty:
                t_df = coil_df[(coil_df['chieu_day'] >= 2.00) & (coil_df['chieu_day'] <= 2.15)]
                if not t_df.empty:
                    dominant_thick = float(t_df['chieu_day'].mode()[0])
                    t_lsl, t_usl, nom_t = get_specs_thickness(t_df, dominant_thick)
                    if t_lsl is not None:
                        cpk_thickness_data = generate_cpk_data(t_df['chieu_day'], t_lsl, t_usl)
                        if cpk_thickness_data:
                            tol_val = round(t_usl - nom_t, 2)
                            cpk_thickness_data['title'] = f"Dày {nom_t}mm (Tol: ±{tol_val}mm)"
                            # Tính Weighted Mean của SO tháng này
                            cpk_thickness_data['so_mean'] = None
                            cpk_thickness_data['so_month'] = latest_month_str
                            if not so_current_month.empty:
                                so_t = so_current_month[(so_current_month['chieu_day'] >= 2.00) & (so_current_month['chieu_day'] <= 2.15)]
                                if not so_t.empty:
                                    total_qty = so_t['Quantity (KG)'].sum()
                                    if total_qty > 0:
                                        weighted_mean = (so_t['chieu_day'] * so_t['Quantity (KG)']).sum() / total_qty
                                        cpk_thickness_data['so_mean'] = round(weighted_mean, 3)

            # ==========================================
            # 4. TÍNH BIỂU ĐỒ CUNG CẦU THEO THÁNG
            # ==========================================
            monthly_supply = pd.DataFrame(columns=['Month', 'Supply_KG'])
            if not coil_df.empty and 'Ngày sản xuất' in coil_df.columns:
                coil_df['ValidDate'] = pd.to_datetime(coil_df['Ngày sản xuất'], errors='coerce')
                temp_coil = coil_df.dropna(subset=['ValidDate']).copy()
                temp_coil['Month'] = temp_coil['ValidDate'].dt.to_period('M').astype(str)
                monthly_supply = temp_coil.groupby('Month')['Khối lượng'].sum().reset_index().rename(columns={'Khối lượng': 'Supply_KG'})

            monthly_demand = pd.DataFrame(columns=['Month', 'Demand_KG'])
            if not so_df.empty and 'Month' in so_df.columns:
                # so_df đã có cột 'Month' từ bước 2
                temp_so = so_df.dropna(subset=['Month'])
                monthly_demand = temp_so.groupby('Month')['Quantity (KG)'].sum().reset_index().rename(columns={'Quantity (KG)': 'Demand_KG'})
            
            monthly_stats_df = pd.merge(monthly_supply, monthly_demand, on='Month', how='outer').fillna(0).sort_values('Month')
            
            if monthly_stats_df.empty:
                monthly_chart_data = {"months": [], "supply": [], "demand": []}
            else:
                monthly_chart_data = {
                    "months": monthly_stats_df['Month'].tolist(),
                    "supply": (monthly_stats_df['Supply_KG'] / 1000).tolist(), 
                    "demand": (monthly_stats_df['Demand_KG'] / 1000).tolist()
                }

            # ==========================================
            # 5. LẤY DỮ LIỆU SO DETAILS (MAPPING TREE & TABLE)
            # ==========================================
            # Query này cần logic join phức tạp hơn để map nhóm chính xác cho bảng
            sql_so_details_query = """
                WITH so_processed AS (
                SELECT
                    [Material], [Sales Document], [Material Description],
                    [Shipped Quantity (KG)], [Quantity (KG)],
                    RTRIM(CASE WHEN RIGHT([Material Description], 3) = ' II' THEN LEFT([Material Description], LEN([Material Description]) - 3) ELSE [Material Description] END) AS CleanDesc
                FROM so
            ),
            so_with_mac_thep AS (
                SELECT *, CASE WHEN CHARINDEX(' ', REVERSE(CleanDesc)) = 0 THEN CleanDesc ELSE REVERSE(LEFT(REVERSE(CleanDesc), CHARINDEX(' ', REVERSE(CleanDesc)) - 1)) END AS [Mác thép]
                FROM so_processed
            ),
            kho_clean AS (
                SELECT [Order], [Material], [SO Mapping], [Material Description], [Khối lượng]
                FROM kho
                WHERE [SO Mapping] IS NOT NULL AND [SO Mapping] <> '0'
            ),
            SO_Request_Representative AS (
                SELECT [SO Mapping], [NHÓM] AS Representative_NHOM, ROW_NUMBER() OVER(PARTITION BY [SO Mapping] ORDER BY [Material description] ASC) as rn
                FROM dbo.so_request WHERE [NHÓM] IS NOT NULL 
            ),
            so_request_complete AS (
                SELECT s.[Material Description], s.[Sales Document], COALESCE(sr_direct.[NHÓM], sr_rep.Representative_NHOM) AS [NHÓM]
                FROM (SELECT DISTINCT [Sales Document], [Material Description] FROM so_with_mac_thep) s
                LEFT JOIN dbo.so_request sr_direct ON s.[Sales Document] = sr_direct.[SO Mapping] AND s.[Material Description] = sr_direct.[Material description]
                LEFT JOIN SO_Request_Representative sr_rep ON s.[Sales Document] = sr_rep.[SO Mapping] AND sr_rep.rn = 1
            ),
            base AS (
                SELECT
                    k.[Order], s.[Sales Document] AS [SO Mapping], s.Material,
                    COALESCE(k.[Material Description], s.[Material Description]) AS [Material Description],
                    s.[Mác thép], sr.[NHÓM],
                    SUM(k.[Khối lượng]) AS SL_per_order,
                    s.[Shipped Quantity (KG)], s.[Quantity (KG)]
                FROM so_with_mac_thep s
                INNER JOIN kho_clean k ON k.[SO Mapping] = s.[Sales Document] AND k.Material = s.Material
                LEFT JOIN so_request_complete sr ON s.[Sales Document] = sr.[Sales Document] AND s.[Material Description] = sr.[Material Description]
                GROUP BY k.[Order], s.[Sales Document], s.Material, COALESCE(k.[Material Description], s.[Material Description]), s.[Shipped Quantity (KG)], s.[Quantity (KG)], s.[Mác thép], sr.[NHÓM]
            )
            SELECT DISTINCT
                [SO Mapping], Material, [Material Description], [Mác thép], [NHÓM] AS [Nhóm],
                SUM(ISNULL(SL_per_order, 0)) OVER (PARTITION BY [SO Mapping], Material) AS [SL Mapping kho],
                [Shipped Quantity (KG)], [Quantity (KG)],
                CASE WHEN [Quantity (KG)] > 0 THEN ROUND((SUM(ISNULL(SL_per_order, 0)) OVER (PARTITION BY [SO Mapping], Material) + [Shipped Quantity (KG)]) * 100.0 / [Quantity (KG)], 2) ELSE 0 END AS Process
            FROM base
            """
            so_details_df = pd.read_sql(sql_so_details_query, conn)
            
            # Xử lý Dày/Rộng cho SO Details (để tô màu trên cây)
            if not so_details_df.empty:
                so_details_df[['chieu_day', 'kho_rong']] = so_details_df['Material Description'].apply(lambda x: pd.Series(parse_material_desc(x)))
                so_details_df['kho_rong_num'] = so_details_df['kho_rong'].apply(_clean_kho_rong)
                so_details_df['kho_rong_group'] = so_details_df['kho_rong_num'].apply(_group_kho_rong)
                so_details_df['chieu_day_group'] = so_details_df['chieu_day'].apply(_group_chieu_day)

            # ==========================================
            # 6. XÂY DỰNG TREE
            # ==========================================
            # ==========================================
            # 6. XÂY DỰNG TREE
            # ==========================================
            tree = {}
            
            # --- [THÊM MỚI] Lọc bỏ trạng thái 'Đã bán' khi tạo cây ---
            # Chỉ lấy dữ liệu hiện đang có trong kho hoặc chờ nhập
            tree_df = coil_df[coil_df['TrangThai'] != 'Đã bán'] 

            # Sửa: Dùng tree_df thay vì coil_df để groupby
            width_groups = tree_df.groupby(['Mác thép', 'kho_rong_group', 'Nhóm'])['ID Cuộn Bó'].nunique().reset_index()
            for _, row in width_groups.iterrows():
                mac_thep, kho_rong_group, nhom, count = row['Mác thép'], row['kho_rong_group'], row['Nhóm'], row['ID Cuộn Bó']
                if pd.isna(mac_thep) or pd.isna(kho_rong_group) or pd.isna(nhom): continue
                if mac_thep not in tree: tree[mac_thep] = {'by_width': {}, 'by_thickness': {}, '_total_coils': 0}
                if kho_rong_group not in tree[mac_thep]['by_width']: tree[mac_thep]['by_width'][kho_rong_group] = {'_total_coils': 0}
                tree[mac_thep]['by_width'][kho_rong_group][nhom] = {'coil_count': count}

            # Sửa: Dùng tree_df thay vì coil_df để groupby
            thickness_groups = tree_df.groupby(['Mác thép', 'chieu_day_group', 'Nhóm'])['ID Cuộn Bó'].nunique().reset_index()
            for _, row in thickness_groups.iterrows():
                mac_thep, chieu_day_group, nhom, count = row['Mác thép'], row['chieu_day_group'], row['Nhóm'], row['ID Cuộn Bó']
                if pd.isna(mac_thep) or pd.isna(chieu_day_group) or pd.isna(nhom): continue
                if mac_thep not in tree: tree[mac_thep] = {'by_width': {}, 'by_thickness': {}, '_total_coils': 0}
                chieu_day_str = str(chieu_day_group)
                if chieu_day_str not in tree[mac_thep]['by_thickness']: tree[mac_thep]['by_thickness'][chieu_day_str] = {'_total_coils': 0}
                tree[mac_thep]['by_thickness'][chieu_day_str][nhom] = {'coil_count': count}
            
            # ... (Đoạn code tính tổng total_mac_thep giữ nguyên)
            for mac_thep, data in tree.items():
                total_mac_thep = 0
                for kho_rong_group, nhom_data in data['by_width'].items():
                    total_kho_rong = sum(d['coil_count'] for k, d in nhom_data.items() if k != '_total_coils')
                    nhom_data['_total_coils'] = total_kho_rong
                    total_mac_thep += total_kho_rong
                data['_total_coils'] = total_mac_thep
                for chieu_day_group, nhom_data in data['by_thickness'].items():
                    total_chieu_day = sum(d['coil_count'] for k, d in nhom_data.items() if k != '_total_coils')
                    nhom_data['_total_coils'] = total_chieu_day

            # ==========================================
            # 7. TRẢ VỀ JSON
            # ==========================================
            # Thay thế NaN để JSON không bị lỗi
            replace_values = {pd.NA: None, pd.NaT: None, np.nan: None, float('inf'): None, float('-inf'): None}
            coil_df = coil_df.replace(replace_values)
            so_details_df = so_details_df.replace(replace_values)
            so_df = so_df.replace(replace_values)
            all_so_full_json = so_df.to_dict('records')
            all_coils_json = coil_df.to_dict('records')
            all_so_details_json = so_details_df.to_dict('records')
            future_data = get_future_plans()
            return jsonify({
                "tree": tree,
                "coils": all_coils_json,
                "sales_orders": all_so_full_json, # Key dùng để vẽ màu cây
                "so_details": all_so_details_json,   # Key dùng cho bảng (giống nhau)
                "top_5_data": top_5_data,
                "top_5_names": top_5_names,
                "cpk_width": cpk_width_data,
                "cpk_thickness": cpk_thickness_data,
                "monthly_stats": monthly_chart_data,
                'future_plans': future_data
            })

    except Exception as e:
        print("\n[ERROR] Backend Warehouse Data:")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500