import pandas as pd
import numpy as np
import re

# ==========================================
# 1. CẤU HÌNH & DATA (GIỮ NGUYÊN)
# ==========================================
data_ratios = {
    '900-1000':  [0.0, 0.0, 0.05, 0.05, 0.15, 0.45, 0.15, 0.05, 0.05, 0.05, 0.0],
    '1000-1100': [0.0, 0.0, 0.05, 0.05, 0.10, 0.45, 0.15, 0.10, 0.05, 0.05, 0.0],
    '1100-1200': [0.0, 0.0, 0.05, 0.05, 0.10, 0.40, 0.20, 0.10, 0.05, 0.05, 0.0],
    '1200-1300': [0.0, 0.0, 0.0,  0.0,  0.0,  0.0,  0.65, 0.20, 0.10, 0.05, 0.0],
    '1300-1400': [0.0, 0.0, 0.0,  0.05, 0.10, 0.40, 0.20, 0.15, 0.05, 0.05, 0.0],
    '1400-1500': [0.0, 0.0, 0.0,  0.0,  0.05, 0.35, 0.25, 0.20, 0.10, 0.05, 0.0],
    '1500-1524': [0.0, 0.0, 0.0,  0.0,  0.0,  0.30, 0.25, 0.25, 0.10, 0.05, 0.05]
}

thickness_labels = [
    '1.20<=T<1.30', '1.30<=T<1.40', '1.40<=T<1.50', '1.50<=T<1.65',
    '1.65<=T<1.80', '1.80<=T<2.00', '2.00<=T<2.20', '2.20<=T<2.40',
    '2.40<=T<2.75', '2.75<=T<2.90', '2.90>=T'
]

# ==========================================
# 2. HÀM TRA CỨU GIÁ CHÍNH XÁC
# ==========================================
def get_exact_surcharge(width_val, thick_val):
    # 1. Xác định Cột
    col_idx = -1
    if 900 <= width_val <= 1199: col_idx = 0
    elif 1200 <= width_val <= 1500: col_idx = 1
    elif 1501 <= width_val <= 1650: col_idx = 2
    if col_idx == -1: return 0

    # 2. Ma trận giá
    matrix = {
        (1.20, 1.34): [35, 35, 0],
        (1.35, 1.54): [22, 25, 55],
        (1.55, 1.74): [20, 15, 45],
        (1.75, 1.99): [15, 10, 20],
        (2.00, 2.54): [10, 0, 6],
        (2.55, 3.99): [10, 0, 6],
        (4.00, 8.99): [10, 0, 7],
        (9.00, 15.99): [20, 0, 5],
        (16.00, 25.40): [22, 0, 5]
    }

    # 3. Tra cứu
    for (t_min, t_max), rates in matrix.items():
        if t_min <= thick_val <= t_max + 0.001:
            return rates[col_idx]
    return 0

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================
def normalize_columns(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    col_mapping = {
        'Khổ rộng': ['width', 'khổ rộng', 'kho rong', 'k/r', 'rong'],
        'Chiều dày': ['thickness', 'chiều dày', 'chieu day', 'dày', 'day', 'thick'],
        'Khối lượng': ['mass', 'weight', 'khối lượng', 'khoi luong', 'kl', 'qty', 'tấn', 'tan', 'kg']
    }
    new_cols = {}
    for standard_col, variations in col_mapping.items():
        for col in df.columns:
            if col in variations:
                new_cols[col] = standard_col
                break
    if new_cols: df = df.rename(columns=new_cols)
    return df

def get_width_label(width):
    if pd.isna(width): return None
    w = float(width)
    if 900 <= w < 1000: return '900-1000'
    if 1000 <= w < 1100: return '1000-1100'
    if 1100 <= w < 1200: return '1100-1200'
    if 1200 <= w < 1300: return '1200-1300'
    if 1300 <= w < 1400: return '1300-1400'
    if 1400 <= w < 1500: return '1400-1500'
    if 1500 <= w <= 1524: return '1500-1524'
    return None

def get_thickness_index_full(thick):
    if pd.isna(thick): return -1
    t = float(thick)
    if 1.20 <= t < 1.30: return 0
    if 1.30 <= t < 1.40: return 1
    if 1.40 <= t < 1.50: return 2
    if 1.50 <= t < 1.65: return 3
    if 1.65 <= t < 1.80: return 4
    if 1.80 <= t < 2.00: return 5
    if 2.00 <= t < 2.20: return 6
    if 2.20 <= t < 2.40: return 7
    if 2.40 <= t < 2.75: return 8
    if 2.75 <= t < 2.90: return 9
    if t >= 2.90: return 10
    return -1

def validate_spec(width, thick):
    try: w, t = float(width), float(thick)
    except: return False, "Lỗi số liệu"
    if 1.20 <= t < 1.30: return False, f"Độ dày {t}mm chưa hỗ trợ (Vùng đỏ)."
    if 1.30 <= t < 1.40: return False, f"Độ dày {t}mm chưa hỗ trợ (Vùng đỏ)."
    if 1.40 <= t < 1.50 and w >= 1200: return False, f"Độ dày {t}mm cấm khổ >= 1200."
    if 1.50 <= t < 1.65 and w >= 1400: return False, f"Độ dày {t}mm cấm khổ >= 1400."
    if 1.65 <= t < 1.80 and w >= 1500: return False, f"Độ dày {t}mm cấm khổ >= 1500."
    return True, ""

# ==========================================
# 4. LOGIC TÍNH TOÁN (ĐÃ UPDATE HIỂN THỊ)
# ==========================================
def calculate_production_status(demand_data, width_label):
    if width_label not in data_ratios: return []
    ratios = data_ratios[width_label]
    
    results = []
    current_mass_sum = 0.0   
    current_ratio_sum = 0.0
    
    for i, ratio in enumerate(ratios):
        label = thickness_labels[i]
        
        # Lấy dữ liệu từ bước gộp nhóm
        item_data = demand_data.get(i, {'mass': 0, 'money': 0, 'details': ''})
        actual_demand = item_data['mass']
        total_surcharge_amount = item_data['money']
        detail_html = item_data['details'] # Chuỗi HTML đã nối sẵn
        
        # Logic Supply
        current_ratio_sum += ratio
        prev_ratio_sum = current_ratio_sum - ratio
        if prev_ratio_sum == 0:
            generated_supply = current_mass_sum * ratio if ratio > 0 else 0
        else:
            generated_supply = current_mass_sum * (ratio / prev_ratio_sum)
            
        final_production = max(generated_supply, actual_demand)
        diff = generated_supply - actual_demand
        status_text = f"Dư {int(diff):,} kg".replace(',', '.') if diff > 0 else ""
        current_mass_sum += final_production
        
        if final_production > 0 or actual_demand > 0:
            results.append({
                'Khổ rộng': width_label,
                'Độ dày': label,
                'Chốt (Sản xuất)': int(round(final_production, 0)),
                'Đơn hàng nhập vào': int(round(actual_demand, 0)),
                'Trạng thái': status_text,
                'Phụ thu (Thành tiền)': total_surcharge_amount,
                # 🟢 Dữ liệu chi tiết dạng text HTML (đã được tạo ở bước run_calculation_tool)
                'Phụ thu (Chi tiết)': detail_html 
            })
            
    return results

def run_calculation_tool(df_input):
    df = normalize_columns(df_input)
    required_cols = ['Khổ rộng', 'Chiều dày', 'Khối lượng']
    if any(c not in df.columns for c in required_cols):
        return {'error': "Thiếu cột dữ liệu bắt buộc."}

    for col in required_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=required_cols)
    if df.empty: return {'error': "Dữ liệu không hợp lệ."}

    errors = []
    for idx, row in df.iterrows():
        valid, msg = validate_spec(row['Khổ rộng'], row['Chiều dày'])
        if not valid: errors.append(f"Dòng {idx+2}: {msg}")
    if errors: return {'error': "<br>".join(errors)}

    # =======================================================
    # 🟢 BƯỚC 1: TẠO THÔNG TIN CHI TIẾT CHO TỪNG DÒNG
    # =======================================================
    # Tạo danh sách các dictionary chứa thông tin từng dòng
    detail_list = []
    surcharge_list = []
    
    for _, row in df.iterrows():
        rate = get_exact_surcharge(row['Khổ rộng'], row['Chiều dày'])
        mass_ton = row['Khối lượng'] 
        money = mass_ton * rate
        
        # Tạo chuỗi hiển thị: "1.25mm - 5T - $35"
        # Dùng thẻ span để format đẹp
        # info_str = f"{row['Chiều dày']}mm - {row['Khối lượng']:g}T - ${rate}"
        if rate > 0:
            # Thêm thẻ <b> để số tiền đậm lên cho dễ nhìn
            info_str = f"{row['Chiều dày']}mm - {mass_ton:g}T - ${rate} - <b>${money:,.0f}</b>"
        else:
            info_str = f"{row['Chiều dày']}mm - {mass_ton:g}T - (Không phụ thu)"
        surcharge_list.append(money)
        detail_list.append(info_str)
        
    df['Surcharge_Amount'] = surcharge_list
    df['Detail_Info'] = detail_list
    
    # =======================================================
    # 🟡 BƯỚC 2: GỘP NHÓM & NỐI CHUỖI
    # =======================================================
    df['Width_Label'] = df['Khổ rộng'].apply(get_width_label)
    df['Thickness_Index'] = df['Chiều dày'].apply(get_thickness_index_full)
    
    # Gộp dữ liệu:
    # 1. Cộng tổng khối lượng
    # 2. Cộng tổng tiền
    # 3. Nối chuỗi chi tiết bằng dấu xuống dòng <br>
    df_agg = df.dropna(subset=['Width_Label']).groupby(['Width_Label', 'Thickness_Index']).agg({
        'Khối lượng': 'sum',
        'Surcharge_Amount': 'sum',
        'Detail_Info': lambda x: '<br>'.join(x) # Nối các dòng lại
    }).reset_index()

    final_report = []
    for width in df_agg['Width_Label'].unique():
        group_data = df_agg[df_agg['Width_Label'] == width]
        
        demand_data = {}
        for _, row in group_data.iterrows():
            idx = row['Thickness_Index']
            demand_data[idx] = {
                'mass': row['Khối lượng'],
                'money': row['Surcharge_Amount'],
                'details': row['Detail_Info']
            }
            
        final_report.extend(calculate_production_status(demand_data, width))

    return final_report