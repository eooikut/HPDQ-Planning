from flask import Blueprint, jsonify, render_template
import pandas as pd
from db import engine  # Import engine từ file cùng thư mục

# Khai báo Blueprint
# template_folder giúp Flask tìm thấy file html trong thư mục con
kho2d_bp = Blueprint('kho2d_bp', __name__, 
                        template_folder='templates',
                        static_folder='static') # Prefix đường dẫn

# 1. Route hiển thị giao diện (Truy cập: /warehouse/map)
@kho2d_bp.route('/kho2d')
def kho2d():
    return render_template('kho2d.html')

# 2. Route API trả dữ liệu JSON (Truy cập: /warehouse/api/data)
@kho2d_bp.route('/api/data')
def get_data():
    if not engine:
        return jsonify({"status": "error", "message": "Lỗi kết nối DB"}), 500

    try:
        query = "SELECT [ID Cuộn Bó], [Vị trí], [Khối lượng], Nhóm, [SO Mapping] FROM kho"
        df = pd.read_sql(query, engine)
        
        mapped_data = {}
        missing_list = []      # List lỗi (Đỏ)
        auto_assigned_list = [] # List gợi ý (Cam) - MỚI THÊM
        
        stats = {"total": 0, "valid": 0, "auto_assigned": 0, "invalid": 0}

        for _, row in df.iterrows():
            stats["total"] += 1
            try:
                coil_id = str(row['ID Cuộn Bó'])
                pos = str(row['Vị trí']).strip()
                raw_so = row['SO Mapping']
                
                # Logic: Nếu là None, NaN, hoặc bằng 0 -> Coi là chưa map (Rỗng)
                if pd.isnull(raw_so) or str(raw_so).strip() == '0' or raw_so == 0:
                    so_val = "" # Gán bằng rỗng để dễ kiểm tra ở Frontend
                else:
                    so_val = str(raw_so).strip()
                w_val = f"{row['Khối lượng']:,.0f}" if pd.notnull(row['Khối lượng']) else "0"
                group_val = str(row['Nhóm']) if pd.notnull(row['Nhóm']) else ""

                # --- CASE 1: ĐỦ VỊ TRÍ (HA1.01001) ---
                if len(pos) >= 9:
                    # ... (Giữ nguyên logic cũ) ...
                    zone = pos[0:3]
                    line_str = pos[4:6]
                    suffix = pos[6:]
                    try: line_key = int(line_str)
                    except: 
                        missing_list.append({"id": coil_id, "pos": pos, "reason": "Line lỗi"})
                        stats["invalid"] += 1
                        continue

                    if zone not in mapped_data: mapped_data[zone] = {}
                    if line_key not in mapped_data[zone]: mapped_data[zone][line_key] = {"fixed": {}, "pending": []}

                    mapped_data[zone][line_key]["fixed"][suffix] = {
                        "id": coil_id, "pos": pos, "w": w_val, "group": group_val, "so": so_val
                    }
                    stats["valid"] += 1

                # --- CASE 2: THIẾU VỊ TRÍ (HA1.01) -> MÀU CAM ---
                elif len(pos) == 6: 
                    zone = pos[0:3]
                    line_str = pos[4:6]
                    try: line_key = int(line_str)
                    except:
                        missing_list.append({"id": coil_id, "pos": pos, "reason": "Line lỗi"})
                        stats["invalid"] += 1
                        continue
                        
                    if zone not in mapped_data: mapped_data[zone] = {}
                    if line_key not in mapped_data[zone]: mapped_data[zone][line_key] = {"fixed": {}, "pending": []}
                    
                    # Thêm vào cấu trúc vẽ
                    mapped_data[zone][line_key]["pending"].append({
                        "id": coil_id, "pos": pos, "w": w_val, "group": group_val, "so": so_val,
                        "is_auto": True
                    })
                    
                    # --- MỚI: Thêm vào danh sách để xem chi tiết ---
                    auto_assigned_list.append({
                        "id": coil_id, "pos": pos, "so": so_val, 
                        "reason": "Vị trí chưa chi tiết (Tự động xếp)"
                    })
                    
                    stats["auto_assigned"] += 1

                else:
                    missing_list.append({"id": coil_id, "pos": pos, "reason": "Format sai"})
                    stats["invalid"] += 1

            except Exception as e:
                missing_list.append({"id": str(row.get('ID Cuộn Bó')), "pos": str(row.get('Vị trí')), "reason": str(e)})
                stats["invalid"] += 1

        return jsonify({
            "status": "success",
            "data": mapped_data,
            "invalid": missing_list,        
            "auto_list": auto_assigned_list, 
            "stats": stats
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500