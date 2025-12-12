from flask import Blueprint, jsonify, render_template
import pandas as pd
import re
import numpy as np
from db import engine

kho2d_bp = Blueprint('kho2d_bp', __name__, template_folder='templates', static_folder='static')

@kho2d_bp.route('/kho2d')
def kho2d():
    return render_template('kho2d.html')

def clean_obj(obj):
    if isinstance(obj, (np.integer, np.int64)): return int(obj)
    elif isinstance(obj, (np.floating, np.float64)): return float(obj)
    elif isinstance(obj, np.ndarray): return obj.tolist()
    elif isinstance(obj, dict): return {str(k): clean_obj(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [clean_obj(i) for i in obj]
    else: return obj

@kho2d_bp.route('/api/data')
def get_data():
    if not engine: return jsonify({"status": "error", "message": "Lỗi kết nối DB"}), 500

    try:
        query = "SELECT [ID Cuộn Bó], [Vị trí], [Khối lượng], Nhóm, [SO Mapping] FROM kho"
        df = pd.read_sql(query, engine)
        df = df.fillna('') 
        
        data_hspm = {} 
        data_hrc1 = {} 
        data_hrc2 = {} 
        
        errors_by_wh = { "ALL": [], "HSPM": [], "HRC1": [], "HRC2": [], "OTHER": [] }
        auto_assigned_list = []
        
        stats = { "total": 0, "valid": 0, "invalid": 0, "auto_assigned": 0, "hspm_count": 0, "hrc1_count": 0, "hrc2_count": 0 }

        for _, row in df.iterrows():
            stats["total"] += 1
            
            coil_id = str(row['ID Cuộn Bó']).strip()
            pos = str(row['Vị trí']).strip().upper()
            raw_so = str(row['SO Mapping']).strip()
            so_val = raw_so if raw_so not in ['0', 'NONE', 'NAN', '', 'None'] else ""
            try: w_val = f"{float(row['Khối lượng']):,.0f}" if row['Khối lượng'] else "0"
            except: w_val = "0"
            group_val = str(row['Nhóm'])

            if not coil_id: continue

            # --- CASE 1: KHO HSPM ---
            if pos.startswith('H'):
                if len(pos) >= 9:
                    try:
                        zone = pos[0:3]; line_key = int(pos[4:6]); suffix = pos[6:]
                        if zone not in data_hspm: data_hspm[zone] = {}
                        if line_key not in data_hspm[zone]: data_hspm[zone][line_key] = {"fixed": {}, "pending": []}
                        data_hspm[zone][line_key]["fixed"][suffix] = [coil_id, pos, w_val, group_val, so_val]
                        stats["valid"] += 1; stats["hspm_count"] += 1
                        continue
                    except: pass
                elif len(pos) == 6:
                    try:
                        zone = pos[0:3]; line_key = int(pos[4:6])
                        if zone not in data_hspm: data_hspm[zone] = {}
                        if line_key not in data_hspm[zone]: data_hspm[zone][line_key] = {"fixed": {}, "pending": []}
                        data_hspm[zone][line_key]["pending"].append([coil_id, pos, w_val, group_val, so_val, True])
                        auto_assigned_list.append({"id": coil_id, "pos": pos, "reason": "Auto HSPM", "so": so_val})
                        stats["valid"] += 1; stats["auto_assigned"] += 1
                        continue
                    except: pass

            # --- CASE 2: KHO HRC1 ---
            elif pos.startswith('K'):
                if len(pos) >= 9:
                    try:
                        zone = pos[0:3]; line_key = int(pos[4:6]); suffix = pos[6:]
                        if zone not in data_hrc1: data_hrc1[zone] = {}
                        if line_key not in data_hrc1[zone]: data_hrc1[zone][line_key] = {"fixed": {}, "pending": []}
                        data_hrc1[zone][line_key]["fixed"][suffix] = [coil_id, pos, w_val, group_val, so_val]
                        stats["valid"] += 1; stats["hrc1_count"] += 1
                        continue
                    except: pass
                elif len(pos) == 6:
                    try:
                        zone = pos[0:3]; line_key = int(pos[4:6])
                        if zone not in data_hrc1: data_hrc1[zone] = {}
                        if line_key not in data_hrc1[zone]: data_hrc1[zone][line_key] = {"fixed": {}, "pending": []}
                        data_hrc1[zone][line_key]["pending"].append([coil_id, pos, w_val, group_val, so_val, True])
                        auto_assigned_list.append({"id": coil_id, "pos": pos, "reason": "Auto HRC1", "so": so_val})
                        stats["valid"] += 1; stats["auto_assigned"] += 1
                        continue
                    except: pass

            # --- CASE 3: KHO HRC2 ---
            match_hrc = re.match(r"^([N][A-G])[\.\s\-_]*0*(\d+)$", pos)
            if match_hrc:
                try:
                    zone = match_hrc.group(1); line_key = int(match_hrc.group(2))
                    if zone not in data_hrc2: data_hrc2[zone] = {}
                    if line_key not in data_hrc2[zone]: data_hrc2[zone][line_key] = []
                    data_hrc2[zone][line_key].append([coil_id, pos, w_val, group_val, so_val])
                    stats["valid"] += 1; stats["hrc2_count"] += 1
                    continue
                except: pass

            # --- CASE 4: XỬ LÝ LỖI (CÓ THÊM SO) ---
            stats["invalid"] += 1
            # [NEW] Thêm trường "so": so_val vào đây
            err_item = {"id": coil_id, "pos": pos, "so": so_val, "reason": "Sai định dạng/Chưa Mapping"}
            
            errors_by_wh["ALL"].append(err_item)
            if pos.startswith('H'): errors_by_wh["HSPM"].append(err_item)
            elif pos.startswith('K'): errors_by_wh["HRC1"].append(err_item)
            elif pos.startswith('N'): errors_by_wh["HRC2"].append(err_item)
            else: errors_by_wh["OTHER"].append(err_item)

        MAX_COILS_T1 = 100 # Giới hạn 100 cuộn tầng 1

        final_hrc2_data = {}
        for z, lines in data_hrc2.items():
            final_hrc2_data[z] = {}
            for l_key, items in lines.items():
                fixed_sim = {}
                for idx, coil_data in enumerate(items):
                    if idx < MAX_COILS_T1:
                        # Tầng 1: Gán các số lẻ (1, 3, 5... đến 199)
                        sim_pos_key = str((idx * 2) + 1)
                    else:
                        # Tầng 2: Gán các số chẵn (2, 4, 6...)
                        # idx = 100 (cuộn thứ 101) -> sẽ về vị trí 2 (đè lên đầu hàng)
                        idx_t2 = idx - MAX_COILS_T1
                        sim_pos_key = str((idx_t2 * 2) + 2)
                        
                    fixed_sim[sim_pos_key] = coil_data
                
                final_hrc2_data[z][l_key] = {"fixed": fixed_sim, "pending": []}

        response_data = {
            "status": "success",
            "data_hspm": data_hspm, "data_hrc1": data_hrc1, "data_hrc2": final_hrc2_data,
            "errors_by_wh": errors_by_wh, "auto_list": auto_assigned_list, "stats": stats
        }
        return jsonify(clean_obj(response_data))

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500