from flask import Blueprint, render_template, request, redirect, url_for
from uuid import uuid4
import os, shutil
import pandas as pd
from sqlalchemy import text
from storage_utils import load_metadata, get_lsx_by_id, save_metadata, update_metadata
from ProcessData import process_create_lsx
from datetime import datetime
import re
from auth.decorator import permission_required
from db import engine        
import pandas as pd
import openpyxl
import io
import sqlalchemy
from openpyxl.utils import get_column_letter 
from flask import (
    request, 
    send_file, 
    jsonify, 
    session, 
    Blueprint # Hoặc 'Flask' nếu bạn dùng 1 file
)
lsx_bp = Blueprint("lsx", __name__)


@lsx_bp.route("/danhsach")
@permission_required('manage_lsx')
def danhsach_lsx():
    """
    Liệt kê toàn bộ LSX từ metadata (file JSON).
    """
    metadata = load_metadata()
    lsx_list = [d for d in metadata if d.get("type") == "lsx"]  # ✅ chỉ lấy LSX
    if not lsx_list:
        return render_template("no_data.html")
    return render_template("danhsach.html", datasets=lsx_list)


@lsx_bp.route("/xem/<lsx_id>")
@permission_required('manage_lsx')
def xem_thong_tin_lsx(lsx_id):
    """
    Xem chi tiết 1 LSX.
    """
    selected = get_lsx_by_id(lsx_id)
    if not selected:
        return "Không tìm thấy LSX", 404
    return render_template("chitietlsx.html", lsx=selected)


@lsx_bp.route("/edit/<lsx_id>", methods=["GET", "POST"])
@permission_required('manage_lsx')
def edit_lsx(lsx_id):
    """
    Chỉnh sửa tên LSX.
    """
    entry = get_lsx_by_id(lsx_id)
    if not entry:
        return "Không tìm thấy LSX", 404

    if request.method == "POST":
        new_name = request.form.get("name", entry["name"])
        entry["name"] = new_name
        update_metadata(lsx_id, entry)     # cập nhật JSON
        return redirect(url_for("lsx.danhsach_lsx"))

    return render_template("edit_lsx.html", entry=entry)


@lsx_bp.route("/delete/<lsx_id>", methods=["POST"])
@permission_required('manage_lsx')
def delete_lsx(lsx_id):
    metadata = load_metadata()
    entry = get_lsx_by_id(lsx_id)
    if not entry:
        return "Không tìm thấy LSX", 404

    folder = os.path.dirname(entry["lsx"])
    try:
        if os.path.exists(folder):
            shutil.rmtree(folder)
    except Exception as e:
        return f"Lỗi khi xóa thư mục: {e}", 500

    # ✅ Giữ lại các bản ghi khác loại hoặc LSX khác ID
    metadata = [d for d in metadata if not (d.get("type") == "lsx" and d.get("id") == lsx_id)]
    save_metadata(metadata)

    # --- Xóa trong DB SQL Server ---
    tables = ["ton_kho", "report", "lsx"]
    with engine.begin() as conn:
        for table in tables:
            try:
                stmt = text(f"DELETE FROM {table} WHERE lsx_id = :lsx_id")
                result = conn.execute(stmt, {"lsx_id": lsx_id})
                print(f"Deleted {result.rowcount} rows from {table}")
            except Exception as e:
                print(f"Lỗi khi xóa bảng {table}: {e}")
                pass

    # --- Điều hướng ---
    remaining_lsx = [d for d in metadata if d.get("type") == "lsx"]
    if not remaining_lsx:
        return render_template("no_data.html")
    return redirect(url_for("lsx.danhsach_lsx"))
@lsx_bp.route('/api/get-lsx-data', methods=['GET'])
@permission_required('manage_lsx') # Thêm decorator để bảo vệ API
def get_lsx_data():
    try:
        import numpy as np

        sql = "SELECT * FROM LenhSanXuat_ChiTiet ORDER BY STT"
        
        # Dùng `engine` bạn đã tạo
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        # === SỬA LỖI: Thay thế NaN bằng None ===
        # Chuyển đổi tất cả các giá trị NaN trong DataFrame thành None của Python
        # trước khi gửi về client. Khi jsonify(None) sẽ tạo ra chuỗi 'null'
        # hợp lệ trong JSON, mà JavaScript có thể hiểu được.
        df = df.replace({np.nan: None})

        data_json = df.to_dict(orient='records')
        return jsonify(data_json)
        
    except Exception as e:
        print(f"Lỗi get_lsx_data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# =================================================================
# API 2: Lưu dữ liệu (POST)
# =================================================================
@lsx_bp.route('/api/save-lsx-data', methods=['POST'])
@permission_required('manage_lsx') # Thêm dòng này để kiểm tra quyền
def save_lsx_data():
    data_rows = request.get_json()
    if not data_rows:
        return jsonify({"status": "error", "message": "Không có dữ liệu"}), 400

    try:
        with engine.begin() as conn: # Bắt đầu một transaction
            # 1. Lấy danh sách ID từ frontend và ID từ database
            frontend_ids = {row.get('ID') for row in data_rows if row.get('ID') is not None}
            
            # Lấy tất cả ID hiện có trong bảng
            result = conn.execute(text("SELECT ID FROM LenhSanXuat_ChiTiet"))
            db_ids = {row[0] for row in result}

            # 2. Xác định các ID cần xóa
            ids_to_delete = db_ids - frontend_ids
            if ids_to_delete:
                # Chuyển set thành list để dùng trong câu lệnh IN
                delete_list = list(ids_to_delete)
                # Tạo placeholders cho câu lệnh IN
                placeholders = ', '.join([f':id_{i}' for i in range(len(delete_list))])
                delete_stmt = text(f"DELETE FROM LenhSanXuat_ChiTiet WHERE ID IN ({placeholders})")
                
                # Tạo dictionary cho các tham số
                params = {f'id_{i}': val for i, val in enumerate(delete_list)}
                conn.execute(delete_stmt, params)
                print(f"--- DEBUG: Đã xóa {len(ids_to_delete)} dòng với ID: {ids_to_delete} ---")

            # 3. Lặp qua dữ liệu từ frontend để INSERT hoặc UPDATE
            for row in data_rows:
                row_id = row.get('ID')
                params = {
                        "STT": row.get('STT'), "ThoiGianSX": row.get('ThoiGianSX'), "KichCo": row.get('KichCo'),
                        "MacThep": row.get('MacThep'), "SanLuong_1A": row.get('SanLuong_1A'), "SanLuong_1B": row.get('SanLuong_1B'),
                        "SanLuong_YeuCau_Cuon": row.get('SanLuong_YeuCau_Cuon'), "DungSai": row.get('DungSai'),
                        "CoTinh_GHC": row.get('CoTinh_GHC'), "CoTinh_GHB": row.get('CoTinh_GHB'), "CoTinh_GianDai": row.get('CoTinh_GianDai'),
                        "CoTinh_DoCung": row.get('CoTinh_DoCung'), "Phoi_MacPhoi": row.get('Phoi_MacPhoi'),
                        "Phoi_KichThuoc": row.get('Phoi_KichThuoc'), "YeuCauDacBiet": row.get('YeuCauDacBiet'),
                        "OrderNumber": row.get('OrderNumber'), "Batch": row.get('Batch'), "KL_Cuon": row.get('KL_Cuon'),
                        "MucDichSuDung": row.get('MucDichSuDung'), "KhachHang": row.get('KhachHang'),
                        "DotSX": row.get('DotSX'), # Thêm DotSX
                        "NguoiChinhSua": session.get('username', 'system')
                    }

                if row_id is not None and row_id in db_ids:
                    # --- UPDATE DÒNG HIỆN CÓ ---
                    update_stmt = sqlalchemy.text("""
                        UPDATE LenhSanXuat_ChiTiet SET
                            STT = :STT, ThoiGianSX = :ThoiGianSX, KichCo = :KichCo, MacThep = :MacThep, SanLuong_1A = :SanLuong_1A,
                            SanLuong_1B = :SanLuong_1B, SanLuong_YeuCau_Cuon = :SanLuong_YeuCau_Cuon, DungSai = :DungSai,
                            CoTinh_GHC = :CoTinh_GHC, CoTinh_GHB = :CoTinh_GHB, CoTinh_GianDai = :CoTinh_GianDai,
                            CoTinh_DoCung = :CoTinh_DoCung, Phoi_MacPhoi = :Phoi_MacPhoi, Phoi_KichThuoc = :Phoi_KichThuoc,
                            YeuCauDacBiet = :YeuCauDacBiet, OrderNumber = :OrderNumber, Batch = :Batch, KL_Cuon = :KL_Cuon,
                            MucDichSuDung = :MucDichSuDung, KhachHang = :KhachHang, DotSX = :DotSX,
                            DaChinhSua = 1, NguoiChinhSua = :NguoiChinhSua, ThoiGianCapNhat = GETDATE()
                        WHERE ID = :ID
                    """)
                    params['ID'] = row_id
                    conn.execute(update_stmt, params)
                else:
                    # --- INSERT DÒNG MỚI ---
                    insert_stmt = sqlalchemy.text("""
                        INSERT INTO LenhSanXuat_ChiTiet (
                            STT, ThoiGianSX, KichCo, MacThep, SanLuong_1A, SanLuong_1B, SanLuong_YeuCau_Cuon, DungSai,
                            CoTinh_GHC, CoTinh_GHB, CoTinh_GianDai, CoTinh_DoCung, Phoi_MacPhoi, Phoi_KichThuoc,
                            YeuCauDacBiet, OrderNumber, Batch, KL_Cuon, MucDichSuDung, KhachHang, DotSX,
                            DaChinhSua, NguoiChinhSua, ThoiGianCapNhat
                        ) VALUES (
                            :STT, :ThoiGianSX, :KichCo, :MacThep, :SanLuong_1A, :SanLuong_1B, :SanLuong_YeuCau_Cuon, :DungSai,
                            :CoTinh_GHC, :CoTinh_GHB, :CoTinh_GianDai, :CoTinh_DoCung, :Phoi_MacPhoi, :Phoi_KichThuoc,
                            :YeuCauDacBiet, :OrderNumber, :Batch, :KL_Cuon, :MucDichSuDung, :KhachHang, :DotSX,
                            1, :NguoiChinhSua, GETDATE()
                        )
                    """)
                    conn.execute(insert_stmt, params)
            
        return jsonify({"status": "success", "message": "Cập nhật thành công!"})
        
    except Exception as e:
        # Transaction sẽ tự động rollback khi thoát khỏi khối 'with' nếu có lỗi
        print(f"Lỗi save_lsx_data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500
from openpyxl.cell.cell import MergedCell
# =================================================================
# API 3: Xuất Excel (POST)
# (Route này không cần 'engine')
# =================================================================
def parse_thg_sx(input_text, current_year):

    try:
        parts = input_text.split('-')
        if len(parts) != 2: 
            return None, None

        part1_raw, part2_raw = parts[0].strip(), parts[1].strip()

        # 1. Phân tích ngày kết thúc (luôn có tháng, vd: '6/11')
        end_parts = part2_raw.split('/')
        if len(end_parts) != 2: 
            return None, None
        end_day = int(end_parts[0])
        end_month = int(end_parts[1])

        # 2. Phân tích ngày bắt đầu
        start_parts = part1_raw.split('/')
        if len(start_parts) == 2:
            # Dạng '30/10'
            start_day = int(start_parts[0])
            start_month = int(start_parts[1])
        elif len(start_parts) == 1:
            # Dạng '30' (cùng tháng hoặc tháng trước)
            start_day = int(start_parts[0])
            if start_day > end_day:
                # Nếu ngày bắt đầu > ngày kết thúc (vd: 30 > 6)
                # -> hiểu là tháng trước
                start_month = end_month - 1
                if start_month == 0: 
                    start_month = 12 # Xử lý trường hợp tháng 1
            else:
                # Nếu ngày bắt đầu <= ngày kết thúc (vd: 5-10/11)
                # -> hiểu là cùng tháng
                start_month = end_month
        else:
            return None, None

        # 3. Định dạng chuỗi (thêm zero-padding và năm)
        start_date_str = f"{start_day:02d}/{start_month:02d}/{current_year}"
        end_date_str = f"{end_day:02d}/{end_month:02d}/{current_year}"
        
        return start_date_str, end_date_str
        
    except Exception as e:
        print(f"--- LỖI parse_thg_sx: {e} ---")
        return None, None
from copy import copy
from openpyxl.styles import Alignment
def get_sheet_name_from_dotsx(dot_sx, year):
    """
    SỬA LỖI:
    Chuyển 'T11 1b' thành '01.11.2025'
    Chuyển 'T10 5b' thành '05.10.2025'
    """
    try:
        if not dot_sx or not isinstance(dot_sx, str):
            return f"Sheet_{year}"

        parts = dot_sx.split()
        
        # 1. Xử lý Tháng (luôn ở phần 1)
        month_part = parts[0] # vd: "T10"
        month_str = month_part.replace('T', '').strip()
        month_padded = month_str.zfill(2) # vd: "10"

        # 2. Xử lý Ngày (ở phần 2)
        day_padded = "01" # Mặc định là 01
        
        if len(parts) > 1:
            day_part_raw = parts[1] # vd: "5b"
            
            # Tìm số đầu tiên trong chuỗi này
            # re.search(r"^\d+", ...) tìm các chữ số ở ĐẦU chuỗi
            match = re.search(r"^\d+", day_part_raw) 
            
            if match:
                day_str = match.group(0) # vd: "5"
                day_padded = day_str.zfill(2) # vd: "05"

        # 3. Tạo tên
        new_name = f"{day_padded}.{month_padded}.{year}"
        return new_name
        
    except Exception as e:
        print(f"--- LỖI get_sheet_name_from_dotsx: {e}. Input: {dot_sx} ---")
        # Tên dự phòng nếu lỗi
        safe_name = re.sub(r'[\[\]\*\/\\?\:]', '_', dot_sx)
        return safe_name[:31]
def copy_sheet_between_workbooks(source_sheet, target_workbook, new_title):
    """
    Hàm copy thủ công 1 sheet (bao gồm style) từ workbook này sang workbook khác.
    """
    # 1. Tạo sheet mới trong workbook đích
    target_sheet = target_workbook.create_sheet(title=new_title)

    # 2. Copy giá trị và style của từng ô
    for row in source_sheet.iter_rows():
        for cell in row:
            
            # === BẮT ĐẦU SỬA LỖI ===
            # Bỏ qua các ô đã bị gộp (MergedCell), 
            # vì chúng chỉ là placeholder. Ô gốc (top-left) sẽ được copy.
            if isinstance(cell, MergedCell):
                continue
            # === KẾT THÚC SỬA LỖI ===

            new_cell = target_sheet.cell(row=cell.row, column=cell.col_idx, value=cell.value)
            if cell.has_style:
                new_cell.font = copy(cell.font)
                new_cell.border = copy(cell.border)
                new_cell.fill = copy(cell.fill)
                new_cell.number_format = cell.number_format
                new_cell.protection = copy(cell.protection)
                new_cell.alignment = copy(cell.alignment)

    # 3. Copy chiều cao của dòng
    for row_dim_key in source_sheet.row_dimensions:
        if source_sheet.row_dimensions[row_dim_key].height:
            target_sheet.row_dimensions[row_dim_key].height = source_sheet.row_dimensions[row_dim_key].height

    # 4. Copy độ rộng của cột
    for col_dim_key in source_sheet.column_dimensions:
        if source_sheet.column_dimensions[col_dim_key].width:
            target_sheet.column_dimensions[col_dim_key].width = source_sheet.column_dimensions[col_dim_key].width
            
    # 5. Copy các ô đã gộp (merged cells) - Logic này vẫn đúng
    for merge_range in source_sheet.merged_cells.ranges:
        target_sheet.merge_cells(str(merge_range))
        
    return target_sheet
@lsx_bp.route('/api/export-with-template', methods=['POST'])
def export_with_template():
    try:
        # --- BẮT ĐẦU LOGIC MỚI: TẠO 1 SHEET DUY NHẤT ---
        data_rows = request.get_json()
        if not data_rows:
            return jsonify({"status": "error", "message": "Không có dữ liệu"}), 400

        # 1. Tải trực tiếp file Template để làm việc trên đó
        TEMPLATE_PATH = "templates/excel/test.xlsx" 
        wb_out = openpyxl.load_workbook(TEMPLATE_PATH)
        
        # Lấy sheet đang hoạt động để điền dữ liệu
        ws = wb_out.active
        ws.title = "LSX Tong Hop"
        # 3. Lấy năm hiện tại
        current_year = datetime.today().year 

        # 4. Xử lý Header (A6 và B3)
        # --- Logic cập nhật B3 (Tiêu đề chính) ---
        try:
            original_b3_value = str(ws['B3'].value)
            new_b3_value = re.sub(r"Tháng \d+/\d{4}", "Tổng Hợp", original_b3_value)
            ws['B3'].value = new_b3_value
        except Exception as e:
            print(f"--- WARNING: Không thể cập nhật B3. Lỗi: {e} ---")

        # --- Logic cập nhật A6 (Dải ngày tổng hợp) ---
        all_start_dates, all_end_dates = [], []
        for row in data_rows:
            input_thg_sx = row.get('ThoiGianSX')
            if input_thg_sx:
                start_date_str, end_date_str = parse_thg_sx(input_thg_sx, current_year)
                if start_date_str: all_start_dates.append(datetime.strptime(start_date_str, "%d/%m/%Y"))
                if end_date_str: all_end_dates.append(datetime.strptime(end_date_str, "%d/%m/%Y"))
        
        if all_start_dates and all_end_dates:
            min_start_date = min(all_start_dates).strftime("%d/%m/%Y")
            max_end_date = max(all_end_dates).strftime("%d/%m/%Y")
            try:
                original_a6_value = str(ws['A6'].value)
                date_regex = r"\d{2}/\d{2}/\d{4}t"
                dates_to_insert = [min_start_date, max_end_date]
                def replacer(match):
                    if not dates_to_insert: return match.group(0)
                    return dates_to_insert.pop(0)
                new_a6_value = re.sub(r"\d{2}/\d{2}/\d{4}", replacer, original_a6_value, count=2)
                ws['A6'].value = new_a6_value
            except Exception as e:
                print(f"--- WARNING: Không thể cập nhật A6. Lỗi: {e} ---")

        # 5. Cấu hình ghi dữ liệu
        start_row_write = 10 
        start_row_insert = 11
        start_col = 1
        num_records = len(data_rows)

        # 6. Chèn thêm dòng nếu cần
        num_rows_to_insert = num_records - 1
        if num_rows_to_insert > 0:
            ws.insert_rows(start_row_insert, amount=num_rows_to_insert)

        # 7. Lặp qua dữ liệu và điền vào sheet
        for i, row in enumerate(data_rows):
            cur_row = start_row_write + i
            
            # Copy style từ dòng mẫu cho các dòng được chèn mới
            if i > 0:
                template_row = ws[start_row_write]
                for col_idx, template_cell in enumerate(template_row, start=1):
                    new_cell = ws.cell(row=cur_row, column=col_idx)
                    if template_cell.has_style:
                        new_cell.font = copy(template_cell.font)
                        new_cell.border = copy(template_cell.border)
                        new_cell.fill = copy(template_cell.fill)
                        new_cell.number_format = copy(template_cell.number_format)
                        new_cell.protection = copy(template_cell.protection)
                        new_cell.alignment = copy(template_cell.alignment)
                ws.row_dimensions[cur_row].height = ws.row_dimensions[start_row_write].height

            # Ghi dữ liệu vào các ô
            ws.cell(row=cur_row, column=start_col + 0, value=i + 1)
            
            # Xử lý và ghi cột ThoiGianSX
            original_thg_sx_row = row.get('ThoiGianSX')
            new_thg_sx_value = original_thg_sx_row
            if original_thg_sx_row:
                start_date_row, end_date_row = parse_thg_sx(original_thg_sx_row, current_year)
                if start_date_row and end_date_row:
                    new_thg_sx_value = f"08h00 {start_date_row}\n-\n08h00\n{end_date_row}"
                    cell_to_format = ws.cell(row=cur_row, column=start_col + 1)
                    cell_to_format.alignment = Alignment(wrapText=True, vertical='center', horizontal='center')
            ws.cell(row=cur_row, column=start_col + 1, value=new_thg_sx_value)

            # Ghi các cột còn lại
            ws.cell(row=cur_row, column=start_col + 2, value=row.get('KichCo'))
            ws.cell(row=cur_row, column=start_col + 3, value=row.get('MacThep'))
            ws.cell(row=cur_row, column=start_col + 4, value=row.get('SanLuong_1A'))
            ws.cell(row=cur_row, column=start_col + 5, value=row.get('SanLuong_1B'))
            ws.cell(row=cur_row, column=start_col + 6, value=row.get('SanLuong_YeuCau_Cuon'))
            ws.cell(row=cur_row, column=start_col + 7, value=row.get('DungSai'))
            ws.cell(row=cur_row, column=start_col + 8, value=row.get('CoTinh_GHC'))
            ws.cell(row=cur_row, column=start_col + 9, value=row.get('CoTinh_GHB'))
            ws.cell(row=cur_row, column=start_col + 10, value=row.get('CoTinh_GianDai'))
            ws.cell(row=cur_row, column=start_col + 11, value=row.get('CoTinh_DoCung'))
            ws.cell(row=cur_row, column=start_col + 12, value=row.get('Phoi_MacPhoi'))
            ws.cell(row=cur_row, column=start_col + 13, value=row.get('Phoi_KichThuoc'))
            ws.cell(row=cur_row, column=start_col + 14, value=row.get('YeuCauDacBiet'))
            ws.cell(row=cur_row, column=start_col + 15, value=row.get('OrderNumber'))
            ws.cell(row=cur_row, column=start_col + 16, value=row.get('Batch'))
            ws.cell(row=cur_row, column=start_col + 17, value=row.get('KL_Cuon'))
            ws.cell(row=cur_row, column=start_col + 18, value=row.get('MucDichSuDung'))
            ws.cell(row=cur_row, column=start_col + 19, value=row.get('KhachHang'))

        # 8. Cập nhật công thức SUM
        if num_records > 0:
            last_data_row_index = start_row_write + num_records - 1
            total_row_index = last_data_row_index + 1
            
            col_1A_idx, col_1B_idx, col_SLYC_idx = start_col + 4, start_col + 5, start_col + 6
            col_1A_letter, col_1B_letter, col_SLYC_letter = get_column_letter(col_1A_idx), get_column_letter(col_1B_idx), get_column_letter(col_SLYC_idx)
            
            formula_1A = f"=SUM({col_1A_letter}{start_row_write}:{col_1A_letter}{last_data_row_index})"
            formula_1B = f"=SUM({col_1B_letter}{start_row_write}:{col_1B_letter}{last_data_row_index})"
            formula_SLYC = f"=SUM({col_SLYC_letter}{start_row_write}:{col_SLYC_letter}{last_data_row_index})"
            
            ws.cell(row=total_row_index, column=col_1A_idx).value = formula_1A
            ws.cell(row=total_row_index, column=col_1B_idx).value = formula_1B
            ws.cell(row=total_row_index, column=col_SLYC_idx).value = formula_SLYC

        # 9. Lưu file vào buffer và trả về
        file_buffer = io.BytesIO()
        wb_out.save(file_buffer) # Lưu workbook đã được chỉnh sửa
        file_buffer.seek(0)

        return send_file(
            file_buffer,
            as_attachment=True,
            download_name=f'LSX_TongHop_{datetime.now().strftime("%Y%m%d")}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        print(f"Lỗi export_with_template: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500
@lsx_bp.route('/lap-lsx-form')
 # Thêm check quyền nếu cần
def lap_lsx():
    """
    Render trang Lập Form LSX bằng Handsontable.
    """
    # Bạn có thể query dữ liệu mặc định từ SQL Server ở đây nếu cần
    # (Nếu không, cứ để Handsontable tự fetch qua API như ta đã bàn)
    
    return render_template('laplsx.html')

# =================================================================
# API 4: Import Đơn Hàng từ Excel và ghi đè dữ liệu
# =================================================================
@lsx_bp.route('/api/import-don-hang', methods=['POST'])
@permission_required('manage_lsx')
def import_don_hang():
    # --- SỬA LỖI: Nhận file trực tiếp từ request ---
    if 'donhang_input_file' not in request.files:
        return jsonify({"status": "error", "message": "Không tìm thấy file 'donhang_input_file' trong request."}), 400

    file = request.files['donhang_input_file']

    if file.filename == '':
        return jsonify({"status": "error", "message": "Chưa chọn file nào."}), 400

    # Lưu file vào một vị trí tạm thời để xử lý
    temp_dir = os.path.join("uploads", "temp_imports")
    os.makedirs(temp_dir, exist_ok=True)
    unique_filename = f"{uuid4()}_{file.filename}"
    temp_file_path = os.path.join(temp_dir, unique_filename)
    file.save(temp_file_path)

    try:
        # Xử lý file tạm để lấy dữ liệu
        df_processed = process_create_lsx(temp_file_path)
        if df_processed is None:
            raise ValueError("File không hợp lệ hoặc không có dữ liệu (hàm process_create_lsx trả về None).")

        # === SỬA LỖI 1: Loại bỏ cột 'ID' khỏi DataFrame ===
        # Cột 'ID' là IDENTITY (tự động tăng) trong DB, không được chèn giá trị tường minh.
        df_processed = df_processed.drop(columns=['ID'], errors='ignore')

        # === SỬA LỖI 2: Định nghĩa kiểu dữ liệu cho các cột (đặc biệt là DotSX) ===
        # Điều này giúp tránh lỗi 'Invalid column name' và đảm bảo kiểu dữ liệu đúng.
        dtype_mapping = {
            'ThoiGianSX': sqlalchemy.types.NVARCHAR(100),
            'KichCo': sqlalchemy.types.NVARCHAR(50),
            'MacThep': sqlalchemy.types.NVARCHAR(50),
            'DungSai': sqlalchemy.types.NVARCHAR(20),
            'CoTinh_GHC': sqlalchemy.types.NVARCHAR(50),
            'CoTinh_GHB': sqlalchemy.types.NVARCHAR(50),
            'CoTinh_GianDai': sqlalchemy.types.NVARCHAR(50),
            'CoTinh_DoCung': sqlalchemy.types.NVARCHAR(50),
            'Phoi_MacPhoi': sqlalchemy.types.NVARCHAR(50),
            'Phoi_KichThuoc': sqlalchemy.types.NVARCHAR(50),
            'YeuCauDacBiet': sqlalchemy.types.NVARCHAR(255),
            'KL_Cuon': sqlalchemy.types.NVARCHAR(50),
            'MucDichSuDung': sqlalchemy.types.NVARCHAR(100),
            'KhachHang': sqlalchemy.types.NVARCHAR(100),
            'DotSX': sqlalchemy.types.NVARCHAR(50),
            'HasWarning': sqlalchemy.types.Boolean # Thêm cột DotSX vào mapping
        }

        # Ghi dữ liệu vào DB
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE LenhSanXuat_ChiTiet"))
            df_processed.to_sql(
                'LenhSanXuat_ChiTiet', 
                con=conn, 
                if_exists='append',
                index=False,
                dtype=dtype_mapping # Sử dụng dtype_mapping đã định nghĩa
            )
        
        return jsonify({"status": "success", "message": f"Import thành công {len(df_processed)} dòng dữ liệu."})

    except Exception as e:
        return jsonify({"status": "error", "message": f"Lỗi khi xác nhận import: {str(e)}"}), 500
    finally:
        # Luôn xóa file tạm sau khi xử lý xong (dù thành công hay thất bại)
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# =================================================================
# API 5: Xóa toàn bộ dữ liệu trong bảng
# =================================================================
@lsx_bp.route('/api/clear-lsx-data', methods=['POST'])
@permission_required('manage_lsx')
def clear_lsx_data():
    try:
        with engine.begin() as conn:
            # Dùng TRUNCATE TABLE để reset bảng nhanh và hiệu quả
            conn.execute(text("TRUNCATE TABLE LenhSanXuat_ChiTiet"))
        
        return jsonify({"status": "success", "message": "Đã xóa toàn bộ dữ liệu thành công."})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Đã xảy ra lỗi khi xóa dữ liệu: {str(e)}"}), 500
