import win32com.client
import time
import os
import sys
import subprocess
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta 

# ===============================
# 📌 CẤU HÌNH CHUNG
# ===============================
# Đảm bảo thư mục này đã tồn tại trên hệ thống
CUSTOM_DIR = r"C:\Users\Administrator\Desktop\ProjectPKH\data_auto_update"
LOG_PATH = os.path.join(CUSTOM_DIR, "master_export_log.txt")

# --- Hàm ghi log tập trung ---
def log_message(message, level="INFO"):
    """Ghi thông báo ra console và file log."""
    time_stamp = datetime.now().strftime('%H:%M:%S')
    full_message = f"[{time_stamp}] {level.upper()[:1]}️ {message}"
    print(full_message)
    
    # Ghi vào file log nếu là lỗi nghiêm trọng hoặc thông báo quan trọng
    if level in ["ERROR", "WARN", "CRITICAL", "SUCCESS"]: # Thêm SUCCESS vào đây để dễ theo dõi
        with open(LOG_PATH, "a", encoding="utf-8") as log:
             log.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {'✅' if level == 'SUCCESS' else '🛑'} {message}\n")


log_message(f"Thư mục đích chung: {CUSTOM_DIR}")

# --- CÁC ID SAP CHUNG (KHÔNG ĐƯỢC THAY ĐỔI) ---
MULTI_SELECT_TABLE_PATH = "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE"
MULTI_SELECT_INPUT_BASE = "ctxtRSCSEL_255-SLOW_I"

PLANT_INPUT_ID_ZPP04A = "wnd[0]/usr/ctxtS_WERKS-LOW"
STORAGE_LOC_BUTTON_ID = "wnd[0]/usr/btn%_S_LGORT_%_APP_%-VALU_PUSH"

DATE_FROM_ID_ZBC04B = "wnd[0]/usr/ctxtS_NGAYSX-LOW"
DATE_TO_ID_ZBC04B = "wnd[0]/usr/ctxtS_NGAYSX-HIGH"
PLANT_ID_ZBC04B = "wnd[0]/usr/ctxtS_WERKS-LOW" 
PRODUCT_GROUP_ID = "wnd[0]/usr/ctxtS_PX-LOW"
L1_CHECKBOX_ID = "wnd[0]/usr/chkP_L1"
L2_CHECKBOX_ID = "wnd[0]/usr/chkP_L2"

DATE_FROM_ID_ZSD04A = "wnd[0]/usr/ctxtS_VDATU-LOW"
DATE_TO_ID_ZSD04A = "wnd[0]/usr/ctxtS_VDATU-HIGH" 
ORDER_TYPE_BUTTON_ID = "wnd[0]/usr/btn%_S_AUART_%_APP_%-VALU_PUSH" 


# ===============================
# 📝 CẤU HÌNH TÁC VỤ (TASK CONFIGURATIONS)
# ===============================
TASK_CONFIGS = [
    # 1. ZSD04A (Chạy trước, khoảng 5-7 phút)
    {
        "name": "ZSD04A_ALL", 
        "tcode": "ZSD04A",
        "output_filename": "so.xlsx",
        "menu_export_path": "wnd[0]/mbar/menu[0]/menu[3]/menu[0]",
        "group": "SLOW", # Đánh dấu là SLOW
        "params": {
            "DATE_FROM": "{ZSD04A_FROM}",
            "DATE_TO": "{ZSD04A_TO}",
            "ORDER_TYPES_LIST": ["ZOR5", "ZOR6", "ZOR8", "ZOR7", "ZORI", "ZORZ", "ZORY"], 
        }
    },
    # 2. ZPP04A - HRC2 (Kho, ~1.5 phút)
    {
        "name": "ZPP04A_HRC2",
        "tcode": "ZPP04A",
        "output_filename": "kho_nm2.xlsx",
        "menu_export_path": "wnd[0]/mbar/menu[0]/menu[3]/menu[0]",
        "group": "FAST", # Đánh dấu là FAST
        "params": {
            "PLANT_VALUE": "1600",
            "STORAGE_LOCATIONS_LIST": ["1505", "1506"],
        }
    },
    # 3. ZPP04 - HRC1 (Kho, ~1.5 phút)
    {
        "name": "ZPP04_HRC1",
        "tcode": "ZPP04",
        "output_filename": "kho_nm1.xlsx",
        "menu_export_path": "wnd[0]/mbar/menu[0]/menu[3]/menu[0]",
        "group": "FAST", # Đánh dấu là FAST
        "params": {
            "PLANT_VALUE": "1000",
            "STORAGE_LOCATIONS_LIST": ["1519", "1522"],
        }
    },
    # 4. ZBC04B - HRC1 (Sản lượng, ~1.5 phút)
    {
        "name": "ZBC04B_HRC1",
        "tcode": "ZBC04B",
        "output_filename": "sanluong_nm1.xlsx",
        "menu_export_path": "wnd[0]/mbar/menu[0]/menu[1]/menu[0]",
        "group": "FAST", # Đánh dấu là FAST
        "params": {
            "DATE_FROM": "{ZBC04B_FROM}",
            "DATE_TO": "{ZBC04B_TO}",
            "PLANT_VALUE": "1000",
            "PRODUCT_GROUP_VALUE": "7",
            "UNCHECK_L1_L2": True,
        }
    },
    # 5. ZBC04B - HRC2 (Sản lượng, ~1.5 phút)
    {
        "name": "ZBC04B_HRC2",
        "tcode": "ZBC04B",
        "output_filename": "sanluong_nm2.xlsx",
        "menu_export_path": "wnd[0]/mbar/menu[0]/menu[1]/menu[0]",
        "group": "FAST", # Đánh dấu là FAST
        "params": {
            "DATE_FROM": "{ZBC04B_FROM}",
            "DATE_TO": "{ZBC04B_TO}",
            "PLANT_VALUE": "1600",
            "PRODUCT_GROUP_VALUE": "8",
            "UNCHECK_L1_L2": True,
        }
    },
]

# ===============================
# ⚙️ HÀM TÍNH TOÁN NGÀY THÁNG ĐỘNG 
# ===============================
def calculate_dynamic_dates():
    """Tính toán các ngày động theo yêu cầu."""
    today = date.today()
    tomorrow = today + timedelta(days=1)
    
    # Định dạng ngày theo chuẩn SAP (DD.MM.YYYY)
    today_sap_format = today.strftime("%d.%m.%Y")
    tomorrow_sap_format = tomorrow.strftime("%d.%m.%Y")
    
    # 1. ZSD04A: Ngày đầu tiên của 6 tháng trước (Ngày kết thúc là Hôm nay).
    six_months_ago = today - relativedelta(months=5)
    start_date_zsd04a = six_months_ago.replace(day=1)
    start_date_zsd04a_sap_format = start_date_zsd04a.strftime("%d.%m.%Y")
    
    # 2. ZBC04B: Ngày bắt đầu là (Ngày mai - 26 ngày) và Ngày kết thúc là (Ngày mai).
    start_date_zbc04b = tomorrow - timedelta(days=26)
    start_date_zbc04b_sap_format = start_date_zbc04b.strftime("%d.%m.%Y")
    
    date_map = {
        "ZSD04A_FROM": start_date_zsd04a_sap_format,
        "ZSD04A_TO": today_sap_format,
        "ZBC04B_FROM": start_date_zsd04a_sap_format,
        "ZBC04B_TO": today_sap_format,
    }
    
    log_message("Ngày tính toán động:")
    log_message(f"  - ZSD04A Start Date: {date_map['ZSD04A_FROM']} (End: {date_map['ZSD04A_TO']})")
    log_message(f"  - ZBC04B Start Date: {date_map['ZBC04B_FROM']} (End: {date_map['ZBC04B_TO']})")
    
    return date_map

# 🟢 HÀM: TẮT EXCEL ĐỂ GIẢI PHÓNG FILE LOCK 
def force_close_excel_processes():
    """Tắt tất cả các tiến trình Excel đang chạy để giải phóng file lock."""
    log_message("Đang buộc đóng tất cả tiến trình EXCEL.EXE...", level="WARN")
    try:
        # Lệnh /f (force) và /im (image name)
        result = subprocess.run(
            ['taskkill', '/f', '/im', 'excel.exe'],
            capture_output=True,
            text=True,
            check=False
        )
        # Sử dụng log_message()
        if "ERROR: The process" in result.stdout or "Không tìm thấy tiến trình" in result.stderr:
            log_message("Không tìm thấy tiến trình Excel nào để đóng.")
        else:
            log_message("Đã tắt thành công các tiến trình Excel đang chạy.", level="SUCCESS")

    except Exception as e:
        log_message(f"Lỗi khi cố gắng tắt Excel bằng Taskkill: {e}", level="ERROR")

# 🔌 KẾT NỐI TỚI SAP 
def sap_connect():
    """Kết nối tới SAP session hiện tại."""
    try:
        SapGuiAuto = win32com.client.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        connection = application.Children(0)
        session = connection.Children(0)
        log_message("Đã kết nối tới SAP.", level="SUCCESS")
        return session
    except Exception as e:
        error_message = f"Lỗi kết nối SAP. Đảm bảo SAP GUI đang mở và đã bật Scripting. Lỗi: {e}"
        log_message(error_message, level="CRITICAL") # Sử dụng log_message cho critical error
        sys.exit(1)

# 🚀 HÀM ĐIỀN THAM SỐ LỌC LINH HOẠT (Cập nhật để nhận wait_seconds)
def run_tcode_and_fill_selections(session, config, wait_seconds):
    """Chạy T-Code và điền các tham số lọc dựa trên cấu hình."""
    tcode = config['tcode']
    params = config['params']
    log_message(f"Bắt đầu chạy {tcode} cho tác vụ: {config['name']}...")
    
    try:
        session.StartTransaction(tcode)
        time.sleep(2) # Chờ màn hình T-Code load

        # --- Logic điền tham số (giữ nguyên, đã rất tốt) ---
        if tcode in ["ZPP04A", "ZPP04"]:
            plant_value = params.get("PLANT_VALUE")
            storage_locations_list = params.get("STORAGE_LOCATIONS_LIST", [])

            session.findById(PLANT_INPUT_ID_ZPP04A).text = plant_value
            log_message(f"Lọc Plant: {plant_value}")

            if storage_locations_list:
                session.findById(STORAGE_LOC_BUTTON_ID).press()
                time.sleep(1)
                for index, location in enumerate(storage_locations_list):
                    input_id = f"{MULTI_SELECT_TABLE_PATH}/{MULTI_SELECT_INPUT_BASE}[0,{index}]"
                    session.findById(input_id).text = location
                session.findById("wnd[1]/tbar[0]/btn[8]").press() # Nhấn Copy (F8)
                log_message(f"Lọc Storage Locs: {', '.join(storage_locations_list)}")
                time.sleep(1)

        elif tcode == "ZBC04B":
            session.findById(DATE_FROM_ID_ZBC04B).text = params["DATE_FROM"]
            session.findById(DATE_TO_ID_ZBC04B).text = params["DATE_TO"]
            session.findById(PLANT_ID_ZBC04B).text = params["PLANT_VALUE"]
            session.findById(PRODUCT_GROUP_ID).text = params["PRODUCT_GROUP_VALUE"]
            
            if params.get("UNCHECK_L1_L2", False):
                session.findById(L1_CHECKBOX_ID).selected = False
                session.findById(L2_CHECKBOX_ID).selected = False

            log_message(f"Lọc Ngày SX (S_NGAYSX): {params['DATE_FROM']} đến {params['DATE_TO']}")
            log_message(f"Lọc Plant: {params['PLANT_VALUE']}, Product Group: {params['PRODUCT_GROUP_VALUE']}")

        elif tcode == "ZSD04A":
            date_from_value = params["DATE_FROM"]
            date_to_value = params["DATE_TO"]
            order_types_list = params.get("ORDER_TYPES_LIST", [])

            session.findById(DATE_FROM_ID_ZSD04A).text = date_from_value
            session.findById(DATE_TO_ID_ZSD04A).text = date_to_value
            log_message(f"Lọc Ngày đơn hàng (S_VDATU): {date_from_value} đến {date_to_value}")

            if order_types_list:
                session.findById(ORDER_TYPE_BUTTON_ID).press()
                time.sleep(1)

                for index, order_type in enumerate(order_types_list):
                    input_id = f"{MULTI_SELECT_TABLE_PATH}/{MULTI_SELECT_INPUT_BASE}[0,{index}]"
                    try:
                        session.findById(input_id).text = order_type
                    except Exception as e_fill:
                        # Log lỗi điền, nhưng không crash. Rất quan trọng khi danh sách dài (7 giá trị)
                        log_message(f"Không thể điền giá trị thứ {index+1} ({order_type}). Lỗi: {e_fill}. Thoát vòng lặp điền.", level="WARN")
                        break # Thoát vòng lặp nếu có lỗi điền

                session.findById("wnd[1]/tbar[0]/btn[8]").press()
                time.sleep(1)
                log_message(f"Lọc Loại Đơn hàng (S_AUART): {', '.join(order_types_list)}")

        else:
            raise ValueError(f"T-Code {tcode} chưa được hỗ trợ trong hàm này.")

        # Thực thi báo cáo
        session.findById("wnd[0]").sendVKey(8)    # F8 = Execute
        log_message(f"Đã chạy báo cáo. Đang chờ {wait_seconds}s...")
        time.sleep(wait_seconds) # Dùng thời gian chờ động
        # Đoạn code trong hàm run_tcode_and_fill_selections
        if tcode == "ZPP04":
            log_message("Đang kiểm tra popup hoặc màn hình xác nhận sau F8 cho ZPP04...", level="INFO")
            time.sleep(3) # Chờ giao diện phản hồi

            # Ưu tiên kiểm tra pop-up wnd[1] trước
            try:
                # Nếu có cửa sổ pop-up (wnd[1]) hiện lên
                popup_window = session.findById("wnd[1]")
                log_message("Phát hiện cửa sổ pop-up (wnd[1]). Đang đóng...", level="INFO")
                popup_window.close() # Lệnh đóng cửa sổ pop-up
                time.sleep(1)
                log_message("Đã đóng cửa sổ pop-up (wnd[1]) thành công.", level="SUCCESS")
                back_button = session.findById("wnd[0]/tbar[0]/btn[3]")
                back_button.press()
                    # Sửa lại log cho chính xác
                log_message("Đã nhấn nút 'Back' (btn[3]) trên cửa sổ chính để quay lại.", level="SUCCESS")
                time.sleep(2)
            except:
                    # Nếu cả hai đều không thành công, ghi nhận và bỏ qua
                log_message("Không tìm thấy pop-up (wnd[1]) hay nút Back (wnd[0]/btn[3]). Bỏ qua bước xác nhận.", level="INFO")


                    # Tìm nút Back trên thanh công cụ của cửa sổ chính
                    
        # --- Cải tiến: Xử lý cửa sổ thông báo (ví dụ: No data found) ---
        if session.Children.Count > 0 and session.Children(0).Type == "GuiModalWindow":
            modal_window = session.Children(0)
            status_text = ""
            try:
                # Cố gắng lấy văn bản thông báo trên thanh trạng thái (status bar) của cửa sổ chính
                status_text = session.findById("wnd[0]/sbar").Text
            except:
                pass

            if "Không tìm thấy dữ liệu" in status_text or "No data found" in status_text or "Dữ liệu không đủ" in status_text:
                log_message(f"Báo cáo {tcode} không tìm thấy dữ liệu hoặc có cảnh báo. Status: {status_text}. Đang đóng cửa sổ cảnh báo/modal.", level="WARN")
                
                # Nhấn nút OK (hoặc tương đương) trên cửa sổ modal (wnd[1])
                try:
                    modal_window.sendVKey(0) # Gửi Enter (OK)
                except:
                    # Nếu không phải cửa sổ đơn giản, thử phím F12 (Cancel)
                    modal_window.sendVKey(12) 
                time.sleep(1)
                
                # Sau khi đóng modal, quay lại màn hình chọn
                if session.ActiveWindow.Name != "wnd[0]": # Nếu vẫn còn ở cửa sổ chọn tham số
                    session.findById("wnd[0]").sendVKey(12) # F12 (Cancel) để đảm bảo thoát
                
                raise Exception("NO_DATA_FOUND") # Báo hiệu không có dữ liệu để Export

        # Kiểm tra lại xem có lỗi nào làm Script dừng không (ví dụ: dump)
        if session.ActiveWindow.Name != "wnd[0]":
            log_message(f"Cửa sổ hiện tại không phải cửa sổ chính (wnd[0]) sau khi chạy T-Code.", level="WARN")

    except Exception as e:
        log_message(f"Lỗi khi điền tham số lọc hoặc thực thi cho {tcode}: {e}", level="ERROR")
        try:
             # Cố gắng quay lại màn hình chính nếu có lỗi.
             session.findById("wnd[0]").sendVKey(12) # F12 (Cancel)
             session.findById("wnd[0]").sendVKey(12) # Lần 2 nếu cần
        except:
             pass
        time.sleep(3)
        raise

# 📤 EXPORT VÀ LƯU TRỰC TIẾP 
def export_data_to_excel(session, output_filename, custom_dir, menu_export_path):
    """Sử dụng Menu Bar để Export và ÉP SAP lưu TRỰC TIẾP vào thư mục ĐÍCH cuối cùng."""
    log_message("Đang Export dữ liệu bằng Menu Bar...")
    
    # ---------------------------------------------
    # GỌI LỆNH MENU BAR
    # ---------------------------------------------
    try:
        session.findById(menu_export_path).select()
        log_message("Đã gửi lệnh Menu Bar Export thành công.")
    except Exception as e:
        log_message(f"Export thất bại với Menu Bar. Lỗi: {e}", level="ERROR")
        raise Exception("EXPORT_FAILED")

    time.sleep(2) # Chờ cửa sổ save file xuất hiện
    
    # 2. Xử lý cửa sổ Save File (wnd[1])
    try:
        save_window_id = "wnd[1]"
        
        # Cố gắng xử lý trường hợp có nút "Unconverted" (Nếu có, nó thường là btn[20])
        try:
            unconverted_button = session.findById(f"{save_window_id}/tbar[0]/btn[20]")
            unconverted_button.press()
            time.sleep(1)
        except:
            # Nếu không tìm thấy nút 20, tiếp tục
            pass
        
        # 2b. ÉP ĐƯỜNG DẪN ĐÍCH VÀ TÊN FILE
        session.findById(f"{save_window_id}/usr/ctxtDY_FILENAME").text = output_filename
        session.findById(f"{save_window_id}/usr/ctxtDY_PATH").text = custom_dir
        
        session.findById(f"{save_window_id}/usr/ctxtDY_FILENAME").setFocus()
        time.sleep(1)
        
        # 2c. Nút SAVE (btn[0])
        session.findById(f"{save_window_id}/tbar[0]/btn[0]").press()
        
        # **QUAN TRỌNG:** Tăng thời gian chờ ở đây cho việc lưu file
        # Vì ZSD04A có thể tạo file rất lớn, quá trình ghi file có thể mất thời gian
        time.sleep(15) 
        
        log_message("Export trực tiếp hoàn tất.", level="SUCCESS")
        
    except Exception as e:
        log_message(f"Lỗi khi xử lý cửa sổ Save File. Lỗi: {e}", level="ERROR")
        raise Exception("SAVE_WINDOW_FAILED")
#session.findById("wnd[1]").close
#session.findById("wnd[0]/tbar[0]/btn[3]").press
# ===============================
# MAIN EXECUTION SEQUENCE
# ===============================
def main_sequence():
    """Thực hiện tuần tự tất cả 5 tác vụ SAP Export đã cấu hình."""
    
    # 0. Kết nối, Kiểm tra thư mục và TÍNH TOÁN NGÀY THÁNG ĐỘNG
    date_map = calculate_dynamic_dates()
    
    # 🟢 ĐIỀN DỮ LIỆU ĐỘNG VÀO CONFIGS
    for config in TASK_CONFIGS:
        for key, value in config['params'].items():
            if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
                config['params'][key] = date_map.get(value.strip("{}"), value)

    if not os.path.isdir(CUSTOM_DIR):
        log_message(f"Lỗi: Thư mục đích không tồn tại: {CUSTOM_DIR}", level="CRITICAL")
        sys.exit(1)

    configs_to_process = TASK_CONFIGS
    log_message(f"Bắt đầu Full Run. Tổng cộng {len(configs_to_process)} tác vụ.")
        
    if not configs_to_process:
        log_message("Không có tác vụ nào để chạy. Kết thúc.", level="WARN")
        return
        
    # 1. Buộc đóng Excel và Thiết lập kết nối SAP
    force_close_excel_processes()
    time.sleep(1)
    sap_session = sap_connect()

    # 2. Lặp qua từng tác vụ và chạy tuần tự
    for config in configs_to_process:
        
        # --- LOGIC THỜI GIAN CHỜ ĐỘNG ---
        if config['group'] == "SLOW":
            execution_wait = 15 # 60 giây chờ cho báo cáo nặng (ZSD04A)
            log_message("Cấu hình thời gian chờ: 60s (Tác vụ SLOW).")
        elif config['group'] == "FAST":
            execution_wait = 10 # 10 giây chờ cho báo cáo nhẹ (ZBC04B, ZPP04)
            log_message("Cấu hình thời gian chờ: 10s (Tác vụ FAST).")  
        task_name = config['name']
        output_filename = config['output_filename']
        menu_export_path = config['menu_export_path']
        FINAL_FILE_PATH = os.path.join(CUSTOM_DIR, output_filename)
        
        print("\n" + "="*75)
        log_message(f"Bắt đầu TÁC VỤ: {task_name}", level="INFO")
        print("="*75)

        try:
            # A. Xóa file cũ 
            if os.path.exists(FINAL_FILE_PATH):
                try:
                    os.remove(FINAL_FILE_PATH)
                    log_message("Đã xóa file cũ.")
                except Exception as e_remove:
                    log_message(f"KHÔNG THỂ XÓA FILE CŨ. Bỏ qua tác vụ này. Lỗi: {e_remove}", level="ERROR")
                    continue 

            # B. Chạy T-Code và điền tham số (Truyền thời gian chờ vào)
            run_tcode_and_fill_selections(sap_session, config, execution_wait)
            
            # C. Export dữ liệu
            export_data_to_excel(sap_session, output_filename, CUSTOM_DIR, menu_export_path)
            log_message(f"Tác vụ {task_name} HOÀN TẤT THÀNH CÔNG!", level="SUCCESS")
            if config['tcode'] == "ZSD04A":
                log_message("Đang tắt Excel sau khi export ZSD04A để giải phóng bộ nhớ...", level="INFO")
                force_close_excel_processes()
                time.sleep(2)
            
        except Exception as e_task:
            if str(e_task) == "NO_DATA_FOUND":
                log_message(f"Tác vụ {task_name} bị bỏ qua do không có dữ liệu để export.", level="WARN")
            else:
                log_message(f"Lỗi trong quá trình thực thi/Export/Save File. Chuyển sang tác vụ tiếp theo.", level="ERROR")
        
        # D. Thoát T-Code hiện tại
        try:
            # Quay lại màn hình chính
            sap_session.findById("wnd[0]/tbar[0]/okcd").text = "/n"
            sap_session.findById("wnd[0]").sendVKey(0) # VKey 0 là Enter
            log_message(f"Đã thoát T-Code {config['tcode']}.")
            time.sleep(3)
        except Exception as e_quit:
            log_message(f"Lỗi khi thoát T-Code: {e_quit}. Vẫn tiếp tục.", level="WARN")
            time.sleep(3)


    log_message("Đang buộc đóng EXCEL lần cuối để sẵn sàng cho lần chạy tiếp theo.", level="INFO")
    force_close_excel_processes()
    log_message("QUÁ TRÌNH MASTER EXPORT HOÀN TẤT.", level="SUCCESS")


if __name__ == "__main__":
    main_sequence()
