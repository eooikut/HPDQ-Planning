from flask import Blueprint, render_template, request, flash
import pandas as pd
from caculator import run_calculation_tool # Import hàm logic ở trên

# Giả sử bạn có blueprint tên là 'tools_bp' hoặc tạo mới
tools_bp = Blueprint('tools', __name__, url_prefix='/tools')

@tools_bp.route('/production-calc', methods=['GET', 'POST'])
def production_calc():
    results = None
    error_msg = None
    
    if request.method == 'POST':
        try:
            df = pd.DataFrame()
            
            # CASE 1: Người dùng Upload File
            if 'file' in request.files and request.files['file'].filename != '':
                file = request.files['file']
                df = pd.read_excel(file)
            
            # CASE 2: Người dùng Nhập tay (Lấy từ Form)
            elif 'manual_width' in request.form:
                widths = request.form.getlist('manual_width')
                thicks = request.form.getlist('manual_thick')
                masses = request.form.getlist('manual_mass')
                
                # Tạo DataFrame từ dữ liệu nhập tay
                data = {
                    'Khổ rộng': [float(w) for w in widths if w],
                    'Chiều dày': [float(t) for t in thicks if t],
                    'Khối lượng': [float(m) for m in masses if m]
                }
                # Đảm bảo độ dài các list bằng nhau (tránh lỗi nếu user nhập thiếu dòng)
                min_len = min(len(data['Khổ rộng']), len(data['Chiều dày']), len(data['Khối lượng']))
                if min_len > 0:
                     df = pd.DataFrame({k: v[:min_len] for k, v in data.items()})

            if not df.empty:
                calc_result = run_calculation_tool(df)
                if isinstance(calc_result, dict) and 'error' in calc_result:
                    flash(calc_result['error'], 'danger')
                else:
                    results = calc_result
                    flash('Tính toán thành công!', 'success')
            else:
                flash('Chưa có dữ liệu đầu vào.', 'warning')

        except Exception as e:
            flash(f'Lỗi hệ thống: {str(e)}', 'danger')

    return render_template('production_calc.html', results=results)