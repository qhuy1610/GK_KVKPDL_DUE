

# import pandas as pd
# import re
# import json
# import boto3
# import numpy as np
# from io import BytesIO

# # ==========================================
# # 1. CẤU HÌNH KẾT NỐI MINIO S3
# # ==========================================
# S3_ENDPOINT   = "http://minio:9000"
# S3_ACCESS_KEY = "JXHpslDC7PokmWYb"
# S3_SECRET_KEY = "iVNGopKzabBfzrEFGfj08kuM6KFve2Xv"
# BUCKET_BRONZE = "test"
# BUCKET_SILVER = "silver"

# s3_client = boto3.client(
#     's3',
#     endpoint_url=S3_ENDPOINT,
#     aws_access_key_id=S3_ACCESS_KEY,
#     aws_secret_access_key=S3_SECRET_KEY
# )

# def read_json_s3(key):
#     obj = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=key)
#     return json.loads(obj['Body'].read().decode('utf-8'))

# def write_csv_s3(df, key):
#     csv_buffer = BytesIO()
#     df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
#     s3_client.put_object(Bucket=BUCKET_SILVER, Key=key, Body=csv_buffer.getvalue())
# # 2. CÁC HÀM XỬ LÝ (HELPER FUNCTIONS)



# def loai_bo_dau_tieng_viet(s):
#     # Bảng mã chuyển đổi các ký tự có dấu sang không dấu
#     accents = {
#         'a': 'áàảãạăắằẳẵặâấầẩẫậ',
#         'A': 'ÁÀẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬ',
#         'd': 'đ',
#         'D': 'Đ',
#         'e': 'éèẻẽẹêếềểễệ',
#         'E': 'ÉÈẺẼẸÊẾỀỂỄỆ',
#         'i': 'íìỉĩị',
#         'I': 'ÍÌỈĨỊ',
#         'o': 'óòỏõọôốồổỗộơớờởỡợ',
#         'O': 'ÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢ',
#         'u': 'úùủũụưứừửữự',
#         'U': 'ÚÙỦŨỤƯỨỪỬỮỰ',
#         'y': 'ýỳỷỹỵ',
#         'Y': 'ÝỲỶỸỊ'
#     }
#     for char_non_accent, chars_accent in accents.items():
#         for char in chars_accent:
#             s = s.replace(char, char_non_accent)
#     return s

# def tao_id_giangvien(row):
#     # 1. Bỏ học vị (Lưu ý thêm dấu \ trước các dấu chấm)
#     # Thêm \s* để xử lý khoảng trắng sau học vị
#     name = re.sub(r'^(GS\.TSKH\.|GS\.TS\.|PGS\.TS\.|TS\.|ThS\.|Ths\.|CN\.|KS\.|BS\.)\s*', '', str(row["Họ và tên"]))

#     # 2. Chuyển tên về không dấu trước khi lấy chữ cái đầu
#     name_no_accent = loai_bo_dau_tieng_viet(name)
    
#     # 3. Lấy chữ cái đầu (đã sạch dấu)
#     initials = ''.join([w[0].upper() for w in name_no_accent.split()])

#     # 4. Ngày sinh (xử lý trường hợp có "/" hoặc để trống)
#     dob = str(row["Năm sinh"]).replace("/", "") if pd.notna(row["Năm sinh"]) else "000000"

#     # 5. STT (Số thứ tự người)
#     stt = str(row["STT người"])

#     # 6. Số file (Trích xuất số từ tên file)
#     file_match = re.findall(r'\d+', str(row["File"]))
#     file_num = file_match[0] if file_match else "0"

#     return f"{initials}{dob}{stt}f{file_num}"

# #thongtinchung


# def thongtinchung(raw_text):
#     if not isinstance(raw_text, str) or not raw_text.strip():
#         return pd.DataFrame()

#     text = raw_text.replace("\n", " ").replace("\t", " ")
#     text = re.sub(r"\s+", " ", text).strip()

#     data = {}

#     def clean_val(val):
#         if val is None:
#             return None
#         val = val.strip(" ;:.").strip()
#         return val if val else None

#     def get_value(pattern, text):
#         match = re.search(pattern, text, flags=re.IGNORECASE)
#         if match:
#             return clean_val(match.group(1))
#         return None

#     # Các mốc dừng để tránh bắt sang cột sau
#     stop_after_linhvuc = r"(?=\s*(?:Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"
#     stop_after_ngoaingu = r"(?=\s*(?:Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"
#     stop_after_diachi = r"(?=\s*(?:Điện\s*thoại|Mobile|Email)\s*:|$)"
#     stop_after_chucvu = r"(?=\s*(?:Học\s*vị|Dạy\s*CN|Lĩnh\s*vực\s*NC|Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"

#     # 1. Các trường cơ bản
#     data["Họ và tên"] = get_value(
#         r"Họ\s*và\s*tên\s*:?\s*(.*?)(?=\s*Giới\s*tính\s*:|$)", text
#     )

#     data["Giới tính"] = get_value(
#         r"Giới\s*tính\s*:?\s*(.*?)(?=\s*Năm\s*sinh\s*:|$)", text
#     )

#     data["Năm sinh"] = get_value(
#         r"Năm\s*sinh\s*:?\s*(.*?)(?=\s*(?:Nơi\s*sinh|Nơisinh)\s*:|$)", text
#     )

#     data["Nơi sinh"] = get_value(
#         r"(?:Nơi\s*sinh|Nơisinh)\s*:?\s*(.*?)(?=\s*(?:Quê\s*quán|Quêquán)\s*:|$)", text
#     )

#     data["Quê quán"] = get_value(
#         r"(?:Quê\s*quán|Quêquán)\s*:?\s*(.*?)(?=\s*Tốt\s*nghiệp\s*ĐH\s*chuyên\s*ngành\s*:|$)", text
#     )

#     # 2. Khối tốt nghiệp ĐH
#     block_dh = get_value(
#         r"Tốt\s*nghiệp\s*ĐH\s*chuyên\s*ngành\s*:?\s*(.*?)(?=\s*(?:Đơn\s*vị\s*công\s*tác|Đơnvịcôngtác)\s*:|$)",
#         text
#     )

#     data["Tốt nghiệp ĐH chuyên ngành"] = None
#     data["Tại_ĐH"] = None
#     if block_dh:
#         data["Tốt nghiệp ĐH chuyên ngành"] = get_value(
#             r"^(.*?)(?=;\s*Tại\s*:|$)", block_dh
#         )
#         data["Tại_ĐH"] = get_value(
#             r";\s*Tại\s*:?\s*(.*)$", block_dh
#         )

#     # 3. Đơn vị công tác + tách khoa / trường
#     donvi = get_value(
#         r"(?:Đơn\s*vị\s*công\s*tác|Đơnvịcôngtác)\s*:?\s*(.*?)(?=\s*Chức\s*vụ\s*:|\s*Học\s*vị\s*:|\s*Dạy\s*CN\s*:|\s*Lĩnh\s*vực\s*NC\s*:|\s*Ngoại\s*ngữ\s*:|\s*Ngoạingữ\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
#         text
#     )
#     data["Đơn vị công tác"] = donvi

#     data["Khoa"] = None
#     data["Trường/Cơ quan"] = None
#     if donvi:
#         parts = [x.strip() for x in donvi.split(";")]
#         if len(parts) >= 1:
#             data["Khoa"] = clean_val(parts[0])
#         if len(parts) >= 2:
#             data["Trường/Cơ quan"] = clean_val(";".join(parts[1:]))

#     # 4. Chức vụ
#     data["Chức vụ"] = get_value(
#         rf"Chức\s*vụ\s*:?\s*(.*?){stop_after_chucvu}", text
#     )

#     # 5. Khối học vị
#     block_hocvi = get_value(
#         r"Học\s*vị\s*:?\s*(.*?)(?=\s*Dạy\s*CN\s*:|\s*Lĩnh\s*vực\s*NC\s*:|\s*Ngoại\s*ngữ\s*:|\s*Ngoạingữ\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
#         text
#     )

#     data["Học vị"] = None
#     data["năm"] = None
#     data["Chuyên ngành"] = None
#     data["Tại_Học vị"] = None

#     if block_hocvi:
#         data["Học vị"] = get_value(
#             r"^(.*?)(?=;\s*năm\s*:|;\s*Chuyên\s*ngành\s*:|;\s*Tại\s*:|$)",
#             block_hocvi
#         )
#         data["năm"] = get_value(
#             r";\s*năm\s*:?\s*(.*?)(?=;\s*Chuyên\s*ngành\s*:|;\s*Tại\s*:|$)",
#             block_hocvi
#         )
#         data["Chuyên ngành"] = get_value(
#             r";\s*Chuyên\s*ngành\s*:?\s*(.*?)(?=;\s*Tại\s*:|$)",
#             block_hocvi
#         )
#         data["Tại_Học vị"] = get_value(
#             r";\s*Tại\s*:?\s*(.*?)(?=\s*(?:Dạy\s*CN|Lĩnh\s*vực\s*NC|Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)",
#             block_hocvi
#         )

#     # 6. Dạy chuyên ngành
#     data["Dạy CN"] = get_value(
#         r"Dạy\s*CN\s*:?\s*(.*?)(?=\s*Lĩnh\s*vực\s*NC\s*:|\s*(?:Ngoại\s*ngữ|Ngoạingữ)\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
#         text
#     )

#     # 7. Lĩnh vực nghiên cứu
#     # Không có thì None, có thì dừng trước các cột sau
#     linhvuc_match = re.search(
#         rf"Lĩnh\s*vực\s*NC\s*:?\s*(.*?){stop_after_linhvuc}",
#         text,
#         flags=re.IGNORECASE
#     )
#     data["Lĩnh vực NC"] = clean_val(linhvuc_match.group(1)) if linhvuc_match else None

#     # 8. Ngoại ngữ
#     data["Ngoại ngữ"] = get_value(
#         rf"(?:Ngoại\s*ngữ|Ngoạingữ)\s*:?\s*(.*?){stop_after_ngoaingu}",
#         text
#     )

#     # 9. Địa chỉ liên hệ
#     data["Địa chỉ liên hệ"] = get_value(
#         rf"Địa\s*chỉ\s*liên\s*hệ\s*:?\s*(.*?){stop_after_diachi}",
#         text
#     )

#     # 10. Điện thoại / Mobile / Email
#     data["Điện thoại"] = get_value(
#         r"Điện\s*thoại\s*:?\s*(.*?)(?=\s*Mobile\s*:|\s*Email\s*:|$)",
#         text
#     )

#     data["Mobile"] = get_value(
#         r"Mobile\s*:?\s*(.*?)(?=\s*Email\s*:|$)",
#         text
#     )

#     data["Email"] = get_value(
#         r"Email\s*:?\s*(.*)$",
#         text
#     )

#     return pd.DataFrame([data])
# # cac cong tỉnh khoa hoc

# def caccongtrinhkhoahoc(raw_text):

#     # tách từng công trình
#     projects = re.split(r'\[\d+\]', raw_text)
#     projects = [p.strip() for p in projects if p.strip()]

#     all_data = []

#     for project in projects:

#         item = {
#             "Đề tài cấp": None,
#             "Tên đề tài": None,
#             "Chủ nhiệm": None,
#             "Thành viên": None,
#             "Mã số": None,
#             "Năm": None
#         }

#         # ----------------------
#         # ĐỀ TÀI CẤP + TÊN ĐỀ TÀI
#         # ----------------------
#         match_title = re.search(
#             r'([^:]+):\s*(.*?)(?=\.\s*(Chủ nhiệm|Chủnhiệm|Thành viên|Thànhviên|Mã số|Mãsố)|$)',
#             project
#         )

#         if match_title:
#             item["Đề tài cấp"] = match_title.group(1).strip()
#             item["Tên đề tài"] = match_title.group(2).strip()

#         # ----------------------
#         # CHỦ NHIỆM
#         # ----------------------
#         match_cn = re.search(
#             r'Chủ\s*nhiệm\s*:\s*(.*?)(?=\.\s*(Thành viên|Thànhviên|Tham gia|Mã số|Mãsố|Năm)|$)',
#             project
#         )

#         if match_cn:
#             item["Chủ nhiệm"] = match_cn.group(1).strip()

#         # ----------------------
#         # THÀNH VIÊN
#         # ----------------------
#         match_tv = re.search(
#             r'(Thành viên|Thànhviên|Tham gia)\s*:\s*(.*?)(?=\.\s*(Mã số|Mãsố|Năm)|$)',
#             project
#         )

#         if match_tv:
#             item["Thành viên"] = match_tv.group(2).strip()

#         # ----------------------
#         # MÃ SỐ
#         # ----------------------
#         match_ms = re.search(
#             r'(Mã số|Mãsố)\s*:\s*(.*?)(?=\.\s*Năm|$)',
#             project
#         )

#         if match_ms:
#             item["Mã số"] = match_ms.group(2).strip()

#         # ----------------------
#         # NĂM
#         # ----------------------
#         match_year = re.search(
#             r'Năm\s*:\s*(\d{4})',
#             project
#         )

#         if match_year:
#             item["Năm"] = match_year.group(1)

#         all_data.append(item)

#     return pd.DataFrame(all_data)

# # bai bao
# import re
# import pandas as pd

# def baibao(text):

#     text = re.sub(r"\s+", " ", text.replace("\n", " ")).strip()

#     rows = []

#     pattern = r"\[/Images/icon_star_purple\.gif\]\s*(TRONG\s*NƯỚC|QUỐC\s*TẾ)\s*:\s*(.*?)(?=\[/Images/icon_star_purple\.gif\]\s*(TRONG\s*NƯỚC|QUỐC\s*TẾ)\s*:|$)"

#     matches = re.finditer(pattern, text, re.I | re.DOTALL)

#     for match in matches:

#         loai = match.group(1).upper()
#         block = match.group(2)

#         items = re.split(r"\[\d+\]", block)

#         for item in items:

#             item = item.strip()
#             if not item:
#                 continue

#             m_title = re.search(
#                 r"(?:Bài\s*báo|Article|Presentations|Tham luận)\s*:\s*(.*?)(?=\s*(?:Tác\s*giả|Authors?)\s*:)",
#                 item, re.I
#             )

#             title = m_title.group(1).strip(" .;-") if m_title else ""

#             m_year = re.search(r"(?:Năm|Year)\s*[: ]\s*([12]\d{3})", item, re.I)
#             year = m_year.group(1) if m_year else ""

#             rows.append({
#                 "Loại bài báo": loai,
#                 "Tên bài báo": title,
#                 "Năm": year
#             })

#     return pd.DataFrame(rows)
# # giang day sdh
# # import re
# # import pandas as pd

# def giangdaysaudaihoc(raw_text):
#     # 1. Chuẩn hóa khoảng trắng và loại bỏ tiêu đề đầu bảng
#     raw_text = re.sub(r"SttHọ và Tên,.*Bảo vệ năm", "", raw_text)
#     raw_text = raw_text.replace("\n", " ")
#     raw_text = re.sub(r"\s+", " ", raw_text).strip()

#     # 2. Tách từng mục dựa trên dấu hiệu [1], [2]...
#     items = re.split(r"\[\d+\]", raw_text)
#     items = [it.strip() for it in items if it.strip()]

#     rows = []
#     for it in items:
#         # Khởi tạo giá trị mặc định
#         row = {
#             # "Họ và Tên s": "",
#             "Tên đề tài": "",
#             "Trình độ": "",
#             "Cơ sở đào tạo": "",
#             "Năm hướng dẫn": "",
#             "Bảo vệ năm": ""
#         }

#         # --- TRÍCH XUẤT DỮ LIỆU ---
        
#         # A. Tên và Đề tài (Lấy đoạn từ đầu đến "Trình độ" hoặc "Trách nhiệm")
#         # Tên học viên nằm trước chữ "Đề tài:"
#         name_match = re.search(r"^(.*?)(?=\s*Đề tài:)", it)
#         if name_match:
#             row["Họ và Tên sinh viên"] = name_match.group(1).strip()

#         # Tên đề tài nằm giữa "Đề tài:" và thông tin "Trình độ" (Thạc sĩ/Tiến sĩ)
#         # Lưu ý: Có trường hợp chèn chữ "Trách nhiệm: Hướng dẫn 1" ở giữa
#         topic_match = re.search(r"Đề tài:\s*(.*?)(?=\s*(?:Trình độ:|Trách nhiệm:|Tiến sĩ|Thạc sĩ))", it)
#         if topic_match:
#             row["Tên đề tài"] = topic_match.group(1).strip(" ,.")

#         # B. Trình độ (Tiến sĩ hoặc Thạc sĩ)
#         level_match = re.search(r"(Tiến sĩ|Thạc sĩ)", it)
#         if level_match:
#             row["Trình độ"] = level_match.group(1)

#         # C. Năm hướng dẫn và Bảo vệ (Thường là 2 cụm 4 chữ số ở cuối)
#         years = re.findall(r"\b(\d{4})\b", it)
#         if len(years) >= 2:
#             row["Năm hướng dẫn"] = years[-2]
#             row["Bảo vệ năm"] = years[-1]
#         elif len(years) == 1:
#             row["Năm hướng dẫn"] = years[0]

#         # D. Cơ sở đào tạo (Đoạn text nằm giữa Trình độ và Năm)
#         # Thường bắt đầu bằng "Trường Đại học..."
#         school_match = re.search(r"(?:Tiến sĩ|Thạc sĩ)\s*(.*?)(?=\s*\d{4})", it)
#         if school_match:
#             row["Cơ sở đào tạo"] = school_match.group(1).strip(" ,.")

#         rows.append(row)

#     return pd.DataFrame(rows)

# # raw_data = df[0]['data'][16]["Giang Day Sau Dai Hoc"]

# # df_giangdaysaudaihoc = sach(raw_data)
# # df_sach["Họ và tên"]=df[0]['data'][1]["ten"]

# # df_giangdaysaudaihoc


# #  sách
# # import re
# # import pandas as pd

# def sach(raw_text: str) -> pd.DataFrame:
#     raw_text = raw_text.replace("\n", " ")
#     raw_text = re.sub(r"\s+", " ", raw_text).strip()

#     items = [it.strip() for it in re.split(r"\[\d+\]", raw_text) if it.strip()]

#     rows = []
#     for it in items:
#         it = re.sub(r"^https?://\S+\s*", "", it).strip()

#         # --- TÊN SÁCH ---
#         # dừng trước Chủ biên / Nơi XB / Năm (cho phép dính khoảng trắng)
#         m_title = re.search(
#             r"^(.*?)(?=\s*(?:Chủ\s*biên:|Authors?:|Nơi\s*XB:|Năm\s*[12]\d{3})|$)",
#             it, flags=re.IGNORECASE
#         )
#         title = m_title.group(1).strip(" ,.-") if m_title else ""

#         # --- NƠI XB ---
#         m_nxb = re.search(
#             r"Nơi\s*XB:\s*(.*?)(?=\s*Năm\s*[12]\d{3}|$)",
#             it, flags=re.IGNORECASE
#         )
#         nxb = m_nxb.group(1).strip(" ,.-") if m_nxb else ""

#         # --- NĂM ---
#         m_year = re.search(r"Năm\s*([12]\d{3})", it, flags=re.IGNORECASE)
#         year = m_year.group(1) if m_year else ""

#         rows.append({
#             "Tên sách/Giáo trình": title,
#             "Nơi XB": nxb,
#             "Năm": year
#         })

#     return pd.DataFrame(rows)

# # môn giảng dạy


# def mongiangday(raw_text: str) -> pd.DataFrame:
#     # chuẩn hoá
#     raw_text = raw_text.replace("\n", " ")
#     raw_text = re.sub(r"\s+", " ", raw_text).strip()

#     # bỏ phần tiêu đề đầu chuỗi nếu có
#     raw_text = re.sub(r"^SttTên mônNăm bắt đầuĐối tượngNơi dạy", "", raw_text).strip()

#     # tách từng môn theo [1], [2], ...
#     items = re.split(r"\[(\d+)\]", raw_text)

#     rows = []
#     for i in range(1, len(items), 2):
#         stt = items[i]
#         item = items[i + 1].strip()

#         row = {
#             # "STT": stt,
#             "Tên môn": "",
#             "Ngành": "",
#             "Năm bắt đầu": "",
#             "Đối tượng": "",
#             "Nơi dạy": ""
#         }

#         # Tên môn: từ đầu đến trước "Ngành:"
#         m_ten = re.search(r"^(.*?)(?=Ngành\s*:)", item, flags=re.IGNORECASE)
#         if m_ten:
#             row["Tên môn"] = m_ten.group(1).strip(" ,.-")

#         # Ngành
#         m_nganh = re.search(r"Ngành\s*:\s*(.*?)(?=\s+[12]\d{3}\b)", item, flags=re.IGNORECASE)
#         if m_nganh:
#             row["Ngành"] = m_nganh.group(1).strip(" ,.-")

#         # Năm bắt đầu
#         m_nam = re.search(r"\b([12]\d{3})\b", item)
#         if m_nam:
#             row["Năm bắt đầu"] = m_nam.group(1)

#         # Phần sau năm
#         tail = ""
#         if m_nam:
#             tail = item[m_nam.end():].strip()

#         # Nơi dạy: thường bắt đầu bằng "Trường ..."
#         m_noiday = re.search(r"(Trường.*)$", tail, flags=re.IGNORECASE)
#         if m_noiday:
#             row["Nơi dạy"] = m_noiday.group(1).strip(" ,.-")
#             row["Đối tượng"] = tail[:m_noiday.start()].strip(" ,.-")
#         else:
#             row["Đối tượng"] = tail.strip(" ,.-")

#         rows.append(row)

#     return pd.DataFrame(rows)
# # raw_mon = df[0]['data'][5]["Mon Giang Day"]
# # df_mongiangday = mongiangday(raw_mon)
# # df_mongiangday["Họ và tên"]=df[0]['data'][1]["ten"]

# # df_mongiangday

# # giải thưởng

# def giaithuong(raw_text: str) -> pd.DataFrame:
#     if not isinstance(raw_text, str) or not raw_text.strip():
#         return pd.DataFrame(columns=["Tên giải thưởng","Lĩnh vực","Năm nhận","Nơi cấp"])

#     # 1. Chuẩn hoá
#     text = raw_text.replace("\n", " ")
#     text = re.sub(r"\s+", " ", text).strip()

#     # bỏ header
#     text = re.sub(r"^SttTênLĩnh vựcNăm nhậnSốNơi cấp", "", text, flags=re.I)

#     # # bỏ link
#     # text = re.sub(r'https?://\S+', ' ', text)
#     # text = re.sub(r'\[[^\]]*http[^\]]*\]', ' ', text)

#     # bỏ rác quảng cáo
#     text = re.sub(r'(viagra|coupon|discount|abortion|prescription|drug|pharmacy|walgreens).*?', ' ', text, flags=re.I)

#     text = re.sub(r"\s+", " ", text).strip()

#     # 2. Danh sách lĩnh vực
#     linh_vuc_list = [
#         "Chưa xác định","Kinh tế","Môi trường","Giáo dục","Khoa học",
#         "Khác","Ngôn ngữ","Y - Dược","Tự nhiên",
#         "Khoa học công nghệ","Công nghệ thông tin",
#         "Xã hội nhân văn","Kỹ thuật"
#     ]
#     linh_vuc_pattern = "|".join(sorted(map(re.escape, linh_vuc_list), key=len, reverse=True))

#     # 3. Tách từng mục theo [1], [2]...
#     items = re.split(r"\[\d+\]", text)

#     rows = []

#     for item in items:
#         item = item.strip()
#         if not item:
#             continue

#         row = {
#             "Tên giải thưởng": None,
#             "Lĩnh vực": None,
#             "Năm nhận": None,
#             # "Số": None,
#             "Nơi cấp": None
#         }

#         # ===== MẪU CHUẨN =====
#         # Tên giải + Lĩnh vực + Năm + Số + Nơi cấp
#         m = re.search(
#             rf"^(.*?)\s+({linh_vuc_pattern})\s+([12]\d{{3}})\s+(\d+)\s+(.*)$",
#             item,
#             flags=re.I
#         )

#         if m:
#             row["Tên giải thưởng"] = m.group(1).strip(" ,.;-\"'")
#             row["Lĩnh vực"] = m.group(2)
#             row["Năm nhận"] = m.group(3)
#             # 1row["Số"] = m.group(4)              # ⭐ MÃ SỐ SAU NĂM
#             row["Nơi cấp"] = (m.group(4) + " " + m.group(5)).strip(" ,.;-\"'")
#             # row["Nơi cấp"] = m.group(5).strip(" ,.;-\"'")
#         else:
#             # fallback: không có mã số
#             m2 = re.search(
#                 rf"^(.*?)\s+({linh_vuc_pattern})\s+([12]\d{{3}})\s+(.*)$",
#                 item,
#                 flags=re.I
#             )
#             if m2:
#                 row["Tên giải thưởng"] = m2.group(1).strip(" ,.;-\"'")
#                 row["Lĩnh vực"] = m2.group(2)
#                 row["Năm nhận"] = m2.group(3)
#                 row["Nơi cấp"] = m2.group(4).strip(" ,.;-\"'")
#             else:
#                 row["Tên giải thưởng"] = item.strip(" ,.;-\"'")

#         # làm sạch nơi cấp lần cuối
#         if row["Nơi cấp"]:
#             row["Nơi cấp"] = re.sub(r'\[.*$', '', row["Nơi cấp"]).strip()

#         rows.append(row)
#         # rows["Nơi cấp"]= rows["Số"] + rows["Nơi cấp"]

#     return pd.DataFrame(rows, columns=[
#         "Tên giải thưởng","Lĩnh vực","Năm nhận","Nơi cấp"
#     ])

# # khen thưởng


# def khenthuong(raw_text: str) -> pd.DataFrame:
#     # 1. Tiền xử lý: Thay thế xuống dòng bằng khoảng trắng và khử khoảng trắng thừa
#     raw_text = raw_text.replace("\n", " ")
#     raw_text = re.sub(r"\s+", " ", raw_text).strip()

#     # 2. Tách dựa trên số thứ tự [1], [2], [3]...
#     # Regex này tìm các số nằm trong ngoặc vuông
#     parts = re.split(r"\[\d+\]", raw_text)

#     # 3. Lọc bỏ các phần tử rỗng và làm sạch nội dung từng mục
#     # (Khi split, phần tử đầu tiên thường rỗng nếu văn bản bắt đầu bằng [1])
#     data = [p.strip() for p in parts if p.strip()]

#     # 4. Tạo DataFrame với duy nhất một cột
#     df = pd.DataFrame(data, columns=["Nội dung khen thưởng"])

#     return df


#                         # update 20/3
# # id cột
# import numpy as np
# def idcot(df, ten, id):
# #     # không cần df = pd.DataFrame(df) nữa
#     df[id] = [ten + str(i+1).zfill(6) for i in range(len(df))]
#     return df
# # xóa khoảng trắng, null
# def xoakhoangtrang(df):
#     # Bước 1: chuyển tất cả về string dtype
#     df = df.astype("string")
#     # Bước 2: xóa khoảng trắng đầu/cuối
#     df = df.apply(lambda x: x.str.strip())
    
#     # df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
#     df = df.replace({np.nan: "", None: "","none": "", "null": "","Null":""})
#     return df

# def main_etl():

#     all_df_thongtinchung = []
#     all_df_khoahoc = []
#     all_df_baibao = []
#     all_df_giangdaysaudaihoc = []
#     all_df_sach = []
#     all_df_mongiangday = []
#     all_df_giaithuong = []
#     all_df_khenthuong = []
#     # Vòng lặp xử lý file
#     for file_num in range(1, 62):
#         file_key = f"page{file_num}.json"
#         try:
#             df = read_json_s3(file_key)
#             for i in range(len(df[0]['data'])-1):
                
#                 # thong tin chung
#                 raw_input = df[0]['data'][i]["thông tin chung"]
#                 df_thongtinchung = thongtinchung(raw_input)
#                 df_thongtinchung["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_thongtinchung["STT người"] = i + 1
#                 df_thongtinchung["File"] = f"page{file_num}"
                
#                 all_df_thongtinchung.append(df_thongtinchung)
#                 # khoa hoc
#                 raw_input = df[0]['data'][i]["cac cong trinh khoa hoc"]
#                 df_khoahoc = caccongtrinhkhoahoc(raw_input)
#                 df_khoahoc["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_khoahoc["STT người"] = i + 1
#                 df_khoahoc["File"] = f"page{file_num}"
                
#                 all_df_khoahoc.append(df_khoahoc)

#                 # bai bao
#                 raw_input = df[0]['data'][i]["Bai Bao"]
#                 df_baibao = baibao(raw_input)
#                 df_baibao["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_baibao["STT người"] = i + 1
#                 df_baibao["File"] = f"page{file_num}"
                
#                 all_df_baibao.append(df_baibao)

#                 #giang day
#                 raw_input = df[0]['data'][i]["Giang Day Sau Dai Hoc"]
#                 df_giangdaysaudaihoc = giangdaysaudaihoc(raw_input)
#                 df_giangdaysaudaihoc["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_giangdaysaudaihoc["STT người"] = i + 1
#                 df_giangdaysaudaihoc["File"] = f"page{file_num}"
                
#                 all_df_giangdaysaudaihoc.append(df_giangdaysaudaihoc)

#                 # sach
#                 raw_input = df[0]['data'][i]["Sach Va Giao Trinh"]
#                 df_sach = sach(raw_input)
#                 df_sach["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_sach["STT người"] = i + 1
#                 df_sach["File"] = f"page{file_num}"
                
#                 all_df_sach.append(df_sach)

#                 # mon giang day
#                 raw_input = df[0]['data'][i]["Mon Giang Day"]
#                 raw_input = re.sub(r'https?://\S+', ' ', raw_input)   # xoá link thường
#                 raw_input = re.sub(r'\[[^\]]*http[^\]]*\]', ' ', raw_input)  # xoá link dạng [http...]
#                 raw_input = re.sub(r"\s+", " ", raw_input).strip()   # xoá khoảng trắng thừa
#                 raw_input = re.sub(r'\S+@\S+', ' ', raw_input)
#                 df_mongiangday = mongiangday(raw_input)
#                 df_mongiangday["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_mongiangday["STT người"] = i + 1
#                 df_mongiangday["File"] = f"page{file_num}"
                
#                 all_df_mongiangday.append(df_mongiangday)

#                 # giai thuong
#                 raw_input = df[0]['data'][i]["Giai Thuong Va Phat Minh"]
#                 df_giaithuong = giaithuong(raw_input)
#                 df_giaithuong["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_giaithuong["STT người"] = i + 1
#                 df_giaithuong["File"] = f"page{file_num}"
                
#                 all_df_giaithuong.append(df_giaithuong)

#                 # khen thuong
#                 raw_input = df[0]['data'][i]["Khen Thuong"]
#                 df_khenthuong = khenthuong(raw_input)
#                 df_khenthuong["Họ và tên"] = df[0]['data'][i]["ten"]
#                 df_khenthuong["STT người"] = i + 1
#                 df_khenthuong["File"] = f"page{file_num}"
                
#                 all_df_khenthuong.append(df_khenthuong)
#         except Exception as e:
#             print(f"Bỏ qua file {file_key} do lỗi: {e}")

#     df_final_thongtinchung1 = pd.concat(all_df_thongtinchung, ignore_index=True)
#     # df_final_thongtinchung1["ID_giangvien"] = df_final_thongtinchung1.apply(tao_id_giangvien, axis=1)

        
#     def xu_ly_dataframe(ds_df, df_thongtinchung, hamid):
#         # Ghép các dataframe
#         df_final = pd.concat(ds_df, ignore_index=True)

#         # Merge với bảng thông tin chung
#         df_final = df_final.merge(
#             df_thongtinchung[['Họ và tên', 'Năm sinh', 'STT người', 'File']],
#             on=['Họ và tên', 'STT người', 'File'],
#             how='left'
#         )
#                             # update 20/3
#                             # Tạo ID giảng viên
#                             # df_final["ID_giangvien"] = df_final.apply(hamid, axis=1)

#         return df_final
#     df_final_khoahoc1 = xu_ly_dataframe(all_df_khoahoc, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_baibao1 = xu_ly_dataframe(all_df_baibao, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_giangdaysaudaihoc1 = xu_ly_dataframe(all_df_giangdaysaudaihoc, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_sach1 = xu_ly_dataframe(all_df_sach, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_mongiangday1 = xu_ly_dataframe(all_df_mongiangday, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_giaithuong1 = xu_ly_dataframe(all_df_giaithuong, df_final_thongtinchung1, tao_id_giangvien)
#     df_final_khenthuong1 = xu_ly_dataframe(all_df_khenthuong, df_final_thongtinchung1, tao_id_giangvien)


#                         # tạo id cho âccs cái còn lại

#                         # df_final_baibao1 = idcot(df_final_baibao1, "BB", "id_baibao")
#                         # df_final_giaithuong1 = idcot(df_final_giaithuong1, "GT", "id_giaithuong")
#                         # df_final_giangdaysaudaihoc1 = idcot(df_final_giangdaysaudaihoc1, "GD", "id_giangdaysdh")
#                         # df_final_khenthuong1 = idcot(df_final_khenthuong1, "KT", "id_khenthuong")
#                         # df_final_khoahoc1 = idcot(df_final_khoahoc1, "KH", "id_khoa_hoc")
#                         # df_final_mongiangday1 = idcot(df_final_mongiangday1, "MGD", "id_mongiangday")
#                         # df_final_sach1 = idcot(df_final_sach1, "SA", "id_sach")

#     # xử lý 2
#     df_final_thongtinchung1["Tốt nghiệp ĐH chuyên ngành"] = df_final_thongtinchung1["Tốt nghiệp ĐH chuyên ngành"].str.replace(
#     r'Tại\s*|[:;]', '', regex=True
#     )
#     df_final_thongtinchung1["Tại_ĐH"] = df_final_thongtinchung1["Tại_ĐH"].str.replace(
#         r'Tại\s*|[:;]', '', regex=True
#     )
#     df_final_thongtinchung1["Tại_Học vị"] = df_final_thongtinchung1["Tại_Học vị"].str.replace(
#         r'Chức danh.*', '', regex=True
#     )
#     df_final_thongtinchung1["Dạy CN"] = df_final_thongtinchung1["Dạy CN"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )


#     df_final_thongtinchung1["Chuyên ngành"] = df_final_thongtinchung1["Chuyên ngành"].str.replace(
#         r'; Tại\s*|Tại', '', regex=True
#     )
#     df_final_thongtinchung1["Lĩnh vực NC"] = df_final_thongtinchung1["Lĩnh vực NC"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
#     df_final_thongtinchung1["Ngoại ngữ"] = df_final_thongtinchung1["Ngoại ngữ"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
#     df_final_thongtinchung1["Địa chỉ liên hệ"] = df_final_thongtinchung1["Địa chỉ liên hệ"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
#     # df_final_thongtinchung1.loc[
#     #     df_final_thongtinchung1["Ngoại ngữ"].str.contains("Đơn|Chức danh|Đà|ĐHĐN|Học vị|Chuyên ngành|Trường", case=False, na=False),
#     #     "Ngoại ngữ"
#     # ] = np.nan
#     # df_final_thongtinchung1.loc[
#     #     df_final_thongtinchung1["Điện thoại"].str.contains("Đăng", case=False, na=False),
#     #     "Điện thoại"
#     # ] = np.nan
#     df_final_thongtinchung1["Điện thoại"] = df_final_thongtinchung1["Điện thoại"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
#     df_final_thongtinchung1["Mobile"] = df_final_thongtinchung1["Mobile"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
    
#     # df_final_thongtinchung1.loc[
#     #     df_final_thongtinchung1["Email"].str.contains("Đăng", case=False, na=False),
#     #     "Email"
#     # ] = np.nan
#     # df_final_thongtinchung1.loc[
#     #     df_final_thongtinchung1["Mobile"].str.contains("Đăng", case=False, na=False),
#     #     "Mobile"
#     # ] = np.nan

#     df_final_thongtinchung1["Tại_Học vị"] = df_final_thongtinchung1["Tại_Học vị"].apply(
#     lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)

#     df_final_thongtinchung1['Năm sinh'] = df_final_thongtinchung1['Năm sinh'].astype(str).str.replace(
#         r'.*00/0.*|.*00.*|.*/0$', '', regex=True
#     ).str.strip()


#     df_final_khoahoc1["Tên đề tài"] = df_final_khoahoc1["Tên đề tài"].str.replace(
#     r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.', 
#     '', 
#     regex=True)

#     df_final_khoahoc1["Tên đề tài"] = df_final_khoahoc1["Tên đề tài"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   
#     # df_final_khoahoc1.loc[
#     #     df_final_khoahoc1["Tên đề tài"].str.contains("41 Lê", case=False, na=False),
#     #         "Tên đề tài"
#     # ] = np.nan

#     df_final_khoahoc1["Chủ nhiệm"] = df_final_khoahoc1["Chủ nhiệm"].str.replace(
#         r'[Tt]hành viên:.*|[Tt]hành viên chính.*:|Tham [Gg]ia.*|SV:.*', 
#         '', 
#         regex=True).str.replace(r',$|, $|;$|; $|: $|:$|- $|-$', '', regex=True)
#     df_final_khoahoc1["Thành viên"] = df_final_khoahoc1["Thành viên"].str.replace(
#         r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.', 
#         '', 
#         regex=True)
#     df_final_khoahoc1["Thành viên"] = df_final_khoahoc1["Thành viên"].str.replace(
#         r'^\s*[-=+]\s*', '', regex=True
#     )   

#     df_final_giangdaysaudaihoc1["Tên đề tài"] = df_final_giangdaysaudaihoc1["Tên đề tài"].str.replace(
#     r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
#     '', 
#     regex=True)
#     df_final_giangdaysaudaihoc1["Cơ sở đào tạo"] = df_final_giangdaysaudaihoc1["Cơ sở đào tạo"].str.replace(
#         r'^- |^-',
#         '',
#         regex=True
#     )
#     df_final_baibao1["Tên bài báo"] = df_final_baibao1["Tên bài báo"].str.replace(
#     r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.*|unfait.*', 
#     '', 
#     regex=True)

#     df_final_mongiangday1["Đối tượng"] = df_final_mongiangday1["Đối tượng"].str.replace(
#     r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
#     '', 
#     regex=True)
#     df_final_mongiangday1["Nơi dạy"] = df_final_mongiangday1["Nơi dạy"].str.replace(
#         r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
#         '', 
#     regex=True)
#     df_final_mongiangday1["Đối tượng"] = df_final_mongiangday1["Đối tượng"].apply(
#     lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)
#     df_final_sach1["Tên sách/Giáo trình"]=df_final_sach1["Tên sách/Giáo trình"].str.replace(
#         r'^\s*[-=]\s*', '', regex=True
#     )   

#     df_final_giaithuong1["Nơi cấp"]=df_final_giaithuong1["Nơi cấp"].str.replace(
#     r'marri.*|unfait.*|cvs.*|walgr.*|abortion.*','',regex=True)
#     # df_final_khenthuong1["Nội dung khen thưởng"]=df_final_khenthuong1.apply(
#     # lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)

#     df_final_khenthuong1["Nội dung khen thưởng"] = df_final_khenthuong1["Nội dung khen thưởng"].apply(
#     lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)
#     # xoa khoang trang
#     dfs = [df_final_thongtinchung1, df_final_khoahoc1, df_final_baibao1, df_final_giaithuong1, 
#            df_final_giangdaysaudaihoc1, df_final_khenthuong1, df_final_mongiangday1, df_final_sach1]
    
#     clean_dfs = [xoakhoangtrang(df) for df in dfs]
#     # Lưu kết quả lên S3 Silver
#     csv_names = ["df_final_thongtinchung1.csv", "df_final_khoahoc1.csv", "df_final_baibao1.csv", 
#                  "df_final_giaithuong1.csv", "df_final_giangdaysaudaihoc1.csv", "df_final_khenthuong1.csv", 
#                  "df_final_mongiangday1.csv", "df_final_sach1.csv"]
    
#     for df, name in zip(clean_dfs, csv_names):
#         write_csv_s3(df, name)

#     print("ngon thiiiiiii")
# if __name__ == "__main__":
#     main_etl()



























import pandas as pd
import re
import json
import boto3
import numpy as np
from io import BytesIO

# ==========================================
# 1. CẤU HÌNH KẾT NỐI MINIO S3
# ==========================================
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "JXHpslDC7PokmWYb"
S3_SECRET_KEY = "iVNGopKzabBfzrEFGfj08kuM6KFve2Xv"
BUCKET_BRONZE = "test"
BUCKET_SILVER = "silver"

s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

def read_json_s3(key):
    obj = s3_client.get_object(Bucket=BUCKET_BRONZE, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

def write_csv_s3(df, key):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    s3_client.put_object(Bucket=BUCKET_SILVER, Key=key, Body=csv_buffer.getvalue())
# 2. CÁC HÀM XỬ LÝ (HELPER FUNCTIONS)



def loai_bo_dau_tieng_viet(s):
    # Bảng mã chuyển đổi các ký tự có dấu sang không dấu
    accents = {
        'a': 'áàảãạăắằẳẵặâấầẩẫậ',
        'A': 'ÁÀẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬ',
        'd': 'đ',
        'D': 'Đ',
        'e': 'éèẻẽẹêếềểễệ',
        'E': 'ÉÈẺẼẸÊẾỀỂỄỆ',
        'i': 'íìỉĩị',
        'I': 'ÍÌỈĨỊ',
        'o': 'óòỏõọôốồổỗộơớờởỡợ',
        'O': 'ÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢ',
        'u': 'úùủũụưứừửữự',
        'U': 'ÚÙỦŨỤƯỨỪỬỮỰ',
        'y': 'ýỳỷỹỵ',
        'Y': 'ÝỲỶỸỊ'
    }
    for char_non_accent, chars_accent in accents.items():
        for char in chars_accent:
            s = s.replace(char, char_non_accent)
    return s

def tao_id_giangvien(row):
    # 1. Bỏ học vị (Lưu ý thêm dấu \ trước các dấu chấm)
    # Thêm \s* để xử lý khoảng trắng sau học vị
    name = re.sub(r'^(GS\.TSKH\.|GS\.TS\.|PGS\.TS\.|TS\.|ThS\.|Ths\.|CN\.|KS\.|BS\.)\s*', '', str(row["Họ và tên"]))

    # 2. Chuyển tên về không dấu trước khi lấy chữ cái đầu
    name_no_accent = loai_bo_dau_tieng_viet(name)
    
    # 3. Lấy chữ cái đầu (đã sạch dấu)
    initials = ''.join([w[0].upper() for w in name_no_accent.split()])

    # 4. Ngày sinh (xử lý trường hợp có "/" hoặc để trống)
    dob = str(row["Năm sinh"]).replace("/", "") if pd.notna(row["Năm sinh"]) else "000000"

    # 5. STT (Số thứ tự người)
    stt = str(row["STT người"])

    # 6. Số file (Trích xuất số từ tên file)
    file_match = re.findall(r'\d+', str(row["File"]))
    file_num = file_match[0] if file_match else "0"

    return f"{initials}{dob}{stt}f{file_num}"

#thongtinchung


def thongtinchung(raw_text):
    #thêm cái này1
    # columns = [
    # "Họ và tên", "Giới tính", "Năm sinh", "Nơi sinh", "Quê quán",
    # "Tốt nghiệp ĐH chuyên ngành", "Tại_ĐH",
    # "Đơn vị công tác", "Khoa", "Trường/Cơ quan",
    # "Chức vụ",
    # "Học vị", "năm", "Chuyên ngành", "Tại_Học vị",
    # "Dạy CN", "Lĩnh vực NC", "Ngoại ngữ",
    # "Địa chỉ liên hệ", "Điện thoại", "Mobile", "Email"
    # ]
    #thêm cái này2
    


    #thêm cái này1

    if not isinstance(raw_text, str) or not raw_text.strip():
        columns = [
        "Họ và tên", "Giới tính", "Năm sinh", "Nơi sinh", "Quê quán",
        "Tốt nghiệp ĐH chuyên ngành", "Tại_ĐH",
        "Đơn vị công tác", "Khoa", "Trường/Cơ quan",
        "Chức vụ",
        "Học vị", "năm", "Chuyên ngành", "Tại_Học vị",
        "Dạy CN", "Lĩnh vực NC", "Ngoại ngữ",
        "Địa chỉ liên hệ", "Điện thoại", "Mobile", "Email"
        ]
        return pd.DataFrame([{col: None for col in columns}])
    #thêm cái này2


    #bằng cái này1

    # if not isinstance(raw_text, str) or not raw_text.strip():
    #     return pd.DataFrame()
    #bằng cái này2

    text = raw_text.replace("\n", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()

    data = {}

    def clean_val(val):
        if val is None:
            return None
        val = val.strip(" ;:.").strip()
        return val if val else None

    def get_value(pattern, text):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return clean_val(match.group(1))
        return None

    # Các mốc dừng để tránh bắt sang cột sau
    stop_after_linhvuc = r"(?=\s*(?:Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"
    stop_after_ngoaingu = r"(?=\s*(?:Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"
    stop_after_diachi = r"(?=\s*(?:Điện\s*thoại|Mobile|Email)\s*:|$)"
    stop_after_chucvu = r"(?=\s*(?:Học\s*vị|Dạy\s*CN|Lĩnh\s*vực\s*NC|Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)"

    # 1. Các trường cơ bản
    data["Họ và tên"] = get_value(
        r"Họ\s*và\s*tên\s*:?\s*(.*?)(?=\s*Giới\s*tính\s*:|$)", text
    )

    data["Giới tính"] = get_value(
        r"Giới\s*tính\s*:?\s*(.*?)(?=\s*Năm\s*sinh\s*:|$)", text
    )

    data["Năm sinh"] = get_value(
        r"Năm\s*sinh\s*:?\s*(.*?)(?=\s*(?:Nơi\s*sinh|Nơisinh)\s*:|$)", text
    )

    data["Nơi sinh"] = get_value(
        r"(?:Nơi\s*sinh|Nơisinh)\s*:?\s*(.*?)(?=\s*(?:Quê\s*quán|Quêquán)\s*:|$)", text
    )

    data["Quê quán"] = get_value(
        r"(?:Quê\s*quán|Quêquán)\s*:?\s*(.*?)(?=\s*Tốt\s*nghiệp\s*ĐH\s*chuyên\s*ngành\s*:|$)", text
    )

    # 2. Khối tốt nghiệp ĐH
    block_dh = get_value(
        r"Tốt\s*nghiệp\s*ĐH\s*chuyên\s*ngành\s*:?\s*(.*?)(?=\s*(?:Đơn\s*vị\s*công\s*tác|Đơnvịcôngtác)\s*:|$)",
        text
    )

    data["Tốt nghiệp ĐH chuyên ngành"] = None
    data["Tại_ĐH"] = None
    if block_dh:
        data["Tốt nghiệp ĐH chuyên ngành"] = get_value(
            r"^(.*?)(?=;\s*Tại\s*:|$)", block_dh
        )
        data["Tại_ĐH"] = get_value(
            r";\s*Tại\s*:?\s*(.*)$", block_dh
        )

    # 3. Đơn vị công tác + tách khoa / trường
    donvi = get_value(
        r"(?:Đơn\s*vị\s*công\s*tác|Đơnvịcôngtác)\s*:?\s*(.*?)(?=\s*Chức\s*vụ\s*:|\s*Học\s*vị\s*:|\s*Dạy\s*CN\s*:|\s*Lĩnh\s*vực\s*NC\s*:|\s*Ngoại\s*ngữ\s*:|\s*Ngoạingữ\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
        text
    )
    data["Đơn vị công tác"] = donvi

    data["Khoa"] = None
    data["Trường/Cơ quan"] = None
    if donvi:
        parts = [x.strip() for x in donvi.split(";")]
        if len(parts) >= 1:
            data["Khoa"] = clean_val(parts[0])
        if len(parts) >= 2:
            data["Trường/Cơ quan"] = clean_val(";".join(parts[1:]))

    # 4. Chức vụ
    data["Chức vụ"] = get_value(
        rf"Chức\s*vụ\s*:?\s*(.*?){stop_after_chucvu}", text
    )

    # 5. Khối học vị
    block_hocvi = get_value(
        r"Học\s*vị\s*:?\s*(.*?)(?=\s*Dạy\s*CN\s*:|\s*Lĩnh\s*vực\s*NC\s*:|\s*Ngoại\s*ngữ\s*:|\s*Ngoạingữ\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
        text
    )

    data["Học vị"] = None
    data["năm"] = None
    data["Chuyên ngành"] = None
    data["Tại_Học vị"] = None

    if block_hocvi:
        data["Học vị"] = get_value(
            r"^(.*?)(?=;\s*năm\s*:|;\s*Chuyên\s*ngành\s*:|;\s*Tại\s*:|$)",
            block_hocvi
        )
        data["năm"] = get_value(
            r";\s*năm\s*:?\s*(.*?)(?=;\s*Chuyên\s*ngành\s*:|;\s*Tại\s*:|$)",
            block_hocvi
        )
        data["Chuyên ngành"] = get_value(
            r";\s*Chuyên\s*ngành\s*:?\s*(.*?)(?=;\s*Tại\s*:|$)",
            block_hocvi
        )
        data["Tại_Học vị"] = get_value(
            r";\s*Tại\s*:?\s*(.*?)(?=\s*(?:Dạy\s*CN|Lĩnh\s*vực\s*NC|Ngoại\s*ngữ|Ngoạingữ|Địa\s*chỉ\s*liên\s*hệ|Điện\s*thoại|Mobile|Email)\s*:|$)",
            block_hocvi
        )

    # 6. Dạy chuyên ngành
    data["Dạy CN"] = get_value(
        r"Dạy\s*CN\s*:?\s*(.*?)(?=\s*Lĩnh\s*vực\s*NC\s*:|\s*(?:Ngoại\s*ngữ|Ngoạingữ)\s*:|\s*Địa\s*chỉ\s*liên\s*hệ\s*:|\s*Điện\s*thoại\s*:|\s*Mobile\s*:|\s*Email\s*:|$)",
        text
    )

    # 7. Lĩnh vực nghiên cứu
    # Không có thì None, có thì dừng trước các cột sau
    linhvuc_match = re.search(
        rf"Lĩnh\s*vực\s*NC\s*:?\s*(.*?){stop_after_linhvuc}",
        text,
        flags=re.IGNORECASE
    )
    data["Lĩnh vực NC"] = clean_val(linhvuc_match.group(1)) if linhvuc_match else None

    # 8. Ngoại ngữ
    data["Ngoại ngữ"] = get_value(
        rf"(?:Ngoại\s*ngữ|Ngoạingữ)\s*:?\s*(.*?){stop_after_ngoaingu}",
        text
    )

    # 9. Địa chỉ liên hệ
    data["Địa chỉ liên hệ"] = get_value(
        rf"Địa\s*chỉ\s*liên\s*hệ\s*:?\s*(.*?){stop_after_diachi}",
        text
    )

    # 10. Điện thoại / Mobile / Email
    data["Điện thoại"] = get_value(
        r"Điện\s*thoại\s*:?\s*(.*?)(?=\s*Mobile\s*:|\s*Email\s*:|$)",
        text
    )

    data["Mobile"] = get_value(
        r"Mobile\s*:?\s*(.*?)(?=\s*Email\s*:|$)",
        text
    )

    data["Email"] = get_value(
        r"Email\s*:?\s*(.*)$",
        text
    )
    # thế1
    return pd.DataFrame([data])
    # thế cái này2
    # return pd.DataFrame([data], columns=columns)
# cac cong tỉnh khoa hoc

def caccongtrinhkhoahoc(raw_text):

    # tách từng công trình
    projects = re.split(r'\[\d+\]', raw_text)
    projects = [p.strip() for p in projects if p.strip()]

    all_data = []

    for project in projects:

        item = {
            "Đề tài cấp": None,
            "Tên đề tài": None,
            "Chủ nhiệm": None,
            "Thành viên": None,
            "Mã số": None,
            "Năm": None
        }

        # ----------------------
        # ĐỀ TÀI CẤP + TÊN ĐỀ TÀI
        # ----------------------
        match_title = re.search(
            r'([^:]+):\s*(.*?)(?=\.\s*(Chủ nhiệm|Chủnhiệm|Thành viên|Thànhviên|Mã số|Mãsố)|$)',
            project
        )

        if match_title:
            item["Đề tài cấp"] = match_title.group(1).strip()
            item["Tên đề tài"] = match_title.group(2).strip()

        # ----------------------
        # CHỦ NHIỆM
        # ----------------------
        match_cn = re.search(
            r'Chủ\s*nhiệm\s*:\s*(.*?)(?=\.\s*(Thành viên|Thànhviên|Tham gia|Mã số|Mãsố|Năm)|$)',
            project
        )

        if match_cn:
            item["Chủ nhiệm"] = match_cn.group(1).strip()

        # ----------------------
        # THÀNH VIÊN
        # ----------------------
        match_tv = re.search(
            r'(Thành viên|Thànhviên|Tham gia)\s*:\s*(.*?)(?=\.\s*(Mã số|Mãsố|Năm)|$)',
            project
        )

        if match_tv:
            item["Thành viên"] = match_tv.group(2).strip()

        # ----------------------
        # MÃ SỐ
        # ----------------------
        match_ms = re.search(
            r'(Mã số|Mãsố)\s*:\s*(.*?)(?=\.\s*Năm|$)',
            project
        )

        if match_ms:
            item["Mã số"] = match_ms.group(2).strip()

        # ----------------------
        # NĂM
        # ----------------------
        match_year = re.search(
            r'Năm\s*:\s*(\d{4})',
            project
        )

        if match_year:
            item["Năm"] = match_year.group(1)

        all_data.append(item)

    return pd.DataFrame(all_data)

# bai bao
import re
import pandas as pd

def baibao(text):

    text = re.sub(r"\s+", " ", text.replace("\n", " ")).strip()

    rows = []

    pattern = r"\[/Images/icon_star_purple\.gif\]\s*(TRONG\s*NƯỚC|QUỐC\s*TẾ)\s*:\s*(.*?)(?=\[/Images/icon_star_purple\.gif\]\s*(TRONG\s*NƯỚC|QUỐC\s*TẾ)\s*:|$)"

    matches = re.finditer(pattern, text, re.I | re.DOTALL)

    for match in matches:

        loai = match.group(1).upper()
        block = match.group(2)

        items = re.split(r"\[\d+\]", block)

        for item in items:

            item = item.strip()
            if not item:
                continue

            m_title = re.search(
                r"(?:Bài\s*báo|Article|Presentations|Tham luận)\s*:\s*(.*?)(?=\s*(?:Tác\s*giả|Authors?)\s*:)",
                item, re.I
            )

            title = m_title.group(1).strip(" .;-") if m_title else ""

            m_year = re.search(r"(?:Năm|Year)\s*[: ]\s*([12]\d{3})", item, re.I)
            year = m_year.group(1) if m_year else ""

            rows.append({
                "Loại bài báo": loai,
                "Tên bài báo": title,
                "Năm": year
            })

    return pd.DataFrame(rows)
# giang day sdh
# import re
# import pandas as pd

def giangdaysaudaihoc(raw_text):
    # 1. Chuẩn hóa khoảng trắng và loại bỏ tiêu đề đầu bảng
    raw_text = re.sub(r"SttHọ và Tên,.*Bảo vệ năm", "", raw_text)
    raw_text = raw_text.replace("\n", " ")
    raw_text = re.sub(r"\s+", " ", raw_text).strip()

    # 2. Tách từng mục dựa trên dấu hiệu [1], [2]...
    items = re.split(r"\[\d+\]", raw_text)
    items = [it.strip() for it in items if it.strip()]

    rows = []
    for it in items:
        # Khởi tạo giá trị mặc định
        row = {
            # "Họ và Tên s": "",
            "Tên đề tài": "",
            "Trình độ": "",
            "Cơ sở đào tạo": "",
            "Năm hướng dẫn": "",
            "Bảo vệ năm": ""
        }

        # --- TRÍCH XUẤT DỮ LIỆU ---
        
        # A. Tên và Đề tài (Lấy đoạn từ đầu đến "Trình độ" hoặc "Trách nhiệm")
        # Tên học viên nằm trước chữ "Đề tài:"
        name_match = re.search(r"^(.*?)(?=\s*Đề tài:)", it)
        if name_match:
            row["Họ và Tên sinh viên"] = name_match.group(1).strip()

        # Tên đề tài nằm giữa "Đề tài:" và thông tin "Trình độ" (Thạc sĩ/Tiến sĩ)
        # Lưu ý: Có trường hợp chèn chữ "Trách nhiệm: Hướng dẫn 1" ở giữa
        topic_match = re.search(r"Đề tài:\s*(.*?)(?=\s*(?:Trình độ:|Trách nhiệm:|Tiến sĩ|Thạc sĩ))", it)
        if topic_match:
            row["Tên đề tài"] = topic_match.group(1).strip(" ,.")

        # B. Trình độ (Tiến sĩ hoặc Thạc sĩ)
        level_match = re.search(r"(Tiến sĩ|Thạc sĩ)", it)
        if level_match:
            row["Trình độ"] = level_match.group(1)

        # C. Năm hướng dẫn và Bảo vệ (Thường là 2 cụm 4 chữ số ở cuối)
        years = re.findall(r"\b(\d{4})\b", it)
        if len(years) >= 2:
            row["Năm hướng dẫn"] = years[-2]
            row["Bảo vệ năm"] = years[-1]
        elif len(years) == 1:
            row["Năm hướng dẫn"] = years[0]

        # D. Cơ sở đào tạo (Đoạn text nằm giữa Trình độ và Năm)
        # Thường bắt đầu bằng "Trường Đại học..."
        school_match = re.search(r"(?:Tiến sĩ|Thạc sĩ)\s*(.*?)(?=\s*\d{4})", it)
        if school_match:
            row["Cơ sở đào tạo"] = school_match.group(1).strip(" ,.")

        rows.append(row)

    return pd.DataFrame(rows)

# raw_data = df[0]['data'][16]["Giang Day Sau Dai Hoc"]

# df_giangdaysaudaihoc = sach(raw_data)
# df_sach["Họ và tên"]=df[0]['data'][1]["ten"]

# df_giangdaysaudaihoc


#  sách
# import re
# import pandas as pd

def sach(raw_text: str) -> pd.DataFrame:
    raw_text = raw_text.replace("\n", " ")
    raw_text = re.sub(r"\s+", " ", raw_text).strip()

    items = [it.strip() for it in re.split(r"\[\d+\]", raw_text) if it.strip()]

    rows = []
    for it in items:
        it = re.sub(r"^https?://\S+\s*", "", it).strip()

        # --- TÊN SÁCH ---
        # dừng trước Chủ biên / Nơi XB / Năm (cho phép dính khoảng trắng)
        m_title = re.search(
            r"^(.*?)(?=\s*(?:Chủ\s*biên:|Authors?:|Nơi\s*XB:|Năm\s*[12]\d{3})|$)",
            it, flags=re.IGNORECASE
        )
        title = m_title.group(1).strip(" ,.-") if m_title else ""

        # --- NƠI XB ---
        m_nxb = re.search(
            r"Nơi\s*XB:\s*(.*?)(?=\s*Năm\s*[12]\d{3}|$)",
            it, flags=re.IGNORECASE
        )
        nxb = m_nxb.group(1).strip(" ,.-") if m_nxb else ""

        # --- NĂM ---
        m_year = re.search(r"Năm\s*([12]\d{3})", it, flags=re.IGNORECASE)
        year = m_year.group(1) if m_year else ""

        rows.append({
            "Tên sách/Giáo trình": title,
            "Nơi XB": nxb,
            "Năm": year
        })

    return pd.DataFrame(rows)

# môn giảng dạy


def mongiangday(raw_text: str) -> pd.DataFrame:
    # chuẩn hoá
    raw_text = raw_text.replace("\n", " ")
    raw_text = re.sub(r"\s+", " ", raw_text).strip()

    # bỏ phần tiêu đề đầu chuỗi nếu có
    raw_text = re.sub(r"^SttTên mônNăm bắt đầuĐối tượngNơi dạy", "", raw_text).strip()

    # tách từng môn theo [1], [2], ...
    items = re.split(r"\[(\d+)\]", raw_text)

    rows = []
    for i in range(1, len(items), 2):
        stt = items[i]
        item = items[i + 1].strip()

        row = {
            # "STT": stt,
            "Tên môn": "",
            "Ngành": "",
            "Năm bắt đầu": "",
            "Đối tượng": "",
            "Nơi dạy": ""
        }

        # Tên môn: từ đầu đến trước "Ngành:"
        m_ten = re.search(r"^(.*?)(?=Ngành\s*:)", item, flags=re.IGNORECASE)
        if m_ten:
            row["Tên môn"] = m_ten.group(1).strip(" ,.-")

        # Ngành
        m_nganh = re.search(r"Ngành\s*:\s*(.*?)(?=\s+[12]\d{3}\b)", item, flags=re.IGNORECASE)
        if m_nganh:
            row["Ngành"] = m_nganh.group(1).strip(" ,.-")

        # Năm bắt đầu
        m_nam = re.search(r"\b([12]\d{3})\b", item)
        if m_nam:
            row["Năm bắt đầu"] = m_nam.group(1)

        # Phần sau năm
        tail = ""
        if m_nam:
            tail = item[m_nam.end():].strip()

        # Nơi dạy: thường bắt đầu bằng "Trường ..."
        m_noiday = re.search(r"(Trường.*)$", tail, flags=re.IGNORECASE)
        if m_noiday:
            row["Nơi dạy"] = m_noiday.group(1).strip(" ,.-")
            row["Đối tượng"] = tail[:m_noiday.start()].strip(" ,.-")
        else:
            row["Đối tượng"] = tail.strip(" ,.-")

        rows.append(row)

    return pd.DataFrame(rows)
# raw_mon = df[0]['data'][5]["Mon Giang Day"]
# df_mongiangday = mongiangday(raw_mon)
# df_mongiangday["Họ và tên"]=df[0]['data'][1]["ten"]

# df_mongiangday

# giải thưởng

def giaithuong(raw_text: str) -> pd.DataFrame:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return pd.DataFrame(columns=["Tên giải thưởng","Lĩnh vực","Năm nhận","Nơi cấp"])

    # 1. Chuẩn hoá
    text = raw_text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()

    # bỏ header
    text = re.sub(r"^SttTênLĩnh vựcNăm nhậnSốNơi cấp", "", text, flags=re.I)

    # # bỏ link
    # text = re.sub(r'https?://\S+', ' ', text)
    # text = re.sub(r'\[[^\]]*http[^\]]*\]', ' ', text)

    # bỏ rác quảng cáo
    text = re.sub(r'(viagra|coupon|discount|abortion|prescription|drug|pharmacy|walgreens).*?', ' ', text, flags=re.I)

    text = re.sub(r"\s+", " ", text).strip()

    # 2. Danh sách lĩnh vực
    linh_vuc_list = [
        "Chưa xác định","Kinh tế","Môi trường","Giáo dục","Khoa học",
        "Khác","Ngôn ngữ","Y - Dược","Tự nhiên",
        "Khoa học công nghệ","Công nghệ thông tin",
        "Xã hội nhân văn","Kỹ thuật"
    ]
    linh_vuc_pattern = "|".join(sorted(map(re.escape, linh_vuc_list), key=len, reverse=True))

    # 3. Tách từng mục theo [1], [2]...
    items = re.split(r"\[\d+\]", text)

    rows = []

    for item in items:
        item = item.strip()
        if not item:
            continue

        row = {
            "Tên giải thưởng": None,
            "Lĩnh vực": None,
            "Năm nhận": None,
            # "Số": None,
            "Nơi cấp": None
        }

        # ===== MẪU CHUẨN =====
        # Tên giải + Lĩnh vực + Năm + Số + Nơi cấp
        m = re.search(
            rf"^(.*?)\s+({linh_vuc_pattern})\s+([12]\d{{3}})\s+(\d+)\s+(.*)$",
            item,
            flags=re.I
        )

        if m:
            row["Tên giải thưởng"] = m.group(1).strip(" ,.;-\"'")
            row["Lĩnh vực"] = m.group(2)
            row["Năm nhận"] = m.group(3)
            # 1row["Số"] = m.group(4)              # ⭐ MÃ SỐ SAU NĂM
            row["Nơi cấp"] = (m.group(4) + " " + m.group(5)).strip(" ,.;-\"'")
            # row["Nơi cấp"] = m.group(5).strip(" ,.;-\"'")
        else:
            # fallback: không có mã số
            m2 = re.search(
                rf"^(.*?)\s+({linh_vuc_pattern})\s+([12]\d{{3}})\s+(.*)$",
                item,
                flags=re.I
            )
            if m2:
                row["Tên giải thưởng"] = m2.group(1).strip(" ,.;-\"'")
                row["Lĩnh vực"] = m2.group(2)
                row["Năm nhận"] = m2.group(3)
                row["Nơi cấp"] = m2.group(4).strip(" ,.;-\"'")
            else:
                row["Tên giải thưởng"] = item.strip(" ,.;-\"'")

        # làm sạch nơi cấp lần cuối
        if row["Nơi cấp"]:
            row["Nơi cấp"] = re.sub(r'\[.*$', '', row["Nơi cấp"]).strip()

        rows.append(row)
        # rows["Nơi cấp"]= rows["Số"] + rows["Nơi cấp"]

    return pd.DataFrame(rows, columns=[
        "Tên giải thưởng","Lĩnh vực","Năm nhận","Nơi cấp"
    ])

# khen thưởng


def khenthuong(raw_text: str) -> pd.DataFrame:
    # 1. Tiền xử lý: Thay thế xuống dòng bằng khoảng trắng và khử khoảng trắng thừa
    raw_text = raw_text.replace("\n", " ")
    raw_text = re.sub(r"\s+", " ", raw_text).strip()

    # 2. Tách dựa trên số thứ tự [1], [2], [3]...
    # Regex này tìm các số nằm trong ngoặc vuông
    parts = re.split(r"\[\d+\]", raw_text)

    # 3. Lọc bỏ các phần tử rỗng và làm sạch nội dung từng mục
    # (Khi split, phần tử đầu tiên thường rỗng nếu văn bản bắt đầu bằng [1])
    data = [p.strip() for p in parts if p.strip()]

    # 4. Tạo DataFrame với duy nhất một cột
    df = pd.DataFrame(data, columns=["Nội dung khen thưởng"])

    return df


                        # update 20/3
# id cột
import numpy as np
def idcot(df, ten, id):
#     # không cần df = pd.DataFrame(df) nữa
    df[id] = [ten + str(i+1).zfill(6) for i in range(len(df))]
    return df
# xóa khoảng trắng, null
def xoakhoangtrang(df):
    # Bước 1: chuyển tất cả về string dtype
    df = df.astype("string")
    # Bước 2: xóa khoảng trắng đầu/cuối
    df = df.apply(lambda x: x.str.strip())
    
    # df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df = df.replace({np.nan: "", None: "","none": "", "null": "","Null":""})
    return df

def main_etl():

    all_df_thongtinchung = []
    all_df_khoahoc = []
    all_df_baibao = []
    all_df_giangdaysaudaihoc = []
    all_df_sach = []
    all_df_mongiangday = []
    all_df_giaithuong = []
    all_df_khenthuong = []
    # Vòng lặp xử lý file
    for file_num in range(1, 62):
        file_key = f"page{file_num}.json"
        try:
            df = read_json_s3(file_key)
            for i in range(len(df[0]['data'])-1):
                
                # thong tin chung
                raw_input = df[0]['data'][i]["thông tin chung"]
                df_thongtinchung = thongtinchung(raw_input)
                df_thongtinchung["Họ và tên"] = df[0]['data'][i]["ten"]
                df_thongtinchung["STT người"] = i + 1
                df_thongtinchung["File"] = f"page{file_num}"
                
                all_df_thongtinchung.append(df_thongtinchung)
                # khoa hoc
                raw_input = df[0]['data'][i]["cac cong trinh khoa hoc"]
                df_khoahoc = caccongtrinhkhoahoc(raw_input)
                df_khoahoc["Họ và tên"] = df[0]['data'][i]["ten"]
                df_khoahoc["STT người"] = i + 1
                df_khoahoc["File"] = f"page{file_num}"
                
                all_df_khoahoc.append(df_khoahoc)

                # bai bao
                raw_input = df[0]['data'][i]["Bai Bao"]
                df_baibao = baibao(raw_input)
                df_baibao["Họ và tên"] = df[0]['data'][i]["ten"]
                df_baibao["STT người"] = i + 1
                df_baibao["File"] = f"page{file_num}"
                
                all_df_baibao.append(df_baibao)

                #giang day
                raw_input = df[0]['data'][i]["Giang Day Sau Dai Hoc"]
                df_giangdaysaudaihoc = giangdaysaudaihoc(raw_input)
                df_giangdaysaudaihoc["Họ và tên"] = df[0]['data'][i]["ten"]
                df_giangdaysaudaihoc["STT người"] = i + 1
                df_giangdaysaudaihoc["File"] = f"page{file_num}"
                
                all_df_giangdaysaudaihoc.append(df_giangdaysaudaihoc)

                # sach
                raw_input = df[0]['data'][i]["Sach Va Giao Trinh"]
                df_sach = sach(raw_input)
                df_sach["Họ và tên"] = df[0]['data'][i]["ten"]
                df_sach["STT người"] = i + 1
                df_sach["File"] = f"page{file_num}"
                
                all_df_sach.append(df_sach)

                # mon giang day
                raw_input = df[0]['data'][i]["Mon Giang Day"]
                raw_input = re.sub(r'https?://\S+', ' ', raw_input)   # xoá link thường
                raw_input = re.sub(r'\[[^\]]*http[^\]]*\]', ' ', raw_input)  # xoá link dạng [http...]
                raw_input = re.sub(r"\s+", " ", raw_input).strip()   # xoá khoảng trắng thừa
                raw_input = re.sub(r'\S+@\S+', ' ', raw_input)
                df_mongiangday = mongiangday(raw_input)
                df_mongiangday["Họ và tên"] = df[0]['data'][i]["ten"]
                df_mongiangday["STT người"] = i + 1
                df_mongiangday["File"] = f"page{file_num}"
                
                all_df_mongiangday.append(df_mongiangday)

                # giai thuong
                raw_input = df[0]['data'][i]["Giai Thuong Va Phat Minh"]
                df_giaithuong = giaithuong(raw_input)
                df_giaithuong["Họ và tên"] = df[0]['data'][i]["ten"]
                df_giaithuong["STT người"] = i + 1
                df_giaithuong["File"] = f"page{file_num}"
                
                all_df_giaithuong.append(df_giaithuong)

                # khen thuong
                raw_input = df[0]['data'][i]["Khen Thuong"]
                df_khenthuong = khenthuong(raw_input)
                df_khenthuong["Họ và tên"] = df[0]['data'][i]["ten"]
                df_khenthuong["STT người"] = i + 1
                df_khenthuong["File"] = f"page{file_num}"
                
                all_df_khenthuong.append(df_khenthuong)
        except Exception as e:
            print(f"Bỏ qua file {file_key} do lỗi: {e}")

    df_final_thongtinchung1 = pd.concat(all_df_thongtinchung, ignore_index=True)
    # df_final_thongtinchung1["ID_giangvien"] = df_final_thongtinchung1.apply(tao_id_giangvien, axis=1)

        
    def xu_ly_dataframe(ds_df, df_thongtinchung, hamid):
        # Ghép các dataframe
        df_final = pd.concat(ds_df, ignore_index=True)

        # Merge với bảng thông tin chung
        df_final = df_final.merge(
            df_thongtinchung[['Họ và tên', 'Năm sinh', 'STT người', 'File']],
            on=['Họ và tên', 'STT người', 'File'],
            how='left'
        )
                            # update 20/3
                            # Tạo ID giảng viên
                            # df_final["ID_giangvien"] = df_final.apply(hamid, axis=1)

        return df_final
    df_final_khoahoc1 = xu_ly_dataframe(all_df_khoahoc, df_final_thongtinchung1, tao_id_giangvien)
    df_final_baibao1 = xu_ly_dataframe(all_df_baibao, df_final_thongtinchung1, tao_id_giangvien)
    df_final_giangdaysaudaihoc1 = xu_ly_dataframe(all_df_giangdaysaudaihoc, df_final_thongtinchung1, tao_id_giangvien)
    df_final_sach1 = xu_ly_dataframe(all_df_sach, df_final_thongtinchung1, tao_id_giangvien)
    df_final_mongiangday1 = xu_ly_dataframe(all_df_mongiangday, df_final_thongtinchung1, tao_id_giangvien)
    df_final_giaithuong1 = xu_ly_dataframe(all_df_giaithuong, df_final_thongtinchung1, tao_id_giangvien)
    df_final_khenthuong1 = xu_ly_dataframe(all_df_khenthuong, df_final_thongtinchung1, tao_id_giangvien)


                        # tạo id cho âccs cái còn lại

                        # df_final_baibao1 = idcot(df_final_baibao1, "BB", "id_baibao")
                        # df_final_giaithuong1 = idcot(df_final_giaithuong1, "GT", "id_giaithuong")
                        # df_final_giangdaysaudaihoc1 = idcot(df_final_giangdaysaudaihoc1, "GD", "id_giangdaysdh")
                        # df_final_khenthuong1 = idcot(df_final_khenthuong1, "KT", "id_khenthuong")
                        # df_final_khoahoc1 = idcot(df_final_khoahoc1, "KH", "id_khoa_hoc")
                        # df_final_mongiangday1 = idcot(df_final_mongiangday1, "MGD", "id_mongiangday")
                        # df_final_sach1 = idcot(df_final_sach1, "SA", "id_sach")

    # xử lý 2
    df_final_thongtinchung1["Tốt nghiệp ĐH chuyên ngành"] = df_final_thongtinchung1["Tốt nghiệp ĐH chuyên ngành"].str.replace(
    r'Tại\s*|[:;]', '', regex=True
    )
    df_final_thongtinchung1["Tại_ĐH"] = df_final_thongtinchung1["Tại_ĐH"].str.replace(
        r'Tại\s*|[:;]', '', regex=True
    )
    df_final_thongtinchung1["Tại_Học vị"] = df_final_thongtinchung1["Tại_Học vị"].str.replace(
        r'Chức danh.*', '', regex=True
    )
    df_final_thongtinchung1["Dạy CN"] = df_final_thongtinchung1["Dạy CN"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )


    df_final_thongtinchung1["Chuyên ngành"] = df_final_thongtinchung1["Chuyên ngành"].str.replace(
        r'; Tại\s*|Tại', '', regex=True
    )
    df_final_thongtinchung1["Lĩnh vực NC"] = df_final_thongtinchung1["Lĩnh vực NC"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    df_final_thongtinchung1["Ngoại ngữ"] = df_final_thongtinchung1["Ngoại ngữ"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    df_final_thongtinchung1["Địa chỉ liên hệ"] = df_final_thongtinchung1["Địa chỉ liên hệ"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    # df_final_thongtinchung1.loc[
    #     df_final_thongtinchung1["Ngoại ngữ"].str.contains("Đơn|Chức danh|Đà|ĐHĐN|Học vị|Chuyên ngành|Trường", case=False, na=False),
    #     "Ngoại ngữ"
    # ] = np.nan
    # df_final_thongtinchung1.loc[
    #     df_final_thongtinchung1["Điện thoại"].str.contains("Đăng", case=False, na=False),
    #     "Điện thoại"
    # ] = np.nan
    df_final_thongtinchung1["Điện thoại"] = df_final_thongtinchung1["Điện thoại"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    df_final_thongtinchung1["Mobile"] = df_final_thongtinchung1["Mobile"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    
    # df_final_thongtinchung1.loc[
    #     df_final_thongtinchung1["Email"].str.contains("Đăng", case=False, na=False),
    #     "Email"
    # ] = np.nan
    # df_final_thongtinchung1.loc[
    #     df_final_thongtinchung1["Mobile"].str.contains("Đăng", case=False, na=False),
    #     "Mobile"
    # ] = np.nan

    df_final_thongtinchung1["Tại_Học vị"] = df_final_thongtinchung1["Tại_Học vị"].apply(
    lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)

    df_final_thongtinchung1['Năm sinh'] = df_final_thongtinchung1['Năm sinh'].astype(str).str.replace(
        r'.*00/0.*|.*00.*|.*/0$', '', regex=True
    ).str.strip()


    df_final_khoahoc1["Tên đề tài"] = df_final_khoahoc1["Tên đề tài"].str.replace(
    r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.', 
    '', 
    regex=True)

    df_final_khoahoc1["Tên đề tài"] = df_final_khoahoc1["Tên đề tài"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   
    # df_final_khoahoc1.loc[
    #     df_final_khoahoc1["Tên đề tài"].str.contains("41 Lê", case=False, na=False),
    #         "Tên đề tài"
    # ] = np.nan

    df_final_khoahoc1["Chủ nhiệm"] = df_final_khoahoc1["Chủ nhiệm"].str.replace(
        r'[Tt]hành viên:.*|[Tt]hành viên chính.*:|Tham [Gg]ia.*|SV:.*', 
        '', 
        regex=True).str.replace(r',$|, $|;$|; $|: $|:$|- $|-$', '', regex=True)
    df_final_khoahoc1["Thành viên"] = df_final_khoahoc1["Thành viên"].str.replace(
        r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.', 
        '', 
        regex=True)
    df_final_khoahoc1["Thành viên"] = df_final_khoahoc1["Thành viên"].str.replace(
        r'^\s*[-=+]\s*', '', regex=True
    )   

    df_final_giangdaysaudaihoc1["Tên đề tài"] = df_final_giangdaysaudaihoc1["Tên đề tài"].str.replace(
    r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
    '', 
    regex=True)
    df_final_giangdaysaudaihoc1["Cơ sở đào tạo"] = df_final_giangdaysaudaihoc1["Cơ sở đào tạo"].str.replace(
        r'^- |^-',
        '',
        regex=True
    )
    df_final_baibao1["Tên bài báo"] = df_final_baibao1["Tên bài báo"].str.replace(
    r'marria.*|Chủ nhiệm:.*|cvs.*|walgreen.*|abortion.*|unfait.*', 
    '', 
    regex=True)

    df_final_mongiangday1["Đối tượng"] = df_final_mongiangday1["Đối tượng"].str.replace(
    r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
    '', 
    regex=True)
    df_final_mongiangday1["Nơi dạy"] = df_final_mongiangday1["Nơi dạy"].str.replace(
        r'marri.*|unfait.*|cvs.*|walgreen.*|abortion.*', 
        '', 
    regex=True)
    df_final_mongiangday1["Đối tượng"] = df_final_mongiangday1["Đối tượng"].apply(
    lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)
    df_final_sach1["Tên sách/Giáo trình"]=df_final_sach1["Tên sách/Giáo trình"].str.replace(
        r'^\s*[-=]\s*', '', regex=True
    )   

    df_final_giaithuong1["Nơi cấp"]=df_final_giaithuong1["Nơi cấp"].str.replace(
    r'marri.*|unfait.*|cvs.*|walgr.*|abortion.*','',regex=True)
    # df_final_khenthuong1["Nội dung khen thưởng"]=df_final_khenthuong1.apply(
    # lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)

    df_final_khenthuong1["Nội dung khen thưởng"] = df_final_khenthuong1["Nội dung khen thưởng"].apply(
    lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x) if isinstance(x, str) else x)
    # xoa khoang trang
    dfs = [df_final_thongtinchung1, df_final_khoahoc1, df_final_baibao1, df_final_giaithuong1, 
           df_final_giangdaysaudaihoc1, df_final_khenthuong1, df_final_mongiangday1, df_final_sach1]
    
    clean_dfs = [xoakhoangtrang(df) for df in dfs]
    # Lưu kết quả lên S3 Silver
    csv_names = ["df_final_thongtinchung1.csv", "df_final_khoahoc1.csv", "df_final_baibao1.csv", 
                 "df_final_giaithuong1.csv", "df_final_giangdaysaudaihoc1.csv", "df_final_khenthuong1.csv", 
                 "df_final_mongiangday1.csv", "df_final_sach1.csv"]
    
    for df, name in zip(clean_dfs, csv_names):
        write_csv_s3(df, name)

    print("ngon thiiiiiii")
if __name__ == "__main__":
    main_etl()