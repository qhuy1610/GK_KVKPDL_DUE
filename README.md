# GK_KVKPDL_DUE
## Data Warehouse hỗ trợ tìm giảng viên hướng dẫn NCKH

## Giới thiệu
Dự án xây dựng hệ thống **Data Warehouse** nhằm hỗ trợ sinh viên tìm kiếm giảng viên hướng dẫn phù hợp với đề tài nghiên cứu khoa học.

Hệ thống tích hợp và phân tích dữ liệu học thuật của giảng viên từ Đại học Đà Nẵng, giúp tra cứu nhanh chóng và chính xác.

---

## Mục tiêu
- Tích hợp dữ liệu giảng viên từ nhiều nguồn  
- Làm sạch và chuẩn hóa dữ liệu  
- Hỗ trợ tìm kiếm giảng viên theo:
  - Lĩnh vực nghiên cứu  
  - Công trình khoa học  
  - Kinh nghiệm hướng dẫn  
- Phục vụ phân tích và khai phá dữ liệu  

---

## Dataset
**Nguồn**: Trang thông tin khoa học cá nhân – Đại học Đà Nẵng  

Gồm **8 bảng dữ liệu chính**:
- `thongtinchung` – Thông tin giảng viên  
- `baibao` – Bài báo khoa học  
- `khoahoc` – Đề tài nghiên cứu  
- `sach` – Sách & giáo trình  
- `mongiangday` – Môn giảng dạy  
- `giangdaysaudaihoc` – Hướng dẫn sau đại học  
- `giaithuong` – Giải thưởng  
- `khenthuong` – Khen thưởng  

➡️ Liên kết bằng khóa: `id_giangvien`

---

## Kiến trúc hệ thống

### Data Pipeline
1. **Data Crawling**
   - Công cụ: n8n  
   - Crawl dữ liệu tự động hàng tuần  
   - Lưu JSON vào MinIO  

2. **Data Processing**
   - Python (Pandas, Regex)  
   - Làm sạch, chuẩn hóa  
   - Chuyển sang CSV  

3. **ETL Pipeline**
   - Apache Airflow  
   - Tự động hóa workflow  

4. **Data Warehouse**
   - SQL Server  
   - Mô hình: Star Schema  

---

## Mô hình dữ liệu
- Bảng trung tâm: `thongtinchung`  
- Các bảng liên kết: `baibao`, `khoahoc`, `giaithuong`, `khenthuong`, `sach`, `mongiangday`, `giangdaysaudaihoc`  

Quan hệ: **1 - N (giảng viên → dữ liệu học thuật)**  

---

## Data Mining

### K-Means Clustering
- Phân cụm các trường đại học theo thành tích  

---

### Machine Learning
- Model: Random Forest, KNN, Linear Regression 
- Dự đoán khả năng NCKH của giáo viên



## Data Visualization
Dashboard gồm:
- Tổng quan: số giảng viên, giới tính, học vị  
- Thành tích: bài báo, giải thưởng, sách  
- Top giảng viên nổi bật  

---

## Web App (Demo)
- Xây dựng bằng Python (local)  
- Kết nối SQL Server  

Chức năng:
- Tìm kiếm giảng viên theo bộ lọc  
- Xem chi tiết:
  - Bài báo  
  - Môn giảng dạy  
  - Thành tích  

---

## Công nghệ sử dụng
- n8n  
- MinIO  
- Python (Pandas, Regex)  
- Apache Airflow  
- SQL Server  
- Orange Data Mining  

---

