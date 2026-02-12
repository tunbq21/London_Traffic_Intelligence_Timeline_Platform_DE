CREATE OR REPLACE VIEW v_road_disruptions_analysis AS
SELECT 
    id,
    category,
    severity,
    location,
    -- Chuyển TEXT sang TIMESTAMP để làm biểu đồ thời gian
    CAST(startDateTime AS TIMESTAMP) AS start_time,
    CAST(endDateTime AS TIMESTAMP) AS end_time,
    -- Tính thời gian diễn ra sự cố (phút)
    EXTRACT(EPOCH FROM (CAST(endDateTime AS TIMESTAMP) - CAST(startDateTime AS TIMESTAMP))) / 60 AS duration_minutes,
    -- Tách tọa độ từ cột point (Dạng JSON: "[lat, long]" hoặc {"lat":...})
    -- Giả sử định dạng là: "[51.5, -0.1]"
    CAST(split_part(replace(replace(point, '[', ''), ']', ''), ',', 1) AS FLOAT) AS latitude,
    CAST(split_part(replace(replace(point, '[', ''), ']', ''), ',', 2) AS FLOAT) AS longitude,
    extracted_at
FROM london_road_disruptions;





CREATE OR REPLACE VIEW v_london_traffic_analysis AS
SELECT 
    id,
    category,
    severity,
    -- Tách tọa độ từ chuỗi JSON "[lat, lon]"
    CAST(split_part(trim(both '[]' from point), ',', 1) AS FLOAT) as latitude,
    CAST(split_part(trim(both '[]' from point), ',', 2) AS FLOAT) as longitude,
    -- Chuyển đổi thời gian để làm báo cáo theo giờ/ngày
    CAST(startDateTime AS TIMESTAMP) as start_time,
    CAST(endDateTime AS TIMESTAMP) as end_time,
    -- Tính thời gian tắc đường (phút)
    EXTRACT(EPOCH FROM (CAST(endDateTime AS TIMESTAMP) - CAST(startDateTime AS TIMESTAMP))) / 60 as duration_minutes,
    location
FROM london_road_disruptions;