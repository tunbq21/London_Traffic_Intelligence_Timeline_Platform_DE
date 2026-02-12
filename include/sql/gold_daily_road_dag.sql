INSERT INTO gold_daily_road_stats (report_date, category, severity, total_disruptions, avg_duration_hours)
            SELECT 
                CAST(startDateTime AS DATE) as report_date,
                category,
                severity,
                COUNT(id) as total_disruptions,
                AVG(EXTRACT(EPOCH FROM (CAST(endDateTime AS TIMESTAMP) - CAST(startDateTime AS TIMESTAMP)))/3600) as avg_duration_hours
            FROM london_road_disruptions
            WHERE startDateTime IS NOT NULL AND endDateTime IS NOT NULL
            GROUP BY 1, 2, 3
            ON CONFLICT (report_date, category, severity) DO UPDATE SET 
                total_disruptions = EXCLUDED.total_disruptions,
                avg_duration_hours = EXCLUDED.avg_duration_hours,
                last_updated = CURRENT_TIMESTAMP;