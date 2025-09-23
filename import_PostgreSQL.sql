COPY meter_readings(nmi, timestamp, consumption)
FROM 'C:/meter_readings.sql.csv' CSV;

--command " "\\copy public.meter_readings(nmi, \"timestamp\", consumption) FROM 'C:/meter_readings.sql.csv' WITH(FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '''');""