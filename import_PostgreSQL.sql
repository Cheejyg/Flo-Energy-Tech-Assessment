-- ./psql.exe -h localhost -U postgres -d nem12 -f "C:\NEM12#200506081149#UNITEDDP#NEMMCO.sql"

COPY meter_readings(nmi, timestamp, consumption)
FROM 'C:/meter_readings.sql.csv' CSV;

--command " "\\copy public.meter_readings(nmi, \"timestamp\", consumption) FROM 'C:/meter_readings.sql.csv' WITH(FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '''');""