# Gold Layer Analytical Queries

## 1. Busiest Airports
```sql
SELECT departure_airport_code, SUM(total_flights) AS flights
FROM gold_departure_airport_hourly
GROUP BY departure_airport_code
ORDER BY flights DESC;
```
## 2. Highest Average Departure Delay
```sql
SELECT departure_airport_code,
       SUM(avg_departure_delay_minutes * total_flights) / SUM(total_flights) AS avg_delay
FROM gold_departure_airport_hourly
GROUP BY departure_airport_code
ORDER BY avg_delay DESC;
```

## 3. Cancellation Rate
```sql
SELECT departure_airport_code,
       SUM(cancelled_flights) / SUM(total_flights) AS cancellation_rate
FROM gold_departure_airport_hourly
GROUP BY departure_airport_code
ORDER BY cancellation_rate DESC;
```

## 4. On-Time Performance (OTP)
```sql
SELECT departure_airport_code,
       SUM(on_time_departures) / SUM(total_flights) AS otp
FROM gold_departure_airport_hourly
GROUP BY departure_airport_code
ORDER BY otp DESC;
```

## 5. Peak Delay Hours
```sql
SELECT departure_hour,
       SUM(avg_departure_delay_minutes * total_flights) / SUM(total_flights) AS avg_delay
FROM gold_departure_airport_hourly
GROUP BY departure_hour
ORDER BY avg_delay DESC;
```