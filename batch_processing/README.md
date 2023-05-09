To start execution, use the command

```bash
    docker exec -it spark-master /bin/bash
    ./spark-submit --packages org.postgresql:postgresql:42.4.0 ../batch_processor/run.py
```

# 1. Number of accidents per day

```sql
    SELECT date, COUNT(*) as number_of_accidents
    FROM accidents
    GROUP BY date;
```

# 2. Number of accidents during different periods (23-6h, 06-23h)

```sql
    SELECT COUNT(*),
    CASE
      WHEN part_of_day = 'PART_1' THEN '06:00 - 23:00'
      WHEN part_of_day = 'PART_2' THEN '23:00 - 06:00'
    END as part_of_day_timeframe
    FROM accidents GROUP BY part_of_day;
```

# 3. Number of accidents by county

```sql
    SELECT "County", count(*)
    FROM accidents
    GROUP BY "County";
```

# 4. Percentage of accidents that occurred when the temperature was above/below the average temperature

```sql
    WITH avg_temperature AS (
      SELECT AVG(temperature) AS value
      FROM accidents
    ),
    accidents_above_avg_temp AS (
      SELECT COUNT(*) AS value
      FROM accidents
      WHERE temperature > (SELECT value FROM avg_temperature)
    ),
    total_accidents AS (
      SELECT COUNT(*) AS value
      FROM accidents
    )
    SELECT
      (accidents_above_avg_temp.value::float / total_accidents.value::float) * 100 AS percentage_of_accidents_above_avg_temp,
      ((total_accidents.value::float - accidents_above_avg_temp.value::float) / total_accidents.value::float) * 100 AS percentage_of_accidents_under_avg_temp
    FROM accidents_above_avg_temp, total_accidents;

```

# 5. Accidents by weather conditions

```sql
    SELECT weather_condition, COUNT(*) AS accidents_per_weather_condition
    FROM accidents
    GROUP BY weather_condition;

```

# 6. Percentage of accidents depending on the day of the week

```sql
    WITH total_accidents AS (
      SELECT COUNT(*) AS value
      FROM accidents
    ),
    count_per_day_of_week AS (
        SELECT day_of_week,
              COUNT(*) as count_per_day
        FROM accidents
        GROUP BY day_of_week
    )
    SELECT day_of_week, CAST(count_per_day AS DECIMAL)/total_accidents.value * 100 as percentage_of_accidents_per_day_of_week
    FROM count_per_day_of_week, total_accidents;

```

# 7. Display the 10 cities with the least number of accidents, where cities with the same number of accidents are repeated

```sql
    WITH accidents_per_city AS(
    SELECT "City" as city, COUNT(*) as accidents_count
    FROM accidents
    GROUP BY "City"
    ),
    accidents_per_city_ranked AS(
    SELECT city, accidents_count, DENSE_RANK(*) OVER(ORDER BY accidents_count) rank_no
    FROM accidents_per_city
    )
    SELECT * FROM accidents_per_city_ranked WHERE rank_no < 10;

```

# 8. Difference in the number of accidents compared to the previous day

```sql
WITH accidents_per_date AS(
    SELECT date,
           COUNT(*) as accidents_count
    FROM accidents
    GROUP BY
           date
)
SELECT date,
       (accidents_count - COALESCE(LAG(accidents_count) OVER(ORDER BY date),0)) as accident_diff
FROM accidents_per_date
ORDER BY
       date;
```

# 9. Two years with the most accidents in the state with the year and number of accidents

```sql
WITH accidents_per_year_and_state AS (
    SELECT
        year,
        "State" as state,
        COUNT(*) as accidents_no
    FROM accidents
    GROUP BY
        year,
        "State"
),
accidents_per_year_and_state_ranked AS (
    SELECT
        state,
        year,
        accidents_no,
        ROW_NUMBER() OVER(PARTITION BY state ORDER BY accidents_no DESC) as row_index
    FROM accidents_per_year_and_state
)
SELECT *
FROM accidents_per_year_and_state_ranked
WHERE
    row_index < 3;
```
