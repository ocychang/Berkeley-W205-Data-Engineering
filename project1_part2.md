# Part 2

# Required questions:

- What's the size of this dataset? (i.e., how many trips)


```sql
bq query --use_legacy_sql=false '
SELECT count(*)
FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
```

```
+--------+
|  f0_   |
+--------+
| 983648 |
+--------+
```

- What is the earliest start date and time and latest end date and time for a trip?

```sql
bq query --use_legacy_sql=false '
SELECT min(start_date), max(end_date)  
FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
```

```
+---------------------+---------------------+
|         f0_         |         f1_         |
+---------------------+---------------------+
| 2013-08-29 09:08:00 | 2016-08-31 23:48:00 |
+---------------------+---------------------+
```

- How many bikes are there?

```sql
bq query --use_legacy_sql=false '
SELECT count(distinct bike_number)  
FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
```

```
+-----+
| f0_ |
+-----+
| 700 |
+-----+
```

- How many trips are in the morning vs in the afternoon?

    Found one definition or morning and afternoon as:
    Meteorologists consider morning to be from 6 a.m. to 12 p.m. Afternoon lasts from 12 p.m. to 6 p.m. Evening runs from 6 p.m. to midnight. Overnight lasts from midnight to 6 a.m.


```sql
bq query --use_legacy_sql=false '
SELECT 
COUNT(DISTINCT trip_id),
CASE 
WHEN EXTRACT(HOUR FROM start_date) BETWEEN 6 and 11 THEN "Morning" 
WHEN EXTRACT(HOUR FROM start_date) BETWEEN 12 and 18 THEN "Afternoon" 
ELSE NULL END as MA,
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY MA' 
```

```
+--------+-----------+
|  f0_   |    MA     |
+--------+-----------+
| 475768 | Afternoon |
| 399821 | Morning   |
| 108059 | NULL      |
+--------+-----------+
```

# Project Questions:

Identify the main questions you'll need to answer to make recommendations (list below, add as many questions as you need).

- What is the trip frequency distribution within a week?

```sql
bq query --use_legacy_sql=false '
SELECT CASE WHEN EXTRACT(DAYOFWEEK FROM start_date) = 1 THEN "SUN"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 2 THEN "MON"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 3 THEN "TUE"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 4 THEN "WED"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 5 THEN "THR"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 6 THEN "FRI"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 7 THEN "SAT"
            ELSE Null END as day_of_week,      
COUNT(trip_id) as trip_counts,
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
GROUP BY day_of_week 
ORDER BY trip_counts'
```

```
+-------------+-------------+
| day_of_week | trip_counts |
+-------------+-------------+
| SUN         |       51375 |
| SAT         |       60279 |
| FRI         |      159977 |
| MON         |      169937 |
| THR         |      176908 |
| WED         |      180767 |
| TUE         |      184405 |
+-------------+-------------+
```

- What is the trip frequency distribution within weekdays?

```sql
bq query --use_legacy_sql=false '
SELECT COUNT(trip_id) as trip_counts, hours,  FROM
(
SELECT CASE WHEN EXTRACT(DAYOFWEEK FROM start_date) = 1 THEN "SUN"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 2 THEN "MON"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 3 THEN "TUE"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 4 THEN "WED"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 5 THEN "THR"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 6 THEN "FRI"
            WHEN EXTRACT(DAYOFWEEK FROM start_date) = 7 THEN "SAT"
            ELSE Null END as day_of_week, 
            EXTRACT(HOUR FROM start_date) as hours,
            trip_id,
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
) as inner_table
WHERE day_of_week in ("MON","TUE","WED","THR","FRI")
GROUP BY hours
ORDER BY hours'
```

```
+-------------+-------+
| trip_counts | hours |
+-------------+-------+
|        1696 |     0 |
|         797 |     1 |
|         420 |     2 |
|         427 |     3 |
|        1273 |     4 |
|        4799 |     5 |
|       19830 |     6 |
|       65900 |     7 |
|      128999 |     8 |
|       90264 |     9 |
|       34888 |    10 |
|       30712 |    11 |
|       36382 |    12 |
|       33029 |    13 |
|       27590 |    14 |
|       37424 |    15 |
|       79000 |    16 |
|      118332 |    17 |
|       78188 |    18 |
|       36584 |    19 |
|       19492 |    20 |
|       12828 |    21 |
|        8343 |    22 |
|        4797 |    23 |
+-------------+-------+

```

- Who are the main users of bikeshare for each year?


```sql
bq query --use_legacy_sql=false '
SELECT subscriber_type, COUNT(distinct trip_id) as trip_count,
EXTRACT(YEAR FROM start_date) as years
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
GROUP BY subscriber_type, years
ORDER BY years ASC, subscriber_type ASC'
```

```
+-----------------+------------+-------+
| subscriber_type | trip_count | years |
+-----------------+------------+-------+
| Customer        |      24499 |  2013 |
| Subscriber      |      76064 |  2013 |
| Customer        |      48576 |  2014 |
| Subscriber      |     277763 |  2014 |
| Customer        |      40530 |  2015 |
| Subscriber      |     305722 |  2015 |
| Customer        |      23204 |  2016 |
| Subscriber      |     187290 |  2016 |
+-----------------+------------+-------+
```

- What are the top 5 popular trips?
```sql
bq query --use_legacy_sql=false '
SELECT start_station_name, end_station_name, COUNT(distinct trip_id) as pop_trip 
FROM `bigquery-public-data.san_francisco.bikeshare_trips` 
GROUP BY start_station_name, end_station_name 
ORDER BY pop_trip  DESC LIMIT 5'
```

```
+-----------------------------------------+--------------------------------------+----------+
|           start_station_name            |           end_station_name           | pop_trip |
+-----------------------------------------+--------------------------------------+----------+
| Harry Bridges Plaza (Ferry Building)    | Embarcadero at Sansome               |     9150 |
| San Francisco Caltrain 2 (330 Townsend) | Townsend at 7th                      |     8508 |
| 2nd at Townsend                         | Harry Bridges Plaza (Ferry Building) |     7620 |
| Harry Bridges Plaza (Ferry Building)    | 2nd at Townsend                      |     6888 |
| Embarcadero at Sansome                  | Steuart at Market                    |     6874 |
+-----------------------------------------+--------------------------------------+----------+
```


# The following questions will be answered in part 3 with better visualization.

- What are the rush hours based on the distribution?

- What are the popular trips during rush hours?

- What are the popular trips during weekends?

- What is the trip duration distribution during rush hours?

- What is the trip duration distribution during weekends?

- Which stations have the most frequency "no bikes available" events?

- Which stations have the most frequency "no docks available" events?