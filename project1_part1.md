# Part 1

# Required questions:

- What's the size of this dataset? (i.e., how many trips)


```sql
SELECT count(*)
FROM `bigquery-public-data.san_francisco.bikeshare_trips`;
```
    The answers is: 983648


- What is the earliest start date and time and latest end date and time for a trip?

```sql
SELECT min(start_date), max(end_date)  
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
```

    The answers is: 2013-08-29 09:08:00 UTC and 2016-08-31 23:48:00 UTC


- How many bikes are there?

```sql
SELECT count(distinct bike_number)  
FROM `bigquery-public-data.san_francisco.bikeshare_trips`

```
    The answers is: 700

# Questions for your own:


- How many stations are there?

```sql
SELECT count(distinct station_id) 
FROM `bigquery-public-data.san_francisco.bikeshare_stations`
```

    The answers is: 74

- What is the duraiton of the trip that lasted the longest?

```sql
SELECT duration_sec, start_date, end_date
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
ORDER BY duration_sec desc 
limit 5
```

    The answers is: 17,270,400 sec (almost 200 days, the data might be incorrect)

- How many round trips?

```sql
SELECT  count(trip_id)
FROM `bigquery-public-data.san_francisco.bikeshare_trips`
WHERE  start_station_id = end_station_id
```

    The answers is: 32047