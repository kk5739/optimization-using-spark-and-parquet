# Lab 3: Spark and Parquet Optimization Report

Name: Kyeongmo Kang, Alexander Pegot Ogier, Nikolas Prasinos - Team 94
 
NetID: k5739, ap9283, np3106 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

question_1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40')
question_1_query.show()

```


Output:
```

Question 1
+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

question_2_query = reserves.filter(reserves.bid != 101).groupBy("sid").agg(F.count("bid").alias("count(bid)"))
question_2_query.show()

```


Output:
```

Question 2
+---+----------+
|sid|count(bid)|
+---+----------+
| 64|         1|
| 74|         1|
| 22|         3|
| 31|         3|
+---+----------+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

question_3_query = spark.sql('''SELECT
                                        r.sid,
                                        FIRST(s.sname) AS sname,
                                        COUNT(DISTINCT r.bid) AS num_distinct_boats
                                    FROM
                                        reserves r
                                    JOIN
                                        sailors s
                                    ON
                                        r.sid = s.sid
                                    GROUP BY
                                        r.sid''')
question_3_query.show()

```


Output:
```

Question 3
+---+-------+------------------+
|sid|  sname|num_distinct_boats|
+---+-------+------------------+
| 22|dusting|                 4|
| 31| lubber|                 3|
| 64|horatio|                 2|
| 74|horatio|                 1|
+---+-------+------------------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

joined_4 = tracks.join(artist_term, on="artistID", how="inner")
joined_4 = joined_4.filter((joined_4["year"] > 0) & (joined_4["duration"] > 0))

result_4 = joined_4.groupBy("term").agg(
 F.expr("percentile_approx(year, 0.5)").alias("median_year"),
 F.max("duration").alias("max_duration"),
 F.countDistinct("artistID").alias("num_artists"),
 F.avg("duration").alias("avg_duration")
)

final_result_4 = result_4.orderBy("avg_duration").select(
 "term", "median_year", "max_duration", "num_artists", "avg_duration").limit(10)

final_result_4.show(truncate=False)

```


Output:
```

We want to mention that we filtered out the tracks whose year was 0, as we considered them as invalid data.

Question 4
+--------------------+-----------+------------+-----------+-----------------+
|term                |median_year|max_duration|num_artists|avg_duration     |
+--------------------+-----------+------------+-----------+-----------------+
|true hip hop        |2005       |5.40689     |1          |5.40689          |
|abstract rap        |2000       |25.91302    |1          |25.91302         |
|experimental rap    |2000       |25.91302    |1          |25.91302         |
|crust grind         |2002       |67.21261    |2          |41.58275571428572|
|en catala           |2008       |42.73587    |1          |42.73587         |
|noise grind         |2008       |57.18159    |1          |42.96227         |
|turntablist         |1993       |145.89342   |1          |43.32922428571428|
|sweetzerland artists|1999       |61.80526    |1          |47.046075        |
|night music         |2009       |48.43057    |1          |48.43057         |
|pimp                |2006       |50.41587    |1          |50.41587         |
+--------------------+-----------+------------+-----------+-----------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```python

joined_5 = tracks.join(artist_term, on="artistID", how="inner")
joined_5 = joined_5.filter((joined_4["year"] > 0) & (joined_4["duration"] > 0))

    
distinct_tracks = joined_5.groupBy("term").agg(F.countDistinct("trackID").alias("distinct_tracks"))

# Top 10
top_10 = distinct_tracks.orderBy(F.desc("distinct_tracks")).limit(10)
top_10.show(truncate=False)

# Bot 10
bot_10 = distinct_tracks.orderBy("distinct_tracks").limit(10)
bot_10.show(truncate=False)

```


Output:
```

We want to mention that we filtered out the tracks whose year or duration was 0, as we considered them as invalid data.
 
Question 5
+----------------+---------------+
|term            |distinct_tracks|
+----------------+---------------+
|rock            |13056          |
|pop             |9966           |
|electronic      |9913           |
|alternative rock|7146           |
|alternative     |6158           |
|jazz            |5930           |
|hip hop         |5924           |
|indie           |5587           |
|pop rock        |5520           |
|indie rock      |5512           |
+----------------+---------------+

+-------------------+---------------+
|term               |distinct_tracks|
+-------------------+---------------+
|djax up beats label|1              |
|afrikaans          |1              |
|indie christian    |1              |
|didgeridoo music   |1              |
|female reggae      |1              |
|dance island 2008  |1              |
|early rock n roll  |1              |
|perlon label       |1              |
|hurban             |1              |
|scottish pop       |1              |
+-------------------+---------------+
```
## Part 2: Parquet Optimization:
### 2.3

| File Name              | CSV Dataset            | Maximum Time (s) | Minimum Time (s) | Median Time (s) |
|------------------------|------------------------|------------------|------------------|-----------------|
| `csv_sum_orders.py`    | `peopleSmall.csv`          |5.3947 |    0.2148              |     0.3021         | 
| `csv_sum_orders.py`    | `peopleModerate.csv`       |  10.2670                | 2.0781 |  2.1885          |
| `csv_sum_orders.py`    | `peopleBig.csv`            |    149.9478  |      89.1213 |  101.1348       |
| `csv_big_spender.py`   | `peopleSmall.csv`          |        5.1990 |  0.1635    |    0.2107   |
| `csv_big_spender.py`   | `peopleModerate.csv`       |      8.8514  |       1.5773 |    1.6465    |
| `csv_big_spender.py`   | `peopleBig.csv`            |  100.0117      |  76.0911           |   81.6362  |
| `csv_brian.py`         | `peopleSmall.csv`          |  5.7474    |      0.1541    |   0.2118      |
| `csv_brian.py`         | `peopleModerate.csv`       |     6.1880     |   0.8989   | 0.9929    |
| `csv_brian.py`         | `peopleBig.csv`            | 88.8671   |   80.4797         |  82.2000           |

In this section, each query was run on raw CSV files of three sizes: peopleSmall.csv, peopleModerate.csv, and peopleBig.csv. As expected, CSV parsing is slow — particularly for large files.
For instance:
- csv_sum_orders.py on peopleBig.csv had a median time of ~101s.
- csv_big_spender.py and csv_brian.py also showed large slowdowns on big files, with median times over 80s.

These high latencies reflect the overhead of parsing CSVs and Spark’s limited ability to optimize reads from text-based formats. Also it highlights the limitations in speed of row-based storage formats.

### 2.4

| File Name              | Parquet Dataset            | Maximum Time (s) | Minimum Time (s) | Median Time (s) |
|------------------------|------------------------|------------------|------------------|-----------------|
| `pq_sum_orders.py`    | `peopleSmall.parquet`          |     3.6013     |   0.2043        |   0.2701          |
| `pq_sum_orders.py`    | `peopleModerate.parquet`       |    6.6381   |  3.7697                | 3.8451        |
| `pq_sum_orders.py`    | `peopleBig.parquet`            |  14.3981    |   8.1074        |  8.5747       |
| `pq_big_spender.py`   | `peopleSmall.parquet`          |   2.6122     |  0.1426            | 0.1906          |
| `pq_big_spender.py`   | `peopleModerate.parquet`       |  4.8569   |    0.1438     |      0.2170           |
| `pq_big_spender.py`   | `peopleBig.parquet`            |  7.6381    |  2.1219      |     2.1942          |
| `pq_brian.py`         | `peopleSmall.parquet`          | 5.0872   |    0.1413    |   0.1864         |
| `pq_brian.py`         | `peopleModerate.parquet`       | 5.0890  |    3.6708   |3.8456                 |
| `pq_brian.py`         | `peopleBig.parquet`            |  11.1974   | 8.4899                  |       8.8284          |

Converting the datasets to Parquet yielded dramatic performance improvements:
- pq_sum_orders.py on peopleBig.parquet dropped to ~8.6s median (from 101s).
- All small and moderate Parquet queries completed in well under a second median.

This speedup is mainly due to the column-based storage format that Parquet enforces.

### 2.5

#### Optimization 1
Data is sorted by the zipcode column using .orderBy("zipcode") prior to writing. This improves Spark’s ability to scan data more efficiently when performing group-by operations on zipcode, by improving row-group locality within Parquet files.

#### Optimization 2
The data is repartitioned across 16 partitions using .repartition(16, "zipcode"). This balances the dataset across executors and aligns it with the GROUP BY zipcode pattern in queries. It reduces shuffle cost and improves parallelism during aggregations.

#### Optimization 3
The dataset is written using .partitionBy("rewards", "loyalty"), creating subfolders in HDFS for each combination of these two boolean fields. This enables partition pruning, which allows Spark to skip irrelevant data files during filtering (WHERE rewards = FALSE AND loyalty = FALSE), significantly reducing read time.

The three write-time optimizations were applied and evaluated across all queries and file sizes and the results can be seen in the table below:


| File Name              | Parquet Dataset      | Optimization     | Maximum Time (s) | Minimum Time (s) | Median Time (s) |
|------------------------|----------------------|------------------|------------------|------------------|-----------------|
| `pq_sum_orders.py`     | `peopleSmall`        | Optimization 1   |   5.5341               |   0.2184               |   0.2997              |
| `pq_sum_orders.py`    | `peopleModerate`        | Optimization 1   |   6.5943               |     0.2901             |    0.3631             |
| `pq_sum_orders.py`          | `peopleBig`        | Optimization 1   |   8.6169               |     1.5357             |  1.6082               |
| `pq_sum_orders.py`     | `peopleSmall`        | Optimization 2   |     6.2840             |    0.2624              |     0.3441            |
| `pq_sum_orders.py`    | `peopleModerate`        | Optimization 2   |    5.7815              |   0.3319               |    0.4078             |
| `pq_sum_orders.py`          | `peopleBig`        | Optimization 2   |     7.4803             |     2.1687             |      2.2902           |
| `pq_sum_orders.py`     | `peopleSmall`        | Optimization 3   |    5.7708              |     0.2254             |     0.2845            |
| `pq_sum_orders.py`    | `peopleModerate`        | Optimization 3   |    5.9729              |   0.3770               |    0.4528             |
| `pq_sum_orders.py`          | `peopleBig`        | Optimization 3   |   16.1825               |   8.8401               |  11.6828               |
| `pq_big_spender.py`     | `peopleSmall`        | Optimization 1   |    5.2091             |    0.1472              |     0.2042            |
| `pq_big_spender.py`    | `peopleModerate`        | Optimization 1   |    5.6900             |   0.1967               |   0.2605              |
| `pq_big_spender.py`          | `peopleBig`        | Optimization 1   |    9.3639             |    2.4569              |   2.6969              |
| `pq_big_spender.py`     | `peopleSmall`        | Optimization 2   |     6.1195             |    0.2629              |    0.3266             |
| `pq_big_spender.py`    | `peopleModerate`        | Optimization 2   |    6.4057              |   0.2807               |   0.3490              |
| `pq_big_spender.py`          | `peopleBig`        | Optimization 2   | 8.4419                 |  1.2277                |     1.3585            |
| `pq_big_spender.py`     | `peopleSmall`        | Optimization 3   |  5.4456                |    0.1398              |   0.2097              |
| `pq_big_spender.py`    | `peopleModerate`        | Optimization 3   | 5.3925                 |    0.1777              |   0.2585              |
| `pq_big_spender.py`          | `peopleBig`        | Optimization 3   | 9.2212                 |    2.0660              |   2.3690              |
| `pq_brian.py`     | `peopleSmall`        | Optimization 1   |     5.2793             |     0.1532             |    0.2042             |
| `pq_brian.py`    | `peopleModerate`        | Optimization 1   |  6.9333                |      0.1957            |    0.2865             |
| `pq_brian.py`          | `peopleBig`        | Optimization 1   |   17.0120               |      6.6025            |   7.0391              |
| `pq_brian.py`     | `peopleSmall`        | Optimization 2   |     5.6125             |    0.2399              |  0.3064               |
| `pq_brian.py`    | `peopleModerate`        | Optimization 2   |   6.2555               |   0.2961               |   0.3953              |
| `pq_brian.py`          | `peopleBig`        | Optimization 2   |     16.2363             |  8.4063                |   8.8158              |
| `pq_brian.py`     | `peopleSmall`        | Optimization 3   |      7.1701            |  0.1716                |     0.2268            |
| `pq_brian.py`    | `peopleModerate`        | Optimization 3   |     5.4660             |   0.2073               | 0.3153                 |
| `pq_brian.py`          | `peopleBig`        | Optimization 3   |   24.1304               |   6.4902               |  7.1934               |

#### Optimization 1 Performance
- Helped significantly for sum_orders which does GROUP BY zipcode as the median dropped to ~1.6s on big files (from ~8.6s unoptimized).
- Also gave small performance wins on the other queries.

#### Optimization 2 Performance
- Similar benefits to sorting, particularly effective on group-by queries.
- Slightly higher median times than Optimization 1 in some cases, but consistently better than the unoptimized baseline.

#### Optimization 3 Performance
- Helped with filtering queries like big_spender and brian, which was expected since they have the "WHERE rewards = FALSE" and "loyalty = FALSE" filtering on small/moderate datasets.
- But had poor performance on peopleBig for sum_orders, with median times up to 11.7s due to:
  - Too many small partitions.
  - Mismatch with group-by on zipcode, which wasn’t part of the partitioning scheme.

### What Worked
- Parquet alone (2.4) gave the biggest boost from CSV.
- Sorting or repartitioning by zipcode (Optimizations 1 & 2) worked best for sum_orders.
- Partitioning by loyalty and rewards helped in filter-heavy queries like brian or big_spender.

### What Didn't Work Well
- Optimization 3 hurt performance for sum_orders on peopleBig — partitioning by columns unrelated to the query (loyalty, rewards) led to I/O overhead.

### General observations
- As expected, the optimizations should follow the logic of the queries in order to have the maximum effect. When that is not the case, we might even come across degrading in the performance. This is clearly visible in the tables above.
- Parquet (and more generally column-based storage) is much more efficient than CSV file format for databases, even if the readability is not as great.
