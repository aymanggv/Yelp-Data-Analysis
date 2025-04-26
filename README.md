# Yelp Data Analysis
This project is an end-to-end data analytics pipeline for analyzing Yelp reviews and businesses. It extracts large-scale JSON data, processes it using Python, stores it in Amazon S3, loads it into Snowflake, and performs sentiment analysis using SQL and UDFs. The final step is to analyze and visualize the data. The dataset can be found on [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/).

## Tech Stack
- **Python**: Data extraction, transformation, and upload to S3
- **Amazon S3**: Cloud storage for JSON data
- **Snowflake**: Data warehouse for storing and processing data
- **SQL & UDFs**: Data transformation and sentiment analysis
- **Jupyter Notebooks**: Splitting the Yelp file into multiple smaller files

## Data Pipeline Flow
![image alt](https://github.com/aymanggv/Yelp-Data-Analysis/blob/main/End%20to%20End%20Flow%20-%20Yelp.png?raw=true)

1. **Extract & Process JSON**: Python extracts Yelp reviews and business details.
2. **Upload to S3**: JSON data is uploaded to an S3 bucket.
3. **Load into Snowflake**: Data is ingested into Snowflake tables.
4. **Flatten JSON Data**: SQL queries process nested JSON structures.
5. **Sentiment Analysis**: UDF analyzes review sentiment.
6. **Data Analysis & Insights**: SQL queries provide insights.

## BI: Analytics & Reporting (Data Analysis)

    #### Objective
    Develop SQL-based analytics to deliver detailed insights into:
    - **Sentiment Analysis**
    - **Restaurant Performance**
    - **Category Trends**

    These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

## SQL Queries

### Identify number of businesses for each category:
```sql
with CTE as (
    select business_id, trim(A.value) as category 
    from tbl_yelp_businesses_cleaned,
    lateral split_to_table(business_categories, ',') A
)
select category, count(*) as number_of_businesses
from CTE
group by 1 --1 means first column
order by number_of_businesses desc
```
---

### Identify the top 10 users who reviewed the most businesses in "Restaurants" category:
```sql
with CTE as (
    select business_id, trim(A.value) as category 
    from tbl_yelp_businesses_cleaned,
    lateral split_to_table(business_categories, ',') A
)
select tbl_yelp_reviews_cleaned.user_id, count(distinct CTE.business_id) 
from CTE
Join tbl_yelp_reviews_cleaned 
on CTE.business_id = tbl_yelp_reviews_cleaned.business_id
where category = 'Restaurants'
group by 1
order by 2 desc
limit 10
```
---

### Identify most popular category of businesses based on reviews:
```sql
with CTE as (
    select business_id, trim(A.value) as category 
    from tbl_yelp_businesses_cleaned,
    lateral split_to_table(business_categories, ',') A
)
select CTE.category, count(*) as number_of_reviews 
from CTE
Join tbl_yelp_reviews_cleaned 
on CTE.business_id = tbl_yelp_reviews_cleaned.business_id
group by 1
order by 2 desc
```
---

### Identify the 3 most recent reviews for each business:
```sql
with cte as (
    select row_number() over(partition by tr.business_id order by tr.review_date desc) as rn, 
           tr.business_id, tb.business_name, tr.review_date, tr.review_stars, tr.review_text
    from tbl_yelp_reviews_cleaned as tr
    inner join tbl_yelp_businesses_cleaned as tb
    on tr.business_id = tb.business_id
)
select *
from cte
where rn<=3
```

---

### Identify the months with the highest number of reviews:
```sql
select month(review_date) as review_month, count(*) as number_of_reviews
from tbl_yelp_reviews_cleaned
group by 1
order by 2 desc
```

---


### Find the percentage of 5 star reviews for each business:
```sql
select tb.business_id, tb.business_name, count(*) as total_reviews, 
       count(case when tr.review_stars = '5' then 1 else null end) as five_star_reviews, 
       div0(five_star_reviews, total_reviews) * 100 as five_star_percentage
from tbl_yelp_reviews_cleaned as tr
inner join tbl_yelp_businesses_cleaned as tb
on tr.business_id = tb.business_id
group by 1, 2
```

---

### Identify the top 5 most reviewed businesses in each city:
```sql
with cte as(
    select tb.city, tb.business_id, tb.business_name, count(*) as total_reviews 
    from tbl_yelp_reviews_cleaned as tr
    inner join tbl_yelp_businesses_cleaned as tb
    on tr.business_id = tb.business_id
    group by 1, 2, 3
)
select *
from cte
qualify row_number() over(partition by city order by total_reviews desc) <=5
```

---

### Find the average rating of businesses that have at least 100 reviews:
```sql
select tb.business_id, tb.business_name, count(*) as total_reviews, avg(review_stars) as avg_rating
from tbl_yelp_reviews_cleaned as tr
inner join tbl_yelp_businesses_cleaned as tb
on tr.business_id = tb.business_id
group by 1, 2
having total_reviews >=100
```

---

### Identify the top 10 users who have written the most reviews along with the businesses they reviewed:
```sql
with cte as(
    select tr.user_id, count(*) as total_reviews
    from tbl_yelp_reviews_cleaned as tr
    group by 1
    order by 2 desc
    limit 10
)
select user_id, business_id
from tbl_yelp_reviews_cleaned 
where user_id in (select user_id from cte)
group by 1, 2
order by 1
```

---

### Identify the top 10 businesses with the highest sentiment reviews:
```sql
select tb.business_id, tb.business_name, count(*) as total_reviews
from tbl_yelp_reviews_cleaned as tr 
inner join tbl_yelp_businesses_cleaned as tb
on tr.business_id = tb.business_id
where sentiments = 'Positive'
group by 1,2
order by 3 desc
limit 10
```

---

## Results & Insights
Sentiment analysis provides insights into customer satisfaction.

SQL queries help identify top-performing businesses.

Data-driven decisions can be made based on review trends.


