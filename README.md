# Yelp-Data-Analysis
This project is an end-to-end data analytics pipeline for analyzing Yelp reviews and businesses. It extracts large-scale JSON data, processes it using Python, stores it in Amazon S3, loads it into Snowflake, and performs sentiment analysis using SQL and UDFs. The final step is to analyze and visualize the data. 

<h2>Tech Stack</h2>
<ul>
    <li><strong>Python</strong>: Data extraction, transformation, and upload to S3</li>
    <li><strong>Amazon S3</strong>: Cloud storage for JSON data</li>
    <li><strong>Snowflake</strong>: Data warehouse for storing and processing data</li>
    <li><strong>SQL & UDFs</strong>: Data transformation and sentiment analysis</li>
    <li><strong>Jupyter Notebooks</strong>: Splitting the Yelp file into multiple smaller files</li>
</ul>

<h2>Data Pipeline Flow</h2>
![Image](https://github.com/user-attachments/assets/636b54df-0b31-4950-b9f5-1ffa50166644)
<ol>
    <li>Extract & Process JSON: Python extracts Yelp reviews and business details.</li>
    <li>Upload to S3: JSON data is uploaded to an S3 bucket.</li>
    <li>Load into Snowflake: Data is ingested into Snowflake tables.</li>
    <li>Flatten JSON Data: SQL queries process nested JSON structures.</li>
    <li>Sentiment Analysis: UDF analyzes review sentiment.</li>
    <li>Data Analysis & Insights: SQL queries provide insights.</li>
</ol>
