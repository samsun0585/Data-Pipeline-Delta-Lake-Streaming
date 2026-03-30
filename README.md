This assignment is about building a data pipeline using Delta Lake and Spark. First, we take a CSV file with employee data and load it into a Delta table. This helps convert raw data into a structured format that we can query and update easily.

Then we check the data quality by finding missing values and duplicate rows to make sure the data is clean. After that, we define a schema so that the data types are fixed, and Delta Lake will not allow incorrect data to be written.

Next, we perform operations like adding new employee records and updating existing ones, such as changing performance scores. Delta Lake handles these updates safely using transactions.

We also run simple queries like counting total records and finding how many unique cities are present. One important feature is time travel, where we can go back to a previous version of the table before updates. This is useful if something goes wrong and we need to recover old data.

Then we do some advanced analysis by grouping employees by department, collecting performance scores, filtering scores greater than 4, and counting them.

In the second part, we simulate real-time data using Spark streaming. Instead of static data, new employee records are generated continuously and written into the Delta table. We monitor the stream to make sure it is running and query the table to see new data coming in.

Finally, we run analytics like average performance score and employee count by department on the streaming data. At the end, we stop the stream and clean up resources.

Overall, this assignment shows how to build a reliable data pipeline that handles both batch and real-time data using Delta Lake.


Part 1: Building Reliable Data Pipelines with Delta Lake
This homework aims to provide hands-on experience in building and maintaining a reliable
data pipeline using Delta Lake. The focus will be on key Delta Lake features, such as schema
enforcement, data versioning, querying, and error recovery.
The dataset for this project represents a simplified employee database with the following
fields:
employee_id (integer): A unique identifier for each employee.
age (integer): The age of the employee, ranging between 21 and 65.
city (string): The city where the employee resides.
department (string): The department in which the employee works (e.g., Engineering,
Sales, HR).
salary (integer): The employee’s annual salary in USD, ranging between 40,000 and
150,000.
Join_date (date): The date the employee joined the company.
Performance_score (double): A performance rating on a scale of 1.0 to 5.0.
The dataset is stored in a CSV format and will be used to create and manipulate a Delta Lake
table.
Questions
1. Data Ingestion and Setup:
(a) Load the CSV dataset into a Delta Lake table. Write the PySpark code for loading the
dataset and saving it as a Delta table.
(b) Check for missing or duplicate rows in the dataset. How many such rows did you find for
each column?

2. Schema Enforcement:
(a) Define a schema for the Delta table and enforce it when loading the dataset.
(b) What happens if you try to load data (Write operation) with a mismatched schema?
Demonstrate this with an example.
3. Data Updates:
(a) Append new employee data to the Delta table. Write code to verify that the new data has
been appended successfully.
(b) Update the performance score of employees in a specific city. Show the PySpark code
you used.
4. Querying and Insights:
(a) Write a query to calculate the total number of records in the Delta table.
(b) Find the count of unique cities in the dataset.
5. Time Travel and Recovery:
(a) Use Delta Lake’s time travel feature to retrieve the table state before the most recent
update. Write the code and describe the output.
(b) Explain a scenario where the time travel feature can be useful in real-world applications.
6. Advanced Analytics with Higher-Order Functions:
(a) For each department, collect the performance score values of all employees into an
array.
(b) Then, using the filter higher-order function, return only the scores greater than 4.0.
(c) Count the number of such scores for each department and identify the department with
the highest count.

Part2: Delta Lake with Structured Streaming
This part of the assignment builds on Part 1’s Delta Lake work by introducing streaming
ingestion using Spark. You’ll simulate a real-time employee data feed and write it
incrementally into a Delta table.
Dataset Schema
Simulate streaming data with the following fields: employee_id, age, city, department,
salary, join_date, performance_score
Questions
7. Load Employee Data into Delta Table (Static Write)
(a) Load the provided CSV file into a Spark DataFrame. Save it as a Delta table using
.write.format(&quot;delta&quot;).
8. Simulate Streaming Ingestion
(a) Use readStream.format(&quot;rate&quot;) to simulate streaming data.
(b) Generate synthetic employee records using transformations. Use UDFs or random

functions to populate fields like city and department, ensuring that some of the new values
are not present in the original dataset.
(c) Write the stream to the same Delta table created in Question 7.
9. Monitor the Stream
(a) Confirm the stream is active using spark.streams.active
(b) Query the Delta table periodically to observe new records.
10. Run Simple Analytics
Use spark.sql() to answer:
(a) Average performance score by department
(b) Count of employees by department
11. Stop and Clean Up
(a) Stop all active streams
(b) Delete all checkpoint folders used.
