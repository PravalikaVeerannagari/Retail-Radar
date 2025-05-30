# README: Superstore Sales Analysis with SQL and AI

# SQL Data Cleaning and Analysis
-- Load initial preview
display(spark.sql("SELECT * FROM store_csv LIMIT 10"))

-- Create clean version of the data table
spark.sql("""
CREATE OR REPLACE TABLE store_clean AS
SELECT
  `Row ID` AS row_id,
  `Order ID` AS order_id,
  TO_DATE(`Order Date`, 'yyyy-MM-dd') AS order_date,
  TO_DATE(`Ship Date`, 'yyyy-MM-dd') AS ship_date,
  `Ship Mode` AS ship_mode,
  `Customer ID` AS customer_id,
  `Customer Name` AS customer_name,
  Segment,
  `Postal Code` AS postal_code,
  City,
  State,
  Country,
  Region,
  Market,
  `Product ID` AS product_id,
  Category,
  `Sub-Category` AS sub_category,
  `Product Name` AS product_name,
  CAST(REGEXP_REPLACE(Sales, '[\\$,]', '') AS DOUBLE) AS sales,
  CAST(Quantity AS INT) AS quantity,
  CAST(Discount AS DOUBLE) AS discount,
  CAST(REGEXP_REPLACE(Profit, '[\\$,]', '') AS DOUBLE) AS profit,
  CAST(REGEXP_REPLACE(`Shipping Cost`, '[\\$,]', '') AS DOUBLE) AS shipping_cost,
  `Order Priority` AS order_priority
FROM store_csv
""")

# Key SQL Queries for Insights
spark.sql("""
SELECT customer_name, ROUND(SUM(sales),2) AS total_sales
FROM store_clean
GROUP BY customer_name
ORDER BY total_sales DESC
LIMIT 10
""")

spark.sql("""
SELECT product_name, ROUND(SUM(profit),2) AS total_profits
FROM store_clean
GROUP BY product_name
ORDER BY total_profits DESC
LIMIT 10
""")

# Python: AI Integration and Visualization
import pandas as pd
import matplotlib.pyplot as plt
from openai import OpenAI

# Load cleaned table into pandas
df = spark.table("store_clean").toPandas()

# Clean numeric fields
df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
df['profit'] = pd.to_numeric(df['profit'], errors='coerce')
df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
df.dropna(subset=['sales', 'profit', 'discount'], inplace=True)
df['profit_ratio'] = df['profit'] / df['sales']

# Calculate profit ratio by category
category_insights = df.groupby('Category')['profit_ratio'].mean().sort_values(ascending=False)
print(category_insights)

# Visualization
plt.figure(figsize=(8, 5))
category_insights.sort_values(ascending=False).plot(kind='bar')
plt.title("Average Profit Ratio by Category")
plt.ylabel("Profit Ratio")
plt.xlabel("Category")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# GPT-4 AI Summary
client = OpenAI(api_key="your_openai_api_key_here")
prompt = f"""
You are a business analyst. Based on this profit ratio by category:
{category_insights.to_string()}
Summarize the best-performing categories and suggest improvements for the least performing ones.
"""

response = client.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": prompt}]
)

print(response.choices[0].message.content.strip())
