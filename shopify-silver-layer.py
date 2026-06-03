import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# --- Read Bronze using Spark (not Pandas) and explode line_items properly ---
df_bronze = spark.table("bronze_shopify_orders")

# ✅ Explode line_items at Spark level before converting to Pandas
df_exploded_spark = df_bronze.withColumn("line_item", F.explode_outer(F.col("line_items")))

# ✅ Flatten the fields we need directly in Spark
df_flat = df_exploded_spark.select(
    F.col("id").alias("order_id"),
    F.col("created_at"),
    F.col("closed_at"),
    F.col("fulfilled_at") if "fulfilled_at" in df_bronze.columns else F.lit(None).alias("fulfilled_at"),
    F.col("customer.id").alias("customer_id"),
    F.col("customer.tags").alias("customer_tags"),
    F.col("total_discounts"),
    F.col("total_shipping_price_set.shop_money.amount").alias("shipping_cost"),
    F.col("line_item.title").alias("product_title"),
    F.col("line_item.product_id").alias("product_id"),
    F.col("line_item.sku").alias("sku"),
    F.col("line_item.quantity").alias("quantity"),
    F.col("line_item.price").alias("price"),
)

# ✅ Now convert to Pandas — all fields already flat
df = df_flat.toPandas()
print(f"📥 {len(df)} rows after exploding line items")
print(df.columns.tolist())

n = len(df)

# --- Build Silver table ---
df_orders = pd.DataFrame()
order_dt  = pd.to_datetime(df["created_at"].values, utc=True)

df_orders["row_id"]        = range(1, n + 1)
df_orders["order_id"]      = df["order_id"].values
df_orders["order_date"]    = order_dt.date
df_orders["ship_date"]     = pd.to_datetime(df["closed_at"].values, errors="coerce", utc=True)
df_orders["customer_id"]   = df["customer_id"].values

# Segment
df_orders["segment"] = df["customer_tags"].apply(
    lambda x: "Corporate"    if pd.notna(x) and "wholesale" in str(x).lower()
    else      "Home Office"  if pd.notna(x) and "vip"       in str(x).lower()
    else      "Consumer"
)

# Product fields
df_orders["product_title"] = df["product_title"].values
df_orders["product_id"]    = df["product_id"].values
df_orders["sku"]           = df["sku"].values

# Financials
df_orders["quantity"]      = pd.to_numeric(df["quantity"],      errors="coerce").fillna(0)
price                      = pd.to_numeric(df["price"],         errors="coerce").fillna(0)
df_orders["revenue"]       = price * df_orders["quantity"]
df_orders["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0)
discount                   = pd.to_numeric(df["total_discounts"],errors="coerce").fillna(0)
df_orders["profit"]        = df_orders["revenue"] - discount - df_orders["shipping_cost"]

# Priority
def assign_priority(p):
    if p >= 500: return "Critical"
    if p >= 200: return "High"
    if p >= 50:  return "Medium"
    return "Low"

df_orders["order_priority"]  = df_orders["profit"].apply(assign_priority)
df_orders["month"]           = order_dt.month
df_orders["year"]            = order_dt.year
df_orders["processing_days"] = (
    pd.to_datetime(df_orders["ship_date"],  utc=True) -
    pd.to_datetime(df_orders["order_date"], utc=True)
).dt.days.fillna(0).astype(int)
df_orders["extracted_at"]    = pd.Timestamp.now()

# --- Write to Silver ---
spark_df = spark.createDataFrame(df_orders)
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_shopify_orders")

print(f"✅ Silver: {len(df_orders)} rows written to silver_shopify_orders")
display(spark.table("silver_shopify_orders").select(
    "order_id", "order_date", "segment",
    "quantity", "revenue", "profit",
    "order_priority", "year", "month"
).limit(5))
