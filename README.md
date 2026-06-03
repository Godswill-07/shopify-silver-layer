# shopify-silver-layer
Transformation layer that reads raw Bronze orders, explodes  line items, 
cleans nested Spark Row objects, and produces a  flat, 
analytics-ready Silver Delta table.

## Architecture
bronze_shopify_orders → Clean & Transform → silver_shopify_orders

## Features
- Explodes nested line_items using PySpark F.explode_outer
- Flattens Spark Row/Struct objects before Pandas conversion
- Safe column extraction with fallbacks for missing fields
- Customer segmentation logic (Corporate/Home Office/Consumer)
- Order priority scoring based on profit thresholds
- Processing days calculation (order → ship date)

## Tech Stack
- Microsoft Fabric
- PySpark
- Delta Lake
- Python (Pandas)

## Transformations Applied
| Field | Logic |
|---|---|
| segment | Tagged by customer tags (wholesale/vip/default) |
| order_priority | Critical ≥500, High ≥200, Medium ≥50, Low <50 |
| processing_days | ship_date minus order_date in days |
| revenue | price × quantity per line item |
| profit | revenue minus discount minus shipping |

## Table Schema
| Column | Type | Description |
|---|---|---|
| order_id | long | Shopify order ID |
| order_date | date | Date order was placed |
| ship_date | timestamp | Date order was fulfilled |
| customer_id | long | Customer identifier |
| segment | string | Customer segment |
| product_title | string | Product name |
| quantity | long | Units ordered |
| revenue | double | Line item revenue |
| profit | double | Line item profit |
| order_priority | string | Priority classification |

## How to Run
1. Ensure bronze_shopify_orders table exists
2. Attach LH_Landing lakehouse
3. Run after Bronze notebook completes
