# PySpark: Products and Categories

This project demonstrates how to use PySpark to join product and category data to get all "Product Name - Category Name" pairs, including products without categories.

## How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the script:
   ```bash
   python main.py
   ```
   
## Example Output

```
+------------+--------------------+
|product_name|category_name       |
+------------+--------------------+
|Bread       |Bakery              |
|Milk        |Dairy Products      |
|Butter      |Dairy Products      |
|Cheese      |null                |
+------------+--------------------+
```

## Dependencies

- Python 3.8+
- PySpark
