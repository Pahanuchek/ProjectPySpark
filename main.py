from pyspark.sql import SparkSession

# Создаем сессию
spark = SparkSession.builder.appName("Products and Categories").getOrCreate()

# Создаем пример данных
products_data = [(1, "Product A"), (2, "Product B"), (3, "Product C")]
categories_data = [(1, "Category X"), (2, "Category Y"), (3, "Category Z")]
product_categories_data = [(1, 1), (1, 2), (2, 3)]

# Создаем DataFrame из примеров данных
products = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_categories = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

# Соединяем продукты с категориями
product_category_pairs = product_categories \
    .join(products, "product_id") \
    .join(categories, "category_id") \
    .select("product_name", "category_name")

# Находим продукты без категорий
products_with_categories = product_categories.select("product_id").distinct()
products_without_categories = products \
    .join(products_with_categories, "product_id", "left_anti") \
    .select("product_name")

# 3. Вывод результата
products_without_categories.show(truncate=False)

# Завершаем сессию
spark.stop()
