from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import random
from datetime import datetime, timedelta

# Инициализация Spark
spark = SparkSession.builder \
    .appName("Synthetic Data Generation") \
    .getOrCreate()

# Параметры
num_records = 1000  # Количество записей
products = ["Товар A", "Товар B", "Товар C", "Товар D", "Товар E"]

# Генерация данных
data = []
start_date = datetime.now() - timedelta(days=365)

for _ in range(num_records):
    date = start_date + timedelta(days=random.randint(0, 365))
    user_id = random.randint(1000, 9999)
    product = random.choice(products)
    quantity = random.randint(1, 10)
    price = round(random.uniform(5.0, 100.0), 2)

    data.append((date.strftime('%Y-%m-%d'), user_id, product, quantity, price))

# Создание DataFrame
columns = ["Дата", "UserID", "Продукт", "Количество", "Цена"]
df = spark.createDataFrame(data, schema=columns)

# Сохранение в CSV
df.write.csv('synthetic_data.csv', header=True)

# Завершение работы Spark
spark.stop()
