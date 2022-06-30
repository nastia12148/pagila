import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.config("spark.jars", "C:\\Users\\HP\\PycharmProjects\\pagila\\postgresql-42.2.5.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

customer = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "customer") \
    .option("user", "postgres").option("password", "secret").load()

address = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "address") \
    .option("user", "postgres").option("password", "secret").load()

city = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "city") \
    .option("user", "postgres").option("password", "secret").load()

film = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "film") \
    .option("user", "postgres").option("password", "secret").load()

inventory = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "inventory") \
    .option("user", "postgres").option("password", "secret").load()

film_category = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "film_category") \
    .option("user", "postgres").option("password", "secret").load()

category = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "category") \
    .option("user", "postgres").option("password", "secret").load()

rental = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "rental") \
    .option("user", "postgres").option("password", "secret").load()

film_actor = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "film_actor") \
    .option("user", "postgres").option("password", "secret").load()

actor = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "actor") \
    .option("user", "postgres").option("password", "secret").load()

#task 1

print("\n\n\n\ntask 1")

film_category.join(category, film_category.category_id == category.category_id, "left")\
    .groupby("name")\
    .count()\
    .sort(col("count").desc())\
    .show()


# task 2

print("\n\n\n\ntask 2")

film_actor.join(actor, film_actor.actor_id == actor.actor_id, "left")\
    .groupby("first_name", "last_name").count()\
    .sort(col("count").desc()).show(10)

# task 3

print("\n\n\n\ntask 3")

film.join(film_category, film.film_id == film_category.film_id, "left")\
    .join(category, category.category_id == film_category.category_id, "left")\
    .groupBy("name")\
    .sum("replacement_cost")\
    .sort(col("SUM(replacement_cost)").desc())\
    .show(1)

# task 4

print("\n\n\n\ntask 4")

film.join(inventory, inventory.film_id == film.film_id, "left")\
    .where(col("inventory_id").isNull())\
    .select(col("title"))\
    .show(film.count())

#task 5

print("\n\n\n\ntask 5")

film_category1 = film_category.join(film_actor, film_actor.film_id == film_category.film_id, "left")\
    .join(actor, actor.actor_id == film_actor.actor_id, "left")\
    .where(col("category_id") == 3)\
    .groupby("first_name","last_name").count()\
    .sort(col("count").desc())

top = np.array(film_category1.select("count").distinct().sort(col("count").desc()).collect())
top_3 = int(top[2])

film_category1.select("first_name","last_name").where(col("count") >= top_3).show()

#task 6

print("\n\n\n\ntask 6")

customer.join(address, address.address_id == customer.address_id, "left")\
    .join(city, city.city_id == address.city_id, "left")\
    .withColumn("active_c", when((customer.active == 1), 1).otherwise(0))\
    .withColumn("no_active_c", when((customer.active == 0), 1).otherwise(0))\
    .groupby("city")\
    .agg(sum("active_c").alias("active_count"),
         sum("no_active_c").alias("no_active_count"))\
    .orderBy(col("no_active_count").desc())\
    .show(customer.count())

#task 7

print("\n\n\n\ntask 7")

rental = rental.join(customer, rental.customer_id == customer.customer_id, "left")\
    .join(address, address.address_id == customer.address_id, "left")\
    .join(city, city.city_id == address.city_id, "left")\
    .join(inventory, inventory.inventory_id == rental.inventory_id, "left")\
    .join(film_category, inventory.film_id == film_category.film_id, "left")\
    .join(film, film.film_id == inventory.film_id, "left")\
    .join(category, film_category.category_id == category.category_id, "left") \
    .withColumn("_a", when((col("city").like("a%")), film.rental_duration).otherwise(0)) \
    .withColumn("_", when((col("city").like("%-%")), film.rental_duration).otherwise(0)) \
    .where(col("city").like("a%") | col("city").like("%-%"))

rental.groupBy("name").sum("_a").sort(col("sum(_a)").desc())\
    .show(1)

rental.groupBy("name").sum("_").sort(col("sum(_)").desc())\
    .show(1)

