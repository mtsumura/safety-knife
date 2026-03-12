from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()


data = [
    ("James", 34),
    ("Michael", 29),
    ("Robert", 28),
    ("John", 37),
    ("David", 30),
    ("Alice", 25),
    ("Emily", 22)
]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Find the oldest person in the dataframe
oldest_person = df.orderBy(df.Age.desc()).first()
print(f"The oldest person is {oldest_person.Name}, who is {oldest_person.Age} years old.")

# Find the average age of all people
average_age = df.selectExpr("avg(Age)").collect()[0][0]
print(f"The average age is {average_age:.2f} years.")

# Find the youngest person in the dataframe
youngest_person = df.orderBy(df.Age.asc()).first()
print(f"The youngest person is {youngest_person.Name}, who is {youngest_person.Age} years old.")

df.show()

