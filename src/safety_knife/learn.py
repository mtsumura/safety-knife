from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()


data = [
    ("mike", 51, 75000, "male"),
    ("karen", 44, 82000, "female"),
    ("jake ", 16, 30000, "male"),
    ("sara", 28, 60000, "female"),
    ("david", 30, 70000, "male"),
    ("kyle", 25, 55000, "male"),
]

df0 = spark.createDataFrame(data, ["name", "age", "salary", "gender"])
df0.show()

def find_oldest(df):
    """
    Finds the oldest person in the DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame with columns 'name' and 'age'.

    Returns:
        pyspark.sql.Row: A Row object containing the name and age of the oldest person.
    """
    return df.orderBy(df.age.desc()).first()

oldest_person = find_oldest(df0)
print(f"The oldest person is {oldest_person.name} with an age of {oldest_person.age}.")

def find_youngest(df):
    youngest = df.orderBy(df.age.asc()).first()
    print(f"{youngest=}")
    return youngest

youngest_person = find_youngest(df0)
print(f"The youngest person is {youngest_person.name}")

def find_average_age(df):
    aa = df.selectExpr("avg(age)").collect()[0][0]
    print(f"Average age: {aa:.2f}")
    return aa

average_age = find_average_age(df0)
print(f"Average age: {average_age:.2f}")

def find_lowest_paid_female(df):
    """
    Finds the lowest paid female user in the DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame with columns 'name', 'salary', and 'gender'.

    Returns:
        pyspark.sql.Row: A Row object containing the name, salary, and gender of the lowest paid female.
    """
    return df.filter(df.gender == "female").orderBy(df.salary.asc()).first()

lowest_paid_female = find_lowest_paid_female(df0)
print(f"The lowest paid female is {lowest_paid_female.name} with a salary of ${lowest_paid_female.salary}.")