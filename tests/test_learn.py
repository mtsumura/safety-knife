
import pytest
from pyspark.sql import SparkSession
from safety_knife.learn import find_oldest, find_youngest, find_average_age, find_lowest_paid_female


def test_find_oldest():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    
    oldest_person = find_oldest(df)
    assert oldest_person.name == "Charlie"
    assert oldest_person.age == 35

def test_find_youngest():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    
    youngest_person = find_youngest(df)
    assert youngest_person.name == "Bob"
    assert youngest_person.age == 25

def test_find_average_age():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    
    average_age = find_average_age(df)
    assert average_age == 30.0
def test_find_lowest_paid_female():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("Alice", "female", 5000), ("Bob", "M=male", 6000), ("Charlie", "female", 4000)]
    columns = ["name", "gender", "salary"]
    df = spark.createDataFrame(data, columns)

    lowest_paid_female = find_lowest_paid_female(df)
    assert lowest_paid_female.name == "Charlie"
    assert lowest_paid_female.gender == "female"
    assert lowest_paid_female.salary == 4000
# Run the tests
if __name__ == "__main__":
    pytest.main()

