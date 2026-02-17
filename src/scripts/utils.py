from pyspark.sql import SparkSession,DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import logging

logger = logging.getLogger(__name__)

def spark_init(app_name: str, master: str ='local') ->SparkSession:
    """Создание Spark сессиии"""
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.adaptive.enabled","false") \
        .getOrCreate()

def calculate_distance(df:DataFrame,lat1:str,lon1:str,lat2:str,lon2:str) -> DataFrame:
    """
    Вычисляет расстояние между двумя точками в км.
    Переиспользуется во все витринах.
    """
    return df.withColumn("dlat", F.radians(F.col(lat1) - F.col(lat2))) \
        .withColumn("dlon", F.radians(F.col(lon1) - F.col(lon2))) \
        .withColumn("a", F.pow(F.sin(F.col("dlat")/2), 2) + 
                           F.cos(F.radians(F.col(lat1))) *
                           F.cos(F.radians(F.col(lat2))) *
                           F.pow(F.sin(F.col("dlon")/2), 2)) \
        .withColumn("c", 2 * F.asin(F.sqrt(F.col("a")))) \
        .withColumn("distance_km", 6371 * F.col("c"))

def get_timezone_by_city(city_col:str) -> F.Column:
    """
    Возвращает таймзону по названию города.
    Переиспользуется во всех витринах.
    """
    return F.when(F.col(city_col) == "Sydney", "Australia/Sydney") \
        .when(F.col(city_col) == "Melbourne", "Australia/Melbourne") \
        .when(F.col(city_col) == "Brisbane", "Australia/Brisbane") \
        .when(F.col(city_col) == "Perth", "Australia/Perth") \
        .when(F.col(city_col) == "Adelaide", "Australia/Adelaide") \
        .when(F.col(city_col) == "Hobart", "Australia/Hobart") \
        .when(F.col(city_col) == "Darwin", "Australia/Darwin") \
        .when(F.col(city_col) == "Canberra", "Australia/Sydney") \
        .when(F.col(city_col) == "Newcastle", "Australia/Sydney") \
        .when(F.col(city_col) == "Wollongong", "Australia/Sydney") \
        .when(F.col(city_col) == "Maitland", "Australia/Sydney") \
        .when(F.col(city_col) == "Gold Coast", "Australia/Brisbane") \
        .when(F.col(city_col) == "Townsville", "Australia/Brisbane") \
        .when(F.col(city_col) == "Cairns", "Australia/Brisbane") \
        .when(F.col(city_col) == "Toowoomba", "Australia/Brisbane") \
        .when(F.col(city_col) == "Ipswich", "Australia/Brisbane") \
        .when(F.col(city_col) == "Mackay", "Australia/Brisbane") \
        .when(F.col(city_col) == "Rockhampton", "Australia/Brisbane") \
        .when(F.col(city_col) == "Geelong", "Australia/Melbourne") \
        .when(F.col(city_col) == "Ballarat", "Australia/Melbourne") \
        .when(F.col(city_col) == "Bendigo", "Australia/Melbourne") \
        .when(F.col(city_col) == "Launceston", "Australia/Hobart") \
        .when(F.col(city_col) == "Bunbury", "Australia/Perth") \
        .when(F.col(city_col) == "Cranbourne", "Australia/Melbourne") \
        .otherwise("Australia/Sydney")

def load_and_prepare_cities(spark, path: str) -> DataFrame:
    """
    Загружает и подготавливает данные о городах.
    Возвращает ВСЕ возможные колонки, витрины сами выберут нужные.
    """
    cities = spark.read.parquet(path)
    
    return cities.withColumn(
        "lat", F.regexp_replace("lat", ",", ".").cast(DoubleType())
    ).withColumn(
        "lng", F.regexp_replace("lng", ",", ".").cast(DoubleType())
    ).withColumnRenamed("id", 'zone_id') \
     .withColumnRenamed("lat", "city_lat") \
     .withColumnRenamed("lng", "city_lon") \
     .withColumnRenamed("city", "city_name") \
     .hint("broadcast")

def find_nearest_city(df_with_coords: DataFrame,
                      cities_df: DataFrame,
                      partition_col: str,
                      lat_col: str = "lat",
                      lon_col: str = "lon") -> DataFrame:
    """
    Для каждой строки находит ближайший город.
    Возвращает ВСЕ колонки из исходного df + city_id, city_name, distance_km
    """
    # Кросс-джойн
    with_cities = df_with_coords.crossJoin(cities_df)
    
    # Расчет расстояния
    with_dist = calculate_distance(
        with_cities,
        lat1=lat_col, lon1=lon_col,
        lat2='city_lat', lon2='city_lon'
    )
    
    # Выбор ближайшего
    window = Window.partitionBy(partition_col).orderBy('distance_km')
    
    return with_dist.withColumn('rnk', F.row_number().over(window)) \
        .filter(F.col('rnk') == 1) \
        .drop('rnk', 'city_lat', 'city_lon')