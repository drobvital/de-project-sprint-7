#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import DataFrame
import sys
import os
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Добавляем путь к папке со скриптами в sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_mart import BaseMart
from config import Mart1Config
from utils import(
    calculate_distance,
    get_timezone_by_city,
    find_nearest_city,
    load_and_prepare_cities,
    spark_init)


logger = logging.getLogger(__name__)

class UserGeoMart(BaseMart):
    """
    Витрина в разрезе пользователей.
    Определяет актуальный город, домашний город, статистику путешествий и локальное время.
    """
    
    def extract(self) -> DataFrame:
        """Загрузка исходных событий и данных о городах"""
        return self.spark.read.parquet(self.config.events_path)
    
    def _get_act_city(self, messages_with_city: DataFrame) -> DataFrame:
        """Актуальный город (последнее сообщение пользователя)"""
        clean_data = messages_with_city.filter(F.col("datetime").isNotNull())
        window = Window.partitionBy("user_id").orderBy(F.desc("datetime"))
        
        return clean_data.withColumn("rank", F.row_number().over(window)) \
            .filter(F.col("rank") == 1) \
            .selectExpr("user_id", "city_name as act_city", "datetime")
    
    def _get_home_city(self, messages_with_city: DataFrame) -> DataFrame:
        """
        Домашний город — последний город, в котором пользователь был дольше 27 дней.
        Используем days_gap (разница между первым и последним днем в городе) >= 27.
        """
        # Уникальные дни в городах для каждого пользователя
        user_city_dates = messages_with_city.select(
            "user_id", "zone_id", "city_name", "date"
        ).distinct()
        
        # Группируем по пользователю и городу
        city_days = user_city_dates.groupBy("user_id", "city_name").agg(
            F.count("*").alias('total_days'),
            F.min("date").alias('first_date'),
            F.max("date").alias('last_date')
        ).withColumn('days_gap', F.datediff("last_date", "first_date"))
        
        # Фильтр по количеству дней (гипотеза: если разница между первым и последним днем >= 27)
        home_cities = city_days.filter(F.col("days_gap") >= self.config.min_days_for_home)
        
        # Выбираем последний по дате город (если их несколько)
        window = Window.partitionBy("user_id").orderBy(F.desc("last_date"))
        
        return home_cities.withColumn("rank", F.row_number().over(window)) \
            .filter(F.col("rank") == 1) \
            .select("user_id", F.col("city_name").alias("home_city"))
    
    def _get_travel_stats(self, messages_with_city: DataFrame) -> DataFrame:
        """Статистика путешествий: количество посещенных городов и массив в порядке посещения"""
        clean_data = messages_with_city.filter(F.col("datetime").isNotNull())
        window = Window.partitionBy('user_id').orderBy('datetime')
        
        # Определяем смену города
        with_prev = clean_data.withColumn('prev_city', F.lag('city_name').over(window))
        
        # Фильтруем только смены города (первый город или отличающийся от предыдущего)
        city_changes = with_prev.filter(
            (F.col("prev_city").isNull()) | (F.col("city_name") != F.col("prev_city"))
        )
        
        return city_changes.groupBy("user_id").agg(
            F.count("*").alias("travel_count"),
            F.collect_list("city_name").alias("travel_array")
        )
    
    def _add_local_time(self, df: DataFrame) -> DataFrame:
        """Добавляет локальное время на основе act_city и datetime"""
        df_with_tz = df.withColumn(
            "timezone",
            get_timezone_by_city("act_city")
        )
        
        return df_with_tz.withColumn(
            "timestamp_utc",
            F.to_timestamp(F.col("datetime"), self.config.date_format)
        ).withColumn(
            "local_time",
            F.from_utc_timestamp(F.col("timestamp_utc"), F.col("timezone"))
        ).drop("timezone", "timestamp_utc", "datetime")
    
    def transform(self,data) -> DataFrame:
        """Основная трансформация для витрины пользователей"""
        events = data
        
        logger.info("Loading cities data")
        cities_prepared = load_and_prepare_cities(self.spark,self.config.geo_path)
        
        logger.info("Processing messages with cities")
        messages = events.filter(
            (F.col("event_type") == "message") &
            F.col("event.message_from").isNotNull()
        ).selectExpr(
            "event.message_id as event_id",
            "event.message_from as user_id",
            "event.datetime",
            "lat", "lon", "date"
        )
        messages_with_city = find_nearest_city(
            messages,
            cities_prepared,
            F.struct('event_id'),
            lat_col='lat',
            lon_col='lon'
         ).select(
             'event_id',
             'user_id',
             'datetime',
             'date',
             'zone_id',
             'city_name',
             'lat',
             'lon'
         )
        logger.info("Calculating act_city (last message)")
        act_city = self._get_act_city(messages_with_city)
        
        logger.info("Calculating home_city (27+ days)")
        home_city = self._get_home_city(messages_with_city)
        
        logger.info("Calculating travel statistics")
        travel_stats = self._get_travel_stats(messages_with_city)
        
        # Объединяем все компоненты
        logger.info("Joining all components")
        result = act_city.join(
            home_city, "user_id", "left"
        ).join(
            travel_stats, "user_id", "left"
        )
        
        # Добавляем локальное время
        result = self._add_local_time(result)
        
        return result.select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "local_time"
        )
    
    def load(self, df: DataFrame) -> None:
        """Сохранение витрины"""
        df.write \
            .mode(self.config.write_mode) \
            .format(self.config.output_format) \
            .save(self.config.output_path)
        
        count = df.count()
        logger.info(f"Saved {count} user profiles to {self.config.output_path}")

if __name__ == "__main__":
    import sys
    from config import Mart1Config 
    from utils import spark_init 
    
    # Парсинг аргументов
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    min_days_for_home = int(sys.argv[4])
    date_format = sys.argv[5]

    # дополнительные аргументы для конкретной витрины
    
    spark = spark_init('Mart_1','local')
    
    try:
        config = Mart1Config(
            events_path=events_path,
            geo_path=geo_path,
            output_path=output_path,
            min_days_for_home = min_days_for_home,
            date_format = date_format
        )

        mart = UserGeoMart(spark, config)
        mart.run()
    
    finally:
        spark.stop()        