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
from config import Mart2Config
from utils import(
    calculate_distance,
    get_timezone_by_city,
    find_nearest_city,
    load_and_prepare_cities,
    spark_init)

logger = logging.getLogger(__name__)

class ZoneGeoMart(BaseMart):
    """
    Витрина в разрезе зон (городов).
    Считает количество событий по типам за неделю и месяц.
    """
    
    def extract(self) -> DataFrame:
        """Загрузка исходных событий"""
        return self.spark.read.parquet(self.config.events_path)
    
    def _get_messages_with_cities(self, events: DataFrame, cities: DataFrame) -> DataFrame:
        """Обработка сообщений: определение города"""
        messages = events.filter(
            (F.col("event_type") == "message") &
            F.col("lat").isNotNull() &
            F.col("lon").isNotNull() &
            F.col("event.datetime").isNotNull()
        ).select(
            F.col("event.message_id").alias("event_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.datetime").alias("datetime"),
            "lat", "lon", "date"
        )
        
        messages_with_city = find_nearest_city(
            messages,
            cities,
            partition_col="event_id",
            lat_col="lat",
            lon_col="lon"
        ).select(
            "user_id",
            F.col("event_id").alias("message_id"),
            F.col("zone_id"),
            F.col("city_name").alias("city"),
            "datetime",
            "date"
        )
        
        return messages_with_city
    
    def _get_reactions_with_cities(self, events: DataFrame, last_location: DataFrame) -> DataFrame:
        """Обработка реакций: присвоение последних координат пользователя"""
        reactions = events.filter(
            (F.col("event_type") == 'reaction') &
            F.col("event.datetime").isNotNull() &
            F.col("event.reaction_from").isNotNull()
        ).selectExpr(
            "event.reaction_from as user_id",
            "event.reaction_type",
            "event.datetime"
        )
        
        reactions_with_city = reactions.join(
            last_location.select("user_id", "zone_id", "city_name"),
            "user_id",
            "left"
        ).filter(F.col("zone_id").isNotNull())
        
        return reactions_with_city
    
    def _get_subscriptions_with_cities(self, events: DataFrame, last_location: DataFrame) -> DataFrame:
        """Обработка подписок: присвоение последних координат пользователя"""
        subscriptions = events.filter(
            (F.col("event_type") == "subscription") &
            F.col("event.subscription_user").isNotNull() &
            F.col("event.datetime").isNotNull()
        ).selectExpr(
            "event.subscription_user as user_id",
            "event.subscription_channel",
            "event.datetime"
        )
        
        subscriptions_with_city = subscriptions.join(
            last_location.select("user_id", "zone_id", "city_name"),
            "user_id",
            "left"
        ).filter(F.col("zone_id").isNotNull())
        
        return subscriptions_with_city
    
    def _get_registrations_with_cities(self, events: DataFrame, cities: DataFrame) -> DataFrame:
        """Обработка регистраций (первые сообщения)"""
        first_messages = events.filter(
            (F.col("event_type") == "message") &
            F.col("event.message_from").isNotNull() &
            F.col("event.datetime").isNotNull()
        ).select(
            F.col("event.message_id").alias("event_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.datetime").alias("datetime"),
            "lat", "lon"
        )
        
        window = Window.partitionBy("user_id").orderBy("datetime")
        first_message = first_messages.withColumn(
            'rnk', F.row_number().over(window)
        ).filter(F.col('rnk') == 1).drop('rnk')
        
        # Определяем город для первого сообщения
        first_with_city = find_nearest_city(
            first_message,
            cities,
            partition_col="event_id",
            lat_col="lat",
            lon_col="lon"
        ).select(
            "user_id",
            F.col("zone_id"),
            F.col("city_name").alias("city"),
            "datetime"
        )
        
        return first_with_city
    
    def _add_time_columns(self, df: DataFrame, datetime_col: str = "datetime") -> DataFrame:
        """Добавляет колонки month и week"""
        return df.withColumn(
            "month",
            F.date_format(F.to_timestamp(datetime_col), "yyyy-MM")
        ).withColumn(
            "week",
            ((F.dayofmonth(F.to_timestamp(datetime_col)) - 1) / 7).cast("int") + 1
        )
    
    def transform(self, data) -> DataFrame:
        """Основная трансформация для витрины зон"""
        events = data
        logger.info("Loading cities data")
        cities = load_and_prepare_cities(self.spark, self.config.geo_path)
        
        logger.info("Processing messages")
        messages = self._get_messages_with_cities(events, cities)
        messages = self._add_time_columns(messages)
        messages = messages.select(
            "user_id", "zone_id", "city", "month", "week",
            F.lit("message").alias("event_type")
        )
        
        logger.info("Getting last user locations for reactions/subscriptions")
        last_location = messages.select(
            "user_id", "zone_id", "city"
        ).dropDuplicates(["user_id"])
        
        logger.info("Processing reactions")
        reactions = self._get_reactions_with_cities(events, last_location)
        reactions = self._add_time_columns(reactions)
        reactions = reactions.select(
            "user_id", "zone_id", "city", "month", "week",
            F.lit("reaction").alias("event_type")
        )
        
        logger.info("Processing subscriptions")
        subscriptions = self._get_subscriptions_with_cities(events, last_location)
        subscriptions = self._add_time_columns(subscriptions)
        subscriptions = subscriptions.select(
            "user_id", "zone_id", "city", "month", "week",
            F.lit("subscription").alias("event_type")
        )
        
        logger.info("Processing registrations")
        registrations = self._get_registrations_with_cities(events, cities)
        registrations = self._add_time_columns(registrations)
        registrations = registrations.select(
            "user_id", "zone_id", "city", "month", "week",
            F.lit("registration").alias("event_type")
        )
        
        # Объединяем все события
        logger.info("Union all events")
        all_events = messages.union(reactions) \
            .union(subscriptions) \
            .union(registrations)
        
        # Создаем витрину
        logger.info("Creating pivot table")
        weekly_pivot = all_events.groupBy("month", "week", "zone_id", "city") \
            .pivot("event_type", ["message", "reaction", "subscription", "registration"]) \
            .agg(F.count("*")).fillna(0)
        
        # Добавляем месячные итоги
        window_month = Window.partitionBy("month", "zone_id", "city")
        result = weekly_pivot.withColumn(
            "month_message", F.sum("message").over(window_month)
        ).withColumn(
            "month_reaction", F.sum("reaction").over(window_month)
        ).withColumn(
            "month_subscription", F.sum("subscription").over(window_month)
        ).withColumn(
            "month_user", F.sum("registration").over(window_month)
        )
        
        return result.select(
            "month",
            "week",
            "zone_id",
            F.col("message").alias("week_message"),
            F.col("reaction").alias("week_reaction"),
            F.col("subscription").alias("week_subscription"),
            F.col("registration").alias("week_user"),
            "month_message",
            "month_reaction",
            "month_subscription",
            "month_user"
        )
    
    def load(self, df: DataFrame) -> None:
        """Сохранение витрины"""
        df.write \
            .mode(self.config.write_mode) \
            .format(self.config.output_format) \
            .save(self.config.output_path)
        
        count = df.count()
        logger.info(f"Saved zone mart with {count} rows to {self.config.output_path}")

if __name__ == "__main__":
    import sys
    from config import Mart2Config
    from utils import spark_init
    
    
    # Парсинг аргументов
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]

    # дополнительные аргументы для конкретной витрины
    
    spark = spark_init('Mart_2','local')
    
    try:
        config = Mart2Config(
            events_path=events_path,
            geo_path=geo_path,
            output_path=output_path,
        )

        mart = ZoneGeoMart(spark, config)
        mart.run()
    
    finally:
        spark.stop()        