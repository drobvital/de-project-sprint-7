from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import logging

from .base_mart import BaseMart
from .utils import (
    calculate_distance,
    get_timezone_by_city, 
    find_nearest_city,
    load_and_prepare_cities,
    spark_init
)

logger = logging.getLogger(__name__)

class RecommendationMart(BaseMart):
    """
    Витрина для рекомендации друзей.
    Условия: общий канал, не переписывались, расстояние ≤ 1 км
    """
    
    def extract(self) -> DataFrame:
        """Загрузка исходных событий"""
        return self.spark.read.parquet(self.config.events_path)
    
    def _get_subscriptions(self, events: DataFrame) -> DataFrame:
        """Получение подписок на каналы"""
        return events.filter(
            (F.col("event_type") == 'subscription') &
            F.col("event.datetime").isNotNull() &
            F.col("event.user").isNotNull()
        ).selectExpr(
            'event.user as user_id',
            'event.subscription_channel',
            'event.datetime'
        )
    
    def _get_last_messages(self, events: DataFrame) -> DataFrame:
        """Последние координаты пользователей из сообщений"""
        messages = events.filter(
            (F.col('event_type') == 'message') &
            F.col('event.message_from').isNotNull() &
            F.col('event.datetime').isNotNull()
        ).selectExpr(
            'event.message_from as user_id',
            'event.datetime',
            'lat',
            'lon'
        )
        
        window = Window.partitionBy('user_id').orderBy(F.desc('datetime'))
        return messages.withColumn('rk', F.row_number().over(window)) \
            .filter(F.col('rk') == 1) \
            .select('user_id', 'lat', 'lon')
    
    def _get_message_history(self, events: DataFrame) -> DataFrame:
        """История переписок между пользователями"""
        messages = events.filter(
            (F.col('event_type') == 'message') &
            F.col('event.message_from').isNotNull() &
            F.col('event.message_to').isNotNull()
        ).selectExpr(
            'cast(event.message_from as int) as sender_id',
            'cast(event.message_to as int) as receiver_id'
        ).distinct()
        
        return messages.select(
            F.least('sender_id', 'receiver_id').alias('user1'),
            F.greatest('sender_id', 'receiver_id').alias('user2')
        ).distinct()
    

    
    def transform(self, events: DataFrame) -> DataFrame:
        """Основная трансформация для витрины рекомендаций"""
        
        # 1. Подписчики нужного канала
        subscriptions = self._get_subscriptions(events)
        channel_users = subscriptions.filter(
            F.col('subscription_channel') == self.config.channel_id
        ).select(
            F.col('user_id').cast('integer').alias('user_id'),
            'datetime'
        ).distinct()
        
        logger.info(f"Channel {self.config.channel_id} has {channel_users.count()} subscribers")
        
        # 2. Последние координаты
        last_coords = self._get_last_messages(events)
        
        # 3. Только пользователи с координатами
        users_with_coords = channel_users.join(
            last_coords.select(
                F.col('user_id').cast('integer'),
                'lat', 'lon'
            ),
            'user_id',
            'inner'
        ).select('user_id', 'lat', 'lon')
        
        logger.info(f"Users with coordinates: {users_with_coords.count()}")
        
        # 4. Все возможные пары
        users_alias = users_with_coords.alias('u')
        all_pairs = users_alias.join(
            users_alias,
            F.col('u.user_id') < F.col('u2.user_id')
        ).select(
            F.col('u.user_id').alias('user_left'),
            F.col('u.lat').alias('lat_left'),
            F.col('u.lon').alias('lon_left'),
            F.col('u2.user_id').alias('user_right'),
            F.col('u2.lat').alias('lat_right'),
            F.col('u2.lon').alias('lon_right')
        )
        
        # 5. Исключаем тех, кто уже переписывался
        comm_pairs = self._get_message_history(events)
        valid_pairs = all_pairs.join(
            comm_pairs,
            (F.col('user_left') == F.col('user1')) &
            (F.col('user_right') == F.col('user2')),
            'left_anti'
        )
        
        # 6. Фильтр по расстоянию
        pairs_with_dist = calculate_distance(
            valid_pairs,
            'lat_left', 'lon_left',
            'lat_right', 'lon_right'
        )
        
        close_pairs = pairs_with_dist.filter(
            F.col('distance_km') <= self.config.max_distance_km
        ).drop('lat_right', 'lon_right', 'distance_km')
        
        logger.info(f"Pairs within {self.config.max_distance_km}km: {close_pairs.count()}")
        
        if close_pairs.isEmpty():
            logger.warning("No pairs found within distance limit")
            # Возвращаем пустой датафрейм с нужной схемой
            return self.spark.createDataFrame([], 
                "user_left int, user_right int, lat_left double, lon_left double")
        
        # 7. Определяем город(zone_id)
        
        cities = load_and_prepare_cities(
            self.spark,
            self.config.geo_path)
        
        pairs_with_zone = find_nearest_city( 
         close_pairs,
         cities,
         F.struct('user_left','user_right'),
         lat_col = 'lat_left',
         lon_col = 'lon_left'
         ).select(
             'user_left','user_right','zone_id','city_name'
         )
        
        # 8. Добавляем временные метки
        result = pairs_with_zone.withColumn('processed_dttm', F.current_timestamp())
        result = result.withColumn(
            'timezone',
            get_timezone_by_city('city_name')
        ).withColumn(
            'local_time',
            F.from_utc_timestamp(F.col('processed_dttm'), F.col('timezone'))
        ).drop('timezone', 'city_name')
        
        return result
    
    def load(self, df: DataFrame) -> None:
        """Сохранение витрины"""
        df.write \
            .mode(self.config.write_mode) \
            .format(self.config.output_format) \
            .save(self.config.output_path)
        
        logger.info(f"Saved {df.count()} recommendations to {self.config.output_path}")

if __name__ == "__main__":
    import sys
    from .config import Mart3Config
    
    
    # Парсинг аргументов
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    channel_id = sys.argv[4]
    max_distance_km = float(sys.argv[5])
    broadcast_cities = sys.argv[6].lower() == 'true'
    
    # дополнительные аргументы для конкретной витрины
    
    spark = spark_init('Mart_3','local')
    
    try:
        config = Mart3Config(
            events_path=events_path,
            geo_path=geo_path,
            output_path=output_path,
            channel_id = channel_id,
            max_distance_km = max_distance_km,
            broadcast_cities = broadcast_cities
        )

        mart = RecommendationMart(spark, config)
        mart.run()
    
    finally:
        spark.stop()        