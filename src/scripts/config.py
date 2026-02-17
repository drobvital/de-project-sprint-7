from dataclasses import dataclass

@dataclass
class BaseConfig:
    """Базовые параметры для всех витрин"""
    #Входные данные
    events_path: str = "/user/master/data/geo/events"
    geo_path: str = "/user/vitekdy/data/geo_1.parquet"
    #Выходные данные
    output_format: str = "parquet"
     #Режим записи
    write_mode: str = "overwrite"

@dataclass
class Mart3Config(BaseConfig):
    """Конфигурация для 3-й витрины"""
    output_path: str = "/user/vitekdy/data/marts/recommendations"
    channel_id: str = "404815"
    max_distance_km: float = 1.0
    broadcast_cities: bool = True
@dataclass
class Mart2Config(BaseConfig):
    """Конфигурация для 2-й витрины"""
    output_path: str = "/user/vitekdy/data/marts/zone_geo"    

@dataclass
class Mart1Config(BaseConfig):
    """Конфигурация для 1-й витрины"""
    output_path: str = "/user/vitekdy/data/marts/user_geo"
    min_days_for_home: int = 27
    date_format: str = "yyyy-MM-dd HH:mm:ss"
