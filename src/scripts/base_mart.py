from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

class BaseMart(ABC):
    """Базовый класс для всех витрин данных"""
    def __init__(self,spark, config):
        self.spark = spark
        self.config = config
        logger.info(f"Initialized  {self.__class__.__name__}")

    @abstractmethod
    def extract(self) -> DataFrame:
        """Загрузка исходных данных"""
        pass
    
    @abstractmethod
    def transform(self,df:DataFrame) -> DataFrame:
        """Траснформация данных"""
        pass

    @abstractmethod
    def load(self,df:DataFrame) -> None:
        """Сохранение результата"""
        pass

    def run(self) -> None:
        """Основной метод выполнения ETL"""
        logger.info(f"Starting {self.__class__.__name__}")

        #Extract
        data = self.extract()
        logger.info(f"Extracted {data.count() if hasattr(data,'count') else 'streaming'} rows")

        #Transform
        result = self.transform(data)
        logger.info("Transformation completed")

        #Load
        self.load(result)
        logger.info(f"Loaded to {self.config.output_path}")
