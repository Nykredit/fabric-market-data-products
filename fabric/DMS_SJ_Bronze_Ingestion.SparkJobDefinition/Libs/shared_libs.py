from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
import logging


class SparkJob(ABC):
    """
    Abstract base class for Spark jobs.

    This class provides a template for creating Spark jobs with a standardized setup_spark
    for Spark sessions and logging.

    Attributes
    ----------
    spark : pyspark.sql.SparkSession
        The Spark session for the job.
    logger : org.apache.log4j.Logger
        The logger for the job.

    Methods
    -------
    setup_spark(job_name: str)
        Sets up the Spark session and logger.
    main()
        Abstract method to be implemented by subclasses with the main job logic.
    """

    def setup_spark(self, job_name: str = None):
        """
        Sets up the Spark session and logger. 'job_name' is used to display in logging.

        Parameters
        ----------
        job_name : str
            The name of the Spark job.
        """
        if job_name is None:
            job_name = self.__class__.__name__
        self.job_name = job_name

        # Spark session builders
        self.spark = (SparkSession.builder.appName(job_name).getOrCreate())
        spark_context = self.spark.sparkContext

        log4jLogger = spark_context._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(job_name)
    
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(job_name)


        self.logger.info(f"Started sparksession and logging for job '{job_name}'.")

    @abstractmethod
    def main(self):
        """
        Main method to be implemented by subclasses with the job logic.

        This method should contain the main logic of the Spark job.
        """
        pass
