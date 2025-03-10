import logging

from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from abc import ABC, abstractmethod
import notebookutils


class SparkJob(ABC):
    """
    Abstract base class for Spark jobs.

    This class provides a template for creating Spark jobs with a standardized setup for
    Spark sessions and logging.

    Attributes
    ----------
    spark : pyspark.sql.SparkSession
        The Spark session for the job.
    logger : org.apache.log4j.Logger
        The logger for the job.
    job_name : str
        The name of the job.

    Methods
    -------
    setup_spark()
        Sets up the Spark session.
    setup_logger()
        Sets up the logger.
    main()
        Abstract method to be implemented by subclasses with the main job logic.
    """

    @property
    def job_name(self) -> str:
        """
        This property returns the name of the job, which defaults to the job class name.

        Returns
        -------
        str
            The name of the job.
        """
        return self.__class__.__name__

    @property
    def spark(self) -> SparkSession:
        """
        This property returns the Spark session for the job. If the Spark session has not
        been initialized, it calls setup_spark() to initialize it.

        Returns
        -------
        pyspark.sql.SparkSession
            The Spark session for the job.
        """
        if not hasattr(self, "_spark"):
            self.setup_spark()
        return self._spark

    @property
    def logger(self) -> logging.Logger:
        """
        This property returns the logger for the job. If the logger has not been initialized,
        it calls setup_logger() to initialize it.

        Returns
        -------
        logging.Logger
            The logger for the job.
        """
        if not hasattr(self, "_logger"):
            self.setup_logger()
        return self._logger

    def setup_spark(self):
        """
        This method initializes the Spark session for the job.
        """
        self._spark = SparkSession.builder.appName(self.job_name).getOrCreate()

    def setup_logger(self):
        """
        This method initializes the logger for the job to write to std:out.
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler()]
        )
        self._logger = logging.getLogger(self.job_name)
        self._logger.info(f"Started sparksession and logging for job '{self.job_name}'.")

    @abstractmethod
    def main(self):
        """
        This abstract method should be implemented by subclasses to contain the main logic
        of the Spark job.
        """
        pass


class LakehouseError(Exception):
    """Custom exception for lakehouse-related errors."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class LakehouseUtils:
    """
    Utility class for managing lakehouses in Microsoft Fabric.

    Methods
    -------
    get_current_workspace_lakehouse_names()
        Retrieves the names of lakehouses in the current workspace.
    get_lakehouse_path(lakehouse_name, workspace_id=None)
        Retrieves the path of a lakehouse by name and optionally workspace ID.
    get_bronze_lakehouse_path(workspace_id=None)
        Retrieves the path of the bronze lakehouse in the current or specified workspace.
    get_silver_lakehouse_path(workspace_id=None)
        Retrieves the path of the silver lakehouse in the current or specified workspace.
    get_gold_lakehouse_path(workspace_id=None)
        Retrieves the path of the gold lakehouse in the current or specified workspace.
    """

    @staticmethod
    def get_current_workspace_lakehouse_names() -> list[str]:
        """
        Retrieves the names of lakehouses in the current workspace.

        Returns
        -------
        list[str]
            A list of lakehouse names in the current workspace.
        """
        return [item["displayName"] for item in notebookutils.lakehouse.list()]

    @staticmethod
    def get_lakehouse_path(lakehouse_name: str, workspace_id: str = None) -> str:
        """
        Retrieves the path of a lakehouse by name and optionally workspace ID.

        Parameters
        ----------
        lakehouse_name : str
            The name of the lakehouse.
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        str
            The path of the lakehouse.

        Raises
        ------
        LakehouseError
            If the lakehouse cannot be retrieved.
        """
        try:
            if workspace_id:
                lakehouse_item = notebookutils.lakehouse.get(lakehouse_name, workspace_id)
            else:
                lakehouse_item = notebookutils.lakehouse.get(lakehouse_name)
        except Py4JJavaError as e:
            raise LakehouseError(f"Failed to retrieve lakehouse '{lakehouse_name}': {e}")

        return lakehouse_item["properties"]["abfsPath"]

    @staticmethod
    def get_bronze_lakehouse_path(workspace_id: str = None) -> str:
        """
        Retrieves the path of the bronze lakehouse in the current or specified workspace.

        Parameters
        ----------
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        str
            The path of the bronze lakehouse.

        Raises
        ------
        LakehouseError
            If the bronze lakehouse cannot be found.
        """
        lakehouse_name = next((name for name in LakehouseUtils.get_current_workspace_lakehouse_names() if "bronze" in name), None)
        if lakehouse_name is None:
            raise LakehouseError("Bronze lakehouse not found in the current workspace.")
        return LakehouseUtils.get_lakehouse_path(lakehouse_name, workspace_id)

    @staticmethod
    def get_silver_lakehouse_path(workspace_id: str = None) -> str:
        """
        Retrieves the path of the silver lakehouse in the current or specified workspace.

        Parameters
        ----------
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        str
            The path of the silver lakehouse.

        Raises
        ------
        LakehouseError
            If the silver lakehouse cannot be found.
        """
        lakehouse_name = next((name for name in LakehouseUtils.get_current_workspace_lakehouse_names() if "silver" in name), None)
        if lakehouse_name is None:
            raise LakehouseError("Silver lakehouse not found in the current workspace.")
        return LakehouseUtils.get_lakehouse_path(lakehouse_name, workspace_id)

    @staticmethod
    def get_gold_lakehouse_path(workspace_id: str = None) -> str:
        """
        Retrieves the path of the gold lakehouse in the current or specified workspace.

        Parameters
        ----------
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        str
            The path of the gold lakehouse.

        Raises
        ------
        LakehouseError
            If the gold lakehouse cannot be found.
        """
        lakehouse_name = next((name for name in LakehouseUtils.get_current_workspace_lakehouse_names() if "gold" in name), None)
        if lakehouse_name is None:
            raise LakehouseError("Gold lakehouse not found in the current workspace.")
        return LakehouseUtils.get_lakehouse_path(lakehouse_name, workspace_id)
