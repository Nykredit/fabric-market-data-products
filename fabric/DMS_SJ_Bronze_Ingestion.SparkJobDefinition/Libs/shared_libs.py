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
    get_workspace_lakehouse_names(workspace_id=None)
        Retrieves the names of lakehouses in a workspace.
    get_lakehouse_path(lakehouse_name, workspace_id=None)
        Retrieves the path of a lakehouse by name and optionally workspace ID.
    get_lakehouse_path_by_keyword(lakehouse_keyword, workspace_id=None)
        Retrieves the path of a lakehouse specified by a keyword in the current or specified workspace.
    """

    @staticmethod
    def get_workspace_lakehouse_names(workspace_id: str = None) -> list[str]:
        """
        Retrieves the names of lakehouses optionally by workspace ID. If no workspace ID
        is provided the default lakehouse is used.

        Parameters
        ----------
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        list[str]
            A list of lakehouse names in the provided workspace.
        """
        if workspace_id is None:
            lakehouses = notebookutils.lakehouse.list()
        else:
            lakehouses = notebookutils.lakehouse.list(workspace_id)

        return [item["displayName"] for item in lakehouses]

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
    def get_lakehouse_path_by_keyword(lakehouse_keyword: str, workspace_id: str = None):
        """
        Retrieves the path of the lakehouse given by a keyword in the default or specified workspace.
        The keyword is just a substring check, where the first matching lakehouse name is returned.

        Parameters
        ----------
        workspace_id : str, optional
            The ID of the workspace.

        Returns
        -------
        str
            The path of the lakehouse.

        Raises
        ------
        LakehouseError
            If no lakehouse can be found by that name.
        """
        lakehouse_name = next((name for name in LakehouseUtils.get_workspace_lakehouse_names(workspace_id) if lakehouse_keyword.lower() in name.lower()), None)
        if lakehouse_name is None:
            raise LakehouseError(f"{lakehouse_keyword} lakehouse not found in the current workspace.")
        return LakehouseUtils.get_lakehouse_path(lakehouse_name, workspace_id)
