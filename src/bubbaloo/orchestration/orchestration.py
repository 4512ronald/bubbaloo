import os
import inspect
import importlib
import sys
import zipfile
import re
from copy import deepcopy
from pathlib import Path
from typing import Dict, Any, List, Tuple, Type

from bubbaloo.pipeline.stages.extract import Extract
from bubbaloo.pipeline.stages.load import Load
from bubbaloo.pipeline.stages.transform import Transform
from bubbaloo.pipeline.pipeline import Pipeline
from bubbaloo.services.pipeline import Config, PipelineState
from bubbaloo.utils.functions.pipeline_orchestration_helper import get_error
from bubbaloo.utils.functions.pipeline_stage_helper import validate_params
from bubbaloo.utils.interfaces.pipeline_logger import ILogger

StageType = Transform | Load | Extract


class Orchestrator:
    """
    Orchestrator class for managing and executing a series of data pipelines.

    This class is responsible for setting up and executing multiple data pipelines based on the provided configuration.
    It handles the orchestration of various pipeline stages, including transforming, loading, and extracting data.

    Attributes:
        _path_to_flows (str): The file path or module path to the flow definitions.
        _flows_to_execute (List[str]): A list of flow names to execute.
        _flows_dir_name (str): The directory name where flows are stored.
        _params (Dict[str, Any]): Dictionary of parameters needed for pipeline execution.
        _conf (Config): Configuration object for the orchestrator.
        _logger (ILogger): Logger instance for logging messages.
        _context (PipelineState): State object for managing pipeline state.
        _pipeline_list (List[Tuple[str, Pipeline]]): List of pipelines to be executed.
        _pipeline_stage_types (Tuple[Type[Transform], Type[Load], Type[Extract]]): Tuple of pipeline stage types.
        _resume (List[Dict[str, str]] | str): Summary of pipeline execution results.
    """

    def __init__(self, path_to_flows: str | object, flows_to_execute: List[str] | None = None, **kwargs):
        """
        Initializes the Orchestrator with the path to pipeline flows, the list of flows to execute, and additional
        parameters.

        Args:
            path_to_flows (str | object): The file path or module path where flow definitions are located.
            flows_to_execute (List[str] | None): Optional list of flow names to execute. Executes all flows if None.
            **kwargs: Additional keyword arguments for pipeline configuration.
        """

        self._path_to_flows: str = self._get_working_path(path_to_flows)
        self._flows_to_execute: List[str] = flows_to_execute if flows_to_execute is not None else []
        self._flows_dir_name: str = self._path_to_flows.split("/")[-1]
        self._params: Dict[str, Any] = validate_params(kwargs)
        self._conf: Config = self._params.get("conf")
        self._logger: ILogger = self._params.get("logger")
        self._context: PipelineState = self._params.get("context")
        self._pipeline_list: List[Tuple[str, Pipeline]] = []
        self._pipeline_stage_types: Tuple[Type[Transform], Type[Load], Type[Extract]] = (Transform, Load, Extract)
        self._resume: List[Dict[str, str]] | str = []
        self._get_flows_to_execute()

    @property
    def resume(self) -> List[Dict[str, str]] | str:
        """
        Property to get the summary of pipeline execution results.

        Returns:
            List[Dict[str, str]] | str: Summary of executed pipelines or a message if no pipelines have been executed
            yet.
        """
        if not self._resume:
            return "the flows are not executed yet"
        return self._resume

    @property
    def flows_to_execute(self) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Property to get the list of flows to be executed along with their named stages.

        Returns:
            List[Tuple[str, Dict[str, Any]]]: List of tuples containing flow names and their corresponding named stages.
        """
        return [(name, flow.named_stages) for name, flow in self._pipeline_list]

    def _get_working_path(self, path_to_flows: str) -> str:
        """
        Determines the working path for pipeline flows.

        Args:
            path_to_flows (str): The file path or module path to the flow definitions.

        Returns:
            str: The determined working path for the flows.
        """
        tmp_path = path_to_flows.__path__[0] if inspect.ismodule(path_to_flows) else path_to_flows
        path_parts = tmp_path.split("/")
        for i in range(len(path_parts)):
            partial_path = "/".join(path_parts[:i + 1])
            if zipfile.is_zipfile(partial_path):
                result = self._handle_zip_file(partial_path, "/".join(path_parts[i + 1:]))
                break
        else:
            result = tmp_path
        return result

    @staticmethod
    def _handle_zip_file(zip_path: str, sub_path: str):
        """
        Handles the extraction and setup of pipeline flows contained within a zip file.

        Args:
            zip_path (str): Path to the zip file containing the flows.
            sub_path (str): Sub-path within the zip file to extract.

        Returns:
            str: Path to the extracted flow definitions.
        """
        working_directory = os.getcwd()
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(working_directory)
        base_dir = os.path.join(working_directory, sub_path.split('/')[0])
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        return os.path.join(working_directory, sub_path)

    @staticmethod
    def _get_module_names_from_package(directory_path: str | Path) -> List[str]:
        """
        Retrieves the names of Python modules from a given directory path.

        Args:
            directory_path (str | Path): The directory path containing Python modules.

        Returns:
            List[str]: A list of module names found in the directory.
        """
        module_names: List[str] = []
        with os.scandir(directory_path) as entries:
            for entry in entries:
                match = re.match(r"(\w+)\.py", entry.name)
                if not match:
                    continue
                module_name = match.group(1)
                module_names.append(module_name)
        return module_names

    def _get_pipeline_phases_from_module(self, module: object) -> List[Tuple[str, StageType]]:
        """
        Extracts pipeline phases from a given module.

        Args:
            module (object): The module from which to extract pipeline phases.

        Returns:
            List[Tuple[str, StageType]]: List of tuples containing phase names and their corresponding stage objects.
        """
        pipeline_phase: List[Tuple[str, StageType]] = []
        for name, obj in inspect.getmembers(module):
            if (
                    inspect.isclass(obj)
                    and issubclass(obj, self._pipeline_stage_types)
                    and obj not in self._pipeline_stage_types
            ):
                pipeline_phase.append((name, obj()))
        return pipeline_phase

    def _get_pipeline_phases_from_package(self, flow_name: str) -> List[Tuple[str, StageType]]:
        """
        Retrieves pipeline phases from a package representing a flow.

        Args:
            flow_name (str): Name of the flow whose phases are to be retrieved.

        Returns:
            List[Tuple[str, StageType]]: List of tuples containing phase names and their corresponding stage objects.
        """
        pipeline_phases: List[Tuple[str, StageType]] = []

        for stage_type in self._get_module_names_from_package(os.path.join(self._path_to_flows, flow_name)):
            module_path = f"{self._flows_dir_name}.{flow_name}.{stage_type}"

            try:
                module = importlib.import_module(module_path)
                phase = self._get_pipeline_phases_from_module(module)
                pipeline_phases.extend(phase)
            except ModuleNotFoundError as e:
                self._logger.error(f"Cannot import the module {module_path}: {e}")
        return pipeline_phases

    def _get_flows_to_execute(self):
        """
        Identifies and sets up the flows to be executed by the Orchestrator.
        """
        for flow_name in os.listdir(self._path_to_flows):
            if not self._flows_to_execute or flow_name in self._flows_to_execute:
                pipeline_phases = self._get_pipeline_phases_from_package(flow_name)
                pipeline = Pipeline(**self._params)
                pipeline.stages(pipeline_phases)
                self._pipeline_list.append((flow_name, pipeline))

    def execute(self):
        """
        Executes the configured pipelines.

        This method runs each pipeline in the list of flows to execute, handling errors and logging as necessary.
        """
        for flow_name, pipeline in self._pipeline_list:
            self._context.entity = flow_name
            self._logger.info(f"Executing pipeline for flow: {flow_name}")
            try:
                pipeline.execute()
            except Exception as e:
                self._context.errors["PipelineExecution"] = str(get_error(e))
                self._logger.error(f"Error executing pipeline for flow {flow_name}: {e}")
            self._context.reset()
            self._logger.info(f"The ETL: {flow_name} has finished")

        self._resume = deepcopy(self._context.resume())
        self._logger.info(f"Finished executing pipelines: {self._resume}")
