import os
import inspect
import importlib
import re
from typing import Dict, Any, List, Tuple, Type

from bubbaloo.pipeline.stages.extract import Extract
from bubbaloo.pipeline.stages.load import Load
from bubbaloo.pipeline.stages.transform import Transform
from bubbaloo.pipeline.pipeline import Pipeline
from bubbaloo.services.pipeline import Config
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


class Orchestrator:

    def __init__(self, path_to_flows: str | object, flows_to_execute: List[str] | None = None, **kwargs):
        self._path_to_flows: str = path_to_flows.__path__[0] if inspect.ismodule(path_to_flows) else path_to_flows
        self._flows_to_execute: List[str] = flows_to_execute if flows_to_execute is not None else []
        self._flows_dir_name: str = self._path_to_flows.split("/")[-1]
        self._params: Dict[str, Any] = kwargs
        self._conf: Config = self._params.get("conf")
        self._logger: ILogger = self._params.get("logger")
        self._pipeline_list: List[Tuple[str, Pipeline]] = []
        self._pipeline_stage_types: Tuple[Type[Transform], Type[Load], Type[Extract]] = (Transform, Load, Extract)

    @staticmethod
    def _get_module_names_from_directory(directory_path: str) -> List[str]:
        module_names: List[str] = []
        with os.scandir(directory_path) as entries:
            for entry in entries:
                match = re.match(r"(\w+)\.py", entry.name)
                if not match:
                    continue
                module_name = match.group(1)
                module_names.append(module_name)
        return module_names

    def _get_pipeline_phases_from_directory(self, flow_name: str) -> list[tuple[str, Transform | Load | Extract]]:
        pipeline_phases: list[tuple[str, Transform | Load | Extract]] = []

        for stage_type in self._get_module_names_from_directory(os.path.join(self._path_to_flows, flow_name)):
            module_path = f"{self._flows_dir_name}.{flow_name}.{stage_type}"

            try:
                module = importlib.import_module(module_path)
                phase = self._get_pipeline_phase(module)
                pipeline_phases.extend(phase)
            except ModuleNotFoundError as e:
                self._logger.error(f"Cannot import the module {module_path}: {e}")
        return pipeline_phases

    def _get_pipeline_phase(self, module) -> List[Tuple[str, Transform | Load | Extract]]:
        pipeline_phase: list[tuple[str, Transform | Load | Extract]] = []
        for name, obj in inspect.getmembers(module):
            if (
                    inspect.isclass(obj)
                    and issubclass(obj, self._pipeline_stage_types)
                    and obj not in self._pipeline_stage_types
            ):
                pipeline_phase.append((name, obj()))
        return pipeline_phase

    def _get_flows_to_execute(self):
        for flow_name in os.listdir(self._path_to_flows):
            if not self._flows_to_execute or flow_name in self._flows_to_execute:
                pipeline_phases = self._get_pipeline_phases_from_directory(flow_name)
                pipeline = Pipeline(**self._params)
                pipeline.stages(pipeline_phases)
                self._pipeline_list.append((flow_name, pipeline))

    def execute(self):
        self._get_flows_to_execute()
        for flow_name, pipeline in self._pipeline_list:
            self._logger.info(f"Executing pipeline for flow: {flow_name}")
            try:
                pipeline.execute()
            except Exception as e:
                self._logger.error(f"Error executing pipeline for flow {flow_name}: {e}")
