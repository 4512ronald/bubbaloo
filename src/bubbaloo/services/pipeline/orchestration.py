import os
import inspect
import importlib
import re

from bubbaloo.pipeline.stages.extract import Extract
from bubbaloo.pipeline.stages.load import Load
from bubbaloo.pipeline.stages.transform import Transform
from bubbaloo.pipeline.pipeline import Pipeline
from bubbaloo.services.pipeline import Config


class PipelineOrchestrator:

    def __init__(self, path_to_flows: str | object, conf: Config, flows_dir_name: str | None = None):
        # TODO Manejar los parámetros con kwargs
        if inspect.ismodule(path_to_flows):
            self.path_to_flows: str = path_to_flows.__path__[0]
            self.flows_dir_name: str = path_to_flows.__name__
        else:
            self.path_to_flows = path_to_flows
            self.flows_dir_name = flows_dir_name

        self.conf = conf
        self.pipeline_list = []
        self.pipeline_stage_types = (Transform, Load, Extract)

    @staticmethod
    def get_module_names_from_directory(directory_path):
        module_names = []
        with os.scandir(directory_path) as entries:
            for entry in entries:
                match = re.match(r"(\w+)\.py", entry.name)
                if not match:
                    continue
                module_name = match.group(1)
                module_names.append(module_name)
        return module_names

    def get_pipeline_phases_from_directory(self, flow_name):
        pipeline_phases = []

        for stage_type in self.get_module_names_from_directory(os.path.join(self.path_to_flows, flow_name)):
            module_path = f"{self.flows_dir_name}.{flow_name}.{stage_type}"

            try:
                module = importlib.import_module(module_path)

                phase = self.get_pipeline_phase(module)

                pipeline_phases.extend(phase)

            except ModuleNotFoundError as e:
                # TODO log error en vez de print
                print(f"Cannot import the module {module_path}: {e}")
        return pipeline_phases

    def get_pipeline_phase(self, module):
        pipeline_phase = []
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and issubclass(obj, self.pipeline_stage_types) and obj not in self.pipeline_stage_types:
                pipeline_phase.append((name, obj()))
        return pipeline_phase

    def orchestrate_pipelines(self):
        for flow_name in os.listdir(self.path_to_flows):
            pipeline_phases = self.get_pipeline_phases_from_directory(flow_name)
            pipeline = Pipeline(conf=self.conf)
            pipeline.stages(pipeline_phases)
            self.pipeline_list.append((flow_name, pipeline))

    # TODO Ejecutar pipelines
