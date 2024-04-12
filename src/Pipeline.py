from typing import Any, List, Set
from pydantic import BaseModel
import dendro.client as dc
import random


class PipelineImportedFile(BaseModel):
    fname: str
    url: str
    metadata: dict


class PipelineJobInput(BaseModel):
    name: str
    fname: str


class PipelineJobOutput(BaseModel):
    name: str
    fname: str
    metadata: dict


class PipelineJobParameter(BaseModel):
    name: str
    value: Any


class PipelineJobRequiredResources(BaseModel):
    num_cpus: int
    num_gpus: int
    memory_gb: int
    time_sec: int


class PipelineJob(BaseModel):
    processor_name: str
    inputs: List[PipelineJobInput]
    outputs: List[PipelineJobOutput]
    parameters: List[PipelineJobParameter]
    required_resources: PipelineJobRequiredResources
    run_method: str


class Pipeline:
    def __init__(self, *, project_id: str):
        self.project_id = project_id
        self.jobs: List[PipelineJob] = []
        self.imported_files: List[PipelineImportedFile] = []
        self.validator = PipelineValidator()

    def add_imported_file(self, imported_file: PipelineImportedFile):
        self.validator.add_imported_file(imported_file)
        self.imported_files.append(imported_file)

    def add_job(self, job: PipelineJob):
        self.validator.add_job(job)
        self.jobs.append(job)

    def submit(self):
        project = dc.load_project(self.project_id)
        for file in self.imported_files:
            dc.set_file(
                project=project,
                file_name=file.fname,
                url=file.url,
                metadata=file.metadata
            )

        batch_id = _random_batch_id()
        for job in self.jobs:
            input_files = [
                dc.SubmitJobInputFile(
                    name=ff.name,
                    file_name=ff.fname,
                    is_folder=False
                )
                for ff in job.inputs
            ]
            output_files = [
                dc.SubmitJobOutputFile(
                    name=ff.name,
                    file_name=ff.fname,
                    is_folder=False,
                    skip_cloud_upload=False
                )
                for ff in job.outputs
            ]
            parameters = [
                dc.SubmitJobParameter(
                    name=pp.name,
                    value=pp.value
                )
                for pp in job.parameters
            ]
            required_resources = dc.DendroJobRequiredResources(
                numCpus=job.required_resources.num_cpus,
                numGpus=job.required_resources.num_gpus,
                memoryGb=job.required_resources.memory_gb,
                timeSec=job.required_resources.time_sec
            )
            dc.submit_job(
                project=project,
                processor_name=job.processor_name,
                input_files=input_files,
                output_files=output_files,
                parameters=parameters,
                batch_id=batch_id,
                rerun_policy='never',
                required_resources=required_resources,
                run_method=job.run_method  # type: ignore
            )
            for ff in job.outputs:
                dc.set_file_metadata(
                    project=project,
                    file_name=ff.fname,
                    metadata=ff.metadata
                )


class PipelineValidator:
    def __init__(self):
        self.files: Set[str] = set()

    def add_imported_file(self, imported_file: PipelineImportedFile):
        if imported_file.fname in self.files:
            raise ValueError(f'Cannot import {imported_file.fname}. File already exists in pipeline.')
        self.files.add(imported_file.fname)

    def add_job(self, job: PipelineJob):
        for input in job.inputs:
            if input.fname not in self.files:
                raise ValueError(f'Cannot add job {job.processor_name}. Input file {input.fname} does not exist in pipeline.')
        for output in job.outputs:
            if output.fname in self.files:
                raise ValueError(f'Cannot add job {job.processor_name}. Output file {output.fname} already exists in pipeline.')
            self.files.add(output.fname)


def _random_batch_id(num_chars: int = 12) -> str:
    choices = 'abcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choices(choices, k=num_chars))
