import sys

print(
    """
TODO: Dask client (based on default client?) that sets database and hashserver config.
""",
    file=sys.stderr,
)

import dataclasses
from dataclasses import dataclass
from typing import Literal, Optional, Any


@dataclass
class ClusterFrontendHashserver:
    bufferdir: str
    conda: str
    network_interface: str
    port_start: int
    port_end: int


@dataclass
class ClusterFrontendDatabase:
    database_dir: str
    conda: str
    network_interface: str
    port_start: int
    port_end: int


@dataclass
class ClusterFrontendJobserver:
    conda: str
    network_interface: str
    port_start: int
    port_end: int


@dataclass
class ClusterFrontendDaskserver:
    network_interface: str
    port_start: int
    port_end: int


@dataclass
class ClusterFrontend:
    hostname: str
    ssh_hostname: Optional[str] = None
    hashserver: Optional[ClusterFrontendHashserver] = None
    database: Optional[ClusterFrontendDatabase] = None
    jobserver: Optional[ClusterFrontendJobserver] = None
    daskserver: Optional[ClusterFrontendDaskserver] = None

    @classmethod
    def from_dict(cls, dic: dict[str, Any]):
        params = dic.copy()
        if "hashserver" in dic:
            params["hashserver"] = ClusterFrontendHashserver(**dic["hashserver"])
        if "database" in dic:
            params["database"] = ClusterFrontendDatabase(**dic["database"])
        if "jobserver" in dic:
            params["jobserver"] = ClusterFrontendJobserver(**dic["jobserver"])
        if "daskserver" in dic:
            params["daskserver"] = ClusterFrontendDaskserver(**dic["daskserver"])

        return cls(**params)


@dataclass
class ClusterQueue:
    name: str
    conda: str
    walltime: str
    cores: int
    memory: str
    unknown_task_duration: str
    target_duration: str
    maximum_jobs: int
    lifetime_stagger: str = "4m"
    interactive: bool = False
    project: str | None = None
    partition: str | None = None
    tmpdir: str = "/tmp"
    lifetime: str | None = None
    extra_dask_config: dict | None = None
    job_extra_directives: list[str] | None = None
    job_script_prologue: list[str] | None = None
    dask_resources: dict[str, str] | None = None


@dataclass
class ClusterQueueWithTemplate:
    name: str
    TEMPLATE: str | None = None
    interactive: bool | None = None
    conda: str | None = None
    project: str | None = None
    partition: str | None = None
    walltime: str | None = None
    cores: int | None = None
    memory: str | None = None
    tmpdir: str | None = None
    unknown_task_duration: str | None = None
    target_duration: str | None = None
    lifetime_stagger: str | None = None
    lifetime: str | None = None
    extra_dask_config: dict | None = None
    maximum_jobs: int | None = None
    job_extra_directives: list[str] | None = None
    dask_resources: dict[str, str] | None = None


@dataclass
class Cluster:
    name: str
    tunnel: bool
    frontends: list[ClusterFrontend]
    type: Literal["local", "slurm", "oar"] | None = None
    workers: Optional[int] = None
    memory_per_core_property_name: str | None = None
    queues: dict[str, ClusterQueue] | None = None
    default_queue: str | None = None

    def __post_init__(self):
        assert self.type is None or self.type in ("local", "slurm", "oar")

    @classmethod
    def from_dict(cls, name, dic: dict[str, Any]):
        params = dic.copy()
        params["name"] = name
        frontends = []
        for frontend_dict in dic["frontends"]:
            frontend = ClusterFrontend.from_dict(frontend_dict)
            frontends.append(frontend)
        params["frontends"] = frontends
        queues0 = dic.get("queues", {})
        if queues0:
            queues = {}
            for queue_name, queue_dict0 in queues0.items():
                queue_dict = {k.replace("-", "_"): v for k, v in queue_dict0.items()}
                queue_dict["name"] = queue_name
                queue_with_template = ClusterQueueWithTemplate(**queue_dict)
                tmpl = queue_with_template.TEMPLATE
                queue_with_template_dict = dataclasses.asdict(queue_with_template)
                if tmpl is not None:
                    if tmpl not in queues:
                        raise TypeError(
                            f"Cannot use unknown queue '{tmpl}' as template"
                        )
                    template_queue = queues[tmpl]
                    template_dict = dataclasses.asdict(template_queue)
                    queue_dict = template_dict
                    queue_dict.update(queue_with_template_dict)
                    queue_dict.pop("TEMPLATE")
                else:
                    queue_dict = queue_with_template_dict
                    queue_dict.pop("TEMPLATE", None)
                queue = ClusterQueue(**queue_dict)
                queues[queue_name] = queue
            params["queues"] = queues
        return cls(**params)


_clusters: dict[str, Cluster] = {}
_local_cluster = None


def define_clusters(clusters):
    global _local_cluster
    assert isinstance(clusters, dict)
    _clusters.clear()
    local_cluster = None
    for key, value in clusters.items():
        assert isinstance(key, str)
        if key == "local_cluster":
            assert isinstance(value, str)
            local_cluster = value
            continue
        assert isinstance(value, dict)
        cluster = Cluster.from_dict(key, value)
        _clusters[key] = cluster
    if local_cluster is not None:
        assert local_cluster in _clusters, (local_cluster, clusters.keys())
        _local_cluster = local_cluster


def get_cluster(cluster):
    return _clusters[cluster]


def get_local_cluster():
    return _local_cluster
