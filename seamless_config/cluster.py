from dataclasses import dataclass
from typing import Optional, Any


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
class ClusterFrontend:
    hostname: str
    ssh_hostname: Optional[str] = None
    hashserver: Optional[ClusterFrontendHashserver] = None
    database: Optional[ClusterFrontendDatabase] = None

    @classmethod
    def from_dict(cls, dic: dict[str, Any]):
        params = dic.copy()
        if "hashserver" in dic:
            params["hashserver"] = ClusterFrontendHashserver(**dic["hashserver"])
        if "database" in dic:
            params["database"] = ClusterFrontendDatabase(**dic["database"])
        return cls(**params)


@dataclass
class Cluster:
    tunnel: bool
    frontends: list[ClusterFrontend]

    @classmethod
    def from_dict(cls, dic: dict[str, Any]):
        params = dic.copy()
        frontends = []
        for frontend_dict in dic["frontends"]:
            frontend = ClusterFrontend.from_dict(frontend_dict)
            frontends.append(frontend)
        params["frontends"] = frontends
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
        cluster = Cluster.from_dict(value)
        _clusters[key] = cluster
    if local_cluster is not None:
        assert local_cluster in _clusters, (local_cluster, clusters.keys())
        _local_cluster = local_cluster


def get_cluster(cluster):
    return _clusters[cluster]


def get_local_cluster():
    return _local_cluster
