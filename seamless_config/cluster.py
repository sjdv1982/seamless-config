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
class ClusterFrontend:
    hostname: str
    hashserver: Optional[ClusterFrontendHashserver] = None

    @classmethod
    def from_dict(cls, dic: dict[str, Any]):
        params = dic.copy()
        if "hashserver" in dic:
            params["hashserver"] = ClusterFrontendHashserver(**dic["hashserver"])
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


def define_clusters(clusters):
    assert isinstance(clusters, dict)
    _clusters.clear()
    for key, value in clusters.items():
        assert isinstance(key, str)
        assert isinstance(value, dict)
        cluster = Cluster.from_dict(value)
        _clusters[key] = cluster


def get_cluster(cluster):
    return _clusters[cluster]
