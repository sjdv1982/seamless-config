import json
from pathlib import Path
import yaml  # type: ignore
import seamless_config.cluster
import seamless_config.tools

seamless_config.set_workdir()

with Path("../clusters.yaml").open("r", encoding="utf-8") as handle:
    clusters = yaml.safe_load(handle)

seamless_config.cluster.define_clusters(clusters)

seamless_config.load_tools()

seamless_config.select_cluster("local")

seamless_config.select_project("myproject")

config = seamless_config.tools.configure_database(mode="rw")
print(json.dumps(config, indent=2))
