import json
from pathlib import Path
import yaml  # type: ignore
import seamless_config.cluster
import seamless_config.tools

with Path("../clusters.yaml").open("r", encoding="utf-8") as handle:
    clusters = yaml.safe_load(handle)

seamless_config.cluster.define_clusters(clusters)

with Path("../tools.yaml").open("r", encoding="utf-8") as handle:
    tools = yaml.safe_load(handle)

seamless_config.tools.define_tools(tools)

seamless_config.select_cluster("newton")

seamless_config.select_project("myproject")

config = seamless_config.tools.configure_hashserver(mode="ro")
print(json.dumps(config, indent=2))
