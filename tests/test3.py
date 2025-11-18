import json
import seamless_config
import seamless_config.tools

seamless_config.set_stage("test3")

config = seamless_config.tools.configure_database(mode="rw")
print(json.dumps(config, indent=2))
