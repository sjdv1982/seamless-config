import json
import seamless_config
import seamless_config.tools

seamless_config.set_stage("test2")

config = seamless_config.tools.configure_hashserver(mode="rw")
print(json.dumps(config, indent=2))
