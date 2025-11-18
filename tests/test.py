import json
import seamless_config
import seamless_config.tools

seamless_config.init()

config = seamless_config.tools.configure_hashserver(mode="ro")
print(json.dumps(config, indent=2))
