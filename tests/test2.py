import json
import seamless_config
import seamless_config.tools

seamless_config.select_stage("test2")
seamless_config.set_workdir()
seamless_config.config_files.load_config_files()

config = seamless_config.tools.configure_hashserver(mode="rw")
print(json.dumps(config, indent=2))
