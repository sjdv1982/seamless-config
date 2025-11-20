import json
import seamless_config
import seamless_config.tools

try:
    import seamless_remote.buffer_remote

    seamless_remote.buffer_remote.DISABLED = True  # to prevent automatic launching
except ImportError:
    pass

seamless_config.set_stage("test3")

config = seamless_config.tools.configure_database(mode="rw")
print(json.dumps(config, indent=2))
