import json
import seamless_config
import seamless_config.tools

try:
    import seamless_remote.buffer_remote
    import seamless_remote.database_remote

    seamless_remote.buffer_remote.DISABLED = True  # to prevent automatic launching
    seamless_remote.database_remote.DISABLED = True  # to prevent automatic launching
except ImportError:
    pass

seamless_config.init()

config = seamless_config.tools.configure_hashserver(mode="ro")
print(json.dumps(config, indent=2))
