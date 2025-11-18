import sys
import json
import remote_http_launcher

params = json.loads(open(sys.argv[1]).read())
result = remote_http_launcher.run(params)
print()
print(json.dumps(result, indent=2))
