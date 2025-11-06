from seamless_config.tools import DOLLAR_RE

tmpl = 'hashserver-$CLUSTER-$MODE-{workdir.strip("/").replace("/", "--")}'
result = tmpl
vars = {
    "MODE": "rw",
    "CLUSTER": "local",
}
for m in reversed(list(DOLLAR_RE.finditer(tmpl))):
    val = vars[m.group()[1:]]
    start, end = m.span()
    result = result[:start] + str(val) + result[end:]

print(result)
