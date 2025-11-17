# Seamless Command Language

`seamless_config` discovers configuration through two optional files in the
work directory and its parents:

| File | Recommended policy | Purpose |
| --- | --- | --- |
| `seamless.yaml` | Commit to version control | Project-wide, deterministic defaults |
| `.seamless.yaml` | Add to `.gitignore` | Local, developer-specific overrides (e.g. cluster
hostnames or experimental settings) |

Both files use the same command language described below. `load_config_files()`
reads the file pair in the current workdir. If either file contains
`inherit_from_parent`, the loader also inspects the parent directory and
prepends its commands so they run before the child’s entries. This repeats
until a directory without `inherit_from_parent` is reached (or the filesystem
root), which means parent defaults are always applied before the local
overrides.

## Syntax

Each document must be a YAML list. Every list item is either a bare command
name (`inherit_from_parent`) or a single-key mapping (`cluster: newton`). If the
file parses as valid YAML but not a list, the loader raises an error and shows
an example (`- project: myproject`).

### Available commands

| Command | Arguments | Description |
| --- | --- | --- |
| `cluster` | string | Calls `seamless_config.select_cluster(value)` |
| `project` | string | Calls `seamless_config.select_project(value)` |
| `subproject` | string | Calls `seamless_config.select_subproject(value)` |
| `inherit_from_parent` | – | Also read commands from the parent directory and prepend them |
| `clusters` | mapping | Updates the local `_clusters` dict and runs before other commands |
| `stage <name>` | list of commands | Executes the nested list only when the current stage equals `<name>` |

Internally, commands are split into two passes: those with priority (currently
only `clusters`) and the rest. Between the passes the loader calls
`seamless_config.cluster.define_clusters(_clusters)` so the later commands use
the freshest cluster data.

### Stage blocks

Stage-specific configuration uses YAML keys that begin with `stage` and a
literal stage name. For example:

```yaml
- stage build:
  - project: build-pipeline
  - subproject: linux-x86
```

When `seamless_config.select_stage("build")` has been called, the commands
inside the block are executed as if they appeared in the outer list; otherwise
they are skipped.

### Example

```yaml
# seamless.yaml (checked in)
- project: my-shared-project
- inherit_from_parent

# .seamless.yaml (ignored)
- clusters:
    local:
      tunnel: false
      frontends: [...]
    local_cluster: local
- cluster: local
- stage prod:
  - project: production-project
```

The `.seamless.yaml` snippet defines local clusters, selects the `local` entry
as both the active cluster and `local_cluster`, and only switches to the
`production-project` when the current stage equals `prod`.
