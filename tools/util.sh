#!/bin/sh


dart_conf() {
    [[ -z "${DART_CONFIG}" ]] && echo "environment variable needs to be set: DART_CONFIG" && return 1
    PYTHONPATH=src/python:${PYTHONPATH} python -c "from dart.config.config import print_config; print_config(\"${DART_CONFIG}\")"
}


dart_conf_value() {
    local CONFIG=$1
    local VAR_PATH=$2
    [[ -z "${CONFIG}" ]] && echo "missing CONFIG parameter" && return 1
    [[ -z "${VAR_PATH}" ]] && echo "missing VAR_PATH parameter" && return 1
    PYTHONPATH=src/python:${PYTHONPATH} python -c "from dart.config.config import print_config_value_from_stdin; print_config_value_from_stdin(\"${VAR_PATH}\")" <<< "${CONFIG}"
}
