#!/usr/bin/env bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

function hadoop_usage()
{
  echo "Usage: dtssrun.sh tracelocation"
}

function calculate_classpath
{
  hadoop_add_to_classpath_tools hadoop-dtss
  hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_DIR}/timelineservice/"'/*'
  hadoop_add_classpath "${HADOOP_YARN_HOME}/${YARN_DIR}/timelineservice/lib/*"
}

function run_simulation() {
  hadoop_add_client_opts
  hadoop_finalize

  # shellcheck disable=SC2086
  echo "Running simulation in dtss.sh!"
  if [[ -z $redirect_path ]]; then
    hadoop_java_exec dtss org.apache.hadoop.yarn.dtss.Runner "$@"
  else
    echo "Redirecting output to $redirect_path"
    mkdir -p "$(dirname "$redirect_path")"
    hadoop_java_exec dtss org.apache.hadoop.yarn.dtss.Runner "$@" > $redirect_path 2>&1
  fi
}

while getopts "i:m:" opt; do
  case $opt in
    i)
      id=${OPTARG}
      ;;
    m)
      metric_path=${OPTARG}
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

# let's locate libexec...
if [[ -n "${HADOOP_HOME}" ]]; then
  HADOOP_DEFAULT_LIBEXEC_DIR="${HADOOP_HOME}/libexec"
else
  this="${BASH_SOURCE-$0}"
  bin=$(cd -P -- "$(dirname -- "${this}")" >/dev/null && pwd -P)
  HADOOP_DEFAULT_LIBEXEC_DIR="${bin}/../../../../../libexec"
fi

HADOOP_LIBEXEC_DIR="${HADOOP_LIBEXEC_DIR:-$HADOOP_DEFAULT_LIBEXEC_DIR}"
# shellcheck disable=SC2034
HADOOP_NEW_CONFIG=true
if [[ -f "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]]; then
  # shellcheck disable=SC1090
  . "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh"
else
  echo "ERROR: Cannot execute ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh." 2>&1
  exit 1
fi

calculate_classpath

sim_args=( "${@:1:$OPTIND-1}" )
shift $((OPTIND-1))

if [[ $# = 0 ]] || [[ $# -gt 2 ]]; then
  hadoop_exit_with_usage 1
fi

sim_args+=( "$1" )
shift 1

if [[ $# = 0 ]]; then
  run_simulation "${sim_args[@]}"
else
  redirect_path=$1
  run_simulation "${sim_args[@]}"
fi
