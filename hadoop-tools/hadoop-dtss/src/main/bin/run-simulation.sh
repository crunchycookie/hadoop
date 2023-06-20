#!/usr/bin/env bash

print_usage() {
  echo "usage: run-simulation.sh [-h] [-b] [-p] [-n] [-o] [-t] sim_conf_path output_base_path"
  echo ""

  echo "This script should be executed from either \$HADOOP_HOME/share/hadoop/tools/dtss/bin/ OR"
  echo "from \$HADOOP_SRC/hadoop-tools/hadoop-dtss/src/main/bin/."
  echo "Note that we can only build if executed from the hadoop source path."

  echo ""

  echo "flags (optional):"
  echo -e "\t-h\t\t\tPrints help message."
  echo -e "\t-b\t\t\tBuild."
  echo -e "\t-p\t\t\tPackage."
  echo -e "\t-n\t\t\tPackage with native libraries."
  echo -e "\t-t\t\t\tSpecifies the base directory for temporary hadoop setups. Requires an argument."
  echo -e "\t-o\t\t\tSpecifies whether or not the output should be overwritten if exists."
  echo ""

  echo "positional arguments:"

  echo -e "\tsim_conf_path\t\tThe simulation configuration path, can either be a directory containing many "
  echo -e "\t\tconfiguration files or a single configuration file, where the name of the configuration file "
  echo -e "\t\tspecifies the run ID. For each configuration file, there should be a directory of the same "
  echo -e "\t\tfile name that specifies the YARN configurations used for the particular run within the same "
  echo -e "\t\tdirectory as the configuration file."

  echo -e "\toutput_base_path\tSpecifies the base directory for run outputs. A run directory will be created"
  echo -e "\t\tfurther within output_base_path for each run of the simulation."
}

pushd () {
  command pushd "$@" > /dev/null
}

popd () {
  command popd "$@" > /dev/null
}

cleanup() {
  local -n child_pids=$1
  local -n tmp_dirs=$2

  echo "Killing child processes ""${child_pids[*]}""..."
  for i in "${child_pids[@]}"
  do :
    echo "Killing child PID $i"
    kill -9 "$i"
  done

  wait

  echo "Cleaning up ""${tmp_dirs[*]}""..."
  for i in "${tmp_dirs[@]}"
  do :
    rm -rf "$i" &
  done

  wait
  echo "Cleanup done!"
}

analyze_simulation() {
  local run_id=$1
  local output_base_path=$2
  local output_path="$output_base_path"/"$run_id"

  python_dir="$( cd "$script_dir/../python/" >/dev/null 2>&1 && pwd )"
  if [ -e $python_dir ]; then
    echo "Detected python script directory at $python_dir"
  else
    echo "Cannot detect python script directory, skipping analysis."
    return
  fi

  pushd $python_dir
    if [[ "$VIRTUAL_ENV" = "" ]]; then
      in_venv=false
      venv_dir="$python_dir"/venv
      if [ ! -d $venv_dir ]; then
        # Create the virtual environment directory
        echo "Creating the virtual environment directory at $venv_dir"
        python3 -m venv $venv_dir
      fi

      echo "Activating virtual environment..."
      source "$venv_dir"/bin/activate
      echo "Upgrading pip..."
      pip3 install --upgrade pip
      echo "Installing requirements..."
      pip3 install -r requirements.txt
    else
      # Do not deactivate later if already in venv
      in_venv=true
      echo "Already in python virtual environment!"
    fi

    echo "Running python analysis on $output_path!"
    python3 sim_analysis.py analyze -i $run_id $output_path $output_base_path
    echo "Done!"
    if [ $in_venv = false ]; then
      deactivate
    fi
  popd
}

run_simulation() {
  local tmp_dir=$1
  local sim_conf_path=$2
  local output_base_path=$3

  local sim_conf_basedir="$( cd "$( dirname "$sim_conf_path" )" >/dev/null 2>&1 && pwd )"
  local sim_conf_file="${sim_conf_path##*/}"
  local sim_conf_id="${sim_conf_file%.*}"
  local output_path="$output_base_path"/"$sim_conf_id"
  local sim_conf_dir="$sim_conf_basedir"/"$sim_conf_id"
  echo "Using simulation configuration directory $sim_conf_dir"
  echo "Using output directory $output_path"

  if [ -d "$output_path" ]; then
    if [ "$overwrite" = false ]; then
      echo "$output_path exists and overwrite is not turned on! Returning..."
      return
    else
      echo "$output_path exists and overwrite is turned on! Deleting $output_path"
      rm -rf "$output_path"
    fi
  fi

  echo "Using temp directory $tmp_dir"
  echo "Copying hadoop home to tmp dir to prevent overwriting..."
  cp -r "$hadoop_home" "$tmp_dir"
  local hadoop_basename="$(basename $hadoop_home)"
  local local_hadoop_home="$tmp_dir/$hadoop_basename"
  echo "Using tmp hadoop home $local_hadoop_home"
  local local_hadoop_conf_dir="$local_hadoop_home/etc/hadoop/"
  cp -r "$sim_conf_dir"/* "$local_hadoop_conf_dir"

  echo "Running simulation!"
  local out_file="$output_path"/out.txt
  echo "Writing output file to $out_file"
  local local_dtss_run_path="$local_hadoop_home/share/hadoop/tools/dtss/bin/dtssrun.sh"
  if [ ! -e "$local_dtss_run_path" ]; then
    echo "The dtssrun.sh does not exist at $local_dtss_run_path!"
    return
  else
    echo "Found dtssrun.sh at $local_dtss_run_path"
  fi

  $local_dtss_run_path -i "$sim_conf_id" -m "$output_base_path" "$sim_conf_path" "$out_file"
  echo "Simulation run complete!"
  echo "Analyzing simulation run metrics..."
  analyze_simulation "$sim_conf_id" "$output_base_path"
}

if [[ $# = 0 ]]; then
  print_usage
  exit 1
fi

build=false
pkg=false
native=false
from_src=false
overwrite=false
tmp_dir_base=""

while getopts ":nbphot:" opt; do
  case $opt in
    o)
      overwrite=true
      ;;
    t)
      if [[ $OPTARG =~ ^-[n/b/p/h/o/t]$ ]]
        then
        echo "-t requires an argument!"
        print_usage
        exit 1
      fi
      tmp_dir_base=$OPTARG

      echo "Using $tmp_dir_base as temp base directory!"
      if [ ! -d "$tmp_dir_base" ]; then
        echo "$tmp_dir_base is not a valid directory!"
        print_usage
        exit 1
      fi

      if [ ! -w "$tmp_dir_base" ] || [ ! -r "$tmp_dir_base" ]; then
        echo "$tmp_dir_base must both be read and writable!"
        exit 1
      fi
      ;;
    n)
      native=true
      ;;
    b)
      build=true
      ;;
    p)
      pkg=true
      ;;
    h)
      print_usage
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      print_usage
      exit 1
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument" >&2
      print_usage
      exit 1
  esac
done

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
src_root_dir="$( cd "$script_dir/../../../../.." >/dev/null 2>&1 && pwd )"

if [ -e "$src_root_dir"/pom.xml ] && [ -e "$src_root_dir"/LICENSE.txt ]; then
  echo "Detected Hadoop source root directory at $src_root_dir"
  from_src=true
fi

if [ $from_src = true ]; then
  pushd $src_root_dir
    # Only build/package if running from source directory
    if [ $build = true ] ; then
      echo "-b is turned on, building..." >&2
      mvn clean install -DskipTests
      if [ $? -ne 0 ]; then
          echo "Maven build failed, exiting..."
          exit 1
        fi
    fi

    if [ $pkg = true ] ; then
      if [ $native = true ] ; then
        echo "-np is turned on, packaging native..." >&2
        mvn package -Pnative -Pdist -DskipTests -nsu
        if [ $? -ne 0 ]; then
          echo "Maven package failed, exiting..."
          exit 1
        fi
      else
        echo "-p is turned on, packaging..." >&2
        mvn package -Pdist -DskipTests -nsu
        if [ $? -ne 0 ]; then
          echo "Maven package failed, exiting..."
          exit 1
        fi
      fi
    fi

    hadoop_home_pattern="hadoop-dist/target/hadoop-*/"
    files=( $hadoop_home_pattern )
    hadoop_home_relative="${files[0]}"
    if [ "$hadoop_home_relative" = "$hadoop_home_pattern" ] ; then
      echo "Unable to find hadoop home! Exiting..."
      exit 1
    fi
    hadoop_home_relative=$src_root_dir/$hadoop_home_relative
  popd
else
  echo "Skipping build, since we are not running from the source directory."
  hadoop_home_relative="$script_dir"/../../../../..
fi

build_or_pkg=$build||$pkg

sim_conf_path_arg=${@:$OPTIND:1}
if [ -z $sim_conf_path_arg ] ; then
  if [ ! $build_or_pkg ]; then
    echo "Must have a simulation configuration path!"
    exit 1
  else
    echo "Done!"
    exit 0
  fi
fi

output_path_arg=${@:$(( $OPTIND + 1)):1}
if [ -z $output_path_arg ] ; then
  if [ ! $build_or_pkg ]; then
    echo "Must have an output path!"
    exit 1
  else
    echo "Done!"
    exit 0
  fi
fi

if [ ! -e $sim_conf_path_arg ]; then
  echo "The simulation configuration path must exist!"
  exit 1
fi

sim_conf_path="$(cd "$(dirname "$sim_conf_path_arg")"; pwd -P)/$(basename "$sim_conf_path_arg")"

echo "Using simulation configuration path: $sim_conf_path"

if [ ! -e $output_path_arg ]; then
  echo "The output path must exist!"
  exit 1
fi

output_base_path="$(cd "$(dirname "$output_path_arg")"; pwd -P)/$(basename "$output_path_arg")"
echo "Using output path: $output_base_path"

hadoop_home="$( cd "$hadoop_home_relative" >/dev/null 2>&1 && pwd )"
echo "Located hadoop home directory at $hadoop_home"

pushd "$hadoop_home"/share/hadoop/tools/dtss/bin
  tmp_dirs=()
  child_pids=()
  if [ -f "$sim_conf_path" ] ; then
    if [ -z "$tmp_dir_base" ] ; then
      tmp_dir=$(mktemp -d -t sim-XXXXXXXXXX)
    else
      tmp_dir=$(mktemp -p "$tmp_dir_base" -d -t sim-XXXXXXXXXX)
    fi
    tmp_dirs+=("$tmp_dir")
    run_simulation "$tmp_dir" "$sim_conf_path" "$output_base_path" &
    child_pids+=("$!")
  elif [ -d "$sim_conf_path" ]; then
    for conf in "$sim_conf_path"/*; do
      if [ ! -f "$conf" ]; then
        echo "$conf is not a file, skipping..."
        continue;
      fi

      if [ -z "$tmp_dir_base" ] ; then
        tmp_dir=$(mktemp -d -t sim-XXXXXXXXXX)
      else
        tmp_dir=$(mktemp -p "$tmp_dir_base" -d -t sim-XXXXXXXXXX)
      fi

      tmp_dirs+=("$tmp_dir")
      echo "Running simulation with configuration $conf!"
      run_simulation "$tmp_dir" "$conf" "$output_base_path" &
      child_pids+=("$!")
    done
  fi

  trap 'cleanup "${child_pids[*]}" "${tmp_dirs[*]}"' EXIT
  wait
popd
