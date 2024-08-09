#!/bin/bash

# Parse command line arguments
while getopts "p:o:" opt; do
  case ${opt} in
    p ) partition_name=$OPTARG ;;
    o ) output_file=$OPTARG ;;
    \? ) echo "Invalid option: -$OPTARG" 1>&2; exit 1 ;;
    : ) echo "Invalid option: -$OPTARG requires an argument" 1>&2; exit 1 ;;
  esac
done

# Define default values
node_prefix="n"

# Declare associative array to store node information
declare -A node_associative

function check_nodes() {
  local nodes=("$@")
  
  for node in "${nodes[@]}"; do
    node_info=$(scontrol show node $node)

    # Extract values for sanity check
    CPUEfctv=$(echo "$node_info" | grep -oP 'CPUEfctv=\K\d+')
    CPUTot=$(echo "$node_info" | grep -oP 'CPUTot=\K\d+')
    CfgTRES_cpu=$(echo "$node_info" | grep -oP 'CfgTRES=cpu=\K\d+')
    gres_info=$(echo "$node_info" | grep -E "Gres=")

    # Sanity check for CPU values
    if ! [[ "$CPUEfctv" -eq "$CPUTot" && "$CPUEfctv" -eq "$CfgTRES_cpu" ]]; then
      echo "Warning: There is an issue with $node - CPU values do not match!"
      continue
    fi

    # Check if Gres is null
    if [[ $gres_info == *"Gres=(null)"* ]]; then
      echo "Warning: No GPUs available on node $node"
      continue
    else
      # Extract GPU type and count
      gpu_type=$(echo $gres_info | sed -n 's/.*gpu:\(.*\):[0-9]*/\1/p')
      gpu_count=$(echo $gres_info | awk -F ":" '{print $3}')

      # Compute Cores per GPU
      cores_per_gpu=$(echo "scale=1; $CPUEfctv / $gpu_count" | bc)

      # Add to associative array
      key="$gpu_count x $gpu_type, $CPUEfctv CPU Cores, $cores_per_gpu Cores per GPU"
      node_associative["$key"]+="$node "
    fi
  done
}

# Check if partition is valid
if [[ -n "$partition_name" ]]; then
  nodes=$(sinfo -p $partition_name -o "%N" --noheader)
  if [[ -z "$nodes" ]]; then
    echo "Error: Partition $partition_name does not exist"
    exit 1
  fi
else
  # Default to all nodes if no partition specified
  nodes=$(sinfo -o "%N" --noheader)
fi

# Check nodes and populate associative array
check_nodes $nodes

# Output results to console or file
output_target="/dev/stdout"
if [[ -n "$output_file" ]]; then
  output_target="$output_file"
fi

# Print results
{
  for config in "${!node_associative[@]}"; do
    echo "Configuration: $config"
    echo "Nodes: ${node_associative[$config]}"
    echo
  done
} > "$output_target"