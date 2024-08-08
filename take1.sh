#!/bin/bash

# Function to check node information
function check_nodes() {
  local nodes=$1
  declare -A node_associative

  for node in $nodes; do
    node_info=$(scontrol show node $node)
    
    # Extract CPU information
    CPUEfctv=$(echo "$node_info" | grep -oP 'CPUEfctv=\K\d+')
    CPUTot=$(echo "$node_info" | grep -oP 'CPUTot=\K\d+')
    CfgTRES_cpu=$(echo "$node_info" | grep -oP 'CfgTRES=cpu=\K\d+')
    gres_info=$(echo "$node_info" | grep -E "Gres=")

    # Sanity check for CPU values
    if ! [[ "$CPUEfctv" -eq "$CPUTot" && "$CPUEfctv" -eq "$CfgTRES_cpu" ]]; then
      echo "Warning: There is an issue with $node: CPU values mismatch"
      continue  # Skip this iteration
    fi

    # Check if Gres is null
    if [[ $gres_info == *"Gres=(null)"* ]]; then
      echo "Warning: No GPUs available on node $node"
      continue  # Skip this iteration
    else
      # Extract GPU type and count
      gpu_type=$(echo $gres_info | sed -n 's/.*gpu:\(.*\):[0-9]*/\1/p')
      gpu_count=$(echo $gres_info | awk -F ":" '{print $3}')

      # Add to associative array
      node_associative[$node]="${gpu_type},${gpu_count},${CPUTot}"
    fi
  done

  # Output unique combinations and their counts
  echo "Unique combinations:"
  for entry in "${node_associative[@]}"; do
    echo "$entry"
  done | sort | uniq -c
}

# Parse command line arguments
while getopts "p:n:o:" opt; do
  case ${opt} in
    p)
      partition_name=$OPTARG
      ;;
    n)
      node_range=$OPTARG
      ;;
    o)
      output_file=$OPTARG
      ;;
    \?)
      echo "Usage: $0 [-p partition_name] [-n node_range] [-o output_file]"
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

# Validate partition and node range
if [[ -n $partition_name ]]; then
  nodes=$(sinfo -p $partition_name -o "%N" --noheader)
else
  nodes=$(sinfo -o "%N" --noheader)
fi

if [[ -n $node_range ]]; then
  # Validate node range
  if ! [[ $node_range =~ ^[0-9]+:[0-9]+$ ]]; then
    echo "Invalid node range format. Use start:end, e.g., 0:15."
    exit 1
  fi

  start_node=$(echo $node_range | cut -d: -f1)
  end_node=$(echo $node_range | cut -d: -f2)
  
  if (( start_node > end_node )); then
    echo "Invalid node range: start node is greater than end node."
    exit 1
  fi

  nodes=$(echo "$nodes" | grep -E "$node_prefix[0-9]{4}.$partition_name" | awk -v start="$start_node" -v end="$end_node" '{print $0}' | grep -E "n($(seq -f "%04g" $start $end))")
fi

# Handle output redirection
if [[ -n $output_file ]]; then
  check_nodes "$nodes" > "$output_file"
else
  check_nodes "$nodes"
fi