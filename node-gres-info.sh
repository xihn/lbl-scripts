#!/bin/bash

if [ -z "$1" ]; then
        echo "Error: usage ./node-gres-info.sh partition"
        echo "Example: ./node-gres-info.sh es1"
        exit 1
fi

BLUE="\e[94m"
BOLDBLUE="\e[1;94m"
NC="\e[0m"

echo -e "Checking Partition: ${BOLDBLUE}$1${NC}"

nodes=$(sinfo -p $1 -o "%N" --noheader)
if [[ -z "$nodes" ]]; then
    echo -e "Error: Partition ${BOLDBLUE}$partition_name${NC} does not exist"
    exit 1
fi
IFS=',' read -r -a nodes <<< "$nodes"

declare -A node_associative

for node in "${nodes[@]}"; do
node_info=$(scontrol show node $node)

# Extract values for sanity check
CPUEfctv=$(echo "$node_info" | grep -oP 'CPUEfctv=\K\d+')
CPUTot=$(echo "$node_info" | grep -oP 'CPUTot=\K\d+')
CfgTRES_cpu=$(echo "$node_info" | grep -oP 'CfgTRES=cpu=\K\d+')
gres_info=$(echo "$node_info" | grep -E "Gres=")


if ! [[ "$CPUEfctv" -eq "$CPUTot" && "$CPUEfctv" -eq "$CfgTRES_cpu" ]]; then
      echo "Warning: There is an issue with $node - CPU values do not match!"
      continue
    fi

if [[ $gres_info == *"Gres=(null)"* ]]; then
      echo -e "Warning: No GPUs available on node ${BLUE}$node${NC}"
      continue
    else
      # Extract GPU type and count
      gpu_type=$(echo $gres_info | sed -n 's/.*gpu:\(.*\):[0-9]*/\1/p')
      gpu_count=$(echo $gres_info | awk -F ":" '{print $3}')

      # Compute Cores per GPU
      cores_per_gpu=$(echo "scale=1; $CPUEfctv / $gpu_count" | bc)


        key="$gpu_count x $gpu_type, $CPUEfctv CPU Cores, $cores_per_gpu Cores per GPU"
        node_associative["$key"]+="$node "
        # echo $key" \n"
    fi

done
echo $node_associative

for config in "${!node_associative[@]}"; do
    node_count=$(echo "${node_associative[$config]}" | wc -w)
    echo -e "Configuration: ${BLUE}$config${NC}"
    echo "Nodes: ${node_associative[$config]}"
    echo "Count: $node_count"
    echo
done