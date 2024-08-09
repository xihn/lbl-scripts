nodes=$(sinfo -p es1 -o "%N" --noheader)
if [[ -z "$nodes" ]]; then
    echo "Error: Partition $partition_name does not exist"
    exit 1
fi


for node in "${nodes[@]}"; do
    node_info=$(scontrol show node $nodes)

    # Extract values for sanity check
    CPUEfctv=$(echo "$node_info" | grep -oP 'CPUEfctv=\K\d+')
    CPUTot=$(echo "$node_info" | grep -oP 'CPUTot=\K\d+')
    CfgTRES_cpu=$(echo "$node_info" | grep -oP 'CfgTRES=cpu=\K\d+')
    gres_info=$(echo "$node_info" | grep -E "Gres=")

    echo $gres_info
    echo $CfgTRES_cpu
    echo $CPUTot
    echo $CPUEfctv
done