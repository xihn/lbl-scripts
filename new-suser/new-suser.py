import argparse
import subprocess
import sys

default_QOS = 'normal'

# Mapping account->PART , we assume QOS is condo_{username} ex. condo_omega
QOS_map_condo = {
    "lr_esd2": "lr6",
    "lr_oppie": "lr6",
    "lr_omega": "lr6",
    "lr_alsu": "lr6",
    "lr_co2seq": "lr4",
    "lr_esd1": "lr3", # Peter Lau 36 nodes
    "lr_axl": "lr3",
    "lr_nokomis": "lr3",
    "lr_jgicloud": "lr3",
    "lr_minnehaha": "lr4",
    "lr_matminer": "lr4",
    "lr_ceder": "lr5",
    "lr_qchem": "cm1",
    "lr_neugroup": "csd_lr6_96",
    "lr_fstheory": "csd_lr6_192",
    "lr_statmech": "csd_lr6_96",
    "lr_farea": "lr6",
    "lr_tns": "lr6"
}

# Maps account: "command ->suffix" . this is a mess, but its better than before... 
lr_map = {
    "lr_cumulus": [
        "partition=lr4 qos=condo_cumulus", 
        "partition=lr6 qos=condo_cumulus_lr6"
        ], 
    "lr_mp": [
        "partition=lr4 qos=condo_mp_lr2", 
        "partition=cf1 qos=condo_mp_cf1", 
        "partition=cf1-hp qos=condo_mp_cf1", 
        "partition=es1 qos=condo_mp_es1", 
        "partition=lr6 qos=condo_mp_lr6"
        ], 
    "lr_chandra": [
        "partition=es1 qos=condo_chandra_es1", 
        "partition=csd_lr6_192 qos=condo_chandra_lr6"
        ],

    "lr_ninjaone": [
        "partition=cm2 qos=condo_ninjaone_cm2",
        "partition=csd_lr6_192 qos=condo_ninjaone",
        "partition=csd_lr6_share qos=condo_ninjaone_share",
        "partition=es1 qos=condo_ninjaone_es1"
        ],
    "lr_amos": [
        "partition=lr7 qos=condo_amos7_lr7,lr_lowprio",
        "partition=csd_lr6_192 qos=condo_amos,lr_lowprio",
        "partition=lr6 qos=lr_lowprio",
        "partition=lr5 qos=lr_lowprio",
        "partition=lr4 qos=lr_lowprio",
        "partition=lr3 qos=lr_lowprio"
        ],
    "lr_essdata": [
        "partition=lr6 qos=condo_essdata_lr6,lr_lowprio",
        "partition=lr5 qos=lr_lowprio",
        "partition=lr4 qos=lr_lowprio",
        "partition=lr3 qos=lr_lowprio"
        ],
    "lr_mhg2": [
        "partition=lr7 qos=condo_mhg_lr7,lr_lowprio",
        "partition=csd_lr6_192 qos=condo_mhg2,lr_lowprio",
        "partition=lr6 qos=lr_lowprio",
        "partition=lr5 qos=lr_lowprio",
        "partition=lr4 qos=lr_lowprio",
        "partition=lr3 qos=lr_lowprio"
        ],
    "lr_rncstar": [
        "partition=lr7 qos=condo_rncstar_lr7,lr_lowprio",
        "partition=lr6 qos=lr_lowprio",
        "partition=lr5 qos=lr_lowprio",
        "partition=lr4 qos=lr_lowprio",
        "partition=lr3 qos=lr_lowprio"
        ],
    "lr_nanotheory": [
        "partition=es1 qos=condo_nanotheory_es1,es_lowprio",
        "partition=lr3 qos=condo_nanotheory,lr_lowprio"
        ],
    "lr_geop": [
        "partition=es1 qos=condo_geop_es1,es_lowprio",
        "partition=lr7 qos=condo_geop_lr7,lr_lowprio",
        "partition=lr6 qos=lr_lowprio",
        "partition=lr5 qos=lr_lowprio",
        "partition=lr4 qos=lr_lowprio",
        "partition=lr3 qos=lr_lowprio"
        ]
}

# partition -> [QOS]
QOS_map_main = {
    "lr3": ["lr_debug", "lr_normal", "lr_lowprio"],
    "lr4": ["lr_debug", "lr_normal", "lr_lowprio"],
    "lr5": ["lr_debug", "lr_normal", "lr_lowprio"],
    "lr7": ["lr_debug", "lr_normal", "lr_lowprio"],
    "lr6": ["lr_debug", "lr_normal", "lr6_lowprio"],
    "cf1": ["cf_debug", "cf_normal", "cf_lowprio"],
    "es1": ["es_debug", "es_normal", "es_lowprio"],

    "cm1": ["cm1_debug", "cm1_normal"],
    "lr_bigmem": ["lr_normal", "lr_lowprio"],

    "mhg": [default_QOS],
    "explorer": [default_QOS],
    "hbar": [default_QOS],
    "alsacc": [default_QOS],
    "jbei1": [default_QOS],
    "xmas": [default_QOS],
    "alice": [default_QOS],
    "jgi": [default_QOS],
    "etna": [default_QOS],
    "etna_gpu": [default_QOS],
    "etna-shared": [default_QOS],
    "etna_bigmem": [default_QOS]
}

# partition -> ("PART", [QOS])
QOS_map_standalone = {
    "catamount": ("catamount", ["cm_short", "cm_medium", "cm_long,cm_debug"]),
    "baldur": ("baldur1", [default_QOS]),
    "nano": ("nano1", [default_QOS, "nano_debug"]),
    "dirac1": ("dirac1", [default_QOS]),
    "hep": ("hep0", ["hep_normal"])
    #"ood_inter": ("ood_inter", ["lr_interactive"]),
}

def exec_command(string):
    "Executes a shell command without returning anything"
    subprocess.run(string, shell=True)
    # print("\033[31m" + string + "\033[0m")


def run_command(command):
    "Runs a shell command and returns the output and return code."
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.decode().strip(), result.returncode

def add_user(username, account, cluster, partition, qos):
    "Adds a user to the slurm database for a specific partition after a series of safety checks"

    # Check User Exists in passwd
    _, return_code = run_command(f"getent passwd {username}")
    if return_code != 0:
        print("User does not exist in the password file. You must have an active account on the system before adding them to slurm.")
        sys.exit(10)
    
    # Check condo accounts are only added to the Lawrencium cluster and not institutional clusters 
    account_type = account.split('_')[0]
    if account_type == "lr" and cluster not in ["lawrencium", "ood_inter"]:
        print("All condo accounts lr_ named ones should be given Lawrencium as the cluster name")
        print(f"Please reenter your input as {username} lawrencium {account}")
        sys.exit(1)

    # Edge Case: Override GROUP for vulcan and etna partitions because they belong to the nano project
    group = "nano" if account in ["vulcan", "etna"] else account

    # Check that the user belongs to the Project Account they are being added to
    _, return_code = run_command(f"getent group {group} | grep {username}")
    if return_code != 0:
        print(f"{username} does not belong to this account {group}")
        print(f"This user will not be added to slurm on {cluster} until the problem is fixed")
        sys.exit(10)

    # Check if the user exist in the slurm database and has the correct partition
    _, return_code = run_command(f"/usr/bin/sacctmgr show association user={username} | grep -w {account} | grep '{partition} '")
    if return_code == 0:
        print(f"User {username} exists")
    else: 
        print(f"User {username} does not exist")
        print(f"Going to add user {username} to partition {partition} with qos {qos}")
        exec_command(f"/usr/bin/sacctmgr -i add user Name={username} Partition={partition} QOS={qos} Account={account} AdminLevel=None")
    
def get_QOS_partition(cluster, account):
    "Returns the partition and QOS list as a touple"
    if cluster in QOS_map_main:
        account_type = account.split('_')[0]
        if account_type == "pc":
            temp = QOS_map_main[cluster]
            if len(temp) > 2:
                return (cluster, temp[:2])
            else:
                return (cluster, temp)
        else:
            return (cluster, QOS_map_main[cluster])
    
    elif cluster in QOS_map_standalone:
        return QOS_map_standalone[cluster]
    else:
        print("Partition name is not valid.")
        print("Valid ones are:")
        valids, _ = run_command("/usr/bin/sinfo | grep -v PARTITION | awk '{print $1}' | sort |uniq")
        print(valids)
        exit(10)

def check_account(account):
    "Checks if the account exists in the slurm database."
    _, return_code = run_command(f"/usr/bin/sacctmgr show account -p | grep -w {account}")
    if return_code == 0:
        print(f"Group {account} exists")
    else:
        print(f"Group {account} does not exist")
        print(f"Adding account {account}")
        first_2_char = account.split('_')[0]
        if first_2_char == "pc":
            pc_su_output, _ = run_command(f"grep {account} /global/home/groups/allhands/etc/pca.conf | cut -d'|' -f3")
            pc_su = pc_su_output.strip()
            if not pc_su:
                exec_command(f"/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins=cpu=18000000 qos=lr_debug,lr_normal")
            else:
                exec_command(f"/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins=cpu={pc_su} qos=lr_debug,lr_normal")
        else:
            exec_command(f"/usr/bin/sacctmgr create account name={account} Description={account} cluster Org={account}")

def qos_format(lst):
    return ','.join(lst)

def main():
    parser = argparse.ArgumentParser(description="Add a user to the slurm database. Usage: 'new-suser.py username cluster account'")
    parser.add_argument("username", type=str, help="Username of the user to add.")
    parser.add_argument("cluster", type=str, help="Cluster name. (lr3 and lr4 clustername is just lawrencium)")
    parser.add_argument("account", type=str, help="Account name. (ac_|clustername|lr_|scs)")

    args = parser.parse_args()

    cluster = args.cluster
    account = args.account
    username = args.username

    first_2_char = account.split('_')[0]

    # if python >= 3.10 could have used match and case. 
    # match cluster: case "lawrencium" 
    if cluster == "lawrencium":
        if first_2_char in ["ac","scs","ld","pc"]: 
            print(f"{account} is ok")
            parts = ["lr3", "lr4", "lr5", "lr6", "lr7", "lr_bigmem"]
            for i in parts:
                temppart, tempqos = get_QOS_partition(i, account)
                check_account(account)
                add_user(username, account, cluster, temppart, qos_format(tempqos))
            exec_command(f"/usr/bin/sacctmgr -i modify user where name={username} account={account} partition=lr_bigmem set qos=lr_normal,lr_debug")
            if account == "pc_heptheory":
                check_account(account)
                exec_command(f"/usr/bin/sacctmgr -i add user {username}  account={account} qos=lr_interactive partition=lr3_htc")
        
        elif first_2_char == "lr":
            if account in lr_map:
                check_account(account)
                tempargs = lr_map[account]

                for i in tempargs:
                    exec_command(f"/usr/bin/sacctmgr -i create user {username} account={account} " + i)
            else:
                if account in QOS_map_condo:
                    condopart = QOS_map_condo[account]
                    tempaccholder = account.split('_', 1)
                    condoqos = "condo_" + tempaccholder[1]
                    check_account(account)
                    add_user(username, account, cluster, condopart, condoqos)
                else:
                    print(f"Error {account} not in QOS_map_condo. Exiting")
                    exit(10)

        else:
            print("Accounts for Lawrencium or Mako must must begin with ac_, lr_, ld_, pc_ or scs.  Exiting")
            exit(10)


    elif cluster == "californium":
        if first_2_char in ["ac","scs","ld","pc"]:
            print(f"{account} is ok")
            parts = ["cf1"]
            for i in parts:
                temppart, tempqos = get_QOS_partition(i, account)
                check_account(account)
                add_user(username, account, cluster, temppart, qos_format(tempqos))
        elif first_2_char == "lr":
            if account in QOS_map_condo:
                    condopart = QOS_map_condo[account]
                    tempaccholder = account.split('_', 1)
                    condoqos = "condo_" + tempaccholder[1]
                    check_account(account)
                    add_user(username, account, cluster, condopart, condoqos)
            else:
                    print(f"Error {account} not in QOS_map_condo. Exiting")
                    exit(10)

    elif cluster == "nano":
        parts = ["nano", "etna", "etna_gpu", "etna-shared", "etna_bigmem"]
        for i in parts:
                temppart, tempqos = get_QOS_partition(i, account)
                check_account(account)
                add_user(username, account, cluster, temppart, qos_format(tempqos))
    
    else:
        temppart, tempqos = get_QOS_partition(cluster, account)
        check_account(account)
        add_user(username, account, cluster, temppart, qos_format(tempqos))


if __name__ == "__main__":
    main()
