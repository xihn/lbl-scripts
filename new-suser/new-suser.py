import argparse
import subprocess
import sys

def run_command(command):
    """Runs a shell command and returns the output and return code."""
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.decode().strip(), result.returncode

def check_and_add_slurm_user(username, cluster, account, partition, qos, filename):
    """Adds a user to the slurm database for a specific partition after a series of safety checks."""
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

    # Override GROUP for vulcan and etna partitions
    group = "nano" if account in ["vulcan", "etna"] else account

    # Check that the user belongs to the Project Account they are being added to
    print("Checking /etc/group as well")
    _, return_code = run_command(f"getent group {group} | grep {username}")
    if return_code != 0:
        print(f"{username} does not belong to this account {group}")
        print(f"This user will not be added to slurm on {cluster} until the problem is fixed")
        sys.exit(10)

    # Check if the user exists in the slurm database and has the correct partition
    output, return_code = run_command(f"/usr/bin/sacctmgr show association user={username} | grep -w {account} | grep '{partition} '")
    if return_code == 0:
        print(f"User {username} exists")
    else:
        print(f"User {username} does not exist")
        print(f"Going to add user {username} to partition {partition} with qos {qos}")
        with open(filename, 'a') as f:
            run_command(f"/usr/bin/sacctmgr -i add user Name={username} Partition={partition} QOS={qos} Account={account} AdminLevel=None\n")

def set_general_partition(cluster, account, qos, partition):
    """Sets up the qos and partition for the user to be added."""
    first_2_char = account.split('_')[0]
    partition_map = {
        "lr3": "lr_debug,lr_normal,lr_lowprio",
        "lr4": "lr_debug,lr_normal,lr_lowprio",
        "lr5": "lr_debug,lr_normal,lr_lowprio",
        "lr7": "lr_debug,lr_normal,lr_lowprio",
        "lr_bigmem": "lr_normal,lr_lowprio",
        "lr6": "lr_debug,lr_normal,lr6_lowprio",
        "cf1": "cf_debug,cf_normal,cf_lowprio",
        "es1": "es_debug,es_normal,es_lowprio",
        "cm1": "cm1_debug,cm1_normal",
        "mhg": "normal",
        "explorer": "normal",
        "hbar": "normal",
        "alsacc": "normal",
        "jbei1": "normal",
        "xmas": "normal",
        "alice": "normal",
        "jgi": "normal",
        "catamount": "cm_short,cm_medium,cm_long,cm_debug",
        "baldur": "normal",
        "nano": "normal,nano_debug",
        "etna": "normal",
        "dirac1": "normal",
        "hep": "hep_normal",
        "ood_inter": "lr_interactive",
    }
    if partition in partition_map:
        qos = partition_map[partition]
        part = partition
    else:
        print("Partition name is not valid.")
        output, _ = run_command("/usr/bin/sinfo | grep -v PARTITION | awk '{print $1}' | sort | uniq")
        print("Valid partitions are:", output)
        sys.exit(1)
    
    return part, qos

def set_condo_partition(account):
    """Sets QOS and partition for special accounts."""
    condo_map = {
        "lr_esd2": ("condo_esd2", "lr6"),
        "lr_oppie": ("condo_oppie", "lr6"),
        "lr_omega": ("condo_omega", "lr6"),
        "lr_alsu": ("condo_alsu", "lr6"),
        "lr_co2seq": ("condo_co2seq", "lr4"),
        "lr_esd1": ("condo_esd1", "lr3"),
        "lr_axl": ("condo_axl", "lr3"),
        "lr_nokomis": ("condo_nokomis", "lr3"),
        "lr_jgicloud": ("condo_jgicloud", "lr3"),
        "lr_minnehaha": ("condo_minnehaha", "lr4"),
        "lr_matminer": ("condo_matminer", "lr4"),
        "lr_ceder": ("condo_ceder", "lr5"),
        "lr_qchem": ("condo_qchem", "cm1"),
        "lr_neugroup": ("condo_neugroup", "csd_lr6_96"),
        "lr_fstheory": ("condo_fstheory", "csd_lr6_192"),
        "lr_statmech": ("condo_statmech", "csd_lr6_96"),
        "lr_farea": ("condo_farea", "lr6"),
        "lr_tns": ("condo_tns", "lr6")
    }
    return condo_map.get(account, (None, None))

def check_account(account, filename):
    """Checks if the account exists in the slurm database."""
    output, return_code = run_command(f"/usr/bin/sacctmgr show account -p | grep -w {account}")
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
                with open(filename, 'a') as f:
                    run_command(f"/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins=cpu=18000000 qos=lr_debug,lr_normal\n")
            else:
                with open(filename, 'a') as f:
                    run_command(f"/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins=cpu={pc_su} qos=lr_debug,lr_normal\n")
        else:
            with open(filename, 'a') as f:
                run_command(f"/usr/bin/sacctmgr create account name={account} Description={account} cluster Org={account}\n")

def main():
    parser = argparse.ArgumentParser(description="Add a user to the slurm database.")
    parser.add_argument("username", type=str, help="Username of the user to add.")
    parser.add_argument("cluster", type=str, help="Cluster name.")
    parser.add_argument("account", type=str, help="Account name.")

    args = parser.parse_args()

    filename = "out.txt"
    cluster = args.cluster
    account = args.account
    username = args.username

    if len(sys.argv) != 4:
        print("Usage: new-suser.py username cluster account")
        sys.exit(1)

    if cluster == "lawrencium":
        first_2_char = account.split('_')[0]
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            print(f"{account} is ok")
            partitions = ["lr3", "lr4", "lr5", "lr6", "lr7", "lr_bigmem"]
            for part in partitions:
                partition, qos = set_general_partition(cluster, account, qos=None, partition=part)
                check_account(account, filename)
                check_and_add_slurm_user(username, cluster, account, partition, qos, filename)
            if account == "pc_heptheory":
                check_account(account, filename)
                with open(filename, 'a') as f:
                    run_command(f"/usr/bin/sacctmgr -i add user {username} account={account} qos=lr_interactive partition=lr3_htc\n")
        elif first_2_char == "lr":
            qos, partition = set_condo_partition(account)
            if qos and partition:
                check_account(account, filename)
                check_and_add_slurm_user(username, cluster, account, partition, qos, filename)
            else:
                print("No special condo partition found. Exiting.")
                sys.exit(10)
        else:
            print("Accounts for Lawrencium or Mako must begin with ac_, lr_, ld_, pc_ or scs.")
            sys.exit(10)
    elif cluster == "californium":
        first_2_char = account.split('_')[0]
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            print(f"{account} is ok")
            partitions = ["cf1"]
            for part in partitions:
                partition, qos = set_general_partition(cluster, account, qos=None, partition=part)
                check_account(account, filename)
                check_and_add_slurm_user(username, cluster, account, partition, qos, filename)
        elif first_2_char == "lr":
            qos, partition = set_condo_partition(account)
            if qos and partition:
                check_account(account, filename)
                check_and_add_slurm_user(username, cluster, account, partition, qos, filename)
            else:
                print("No special condo partition found. Exiting.")
                sys.exit(10)
        else:
            print("Accounts for Californium must begin with ac_, lr_, ld_, pc_ or scs.")
            sys.exit(10)
    else:
        partition, qos = set_general_partition(cluster, account, qos=None, partition=cluster)
        check_account(account, filename)
        check_and_add_slurm_user(username, cluster, account, partition, qos, filename)

if __name__ == "__main__":
    main()
