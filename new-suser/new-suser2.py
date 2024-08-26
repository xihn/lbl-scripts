import sys
import subprocess


def check_and_add_slurm_user(username, cluster, account, part, qos):
    # Check if user exists in passwd
    user_exists = subprocess.run(['getent', 'passwd', username], capture_output=True)
    if user_exists.returncode != 0:
        print("User does not exist in the password file. You must have an active account on the system before adding them to slurm.")
        sys.exit(10)

    # Check if condo accounts are only added to the Lawrencium cluster and not institutional clusters
    account_type = account.split('_')[0]
    if account_type == "lr" and cluster not in ["lawrencium", "ood_inter"]:
        print("All condo accounts lr_ named ones should be given Lawrencium as the cluster name.")
        print(f"Please reenter your input as {username} lawrencium {account}")
        sys.exit()

    # Override GROUP for specific partitions
    group = "nano" if account in ["vulcan", "etna"] else account

    # Check if user belongs to the project account
    group_check = subprocess.run(['getent', 'group', group], capture_output=True)
    if username not in group_check.stdout.decode():
        print(f"{username} does not belong to this account {group}.")
        print(f"This user will not be added to slurm on {cluster} until the problem is fixed.")
        sys.exit(10)

    # Check if the user exists in the SLURM database and has the correct partition
    slurm_check = subprocess.run(
        ['/usr/bin/sacctmgr', 'show', 'association', f'user={username}'],
        capture_output=True
    )
    if f"{account}" in slurm_check.stdout.decode() and f"{part}" in slurm_check.stdout.decode():
        print(f"User {username} exists.")
    else:
        print(f"User {username} does not exist.")
        print(f"Going to add user {username} to partition {part} with qos {qos}.")
        print(f"/usr/bin/sacctmgr -i add user Name={username} Partition={part} QOS={qos} Account={account} AdminLevel=None")


def set_general_partition(partition, cluster, first_2_char):
    if partition:
        part = partition
    else:
        part = cluster

    qos_map = {
        "lr3": "lr_debug,lr_normal,lr_lowprio",
        "lr4": "lr_debug,lr_normal,lr_lowprio",
        "lr5": "lr_debug,lr_normal,lr_lowprio",
        "lr7": "lr_debug,lr_normal,lr_lowprio",
        "lr_bigmem": "lr_normal,lr_lowprio",
        "lr6": "lr_debug,lr_normal,lr6_lowprio",
        "cf1": "cf_debug,cf_normal,cf_lowprio",
        "es1": "es_debug,es_normal,es_lowprio",
        "cm1": "cm1_debug,cm1_normal",
        "ood_inter": "lr_interactive"
    }

    qos = qos_map.get(part, "normal")
    if first_2_char == "pc":
        qos = qos_map.get(part, "lr_debug,lr_normal")

    return part, qos


def check_account(account):
    account_check = subprocess.run(['/usr/bin/sacctmgr', 'show', 'account', '-p'], capture_output=True)
    if account not in account_check.stdout.decode():
        print(f"Group {account} does not exist.")
        print(f"Adding account {account}.")
        first_2_char = account.split('_')[0]
        if first_2_char == "pc":
            print(f"/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins=cpu=18000000 qos=lr_debug,lr_normal")
        else:
            print(f"/usr/bin/sacctmgr create account name={account} Description={account} cluster Org={account}")


def set_condo_partition(account):
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


def main():
    if len(sys.argv) != 4:
        print("Usage: new-suser.py username cluster account")
        sys.exit()

    username = sys.argv[1]
    cluster = sys.argv[2]
    account = sys.argv[3]
    first_2_char = account.split('_')[0]

    if cluster == "lawrencium":
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            parts = ["lr3", "lr4", "lr5", "lr6", "lr7", "lr_bigmem"]
            for part in parts:
                part, qos = set_general_partition(part, cluster, first_2_char)
                check_account(account)
                check_and_add_slurm_user(username, cluster, account, part, qos)
        elif first_2_char == "lr":
            qos, part = set_condo_partition(account)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part, qos)
        else:
            print("Accounts for Lawrencium or Mako must begin with ac_, lr_, ld_, pc_ or scs. Exiting.")
            sys.exit(10)
    elif cluster == "californium":
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            part, qos = set_general_partition("cf1", cluster, first_2_char)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part, qos)
        elif first_2_char == "lr":
            qos, part = set_condo_partition(account)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part, qos)
        else:
            print("Accounts for Californium must begin with ac_, lr_, ld_, pc_ or scs. Exiting.")
            sys.exit(10)
    elif cluster == "nano":
        parts = ["nano", "etna", "etna_gpu", "etna-shared", "etna_bigmem"]
        for part in parts:
            part, qos = set_general_partition(part, cluster, first_2_char)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part, qos)
    else:
        part, qos = set_general_partition(cluster, cluster, first_2_char)
        check_account(account)
        check_and_add_slurm_user(username, cluster, account, part, qos)


if __name__ == "__main__":
    main()
