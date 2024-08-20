import os
import sys
import subprocess

def check_and_add_slurm_user(username, cluster, account, part, qos):
    # Check if user exists in passwd
    if subprocess.call(['getent', 'passwd', username]) != 0:
        print("User does not exist in the password file. You must have an active account on the system before adding them to slurm.")
        sys.exit(10)

    # Check condo accounts are only added to the Lawrencium cluster
    account_type = account.split('_')[0]
    if account_type == "lr" and cluster not in ["lawrencium", "ood_inter"]:
        print("All condo accounts lr_ named ones should be given Lawrencium as the cluster name")
        print(f"Please reenter your input as {username} lawrencium {account}")
        sys.exit()

    # Override GROUP for vulcan and etna partitions
    group = 'nano' if account in ["vulcan", "etna"] else account

    # Check that the user belongs to the Project Account they are being added to
    if subprocess.call(['getent', 'group', group, '|', 'grep', username]) != 0:
        print(f"{username} does not belong to this account {group}")
        print(f"This user will not be added to slurm on {cluster} until the problem is fixed")
        sys.exit(10)

    # Check if the user exists in the slurm database and has the correct partition
    cmd = f'/usr/bin/sacctmgr show association user={username} | grep -w "{account}" | grep "{part}"'
    if subprocess.call(cmd, shell=True) == 0:
        print(f"User {username}: exists")
    else:
        print(f"User {username}: does not exist")
        print(f"Going to add user {username} to partition {part} with qos {qos}")
        with open('out.txt', 'a') as f:
            f.write(f"/usr/bin/sacctmgr -i add user Name={username} Partition={part} QOS={qos} Account={account} AdminLevel=None\n")


def set_general_partition(cluster, i, defqos="normal"):
    part = i if i else cluster
    qos = defqos

    partition_map = {
        'lr3': ("lr_debug,lr_normal,lr_lowprio", "lr"),
        'lr4': ("lr_debug,lr_normal,lr_lowprio", "lr"),
        'lr5': ("lr_debug,lr_normal,lr_lowprio", "lr"),
        'lr6': ("lr_debug,lr_normal,lr6_lowprio", "lr"),
        'lr7': ("lr_debug,lr_normal,lr_lowprio", "lr"),
        'lr_bigmem': ("lr_normal,lr_lowprio", "lr"),
        'cf1': ("cf_debug,cf_normal,cf_lowprio", "cf"),
        'es1': ("es_debug,es_normal,es_lowprio", "es"),
        'cm1': ("cm1_debug,cm1_normal", "cm"),
        'mhg': (defqos, "mhg"),
        'explorer': (defqos, "explorer"),
        'hbar': (defqos, "hbar"),
        'alsacc': (defqos, "alsacc"),
        'jbei1': (defqos, "jbei1"),
        'xmas': (defqos, "xmas"),
        'alice': (defqos, "alice"),
        'jgi': (defqos, "jgi"),
        'catamount': ("cm_short,cm_medium,cm_long,cm_debug", "catamount"),
        'baldur': (defqos, "baldur"),
        'nano': (f"{defqos},nano_debug", "nano"),
        'etna': (defqos, "etna"),
        'etna_gpu': (defqos, "etna_gpu"),
        'etna-shared': (defqos, "etna-shared"),
        'etna_bigmem': (defqos, "etna_bigmem"),
        'dirac1': (defqos, "dirac1"),
        'hep': ("hep_normal", "hep"),
        'ood_inter': ("lr_interactive", "ood_inter")
    }

    return partition_map.get(partition_map, (qos, part))


def check_account(account):
    if subprocess.call(['/usr/bin/sacctmgr', 'show', 'account', '-p', '|', 'grep', '-w', account]) != 0:
        print(f"Group {account}: Does not exist")
        print(f"Adding account {account}")

        first_2_char = account.split('_')[0]
        if first_2_char == "pc":
            pc_su = subprocess.check_output(['grep', account, '/global/home/groups/allhands/etc/pca.conf', '|', 'cut', '-d"|"', '-f3']).decode().strip()
            qos = pc_su if pc_su else "18000000"
            with open('out.txt', 'a') as f:
                f.write(f'/usr/bin/sacctmgr modify account where name={account} set GrpTRESMins="cpu={qos}" qos="lr_debug,lr_normal"\n')
        else:
            with open('out.txt', 'a') as f:
                f.write(f'/usr/bin/sacctmgr create account name={account} Description="{account} cluster" Org="{account}"\n')


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

    return condo_map.get(account, ("", ""))


def main():
    if len(sys.argv) != 4:
        print("Usage: new-suser.py username cluster account")
        sys.exit()

    username, cluster, account = sys.argv[1], sys.argv[2], sys.argv[3]

    with open('out.txt', 'w') as f:
        f.truncate(0)

    if cluster == "lawrencium":
        first_2_char = account.split('_')[0]
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            parts = ["lr3", "lr4", "lr5", "lr6", "lr7", "lr_bigmem"]
            for part in parts:
                qos, part_name = set_general_partition(cluster, part)
                check_account(account)
                check_and_add_slurm_user(username, cluster, account, part_name, qos)

            with open('out.txt', 'a') as f:
                f.write(f"/usr/bin/sacctmgr -i modify user where name={username} account={account} partition=lr_bigmem set qos=lr_normal,lr_debug\n")

            if account == "pc_heptheory":
                check_account(account)
                with open('out.txt', 'a') as f:
                    f.write(f"/usr/bin/sacctmgr -i add user {username} account={account} qos=lr_interactive partition=lr3_htc\n")
        elif first_2_char == "lr":
            qos, part = set_condo_partition(account)
            check_account(account)
            if account == "lr_cumulus":
                with open('out.txt', 'a') as f:
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=lr4 qos=condo_cumulus\n")
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=lr6 qos=condo_cumulus_lr6\n")
            elif account == "lr_mp":
                with open('out.txt', 'a') as f:
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=lr4 qos=condo_mp_lr2\n")
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=cf1 qos=condo_mp_cf1\n")
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=cf1-hp qos=condo_mp_cf1\n")
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=es1 qos=condo_mp_es1\n")
                    f.write(f"/usr/bin/sacctmgr -i create user {username} account={account} partition=lr6 qos=condo_mp_lr6\n")
            # Add other lr_* cases as needed
        else:
            print("Accounts for Lawrencium or Mako must begin with ac_, lr_, ld_, pc_ or scs. Exiting")
            sys.exit(10)

    elif cluster == "californium":
        first_2_char = account.split('_')[0]
        if first_2_char in ["ac", "scs", "ld", "pc"]:
            parts = ["cf1"]
            for part in parts:
                qos, part_name = set_general_partition(cluster, part)
                check_account(account)
                check_and_add_slurm_user(username, cluster, account, part_name, qos)
        elif first_2_char == "lr":
            qos, part = set_condo_partition(account)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part, qos)
        else:
            print("Accounts for Californium must begin with ac_, lr_, ld_, pc_ or scs. Exiting")
            sys.exit(10)

    elif cluster == "nano":
        parts = ["nano", "etna", "etna_gpu", "etna-shared", "etna_bigmem"]
        for part in parts:
            qos, part_name = set_general_partition(cluster, part)
            check_account(account)
            check_and_add_slurm_user(username, cluster, account, part_name, qos)

    else:
        qos, part_name = set_general_partition(cluster, cluster)
        check_account(account)
        check_and_add_slurm_user(username, cluster, account, part_name, qos)


if __name__ == "__main__":
    main()
