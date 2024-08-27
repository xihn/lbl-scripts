import subprocess
import difflib 
import newsuser as sus
import os

username = "mhyeh"

clusternames = ["lawrencium", "ood_inter", "californium", "nano", "errorexample"]

accounts_condo = ["errorexample", "lr_esd2", "lr_oppie", "lr_omega", "lr_alsu", "condo_co2seq", "lr_esd1", 
            "lr_axl", "lr_nokomis", "lr_jgicloud", "lr_minnehaha", "lr_matminer", "lr_ceder", "lr_qchem", 
            "lr_neugroup", "lr_fstheory", "lr_statmech", "lr_farea", "lr_tns"]

accounts_normal = ["lr_cumulus", "lr_chandra", "lr_ninjaone", "lr_amos", "lr_essdata", "lr_mhg2", 
                   "lr_rncstar", "lr_nanotheory", "lr_geop", ]

def check_differences():
    print("difference between: " + _ + " with " + i + " :\n")

    with open('python.txt') as file_1: 
        file_1_text = file_1.readlines() 
    
    with open('bash.txt') as file_2: 
        file_2_text = file_2.readlines() 
    
    # Find and print the diff: 
    for line in difflib.unified_diff( 
            file_1_text, file_2_text, fromfile='python.txt',  
            tofile='bash.txt', lineterm=''): 
        print(line) 
    
    print("\n")

for _ in clusternames:
    for i in accounts_normal:
        result_python = subprocess.run(['python3', 'newsuser2.py', username, _, i])
        result_bash = subprocess.run(['./printinfile.sh %s %s %s' %(username,_,i)], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if not os.path.isfile("bash.txt"):
            # Create an empty file
            with open("bash.txt", "w") as file:
                pass 

        check_differences()

        os.remove("bash.txt")
        os.remove("python.txt") 

    for a in accounts_normal:
        result_python = subprocess.run(['python3', 'newsuser2.py', username, _, i])
        result_bash = subprocess.run(['./printinfile.sh %s %s %s' %(username,_,a)], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if not os.path.isfile("bash.txt"):
            # Create an empty file
            with open("bash.txt", "w") as file:
                pass 
        
        check_differences()

        os.remove("bash.txt")
        os.remove("python.txt") 

