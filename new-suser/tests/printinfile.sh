#!/bin/sh

## this script is written add individual users accounts to the slurm database
##
## The format for the input is as follows:
###
##  sacctmgr add user name=scoggins account=cortex partition=cortex adminlevel=admin defaultaccount=ac_scs QOS=normal
##  sacctmgr add user name=username account=group-name partition=queue adminlevel=NONE defaultaccount=group-name QOS=qos-list
### echo "sacctmgr add user name=$Name account=$Account partition=$Partition defaultaccount=$Account QOS=$QOS"

username=$1
CLUSTER=$2
ACCOUNT=$3
DEFQOS=normal

if [ $# -ne 3 ] ; then
  echo "Usage: new-suser.sh username cluster account"
  echo "Account = clustername, account =  ac_|clustername|lr_|scs "
  echo "lr3 and lr4 clustername is just lawrencium"
  exit
fi


filename="bash.txt"


check_and_add_slurm_user() {
# Adds a user to the slurm database for a specific partition after a series of safety checks.
#
# Safety Checks:
# - User exists in passwd
# - Condo accounts are added to Lawrencium cluster and not institutional clusters.
# - User belongs to the Project Account they are being added to
# - Avoid duplicate entry in slurm database
#
  # Check User Exists in passwd
  #grep "${username}:" /etc/passwd  > /dev/null
  getent passwd "${username}" > /dev/null
  if [ $? -ne 0 ] ; then
    echo "user does not exist in the password file.  You must have an active account on the system before adding them to slurm."
    exit 10
  fi

  # Check condo accounts are only added to the Lawrencium cluster and not institutional clusters
  local account_type=`echo $ACCOUNT | awk -F\_ '{print $1}'`
  if [ "${account_type}" = "lr" ] && { [ ${CLUSTER} != "lawrencium" ] && [ ${CLUSTER} != "ood_inter" ] ; } ; then
    echo "All condo accounts lr_ named ones should be given Lawrencium as the cluster name"
    echo "The script will appropriately setup the correct partitions"
    echo " Please reenter your input as ${username} lawrencium ${ACCOUNT}"
    exit
  fi

  # Edge Case: Override GROUP for vulcan and etna partitions because they belong to the nano project
  if [ ${ACCOUNT} = "vulcan" ] || [ ${ACCOUNT} = "etna" ] ; then
    local GROUP=nano
  else
    local GROUP=$ACCOUNT
  fi

  # Check that the user belongs to the Project Account they are being added to
  echo "Checking /etc/group as well"
  #id ${username} | grep "${GROUP}" > /dev/null
  getent group "${GROUP}" | grep ${username} > /dev/null
  if [ $? -ne 0 ]; then
    echo "${username} does not belong to this account $GROUP"
    echo "This user will not be added to slurm on $CLUSTER until the problem is fixed"
    echo "Please pick another account the user is a member of. Or fix the account and then come back"
    exit 10
  fi

  # Check if the user exist in the slurm database and has the correct partition
  ## check to see if the user exist then check to see if the partition exist
  /usr/bin/sacctmgr show association user=${username} | grep -w "${ACCOUNT}" | grep "${PART} "  > /dev/null
  if [ $? -eq 0 ] ; then
    echo User $username: exist
  else
    echo User $username: does not exist
    echo "going to add user $username to partition $PART with qos  $QOS"
    echo -e "/usr/bin/sacctmgr -i  add  user Name=$username  Partition=$PART  QOS=$QOS Account=$ACCOUNT AdminLevel=None" >> $filename
  fi
}





set_general_partition() {
# For a given Cluster, update global variables for QOS and PARTITION
#
  ## setup the qos and the partition for the user to be added
#

  if [ "$i" != "" ] ; then
    PARTITION=$i
  else
    PARTITION=$CLUSTER
  fi

  ## NOTE: Not sure what qos lr_bigmem should be

  case $PARTITION  in
    lr3|lr4|lr5|lr7)  PART=${PARTITION};
      if [ "${first_2_char}" != pc ] ; then
        QOS="lr_debug,lr_normal,lr_lowprio"
      else
      ## Not sure why this is here - all qos names should be lr_debug or lr_normal - 12/18/2019
      ##    if [ "$QOS" != " " ] ; then
      ##      QOS=$QOS
      ##    else
      ##      QOS=lr_debug,lr_normal
      ##    fi
        QOS=lr_debug,lr_normal
      fi
      ;;

    lr_bigmem)  PART=${PARTITION};    QOS="lr_normal,lr_lowprio" ;;

    lr6)  PART=${PARTITION};
      if [ "${first_2_char}" != pc ] ; then
        QOS="lr_debug,lr_normal,lr6_lowprio"
      else
        QOS=lr_debug,lr_normal
      fi
      ;;

    cf1) PART=${PARTITION};
      if [ "${first_2_char}" != pc ] ; then
        QOS="cf_debug,cf_normal,cf_lowprio"
      else
        QOS="cf_debug,cf_normal"
      fi
      ;;

    es1) PART=${PARTITION};
      if [ "${first_2_char}" != "pc" ] ; then
        QOS="es_debug,es_normal,es_lowprio"
      else
        QOS="es_debug,es_normal"
      fi
      ;;

    cm1) PART=${PARTITION};
      if [ "${first_2_char}" != pc ] ; then
        QOS="cm1_debug,cm1_normal"
      else
        QOS="cm1_debug,cm1_normal"
      fi
      ;;

    mhg|explorer|hbar|alsacc|jbei1|xmas|alice|jgi ) PART=${PARTITION};   QOS=$DEFQOS  ;;
    ## musigny )  PART=musigny; QOS="normal,musigny_c48" ;;
    catamount )  PART=catamount; QOS="cm_short,cm_medium,cm_long,cm_debug"  ;;
    baldur )  PART=baldur1; QOS="$DEFQOS" ;;
    nano )  PART=nano1; QOS="$DEFQOS,nano_debug" ;;
    etna|etna_gpu|etna-shared|etna_bigmem ) PART=${PARTITION};   QOS=$DEFQOS  ;;
    dirac1 )  PART=dirac1; QOS="$DEFQOS" ;;
    hep )  PART=hep0; QOS="hep_normal" ;;
    ##vulcan )  PART=vulcan; QOS="$DEFQOS,vulcan_debug" ;;
    ##vulcan_c20|vulcan_gpu|vulcan_c24)  PART=${PARTITION};  QOS="$DEFQOS" ;;
    ood_inter)  PART=ood_inter;  QOS="lr_interactive" ;;
    * )
      echo "Partition name is not valid."
      echo "Valid ones are:"
      /usr/bin/sinfo | grep -v PARTITION | awk '{print $1}' | sort |uniq
      ;;
  esac
}





check_account() {
  /usr/bin/sacctmgr show account -p | grep -w  "$ACCOUNT"  > /dev/null
  if [ $? -eq 0 ] ; then
    echo Group $ACCOUNT : exist
  else
    echo "Group $ACCOUNT: Does not exist"
    echo "Adding account $ACCOUNT"

    first_2_char=`echo ${ACCOUNT} | awk -F\_ '{print $1}'`
    if [[ $first_2_char == "pc" ]]; then
      PC_SU=`grep $ACCOUNT /global/home/groups/allhands/etc/pca.conf|cut -d"|" -f3`
      if [[ -z $PC_SU ]]; then
        echo -e "/usr/bin/sacctmgr modify account where name=$ACCOUNT set GrpTRESMins="cpu=18000000" qos="lr_debug,lr_normal"" >> $filename
      else
        echo -e "/usr/bin/sacctmgr modify account where name=$ACCOUNT set GrpTRESMins="cpu=$PC_SU" qos="lr_debug,lr_normal"" >> $filename
      fi
    else
      echo -e "/usr/bin/sacctmgr create account name=$ACCOUNT Description="$ACCOUNT cluster" Org="$ACCOUNT"" >> $filename
    fi
  fi
}





set_condo_partition() {
  ## Condo for Peter Lau  16 nodes
  if [ "${ACCOUNT}" = "lr_esd2" ] ; then
    QOS=condo_esd2
    PART=lr6
  fi

  ## Condo for Joel Moore 12 nodes
  if [ "${ACCOUNT}" = "lr_oppie" ] ; then
    QOS=condo_oppie
    PART=lr6
  fi

  ## Condo for Mark Asta 12 nodes
  if [ "${ACCOUNT}" = "lr_omega" ] ; then
    QOS=condo_omega
    PART=lr6
  fi

  ## Condo for Thorsten Hellert 4 nodes
  if [ "${ACCOUNT}" = "lr_alsu" ] ; then
    QOS=condo_alsu
    PART=lr6
  fi

  ##  Michael Commer Moved from lr2 to lr4 32 nodes
  if [ "${ACCOUNT}" = "lr_co2seq"  ] ; then
    QOS=condo_co2seq
    PART=lr4
  fi

  ## Peter Lau 36 nodes
  if [ "${ACCOUNT}" = "lr_esd1"  ] ; then
    QOS=condo_esd1
    PART=lr3
  fi

  ## Robert Lucchese 30 nodes
  if [ "${ACCOUNT}" = "lr_axl" ] ; then
    QOS=condo_axl
    PART=lr3
  fi

  ## Jeff Neaton 40 nodes
  if [ "${ACCOUNT}" = "lr_nokomis" ] ; then
    QOS=condo_nokomis
    PART=lr3
  fi

  ## Kjiersten Fagnan 40 nodes
  if [ "${ACCOUNT}" = "lr_jgicloud" ] ; then
    QOS=condo_jgicloud
    PART=lr3
  fi

  ## Jeff Neaton 36 nodes
  if [ "${ACCOUNT}" = "lr_minnehaha" ] ; then
    QOS=condo_minnehaha
    PART=lr4
  fi

  ## Anubhav Jain  4 nodes
  if [ "${ACCOUNT}" = "lr_matminer" ] ; then
    QOS=condo_matminer
    PART=lr4
  fi

  ## Gerbrand Ceder 44 nodes
  if [ "${ACCOUNT}" = "lr_ceder" ] ; then
    QOS=condo_ceder
    PART=lr5
  fi

  ## Martin Head-Gordon 14 nodes
  if [ "${ACCOUNT}" = "lr_qchem" ] ; then
    QOS=condo_qchem
    PART=cm1
  fi

  ## Eric Neuscamman 22 nodes
  if [ "${ACCOUNT}" = "lr_neugroup" ] ; then
    QOS=condo_neugroup
    PART=csd_lr6_96
  fi

  ## Theresa Head-Gordan  18 nodes
  ##if [ "${ACCOUNT}" = "lr_ninjaone" ] ; then
  ##  QOS=condo_ninjaone
  ##  PART=csd_lr6_192
  ##fi

  ## David Prendergast  18 nodes
  if [ "${ACCOUNT}" = "lr_fstheory" ] ; then
    QOS=condo_fstheory
    PART=csd_lr6_192
  fi

  ## Phil Geissler  22 nodes
  if [ "${ACCOUNT}" = "lr_statmech" ] ; then
    QOS=condo_statmech
    PART=csd_lr6_96
  fi

  ## Haruko Wainwright  4 nodes
  if [ "${ACCOUNT}" = "lr_farea" ] ; then
    QOS=condo_farea
    PART=lr6
  fi

  ## Michael Zaletel   4 nodes
  if [ "${ACCOUNT}" = "lr_tns" ]; then
          QOS=condo_tns
          PART=lr6
  fi
}





############################################
## beginning

if [ ${CLUSTER} = "lawrencium" ] ; then
  first_2_char=`echo ${ACCOUNT} | awk -F\_ '{print $1}'`
  case $first_2_char in

    ac|scs|ld|pc)
      echo $ACCOUNT is ok ;
      PARTS="lr3 lr4 lr5 lr6 lr7 lr_bigmem";
      for i in $PARTS ; do
        set_general_partition $i
        check_account $ACCOUNT
        check_and_add_slurm_user $username
      done
      ## TODO: What are we doing with lr_bigmem and this need to be documented  - JBS
      MANYCORE_LRC="lr_bigmem";
      for LR_PART in $MANYCORE_LRC; do
        echo -e "/usr/bin/sacctmgr -i modify user where name=$username account=$ACCOUNT partition=$LR_PART set qos=lr_normal,lr_debug" >> $filename
      done
      if [ "${ACCOUNT}" = "pc_heptheory" ]; then
        check_account $ACCOUNT
        echo -e "/usr/bin/sacctmgr -i add user $username  account=$ACCOUNT qos=lr_interactive partition=lr3_htc   # for the htc testbed" >> $filename
      fi
      ;;

    lr)
      if [ "${ACCOUNT}" == "lr_cumulus" ]; then
        check_account $ACCOUNT
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=condo_cumulus" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=condo_cumulus_lr6   # condo for David Romps" >> $filename
      elif [ "${ACCOUNT}" = "lr_mp" ] ; then
        check_account $ACCOUNT
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=condo_mp_lr2   # Condo for Kristin" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=cf1 qos=condo_mp_cf1   # Condo for Kristin" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=cf1-hp qos=condo_mp_cf1   # Condo for Kristin" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=es1 qos=condo_mp_es1   # Condo for Kristin" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=condo_mp_lr6   # condo for Sam Blau" >> $filename
      ## Condo for Krinith
      elif [ "${ACCOUNT}" = "lr_chandra" ] ; then
        check_account $ACCOUNT
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=es1 qos=condo_chandra_es1" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=csd_lr6_192 qos=condo_chandra_lr6" >> $filename
      ## Condo for Teresa Head-Gordon
      elif [ "${ACCOUNT}" = "lr_ninjaone" ] ; then
        check_account $ACCOUNT
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=cm2 qos=condo_ninjaone_cm2" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=csd_lr6_192 qos=condo_ninjaone" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=csd_lr6_share qos=condo_ninjaone_share" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=es1 qos=condo_ninjaone_es1" >> $filename

      elif [ "${ACCOUNT}" = "lr_amos" ] ; then
        check_account $ACCOUNT
        ## Robert  Lucchese/McCurdy 12 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr7 qos=condo_amos7_lr7,lr_lowprio" >> $filename
        ## Robert Lucchese  28 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=csd_lr6_192 qos=condo_amos,lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr5 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr3 qos=lr_lowprio" >> $filename

      elif [ "${ACCOUNT}" = "lr_essdata" ] ; then
        check_account $ACCOUNT
        ## Shreyas Cholia 8 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=condo_essdata_lr6,lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr5 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr3 qos=lr_lowprio" >> $filename

      elif [ "${ACCOUNT}" = "lr_mhg2" ] ; then
        check_account $ACCOUNT
        ## Martin Head-Gordon  28 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr7 qos=condo_mhg_lr7,lr_lowprio" >> $filename
        ## Martin Head Gordon  18 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=csd_lr6_192 qos=condo_mhg2,lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr5 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr3 qos=lr_lowprio" >> $filename

      elif [ "${ACCOUNT}" = "lr_rncstar" ] ; then
        check_account $ACCOUNT
        ## STAR/sPHENIX experiments @ RNC group  4 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr7 qos=condo_rncstar_lr7,lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr6 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr5 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr4 qos=lr_lowprio" >> $filename
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr3 qos=lr_lowprio" >> $filename

      elif [ "${ACCOUNT}" = "lr_nanotheory" ] ; then
        check_account $ACCOUNT
        ## David Prendergast 4 GPU Nodes: 4x A40 each (PO#7697311)
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=es1 qos=condo_nanotheory_es1,es_lowprio" >> $filename
        ## David Prendergast 4 nodes
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=lr3 qos=condo_nanotheory,lr_lowprio" >> $filename

      elif [ "${ACCOUNT}" = "lr_geop" ] ; then
        check_account $ACCOUNT

        # Nori Nakata 1 Chassis Dell C6520, 4 nodes 512GB nodes;  po_number=7721693; req#1000478231; LR7 Condo
        lowprio_partitions=$(echo lr{7..3})
        for lowprio_partition in $lowprio_partitions ; do
          echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=${lowprio_partition} qos=lr_lowprio" >> $filename
        done
        echo -e "/usr/bin/sacctmgr -i modify user $username account=$ACCOUNT partition=lr7 qos+=condo_geop_lr7" >> $filename

        # Nori Nakata 1 ea. Gigabyte GPU node, 4 x A40 GPU; po_number=7734064; req#1000478254; ES1 Condo
        echo -e "/usr/bin/sacctmgr -i create user $username account=$ACCOUNT partition=es1 qos=condo_geop_es1,es_lowprio" >> $filename

      else
        set_condo_partition $ACCOUNT
        check_account $ACCOUNT
        check_and_add_slurm_user $username
      fi
      ;;

    *)   echo "Accounts for Lawrencium or Mako must must begin with ac_, lr_, ld_, pc_ or scs.  Exiting"
      exit 10
      ;;
  esac

elif [ ${CLUSTER} = "californium" ] ; then
  first_2_char=`echo ${ACCOUNT} | awk -F\_ '{print $1}'`
  case $first_2_char in
    ac|scs|ld|pc)
      echo $ACCOUNT is ok ;
      PARTS="cf1"
      for i in $PARTS ; do
        set_general_partition  $i
        check_account $ACCOUNT
        check_and_add_slurm_user $username
      done
      ;;
    lr)
      set_condo_partition $ACCOUNT
      check_account $ACCOUNT
      check_and_add_slurm_user $username
      ;;
    *)
      echo "Accounts for Californium must must begin with ac_, lr_, ld_, pc_ or scs.  Exiting"
      exit 10
      ;;
  esac

elif  [ ${CLUSTER} = "nano" ] ; then
  PARTS="nano etna etna_gpu etna-shared etna_bigmem"
  for i in $PARTS ; do
    set_general_partition $i
    check_account $ACCOUNT
    check_and_add_slurm_user $username
  done
## VULCAN Cluster is decomissioned (Feb 2023)
#elif  [ ${CLUSTER} = "vulcan" ] ; then
#  PARTS="vulcan vulcan_c20 vulcan_gpu vulcan_c24"
#  for i in $PARTS ; do
#    set_general_partition $i
#    check_account $ACCOUNT
#    check_and_add_slurm_user $username
#  done
#
## MERGE etna partitions under nano since there isn't a posix group for etna
#elif  [ ${CLUSTER} = "etna" ] ; then
#  PARTS="etna etna_gpu etna-shared etna_bigmem"
#  for i in $PARTS ; do
#    set_general_partition $i
#    check_account $ACCOUNT
#    check_and_add_slurm_user $username
#  done

else
  set_general_partition ${CLUSTER}
  check_account $ACCOUNT
  check_and_add_slurm_user $username
fi

## END
############################################


## Do this later
## Now check to see if the user has been added to usrdata
## Master can't read usrdata_LRC file because its under accounts and it has no permission issue"
   ### usrdata_entry='grep $username /global/home/groups/scs/usrdata_LRC'
## if [[ -z $usrdata_entry ]]; then
##      echo "ERROR: Added user to SLURM with no usrdata_LRC entry"
## fi
