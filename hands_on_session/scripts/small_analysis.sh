#!/bin/bash
# vim:ft=awk

echo

if [ "$#" -lt 3 ] ; then
    echo "Usage: ./script <topology> <trajectory> <CORRECTED_exp-noe-data (turner-noe_corr)> <CORRECTED_exp-unoe-data (turner-unoe_corr)>"
    echo "Corrected - atom labelling, i.e., H2' --> H2'1, HO2' --> HO'2 ect."
    echo
    exit 1
fi

top=$1
traj=$2

nice -n 19 /tmp/cpptraj/bin/cpptraj $top nmr_noe_ptraj.in
wait