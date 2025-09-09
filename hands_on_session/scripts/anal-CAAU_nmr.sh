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
exp_noe=$3
exp_unoe=$4

rm nmr_noe_ptraj.in nmr_unoe_ptraj.in result_noes.dat result_unoes.dat exp_NOES_fin result_noes_diff.dat result_unoes_diff.dat nmr_md.dat
touch nmr_noe_ptraj.in nmr_unoe_ptraj.in result_noes.dat result_unoes.dat exp_NOES_fin result_noes_diff.dat result_unoes_diff.dat nmr_md.dat

# prepare to grep distances for NOEs --> generate input for cpptraj
echo "trajin $traj" > nmr_noe_ptraj.in
grep -v \# $exp_noe | awk '{printf("distance d%d :%s@%s :%s@%s\n",NR,$1,$2,$3,$4)} END{printf("run\nwritedata md-dist-4-noes.dat "); for(i=1;i<=NR;i++) printf("d%d ",i); printf("\n");}' >> nmr_noe_ptraj.in

# prepare to grep distances for uNOEs --> generate input for cpptraj
echo "trajin $traj" > nmr_unoe_ptraj.in
grep -v \# $exp_unoe | awk '{printf("distance d%d :%s@%s :%s@%s\n",NR,$1,$2,$3,$4)} END{printf("run\nwritedata md-dist-4-unoes.dat "); for(i=1;i<=NR;i++) printf("d%d ",i); printf("\n");}' >> nmr_unoe_ptraj.in

# getting MD data --> run ccptraj program
nice -n 19 cpptraj $top nmr_noe_ptraj.in
wait
nice -n 19 cpptraj $top nmr_unoe_ptraj.in
wait
nice -n 19 cpptraj $top grep-3J-cop.in
wait

# merging 3J-couplings
paste jcop_1-H1H2.dat jcop_1-H2H3.dat jcop_1-H3H4.dat jcop_2-H1H2.dat jcop_2-H2H3.dat jcop_2-H3H4.dat jcop_3-H1H2.dat jcop_3-H2H3.dat jcop_3-H3H4.dat jcop_4-H1H2.dat jcop_4-H2H3.dat | awk '{if(NR!=1) {for(i=2;i<=NF;i+=2) printf("%s ",$i); printf("\n")}}' > jcop_sugar.dat

paste jcop_1-1H5H4.dat jcop_1-2H5H4.dat jcop_1-H3P.dat jcop_2-1H5H4.dat jcop_2-2H5H4.dat jcop_2-1H5P.dat jcop_2-2H5P.dat jcop_2-H3P.dat jcop_3-1H5H4.dat jcop_3-2H5H4.dat jcop_3-1H5P.dat jcop_3-2H5P.dat jcop_3-H3P.dat jcop_4-1H5H4.dat jcop_4-2H5H4.dat jcop_4-1H5P.dat jcop_4-2H5P.dat | awk '{if(NR!=1) {for(i=2;i<=NF;i+=2) printf("%s ",$i); printf("\n")}}' > jcop_bb.dat

# calculate 3J-sugar and 3-backbone couplings from dihedrals
awk '{for(i=1;i<=NF;i++) sum[i]+=(9.67*(cos($i*3.1415926535/180))^2-2.03*(cos($i*3.1415926535/180)))} END{for(i=1;i<=NF;i++) printf "%f\n", sum[i]/NR}' jcop_sugar.dat > result_jcoupl-sugar.dat
awk '{for(i=1;i<=NF;i++) if((i==1)||(i==2)||(i==4)||(i==5)||(i==9)||(i==10)||(i==14)||(i==15)) sum[i]+=(9.7*(cos($i*3.1415926535/180))^2-1.8*(cos($i*3.1415926535/180))); else sum[i]+=(15.3*(cos($i*3.1415926535/180))^2-6.1*(cos($i*3.1415926535/180))+1.6)} END{for(i=1;i<=NF;i++) printf "%f\n", sum[i]/NR}' jcop_bb.dat > result_jcoupl-bb.dat

# NOE, analyze each column
grep -v \# md-dist-4-noes.dat | awk '{for(i=2;i<=NF;i++) sum[i]+=(($i)^(-6));} END {for(i=2; i<=NF; i++) printf "%s %1.1f\n", "column_"i, (sum[i]/NR)^(-1/6)}' > result_noes.dat 

grep -v \# $exp_noe | awk '{x=$7-$6; if($6-$5<x) x=$6-$5; print $1"_"$2, $3"_"$4, $6, x}' > exp_NOES_fin

# uNOE, analyze each column
grep -v \# md-dist-4-unoes.dat | awk '{for(i=2;i<=NF;i++) sum[i]+=(($i)^(-6));} END {for(i=2; i<=NF; i++) printf "%s %1.1f\n", "column_"i, (sum[i]/NR)^(-1/6)}' > result_unoes.dat

# Data analysis, Exp. vs. MD
rm -f *_diff.dat

# averaging
nice -n 19 paste result_jcoupl-sugar.dat exp_jcoupl-sugar | awk '{print $2, (($1-$3)/$4)^2}' > result_jcoupl-sugar_diff.dat
nice -n 19 paste result_jcoupl-bb.dat exp_jcoupl-bb | awk '{print $2, (($1-$3)/$4)^2}' > result_jcoupl-bb_diff.dat

paste result_noes.dat exp_NOES_fin | awk '{print $3"_"$4, (($2-$5)/$6)^2}' > result_noes_diff.dat

paste result_unoes.dat exp_unoes | awk '{if($2>$5) print $1, "0.0"; else printf "%s %1.1f\n", $1, (($2-$5)/$6)^2}' > result_unoes_diff.dat

# comparing MD vs Exp, getting chi**2
nice -n 19 awk '{sum+=$2; n++} END{print "3J_bb", sum/n, n}' result_jcoupl-bb_diff.dat >> nmr_md.dat
nice -n 19 awk '{sum+=$2; n++} END{print "3J_sugar", sum/n, n}' result_jcoupl-sugar_diff.dat >> nmr_md.dat
nice -n 19 awk '{sum+=$2; n++} END{print "NOE", sum/n, n}' result_noes_diff.dat >> nmr_md.dat
nice -n 19 awk '{sum+=$2; n++} END{print "uNOE", sum/n, n}' result_unoes_diff.dat >> nmr_md.dat
awk '{sum+=$2*$3; n+=$3} END{print "chi2", sum/n}' nmr_md.dat >> nmr_md.dat

