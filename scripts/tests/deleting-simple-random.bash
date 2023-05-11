#!/bin/bash -e

###
#
# Insert ${TARGET} k/v pairs, then delete random of them
#
# Useful when checking that delete is working properly
#
###

echo "Executing test $0"

TARGET=100

for ((i=0;i<TARGET;i++)); 
do 
   key=$(printf "key_%04d" $i)
   value=$(printf "value_%04d" $i)
   if ((i==0)); then
       target/release/baildon-store -c default.db insert "${key}" "${value}"
   else
       target/release/baildon-store default.db insert "${key}" "${value}"
   fi
done

for ((i=TARGET-1;i>=0;i--)); 
do 
   num=$((RANDOM % TARGET))
   key=$(printf "key_%04d" $num)
   echo "BEFORE DELETE key: ${key}"
   echo "BEFORE DELETE nodes"
   target/release/baildon-store default.db nodes
   target/release/baildon-store default.db delete "${key}"
   echo "AFTER DELETE nodes"
   target/release/baildon-store default.db nodes
   echo "AFTER DELETE verify"
   target/release/baildon-store default.db verify
done
