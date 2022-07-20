#set -x
File=${1}
awk -F, '{print $2","$3","$4","$5","$6","$7","$8","$9","$10","$11","$12","$13","$14","$15","$16","$17","$18}' OpC_Msg_${File}.csv|egrep  'after'|sed "s/^/,,/"|sort|uniq -c
for i in `ls Op[CUD]_Msg_${File}.csv`
do
 awk -F, '{print $2","$3","$4","$5","$6","$7","$8","$9","$10","$11","$12","$13","$14","$15","$16","$17","$18}' ${i}|egrep  -v 'after'|sort| sed "s/^/,${i},/" |uniq -c
done
