
safeRunCommand() {
  typeset cmnd="$*"
  typeset ret_code

  echo cmnd=$cmnd
  eval $cmnd
  ret_code=$?
  if [ $ret_code != 0 ]; then
    printf "Error : [%d] when executing command: '$cmnd'" $ret_code
    exit $ret_code
  else
    echo "WF killed successfully"
    echo $ret_code
  fi
}

if [ $# -ne 3 ];
then
   echo "Usage: killWFs.sh startwfnum endwfnum wf-suffix"
   echo "Example: ./killWFs.sh 1562 1565 -180827191610233-oozie-oozi-W"
   exit
fi

start=$1
end=$2
wfsuffix=$3

echo $1
echo $2

for i in $(seq -f "%07g" $start $end)
do
  wf="sudo -su pnda oozie job -oozie http://10.25.14.64:11000/oozie -kill "$i$wfsuffix
  echo $wf
  safeRunCommand $wf 
done
