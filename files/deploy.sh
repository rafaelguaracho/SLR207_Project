#!/bin/bash
# A simple variable example
login="rsandrini-22"
port = "9976"
remoteFolder="/tmp/$login/"
fileName="SimpleServerProgram"
listenerFileName="ServerThreadListener"
fileExtension=".java"
computers=("tp-1a201-17.enst.fr" "tp-1a201-18.enst.fr" "tp-1a201-19.enst.fr" "tp-1a201-20.enst.fr" "tp-1a201-21.enst.fr")
#computers=("tp-1a226-01")
for c in ${computers[@]}; do
  command0=("ssh" "$login@$c" "lsof -ti | xargs kill -9")
  command1=("ssh" "$login@$c" "rm -rf $remoteFolder;mkdir $remoteFolder")
  command2=("scp" "$fileName$fileExtension" "$login@$c:$remoteFolder$fileName$fileExtension")
  command3=("scp" "$listenerFileName$fileExtension" "$login@$c:$remoteFolder$listenerFileName$fileExtension")
  command4=("ssh" "$login@$c" "cd $remoteFolder;javac $fileName$fileExtension;javac $listenerFileName$fileExtension;java $fileName $port")
  echo ${command0[*]}
  "${command0[@]}"
  echo ${command1[*]}
  "${command1[@]}"
  echo ${command2[*]}
  "${command2[@]}"
  echo ${command3[*]}
  "${command3[@]}" 
  echo ${command4[*]}
  "${command4[@]}" &
done