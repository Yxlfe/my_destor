#!/bin/bash
echo "lyh123456" | sudo -S ./rebuild
FLODER="/home/$USER/linuxkernel"
LINE="`ls $FLODER | wc -l`"
ls $FLODER | for (( i=1;i<$LINE;i++ ))
do
	read FILENAME
	echo "lyh123456" | sudo -S destor "$FLODER/$FILENAME"

done
