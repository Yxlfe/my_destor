#!/bin/bash
echo "lyh123456" | sudo -S ./rebuild
FLODER="/home/$USER/linuxkernel"
LINE="`ls -v $FLODER | wc -l`"
ls -v $FLODER | for (( i=1;i<$LINE;i++ ))
do
	read FILENAME
	echo "lyh123456" | sudo -S destor "$FLODER/$FILENAME"
	echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache asm 6 0"
	echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache my-asm 6 50 0"
	echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache lru 6 0"
	echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache opt 6 0"

done
