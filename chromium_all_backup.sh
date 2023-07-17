#!/bin/bash
echo "lyh123456" | sudo -S ./rebuild #格式化
FLODER="/home/$USER/chromium"
LINE="`ls -v $FLODER | wc -l`"
ls -v $FLODER | for (( i=1;i<=$LINE;i++ ))
do
        read FILENAME
        echo "lyh123456" | sudo -S destor "$FLODER/$FILENAME"  #备份写入
        echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache asm 6 0"
        echo "lyh123456" | sudo -S destor -r$[i-1] "/home/$USER/restore_file/5" -p"restore-cache my-asm 6 50 0"

done
