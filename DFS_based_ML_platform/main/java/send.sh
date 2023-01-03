des=$1
local_file=$2
sdfs_file=$3
cd local_files
scp -r $local_file yangt2@$des:/home/yangt2/mp4/src/main/java/SDFS_files/$sdfs_file
echo "files are put on $des"