ip=("fa22-cs425-5501.cs.illinois.edu" "fa22-cs425-5502.cs.illinois.edu"
    "fa22-cs425-5503.cs.illinois.edu" "fa22-cs425-5504.cs.illinois.edu"
    "fa22-cs425-5505.cs.illinois.edu" "fa22-cs425-5506.cs.illinois.edu"
    "fa22-cs425-5507.cs.illinois.edu" "fa22-cs425-5508.cs.illinois.edu"
    "fa22-cs425-5509.cs.illinois.edu" "fa22-cs425-5510.cs.illinois.edu")
username=$1
for element in ${ip[*]}
do
ssh $username@$element "cd ./mp4/src; ./Process.sh 1>/dev/null 2>/dev/null &"
done
echo Process started