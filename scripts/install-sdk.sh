echo Installing "kb-sdk"...
echo
docker pull kbase/kb-sdk
docker run kbase/kb-sdk genscript > kb-sdk
chmod u+x kb-sdk
echo 
echo To make the installed kb-sdk available, enter the following into your shell: 
echo 
echo export PATH=\$PWD:\$PATH
echo
