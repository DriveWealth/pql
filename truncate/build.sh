env GOOS=linux GOARCH=amd64 go build -o ../bins/ddbtruncate
echo "Built ddbtruncate for Linux"
env GOOS=darwin GOARCH=amd64 go build -o ../bins/mddbtruncate
echo "Built mddbtruncate for Darwin/AMD64"
env GOOS=darwin GOARCH=arm64 go build -o ../bins/maddbtruncate
echo "Built maddbtruncate for Darwin/ARM64"
env GOOS=windows GOARCH=amd64 go build -o ../bins/ddbtruncate.exe
echo "Built ddbtruncate.exe for Windows"


cp ../bins/ddbtruncate ~/bin