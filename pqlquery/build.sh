env GOOS=linux GOARCH=amd64 go build -o ../bins/pqlquery
echo "Built pqlquery for Linux"
env GOOS=darwin GOARCH=amd64 go build -o ../bins/mpqlquery
echo "Built mpqlquery for Darwin/AMD64"
env GOOS=darwin GOARCH=arm64 go build -o ../bins/mapqlquery
echo "Built mapqlquery for Darwin/ARM64"

env GOOS=windows GOARCH=amd64 go build -o ../bins/pqlquery.exe
echo "Built pqlquery.exe for Windows"


cp ../bins/pqlquery ~/bin