env GOOS=linux GOARCH=amd64 go build -o bins/pql
echo "Built pql for Linux"
env GOOS=darwin GOARCH=amd64 go build -o bins/mpql
echo "Built mpql for Darwin/AMD64"
env GOOS=darwin GOARCH=arm64 go build -o bins/mapql
echo "Built mapql for Darwin/ARM64"
env GOOS=windows GOARCH=amd64 go build -o bins/pql.exe
echo "Built pql.exe for Windows"


cp bins/pql ~/bin