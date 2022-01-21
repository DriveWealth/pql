package ec2

import (
	"bufio"
	"os"
	"strings"
)

const (
	EC2_HYPERVISOR_FILE 	= "/sys/hypervisor/uuid"
	EC2_UUID_PREFIX 		= "ec2"
)

func OnEC2() bool {
	file, err := os.Open(EC2_HYPERVISOR_FILE)
	if err != nil {
		return false
	}
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return false
	}
	return strings.HasPrefix(scanner.Text(), EC2_UUID_PREFIX)
}
