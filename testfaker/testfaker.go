package main

import (
	"regexp"
)

var (
	r  = regexp.MustCompile(`##(.*?)##`)
	r2 = regexp.MustCompile(`##(?P<Name>.*?)\((?P<Args>.*)\)##`)
)

func main() {
	//v := "First: ##first-name##,  Last: ##last-name##  (and again: ##first-name##, ##last-name##) and an op: ##accountNo(CITI, 43)##"
	//allMatches := r.FindAllString(v, -1)
	//for _, match := range allMatches {
	//	fmt.Printf("Initial Match: [%s]\n", match)
	//	arr := r2.FindAllStringSubmatch(match, -1)
	//	if len(arr) > 0 {
	//		args := arr[0][2:]
	//		fmt.Printf("SubMatch Match: key=[%s] op=[%s], args=[%s]\n", arr[0][0], arr[0][1], args)
	//	}
	//}

}
