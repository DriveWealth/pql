package util

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bcicen/jstream"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func IsDirectory(name string) bool {
	if name == "" {
		return false
	}
	fileInfo, err := os.Stat(name)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()
}

func CreateDirectory(name string) error {
	return os.Mkdir(name, os.ModeDir)
}

func IsFile(name string) bool {
	if name == "" {
		return false
	}
	fileInfo, err := os.Stat(name)
	if err != nil {
		return false
	}
	return !fileInfo.IsDir()
}

func CanCreateFile(name string) bool {
	var f *os.File = nil
	var ferr error = nil
	if name == "" {
		return false
	}
	defer func() {
		if f != nil {
			os.Remove(name)
		}
	}()
	f, ferr = os.Create(name)
	if ferr != nil {
		return false
	}
	return true
}


func NewAddress(port int, iface string) string {
	return fmt.Sprintf("%s:%d", iface, port)
}

func ValidateListener(port int, iface string) error {
	var server *http.Server

	defer func() {
		if server != nil {
			server.Close()
		}
	}()
	addr := NewAddress(port, iface)
	server = &http.Server{Addr: addr, Handler: nil}
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err = server.ListenAndServe()
		if err != nil {
			// ?
		}
	}()
	for i := 0; i < 5; i++ {
		if IsHttpSocketActive(port, iface) {
			return nil
		}
		time.Sleep(time.Millisecond * 500)
	}
	return errors.New(fmt.Sprintf("Failed to validate listener: %s", addr))
}

func IsValidListener(port int, iface string) bool {
	err := ValidateListener(port, iface)
	if err != nil {
		return false
	}
	return true
}

func IsUnixSocket(fileName string) bool {
	var err error
	var fileInfo os.FileInfo
	if !IsFile(fileName) {
		return false
	}
	fileInfo, err = os.Stat(fileName)
	if err != nil {
		return false
	}
	fileMode := fileInfo.Mode()
	ms := os.ModeSocket
	m := fileMode&ms
	if m != ms {
		return false
	}
	return true
}

func IsUnixSocketActive(fileName string) bool {
	var c net.Conn
	var err error
	if !IsUnixSocket(fileName) {
		return false
	}
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	c, err = net.Dial("unix", fileName)
	return err == nil
}

func IsHttpSocketActive(port int, address string) bool {
	var c net.Conn
	var err error
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	c, err = net.Dial("tcp", NewAddress(port, address))
	return err == nil
}



func ToTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	utcLong := GetNumberOrNil(value)
	if utcLong == nil {
		fromStr, err := time.Parse(http.TimeFormat, value)
		if err != nil {
			return nil
		}
		return &fromStr
	} else {
		fromInt := time.Unix(0, *utcLong * int64(time.Millisecond))
		return &fromInt
	}
}

func GetNumberOrNil(value string) *int64 {
	if value == "" {
		return nil
	}
	i,err := strconv.Atoi(value)
	if err != nil {
		return nil
	}
	i64 := int64(i)
	return &i64
}

func IsFileGzipped(fileName string) bool {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()
	buff := make([]byte, 6)

	_, err = file.Read(buff)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filetype := http.DetectContentType(buff)

	return "application/x-gzip" == filetype;

}

func Env(defaultValue string, keys ...string) string {
	var value string
	for _,key := range keys {
		value = os.Getenv(key)
		if value != "" {
			return value
		}
	}
	return defaultValue
}

const FLOAT_ZERO float64 = 0


func RateSec(elapsedSecs int64, events int64) float64 {
	if events == 0 {
		return FLOAT_ZERO
	}
	if elapsedSecs == 0 {
		return float64(events)
	}
	return float64(events) / float64(elapsedSecs)
}

func PerSec(elapsedSecs int64, events int) float64 {
	if elapsedSecs == 0 || events == 0 {
		return FLOAT_ZERO
	}
	return  float64(elapsedSecs) / float64(events)
}

func StreamUnmarshal(r io.Reader, emitDepth int, factory func() encoding.BinaryUnmarshaler) ([]interface{}, error) {
	decoder := jstream.NewDecoder(r, emitDepth)
	arr := make([]interface{}, 0)
	for mv := range decoder.Stream() {
		bytes, erx := json.MarshalIndent(mv.Value, "", " ")
		if erx != nil {
			return nil, erx
		}
		fmt.Printf("MANIFEST: %s\n", string(bytes))
		item := factory()
		json.Unmarshal(bytes, &item)
		arr = append(arr, item)
	}
	return arr, nil
}
