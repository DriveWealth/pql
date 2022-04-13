package ddb

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
	"strings"
)

func ExtractAV(av types.AttributeValue) interface{} {
	switch t := av.(type) {
	case *types.AttributeValueMemberS:
		return FromString(t.Value)
		//return t.Value
	case *types.AttributeValueMemberN:
		return ToNumber(t.Value)
	case *types.AttributeValueMemberB:
		return t.Value
	case *types.AttributeValueMemberBOOL:
		return t.Value
	case *types.AttributeValueMemberNULL:
		return t.Value
	case *types.AttributeValueMemberSS:
		return t.Value
	case *types.AttributeValueMemberBS:
		return t.Value
	case *types.AttributeValueMemberNS:
		return ToNumbers(t.Value)
	case *types.AttributeValueMemberL:
		return AVToArray(t.Value)
	case *types.AttributeValueMemberM:
		return AVToMap(t.Value)

	}
	return nil
}

func ExtractItem(item map[string]types.AttributeValue) map[string]interface{} {
	m := make(map[string]interface{}, len(item))
	for k, v := range item {
		m[k] = ExtractAV(v)
	}
	return m
}

func AVToMap(avs map[string]types.AttributeValue) map[string]interface{} {
	size := len(avs)
	m := make(map[string]interface{}, size)
	for k, v := range avs {
		m[k] = ExtractAV(v)
	}
	return m
}

func AVToArray(avs []types.AttributeValue) []interface{} {
	size := len(avs)
	arr := make([]interface{}, size, size)
	for idx := 0; idx < size; idx++ {
		arr[idx] = ExtractAV(avs[idx])
	}
	return arr
}

func ToNumbers(s []string) []interface{} {
	size := len(s)
	arr := make([]interface{}, size, size)
	for idx := 0; idx < size; idx++ {
		arr[idx] = ToNumber(s[idx])
	}
	return arr
}

func ToNumber(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.Contains(s, ".") {
		if n, err := strconv.ParseFloat(s, 64); err != nil {
			return 0
		} else {
			return n
		}
	} else {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil {
			return 0
		} else {
			return n
		}
	}
}

func ToNumberOrErr(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	if strings.Contains(s, ".") {
		if n, err := strconv.ParseFloat(s, 64); err != nil {
			return nil, err
		} else {
			return n, nil
		}
	} else {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil {
			return nil, err
		} else {
			return n, nil
		}
	}
}

func FromString(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "{") {
		m := make(map[string]interface{})
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			return s
		} else {
			for k, v := range m {
				if iface, ok := v.(string); ok {
					if n, err := ToNumberOrErr(iface); err == nil {
						m[k] = n
					}
				}
			}
			return m
		}
	} else if strings.HasPrefix(s, "[") {
		arr := make([]interface{}, 0, 16)
		if err := json.Unmarshal([]byte(s), &arr); err != nil {
			return s
		} else {
			for idx, a := range arr {
				if iface, ok := a.(string); ok {
					if n, err := ToNumberOrErr(iface); err == nil {
						arr[idx] = n
					}
				}
			}
			return arr
		}

	}
	if n, err := ToNumberOrErr(s); err != nil {
		return s
	} else {
		return n
	}
}

func ExtractAVToString(av types.AttributeValue) string {
	switch t := av.(type) {
	case *types.AttributeValueMemberS:
		//return FromString(t.Value)
		return t.Value
	case *types.AttributeValueMemberN:
		return t.Value
		//return ToNumber(t.Value)
	case *types.AttributeValueMemberB:
		return string(t.Value)
		//return t.Value
	case *types.AttributeValueMemberBOOL:
		if t.Value {
			return "true"
		} else {
			return "false"
		}
		//return t.Value
	case *types.AttributeValueMemberNULL:
		return ""
	case *types.AttributeValueMemberSS:
		return strings.Join(t.Value, ",")
		//return t.Value
	case *types.AttributeValueMemberBS:
		size := len(t.Value)
		arr := make([]string, size, size)
		for idx := 0; idx < size; idx++ {
			arr[idx] = string(t.Value[idx])
		}
		return strings.Join(arr, ",")
	case *types.AttributeValueMemberNS:
		return strings.Join(t.Value, ",")
	case *types.AttributeValueMemberL:
		size := len(t.Value)
		arr := make([]string, size, size)
		for idx := 0; idx < size; idx++ {
			arr[idx] = ExtractAVToString(t.Value[idx])
		}
		return strings.Join(arr, ",")
	case *types.AttributeValueMemberM:
		size := len(t.Value)
		last := size - 1
		var b strings.Builder
		b.WriteString("{")
		idx := 0
		for k, v := range t.Value {
			b.WriteString(k)
			b.WriteString(":")
			b.WriteString(ExtractAVToString(v))
			if idx < last {
				b.WriteString(", ")
			}
			idx++
		}
		b.WriteString("}")
		return b.String()
	}
	return ""
}

func AVMapToStrStrMap(item map[string]types.AttributeValue) map[string]string {
	m := make(map[string]string, len(item))
	for k, v := range item {
		m[k] = ExtractAVToString(v)
	}
	return m
}
func AVMapToString(item map[string]types.AttributeValue) string {
	if b, err := json.Marshal(item); err != nil {
		return "ERROR: " + err.Error()
	} else {
		return string(b)
	}
}
