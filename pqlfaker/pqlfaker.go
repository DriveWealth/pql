package pqlfaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jaswdr/faker"
	"log"
	"math/rand"
	"pql/ddb"
	"pql/instrument"
	"pql/refsequence"
	"regexp"
	"strings"
	"time"
)

const (
	ISO_FORMAT = "2006-02-01T15:04:05.000Z"
)

type NoParamFunc func() string

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	accountsTable = "bo.accounts"
	usersTable    = "bo.users"

	r        = regexp.MustCompile(`##(.*?)##`)
	rparamed = regexp.MustCompile(`##(?P<Name>.*?)\((?P<Args>.*)\)##`)

	subst = make(map[string]NoParamFunc)
)

type Faker struct {
	dbClient    *dynamodb.Client
	refSeq      *refsequence.RefSequence
	instruments *instrument.InstrumentLoader
	dbFakers    map[string]func(state map[string]map[string]string, args ...string) string
	noParamOps  map[string]NoParamFunc
	f           faker.Faker
}

func (f *Faker) uuidAV(keys ...string) map[string]types.AttributeValue {
	size := len(keys)
	m := make(map[string]types.AttributeValue, size)
	for _, key := range keys {
		m[key] = &types.AttributeValueMemberS{Value: f.f.UUID().V4()}
	}
	return m
}

func randomSide() string {
	side := ""
	if rand.Intn(1) == 0 {
		side = "B"
	} else {
		side = "S"
	}
	return side
}

func randomDuration() time.Duration {
	sign := ""
	if rand.Intn(1) == 0 {
		sign = "-"
	} else {
		sign = "+"
	}
	d, _ := time.ParseDuration(sign + fmt.Sprintf("%ds", rand.Intn(100000)))
	return d
}

func (f *Faker) randomAccount(dbClient *dynamodb.Client, excludeAccountIDs ...string) (map[string]string, error) {
	exSize := len(excludeAccountIDs)
	excluded := make(map[string]bool, exSize)
	for idx := 0; idx < exSize; idx++ {
		excluded[excludeAccountIDs[idx]] = true
	}

	if out, err := dbClient.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:         &accountsTable,
		ExclusiveStartKey: f.uuidAV("accountID", "userID"),
	}); err != nil {
		return nil, err
	} else {
		//m := make(map[string]string, len(out.))
		for _, item := range out.Items {
			accountIDAV := ddb.ExtractAVToString(item["accountID"])
			if _, ok := excluded[accountIDAV]; !ok {
				m := make(map[string]string, len(item))
				for k, v := range item {
					m[k] = ddb.ExtractAVToString(v)
				}
				return m, nil
			}

		}
	}
	return nil, errors.New("No random account found")
}

func (f *Faker) randomUser(dbClient *dynamodb.Client, excludeUserIDs ...string) (map[string]string, error) {
	exSize := len(excludeUserIDs)
	excluded := make(map[string]bool, exSize)
	for idx := 0; idx < exSize; idx++ {
		excluded[excludeUserIDs[idx]] = true
	}

	if out, err := dbClient.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:         &usersTable,
		ExclusiveStartKey: f.uuidAV("userID"),
	}); err != nil {
		return nil, err
	} else {
		//m := make(map[string]string, len(out.))
		for _, item := range out.Items {
			accountIDAV := ddb.ExtractAVToString(item["userID"])
			if _, ok := excluded[accountIDAV]; !ok {
				m := make(map[string]string, len(item))
				for k, v := range item {
					m[k] = ddb.ExtractAVToString(v)
				}
				return m, nil
			}

		}
	}
	return nil, errors.New("No random user found")
}

//func buildStatefulOps(dbClient *dynamodb.Client) map[string]func(args ...string) map[string]string {
//	return map[string]func(args ...string) map[string]string{
//		"randomAccount": func(args ...string) map[string]string {
//			a, _ := randomAccount(dbClient, args...) // FIXME: what do we do with the error ?
//			return a
//		},
//		"randomUser": func(args ...string) map[string]string {
//			a, _ := randomUser(dbClient, args...)
//			return a
//		},
//	}
//}

func (f *Faker) buildOps(refSeq *refsequence.RefSequence, dbClient *dynamodb.Client) map[string]func(state map[string]map[string]string, args ...string) string {
	return map[string]func(state map[string]map[string]string, args ...string) string{
		"newUserAccountField": func(state map[string]map[string]string, args ...string) string {
			wlp := strings.TrimSpace(args[0])
			fieldName := strings.TrimSpace(args[1])

			var acc map[string]string
			var ok bool
			acc, ok = state["new-user-account"]
			if !ok {
				acc = make(map[string]string, 3)
				uid := f.f.UUID().V4()
				ts := fmt.Sprintf("%d", AbsInt64(f.f.Int64()))
				acc["userID"] = uid + "." + ts
				acc["accountID"] = uid
				a, _ := refSeq.GetRefSequencesWithWLP("accountNo", wlp, 1, false)
				acc["accountNo"] = a[0]
				state["new-user-account"] = acc
			}
			if v, o := acc[fieldName]; o {
				return v
			} else {
				return ""
			}
		},
		"accountNo": func(state map[string]map[string]string, args ...string) string {
			a, _ := refSeq.GetRefSequencesWithWLP("accountNo", args[0], 1, false)
			return a[0]
		},
		"orderNo": func(state map[string]map[string]string, args ...string) string {
			a, _ := refSeq.GetRefSequences("orderNo", 1)
			return a[0]
		},
		"accountField": func(state map[string]map[string]string, args ...string) string {
			fieldName := strings.TrimSpace(args[0])
			excluded := args[1:]
			var acc map[string]string
			var ok bool
			acc, ok = state["random-account"]
			if !ok {
				acc, _ = f.randomAccount(dbClient, excluded...)
				if acc != nil {
					state["random-account"] = acc
				}
			}
			if "*" == fieldName {
				if b, err := json.Marshal(acc); err != nil {
					return "ERROR: " + err.Error()
				} else {
					return string(b)
				}
			}
			if v, o := acc[fieldName]; o {
				return v
			} else {
				return ""
			}
		},
		"isotime": func(state map[string]map[string]string, args ...string) string {
			durationModifier, terr := time.ParseDuration(strings.ToLower(strings.TrimSpace(args[0])))
			if terr != nil {
				log.Fatalf("Failed to parse duration: [%s]\n", args[0])
			}
			if len(args) > 1 {
				key := strings.TrimSpace(args[1])
				var acc map[string]string
				var ok bool
				acc, ok = state[key]
				if !ok {
					v := time.Now().Add(durationModifier).Format(ISO_FORMAT)
					acc = map[string]string{"paramed-iso-time": v}
					state[key] = acc
					return v
				} else {
					return acc["paramed-iso-time"]
				}
			} else {
				return time.Now().Add(durationModifier).Format(ISO_FORMAT)
			}
		},
		"userField": func(state map[string]map[string]string, args ...string) string {
			fieldName := strings.TrimSpace(args[0])
			excluded := args[1:]
			var acc map[string]string
			var ok bool
			acc, ok = state["random-user"]
			if !ok {
				acc, _ = f.randomUser(dbClient, excluded...)
				if acc != nil {
					state["random-user"] = acc
				}
			}
			if "*" == fieldName {
				if b, err := json.Marshal(acc); err != nil {
					return "ERROR: " + err.Error()
				} else {
					return string(b)
				}
			}
			if v, o := acc[fieldName]; o {
				return v
			} else {
				return ""
			}
		},
		"instrumentField": func(state map[string]map[string]string, args ...string) string {
			fieldName := strings.TrimSpace(args[0])
			//excluded := args[1:]
			var acc map[string]string
			var ok bool
			acc, ok = state["random-instrument"]
			if !ok {
				acc := f.instruments.RandomInstrument().ToMap()
				if acc != nil {
					state["random-instrument"] = acc
				}
			}
			if "*" == fieldName {
				if b, err := json.Marshal(acc); err != nil {
					return "ERROR: " + err.Error()
				} else {
					return string(b)
				}
			}
			if v, o := acc[fieldName]; o {
				return v
			} else {
				return ""
			}
		},
	}
}

func NewFaker(dbClient *dynamodb.Client) (*Faker, error) {
	if ref, err := refsequence.NewRefSequence(dbClient); err != nil {
		return nil, err
	} else {
		fk := &Faker{
			f:           faker.NewWithSeed(rand.NewSource(rand.Int63())),
			dbClient:    dbClient,
			refSeq:      ref,
			instruments: instrument.NewInstrumentLoader(dbClient).Load(),
		}
		fk.dbFakers = fk.buildOps(ref, dbClient)
		fk.noParamOps = fk.buildNoParamOps()
		return fk, nil
	}
}

func (f *Faker) buildNoParamOps() map[string]NoParamFunc {
	return map[string]NoParamFunc{
		"##firstname##": func() string {
			return f.f.Person().FirstName()
		},
		"##lastname##": func() string {
			return f.f.Person().LastName()
		},
		"##streetaddress##": func() string {
			return f.f.Address().StreetAddress()
		},
		"##city##": func() string {
			return f.f.Address().City()
		},
		"##state##": func() string {
			return f.f.Address().State()
		},
		"##zip##": func() string {
			return f.f.Address().PostCode()
		},
		"##company##": func() string {
			return f.f.Company().Name()
		},
		"##title##": func() string {
			return f.f.Company().JobTitle()
		},
		"##phone##": func() string {
			return f.f.Phone().Number()
		},
		"##isotime##": func() string {
			return time.Now().Add(randomDuration()).Format(ISO_FORMAT)
		},
		"##uuid##": func() string {
			return f.f.UUID().V4()
		},
		"##int##": func() string {
			return fmt.Sprintf("%d", AbsInt(f.f.Int()))
		},
		"##long##": func() string {
			return fmt.Sprintf("%d", AbsInt64(f.f.Int64()))
		},
		"##float##": func() string {
			return fmt.Sprintf("%d", AbsFloat32(f.f.Float32(8, 0, 99999999)))
		},
		"##bool##": func() string {
			return fmt.Sprintf("%t", f.f.Bool())
		},
		"##gender##": func() string {
			return f.f.Gender().Name()
		},
		"##email##": func() string {
			return f.f.Internet().Email()
		},
		"##userid##": func() string {
			return f.f.UUID().V4()
		},
		"##accountID##": func() string {
			return f.f.UUID().V4() + "." + fmt.Sprintf("%d", AbsInt64(f.f.Int64()))
		},
		"##monthcode##": func() string {
			return refsequence.CurrentMonthCode()
		},
		"##yearcode##": func() string {
			return refsequence.CurrentYearCode()
		},
		"##fintranID##": func() string {
			return refsequence.CurrentYearCode() + refsequence.CurrentMonthCode() + "." + f.f.UUID().V4()
		},
		"##orderID##": func() string {
			return refsequence.CurrentYearCode() + refsequence.CurrentMonthCode() + "." + f.f.UUID().V4()
		},
		"##side##": func() string {
			return randomSide()
		},
		"##symbol##": func() string {
			s := f.instruments.RandomSymbol()
			if s == nil {
				return ""
			}
			return *s
		},
		"##instrumentID##": func() string {
			s := f.instruments.RandomID()
			if s == nil {
				return ""
			}
			return *s
		},
	}
}

// func (f *Faker) Substitute(line *string, passThrough map[string]string) (*string, map[string]string) {
func (f *Faker) Substitute(line *string) *string {
	updated := *line
	matches := r.FindAllString(*line, -1)
	state := make(map[string]map[string]string, 5)
	uniqueMatches := make(map[string]string, len(matches))
	for _, match := range matches {
		if nop, ok := f.noParamOps[match]; ok {
			s := nop()
			updated = strings.ReplaceAll(updated, match, s)
			continue
		}
		dbOp := rparamed.FindAllStringSubmatch(match, -1)
		if len(dbOp) == 0 {
			if _, ok := uniqueMatches[match]; !ok {
				if fx, ok := subst[match]; ok {
					value := fx()
					updated = strings.Replace(updated, match, value, -1)
					uniqueMatches[match] = value
				}
			}
		} else {
			if _, ok := uniqueMatches[match]; !ok {
				for _, op := range dbOp {
					if len(op) >= 2 {
						key := op[0]
						opName := op[1]
						args := mergeArgs(op[2:])

						if fx, ok := f.dbFakers[opName]; ok {
							value := fx(state, args...)
							updated = strings.Replace(updated, key, value, -1)
							uniqueMatches[match] = value
						}
					}
				}
			}
		}
	}
	return &updated
}

func mergeArgs(arr []string) []string {
	a := make([]string, 0)
	for _, v := range arr {
		subarr := strings.Split(v, ",")
		for _, subv := range subarr {
			subv := strings.TrimSpace(subv)
			if subv != "" {
				a = append(a, subv)
			}
		}
	}
	return a
}

func AbsInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func AbsInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func AbsFloat32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
