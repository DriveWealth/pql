package instrument

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"math/rand"
	"pql/ddb"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LOAD_ROUTINES     = int32(32)
	COL_INSTR_ID      = "instrumentID"
	COL_SYMBOL        = "symbol"
	COL_INSTR_TYPE_ID = "instrumentTypeID"
	COL_TPLUS         = "tPlus"
	COL_T_STATUS      = "tradeStatus"
)

var (
	tableName      = aws.String("ref.instruments")
	projectionsArr = []string{COL_INSTR_ID, COL_SYMBOL, COL_INSTR_TYPE_ID, COL_TPLUS, COL_T_STATUS}
	projections    = aws.String(strings.Join(projectionsArr, ","))
)

type Instrument struct {
	InstrumentID     string
	Symbol           string
	InstrumentTypeID int
	TPlus            int
	TradeStatus      int
}

func (i *Instrument) ToMap() map[string]string {
	return map[string]string{
		COL_INSTR_ID:      i.InstrumentID,
		COL_SYMBOL:        i.Symbol,
		COL_INSTR_TYPE_ID: fmt.Sprintf("%d", i.InstrumentTypeID),
		COL_TPLUS:         fmt.Sprintf("%d", i.TPlus),
		COL_T_STATUS:      fmt.Sprintf("%d", i.TradeStatus),
	}
}

type InstrumentLoader struct {
	dbClient            *dynamodb.Client
	instrumentsByID     map[string]Instrument
	instrumentsBySymbol map[string]Instrument
	instrumentsBySeq    map[int32]Instrument
	lock                *sync.Mutex
	seq                 *int32
	loaded              int32
}

func NewInstrumentLoader(client *dynamodb.Client) *InstrumentLoader {
	var l sync.Mutex
	return &InstrumentLoader{
		dbClient:            client,
		instrumentsByID:     make(map[string]Instrument, 10000),
		instrumentsBySymbol: make(map[string]Instrument, 10000),
		instrumentsBySeq:    make(map[int32]Instrument, 10000),
		lock:                &l,
		seq:                 new(int32),
		loaded:              0,
	}
}

func (i *InstrumentLoader) Load() *InstrumentLoader {
	startTime := time.Now()
	scans := make([]*dynamodb.ScanInput, LOAD_ROUTINES, LOAD_ROUTINES)
	routines := int(LOAD_ROUTINES)
	totalRoutines := aws.Int32(LOAD_ROUTINES)
	var wg sync.WaitGroup
	wg.Add(routines)
	for idx := 0; idx < routines; idx++ {
		scans[idx] = &dynamodb.ScanInput{
			TableName:            tableName,
			ProjectionExpression: projections,
			Segment:              aws.Int32(int32(idx)),
			TotalSegments:        totalRoutines,
			//FilterExpression:     filter,
			//ScanFilter: map[string]types.Condition{
			//	"symbol": types.Condition{
			//		ComparisonOperator: types.ComparisonOperatorNotNull,
			//	},
			//},

		}
		go i.loadSegment(scans[idx], &wg)
	}
	wg.Wait()
	i.loaded = int32(len(i.instrumentsBySeq))
	log.Printf("Instruments Loaded: count=%d, elapsed=%s\n", len(i.instrumentsBySymbol), time.Since(startTime).String())
	return i
}

func (i *InstrumentLoader) loadSegment(scanInput *dynamodb.ScanInput, wg *sync.WaitGroup) {
	for {
		if out, err := i.dbClient.Scan(context.Background(), scanInput); err != nil {
			log.Fatalf("Failed to load instruments: error=%s\n", err.Error())
		} else {
			if out.Count > 0 {
				for _, item := range out.Items {
					if _, ok := item[COL_SYMBOL]; !ok {
						continue
					}
					var symbol string
					symbolAV := ddb.ExtractAV(item[COL_SYMBOL])
					switch t := symbolAV.(type) {
					case string:
						symbol = t
					case int64:
						symbol = fmt.Sprintf("%d", t)
					}
					instr := Instrument{
						InstrumentID:     ddb.ExtractAV(item[COL_INSTR_ID]).(string),
						Symbol:           symbol,
						InstrumentTypeID: int(ddb.ExtractAV(item[COL_INSTR_TYPE_ID]).(int64)),
						TPlus:            int(ddb.ExtractAV(item[COL_TPLUS]).(int64)),
						TradeStatus:      int(ddb.ExtractAV(item[COL_T_STATUS]).(int64)),
					}
					i.index(instr)
				}
				if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
					break
				} else {
					scanInput.ExclusiveStartKey = out.LastEvaluatedKey
				}

			}
		}
	}
	wg.Done()
}

func (i *InstrumentLoader) index(instr Instrument) {
	id := atomic.AddInt32(i.seq, 1)
	i.lock.Lock()
	defer i.lock.Unlock()
	i.instrumentsByID[instr.InstrumentID] = instr
	i.instrumentsBySymbol[instr.Symbol] = instr
	i.instrumentsBySeq[id] = instr
}

func (i *InstrumentLoader) RandomInstrument() *Instrument {
	instr, ok := i.instrumentsBySeq[rand.Int31n(i.loaded-1)]
	if ok {
		return &instr
	}
	return nil
}

func (i *InstrumentLoader) RandomSymbol() *string {
	instr, ok := i.instrumentsBySeq[rand.Int31n(i.loaded-1)]
	if ok {
		return &instr.Symbol
	}
	return nil
}

func (i *InstrumentLoader) RandomID() *string {
	instr, ok := i.instrumentsBySeq[rand.Int31n(i.loaded-1)]
	if ok {
		return &instr.InstrumentID
	}
	return nil
}

func (i *InstrumentLoader) ForID(id string) *Instrument {
	instr, ok := i.instrumentsByID[id]
	if ok {
		return &instr
	}
	return nil
}

func (i *InstrumentLoader) ForSymbol(symbol string) *Instrument {
	instr, ok := i.instrumentsBySymbol[symbol]
	if ok {
		return &instr
	}
	return nil
}
