package refsequence

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"math/rand"
	"pql/ddb"
	"strings"
	"time"
)

var (
	A2Z              = []rune("ABCDEFGHJKMNPQRSTUVWXYZ")
	A2ZLast          = len(A2Z) - 1
	refSequenceTable = "ref.sequences"
	wlpsTable        = "bo.wlps"
	updatedNew       = "" + types.ReturnValueUpdatedNew
)

type RefSequence struct {
	dbClient   *dynamodb.Client
	wlpDecodes map[string]string
}

func loadWlps(dbClient *dynamodb.Client) (map[string]string, error) {
	m := make(map[string]string, 256)
	var startKey map[string]types.AttributeValue = nil
	scanRequest := &dynamodb.ScanInput{
		TableName:         &wlpsTable,
		AttributesToGet:   []string{"wlpID", "prefix"},
		ExclusiveStartKey: startKey,
	}
	for {
		if out, err := dbClient.Scan(context.Background(), scanRequest); err != nil {
			return nil, err
		} else {
			for _, item := range out.Items {
				wlpAv := ddb.ExtractAV(item["wlpID"])
				prefAv := ddb.ExtractAV(item["prefix"])

				wlp := wlpAv.(string)
				pref := ""
				switch prefAv.(type) {
				case string:
					pref = prefAv.(string)
					break
				case int64:
					pref = fmt.Sprintf("%02d", pref)
					break
				}
				//log.Printf("WLP: %s\n", wlp)

				m[wlp] = pref
			}
			if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
				break
			} else {
				scanRequest.ExclusiveStartKey = out.LastEvaluatedKey
			}
		}
	}
	log.Printf("Loaded %d Wlps\n", len(m))
	return m, nil
}

func NewRefSequence(dbClient *dynamodb.Client) (*RefSequence, error) {
	if m, err := loadWlps(dbClient); err != nil {
		return nil, err
	} else {
		return &RefSequence{dbClient: dbClient, wlpDecodes: m}, nil
	}
}

func Shard(monthly bool) string {
	var b strings.Builder
	if monthly {
		dp := NewDatePart(time.Now())
		b.WriteString(dp.YearCode())
		b.WriteString(dp.MonthCode())
		b.WriteRune(A2Z[rand.Intn(A2ZLast)])
		b.WriteRune(A2Z[rand.Intn(A2ZLast)])
	} else {
		b.WriteRune(A2Z[rand.Intn(A2ZLast)])
		b.WriteRune(A2Z[rand.Intn(A2ZLast)])
		//b.WriteRune(A2Z[rand.Intn(A2ZLast)])
		//b.WriteRune(A2Z[rand.Intn(A2ZLast)])
	}
	return b.String()
}

func (r *RefSequence) GetRefSequences(shard string, count int) ([]string, error) {
	arr := make([]string, count, count)
	shardID := Shard(true)
	sequenceKey := shard + "_" + shardID
	avUpdate := types.AttributeValueUpdate{
		Action: "ADD",
		Value:  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", count)},
	}
	key := map[string]types.AttributeValue{
		"sequenceName": &types.AttributeValueMemberS{Value: sequenceKey},
	}
	updates := map[string]types.AttributeValueUpdate{
		"nextNo": avUpdate,
	}
	updateRequest := &dynamodb.UpdateItemInput{
		Key:              key,
		TableName:        &refSequenceTable,
		AttributeUpdates: updates,
		ReturnValues:     types.ReturnValueUpdatedNew,
	}
	if out, err := r.dbClient.UpdateItem(context.Background(), updateRequest); err != nil {
		return nil, err
	} else {
		av := out.Attributes["nextNo"]
		nextSeq := int(ddb.ExtractAV(av).(int64))
		next := int(nextSeq)
		for idx := 0; idx < count; idx++ {
			seq := next - idx
			arr[idx] = fmt.Sprintf("%s%06d", shardID, seq)
		}
		return arr, nil
	}

}

func (r *RefSequence) GetRefSequencesWithWLP(shard, wlpID string, count int, monthly bool) ([]string, error) {
	wlpPrefix := ""
	if p, ok := r.wlpDecodes[wlpID]; !ok {
		return nil, errors.New("No WLP Decode for wlpID [" + wlpID + "]")
	} else {
		wlpPrefix = p
	}
	arr := make([]string, count, count)
	shardID := Shard(monthly)
	sequenceKey := shard + "_" + wlpPrefix + shardID
	avUpdate := types.AttributeValueUpdate{
		Action: "ADD",
		Value:  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", count)},
	}
	key := map[string]types.AttributeValue{
		"sequenceName": &types.AttributeValueMemberS{Value: sequenceKey},
	}
	updates := map[string]types.AttributeValueUpdate{
		"nextNo": avUpdate,
	}
	updateRequest := &dynamodb.UpdateItemInput{
		Key:              key,
		TableName:        &refSequenceTable,
		AttributeUpdates: updates,
		ReturnValues:     types.ReturnValueUpdatedNew,
	}
	if out, err := r.dbClient.UpdateItem(context.Background(), updateRequest); err != nil {
		return nil, err
	} else {
		av := out.Attributes["nextNo"]
		nextSeq := int(ddb.ExtractAV(av).(int64))
		next := int(nextSeq)
		for idx := 0; idx < count; idx++ {
			seq := next - idx
			arr[idx] = fmt.Sprintf("%s%s%06d", wlpPrefix, shardID, seq)
		}
		return arr, nil
	}
}
