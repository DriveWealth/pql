package refsequence

import "time"

const (
	ISO_FORMAT = "2006-01-02T15:04:05.999Z"
)

type DatePart struct {
	TimestampStr string
	Timestamp    time.Time
}

func NewDatePartFromString(ts string) (*DatePart, error) {
	if t, err := time.Parse(ISO_FORMAT, ts); err != nil {
		return nil, err
	} else {
		return &DatePart{
			TimestampStr: ts,
			Timestamp:    t,
		}, nil
	}
}

func NewDatePart(ts time.Time) *DatePart {
	return &DatePart{
		TimestampStr: ts.Format(ISO_FORMAT),
		Timestamp:    ts,
	}
}

func CurrentMonthCode() string {
	return MonthCode(NewDatePart(time.Now()).MonthInt())
}

func CurrentYearCode() string {
	return YearCode(NewDatePart(time.Now()).YearInt())
}

func (d *DatePart) String() string {
	return d.TimestampStr
}

func (d *DatePart) MonthInt() int {
	return int(d.Timestamp.Month())
}

func (d *DatePart) MonthCode() string {
	return MonthCode(int(d.Timestamp.Month()))
}

func (d *DatePart) YearCode() string {
	return YearCode(d.Timestamp.Year())
}

func (d *DatePart) YearInt() int {
	return d.Timestamp.Year()
}

func MonthCode(month int) string {
	monthCode := "X"
	switch month {
	case 1:
		monthCode = "A"
		break
	case 2:
		monthCode = "B"
		break
	case 3:
		monthCode = "C"
		break
	case 4:
		monthCode = "D"
		break
	case 5:
		monthCode = "E"
		break
	case 6:
		monthCode = "F"
		break
	case 7:
		monthCode = "G"
		break
	case 8:
		monthCode = "H"
		break
	case 9:
		monthCode = "I"
		break
	case 10:
		monthCode = "J"
		break
	case 11:
		monthCode = "K"
		break
	case 12:
		monthCode = "L"
		break
	default:
		monthCode = "X"
		break
	}
	return monthCode
}

func YearCode(year int) string {
	yearCode := "X"
	switch year {
	case 2013:
		yearCode = "A"
		break
	case 2014:
		yearCode = "B"
		break
	case 2015:
		yearCode = "C"
		break
	case 2016:
		yearCode = "D"
		break
	case 2017:
		yearCode = "E"
		break
	case 2018:
		yearCode = "F"
		break
	case 2019:
		yearCode = "G"
		break
	case 2020:
		yearCode = "H"
		break
	case 2021:
		yearCode = "I"
		break
	case 2022:
		yearCode = "J"
		break
	case 2023:
		yearCode = "K"
		break
	case 2024:
		yearCode = "L"
		break
	default:
		yearCode = "X"
		break
	}
	return yearCode
}
