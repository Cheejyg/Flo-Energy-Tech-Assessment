package nem12

import (
	"strconv"
	"time"
)

func ParseDate8(date string) (time.Time, error) {
	layout := "20060102" // CCYYMMDD
	return time.Parse(layout, date)
}
func ParseDateTime12(datetime string) (time.Time, error) {
	layout := "200601021504" // CCYYMMDDhhmm
	return time.Parse(layout, datetime)
}
func ParseDateTime14(datetime string) (time.Time, error) {
	layout := "20060102150405" // CCYYMMDDhhmmss
	return time.Parse(layout, datetime)
}

func ParseHeaderRecord(record []string) (headerRecord *HeaderRecord, err error) {
	headerRecord = &HeaderRecord{}

	copy(headerRecord.RecordIndicator[:], record[0])
	copy(headerRecord.VersionHeader[:], record[1])
	datetime, err := ParseDateTime12(record[2])
	if err != nil {
		return nil, ErrInvalidDateTime
	}
	headerRecord.DateTime = datetime
	copy(headerRecord.FromParticipant[:], record[3])
	copy(headerRecord.ToParticipant[:], record[4])

	return
}
func ParseNmiDataDetailsRecord(record []string) (nmiDataDetailsRecord *NmiDataDetailsRecord, err error) {
	nmiDataDetailsRecord = &NmiDataDetailsRecord{}

	copy(nmiDataDetailsRecord.RecordIndicator[:], record[0])
	copy(nmiDataDetailsRecord.Nmi[:], record[1])
	nmiDataDetailsRecord.NmiConfiguration = record[2]
	if len(record[3]) > 0 {
		nmiDataDetailsRecord.RegisterId = &[10]byte{}
		copy(nmiDataDetailsRecord.RegisterId[:], record[3])
	}
	copy(nmiDataDetailsRecord.NmiSuffix[:], record[4])
	if len(record[5]) > 0 {
		nmiDataDetailsRecord.MdmDataStreamIdentifier = &[2]byte{}
		copy(nmiDataDetailsRecord.MdmDataStreamIdentifier[:], record[5])
	}
	if len(record[6]) > 0 {
		nmiDataDetailsRecord.MeterSerialNumber = &[12]byte{}
		copy(nmiDataDetailsRecord.MeterSerialNumber[:], record[6])
	}
	copy(nmiDataDetailsRecord.Uom[:], record[7])
	copy(nmiDataDetailsRecord.IntervalLength[:], record[8])
	if len(record) > 9 && len(record[9]) > 0 {
		date, err := ParseDate8(record[9])
		if err != nil {
			return nil, ErrInvalidDate
		}
		nmiDataDetailsRecord.NextScheduledReadDate = &date
	}

	return
}
func ParseIntervalDataRecord(record []string, intervalLength int) (intervalDataRecord *IntervalDataRecord, err error) {
	intervalDataRecord = &IntervalDataRecord{}

	copy(intervalDataRecord.RecordIndicator[:], record[0])
	date, err := ParseDate8(record[1])
	if err != nil {
		return nil, ErrInvalidDate
	}
	intervalDataRecord.IntervalDate = date

	n := 1440 / intervalLength
	intervalDataRecord.IntervalValue = make([]float64, n)
	for i := range n {
		intervalValue, err := strconv.ParseFloat(record[2+i], 64)
		if err != nil {
			return nil, err
		}
		intervalDataRecord.IntervalValue[i] = intervalValue
	}

	copy(intervalDataRecord.QualityMethod[:], record[n+2])
	if len(record) > n+3 && len(record[n+3]) > 0 {
		intervalDataRecord.ReasonCode = &[3]byte{}
		copy(intervalDataRecord.ReasonCode[:], record[n+3])
	}
	if len(record) > n+4 && len(record[n+4]) > 0 {
		intervalDataRecord.ReasonDescription = &record[n+4]
	}
	if len(record) > n+5 && len(record[n+5]) > 0 {
		datetime, err := ParseDateTime14(record[n+5])
		if err != nil {
			return nil, ErrInvalidDateTime
		}
		intervalDataRecord.UpdateDateTime = &datetime
	}
	if len(record) > n+6 && len(record[n+6]) > 0 {
		datetime, err := ParseDateTime14(record[n+6])
		if err != nil {
			return nil, ErrInvalidDateTime
		}
		intervalDataRecord.MsatsLoadDateTime = &datetime
	}

	return
}
func ParseIntervalEventRecord(record []string) (intervalEventRecord *IntervalEventRecord, err error) {
	intervalEventRecord = &IntervalEventRecord{}

	copy(intervalEventRecord.RecordIndicator[:], record[0])
	copy(intervalEventRecord.StartInterval[:], record[1])
	copy(intervalEventRecord.EndInterval[:], record[2])
	copy(intervalEventRecord.QualityMethod[:], record[3])
	if len(record) > 4 && len(record[4]) > 0 {
		intervalEventRecord.ReasonCode = &[3]byte{}
		copy(intervalEventRecord.ReasonCode[:], record[4])
	}
	if len(record) > 5 && len(record[5]) > 0 {
		intervalEventRecord.ReasonDescription = &record[5]
	}

	return
}
func ParseB2bDetailsRecord(record []string) (b2bDetailsRecord *B2bDetailsRecord, err error) {
	b2bDetailsRecord = &B2bDetailsRecord{}

	copy(b2bDetailsRecord.RecordIndicator[:], record[0])
	copy(b2bDetailsRecord.TransCode[:], record[1])
	if len(record) > 2 && len(record[2]) > 0 {
		b2bDetailsRecord.RetServiceOrder = &[15]byte{}
		copy(b2bDetailsRecord.RetServiceOrder[:], record[2])
	}
	if len(record) > 3 && len(record[3]) > 0 {
		datetime, err := ParseDateTime14(record[3])
		if err != nil {
			return nil, err
		}
		b2bDetailsRecord.ReadDateTime = &datetime
	}
	if len(record) > 4 && len(record[4]) > 0 {
		b2bDetailsRecord.IndexRead = &[15]byte{}
		copy(b2bDetailsRecord.IndexRead[:], record[4])
	}

	return
}
func ParseEndOfData(record []string) (endOfData *EndOfData, err error) {
	endOfData = &EndOfData{}

	copy(endOfData.RecordIndicator[:], record[0])

	return
}
