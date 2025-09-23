// Copyright (c) 2025 Cheejyg. All Rights Reserved.

package nem12

import (
	"math"
	"time"
)

var optimize = true

type floatInfo struct {
	mantbits uint
	expbits  uint
	bias     int
}

var float64info = floatInfo{52, 11, -1023}
var float64pow10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
	1e20, 1e21, 1e22,
}

func ParseByteString(bytes []byte) string {
	for i, b := range bytes {
		if b == 0x00 {
			return string(bytes[:i])
		}
	}

	return string(bytes)
}

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

func readFloat(b []byte) (mantissa uint64, exp int, trunc bool, i int, ok bool) {
	if i >= len(b) {
		return
	}
	if b[i] == '+' {
		i++
	}

	base := uint64(10)
	maxMantDigits := 19 // 10^19 fits in uint64
	sawdot := false
	sawdigits := false
	nd := 0
	ndMant := 0
	dp := 0
loop:
	for ; i < len(b); i++ {
		switch c := b[i]; true {
		case c == '.':
			if sawdot {
				break loop
			}
			sawdot = true
			dp = nd
			continue

		case '0' <= c && c <= '9':
			sawdigits = true
			if c == '0' && nd == 0 { // ignore leading zeros
				dp--
				continue
			}
			nd++
			if ndMant < maxMantDigits {
				mantissa *= base
				mantissa += uint64(c - '0')
				ndMant++
			} else if c != '0' {
				trunc = true
			}
			continue
		}
		break
	}
	if !sawdigits {
		return
	}
	if !sawdot {
		dp = nd
	}

	if mantissa != 0 {
		exp = dp - ndMant
	}

	ok = true
	return
}
func atof64exact(mantissa uint64, exp int) (f float64, ok bool) {
	if mantissa>>float64info.mantbits != 0 {
		return
	}
	f = float64(mantissa)
	switch {
	case exp == 0:
		// an integer.
		return f, true
	// Exact integers are <= 10^15.
	// Exact powers of ten are <= 10^22.
	case exp > 0 && exp <= 15+22: // int * 10^k
		// If exponent is big but number of digits is not, can move a few zeros into the integer part.
		if exp > 22 {
			f *= float64pow10[exp-22]
			exp = 22
		}
		if f > 1e15 || f < -1e15 {
			// the exponent was really too large.
			return
		}
		return f * float64pow10[exp], true
	case exp < 0 && exp >= -22: // int / 10^k
		return f / float64pow10[-exp], true
	}
	return
}
func ParseIntervalValue(intervalValue []byte) (float64, error) {
	mantissa, exp, trunc, _, ok := readFloat(intervalValue)
	if !ok {
		return 0, ErrInvalidIntervalValue
	}

	if optimize {
		// Try pure floating-point arithmetic conversion.
		if !trunc {
			if f, ok := atof64exact(mantissa, exp); ok {
				return f, nil
			}
		}
	}

	return float64(mantissa) * math.Pow10(exp), nil
}

func ParseHeaderRecord(record [][]byte) (headerRecord *HeaderRecord, err error) {
	headerRecord = &HeaderRecord{}

	copy(headerRecord.RecordIndicator[:], record[0])
	copy(headerRecord.VersionHeader[:], record[1])
	datetime, err := ParseDateTime12(string(record[2]))
	if err != nil {
		return nil, ErrInvalidDateTime
	}
	headerRecord.DateTime = datetime
	copy(headerRecord.FromParticipant[:], record[3])
	copy(headerRecord.ToParticipant[:], record[4])

	return
}
func ParseNmiDataDetailsRecord(record [][]byte) (nmiDataDetailsRecord *NmiDataDetailsRecord, err error) {
	nmiDataDetailsRecord = &NmiDataDetailsRecord{}

	copy(nmiDataDetailsRecord.RecordIndicator[:], record[0])
	copy(nmiDataDetailsRecord.Nmi[:], record[1])
	nmiDataDetailsRecord.NmiConfiguration = string(record[2])
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
		date, err := ParseDate8(string(record[9]))
		if err != nil {
			return nil, ErrInvalidDate
		}
		nmiDataDetailsRecord.NextScheduledReadDate = &date
	}

	return
}
func ParseIntervalDataRecord(record [][]byte, intervalLength int) (intervalDataRecord *IntervalDataRecord, err error) {
	intervalDataRecord = &IntervalDataRecord{}

	copy(intervalDataRecord.RecordIndicator[:], record[0])
	date, err := ParseDate8(string(record[1]))
	if err != nil {
		return nil, ErrInvalidDate
	}
	intervalDataRecord.IntervalDate = date

	n := 1440 / intervalLength
	intervalDataRecord.IntervalValue = make([]float64, n)
	for i := range n {
		intervalValue, err := ParseIntervalValue(record[2+i])
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
		reasonDescription := string(record[n+4])
		intervalDataRecord.ReasonDescription = &reasonDescription
	}
	if len(record) > n+5 && len(record[n+5]) > 0 {
		datetime, err := ParseDateTime14(string(record[n+5]))
		if err != nil {
			return nil, ErrInvalidDateTime
		}
		intervalDataRecord.UpdateDateTime = &datetime
	}
	if len(record) > n+6 && len(record[n+6]) > 0 {
		datetime, err := ParseDateTime14(string(record[n+6]))
		if err != nil {
			return nil, ErrInvalidDateTime
		}
		intervalDataRecord.MsatsLoadDateTime = &datetime
	}

	return
}
func ParseIntervalEventRecord(record [][]byte) (intervalEventRecord *IntervalEventRecord, err error) {
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
		reasonDescription := string(record[5])
		intervalEventRecord.ReasonDescription = &reasonDescription
	}

	return
}
func ParseB2bDetailsRecord(record [][]byte) (b2bDetailsRecord *B2bDetailsRecord, err error) {
	b2bDetailsRecord = &B2bDetailsRecord{}

	copy(b2bDetailsRecord.RecordIndicator[:], record[0])
	copy(b2bDetailsRecord.TransCode[:], record[1])
	if len(record) > 2 && len(record[2]) > 0 {
		b2bDetailsRecord.RetServiceOrder = &[15]byte{}
		copy(b2bDetailsRecord.RetServiceOrder[:], record[2])
	}
	if len(record) > 3 && len(record[3]) > 0 {
		datetime, err := ParseDateTime14(string(record[3]))
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
func ParseEndOfData(record [][]byte) (endOfData *EndOfData, err error) {
	endOfData = &EndOfData{}

	copy(endOfData.RecordIndicator[:], record[0])

	return
}
