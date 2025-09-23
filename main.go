package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Cheejyg/Flo-Energy-Tech-Assessment/nem12"
)

const sqlTimestampLayout string = "2006-01-02 15:04:05" // YYYY-MM-DD HH:MM:SS

var sep []byte = []byte{','}

type MeterReadingsJob struct {
	Nmi         string
	Timestamp   time.Time
	Consumption float64
}
type IntervalDataJob struct {
	Nmi            string
	IntervalDate   time.Time
	IntervalLength time.Duration
	IntervalValue  []float64
}

var meterReadingsJobChan chan MeterReadingsJob = make(chan MeterReadingsJob, 2048)
var meterReadingsJobWaitGroup sync.WaitGroup

var intervalDataJobWorkers int = 8
var intervalDataJobChan chan IntervalDataJob = make(chan IntervalDataJob, 4096)
var intervalDataJobWaitGroup sync.WaitGroup

func generateInsertStatement(meterReadingsJob MeterReadingsJob) string {
	return fmt.Sprintf("INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ('%s', '%s', %s);", meterReadingsJob.Nmi, meterReadingsJob.Timestamp.Format(sqlTimestampLayout), strconv.FormatFloat(meterReadingsJob.Consumption, 'f', -1, 64))
}

func processLine(line []byte, nmi *string, intervalLength *int) {
	record := bytes.Split(line, sep)
	switch {
	case bytes.Equal(record[0], nem12.RecordIndicatorHeaderBytes):
		break
	case bytes.Equal(record[0], nem12.RecordIndicatorNmiDataDetailsBytes):
		nmiDataDetailsRecord, err := nem12.ParseNmiDataDetailsRecord(record)
		if err != nil {
			return
		}

		*nmi = nem12.ParseByteString(nmiDataDetailsRecord.Nmi[:])

		i, err := strconv.Atoi(nem12.ParseByteString(nmiDataDetailsRecord.IntervalLength[:]))
		if err != nil {
			return
		}
		*intervalLength = i
	case bytes.Equal(record[0], nem12.RecordIndicatorIntervalDataBytes):
		intervalDataRecord, err := nem12.ParseIntervalDataRecord(record, *intervalLength)
		if err != nil {
			return
		}

		intervalDataJobChan <- IntervalDataJob{
			Nmi:            *nmi,
			IntervalDate:   intervalDataRecord.IntervalDate,
			IntervalLength: time.Duration(*intervalLength) * time.Minute,
			IntervalValue:  intervalDataRecord.IntervalValue,
		}
	case bytes.Equal(record[0], nem12.RecordIndicatorIntervalEventBytes):
		break
	case bytes.Equal(record[0], nem12.RecordIndicatorB2bDetailsBytes):
		break
	case bytes.Equal(record[0], nem12.RecordIndicatorEndOfDataBytes):
		return
	default:
		break
	}
}

func main() {
	name := "NEM12#200506081149#UNITEDDP#NEMMCO.csv"
	nem12File, err := os.Open(name)
	if err != nil {
		return
	}
	defer nem12File.Close()

	go func() {
		for meterReadingsJob := range meterReadingsJobChan {
			fmt.Println(generateInsertStatement(meterReadingsJob))

			meterReadingsJobWaitGroup.Done()
		}
	}()

	intervalDataJobWaitGroup.Add(intervalDataJobWorkers)
	for range intervalDataJobWorkers {
		go func() {
			defer intervalDataJobWaitGroup.Done()
			for intervalDataJob := range intervalDataJobChan {
				meterReadingsJobWaitGroup.Add(len(intervalDataJob.IntervalValue))
				timestamp := intervalDataJob.IntervalDate.Add(intervalDataJob.IntervalLength)
				for i := range intervalDataJob.IntervalValue {
					meterReadingsJobChan <- MeterReadingsJob{
						Nmi:         intervalDataJob.Nmi,
						Timestamp:   timestamp,
						Consumption: intervalDataJob.IntervalValue[i],
					}
					timestamp = timestamp.Add(intervalDataJob.IntervalLength)
				}
			}
		}()
	}

	bufferedReader := bufio.NewReader(nem12File)
	var bufferedLine []byte

	var nmi string
	var intervalLength int
loop:
	for {
		bufferedLine = bufferedLine[:0]
		for {
			line, isPrefix, err := bufferedReader.ReadLine()
			if err != nil {
				if err == io.EOF {
					if len(bufferedLine) > 0 {
						processLine(bufferedLine, &nmi, &intervalLength)
					}

					break loop
				} else {
					return
				}
			}
			bufferedLine = append(bufferedLine, line...)

			if !isPrefix {
				processLine(bufferedLine, &nmi, &intervalLength)

				break
			}
		}
	}

	close(intervalDataJobChan)
	intervalDataJobWaitGroup.Wait()

	close(meterReadingsJobChan)
	meterReadingsJobWaitGroup.Wait()
}
