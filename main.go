// Copyright (c) 2025 Cheejyg. All Rights Reserved.

package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Cheejyg/Flo-Energy-Tech-Assessment/nem12"
)

const COMMA = ','
const sqlInsertBatchSize int = 16_384
const sqlTimestampLayout string = "2006-01-02 15:04:05" // YYYY-MM-DD HH:MM:SS
const intervalDataJobWorkers int = 8

var sep []byte = []byte{COMMA}

type MeterReadingsJob struct {
	Nmi         string
	Timestamp   time.Time
	Consumption []byte
}
type IntervalDataJob struct {
	Nmi            string
	IntervalDate   time.Time
	IntervalLength time.Duration
	IntervalValue  [][]byte
}

var meterReadingsJobChan chan MeterReadingsJob = make(chan MeterReadingsJob, 2048)
var meterReadingsJobWaitGroup sync.WaitGroup

var intervalDataJobChan chan IntervalDataJob = make(chan IntervalDataJob, 4096)
var intervalDataJobWaitGroup sync.WaitGroup

func generateInsertStatement(meterReadingsJob MeterReadingsJob) string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(128)

	stringBuilder.WriteString("INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ('")
	stringBuilder.WriteString(meterReadingsJob.Nmi)
	stringBuilder.WriteString("','")
	stringBuilder.WriteString(meterReadingsJob.Timestamp.Format(sqlTimestampLayout))
	stringBuilder.WriteString("',")
	stringBuilder.Write(meterReadingsJob.Consumption)
	stringBuilder.WriteString(");")

	return stringBuilder.String()
}
func generateInsertStatements(meterReadingsJob []MeterReadingsJob) string {
	var stringBuilder strings.Builder
	stringBuilder.Grow((2 + len(meterReadingsJob)) * 64)

	stringBuilder.WriteString("INSERT INTO meter_readings (nmi, timestamp, consumption)\n  VALUES\n")

	for i := range meterReadingsJob {
		stringBuilder.WriteString("    ('")
		stringBuilder.WriteString(meterReadingsJob[i].Nmi)
		stringBuilder.WriteString("','")
		stringBuilder.WriteString(meterReadingsJob[i].Timestamp.Format(sqlTimestampLayout))
		stringBuilder.WriteString("',")
		stringBuilder.Write(meterReadingsJob[i].Consumption)
		if i < len(meterReadingsJob)-1 {
			stringBuilder.WriteString("),\n")
		}
	}

	stringBuilder.WriteString(");")

	return stringBuilder.String()
}
func writeInsertStatements(writer *bufio.Writer, meterReadingsJob []MeterReadingsJob) {
	defer writer.Flush()

	writer.WriteString("INSERT INTO meter_readings (nmi, timestamp, consumption)\n  VALUES\n")

	for i := range meterReadingsJob {
		writer.WriteString("    ('")
		writer.WriteString(meterReadingsJob[i].Nmi)

		writer.WriteString("','")
		writer.WriteString(meterReadingsJob[i].Timestamp.Format(sqlTimestampLayout))
		writer.WriteString("',")
		// writer.WriteString("',to_timestamp(")
		// writer.WriteString(strconv.FormatInt(meterReadingsJob[i].Timestamp.Unix(), 10))
		// writer.WriteString("),")

		writer.Write(meterReadingsJob[i].Consumption)
		if i < len(meterReadingsJob)-1 {
			writer.WriteString("),\n")
		}
	}

	writer.WriteString(");\n")
}
func writeCopyStatements(writer *bufio.Writer, meterReadingsJob []MeterReadingsJob) {
	defer writer.Flush()

	for i := range meterReadingsJob {
		writer.WriteString(meterReadingsJob[i].Nmi)
		writer.WriteByte(',')
		writer.WriteString(meterReadingsJob[i].Timestamp.Format(sqlTimestampLayout))
		writer.WriteByte(',')
		writer.Write(meterReadingsJob[i].Consumption)
		if i < len(meterReadingsJob)-1 {
			writer.WriteByte('\n')
		}
	}

	writer.WriteString("\n")
}

func lineSplit(line *[]byte, sep byte, intervalLength *int) (record [][]byte) {
	switch {
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorHeaderBytes):
		record = make([][]byte, 1, 5)
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorNmiDataDetailsBytes):
		record = make([][]byte, 1, 3)
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorIntervalDataBytes):
		record = make([][]byte, 1, 7 + 1440 / *intervalLength)
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorIntervalEventBytes):
		record = make([][]byte, 1, 4)
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorB2bDetailsBytes):
		record = make([][]byte, 1, 2)
	case bytes.Equal((*line)[0:3], nem12.RecordIndicatorEndOfDataBytes):
		record = make([][]byte, 1)
	default:
		record = make([][]byte, 1, 10)
	}

	record[0] = (*line)[0:3]

	var left, right int
	for left, right = 4, 4; right < len(*line); right++ {
		if (*line)[right] == sep {
			record = append(record, (*line)[left:right])
			left = right + 1
		}
	}

	record = append(record, bytes.TrimRight((*line)[left:], "\r\n"))

	return
}
func processLine(line *[]byte, nmi *string, intervalLength *int) {
	record := lineSplit(line, COMMA, intervalLength)
	switch {
	case bytes.Equal(record[0], nem12.RecordIndicatorHeaderBytes):
		break
	case bytes.Equal(record[0], nem12.RecordIndicatorNmiDataDetailsBytes):
		nmiDataDetailsRecord, err := nem12.ParseNmiDataDetailsRecord(record)
		if err != nil {
			log.Fatalln(err)
			return
		}

		*nmi = nem12.ParseByteString(nmiDataDetailsRecord.Nmi[:])

		i, err := strconv.Atoi(nem12.ParseByteString(nmiDataDetailsRecord.IntervalLength[:]))
		if err != nil {
			log.Fatalln(err)
			return
		}
		*intervalLength = i
	case bytes.Equal(record[0], nem12.RecordIndicatorIntervalDataBytes):
		intervalDataRecord, err := nem12.ParseIntervalDataRecord(record, *intervalLength)
		if err != nil {
			log.Fatalln(err)
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
		log.Fatalln(err)
		return
	}
	defer nem12File.Close()

	sqlInsertFileName := strings.ReplaceAll(name, ".csv", "") + ".sql"
	sqlCopyFileName := "meter_readings.sql.csv"

	sqlInsertFile, err := os.Create(sqlInsertFileName)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer sqlInsertFile.Close()

	sqlCopyFile, err := os.Create(sqlCopyFileName)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer sqlCopyFile.Close()

	sqlInsertBufferedWriter := bufio.NewWriterSize(sqlInsertFile, 1<<20)
	defer sqlInsertBufferedWriter.Flush()

	sqlCopyBufferedWriter := bufio.NewWriterSize(sqlCopyFile, 1<<20)
	defer sqlCopyBufferedWriter.Flush()

	sqlInsertBatch := make([]MeterReadingsJob, 0, sqlInsertBatchSize)
	go func() {
		for meterReadingsJob := range meterReadingsJobChan {
			sqlInsertBatch = append(sqlInsertBatch, meterReadingsJob)

			if len(sqlInsertBatch) >= sqlInsertBatchSize {
				writeInsertStatements(sqlInsertBufferedWriter, sqlInsertBatch)
				writeCopyStatements(sqlCopyBufferedWriter, sqlInsertBatch)
				sqlInsertBatch = sqlInsertBatch[:0]
			}

			meterReadingsJobWaitGroup.Done()
		}
		// if len(sqlInsertBatch) > 0 {
		// 	fmt.Println(generateInsertStatements(sqlInsertBatch))
		// }
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

	bufferedReader := bufio.NewReaderSize(nem12File, 1<<20)
	var bufferedLine []byte

	var nmi string
	var intervalLength int
	for {
		bufferedLine, err = bufferedReader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				if len(bufferedLine) > 0 {
					processLine(&bufferedLine, &nmi, &intervalLength)
				}

				break
			} else {
				log.Fatalln(err)
				return
			}
		}

		processLine(&bufferedLine, &nmi, &intervalLength)
	}

	close(intervalDataJobChan)
	intervalDataJobWaitGroup.Wait()

	close(meterReadingsJobChan)
	meterReadingsJobWaitGroup.Wait()

	if len(sqlInsertBatch) > 0 {
		writeInsertStatements(sqlInsertBufferedWriter, sqlInsertBatch)
		writeCopyStatements(sqlCopyBufferedWriter, sqlInsertBatch)
	}
}
