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

const sqlInsertBatchSize int = 16_384
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
	var stringBuilder strings.Builder
	stringBuilder.Grow(128)

	stringBuilder.WriteString("INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ('")
	stringBuilder.WriteString(meterReadingsJob.Nmi)
	stringBuilder.WriteString("','")
	stringBuilder.WriteString(meterReadingsJob.Timestamp.Format(sqlTimestampLayout))
	stringBuilder.WriteString("',")
	stringBuilder.WriteString(strconv.FormatFloat(meterReadingsJob.Consumption, 'f', -1, 64))
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
		stringBuilder.WriteString(strconv.FormatFloat(meterReadingsJob[i].Consumption, 'f', -1, 64))
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
		writer.WriteString(strconv.FormatFloat(meterReadingsJob[i].Consumption, 'f', -1, 64))
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
		writer.WriteString(strconv.FormatFloat(meterReadingsJob[i].Consumption, 'f', -1, 64))

		if i < len(meterReadingsJob)-1 {
			writer.WriteByte('\n')
		}
	}
}

func processLine(line []byte, nmi *string, intervalLength *int) {
	record := bytes.Split(line, sep)
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
					log.Fatalln(err)
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

	if len(sqlInsertBatch) > 0 {
		writeInsertStatements(sqlInsertBufferedWriter, sqlInsertBatch)
		writeCopyStatements(sqlCopyBufferedWriter, sqlInsertBatch)
	}
}
