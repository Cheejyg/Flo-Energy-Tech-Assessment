package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/Cheejyg/Flo-Energy-Tech-Assessment/nem12"
)

var sep []byte = []byte{','}

func processLine(line []byte, intervalLength *int) {
	record := bytes.Split(line, sep)
	switch {
	case bytes.Equal(record[0], nem12.RecordIndicatorHeaderBytes):
		headerRecord, err := nem12.ParseHeaderRecord(record)
		if err != nil {
			return
		}

		fmt.Println(headerRecord)
	case bytes.Equal(record[0], nem12.RecordIndicatorNmiDataDetailsBytes):
		nmiDataDetailsRecord, err := nem12.ParseNmiDataDetailsRecord(record)
		if err != nil {
			return
		}

		i, err := strconv.Atoi(nem12.ParseByteString(nmiDataDetailsRecord.IntervalLength[:]))
		if err != nil {
			return
		}
		*intervalLength = i

		fmt.Println(nmiDataDetailsRecord)
	case bytes.Equal(record[0], nem12.RecordIndicatorIntervalDataBytes):
		intervalDataRecord, err := nem12.ParseIntervalDataRecord(record, *intervalLength)
		if err != nil {
			return
		}

		fmt.Println(intervalDataRecord)
	case bytes.Equal(record[0], nem12.RecordIndicatorIntervalEventBytes):
		intervalEventRecord, err := nem12.ParseIntervalEventRecord(record)
		if err != nil {
			return
		}

		fmt.Println(intervalEventRecord)
	case bytes.Equal(record[0], nem12.RecordIndicatorB2bDetailsBytes):
		b2bDetailsRecord, err := nem12.ParseB2bDetailsRecord(record)
		if err != nil {
			return
		}

		fmt.Println(b2bDetailsRecord)
	case bytes.Equal(record[0], nem12.RecordIndicatorEndOfDataBytes):
		endOfData, err := nem12.ParseEndOfData(record)
		if err != nil {
			return
		}

		fmt.Println(endOfData)
	}
}

func main() {
	name := "NEM12#200506081149#UNITEDDP#NEMMCO.csv"
	nem12File, err := os.Open(name)
	if err != nil {
		return
	}
	defer nem12File.Close()

	bufferedReader := bufio.NewReader(nem12File)
	var bufferedLine []byte

	var intervalLength int
loop:
	for {
		bufferedLine = bufferedLine[:0]
		for {
			line, isPrefix, err := bufferedReader.ReadLine()
			if err != nil {
				if err == io.EOF {
					if len(bufferedLine) > 0 {
						processLine(bufferedLine, &intervalLength)
					}

					break loop
				} else {
					return
				}
			}
			bufferedLine = append(bufferedLine, line...)

			if !isPrefix {
				processLine(bufferedLine, &intervalLength)

				break
			}
		}
	}
}
