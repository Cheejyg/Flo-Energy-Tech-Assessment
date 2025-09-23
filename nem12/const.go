// Copyright (c) 2025 Cheejyg. All Rights Reserved.

package nem12

const (
	RecordIndicatorHeaderString         = "100"
	RecordIndicatorNmiDataDetailsString = "200"
	RecordIndicatorIntervalDataString   = "300"
	RecordIndicatorIntervalEventString  = "400"
	RecordIndicatorB2bDetailsString     = "500"
	RecordIndicatorEndOfDataString      = "900"
)

var (
	RecordIndicatorHeaderBytes         = []byte(RecordIndicatorHeaderString)
	RecordIndicatorNmiDataDetailsBytes = []byte(RecordIndicatorNmiDataDetailsString)
	RecordIndicatorIntervalDataBytes   = []byte(RecordIndicatorIntervalDataString)
	RecordIndicatorIntervalEventBytes  = []byte(RecordIndicatorIntervalEventString)
	RecordIndicatorB2bDetailsBytes     = []byte(RecordIndicatorB2bDetailsString)
	RecordIndicatorEndOfDataBytes      = []byte(RecordIndicatorEndOfDataString)
)
