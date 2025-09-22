package nem12

import (
	"strconv"
	"strings"
	"time"
)

// # Header record (100)
//
// Example: RecordIndicator,VersionHeader,DateTime,FromParticipant,ToParticipant
//
//	100,NEM12,200301011534,MDP1,Retailer1
type HeaderRecord struct {
	// Header record indicator. 1 per file (100-900 record set).
	//
	// A 100 record must have a matching 900 record.
	//
	// Allowed value: 100.
	RecordIndicator [3]byte

	// Version identifier. Details the version of the data block and hence its format.
	//
	// Allowed value: NEM12.
	VersionHeader [5]byte

	DateTime        time.Time // File creation date/time.
	FromParticipant [10]byte  // The Participant ID of the MDP that generates the file.
	ToParticipant   [10]byte  // The Participant ID of the intended Registered Participant, MDP or ENM.
}

func (headerRecord *HeaderRecord) String() string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(256)

	stringBuilder.WriteString("HeaderRecord{")
	stringBuilder.WriteString("RecordIndicator:")
	stringBuilder.Write(headerRecord.RecordIndicator[:])
	stringBuilder.WriteString(", VersionHeader:")
	stringBuilder.Write(headerRecord.VersionHeader[:])
	stringBuilder.WriteString(", DateTime:")
	stringBuilder.WriteString(headerRecord.DateTime.Format("15:04 on 2 January 2006"))
	stringBuilder.WriteString(", FromParticipant:")
	stringBuilder.Write(headerRecord.FromParticipant[:])
	stringBuilder.WriteString(", ToParticipant:")
	stringBuilder.Write(headerRecord.ToParticipant[:])
	stringBuilder.WriteString("}")

	return stringBuilder.String()
}

// # NMI data details record (200)
//
// Multiple 300-500 record blocks are allowed within a single 200 record.
//
// Example: RecordIndicator,NMI,NMIConfiguration,RegisterID,NMISuffix,MDMDataStreamIdentifier, MeterSerialNumber,UOM,IntervalLength,NextScheduledReadDate
//
//	200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,20040120
type NmiDataDetailsRecord struct {
	// NMI data details record indicator.
	//
	// Allowed value: 200.
	RecordIndicator [3]byte

	// NMI for the connection point.
	//
	// Does not include check-digit or NMI suffix.
	Nmi [10]byte

	// String of all NMISuffixes applicable to the NMI.
	//
	// The NMIConfiguration must represent the actual configuration of the Site.
	//
	// Where there is a NMI configuration change, all active channels on any part of the day must be provided.
	NmiConfiguration string

	// Interval Meter register identifier. Defined the same as the RegisterID field in the CATS_Register_Identifier table.
	//
	// The value should match the value in MSATS.
	//
	// e.g. “1”, “2”, “E1”, “B1”.
	//
	// The RegisterID is:
	//
	// Mandatory for type 4, 4A and type 5 metering data when the sender of the MDFF file is the Current MDP.
	//
	// Not required for types 1-3 and type 7 or when sending metering data to another MDP (eg Meter Churn data).
	RegisterId *[10]byte

	NmiSuffix [2]byte // As defined in the NMI Procedure e.g. “E1”, “B1”, “Q1”, “K1”.

	// Defined as per the suffix field in the CATS_NMI_DataStream table,
	//
	// e.g. “N1”, “N2”.
	//
	// The value must match the value in MSATS.
	//
	// The field must be provided if the metering data has or would be sent to MSATS by the sender. The field is not required when sending the data to another MDP.
	MdmDataStreamIdentifier *[2]byte

	// The Meter Serial ID of the meter installed at a Site.
	//
	// If the meter is replaced, the Meter Serial ID of the new meter will apply on and from the IntervalDate when the meter is replaced.
	//
	// Not required for type 7 metering installations, logical meters, Historical Data, or where multiple meters are summated to form a single RegisterID.
	MeterSerialNumber *[12]byte

	// Unit of measure of data.
	//
	// Refer Appendix B for the list of allowed values for this field.
	Uom [5]byte

	IntervalLength [2]byte // Time in minutes of each Interval period: 5, 15, or 30.

	// This date is the NSRD.
	//
	// This field is not required for remotely read meters.
	//
	// This field is not required where the meter will not be read again (eg meter removed, NMI abolished, MDP will no longer be the MDP).
	//
	// The NSRD provided in this file is accurate at the time the file is generated (noting this may be subject to change e.g. if route change etc.). MSATS is the database of record, therefore, should there be a discrepancy between the NSRD Date in this file, MSATS shall prevail.
	NextScheduledReadDate *time.Time
}

func (nmiDataDetailsRecord *NmiDataDetailsRecord) String() string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(256)

	stringBuilder.WriteString("NmiDataDetailsRecord{")
	stringBuilder.WriteString("RecordIndicator:")
	stringBuilder.Write(nmiDataDetailsRecord.RecordIndicator[:])
	stringBuilder.WriteString(", Nmi:")
	stringBuilder.Write(nmiDataDetailsRecord.Nmi[:])
	stringBuilder.WriteString(", NmiConfiguration:")
	stringBuilder.WriteString(nmiDataDetailsRecord.NmiConfiguration)
	if nmiDataDetailsRecord.RegisterId != nil {
		stringBuilder.WriteString(", RegisterId:")
		stringBuilder.Write(nmiDataDetailsRecord.RegisterId[:])
	} else {
		stringBuilder.WriteString(", RegisterId:<nil>")
	}
	stringBuilder.WriteString(", NmiSuffix:")
	stringBuilder.Write(nmiDataDetailsRecord.NmiSuffix[:])
	if nmiDataDetailsRecord.MdmDataStreamIdentifier != nil {
		stringBuilder.WriteString(", MdmDataStreamIdentifier:")
		stringBuilder.Write(nmiDataDetailsRecord.MdmDataStreamIdentifier[:])
	} else {
		stringBuilder.WriteString(", MdmDataStreamIdentifier:<nil>")
	}
	if nmiDataDetailsRecord.MeterSerialNumber != nil {
		stringBuilder.WriteString(", MeterSerialNumber:")
		stringBuilder.Write(nmiDataDetailsRecord.MeterSerialNumber[:])
	} else {
		stringBuilder.WriteString(", MeterSerialNumber:<nil>")
	}
	stringBuilder.WriteString(", Uom:")
	stringBuilder.Write(nmiDataDetailsRecord.Uom[:])
	stringBuilder.WriteString(", IntervalLength:")
	stringBuilder.Write(nmiDataDetailsRecord.IntervalLength[:])
	if nmiDataDetailsRecord.NextScheduledReadDate != nil {
		stringBuilder.WriteString(", NextScheduledReadDate:")
		stringBuilder.WriteString(nmiDataDetailsRecord.NextScheduledReadDate.Format("2 January 2006"))
	} else {
		stringBuilder.WriteString(", NextScheduledReadDate:<nil>")
	}
	stringBuilder.WriteString("}")

	return stringBuilder.String()
}

// # Interval data record (300)
//
// Example: RecordIndicator,IntervalDate,IntervalValue1 . . . IntervalValueN, QualityMethod,ReasonCode,ReasonDescription,UpdateDateTime,MSATSLoadDateTime
//
//	300,20030501,50.1, . . . ,21.5,V,,,20030101153445,20030102023012
//
// 300 records must be presented in date sequential order. For example, with a series of Meter Readings for a period, the current record is the next incremental IntervalDate after the previous record. Or, where data for individual, non-consecutive days is sent, the IntervalDate for each 300 record is later than the previous one.
//
// Where the same QualityMethod and ReasonCode apply to all IntervalValues in the 300 record, the QualityMethod, ReasonCode and ReasonDescription in the 300 Record must be used. If either of these fields contains multiple values for the IntervalValues, the QualityMethod in the 300 record must be set to “V” and the 400 record must be provided.
//
// The use of ‘V’ as the quality method in this example indicates the QualityMethod, ReasonCode or ReasonDescription vary across the day and will be provided, for each Interval, in the 400 records that would immediately follow this record. Refer 4.5 for details on the use of the 400 records.
type IntervalDataRecord struct {
	// NMI data details record indicator.
	//
	// Allowed value: 300.
	RecordIndicator [3]byte

	IntervalDate time.Time // Interval date.

	// Interval metering data.
	//
	// The total amount of energy or other measured value for the Interval inclusive of any multiplier or scaling factor.
	//
	// The number of values provided must equal 1440 divided by the IntervalLength. This is a repeating field with individual field values separated by comma delimiters.
	//
	// Allowed value rules:
	//
	// A negative value is not allowed.
	//
	// The value may contain decimal places.
	//
	// Exponential values are not allowed.
	IntervalValue []float64

	// Summary of the data quality and Substitution/Estimation flags for all IntervalValues contained in this record.
	//
	// The QualityMethod applies to all IntervalValues in this record. Where multiple QualityMethods or ReasonCodes apply to these IntervalValues, a quality flag ‘V’ must be used.
	//
	// Format: In the form QMM, where quality flag ('Q) = 1 character and method flag (MM) = 2 character.
	//
	// Allowed values:
	//
	// See quality and method tables (Appendix C & D).
	//
	// If quality flag = ’A’ or ’V‘ no method flag is required.
	QualityMethod [3]byte

	// Summary of the reasons for Substitute/Estimate or information for all IntervalValues contained in this record.
	//
	// The ReasonCode applies to all IntervalValues in this record.
	//
	// Not required if quality flag = ’A’ or ‘E‘, but can be provided for information.
	//
	// The field must not be populated if quality flag = ’V’.
	//
	// Allowed values: Refer Appendix E.
	ReasonCode *[3]byte

	// Description of ReasonCode.
	//
	// Mandatory where the ReasonCode is ’0’.
	ReasonDescription *string

	UpdateDateTime    *time.Time // The latest date/time that any updated IntervalValue or QualityMethod for the IntervalDate. This is the MDP’s version date/time that the metering data was created or changed. This date and time applies to data in this 300 record.
	MsatsLoadDateTime *time.Time // This is the date/time stamp MSATS records when metering data was loaded into MSATS. This date is in the acknowledgement notification sent to the MDP by MSATS.
}

func (intervalDataRecord *IntervalDataRecord) String() string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(1024)

	stringBuilder.WriteString("IntervalDataRecord{")
	stringBuilder.WriteString("RecordIndicator:")
	stringBuilder.Write(intervalDataRecord.RecordIndicator[:])
	stringBuilder.WriteString(", IntervalDate:")
	stringBuilder.WriteString(intervalDataRecord.IntervalDate.Format("2 January 2006"))
	stringBuilder.WriteString(", IntervalValue:[")
	for i := range intervalDataRecord.IntervalValue {
		if i > 0 {
			stringBuilder.WriteString(", ")
		}
		stringBuilder.WriteString(strconv.FormatFloat(intervalDataRecord.IntervalValue[i], 'f', -1, 64))
	}
	stringBuilder.WriteString("]")
	stringBuilder.WriteString(", QualityMethod:")
	stringBuilder.Write(intervalDataRecord.QualityMethod[:])
	if intervalDataRecord.ReasonCode != nil {
		stringBuilder.WriteString(", ReasonCode:")
		stringBuilder.Write(intervalDataRecord.ReasonCode[:])
	} else {
		stringBuilder.WriteString(", ReasonCode:<nil>")
	}
	if intervalDataRecord.ReasonDescription != nil {
		stringBuilder.WriteString(", ReasonDescription:")
		stringBuilder.WriteString(*intervalDataRecord.ReasonDescription)
	} else {
		stringBuilder.WriteString(", ReasonDescription:<nil>")
	}
	if intervalDataRecord.UpdateDateTime != nil {
		stringBuilder.WriteString(", UpdateDateTime:")
		stringBuilder.WriteString(intervalDataRecord.UpdateDateTime.Format("15:04 on 2 January 2006"))
	} else {
		stringBuilder.WriteString(", UpdateDateTime:<nil>")
	}
	if intervalDataRecord.MsatsLoadDateTime != nil {
		stringBuilder.WriteString(", MsatsLoadDateTime:")
		stringBuilder.WriteString(intervalDataRecord.MsatsLoadDateTime.Format("15:04 on 2 January 2006"))
	} else {
		stringBuilder.WriteString(", MsatsLoadDateTime:<nil>")
	}
	stringBuilder.WriteString("}")

	return stringBuilder.String()
}

// # Interval event record (400)
//
// Example: RecordIndicator,StartInterval,EndInterval,QualityMethod,ReasonCode,ReasonDescription
//
//	400,1,28,S14,32,
//
// This record is mandatory where the QualityFlag is ‘V’ in the 300 record or where the quality flag is ‘A’ and reason codes 79, 89, and 61 are used.
//
// The StartInterval/EndInterval pairs must be presented in ascending record order. The StartInterval/EndInterval period must cover an entire day without gaps or overlaps. For example, (based on a 30-minute Interval):
//
//	400,1,26,A,,
//	400,27,31,S53,9,
//	400,32,48,E52,,
//
// Refer section 2 (c) for further rules regarding the use of this record.
type IntervalEventRecord struct {
	// Interval event record indicator.
	//
	// Allowed value: 400.
	RecordIndicator [3]byte

	// The first Interval number that the ReasonCode/QualityMethod combination applies to.
	//
	// The StartInterval must be less than or equal to the EndInterval.
	StartInterval [4]byte

	EndInterval [4]byte // The last Interval number that the ReasonCode/QualityMethod combination applies to.

	// Data quality & Substitution/Estimation flag for metering data.
	//
	// The QualityMethod applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval.
	//
	// Format: In the form QMM, where quality flag (Q) = 1 character and method flag (MM) = 2 character
	//
	// Allowed values:
	//
	// See quality and method tables (refer Appendices C & D).
	//
	// If quality flag = “A” no method required.
	//
	// The quality flag of “V” cannot be used in this record.
	QualityMethod [3]byte

	// Reason for Substitute/Estimate or information.
	//
	// The ReasonCode applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval.
	//
	// Not required if quality flag = “E” but can be provided for information.
	//
	// Allowed values: Refer Appendix E.
	ReasonCode *[3]byte

	// Description of ReasonCode.
	//
	// Mandatory where the ReasonCode is “0”.
	//
	// The ReasonDescription applies to all IntervalValues in the inclusive range defined by the StartInterval and EndInterval.
	ReasonDescription *string
}

// # B2B details record (500)
//
// Example: RecordIndicator,TransCode,RetServiceOrder,ReadDateTime,IndexRead
//
//	500,S,RETNSRVCEORD1,20031220154500,001123.5
//
// This record is mandatory where a manual Meter Reading has been performed or attempted.
//
// Only valid 500 records associated with the current Meter Reading period must be provided. For example, a 500 record associated with a Substitute will become invalid if Actual Metering Data subsequently replace the Substitutes.
//
// This record must be repeated where multiple TransCodes or RetServiceOrders apply to the day.
type B2bDetailsRecord struct {
	// B2B details record indicator.
	//
	// Allowed value: 500.
	RecordIndicator [3]byte

	// Indicates why the recipient is receiving this metering data.
	//
	// Refer Appendix A for a list of allowed values for this field.
	//
	// A value of ‘O’ (i.e. capital letter O) must be used when providing Historical Data and where this information is unavailable.
	TransCode [1]byte

	RetServiceOrder *[15]byte // The Service Order number associated with the Meter Reading.

	// Actual date/time of the Meter Reading.
	//
	// The date/time the transaction occurred or, for a Substitution (quality flag = ‘S’ or ‘F’), when the Meter Reading should have occurred.
	//
	// The time component of the ReadDateTime should be the actual time of the attempted Meter Reading. If this is not available the value of the time component must be 00:00:01.
	//
	// The ReadDateTime is required when providing Historical Data and not required for Estimates.
	ReadDateTime *time.Time

	// The total recorded accumulated energy for a Datastream retrieved from a meter’s register at the time of collection.
	//
	// For type 4A and type 5 metering installations the MDP must provide the IndexRead when collected. Refer section 3.3.4.
	IndexRead *[15]byte
}

func (b2bDetailsRecord *B2bDetailsRecord) String() string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(256)

	stringBuilder.WriteString("B2bDetailsRecord{")
	stringBuilder.WriteString("RecordIndicator:")
	stringBuilder.Write(b2bDetailsRecord.RecordIndicator[:])
	stringBuilder.WriteString(", TransCode:")
	stringBuilder.Write(b2bDetailsRecord.TransCode[:])
	if b2bDetailsRecord.RetServiceOrder != nil {
		stringBuilder.WriteString(", RetServiceOrder:")
		stringBuilder.Write(b2bDetailsRecord.RetServiceOrder[:])
	} else {
		stringBuilder.WriteString(", RetServiceOrder:<nil>")
	}
	if b2bDetailsRecord.ReadDateTime != nil {
		stringBuilder.WriteString(", ReadDateTime:")
		stringBuilder.WriteString(b2bDetailsRecord.ReadDateTime.Format("2 January 2006"))
	} else {
		stringBuilder.WriteString(", ReadDateTime:<nil>")
	}
	if b2bDetailsRecord.IndexRead != nil {
		stringBuilder.WriteString(", IndexRead:")
		stringBuilder.Write(b2bDetailsRecord.IndexRead[:])
	} else {
		stringBuilder.WriteString(", IndexRead:<nil>")
	}
	stringBuilder.WriteString("}")

	return stringBuilder.String()
}

// # End of data (900)
//
// Example: RecordIndicator
//
//	900
type EndOfData struct {
	// This is the end of record indicator for the record set commencing with the previous 100 record.
	//
	// Allowed Value: 900.
	RecordIndicator [3]byte
}

func (endOfData *EndOfData) String() string {
	var stringBuilder strings.Builder
	stringBuilder.Grow(30)

	stringBuilder.WriteString("EndOfData{")
	stringBuilder.WriteString("RecordIndicator:")
	stringBuilder.Write(endOfData.RecordIndicator[:])
	stringBuilder.WriteString("}")

	return stringBuilder.String()
}
