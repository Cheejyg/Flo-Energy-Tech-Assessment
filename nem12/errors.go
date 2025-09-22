package nem12

import "errors"

var (
	ErrInvalidDate                 = errors.New("invalid date")
	ErrInvalidDateTime             = errors.New("invalid datetime")
	ErrInvalidHeaderRecord         = errors.New("invalid header record")
	ErrInvalidNmiDataDetailsRecord = errors.New("invalid nmi data details record")
	ErrInvalidIntervalDataRecord   = errors.New("invalid interval data record")
	ErrInvalidIntervalEventRecord  = errors.New("invalid interval event record")
	ErrInvalidB2bDetailsRecord     = errors.New("invalid b2b details record")
	ErrInvalidEndOfData            = errors.New("invalid end of data")
)
