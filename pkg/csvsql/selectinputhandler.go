package csvsql

import (
	"encoding/csv"
	"io"
	"strconv"
	"fmt"

	gzip "github.com/klauspost/pgzip"
	"net/http"
)

// Input represents a record producing input from a  formatted file or pipe.
type Input struct {
	options         *Options
	reader          *csv.Reader
	firstRow        []string
	header          []string
	minOutputLength int
}

// InputOptions options are passed to the underlying encoding/csv reader.
type Options struct {
	// HasHeader when true, will treat the first row as a header row.
	HasHeader bool

	// FieldDelimiter is the string that fields are delimited by.
	FieldDelimiter string

	// Comments is the string the first character of a line of
	// text matches the comment character.
	Comments string

	// Name of the table that is used for querying
	Name string

	// ReadFrom is where the data will be read from.
	ReadFrom io.Reader

	// If true then we need to add gzip reader.
	// to extract the csv.
	CompressedGzip bool

	//SQL expression meant to be evaluated.
	Expression string

	//What the outputted CSV will be delimited by .
	OutputFieldDelimiter string
}

// NewInput sets up a new Input, the first row is read when this is run.
// If there is a problem with reading the first row, the error is returned.
// Otherwise, the returned csvInput can be reliably consumed with ReadRecord()
// until ReadRecord() returns nil.
func NewInput(opts *Options) (*Input, error) {
	reader := opts.ReadFrom
	if opts.CompressedGzip {
		var err error
		if reader, err = gzip.NewReader(opts.ReadFrom); err != nil {
			return nil, err
		}
	}
	csvInput := &Input{
		options: opts,
		reader:  csv.NewReader(reader),
	}
	csvInput.firstRow = nil

	csvInput.reader.FieldsPerRecord = -1
	if csvInput.options.FieldDelimiter != "" {
		csvInput.reader.Comma = rune(csvInput.options.FieldDelimiter[0])
	}

	if csvInput.options.Comments != "" {
		csvInput.reader.Comment = rune(csvInput.options.Comments[0])
	}

	// QuoteCharacter - " (defaulted currently)
	csvInput.reader.LazyQuotes = true

	if err := csvInput.readHeader(); err != nil {
		return nil, err
	}

	return csvInput, nil
}

// Name returns the name of the  being read.
// By default, either the base filename or 'pipe' if it is a unix pipe
func (csvInput *Input) Name() string {
	return csvInput.options.Name
}

// SetName - is a no-op doesn't do anything.
func (csvInput *Input) SetName(_ string) {
}

// ReadRecord reads a single record from the . Always returns successfully.
// If the record is empty, an empty []string is returned.
// Record expand to match the current row size, adding blank fields as needed.
// Records never return less then the number of fields in the first row.
// Returns nil on EOF
// In the event of a parse error due to an invalid record, it is logged, and
// an empty []string is returned with the number of fields in the first row,
// as if the record were empty.
//
// In general, this is a very tolerant of problems  reader.
func (csvInput *Input) ReadRecord() []string {
	var row []string
	var fileErr error

	if csvInput.firstRow != nil {
		row = csvInput.firstRow
		csvInput.firstRow = nil
		return row
	}

	row, fileErr = csvInput.reader.Read()
	emptysToAppend := csvInput.minOutputLength - len(row)
	if fileErr == io.EOF {
		return nil
	} else if _, ok := fileErr.(*csv.ParseError); ok {
		emptysToAppend = csvInput.minOutputLength
	}

	if emptysToAppend > 0 {
		for counter := 0; counter < emptysToAppend; counter++ {
			row = append(row, "")
		}
	}

	return row
}

func (csvInput *Input) readHeader() error {
	var readErr error

	csvInput.firstRow, readErr = csvInput.reader.Read()
	if readErr != nil {
		return readErr
	}

	csvInput.minOutputLength = len(csvInput.firstRow)

	if csvInput.options.HasHeader {
		csvInput.header = csvInput.firstRow
		csvInput.firstRow = nil
	} else {
		csvInput.header = make([]string, csvInput.minOutputLength)
		for i := 0; i < len(csvInput.firstRow); i++ {
			csvInput.header[i] = "c" + strconv.Itoa(i)
		}
	}

	return nil
}

// Header returns the header of the csvInput. Either the first row if a header
// set in the options, or c#, where # is the column number, starting with 0.
func (csvInput *Input) Header() []string {
	return csvInput.header
}

func (csvInput *Input) Execute(writer http.ResponseWriter, reader *io.PipeReader) {
	 myRowVal := make(chan string)
	 myRowState := make(chan string)
	 myRowError := make(chan error)
	 go csvInput.RunSqlParser(csvInput.options.Expression, myRowVal, myRowState, myRowError)
	 for {
	   select {
		 case row, ok := <-myRowVal:
			  if ok && len(row) > 0 {
					fmt.Println("send row header")
				}
		 case rowerror,ok := <-myRowError:
			 if ok {
				 fmt.Println("ERROR!")
				 fmt.Println(rowerror)
			 }
		 case rowstate := <-myRowState:
			 	fmt.Println("I have ended send the end message")
				fmt.Println("QUIT after this")
				fmt.Println(rowstate)
				close(myRowState)
				close(myRowVal)
				close(myRowError)
				return
	    }
		}
	}
