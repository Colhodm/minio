package csvsql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"hash/crc32"
	"io"
	"fmt"
)

// Output represents a TextQL output that transforms sql.Rows into  formatted
// string data using encoding/csv
type Output struct {
	options         *OutputOptions
	writer          *csv.Writer
	firstRow        []string
	header          []string
	minOutputLength int
}

// OutputOptions define options that are passed to encoding/csv for formatting
// the output in specific ways.
type OutputOptions struct {
	// FieldDelimiter is the string used to delimit fields.
	FieldDelimiter string

	// WriteTo is where the formatted data will be written to.
	WriteTo io.Writer
}

// NewOutput returns a new Output configured per the options provided.
func NewOutput(opts *OutputOptions) *Output {
	csvOutput := &Output{
		options: opts,
		writer:  csv.NewWriter(opts.WriteTo),
	}

	if csvOutput.options.FieldDelimiter != "" {
		csvOutput.writer.Comma = rune(csvOutput.options.FieldDelimiter[0])
	}

	return csvOutput
}

func (csvOutput *Output) writeRecordHeader() (int,error){
	var buf bytes.Buffer
	//The below are the specifications of the header for a "record" event
	// 11 -event type - 7 - 7 "Records"
	// 13 -content-type -7 -24 "application/octet-stream"
	// 13 -message-type -7 5 "event"
	headernamebytes := make([]byte, 1)
	copy(headernamebytes[0:], "1")
	binary.Write(&buf, binary.BigEndian, headernamebytes)

	headername := make([]byte, 11)
	copy(headername[0:], ":event-type")
	binary.Write(&buf, binary.BigEndian, headername)

	headertype := make([]byte, 1)
	copy(headertype[0:], "7")
	binary.Write(&buf, binary.BigEndian, headertype)

	headernamesize := make([]byte, 2)
	copy(headernamesize[0:], "7")
	binary.Write(&buf, binary.BigEndian, headernamesize)

	headervalue := make([]byte, 7)
	copy(headervalue[0:], "Records")
	binary.Write(&buf, binary.BigEndian, headervalue)

////// Creation of the Header for Content-Type 	// 13 -content-type -7 -24 "application/octet-stream"

	contentheadernamebytes := make([]byte, 1)
	copy(headernamebytes[0:], "13")
	binary.Write(&buf, binary.BigEndian, contentheadernamebytes)

	contentheadername := make([]byte, 13)
	copy(headername[0:], ":content-type")
	binary.Write(&buf, binary.BigEndian, contentheadername)

	contentheadertype := make([]byte, 1)
	copy(headertype[0:], "7")
	binary.Write(&buf, binary.BigEndian, contentheadertype)

	contentheadernamesize := make([]byte, 2)
	copy(headernamesize[0:], "24")
	binary.Write(&buf, binary.BigEndian, contentheadernamesize)

	contentheadervalue := make([]byte, 24)
	copy(headervalue[0:], "application/octet-stream")
	binary.Write(&buf, binary.BigEndian, contentheadervalue)

	////// Creation of the Header for message-type // 13 -message-type -7 5 "event"

		messageheadernamebytes := make([]byte, 1)
		copy(headernamebytes[0:], "13")
		binary.Write(&buf, binary.BigEndian, messageheadernamebytes)

		messageheadername := make([]byte, 13)
		copy(headername[0:], ":message-type")
		binary.Write(&buf, binary.BigEndian, messageheadername)

		messageheadertype := make([]byte, 1)
		copy(headertype[0:], "7")
		binary.Write(&buf, binary.BigEndian, messageheadertype)

		messageheadernamesize := make([]byte, 2)
		copy(headernamesize[0:], "5")
		binary.Write(&buf, binary.BigEndian, messageheadernamesize)

		messageheadervalue := make([]byte, 5)
		copy(headervalue[0:], "event")
		binary.Write(&buf, binary.BigEndian, messageheadervalue)

		_, err := csvOutput.options.WriteTo.Write(buf.Bytes())
		return 0,err

return 6, nil

}

func (csvOutput *Output) writeEndFrame() error {
	var buf bytes.Buffer
	var data = []interface{}{
		uint32(1 + 8388613),
		uint32(0),
	}

	for _, v := range data {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	cksum := crc32.ChecksumIEEE(buf.Bytes())
	// Check of the header.
	binary.Write(&buf, binary.LittleEndian, cksum)

	// No payload just provide the checksum of the empty payload.
	binary.Write(&buf, binary.LittleEndian, crc32.ChecksumIEEE([]byte{}))

	_, err := csvOutput.options.WriteTo.Write(buf.Bytes())
	return err
}

func (csvOutput *Output) writeRecordFrame(result []string) error {
	var payload = &bytes.Buffer{}
	for _, s := range result {
		payload.WriteString(s)
	}

	var buf bytes.Buffer
	var data = []interface{}{
		uint32(1 + 8388609),
		uint32(payload.Len()),
	}

	for _, v := range data {
		binary.Write(&buf, binary.LittleEndian, v)
	}

	cksum := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(&buf, binary.LittleEndian, cksum)

	if _, err := csvOutput.options.WriteTo.Write(buf.Bytes()); err != nil {
		return err
	}

	if err := csvOutput.writer.Write(result); err != nil {
		return err
	}

	bs := make([]byte, 4)
	cksum = crc32.ChecksumIEEE(payload.Bytes())
	binary.LittleEndian.PutUint32(bs, cksum)
	_, err := csvOutput.options.WriteTo.Write(bs)
	return err
}

// Show writes the sql.Rows given to the destination in
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html format.
func (csvOutput *Output) Write(rows *bufio.Writer) error {
//1). Calculate the Prelude portion of the header
		//if its a record frame then our total byte size is
	//totalbytelength := 16+headerlength + payloadlength
	wantRecord := true
	wantContinuation := false
	wantProgress := false
	wantStat := false
	wantError := false
	if wantRecord{
		// we  know the header length will be 22(Records) + 41(contenttype) + 22(message-type) so this is 85 bytes

	} else if(wantContinuation){
		// if our header is a continuation message it will be 19(event-type) and 22(message-type)
	} else if (wantProgress){
		// if our header is a progress message it will be 23(event-type) and 25(content-type) and 22(message-type)
		//NOTE This does have a payload that contains those bytes scanned thing (XML document)
	} else if (wantStat){
		// Stats message: eventype(20) + contenttype(25) + 22(message type)
		//has a payload similar to stat where everything is the same except this sent when this thing is done
	} else if (wantError){
		// End Message Header eventtype(18) + message-type(22)
		//Error message Header error-code(28) and error-message(69) and message-type(22)
	} else {
		//should throw general error
	}
	payload := make([]byte, 150)
	payloadsize,anerror := rows.Write(payload)
	fmt.Println(payloadsize)
	fmt.Println(anerror)




	//binary.Write(&buf, binary.BigEndian,totalbytelength)

	//Header Length Portion of the Protocol is set.
	//4 bytes long
	//binary.Write(&buf, binary.BigEndian, )

	// Have the IEEE crc32 computed on the prelude bytes
	//4 bytes long
	//preludecrc := crc32.ChecksumIEEE(buf.Bytes())
	//binary.Write(&buf, binary.BigEndian, )

   defer csvOutput.writer.Flush()
	//need to update this so that we get our writer's columns; need to figure out how to do this before entering scanner loop
//	counter := 0
//	cols :=  []string{"randomstuff"}

	// var (
	// 	rawResult = make([][]byte, len(cols))
	// 	result    = make([]string, len(cols))
	// 	dest      = make([]interface{}, len(cols))
	// )
	//Check this stuff out not sure exactly why its messing up
	// for i := range cols {
	// 	dest[i] = &rawResult[i]
	// }
		// if err = csvOutput.writer.Write(result); err != nil {
		// 	return err
		// }

		// Commenting out since its not working yet.
		// if err = csvOutput.writeRecordFrame(result); err != nil {
		// 	return err
		// }
	//}

	// return csvOutput.writeEndFrame()
	return nil
}
