package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	hl7 "github.com/radpartners/go-hl7"
)

const (
    projectID    = "devx-rpml" //"rpx-sandbox"
    location     = "us-central1" //"us-west2"
    datasetID    = "somehl7"  //"sandbox-hl7-store"
    hl7StoreID   = "clitest" //"dicom-store"
	hl7Dir       = "/Users/scot/dev/temp/hl7vvh3"
	retainSent   = true 
	loopit       = 1  // note - 0 will not do anything, 1 will loop once over dir, more than one will loop for that many times on target dir
)

func main () {
	if loopit > 0 {
		for i := 0; i < loopit; i++ {
			fmt.Println()
			fmt.Printf("Loop #%v in folder '%s'\n", i + 1, hl7Dir)
			fmt.Println()
			scanDirectory(hl7Dir)
		}
	}
}

type MsgVals struct {
	SendingFacility string `json:"sending_facility" yaml:"sending_facility"`
}

func hl7WebStoreInstance(hl7Path, hl7File string) error {
    ctx := context.Background()

	// must run thru hl7 parse in order to remove invalid new line chars
	msg, err := hl7.ParseFile(hl7Path + "/" + hl7File, true)
	if err != nil {
		return err
	}

	// Get SendingFacility from header of message
	hdr, err := hl7.GetMessageHeader(*msg)
	if err != nil {
		return  err
	}

	fmt.Println()
	fmt.Printf("Sending Facility for Message is %s\n", hdr.SendingFacility)
	fmt.Println()

	hl7Data := msg.Raw()
	hl7Data = append(hl7Data, []byte("ZAC|" + time.Now().Format("20060102150405.9999999999"))...)
	f, err := os.Create("./tmp/" + hl7File)
	if err != nil {
		return err
	}
	
	f.Write(hl7Data)
	if err := f.Close(); err != nil {
		return err
	}
	

	tmpfile := "./tmp/" + hl7File
	hl7Data2, err := ioutil.ReadFile(tmpfile)

    if err != nil {
        return fmt.Errorf("ReadFile: %v", err)
    }

	if !retainSent {
		err = os.Remove("./tmp/" + hl7File)
		if err != nil {
			fmt.Printf("unable to remove temporary transform file: %s\n", tmpfile)
		}
	}

	cfg := Config{
		Credential: "./.secrets/creds.json",
		ProjectID:  projectID,
		LocationID: location,
		DatasetID:  datasetID,
		HL7StoreID: hl7StoreID,
		RateLimit:  0,
	}
 
	cli, err := NewClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("unable to get new hcapi client to write message to hl7 datastore", err)
	}

	data, pth, err := cli.Send(hl7Data2)
	if err != nil {
		return fmt.Errorf("unable to send msg to hcapi", err)
	}

	fmt.Printf("message successfully stored at: %s\n", pth)

	err = checkAck(data)
	if err != nil {
		return err
	}

    return nil
}

func checkAck(b []byte) error {
	ack, err := hl7.ParseMessage(b, true)
	if err != nil {
		return err
	}

	switch hl7.MessageType(*ack) {
	case "ACK":
		// ack received - pass in ack to analytics
		// if mack != nil {
		// 	if s.anacli != nil {
		// 		s.anacli.AcknowledgementReceived(orig, mack)
		// 	}
		// }
		return nil
	case "NACK":
		return errors.New("receiving system returned nack")
	default:
		return errors.New("invalid ack response")
	}
}

func scanDirectory(path string) {
	files, err := ioutil.ReadDir(path)

	if err != nil {
		panic(err)
	}

	for _, file := range files {
		filepath := path + "/" + file.Name()
		if file.IsDir() {
			fmt.Println(filepath + " is a subdirectory. Moving into it for processing....")
			scanDirectory(filepath)
		} else {
			if strings.Contains(file.Name(), ".hl7") {
				if err := hl7WebStoreInstance(path, file.Name()); err != nil {
					fmt.Printf("error: %s\n", err)
				}
			}
		}
	}
}
