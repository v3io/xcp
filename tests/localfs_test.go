package tests

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/xcp/backends"
	"github.com/v3io/xcp/common"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var log logger.Logger
var tempdir string

func writeFiles() (string, error) {
	content := []byte("dummy content")

	tmpfn := filepath.Join(tempdir, "a.txt")
	if err := ioutil.WriteFile(tmpfn, content, 0666); err != nil {
		return "", err
	}

	tmpfn = filepath.Join(tempdir, "a.csv")
	if err := ioutil.WriteFile(tmpfn, content, 0666); err != nil {
		return "", err
	}

	return tempdir, nil
}

type testLocalBackend struct {
	suite.Suite
}

func (suite *testLocalBackend) TestAWrite() {
	content := []byte("dummy content")
	src, err := common.UrlParse(tempdir)
	suite.Require().Nil(err)

	client, err := backends.NewLocalClient(log, src)
	suite.Require().Nil(err)

	w, err := client.Writer(filepath.Join(tempdir, "a.txt"), nil)
	suite.Require().Nil(err)
	n, err := w.Write(content)
	suite.Require().Nil(err)
	suite.Require().Equal(n, len(content))

	opts := backends.WriteOptions{
		Mtime: time.Now().Add(-23 * time.Hour),
		Mode:  777}
	w, err = client.Writer(filepath.Join(tempdir, "a.csv"), &opts)
	suite.Require().Nil(err)
	n, err = w.Write(content)
	suite.Require().Nil(err)
	suite.Require().Equal(n, len(content))
}

func (suite *testLocalBackend) TestList() {

	src, err := common.UrlParse(tempdir)
	suite.Require().Nil(err)

	listTask := backends.ListDirTask{
		Source: src,
		Filter: "*.*",
	}

	client, err := backends.NewLocalClient(log, src)
	suite.Require().Nil(err)

	fileChan := make(chan *backends.FileDetails, 1000)
	summary := &backends.ListSummary{}

	err = client.ListDir(fileChan, &listTask, summary)
	suite.Require().Nil(err)

	fmt.Printf("Total files: %d,  Total size: %d\n",
		summary.TotalFiles, summary.TotalBytes)

	suite.Require().Equal(summary.TotalFiles, 2)
}

func TestLocalBackendSuite(t *testing.T) {
	log, _ = common.NewLogger("debug")
	var err error
	tempdir, err = ioutil.TempDir("", "xcptest")
	//tempdir = "../tst1"
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Target dir: ", tempdir)

	os.RemoveAll(tempdir) // clean up
	suite.Run(t, new(testLocalBackend))
}
