/*
Copyright 2019 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package tests

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/xcp/backends"
	"github.com/v3io/xcp/common"
	"github.com/v3io/xcp/operators"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var log logger.Logger
var tempdir string
var AWS_TEST_BUCKET string

var dummyContent = []byte("dummy content")

type testLocalBackend struct {
	suite.Suite
}

func (suite *testLocalBackend) SetupSuite() {
	AWS_TEST_BUCKET = os.Getenv("AWS_TEST_BUCKET")
	src, err := common.UrlParse(tempdir, true)
	suite.Require().Nil(err)

	client, err := backends.NewLocalClient(log, src)
	suite.Require().Nil(err)

	w, err := client.Writer(filepath.Join(tempdir, "a.txt"), nil)
	suite.Require().Nil(err)
	n, err := w.Write(dummyContent)
	suite.Require().Nil(err)
	suite.Require().Equal(n, len(dummyContent))

	opts := backends.FileMeta{
		Mtime: time.Now().Add(-23 * time.Hour),
		Mode:  777}
	w, err = client.Writer(filepath.Join(tempdir, "a.csv"), &opts)
	suite.Require().Nil(err)
	n, err = w.Write(dummyContent)
	suite.Require().Nil(err)
	suite.Require().Equal(n, len(dummyContent))
	err = w.Close()
	suite.Require().Nil(err)
}

func (suite *testLocalBackend) TestRead() {
	src, err := common.UrlParse(tempdir, true)
	suite.Require().Nil(err)

	client, err := backends.NewLocalClient(log, src)
	suite.Require().Nil(err)

	r, err := client.Reader(filepath.Join(tempdir, "a.csv"))
	data := make([]byte, 100)
	n, err := r.Read(data)
	suite.Require().Nil(err)
	suite.Require().Equal(n, len(dummyContent))
	suite.Require().Equal(data[0:n], dummyContent)
}

func (suite *testLocalBackend) TestList() {

	src, err := common.UrlParse(tempdir, true)
	listTask := backends.ListDirTask{Source: src}
	fmt.Println(src)
	iter, err := operators.ListDir(&listTask, log)
	suite.Require().Nil(err)

	for iter.Next() {
		fmt.Printf("File %s: %+v", iter.Name(), iter.At())
	}
	suite.Require().Nil(iter.Err())
	summary := iter.Summary()
	fmt.Printf("Total files: %d,  Total size: %d\n",
		summary.TotalFiles, summary.TotalBytes)
	suite.Require().Equal(summary.TotalFiles, 2)

	src, _ = common.UrlParse(tempdir+"/*.csv", true)
	listTask = backends.ListDirTask{Source: src}
	iter, err = operators.ListDir(&listTask, log)
	suite.Require().Nil(err)
	_, err = iter.ReadAll()
	suite.Require().Nil(err)
	summary = iter.Summary()

	fmt.Printf("Total files: %d,  Total size: %d\n",
		summary.TotalFiles, summary.TotalBytes)
	suite.Require().Equal(summary.TotalFiles, 1)
}

func (suite *testLocalBackend) TestCopyToS3() {
	src, err := common.UrlParse(tempdir, true)
	suite.Require().Nil(err)

	listTask := backends.ListDirTask{Source: src, WithMeta: true}
	dst, err := common.UrlParse("s3://"+AWS_TEST_BUCKET+"/xcptests/*.*", true)
	suite.Require().Nil(err)

	err = operators.CopyDir(&listTask, dst, log, 1)
	suite.Require().Nil(err)

	// read list dir content from S3
	listTask = backends.ListDirTask{Source: dst}
	dstdir, err := ioutil.TempDir("", "xcptest-dst")
	suite.Require().Nil(err)
	newdst, err := common.UrlParse(dstdir, true)
	suite.Require().Nil(err)

	err = operators.CopyDir(&listTask, newdst, log, 1)
	suite.Require().Nil(err)
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
	os.RemoveAll(tempdir)
}
