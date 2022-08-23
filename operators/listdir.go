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
package operators

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/v3io/xcp/backends"
)

func ListDir(task *backends.ListDirTask, logger logger.Logger) (*listResults, error) {

	list := listResults{
		fileChan: make(chan *backends.FileDetails, 1000),
		summary:  &backends.ListSummary{},
		errChan:  make(chan error, 60),
	}

	logger.InfoWith("list task", "from", task.Source)
	client, err := backends.GetNewClient(logger, task.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get list source, %v", err)
	}

	go func(errChan chan error) {
		var err error
		err = client.ListDir(list.fileChan, task, list.summary)
		if err != nil {
			errChan <- fmt.Errorf("failed in list dir, %v", err)
			fmt.Println(err)
		}
	}(list.errChan)

	return &list, nil
}

type listResults struct {
	fileChan chan *backends.FileDetails
	summary  *backends.ListSummary
	errChan  chan error

	currFile *backends.FileDetails
	err      error
}

func (l *listResults) Next() bool {
	var more bool
	l.currFile, more = <-l.fileChan

	if !more {
		select {
		case err := <-l.errChan:
			l.err = err
		default:
		}
		return false
	}
	return true
}

func (l *listResults) ReadAll() ([]*backends.FileDetails, error) {
	list := []*backends.FileDetails{}
	for l.Next() {
		list = append(list, l.At())
	}
	return list, l.err
}

func (l *listResults) Err() error {
	return l.err
}

func (l *listResults) At() *backends.FileDetails {
	return l.currFile
}

func (l *listResults) Name() string {
	return l.currFile.Key
}

func (l *listResults) Summary() *backends.ListSummary {
	return l.summary
}
