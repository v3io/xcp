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
