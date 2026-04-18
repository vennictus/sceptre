package kv

import "errors"

var errCommitInterrupted = errors.New("kv: commit interrupted")

type commitStage string

const (
	commitStagePagesWritten  commitStage = "pages-written"
	commitStagePagesSynced   commitStage = "pages-synced"
	commitStageMetaPublished commitStage = "meta-published"
)

type commitHook func(stage commitStage) error

func failAfterCommitStage(target commitStage) commitHook {
	return func(stage commitStage) error {
		if stage == target {
			return commitInterruptedError{stage: stage}
		}
		return nil
	}
}

type commitInterruptedError struct {
	stage commitStage
}

func (e commitInterruptedError) Error() string {
	return errCommitInterrupted.Error()
}

func (e commitInterruptedError) Is(target error) bool {
	return target == errCommitInterrupted
}

func interruptedCommitStage(err error) (commitStage, bool) {
	var interrupted commitInterruptedError
	if !errors.As(err, &interrupted) {
		return "", false
	}
	return interrupted.stage, true
}
