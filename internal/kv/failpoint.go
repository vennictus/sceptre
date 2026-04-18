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
			return errCommitInterrupted
		}
		return nil
	}
}
