package raft

import (
	"kvdb/pkg/utils"
	"testing"
)

func TestGetSnapshotSegment(t *testing.T) {

	dir := "../../build/sst"

	logger := utils.GetLogger(dir)
	sugar := logger.Sugar()

	s := NewRaftStorage(dir, sugar)

	snapc, err := s.getSnapshot(3)

	if err != nil {
		sugar.Errorf(err.Error())
	}

	for {
		snap := <-snapc
		if snap != nil {
			sugar.Infof("读取到 LastIndex; %d ,LastTerm: %d, Level: %d ,Offset: %d ,Size: %d ", snap.LastIncludeIndex, snap.LastIncludeTerm, snap.Level, snap.Offset, len(snap.Data))
		} else {
			break
		}
	}

}

func TestInstallSnapshotSegment(t *testing.T) {

	dir := "../../build/sst"

	logger := utils.GetLogger(dir)
	sugar := logger.Sugar()

	s := NewRaftStorage(dir, sugar)

	snapc, err := s.getSnapshot(3)

	if err != nil {
		sugar.Errorf(err.Error())
	}

	for {
		snap := <-snapc
		if snap != nil {
			// sugar.Infof("LastIndex; %d ,LastTerm: %d, Level: %d ,Offset: %d ,Size: %d ", snap.LastIncludeIndex, snap.LastIncludeTerm, snap.Level, snap.Offset, len(snap.Data))
			s.InstallSnapshot(snap)
		} else {
			break
		}
	}

}
