package raft

import (
	"kvdb/pkg/utils"
	"testing"
)

func TestGetSnapshotSegment(t *testing.T) {

	dir := "../../build/sst"

	logger := utils.GetLogger(dir)
	sugar := logger.Sugar()

	s := NewRaftStorage(dir, &SimpleEncoding{}, sugar)

	snapc, err := s.GetSnapshot(3)

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

	s := NewRaftStorage(dir, &SimpleEncoding{}, sugar)

	snapc, err := s.GetSnapshot(3)

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

func TestRestoreMember(t *testing.T) {

	dir := "../../build/sst"

	logger := utils.GetLogger(dir)
	sugar := logger.Sugar()

	s := NewRaftStorage(dir, &SimpleEncoding{}, sugar)

	member, err := s.RestoreMember()

	if err != nil {
		sugar.Errorf(err.Error())
	}

	sugar.Infof("集群成员 : %v", member)
}
