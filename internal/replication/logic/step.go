package logic

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/dsh2dsh/zrepl/internal/client/jsonclient"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/replication/driver"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/replication/report"
	"github.com/dsh2dsh/zrepl/internal/util/bytecounter"
	"github.com/dsh2dsh/zrepl/internal/util/chainlock"
)

func NewStep(fs *Filesystem, from, to *pdu.FilesystemVersion) *Step {
	return &Step{parent: fs, from: from, to: to}
}

type Step struct {
	parent      *Filesystem
	from, to    *pdu.FilesystemVersion // from may be nil, indicating full send
	multi       bool
	resumeToken string // empty means no resume token shall be used

	expectedSize uint64 // 0 means no size estimate present / possible

	// byteCounter is nil initially, and set later in Step.doReplication
	// => concurrent read of that pointer from Step.ReportInfo must be protected
	byteCounter    *bytecounter.ReadCloser
	byteCounterMtx chainlock.L
}

func (self *Step) TargetEquals(other driver.Step) bool {
	t, ok := other.(*Step)
	if !ok {
		return false
	}
	if !self.parent.EqualToPreviousAttempt(t.parent) {
		panic("Step interface promise broken: parent filesystems must be same")
	}
	return self.from.GetGuid() == t.from.GetGuid() &&
		self.to.GetGuid() == t.to.GetGuid()
}

func (self *Step) TargetDate() time.Time { return self.to.SnapshotTime() }

func (self *Step) Step(ctx context.Context) error {
	return self.doReplication(ctx)
}

func (self *Step) ReportInfo() *report.StepInfo {
	// get current byteCounter value
	var byteCounter uint64
	self.byteCounterMtx.Lock()
	if self.byteCounter != nil {
		byteCounter = self.byteCounter.Count()
	}
	self.byteCounterMtx.Unlock()

	from := ""
	if self.from != nil {
		from = self.from.RelName()
	}
	return &report.StepInfo{
		From:            from,
		To:              self.to.RelName(),
		Resumed:         self.resumeToken != "",
		BytesExpected:   self.expectedSize,
		BytesReplicated: byteCounter,
	}
}

func (self *Step) Receiver() Receiver { return self.parent.receiver }

func (self *Step) Sender() Sender { return self.parent.sender }

func (self *Step) buildSendRequest() pdu.SendReq {
	return pdu.SendReq{
		Filesystem:        self.parent.Path,
		From:              self.from, // may be nil
		To:                self.to,
		Multi:             self.multi,
		ResumeToken:       self.resumeToken,
		ReplicationConfig: self.parent.policy.ReplicationConfig,
	}
}

func (self *Step) doReplication(ctx context.Context) error {
	sr := self.buildSendRequest()
	if err := self.sendRecv(ctx, &sr); err != nil {
		return err
	}

	log := getLogger(ctx).With(slog.String("filesystem", self.parent.Path))
	log.Debug("tell sender replication completed")
	err := self.Sender().SendCompleted(ctx,
		&pdu.SendCompletedReq{OriginalReq: &sr})
	if err != nil {
		logger.WithError(log, err,
			"error telling sender that replication completed successfully")
		return err
	}
	return nil
}

func (self *Step) sendRecv(ctx context.Context, sr *pdu.SendReq) error {
	log := getLogger(ctx).With(slog.String("filesystem", self.parent.Path))
	log.Debug("initiate send request")

	sres, stream, err := self.Sender().Send(ctx, sr)
	switch {
	case err != nil:
		logger.WithError(log, err, "send request failed")
		return err
	case sres == nil:
		err := errors.New("send request returned nil send result")
		log.Error(err.Error())
		return err
	case stream == nil:
		err := errors.New(
			"send request did not return a stream, broken endpoint implementation")
		return err
	}
	defer jsonclient.BodyClose(stream)

	// Install a byte counter to track progress + for status report
	byteCountingStream := bytecounter.NewReadCloser(stream)
	self.WithByteCounter(byteCountingStream)
	defer func() {
		defer self.byteCounterMtx.Lock().Unlock()
		if self.parent.promBytesReplicated != nil {
			self.parent.promBytesReplicated.Add(float64(self.byteCounter.Count()))
		}
	}()

	rr := pdu.ReceiveReq{
		Filesystem:        self.parent.Path,
		To:                sr.GetTo(),
		ClearResumeToken:  !sres.UsedResumeToken,
		ReplicationConfig: self.parent.policy.ReplicationConfig,
	}

	log.Debug("initiate receive request")
	if err := self.Receiver().Receive(ctx, &rr, byteCountingStream); err != nil {
		logger.WithError(
			log.With(slog.String("errType", fmt.Sprintf("%T", err)),
				slog.String("rr", fmt.Sprintf("%v", rr))),
			err, "receive request failed (might also be error on sender)",
		)
		// This failure could be due to
		// 	- an unexpected exit of ZFS on the sending side
		//  - an unexpected exit of ZFS on the receiving side
		//  - a connectivity issue
		return err
	}
	log.Debug("receive finished")
	return nil
}

func (self *Step) WithByteCounter(r *bytecounter.ReadCloser) *Step {
	self.byteCounterMtx.Lock()
	self.byteCounter = r
	self.byteCounterMtx.Unlock()
	return self
}

func (self *Step) String() string {
	if self.from == nil {
		// FIXME: ZFS semantics are that to is nil on non-incremental send
		return fmt.Sprintf("%s%s (full)",
			self.parent.Path, self.to.RelName())
	} else {
		return fmt.Sprintf("%s(%s => %s)",
			self.parent.Path, self.from.RelName(), self.to.RelName())
	}
}

func makeSteps(fs *Filesystem, prefix string, resume *Step,
	snaps []*pdu.FilesystemVersion,
) []*Step {
	steps := make([]*Step, 0, 2)
	if resume != nil {
		steps = append(steps, resume)
	}

	var last *Step
	for i := range len(snaps) - 1 {
		s := NewStep(fs, snaps[i], snaps[i+1])
		switch {
		case i == 0: // always add step
			steps, last = append(steps, s), s
			// for next steps last != nil
		case !strings.HasPrefix(last.to.Name, prefix):
			last.to = s.to
			if strings.HasPrefix(s.to.Name, prefix) {
				last = s
			}
			// for next steps last.to has prefix
		case last.from == nil: // initial replication - add step
			fallthrough
		case !strings.HasPrefix(last.from.Name, prefix):
			// first step with prefix after skipped aliens - add step
			fallthrough
		case last.from.Type == pdu.FilesystemVersion_Bookmark:
			// first step after bookmark
			// s.from == last.to and has prefix. s.to has unknown prefix.
			steps, last = append(steps, s), s
			// for next steps last.to has unknown prefix
		case prefix == "": // send -I latest snapshot
			// with empty prefix use latest snapshot and return early
			last.to, last.multi = snaps[len(snaps)-1], true
			return steps
		case strings.HasPrefix(s.to.Name, prefix): // send -I s.to
			// s.from and s.to have prefix
			last.to, last.multi = s.to, true
		default: // send -i s.to
			// s.to hasn't prefix
			steps, last = append(steps, s), s
		}
	}

	if last != nil && !strings.HasPrefix(last.to.Name, prefix) {
		steps = slices.Delete(steps, len(steps)-1, len(steps))
	}
	return steps
}
