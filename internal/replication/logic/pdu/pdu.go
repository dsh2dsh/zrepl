package pdu

type ListFilesystemRes struct {
	Filesystems []*Filesystem `json:"Filesystems,omitempty"`
}

func (x *ListFilesystemRes) GetFilesystems() []*Filesystem {
	if x != nil {
		return x.Filesystems
	}
	return nil
}

type Filesystem struct {
	Path          string `json:"Path,omitempty"`
	ResumeToken   string `json:"ResumeToken,omitempty"`
	IsPlaceholder bool   `json:"IsPlaceholder,omitempty"`
}

func (x *Filesystem) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *Filesystem) GetResumeToken() string {
	if x != nil {
		return x.ResumeToken
	}
	return ""
}

func (x *Filesystem) GetIsPlaceholder() bool {
	if x != nil {
		return x.IsPlaceholder
	}
	return false
}

type ListFilesystemVersionsReq struct {
	Filesystem string `json:"Filesystem,omitempty"`
}

func (x *ListFilesystemVersionsReq) GetFilesystem() string {
	if x != nil {
		return x.Filesystem
	}
	return ""
}

type ListFilesystemVersionsRes struct {
	Versions []*FilesystemVersion `json:"Versions,omitempty"`
}

func (x *ListFilesystemVersionsRes) GetVersions() []*FilesystemVersion {
	if x != nil {
		return x.Versions
	}
	return nil
}

type FilesystemVersion struct {
	Type      FilesystemVersion_VersionType `json:"Type,omitempty"`
	Name      string                        `json:"Name,omitempty"`
	Guid      uint64                        `json:"Guid,omitempty"`
	CreateTXG uint64                        `json:"CreateTXG,omitempty"`
	Creation  string                        `json:"Creation,omitempty"` // RFC 3339
}

type FilesystemVersion_VersionType int32

const (
	FilesystemVersion_Snapshot FilesystemVersion_VersionType = 0
	FilesystemVersion_Bookmark FilesystemVersion_VersionType = 1
)

func (x *FilesystemVersion) GetType() FilesystemVersion_VersionType {
	if x != nil {
		return x.Type
	}
	return FilesystemVersion_Snapshot
}

func (x *FilesystemVersion) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *FilesystemVersion) GetGuid() uint64 {
	if x != nil {
		return x.Guid
	}
	return 0
}

func (x *FilesystemVersion) GetCreateTXG() uint64 {
	if x != nil {
		return x.CreateTXG
	}
	return 0
}

func (x *FilesystemVersion) GetCreation() string {
	if x != nil {
		return x.Creation
	}
	return ""
}

type SendReq struct {
	Filesystem string `json:"Filesystem,omitempty"`
	// May be empty / null to request a full transfer of To
	From *FilesystemVersion `json:"From,omitempty"`
	To   *FilesystemVersion `json:"To,omitempty"`
	// If ResumeToken is not empty, the resume token that CAN be used for 'zfs
	// send' by the sender. The sender MUST indicate use of ResumeToken in the
	// reply message SendRes.UsedResumeToken If it does not work, the sender
	// SHOULD clear the resume token on their side and use From and To instead If
	// ResumeToken is not empty, the GUIDs of From and To MUST correspond to those
	// encoded in the ResumeToken. Otherwise, the Sender MUST return an error.
	ResumeToken       string             `json:"ResumeToken,omitempty"`
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
}

func (x *SendReq) GetFilesystem() string {
	if x != nil {
		return x.Filesystem
	}
	return ""
}

func (x *SendReq) GetFrom() *FilesystemVersion {
	if x != nil {
		return x.From
	}
	return nil
}

func (x *SendReq) GetTo() *FilesystemVersion {
	if x != nil {
		return x.To
	}
	return nil
}

func (x *SendReq) GetResumeToken() string {
	if x != nil {
		return x.ResumeToken
	}
	return ""
}

func (x *SendReq) GetReplicationConfig() *ReplicationConfig {
	if x != nil {
		return x.ReplicationConfig
	}
	return nil
}

type ReplicationConfig struct {
	Protection *ReplicationConfigProtection `json:"protection,omitempty"`
}

type ReplicationConfigProtection struct {
	Initial     ReplicationGuaranteeKind `json:"Initial,omitempty"`
	Incremental ReplicationGuaranteeKind `json:"Incremental,omitempty"`
}

func (x *ReplicationConfigProtection) GetInitial() ReplicationGuaranteeKind {
	if x != nil {
		return x.Initial
	}
	return ReplicationGuaranteeKind_GuaranteeInvalid
}

func (x *ReplicationConfigProtection) GetIncremental() ReplicationGuaranteeKind {
	if x != nil {
		return x.Incremental
	}
	return ReplicationGuaranteeKind_GuaranteeInvalid
}

type ReplicationGuaranteeKind int32

const (
	ReplicationGuaranteeKind_GuaranteeInvalid                ReplicationGuaranteeKind = 0
	ReplicationGuaranteeKind_GuaranteeResumability           ReplicationGuaranteeKind = 1
	ReplicationGuaranteeKind_GuaranteeIncrementalReplication ReplicationGuaranteeKind = 2
	ReplicationGuaranteeKind_GuaranteeNothing                ReplicationGuaranteeKind = 3
)

// Enum value maps for ReplicationGuaranteeKind.
var (
	ReplicationGuaranteeKind_name = map[int32]string{
		0: "GuaranteeInvalid",
		1: "GuaranteeResumability",
		2: "GuaranteeIncrementalReplication",
		3: "GuaranteeNothing",
	}
	ReplicationGuaranteeKind_value = map[string]int32{
		"GuaranteeInvalid":                0,
		"GuaranteeResumability":           1,
		"GuaranteeIncrementalReplication": 2,
		"GuaranteeNothing":                3,
	}
)

func (self ReplicationGuaranteeKind) String() string {
	return ReplicationGuaranteeKind_name[int32(self)]
}

type SendRes struct {
	// Whether the resume token provided in the request has been used or not.
	// If the SendReq.ResumeToken == "", this field MUST be false.
	UsedResumeToken bool `json:"UsedResumeToken,omitempty"`
	// Expected stream size determined by dry run, not exact.
	// 0 indicates that for the given SendReq, no size estimate could be made.
	ExpectedSize uint64 `json:"ExpectedSize,omitempty"`
}

func (x *SendRes) GetUsedResumeToken() bool {
	if x != nil {
		return x.UsedResumeToken
	}
	return false
}

func (x *SendRes) GetExpectedSize() uint64 {
	if x != nil {
		return x.ExpectedSize
	}
	return 0
}

type SendCompletedReq struct {
	OriginalReq *SendReq `json:"OriginalReq,omitempty"`
}

func (x *SendCompletedReq) GetOriginalReq() *SendReq {
	if x != nil {
		return x.OriginalReq
	}
	return nil
}

type DestroySnapshotsReq struct {
	Filesystem string `json:"Filesystem,omitempty"`
	// Path to filesystem, snapshot or bookmark to be destroyed
	Snapshots []*FilesystemVersion `json:"Snapshots,omitempty"`
}

type DestroySnapshotsRes struct {
	Results []*DestroySnapshotRes `json:"Results,omitempty"`
}

type DestroySnapshotRes struct {
	Snapshot *FilesystemVersion `json:"Snapshot,omitempty"`
	Error    string             `json:"Error,omitempty"`
}

type ReplicationCursorReq struct {
	Filesystem string `json:"Filesystem,omitempty"`
}

func (x *ReplicationCursorReq) GetFilesystem() string {
	if x != nil {
		return x.Filesystem
	}
	return ""
}

type ReplicationCursorRes struct {
	Result *ReplicationCursorRes_Result `json:"Result"`
}

type ReplicationCursorRes_Result struct {
	Notexist bool   `json:"Notexist"`
	Guid     uint64 `json:"Guid"`
}

func (m *ReplicationCursorRes) GetResult() *ReplicationCursorRes_Result {
	if m != nil {
		return m.Result
	}
	return &ReplicationCursorRes_Result{}
}

func (x *ReplicationCursorRes) GetGuid() uint64 {
	return x.GetResult().Guid
}

func (x *ReplicationCursorRes) GetNotexist() bool {
	return x.GetResult().Notexist
}

type ReceiveReq struct {
	Filesystem string             `json:"Filesystem,omitempty"`
	To         *FilesystemVersion `json:"To,omitempty"`
	// If true, the receiver should clear the resume token before performing the
	// zfs recv of the stream in the request
	ClearResumeToken  bool               `json:"ClearResumeToken,omitempty"`
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
}

func (x *ReceiveReq) GetFilesystem() string {
	if x != nil {
		return x.Filesystem
	}
	return ""
}

func (x *ReceiveReq) GetTo() *FilesystemVersion {
	if x != nil {
		return x.To
	}
	return nil
}

func (x *ReceiveReq) GetClearResumeToken() bool {
	if x != nil {
		return x.ClearResumeToken
	}
	return false
}

func (x *ReceiveReq) GetReplicationConfig() *ReplicationConfig {
	if x != nil {
		return x.ReplicationConfig
	}
	return nil
}

type SendDryReq struct {
	Items       []SendReq `json:"Items"`
	Concurrency int       `json:"Concurrency"`
}

type SendDryRes struct {
	Items []SendRes `json:"Items"`
}
