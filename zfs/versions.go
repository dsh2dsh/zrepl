package zfs

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type VersionType string

const (
	Bookmark VersionType = "bookmark"
	Snapshot VersionType = "snapshot"
)

type VersionTypeSet map[VersionType]bool

var (
	AllVersionTypes = VersionTypeSet{
		Bookmark: true,
		Snapshot: true,
	}
	Bookmarks = VersionTypeSet{
		Bookmark: true,
	}
	Snapshots = VersionTypeSet{
		Snapshot: true,
	}
)

func (s VersionTypeSet) zfsListTFlagRepr() string {
	types := make([]string, 0, len(s))
	for t := range s {
		types = append(types, t.String())
	}
	sort.StringSlice(types).Sort()
	return strings.Join(types, ",")
}
func (s VersionTypeSet) String() string { return s.zfsListTFlagRepr() }

func (t VersionType) DelimiterChar() string {
	switch t {
	case Bookmark:
		return "#"
	case Snapshot:
		return "@"
	default:
		panic(fmt.Sprintf("unexpected VersionType %#v", t))
	}
}

func (t VersionType) String() string {
	return string(t)
}

func DecomposeVersionString(v string) (fs string, versionType VersionType, name string, err error) {
	if len(v) < 3 {
		err = fmt.Errorf("snapshot or bookmark name implausibly short: %s", v)
		return
	}

	snapSplit := strings.SplitN(v, "@", 2)
	bookmarkSplit := strings.SplitN(v, "#", 2)
	if len(snapSplit)*len(bookmarkSplit) != 2 {
		err = fmt.Errorf("dataset cannot be snapshot and bookmark at the same time: %s", v)
		return
	}

	if len(snapSplit) == 2 {
		return snapSplit[0], Snapshot, snapSplit[1], nil
	} else {
		return bookmarkSplit[0], Bookmark, bookmarkSplit[1], nil
	}
}

// The data in a FilesystemVersion is guaranteed to stem from a ZFS CLI invocation.
type FilesystemVersion struct {
	Type VersionType

	// Display name. Should not be used for identification, only for user output
	Name string

	// GUID as exported by ZFS. Uniquely identifies a snapshot across pools
	Guid uint64

	// The TXG in which the snapshot was created. For bookmarks,
	// this is the GUID of the snapshot it was initially tied to.
	CreateTXG uint64

	// The time the dataset was created
	Creation time.Time

	// userrefs field (snapshots only)
	UserRefs OptionUint64
}

type OptionUint64 struct {
	Value uint64
	Valid bool
}

func (v FilesystemVersion) GetCreateTXG() uint64 { return v.CreateTXG }
func (v FilesystemVersion) GetGUID() uint64      { return v.Guid }
func (v FilesystemVersion) GetGuid() uint64      { return v.Guid }
func (v FilesystemVersion) GetName() string      { return v.Name }
func (v FilesystemVersion) IsSnapshot() bool     { return v.Type == Snapshot }
func (v FilesystemVersion) IsBookmark() bool     { return v.Type == Bookmark }
func (v FilesystemVersion) RelName() string {
	return fmt.Sprintf("%s%s", v.Type.DelimiterChar(), v.Name)
}
func (v FilesystemVersion) String() string { return v.RelName() }

// Only takes into account those attributes of FilesystemVersion that
// are immutable over time in ZFS.
func FilesystemVersionEqualIdentity(a, b FilesystemVersion) bool {
	// .Name is mutable
	return a.Guid == b.Guid && a.CreateTXG == b.CreateTXG && a.Creation == b.Creation
}

func (v FilesystemVersion) ToAbsPath(p *DatasetPath) string {
	var b bytes.Buffer
	b.WriteString(p.ToString())
	b.WriteString(v.Type.DelimiterChar())
	b.WriteString(v.Name)
	return b.String()
}

func (v FilesystemVersion) FullPath(fs string) string {
	return fmt.Sprintf("%s%s", fs, v.RelName())
}

func (v FilesystemVersion) ToSendArgVersion() ZFSSendArgVersion {
	return ZFSSendArgVersion{
		RelName: v.RelName(),
		GUID:    v.Guid,
	}
}

type ParseFilesystemVersionArgs struct {
	fullname                            string
	guid, createtxg, creation, userrefs string
}

func (self ParseFilesystemVersionArgs) WithFullName(s string,
) ParseFilesystemVersionArgs {
	self.fullname = s
	return self
}

func (self ParseFilesystemVersionArgs) WithGuid(s string,
) ParseFilesystemVersionArgs {
	self.guid = s
	return self
}

func (self ParseFilesystemVersionArgs) WithCreateTxg(s string,
) ParseFilesystemVersionArgs {
	self.createtxg = s
	return self
}

func (self ParseFilesystemVersionArgs) WithCreation(s string,
) ParseFilesystemVersionArgs {
	self.creation = s
	return self
}

func (self ParseFilesystemVersionArgs) WithUserRefs(s string,
) ParseFilesystemVersionArgs {
	self.userrefs = s
	return self
}

func (self ParseFilesystemVersionArgs) Parse() (FilesystemVersion, error) {
	return ParseFilesystemVersion(self)
}

func ParseFilesystemVersion(args ParseFilesystemVersionArgs,
) (v FilesystemVersion, err error) {
	_, v.Type, v.Name, err = DecomposeVersionString(args.fullname)
	if err != nil {
		return
	}

	if v.Guid, err = strconv.ParseUint(args.guid, 10, 64); err != nil {
		err = fmt.Errorf("cannot parse GUID %q: %w", args.guid, err)
		return
	}

	if v.CreateTXG, err = strconv.ParseUint(args.createtxg, 10, 64); err != nil {
		err = fmt.Errorf("cannot parse CreateTXG %q: %w", args.createtxg, err)
		return
	}

	creationUnix, err := strconv.ParseInt(args.creation, 10, 64)
	if err != nil {
		err = fmt.Errorf("cannot parse creation date %q: %w", args.creation, err)
		return
	} else {
		v.Creation = time.Unix(creationUnix, 0)
	}

	switch v.Type {
	case Bookmark:
		if args.userrefs != "-" {
			err = fmt.Errorf(
				"expecting %q for bookmark property userrefs, got %q", "-", args.userrefs)
			return
		}
		v.UserRefs = OptionUint64{Valid: false}
	case Snapshot:
		if v.UserRefs.Value, err = strconv.ParseUint(args.userrefs, 10, 64); err != nil {
			err = fmt.Errorf("cannot parse userrefs %q: %w", args.userrefs, err)
			return
		}
		v.UserRefs.Valid = true
	default:
		panic(v.Type)
	}
	return v, nil
}

type ListFilesystemVersionsOptions struct {
	// the prefix of the version name, without the delimiter char
	// empty means any prefix matches
	ShortnamePrefix string

	// which types should be returned
	// nil or len(0) means any prefix matches
	Types VersionTypeSet
}

func (o *ListFilesystemVersionsOptions) typesFlagArgs() string {
	if len(o.Types) == 0 {
		return AllVersionTypes.zfsListTFlagRepr()
	} else {
		return o.Types.zfsListTFlagRepr()
	}
}

func (o *ListFilesystemVersionsOptions) matches(v FilesystemVersion) bool {
	return (len(o.Types) == 0 || o.Types[v.Type]) && strings.HasPrefix(v.Name, o.ShortnamePrefix)
}

// ZFSListFilesystemVersions returns versions are sorted by createtxg.
//
// FIXME drop sort by createtxg requirement
func ZFSListFilesystemVersions(ctx context.Context, fs *DatasetPath,
	options ListFilesystemVersionsOptions,
) ([]FilesystemVersion, error) {
	promTimer := prometheus.NewTimer(
		prom.ZFSListFilesystemVersionDuration.WithLabelValues(fs.ToString()))
	defer promTimer.ObserveDuration()

	listResults := ZFSListIter(ctx,
		[]string{"name", "guid", "createtxg", "creation", "userrefs"},
		fs,
		"-r", "-d", "1",
		"-t", options.typesFlagArgs(),
		"-s", "createtxg", fs.ToString())

	res := []FilesystemVersion{}
	for r := range listResults {
		if r.Err != nil {
			return nil, r.Err
		}
		line := r.Fields
		var args ParseFilesystemVersionArgs
		args = args.
			WithFullName(line[0]).
			WithGuid(line[1]).
			WithCreateTxg(line[2]).
			WithCreation(line[3]).
			WithUserRefs(line[4])
		if v, err := args.Parse(); err != nil {
			return nil, err
		} else if options.matches(v) {
			res = append(res, v)
		}
	}
	return res, nil
}

func ZFSGetFilesystemVersion(ctx context.Context, ds string,
) (v FilesystemVersion, err error) {
	props, err := zfsGet(ctx, ds,
		[]string{"createtxg", "guid", "creation", "userrefs"}, SourceAny)
	if err != nil {
		return
	}
	var args ParseFilesystemVersionArgs
	return args.
		WithFullName(ds).
		WithCreateTxg(props.Get("createtxg")).
		WithGuid(props.Get("guid")).
		WithCreation(props.Get("creation")).
		WithUserRefs(props.Get("userrefs")).
		Parse()
}
