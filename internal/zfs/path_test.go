package zfs

func toDatasetPath(s string) *DatasetPath {
	p, err := NewDatasetPath(s)
	if err != nil {
		panic(err)
	}
	return p
}
