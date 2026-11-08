package dataset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPathTree(t *testing.T) {
	r := newPathTree(toPath("pool1/foo/bar").comps)

	assert.Equal(t, "pool1", r.Component)
	assert.Len(t, r.Children, 1)
	cr := r.Children[0]
	assert.Equal(t, "foo", cr.Component)
	assert.Len(t, cr.Children, 1)
	ccr := cr.Children[0]
	assert.Equal(t, "bar", ccr.Component)
}

func toPath(s string) *Path {
	p, err := NewPath(s)
	if err != nil {
		panic(err)
	}
	return p
}

type visitRecorder struct {
	visits []PathVisit
}

func makeVisitRecorder() (v PathsVisitor, rec *visitRecorder) {
	rec = &visitRecorder{
		visits: make([]PathVisit, 0),
	}
	v = func(v *PathVisit) bool {
		rec.visits = append(rec.visits, *v)
		return true
	}
	return v, rec
}

func buildForest(paths []*Path) (f *PathForest) {
	f = NewPathForest()
	for _, p := range paths {
		f.Add(p)
	}
	return f
}

type expectedPathVisit struct {
	expectPath   *Path
	expectFillIn bool
}

func (e *expectedPathVisit) AssertEqual(t *testing.T, actual PathVisit) {
	assert.Equal(t, e.expectPath, actual.Path)
	assert.Equal(t, e.expectFillIn, actual.FilledIn)
	if actual.Parent != nil {
		assert.Equal(t, actual.Parent.Path.Length()+1, e.expectPath.Length())
		assert.True(t, e.expectPath.HasPrefix(actual.Parent.Path))
	}
}

func expectedPathVisits(t *testing.T, expected []expectedPathVisit, actual []PathVisit) {
	assert.Len(t, actual, len(expected))
	for i := range expected {
		expected[i].AssertEqual(t, actual[i])
	}
}

func TestPathForestWalkTopDown(t *testing.T) {
	paths := []*Path{
		toPath("pool1"),
		toPath("pool1/foo/bar"),
		toPath("pool1/foo/bar/looloo"),
		toPath("pool2/test/bar"),
	}

	v, rec := makeVisitRecorder()

	buildForest(paths).WalkTopDown(v)

	expectedVisits := []expectedPathVisit{
		{toPath("pool1"), false},
		{toPath("pool1/foo"), true},
		{toPath("pool1/foo/bar"), false},
		{toPath("pool1/foo/bar/looloo"), false},
		{toPath("pool2"), true},
		{toPath("pool2/test"), true},
		{toPath("pool2/test/bar"), false},
	}
	expectedPathVisits(t, expectedVisits, rec.visits)
}

func TestPathWalkTopDownWorksUnordered(t *testing.T) {
	paths := []*Path{
		toPath("pool1"),
		toPath("pool1/foo/bar/looloo"),
		toPath("pool1/foo/bar"),
		toPath("pool1/bang/baz"),
	}

	v, rec := makeVisitRecorder()

	buildForest(paths).WalkTopDown(v)

	expectedVisits := []expectedPathVisit{
		{toPath("pool1"), false},
		{toPath("pool1/foo"), true},
		{toPath("pool1/foo/bar"), false},
		{toPath("pool1/foo/bar/looloo"), false},
		{toPath("pool1/bang"), true},
		{toPath("pool1/bang/baz"), false},
	}

	expectedPathVisits(t, expectedVisits, rec.visits)
}
