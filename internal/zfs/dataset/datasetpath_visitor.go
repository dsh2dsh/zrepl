package dataset

type PathForest struct {
	roots []*pathTree
}

func NewPathForest() *PathForest {
	return &PathForest{
		make([]*pathTree, 0),
	}
}

func (f *PathForest) Add(p *Path) {
	if len(p.comps) == 0 {
		panic("dataset path too short. must have length > 0")
	}

	// Find its root
	var root *pathTree
	for _, r := range f.roots {
		if r.Add(p.comps) {
			root = r
			break
		}
	}
	if root == nil {
		root = newPathTree(p.comps)
		f.roots = append(f.roots, root)
	}
}

type PathVisit struct {
	Path *Path
	// If true, the dataset referenced by Path was not in the list of datasets to traverse
	FilledIn bool
	Parent   *PathVisit
}

type PathsVisitor func(v *PathVisit) (visitChildTree bool)

// Traverse a list of DatasetPaths top down, i.e. given a set of datasets with same
// path prefix, those with shorter prefix are traversed first.
// If there are gaps, i.e. the intermediary component a/b between a and a/b/c,
// those gaps are still visited but the FilledIn property of the visit is set to true.
func (f *PathForest) WalkTopDown(visitor PathsVisitor) {
	for _, r := range f.roots {
		r.WalkTopDown(&PathVisit{
			Path:     new(Path),
			FilledIn: true,
			Parent:   nil,
		}, visitor)
	}
}

/* PRIVATE IMPLEMENTATION */

type pathTree struct {
	Component string
	FilledIn  bool
	Children  []*pathTree
}

func (t *pathTree) Add(p []string) bool {
	if len(p) == 0 {
		return true
	}

	if p[0] == t.Component {

		remainder := p[1:]
		if len(remainder) == 0 {
			t.FilledIn = false
			return true
		}

		for _, c := range t.Children {
			if c.Add(remainder) {
				return true
			}
		}

		t.Children = append(t.Children, newPathTree(remainder))
		return true

	} else {
		return false
	}
}

func (t *pathTree) WalkTopDown(parent *PathVisit, visitor PathsVisitor) {
	thisVisitPath := parent.Path.Copy()
	thisVisitPath.Extend(&Path{comps: []string{t.Component}})

	thisVisit := &PathVisit{
		thisVisitPath,
		t.FilledIn,
		parent,
	}
	visitChildTree := visitor(thisVisit)

	if visitChildTree {
		for _, c := range t.Children {
			c.WalkTopDown(thisVisit, visitor)
		}
	}
}

func newPathTree(initialComps []string) (t *pathTree) {
	t = &pathTree{}
	cur := t
	for i, comp := range initialComps {
		cur.Component = comp
		cur.FilledIn = true
		cur.Children = make([]*pathTree, 0, 1)
		if i == len(initialComps)-1 {
			cur.FilledIn = false // last component is not filled in
			break
		}
		child := &pathTree{}
		cur.Children = append(cur.Children, child)
		cur = child
	}
	return t
}
