package status

import (
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func NewSimpleList(items []ListItem, w, h int, title string) *ListModel {
	d := list.NewDefaultDelegate()
	l := NewList(items, &d, w, h)
	l.InitDelegate(&d)

	ll := &l.list
	ll.Title = title
	ll.KeyMap.ShowFullHelp.Unbind()
	ll.KeyMap.CloseFullHelp.Unbind()
	ll.KeyMap.Quit = key.NewBinding(
		key.WithKeys("esc"),
		key.WithHelp("esc", "back"),
	)
	ll.SetFilteringEnabled(false)
	return l
}

type ListItem struct {
	Caption  string
	Desc     string
	Func     func() tea.Cmd
	ItemFunc func(item *ListItem) tea.Cmd
}

func (self *ListItem) Title() string       { return self.Caption }
func (self *ListItem) Description() string { return self.Desc }
func (self *ListItem) FilterValue() string { return self.Caption }

func NewList(items []ListItem, d list.ItemDelegate, w, h int) *ListModel {
	s := &ListModel{
		Choose: key.NewBinding(key.WithKeys("enter"),
			key.WithHelp("enter", "choose")),
	}
	return s.init(items, d, w, h)
}

type ListModel struct {
	Choose key.Binding
	Style  lipgloss.Style

	items []ListItem
	list  list.Model

	itemFunc   func(item *ListItem) tea.Cmd
	backToFunc func()
}

func (self *ListModel) init(items []ListItem, d list.ItemDelegate, w, h int,
) *ListModel {
	self.items = items
	l := list.New(makeListItems(self.items), d, w, h)
	l.AdditionalShortHelpKeys = self.helpKeys
	l.AdditionalFullHelpKeys = self.helpKeys
	l.Filter = list.UnsortedFilter
	l.SetShowStatusBar(false)
	self.list = l
	return self
}

func makeListItems(items []ListItem) []list.Item {
	listItems := make([]list.Item, len(items))
	for i := range items {
		listItems[i] = &items[i]
	}
	return listItems
}

func (self *ListModel) InitDelegate(d *list.DefaultDelegate) {
	d.UpdateFunc = self.updateDelegate
}

func (self *ListModel) updateDelegate(msg tea.Msg, m *list.Model) tea.Cmd {
	if msg, ok := msg.(tea.KeyMsg); ok && key.Matches(msg, self.Choose) {
		if item, ok := m.SelectedItem().(*ListItem); ok {
			switch {
			case item.Func != nil:
				return item.Func()
			case item.ItemFunc != nil:
				return item.ItemFunc(item)
			case self.itemFunc != nil:
				return self.itemFunc(item)
			}
		}
	}
	return nil
}

func (self *ListModel) helpKeys() []key.Binding {
	return []key.Binding{self.Choose}
}

func (self *ListModel) WithBackTo(fn func()) *ListModel {
	self.backToFunc = fn
	return self
}

func (self *ListModel) WithItemFunc(fn func(item *ListItem) tea.Cmd,
) *ListModel {
	self.itemFunc = fn
	return self
}

func (self *ListModel) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if self.list.FilterState() != list.Filtering {
			return self.handleKeys(msg)
		}
	case tea.WindowSizeMsg:
		w, h := self.Style.GetFrameSize()
		self.list.SetSize(msg.Width-w, msg.Height-h)
	}

	var cmd tea.Cmd
	self.list, cmd = self.list.Update(msg)
	return cmd
}

func (self *ListModel) handleKeys(msg tea.KeyMsg) tea.Cmd {
	l := &self.list
	switch {
	// Note: we match clear filter before quit because, by default, they're
	// both mapped to escape.
	case key.Matches(msg, l.KeyMap.ClearFilter) && l.FilterState() == list.FilterApplied:
		// do nothing and pass to the list's resetFilter()
	case key.Matches(msg, l.KeyMap.Quit):
		if self.backToFunc == nil {
			return tea.Quit
		}
		self.backToFunc()
		return nil
	}

	var cmd tea.Cmd
	self.list, cmd = l.Update(msg)
	return cmd
}

func (self *ListModel) View() string {
	return self.Style.Render(self.list.View())
}

func (self *ListModel) List() *list.Model {
	return &self.list
}

func (self *ListModel) SetItems(items []ListItem) {
	self.items = items
	self.list.SetItems(makeListItems(self.items))
}
