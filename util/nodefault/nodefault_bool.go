package nodefault

import (
	"errors"
	"strconv"
)

type Bool struct{ B bool }

func (n *Bool) ValidateNoDefault() error {
	if n == nil {
		return errors.New("must explicitly set `true` or `false`")
	}
	return nil
}

func (n *Bool) String() string {
	if n == nil {
		return "unset"
	}
	return strconv.FormatBool(n.B)
}
