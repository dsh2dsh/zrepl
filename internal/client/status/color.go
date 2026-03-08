package status

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

type lightDarkFunc func(light, dark string) color.Color

func makeLightDark(darkMode bool) lightDarkFunc {
	lightDark := lipgloss.LightDark(darkMode)
	return func(light, dark string) color.Color {
		return lightDark(lipgloss.Color(light), lipgloss.Color(dark))
	}
}
