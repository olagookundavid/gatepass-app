// Package ticketpng (notifications adapter) implements ports.Renderer: it draws
// the ticket as a PNG with a QR code, using only the Go standard library plus
// github.com/skip2/go-qrcode for the QR matrix.
//
// HEXAGONAL BOUNDARY: adapter (pure CPU, no I/O).
//
// There is no text-rendering library in the approved dependency list, so the few
// human-readable lines use a tiny built-in 3x5 pixel font (digits, A-Z, a few
// symbols), scaled up. The QR code carries the signed token; the text is a
// convenience for people at the door.
package ticketpng

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"strings"
	"unicode"

	qrcode "github.com/skip2/go-qrcode"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
)

// Renderer implements ports.Renderer.
type Renderer struct{}

var _ ports.Renderer = Renderer{}

const (
	width    = 640
	height   = 820
	qrSize   = 480
	headerH  = 110
	fontCell = 6 // pixels per font dot
)

var (
	ink    = color.RGBA{R: 20, G: 24, B: 40, A: 255}
	paper  = color.RGBA{R: 255, G: 255, B: 255, A: 255}
	accent = color.RGBA{R: 255, G: 255, B: 255, A: 255}
)

// Render draws the ticket.
func (Renderer) Render(t domain.Ticket) ([]byte, error) {
	if t.Token == "" {
		return nil, fmt.Errorf("ticket has no token")
	}
	qr, err := qrcode.New(t.Token, qrcode.Medium)
	if err != nil {
		return nil, fmt.Errorf("encode qr: %w", err)
	}
	qr.DisableBorder = false
	qrImg := qr.Image(qrSize)

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(img, img.Bounds(), &image.Uniform{paper}, image.Point{}, draw.Src)
	draw.Draw(img, image.Rect(0, 0, width, headerH), &image.Uniform{ink}, image.Point{}, draw.Src)
	drawText(img, 40, 30, "GATEPASS TICKET", fontCell+2, accent)

	qrX := (width - qrSize) / 2
	draw.Draw(img, image.Rect(qrX, headerH+30, qrX+qrSize, headerH+30+qrSize), qrImg, image.Point{}, draw.Src)

	y := headerH + 30 + qrSize + 40
	for _, line := range []string{
		"ORDER " + strings.ToUpper(short(t.OrderID)),
		"EVENT " + strings.ToUpper(short(t.EventID)),
		fmt.Sprintf("TICKETS %d", t.Quantity),
	} {
		drawText(img, 40, y, line, fontCell, ink)
		y += 7 * fontCell
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// glyphs: 3 columns x 5 rows per character. 'X' = dot.
var glyphs = map[rune][5]string{
	'0': {"XXX", "X.X", "X.X", "X.X", "XXX"}, '1': {".X.", "XX.", ".X.", ".X.", "XXX"},
	'2': {"XXX", "..X", "XXX", "X..", "XXX"}, '3': {"XXX", "..X", "XXX", "..X", "XXX"},
	'4': {"X.X", "X.X", "XXX", "..X", "..X"}, '5': {"XXX", "X..", "XXX", "..X", "XXX"},
	'6': {"XXX", "X..", "XXX", "X.X", "XXX"}, '7': {"XXX", "..X", "..X", ".X.", ".X."},
	'8': {"XXX", "X.X", "XXX", "X.X", "XXX"}, '9': {"XXX", "X.X", "XXX", "..X", "XXX"},
	'A': {".X.", "X.X", "XXX", "X.X", "X.X"}, 'B': {"XX.", "X.X", "XX.", "X.X", "XX."},
	'C': {".XX", "X..", "X..", "X..", ".XX"}, 'D': {"XX.", "X.X", "X.X", "X.X", "XX."},
	'E': {"XXX", "X..", "XX.", "X..", "XXX"}, 'F': {"XXX", "X..", "XX.", "X..", "X.."},
	'G': {".XX", "X..", "X.X", "X.X", ".XX"}, 'H': {"X.X", "X.X", "XXX", "X.X", "X.X"},
	'I': {"XXX", ".X.", ".X.", ".X.", "XXX"}, 'J': {"..X", "..X", "..X", "X.X", ".X."},
	'K': {"X.X", "X.X", "XX.", "X.X", "X.X"}, 'L': {"X..", "X..", "X..", "X..", "XXX"},
	'M': {"X.X", "XXX", "XXX", "X.X", "X.X"}, 'N': {"XX.", "X.X", "X.X", "X.X", "X.X"},
	'O': {".X.", "X.X", "X.X", "X.X", ".X."}, 'P': {"XX.", "X.X", "XX.", "X..", "X.."},
	'Q': {".X.", "X.X", "X.X", "XX.", ".XX"}, 'R': {"XX.", "X.X", "XX.", "X.X", "X.X"},
	'S': {".XX", "X..", ".X.", "..X", "XX."}, 'T': {"XXX", ".X.", ".X.", ".X.", ".X."},
	'U': {"X.X", "X.X", "X.X", "X.X", "XXX"}, 'V': {"X.X", "X.X", "X.X", "X.X", ".X."},
	'W': {"X.X", "X.X", "XXX", "XXX", "X.X"}, 'X': {"X.X", "X.X", ".X.", "X.X", "X.X"},
	'Y': {"X.X", "X.X", ".X.", ".X.", ".X."}, 'Z': {"XXX", "..X", ".X.", "X..", "XXX"},
	'-': {"...", "...", "XXX", "...", "..."}, ' ': {"...", "...", "...", "...", "..."},
	':': {"...", ".X.", "...", ".X.", "..."}, '.': {"...", "...", "...", "...", ".X."},
}

func drawText(img *image.RGBA, x, y int, s string, cell int, c color.Color) {
	for _, r := range s {
		g, ok := glyphs[unicode.ToUpper(r)]
		if !ok {
			g = glyphs[' ']
		}
		for row, line := range g {
			for col, ch := range line {
				if ch == 'X' {
					rect := image.Rect(x+col*cell, y+row*cell, x+(col+1)*cell, y+(row+1)*cell)
					draw.Draw(img, rect, &image.Uniform{c}, image.Point{}, draw.Src)
				}
			}
		}
		x += 4 * cell
	}
}
