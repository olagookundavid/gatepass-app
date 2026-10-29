package ticketpng

import (
	"bytes"
	"image/png"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/notifications/domain"
)

func TestRenderProducesAPNGOfTheExpectedSize(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	b, err := Renderer{}.Render(domain.Ticket{OrderID: "11111111-2222", EventID: "aaaaaaaa-bbbb", Quantity: 3, Token: domain.SignToken(key, "11111111-2222")})
	if err != nil {
		t.Fatal(err)
	}
	img, err := png.Decode(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("output is not a PNG: %v", err)
	}
	if img.Bounds().Dx() != width || img.Bounds().Dy() != height {
		t.Fatalf("size %v", img.Bounds())
	}
	if _, err := (Renderer{}).Render(domain.Ticket{}); err == nil {
		t.Fatal("ticket without token rendered")
	}
}

func TestEveryGlyphIsWellFormed(t *testing.T) {
	for r, g := range glyphs {
		for _, line := range g {
			if len(line) != 3 {
				t.Errorf("glyph %q has a row of width %d", r, len(line))
			}
		}
	}
}
