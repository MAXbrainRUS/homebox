package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sysadminsmedia/homebox/backend/internal/data/ent/enttest"
	"github.com/sysadminsmedia/homebox/backend/internal/sys/config"
	"gocloud.dev/blob"
	"golang.org/x/image/webp"
)

func TestProcessThumbnailFromImageHEICOrientation(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	client := enttest.Open(t, "sqlite3", "file:heic_thumb?mode=memory&cache=shared&_fk=1")
	defer func() {
		_ = client.Close()
	}()

	groupID := uuid.New()
	if _, err := client.Group.Create().SetID(groupID).SetName("Test Group").SetCurrency("usd").Save(ctx); err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	tempDir := t.TempDir()
	repo := AttachmentRepo{
		db:        client,
		storage:   config.Storage{ConnString: fmt.Sprintf("file://%s", tempDir)},
		thumbnail: config.Thumbnail{Enabled: true, Width: 512, Height: 512},
	}

	orientation, err := readImageOrientation(heicPortraitFixture(), "image/heic")
	if err != nil && !errors.Is(err, errHeicOrientationNotFound) {
		t.Fatalf("unexpected orientation error: %v", err)
	}
	if orientation <= 1 {
		t.Fatalf("expected portrait orientation, got %d", orientation)
	}

	baseImage := image.NewRGBA(image.Rect(0, 0, 1600, 900))

	thumbPath, err := repo.processThumbnailFromImage(ctx, groupID, baseImage, "portrait.heic", orientation)
	if err != nil {
		t.Fatalf("processThumbnailFromImage failed: %v", err)
	}

	bucket, err := blob.OpenBucket(ctx, repo.GetConnString())
	if err != nil {
		t.Fatalf("failed to open bucket: %v", err)
	}
	defer func() {
		_ = bucket.Close()
	}()

	data, err := bucket.ReadAll(ctx, repo.fullPath(thumbPath))
	if err != nil {
		t.Fatalf("failed to read thumbnail: %v", err)
	}

	decoded, err := webp.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to decode thumbnail: %v", err)
	}

	bounds := decoded.Bounds()
	if bounds.Dy() <= bounds.Dx() {
		t.Fatalf("expected portrait thumbnail dimensions, got %dx%d", bounds.Dx(), bounds.Dy())
	}
}
