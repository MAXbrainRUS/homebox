package repo

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/evanoberholster/imagemeta"
	"github.com/evanoberholster/imagemeta/isobmff"
	"github.com/gen2brain/avif"
	"github.com/gen2brain/heic"
	"github.com/gen2brain/jpegxl"
	"github.com/gen2brain/webp"
	"github.com/rs/zerolog/log"
	"github.com/sysadminsmedia/homebox/backend/internal/data/ent/group"
	"github.com/sysadminsmedia/homebox/backend/internal/sys/config"
	"github.com/sysadminsmedia/homebox/backend/pkgs/utils"
	"github.com/zeebo/blake3"
	"golang.org/x/image/draw"

	"github.com/google/uuid"
	"github.com/sysadminsmedia/homebox/backend/internal/data/ent"
	"github.com/sysadminsmedia/homebox/backend/internal/data/ent/attachment"
	"github.com/sysadminsmedia/homebox/backend/internal/data/ent/item"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
	_ "gocloud.dev/pubsub/mempubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

// AttachmentRepo is a repository for Attachments table that links Items to their
// associated files while also specifying the type of the attachment.
type AttachmentRepo struct {
	db         *ent.Client
	storage    config.Storage
	pubSubConn string
	thumbnail  config.Thumbnail
}

type (
	ItemAttachment struct {
		ID        uuid.UUID       `json:"id"`
		CreatedAt time.Time       `json:"createdAt"`
		UpdatedAt time.Time       `json:"updatedAt"`
		Type      string          `json:"type"`
		Primary   bool            `json:"primary"`
		Path      string          `json:"path"`
		Title     string          `json:"title"`
		MimeType  string          `json:"mimeType,omitempty"`
		Thumbnail *ent.Attachment `json:"thumbnail,omitempty"`
	}

	ItemAttachmentUpdate struct {
		ID      uuid.UUID `json:"-"`
		Type    string    `json:"type"`
		Title   string    `json:"title"`
		Primary bool      `json:"primary"`
	}

	ItemCreateAttachment struct {
		Title   string    `json:"title"`
		Content io.Reader `json:"content"`
	}
)

func ToItemAttachment(attachment *ent.Attachment) ItemAttachment {
	return ItemAttachment{
		ID:        attachment.ID,
		CreatedAt: attachment.CreatedAt,
		UpdatedAt: attachment.UpdatedAt,
		Type:      attachment.Type.String(),
		Primary:   attachment.Primary,
		Path:      attachment.Path,
		Title:     attachment.Title,
		MimeType:  attachment.MimeType,
		Thumbnail: attachment.QueryThumbnail().FirstX(context.Background()),
	}
}

func (r *AttachmentRepo) path(gid uuid.UUID, hash string) string {
	return filepath.Join(gid.String(), "documents", hash)
}

func (r *AttachmentRepo) fullPath(relativePath string) string {
	return filepath.Join(r.storage.PrefixPath, relativePath)
}

func (r *AttachmentRepo) GetFullPath(relativePath string) string {
	return r.fullPath(relativePath)
}

func (r *AttachmentRepo) GetConnString() string {
	// Handle the default case for file storage
	// which is file:///./ meaning relative to the current working directory
	if strings.HasPrefix(r.storage.ConnString, "file:///./") {
		dir, err := filepath.Abs(strings.TrimPrefix(r.storage.ConnString, "file:///./"))
		if runtime.GOOS == "windows" {
			dir = fmt.Sprintf("/%s", dir)
		}
		if err != nil {
			log.Err(err).Msg("failed to get absolute path for attachment directory")
			return r.storage.ConnString
		}
		return strings.ReplaceAll(fmt.Sprintf("file://%s?no_tmp_dir=true", dir), "\\", "/")
	} else if strings.HasPrefix(r.storage.ConnString, "file://") {
		// Handle the case for file storage with an absolute path
		// Convert Windows paths to a format compatible with fileblob
		// e.g. file:///C:/path/to/file becomes file:///C/path
		dir := strings.TrimPrefix(strings.ReplaceAll(r.storage.ConnString, "\\", "/"), "file://")
		if runtime.GOOS == "windows" {
			// Remove the colon from the drive letter (in case the user adds it)
			dir = strings.ReplaceAll(dir, ":", "")
			// Ensure the path starts with a slash for Windows compatibility
			dir = fmt.Sprintf("/%s", dir)
		}
		return fmt.Sprintf("file://%s", dir)
	}
	return r.storage.ConnString
}

func (r *AttachmentRepo) Create(ctx context.Context, itemID uuid.UUID, doc ItemCreateAttachment, typ attachment.Type, primary bool) (*ent.Attachment, error) {
	tx, err := r.db.Tx(ctx)
	if err != nil {
		return nil, err
	}

	// If there is an error during file creation rollback the database
	defer func() {
		if v := recover(); v != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}
	}()

	bldrId := uuid.New()

	now := time.Now()
	setPrimary := false
	buildAttachment := func(path string, mimeType string) *ent.AttachmentCreate {
		builder := tx.Attachment.Create().
			SetID(bldrId).
			SetCreatedAt(now).
			SetUpdatedAt(now).
			SetType(typ).
			SetItemID(itemID).
			SetTitle(doc.Title).
			SetPath(path).
			SetMimeType(mimeType)
		if setPrimary {
			builder = builder.SetPrimary(true)
		}
		return builder
	}

	if typ == attachment.TypePhoto && primary {
		setPrimary = true
		err := r.db.Attachment.Update().
			Where(
				attachment.HasItemWith(item.ID(itemID)),
				attachment.IDNEQ(bldrId),
			).
			SetPrimary(false).
			Exec(ctx)
		if err != nil {
			log.Err(err).Msg("failed to remove primary from other attachments")
			err := tx.Rollback()
			if err != nil {
				return nil, err
			}
			return nil, err
		}
	} else if typ == attachment.TypePhoto {
		// Autoset primary to true if this is the first attachment
		// that is of type photo
		cnt, err := tx.Attachment.Query().
			Where(
				attachment.HasItemWith(item.ID(itemID)),
				attachment.TypeEQ(typ),
			).
			Count(ctx)
		if err != nil {
			log.Err(err).Msg("failed to count attachments")
			err := tx.Rollback()
			if err != nil {
				return nil, err
			}
			return nil, err
		}

		if cnt == 0 {
			setPrimary = true
		}
	}

	// Get the group ID for the item the attachment is being created for
	itemGroup, err := tx.Item.Query().QueryGroup().Where(group.HasItemsWith(item.ID(itemID))).First(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get item group")
		err := tx.Rollback()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	// Upload the file to the storage bucket
	path, err := r.UploadFile(ctx, itemGroup, doc)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	limitedReader := io.LimitReader(doc.Content, 1024*128)
	file, err := io.ReadAll(limitedReader)
	if err != nil {
		log.Err(err).Msg("failed to read file content")
		err = tx.Rollback()
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	mimeType := http.DetectContentType(file[:min(512, len(file))])

	const maxAttachmentSaveRetries = 10
	const retryDelay = 100 * time.Millisecond

	var attachmentDb *ent.Attachment
	var saveErr error
	for attempt := 0; attempt < maxAttachmentSaveRetries; attempt++ {
		attachmentDb, saveErr = buildAttachment(path, mimeType).Save(ctx)
		if saveErr == nil {
			break
		}
		if !isSQLiteBusy(saveErr) {
			break
		}
		log.Warn().Err(saveErr).Int("attempt", attempt+1).Msg("retrying attachment save due to sqlite busy")
		backoff := retryDelay * time.Duration(attempt+1)
		time.Sleep(backoff)
	}
	if saveErr != nil {
		log.Err(saveErr).Msg("failed to save attachment to database")
		err = tx.Rollback()
		if err != nil {
			return nil, err
		}
		return nil, saveErr
	}

	if err := tx.Commit(); err != nil {
		log.Err(err).Msg("failed to commit transaction")
		return nil, err
	}

	if r.thumbnail.Enabled {
		pubsubString, err := utils.GenerateSubPubConn(r.pubSubConn, "thumbnails")
		if err != nil {
			log.Err(err).Msg("failed to generate pubsub connection string")
			return nil, err
		}
		topic, err := pubsub.OpenTopic(ctx, pubsubString)
		if err != nil {
			log.Err(err).Msg("failed to open pubsub topic")
			return nil, err
		}

		err = topic.Send(ctx, &pubsub.Message{
			Body: []byte(fmt.Sprintf("attachment_created:%s", attachmentDb.ID.String())),
			Metadata: map[string]string{
				"group_id":      itemGroup.ID.String(),
				"attachment_id": attachmentDb.ID.String(),
				"title":         doc.Title,
				"path":          attachmentDb.Path,
			},
		})
		if err != nil {
			log.Err(err).Msg("failed to send message to topic")
			return nil, err
		}
	}

	return attachmentDb, nil
}

func (r *AttachmentRepo) Get(ctx context.Context, gid uuid.UUID, id uuid.UUID) (*ent.Attachment, error) {
	first, err := r.db.Attachment.Query().Where(attachment.ID(id)).Only(ctx)
	if err != nil {
		return nil, err
	}
	if first.Type == attachment.TypeThumbnail {
		// If the attachment is a thumbnail, get the parent attachment and check if it belongs to the specified group
		return r.db.Attachment.
			Query().
			Where(attachment.ID(id),
				attachment.HasThumbnailWith(attachment.HasItemWith(item.HasGroupWith(group.ID(gid)))),
			).
			WithItem().
			WithThumbnail().
			Only(ctx)
	} else {
		// For regular attachments, check if the attachment's item belongs to the specified group
		return r.db.Attachment.
			Query().
			Where(attachment.ID(id),
				attachment.HasItemWith(item.HasGroupWith(group.ID(gid))),
			).
			WithItem().
			WithThumbnail().
			Only(ctx)
	}
}

func (r *AttachmentRepo) Update(ctx context.Context, gid uuid.UUID, id uuid.UUID, data *ItemAttachmentUpdate) (*ent.Attachment, error) {
	// Validate that the attachment belongs to the specified group
	_, err := r.db.Attachment.Query().
		Where(
			attachment.ID(id),
			attachment.HasItemWith(item.HasGroupWith(group.ID(gid))),
		).
		Only(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: execute within Tx
	typ := attachment.Type(data.Type)

	bldr := r.db.Attachment.UpdateOneID(id).
		SetType(typ)

	// Primary only applies to photos
	if typ == attachment.TypePhoto {
		bldr = bldr.SetPrimary(data.Primary)
	} else {
		bldr = bldr.SetPrimary(false)
	}

	updatedAttachment, err := bldr.Save(ctx)
	if err != nil {
		return nil, err
	}

	attachmentItem, err := updatedAttachment.QueryItem().Only(ctx)
	if err != nil {
		return nil, err
	}

	// Only remove primary status from other photo attachments when setting a new photo as primary
	if typ == attachment.TypePhoto && data.Primary {
		err = r.db.Attachment.Update().
			Where(
				attachment.HasItemWith(item.ID(attachmentItem.ID)),
				attachment.IDNEQ(updatedAttachment.ID),
				attachment.TypeEQ(attachment.TypePhoto),
			).
			SetPrimary(false).
			Exec(ctx)
		if err != nil {
			return nil, err
		}
	}

	return r.Get(ctx, gid, updatedAttachment.ID)
}

func (r *AttachmentRepo) Delete(ctx context.Context, gid uuid.UUID, itemId uuid.UUID, id uuid.UUID) error {
	// Validate that the attachment belongs to the specified group
	doc, err := r.db.Attachment.Query().
		Where(
			attachment.ID(id),
			attachment.HasItemWith(item.HasGroupWith(group.ID(gid))),
		).
		Only(ctx)
	if err != nil {
		return err
	}

	all, err := r.db.Attachment.Query().Where(attachment.Path(doc.Path)).All(ctx)
	if err != nil {
		return err
	}
	// If this is the last attachment for this path, delete the file
	if len(all) == 1 {
		thumb, err := doc.QueryThumbnail().First(ctx)
		if err != nil && !ent.IsNotFound(err) {
			log.Err(err).Msg("failed to query thumbnail for attachment")
			return err
		}
		if thumb != nil {
			thumbBucket, err := blob.OpenBucket(ctx, r.GetConnString())
			if err != nil {
				log.Err(err).Msg("failed to open bucket for thumbnail deletion")
				return err
			}
			err = thumbBucket.Delete(ctx, r.fullPath(thumb.Path))
			if err != nil {
				return err
			}
			_ = doc.Update().SetNillableThumbnailID(nil).SaveX(ctx)
			_ = thumb.Update().SetNillableThumbnailID(nil).SaveX(ctx)
			err = r.db.Attachment.DeleteOneID(thumb.ID).Exec(ctx)
			if err != nil {
				return err
			}
		}
		bucket, err := blob.OpenBucket(ctx, r.GetConnString())
		if err != nil {
			log.Err(err).Msg("failed to open bucket")
			return err
		}
		defer func(bucket *blob.Bucket) {
			err := bucket.Close()
			if err != nil {
				log.Err(err).Msg("failed to close bucket")
			}
		}(bucket)
		err = bucket.Delete(ctx, r.fullPath(doc.Path))
		if err != nil {
			return err
		}
	}

	return r.db.Attachment.DeleteOneID(id).Exec(ctx)
}

func (r *AttachmentRepo) Rename(ctx context.Context, gid uuid.UUID, id uuid.UUID, title string) (*ent.Attachment, error) {
	// Validate that the attachment belongs to the specified group
	_, err := r.db.Attachment.Query().
		Where(
			attachment.ID(id),
			attachment.HasItemWith(item.HasGroupWith(group.ID(gid))),
		).
		Only(ctx)
	if err != nil {
		return nil, err
	}

	return r.db.Attachment.UpdateOneID(id).SetTitle(title).Save(ctx)
}

//nolint:gocyclo
func (r *AttachmentRepo) CreateThumbnail(ctx context.Context, groupId, attachmentId uuid.UUID, title string, path string) error {
	log.Debug().Msg("starting thumbnail creation")
	tx, err := r.db.Tx(ctx)
	if err != nil {
		return nil
	}
	// If there is an error during file creation rollback the database
	defer func() {
		if v := recover(); v != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
		}
	}()

	log.Debug().Msg("set initial database transaction")
	att := tx.Attachment.Create().
		SetID(uuid.New()).
		SetTitle(fmt.Sprintf("%s-thumb", title)).
		SetType("thumbnail")

	log.Debug().Msg("opening original file")
	bucket, err := blob.OpenBucket(ctx, r.GetConnString())
	if err != nil {
		log.Err(err).Msg("failed to open bucket")
		err := tx.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	defer func(bucket *blob.Bucket) {
		err := bucket.Close()
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
			log.Err(err).Msg("failed to close bucket")
		}
	}(bucket)

	origFile, err := bucket.Open(r.fullPath(path))
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		return err
	}
	defer func(file fs.File) {
		err := file.Close()
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
			log.Err(err).Msg("failed to close file")
		}
	}(origFile)

	log.Debug().Msg("stat original file for file size")
	stats, err := origFile.Stat()
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		log.Err(err).Msg("failed to stat original file")
		return err
	}

	if stats.Size() > 100*1024*1024 {
		return fmt.Errorf("original file %s is too large to create a thumbnail", title)
	}

	log.Debug().Msg("reading original file content")
	contentBytes, err := io.ReadAll(origFile)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		log.Err(err).Msg("failed to read original file content")
		return err
	}

	log.Debug().Msg("detecting content type of original file")
	contentType := http.DetectContentType(contentBytes[:min(512, len(contentBytes))])
	titleLower := strings.ToLower(title)

	if contentType == "application/octet-stream" {
		switch {
		case strings.HasSuffix(titleLower, ".heic") || strings.HasSuffix(titleLower, ".heif"):
			contentType = "image/heic"
		case strings.HasSuffix(titleLower, ".avif"):
			contentType = "image/avif"
		case strings.HasSuffix(titleLower, ".jxl"):
			contentType = "image/jxl"
		}
	}

	switch {
	case isImageFile(contentType):
		log.Debug().Msg("creating thumbnail for image file")
		img, _, err := image.Decode(bytes.NewReader(contentBytes))
		if err != nil {
			log.Err(err).Msg("failed to decode image file")
			err := tx.Rollback()
			if err != nil {
				log.Err(err).Msg("failed to rollback transaction")
				return err
			}
			return err
		}
		log.Debug().Msg("reading original file orientation")
		orientation, err := readImageOrientation(contentBytes, contentType)
		if err != nil {
			log.Err(err).Msg("failed to decode original file content")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		thumbnailPath, err := r.processThumbnailFromImage(ctx, groupId, img, title, orientation)
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		att.SetPath(thumbnailPath)
	case contentType == "image/webp":
		log.Debug().Msg("creating thumbnail for webp file")
		img, err := webp.Decode(bytes.NewReader(contentBytes))
		if err != nil {
			log.Err(err).Msg("failed to decode webp image")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		log.Debug().Msg("reading original file orientation")
		orientation, err := readImageOrientation(contentBytes, contentType)
		if err != nil {
			log.Err(err).Msg("failed to decode original file content")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		thumbnailPath, err := r.processThumbnailFromImage(ctx, groupId, img, title, orientation)
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		att.SetPath(thumbnailPath)
	case contentType == "image/avif":
		log.Debug().Msg("creating thumbnail for avif file")
		img, err := avif.Decode(bytes.NewReader(contentBytes))
		if err != nil {
			log.Err(err).Msg("failed to decode avif image")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		thumbnailPath, err := r.processThumbnailFromImage(ctx, groupId, img, title, uint16(1))
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		att.SetPath(thumbnailPath)
	case contentType == "image/heic" || contentType == "image/heif":
		log.Debug().Msg("creating thumbnail for heic file")
		img, err := heic.Decode(bytes.NewReader(contentBytes))
		if err != nil {
			log.Err(err).Msg("failed to decode heic image")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		log.Debug().Msg("reading original file orientation")
		orientation, err := readImageOrientation(contentBytes, contentType)
		if err != nil {
			if errors.Is(err, imagemeta.ErrMetadataNotSupported) || errors.Is(err, imagemeta.ErrNoExif) || errors.Is(err, errHeicOrientationNotFound) {
				log.Warn().Err(err).Msg("using default orientation for heic file")
			} else {
				log.Err(err).Msg("failed to decode original file content")
				err := tx.Rollback()
				if err != nil {
					return err
				}
				return err
			}
		}
		thumbnailPath, err := r.processThumbnailFromImage(ctx, groupId, img, title, orientation)
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		att.SetPath(thumbnailPath)
	case contentType == "image/jxl":
		log.Debug().Msg("creating thumbnail for jpegxl file")
		img, err := jpegxl.Decode(bytes.NewReader(contentBytes))
		if err != nil {
			log.Err(err).Msg("failed to decode avif image")
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		thumbnailPath, err := r.processThumbnailFromImage(ctx, groupId, img, title, uint16(1))
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				return err
			}
			return err
		}
		att.SetPath(thumbnailPath)
	default:
		return fmt.Errorf("file type %s is not supported for thumbnail creation or document thumnails disabled", title)
	}

	att.SetMimeType("image/webp")

	log.Debug().Msg("saving thumbnail attachment to database")
	thumbnail, err := att.Save(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Attachment.UpdateOneID(attachmentId).SetThumbnail(thumbnail).Save(ctx)
	if err != nil {
		return err
	}

	log.Debug().Msg("finishing thumbnail creation transaction")
	if err := tx.Commit(); err != nil {
		log.Err(err).Msg("failed to commit transaction")
		return nil
	}
	return nil
}

func (r *AttachmentRepo) CreateMissingThumbnails(ctx context.Context, groupId uuid.UUID) (int, error) {
	attachments, err := r.db.Attachment.Query().
		Where(
			attachment.HasItemWith(item.HasGroupWith(group.ID(groupId))),
			attachment.TypeNEQ("thumbnail"),
		).
		All(ctx)
	if err != nil {
		return 0, err
	}

	pubsubString, err := utils.GenerateSubPubConn(r.pubSubConn, "thumbnails")
	if err != nil {
		log.Err(err).Msg("failed to generate pubsub connection string")
	}
	topic, err := pubsub.OpenTopic(ctx, pubsubString)
	if err != nil {
		log.Err(err).Msg("failed to open pubsub topic")
	}

	count := 0
	for _, attachment := range attachments {
		if r.thumbnail.Enabled {
			if !attachment.QueryThumbnail().ExistX(ctx) {
				if count > 0 && count%100 == 0 {
					time.Sleep(2 * time.Second)
				}
				err = topic.Send(ctx, &pubsub.Message{
					Body: []byte(fmt.Sprintf("attachment_created:%s", attachment.ID.String())),
					Metadata: map[string]string{
						"group_id":      groupId.String(),
						"attachment_id": attachment.ID.String(),
						"title":         attachment.Title,
						"path":          attachment.Path,
					},
				})
				if err != nil {
					log.Err(err).Msg("failed to send message to topic")
					continue
				} else {
					count++
				}
			}
		}
	}

	return count, nil
}

func (r *AttachmentRepo) UploadFile(ctx context.Context, itemGroup *ent.Group, doc ItemCreateAttachment) (string, error) {
	// Prepare for the hashing of the file contents
	hashOut := make([]byte, 32)

	// Read all content into a buffer
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, doc.Content)
	if err != nil {
		log.Err(err).Msg("failed to read file content")
		return "", err
	}
	// Now the buffer contains all the data, use it for hashing
	contentBytes := buf.Bytes()

	// We use blake3 to generate a hash of the file contents, the group ID is used as context to ensure unique hashes
	// for the same file across different groups to reduce the chance of collisions
	// additionally, the hash can be used to validate the file contents if needed
	blake3.DeriveKey(itemGroup.ID.String(), contentBytes, hashOut)

	// Write the file to the blob storage bucket which might be a local file system or cloud storage
	bucket, err := blob.OpenBucket(ctx, r.GetConnString())
	if err != nil {
		log.Err(err).Msg("failed to open bucket")
		return "", err
	}
	defer func(bucket *blob.Bucket) {
		err := bucket.Close()
		if err != nil {
			log.Err(err).Msg("failed to close bucket")
		}
	}(bucket)
	md5hash := md5.New()
	_, err = md5hash.Write(contentBytes)
	if err != nil {
		log.Err(err).Msg("failed to generate MD5 hash for storage")
		return "", err
	}
	contentType := http.DetectContentType(contentBytes[:min(512, len(contentBytes))])
	options := &blob.WriterOptions{
		ContentType: contentType,
		ContentMD5:  md5hash.Sum(nil),
	}
	relativePath := r.path(itemGroup.ID, fmt.Sprintf("%x", hashOut))
	fullPath := r.fullPath(relativePath)
	err = bucket.WriteAll(ctx, fullPath, contentBytes, options)
	if err != nil {
		log.Err(err).Msg("failed to write file to bucket")
		return "", err
	}

	return relativePath, nil
}

func isImageFile(mimetype string) bool {
	// Check file extension for image types
	return strings.Contains(mimetype, "image/jpeg") || strings.Contains(mimetype, "image/png") || strings.Contains(mimetype, "image/gif")
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "database is locked") || strings.Contains(errMsg, "SQLITE_BUSY")
}

// calculateThumbnailDimensions calculates new dimensions that preserve aspect ratio
// while fitting within the configured maximum width and height
func calculateThumbnailDimensions(origWidth, origHeight, maxWidth, maxHeight int) (int, int) {
	if origWidth <= maxWidth && origHeight <= maxHeight {
		return origWidth, origHeight
	}

	// Calculate scaling factors for both dimensions
	scaleX := float64(maxWidth) / float64(origWidth)
	scaleY := float64(maxHeight) / float64(origHeight)

	// Use the smaller scaling factor to ensure both dimensions fit
	scale := scaleX
	if scaleY < scaleX {
		scale = scaleY
	}

	newWidth := int(float64(origWidth) * scale)
	newHeight := int(float64(origHeight) * scale)

	// Ensure we don't get zero dimensions
	if newWidth < 1 {
		newWidth = 1
	}
	if newHeight < 1 {
		newHeight = 1
	}

	return newWidth, newHeight
}

var (
	errHeicOrientationNotFound   = errors.New("heic orientation not found")
	errHeicAssociationsNotParsed = errors.New("heic property associations missing")
)

func readImageOrientation(contentBytes []byte, contentType string) (uint16, error) {
	const defaultOrientation = uint16(1)

	imageMeta, err := imagemeta.Decode(bytes.NewReader(contentBytes))
	if err != nil {
		if isHeicMime(contentType) && (errors.Is(err, imagemeta.ErrMetadataNotSupported) || errors.Is(err, imagemeta.ErrNoExif)) {
			return readHeicOrientation(contentBytes)
		}
		return defaultOrientation, err
	}

	orientation := uint16(imageMeta.Orientation)
	if isHeicMime(contentType) {
		if orientation == 0 {
			return readHeicOrientation(contentBytes)
		}
		if orientation <= 1 {
			heicOrientation, err := readHeicOrientation(contentBytes)
			if err == nil {
				return heicOrientation, nil
			}
			if errors.Is(err, errHeicOrientationNotFound) {
				return orientation, nil
			}
			return orientation, err
		}
	}

	return orientation, nil
}

func readHeicOrientation(contentBytes []byte) (uint16, error) {
	const defaultOrientation = uint16(1)

	reader := isobmff.NewReader(bytes.NewReader(contentBytes))
	if err := reader.ReadFTYP(); err != nil {
		reader.Close()
		return defaultOrientation, err
	}
	reader.Close()

	orientation, err := parseHeicOrientation(contentBytes)
	if err != nil {
		return defaultOrientation, err
	}
	if orientation == 0 {
		return defaultOrientation, errHeicOrientationNotFound
	}
	return orientation, nil
}

func parseHeicOrientation(data []byte) (uint16, error) {
	offset := 0
	for offset < len(data) {
		size, boxType, headerLen, err := readBoxHeader(data[offset:])
		if err != nil {
			return 0, err
		}
		end := offset + int(size)
		if end > len(data) {
			return 0, fmt.Errorf("meta box exceeds buffer: %w", isobmff.ErrRemainLengthInsufficient)
		}
		if boxType == "meta" {
			return parseMetaBox(data[offset+headerLen : end])
		}
		if size == 0 {
			break
		}
		offset += int(size)
	}
	return 0, errHeicOrientationNotFound
}

func parseMetaBox(data []byte) (uint16, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("meta payload too small: %w", isobmff.ErrRemainLengthInsufficient)
	}
	payload := data[4:]
	var primaryID uint32
	properties := map[uint16]heicItemProperty{}
	associations := map[uint32][]uint16{}
	var associationsErr error

	for len(payload) > 0 {
		size, boxType, headerLen, err := readBoxHeader(payload)
		if err != nil {
			return 0, err
		}
		if int(size) > len(payload) {
			return 0, fmt.Errorf("meta child box exceeds payload: %w", isobmff.ErrRemainLengthInsufficient)
		}
		start := headerLen
		end := int(size)
		content := payload[start:end]
		switch boxType {
		case "pitm":
			primaryID, err = parsePitm(content)
			if err != nil {
				return 0, err
			}
		case "iprp":
			props, assoc, err := parseIprp(content)
			if err != nil && !errors.Is(err, errHeicAssociationsNotParsed) {
				return 0, err
			}
			for idx, prop := range props {
				existing := properties[idx]
				if prop.hasRotation {
					existing.rotationSteps = prop.rotationSteps
					existing.hasRotation = true
				}
				if prop.hasMirror {
					existing.mirrorAxis = prop.mirrorAxis
					existing.hasMirror = true
				}
				properties[idx] = existing
			}
			for item, list := range assoc {
				associations[item] = append(associations[item], list...)
			}
			if err != nil && errors.Is(err, errHeicAssociationsNotParsed) {
				associationsErr = err
			}
		}
		if size == 0 {
			break
		}
		payload = payload[int(size):]
	}

	indices := associations[primaryID]
	if len(indices) == 0 {
		if orientation, ok := resolveHeicOrientationWithoutAssociations(properties); ok {
			return orientation, nil
		}
		if associationsErr != nil {
			return 0, errors.Join(errHeicOrientationNotFound, associationsErr)
		}
		return 0, errHeicOrientationNotFound
	}

	var rotationSteps int
	var mirrorAxis *int
	var mirrorAxisValue int
	for _, idx := range indices {
		if prop, ok := properties[idx]; ok {
			if prop.hasRotation {
				rotationSteps = prop.rotationSteps
			}
			if prop.hasMirror {
				mirrorAxisValue = prop.mirrorAxis
				mirrorAxis = &mirrorAxisValue
			}
		}
	}

	orientation := orientationFromHeic(rotationSteps, mirrorAxis)
	if orientation == 0 {
		if associationsErr != nil {
			return 0, errors.Join(errHeicOrientationNotFound, associationsErr)
		}
		return 0, errHeicOrientationNotFound
	}
	return orientation, nil
}

func parsePitm(data []byte) (uint32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("pitm payload too small: %w", isobmff.ErrRemainLengthInsufficient)
	}
	version := data[0]
	offset := 4
	if version == 0 {
		if len(data) < offset+2 {
			return 0, fmt.Errorf("pitm short payload: %w", isobmff.ErrRemainLengthInsufficient)
		}
		return uint32(binary.BigEndian.Uint16(data[offset : offset+2])), nil
	}
	if len(data) < offset+4 {
		return 0, fmt.Errorf("pitm short payload: %w", isobmff.ErrRemainLengthInsufficient)
	}
	return binary.BigEndian.Uint32(data[offset : offset+4]), nil
}

type heicItemProperty struct {
	rotationSteps int
	hasRotation   bool
	mirrorAxis    int
	hasMirror     bool
}

func parseIprp(data []byte) (map[uint16]heicItemProperty, map[uint32][]uint16, error) {
	props := make(map[uint16]heicItemProperty)
	associations := make(map[uint32][]uint16)
	buf := data
	var assocErr error
	for len(buf) > 0 {
		size, boxType, headerLen, err := readBoxHeader(buf)
		if err != nil {
			return nil, nil, err
		}
		if int(size) > len(buf) {
			return nil, nil, fmt.Errorf("iprp child box exceeds payload: %w", isobmff.ErrRemainLengthInsufficient)
		}
		start := headerLen
		end := int(size)
		content := buf[start:end]
		switch boxType {
		case "ipco":
			parsed, err := parseIpco(content)
			if err != nil {
				return nil, nil, err
			}
			for idx, prop := range parsed {
				props[idx] = prop
			}
		case "ipma":
			assoc, err := parseIpma(content)
			if err != nil {
				assocErr = fmt.Errorf("%w: %w", errHeicAssociationsNotParsed, err)
				break
			}
			for item, list := range assoc {
				associations[item] = append(associations[item], list...)
			}
		}
		if size == 0 {
			break
		}
		buf = buf[int(size):]
	}
	if assocErr != nil && len(associations) == 0 {
		return props, associations, assocErr
	}
	return props, associations, assocErr
}

func parseIpco(data []byte) (map[uint16]heicItemProperty, error) {
	props := make(map[uint16]heicItemProperty)
	buf := data
	index := uint16(1)
	for len(buf) > 0 {
		if len(buf) >= 16 {
			size32 := binary.BigEndian.Uint32(buf[:4])
			if size32 == 1 {
				largeSize := binary.BigEndian.Uint64(buf[8:16])
				if largeSize > uint64(len(buf)) {
					break
				}
			}
		}
		size, boxType, headerLen, err := readBoxHeader(buf)
		if err != nil {
			return nil, err
		}
		if int(size) > len(buf) {
			break
		}
		start := headerLen
		end := int(size)
		content := buf[start:end]
		switch boxType {
		case "irot":
			if len(content) > 0 {
				value := content[len(content)-1] & 0x3
				props[index] = heicItemProperty{rotationSteps: int(value), hasRotation: true}
			}
		case "imir":
			if len(content) > 0 {
				axis := int(content[len(content)-1] & 0x1)
				props[index] = heicItemProperty{mirrorAxis: axis, hasMirror: true}
			}
		}
		if size == 0 {
			break
		}
		buf = buf[int(size):]
		index++
	}
	return props, nil
}

func parseIpma(data []byte) (map[uint32][]uint16, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("ipma payload too small: %w", isobmff.ErrRemainLengthInsufficient)
	}
	version := data[0]
	flags := uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	count := binary.BigEndian.Uint32(data[4:8])
	use16 := flags&0x1 != 0
	offset := 8
	associations := make(map[uint32][]uint16)

	for i := uint32(0); i < count; i++ {
		var itemID uint32
		if version == 0 {
			if len(data) < offset+2 {
				return nil, fmt.Errorf("ipma item payload too small: %w", isobmff.ErrRemainLengthInsufficient)
			}
			itemID = uint32(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2
		} else {
			if len(data) < offset+4 {
				return nil, fmt.Errorf("ipma item payload too small: %w", isobmff.ErrRemainLengthInsufficient)
			}
			itemID = binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
		}
		if len(data) <= offset {
			return nil, fmt.Errorf("ipma association count missing: %w", isobmff.ErrRemainLengthInsufficient)
		}
		assocCount := int(data[offset])
		offset++
		for j := 0; j < assocCount; j++ {
			if use16 {
				if len(data) < offset+2 {
					return nil, fmt.Errorf("ipma association payload too small: %w", isobmff.ErrRemainLengthInsufficient)
				}
				entry := binary.BigEndian.Uint16(data[offset : offset+2])
				offset += 2
				index := entry & 0x7FFF
				if index != 0 {
					associations[itemID] = append(associations[itemID], index)
				}
			} else {
				if len(data) <= offset {
					return nil, fmt.Errorf("ipma association payload too small: %w", isobmff.ErrRemainLengthInsufficient)
				}
				entry := data[offset]
				offset++
				index := uint16(entry & 0x7F)
				if index != 0 {
					associations[itemID] = append(associations[itemID], index)
				}
			}
		}
	}
	return associations, nil
}

func readBoxHeader(data []byte) (uint64, string, int, error) {
	if len(data) < 8 {
		return 0, "", 0, fmt.Errorf("box header too small: %w", isobmff.ErrRemainLengthInsufficient)
	}
	size32 := binary.BigEndian.Uint32(data[:4])
	boxType := string(data[4:8])
	headerLen := 8
	var size uint64
	switch size32 {
	case 0:
		size = uint64(len(data))
	case 1:
		if len(data) < 16 {
			return 0, "", 0, fmt.Errorf("large box header too small: %w", isobmff.ErrRemainLengthInsufficient)
		}
		size = binary.BigEndian.Uint64(data[8:16])
		headerLen = 16
	default:
		size = uint64(size32)
	}
	if size < uint64(headerLen) {
		return 0, "", 0, fmt.Errorf("invalid box size: %w", isobmff.ErrRemainLengthInsufficient)
	}
	if size > uint64(len(data)) && size32 != 0 {
		return 0, "", 0, fmt.Errorf("box exceeds payload: %w", isobmff.ErrRemainLengthInsufficient)
	}
	return size, boxType, headerLen, nil
}

func resolveHeicOrientationWithoutAssociations(properties map[uint16]heicItemProperty) (uint16, bool) {
	var rotationSteps int
	var rotationCount int
	var mirrorAxisValue int
	var mirrorCount int

	for _, prop := range properties {
		if prop.hasRotation {
			rotationSteps = prop.rotationSteps
			rotationCount++
		}
		if prop.hasMirror {
			mirrorAxisValue = prop.mirrorAxis
			mirrorCount++
		}
	}

	if rotationCount > 1 || mirrorCount > 1 {
		return 0, false
	}
	if rotationCount == 0 && mirrorCount == 0 {
		return 0, false
	}

	var mirrorAxis *int
	if mirrorCount == 1 {
		mirrorAxis = &mirrorAxisValue
	}

	orientation := orientationFromHeic(rotationSteps, mirrorAxis)
	if orientation == 0 {
		return 0, false
	}
	return orientation, true
}

type heicTransform struct {
	ax, ay int
	bx, by int
}

func multiplyTransform(a, b heicTransform) heicTransform {
	return heicTransform{
		ax: a.ax*b.ax + a.bx*b.ay,
		ay: a.ay*b.ax + a.by*b.ay,
		bx: a.ax*b.bx + a.bx*b.by,
		by: a.ay*b.bx + a.by*b.by,
	}
}

var (
	identityTransform   = heicTransform{ax: 1, ay: 0, bx: 0, by: 1}
	rotate90Transform   = heicTransform{ax: 0, ay: -1, bx: 1, by: 0}
	mirrorHorizontal    = heicTransform{ax: -1, ay: 0, bx: 0, by: 1}
	mirrorVertical      = heicTransform{ax: 1, ay: 0, bx: 0, by: -1}
	orientationMappings = map[heicTransform]uint16{
		{ax: 1, ay: 0, bx: 0, by: 1}:   1,
		{ax: -1, ay: 0, bx: 0, by: 1}:  2,
		{ax: -1, ay: 0, bx: 0, by: -1}: 3,
		{ax: 1, ay: 0, bx: 0, by: -1}:  4,
		{ax: 0, ay: 1, bx: 1, by: 0}:   5,
		{ax: 0, ay: -1, bx: 1, by: 0}:  6,
		{ax: 0, ay: -1, bx: -1, by: 0}: 7,
		{ax: 0, ay: 1, bx: -1, by: 0}:  8,
	}
)

func orientationFromHeic(rotationSteps int, mirrorAxis *int) uint16 {
	transform := identityTransform
	if mirrorAxis != nil {
		if *mirrorAxis == 0 {
			transform = multiplyTransform(mirrorHorizontal, transform)
		} else {
			transform = multiplyTransform(mirrorVertical, transform)
		}
	}
	rotationSteps = ((rotationSteps % 4) + 4) % 4
	for i := 0; i < rotationSteps; i++ {
		transform = multiplyTransform(rotate90Transform, transform)
	}
	if orientation, ok := orientationMappings[transform]; ok {
		return orientation
	}
	return 0
}

func isHeicMime(contentType string) bool {
	switch contentType {
	case "image/heic", "image/heif":
		return true
	default:
		return false
	}
}

// processThumbnailFromImage handles the common thumbnail processing logic after image decoding
// Returns the thumbnail file path or an error
func (r *AttachmentRepo) processThumbnailFromImage(ctx context.Context, groupId uuid.UUID, img image.Image, title string, orientation uint16) (string, error) {
	bounds := img.Bounds()
	// Apply EXIF orientation if needed
	if orientation > 1 {
		img = utils.ApplyOrientation(img, orientation)
		bounds = img.Bounds()
	}
	newWidth, newHeight := calculateThumbnailDimensions(bounds.Dx(), bounds.Dy(), r.thumbnail.Width, r.thumbnail.Height)
	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	draw.CatmullRom.Scale(dst, dst.Rect, img, img.Bounds(), draw.Over, nil)

	buf := new(bytes.Buffer)
	err := webp.Encode(buf, dst, webp.Options{Quality: 80, Lossless: false})
	if err != nil {
		return "", err
	}
	contentBytes := buf.Bytes()
	log.Debug().Msg("uploading thumbnail file")

	// Get the group for uploading the thumbnail
	group, err := r.db.Group.Get(ctx, groupId)
	if err != nil {
		return "", err
	}

	thumbnailFile, err := r.UploadFile(ctx, group, ItemCreateAttachment{
		Title:   fmt.Sprintf("%s-thumb", title),
		Content: bytes.NewReader(contentBytes),
	})
	if err != nil {
		log.Err(err).Msg("failed to upload thumbnail file")
		return "", err
	}

	return thumbnailFile, nil
}
