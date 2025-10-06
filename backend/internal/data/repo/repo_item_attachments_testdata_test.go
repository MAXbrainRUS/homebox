package repo

import (
	"encoding/base64"
)

func heicPortraitFixture() []byte {
	return append([]byte(nil), heicPortraitTestData...)
}

var heicPortraitTestData = mustDecodeHEICPortrait()

func mustDecodeHEICPortrait() []byte {
	data, err := base64.StdEncoding.DecodeString(heicPortraitBase64)
	if err != nil {
		panic(err)
	}
	return data
}

const heicPortraitBase64 = "AAAAFGZ0eXBoZWljAAAAAGhlaWMAAABrbWV0YQAAAAAAAAAkaGRscgAAAAAAAAAAAAAAAHBpY3QAAAAAAAAAAAAAAAAAAAAOcGl0bQAAAAAAAQAAAC1pcHJwAAAAEWlwY28AAAAJaXJvdAEAAAAUaXBtYQAAAAAAAAABAAEBAQ=="
