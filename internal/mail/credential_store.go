package mail

import (
	"github.com/pranavkhaspa/knightly/internal/googleauth"
)

// TokenSet, CredentialStore, and FileCredentialStore delegate to the shared
// Google token store. Mail reuses the same owner-only, outside-the-graph
// credential file as the other Google providers.
type TokenSet = googleauth.TokenSet

type CredentialStore = googleauth.Store

type FileCredentialStore = googleauth.FileStore

var ErrCredentialNotFound = googleauth.ErrCredentialNotFound

func NewFileCredentialStore(path string) (*FileCredentialStore, error) {
	return googleauth.NewFileStore(path)
}
