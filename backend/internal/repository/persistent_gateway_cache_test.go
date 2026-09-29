package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistentOpenAISessionNamespace(t *testing.T) {
	user, hash, ok := persistentOpenAISession("openai:u42:0123456789abcdef")
	require.True(t, ok)
	require.EqualValues(t, 42, user)
	require.Equal(t, "0123456789abcdef", hash)
	for _, key := range []string{
		"openai:0123456789abcdef", "openai:u0:0123456789abcdef", "openai:u042:0123456789abcdef",
		"openai:u-1:0123456789abcdef", "openai:u42:0123456789abcdeg", "openai:u42:0123456789ABCDEF",
		"openai:u42:short", "claude:u42:0123456789abcdef", "openai:response:0123456789abcdef",
	} {
		_, _, ok := persistentOpenAISession(key)
		require.False(t, ok, key)
	}
}
