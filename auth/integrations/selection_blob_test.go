// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
)

// With an encryptor, a picker session blob in Redis that is not an envelope
// is refused: the flow never parks plaintext, so its tokens are not used.
func TestSelectionBlobRefusesPlaintextUnderAKey(t *testing.T) {
	enc, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{3}, 32))
	require.NoError(t, err)
	svc := NewService(Deps{Cfg: webhookConfig(), Registry: testRegistry(), Enc: enc})

	_, err = svc.decryptSelectionBlob([]byte(`{"access_token":"planted"}`))
	require.Error(t, err)

	sealed, err := svc.encryptSelectionBlob([]byte(`{"access_token":"real"}`))
	require.NoError(t, err)
	plain, err := svc.decryptSelectionBlob(sealed)
	require.NoError(t, err)
	assert.JSONEq(t, `{"access_token":"real"}`, string(plain))
}
