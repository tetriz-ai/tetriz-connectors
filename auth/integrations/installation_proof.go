// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const installationValidationTTL = 5 * time.Minute
const installationValidationPrefix = "github:installation-validation:"

type installationValidation struct {
	OrgID          string `json:"org_id"`
	InstallationID string `json:"installation_id"`
	AccountID      int64  `json:"account_id"`
}

func (s *Service) issueInstallationValidation(ctx context.Context, proof installationValidation) (string, error) {
	if s.redis == nil {
		return "", status.Error(codes.FailedPrecondition, "GitHub installation validation storage unavailable")
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return "", status.Error(codes.Internal, "create installation validation")
	}
	value, err := json.Marshal(proof)
	if err != nil {
		return "", status.Error(codes.Internal, "encode installation validation")
	}
	receipt := hex.EncodeToString(token)
	if err = s.redis.Set(ctx, installationValidationPrefix+receipt, value, installationValidationTTL).Err(); err != nil {
		return "", status.Error(codes.Unavailable, "store installation validation")
	}
	return receipt, nil
}

func (s *Service) consumeInstallationValidation(ctx context.Context, token, orgID, installationID string) (installationValidation, error) {
	var proof installationValidation
	if len(token) != 64 {
		return proof, status.Error(codes.InvalidArgument, "validation_token from ValidateInstallation is required")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return proof, status.Error(codes.InvalidArgument, "invalid validation_token")
	}
	if s.redis == nil {
		return proof, status.Error(codes.FailedPrecondition, "GitHub installation validation storage unavailable")
	}
	value, err := s.redis.GetDel(ctx, installationValidationPrefix+token).Result()
	if errors.Is(err, redis.Nil) {
		return proof, status.Error(codes.FailedPrecondition, "invalid, expired or consumed installation validation")
	}
	if err != nil {
		return proof, status.Error(codes.Unavailable, "read installation validation")
	}
	if err = json.Unmarshal([]byte(value), &proof); err != nil || proof.OrgID != orgID || proof.InstallationID != installationID || proof.AccountID <= 0 {
		return proof, status.Error(codes.FailedPrecondition, "installation validation does not match request")
	}
	return proof, nil
}
