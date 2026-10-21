// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package awscreds is the one place this module resolves AWS credentials
// from the process environment, so tests can replace it.
package awscreds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// Load resolves the default AWS credential chain (environment, shared
// config files, instance metadata).
var Load func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) = awsconfig.LoadDefaultConfig
