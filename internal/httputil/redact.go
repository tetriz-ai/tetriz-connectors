// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package httputil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// RedactURL returns raw without its query string, fragment and user info, so
// a vendor request URL can go into a log line or an error message without
// the search terms, emails or tokens a query may carry. A value that does not
// parse is cut at its first '?' or '#'.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		if i := strings.IndexAny(raw, "?#"); i >= 0 {
			return raw[:i]
		}
		return raw
	}
	u.RawQuery, u.ForceQuery = "", false
	u.Fragment, u.RawFragment = "", ""
	u.User = nil
	return u.String()
}

// redactedError carries an error whose message names a request URL with the
// URL redacted. Unwrap keeps the original, so errors.Is and errors.As still
// see every error in the chain.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// RedactURLError returns err with every request URL its chain carries (the
// *url.Error net/http returns for a failed request, wrapped at any depth)
// redacted by RedactURL in its message. An error without one is returned as
// it is.
func RedactURLError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for e := err; e != nil; {
		var ue *url.Error
		if !errors.As(e, &ue) {
			break
		}
		if ue.URL != "" {
			msg = strings.ReplaceAll(msg, ue.URL, RedactURL(ue.URL))
		}
		e = ue.Err
	}
	if msg == err.Error() {
		return err
	}
	return &redactedError{msg: msg, err: err}
}

// errorFieldLimit caps each vendor error field kept in an error message.
const errorFieldLimit = 200

// SummarizeErrorBody returns what an error message may carry from a vendor's
// error response body: for a JSON body, only the vendor's error code and
// message fields, each cut to 200 bytes; for any other body, only its length
// and content type, never its text. It knows the shapes GitHub, GitLab,
// Bitbucket, Jira, Linear, Slack and Google use: message, error (a string or
// an object with message, code and status), error_description,
// errors[].message (with extensions.code), errorMessages[], and Jira's
// errors{field: message}. It returns "" for an empty body.
func SummarizeErrorBody(body []byte, contentType string) string {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return ""
	}
	var v any
	if json.Unmarshal(body, &v) != nil {
		if contentType == "" {
			contentType = "unknown content type"
		}
		return fmt.Sprintf("%d-byte %s body", len(body), contentType)
	}
	var parts []string
	add := func(name string, val any) {
		if s := fieldText(val); s != "" {
			parts = append(parts, name+"="+quoteCapped(s))
		}
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return fmt.Sprintf("%d-byte JSON body without error fields", len(body))
	}
	add("message", obj["message"])
	switch e := obj["error"].(type) {
	case map[string]any:
		add("code", e["code"])
		add("status", e["status"])
		add("message", e["message"])
	default:
		add("error", e)
	}
	add("error_description", obj["error_description"])
	switch errs := obj["errors"].(type) {
	case []any:
		for _, item := range errs {
			if m, ok := item.(map[string]any); ok {
				if ext, ok := m["extensions"].(map[string]any); ok {
					add("code", ext["code"])
				}
				add("message", m["message"])
			} else {
				add("error", item)
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(errs))
		for k := range errs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			add("error", fmt.Sprintf("%s: %s", k, fieldText(errs[k])))
		}
	}
	if msgs, ok := obj["errorMessages"].([]any); ok {
		for _, m := range msgs {
			add("message", m)
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("%d-byte JSON body without error fields", len(body))
	}
	return strings.Join(parts, " ")
}

// fieldText renders a scalar JSON value as text, and a nested value (GitLab
// sends a message as an object or a list) as compact JSON.
func fieldText(val any) string {
	switch t := val.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64, bool:
		return fmt.Sprint(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// quoteCapped quotes s, cut to errorFieldLimit bytes on a rune boundary.
func quoteCapped(s string) string {
	if len(s) > errorFieldLimit {
		cut := errorFieldLimit
		for cut > 0 && !utf8RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "…"
	}
	return fmt.Sprintf("%q", s)
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }
