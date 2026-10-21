// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command headers adds the two-line copyright and license header to every
// source file in the repository, or with -check reports the files that lack
// it and exits 1. Generated files take the header too, so run it after
// `buf generate` or `go generate`.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	copyrightLine = "Copyright 2026 Workloom Solutions Private Limited"
	licenseLine   = "SPDX-License-Identifier: MIT"
)

// commentPrefix maps a file to its line-comment syntax; "" means the file
// takes no header.
func commentPrefix(path string) string {
	base := filepath.Base(path)
	switch {
	case base == "Dockerfile", base == ".dockerignore", base == ".gitignore":
		return "#"
	case strings.HasSuffix(base, ".go"), strings.HasSuffix(base, ".proto"), base == "go.mod":
		return "//"
	case strings.HasSuffix(base, ".sql"):
		return "--"
	case strings.HasSuffix(base, ".sh"), strings.HasSuffix(base, ".py"), strings.HasSuffix(base, ".yaml"), strings.HasSuffix(base, ".yml"):
		return "#"
	case strings.HasSuffix(base, ".svg"):
		return "<!--"
	}
	return ""
}

func header(prefix string) string {
	if prefix == "<!--" {
		return "<!-- " + copyrightLine + " -->\n<!-- " + licenseLine + " -->\n"
	}
	return prefix + " " + copyrightLine + "\n" + prefix + " " + licenseLine + "\n"
}

// hasHeader reports whether src starts with the header, after a shebang line
// if there is one. A file that mentions the license elsewhere does not count.
func hasHeader(src []byte, prefix string) bool {
	if bytes.HasPrefix(src, []byte("#!")) {
		i := bytes.IndexByte(src, '\n')
		if i < 0 {
			return false
		}
		src = src[i+1:]
	}
	return bytes.HasPrefix(src, []byte(header(prefix)))
}

// apply returns src with the header inserted, after a shebang line if there
// is one, followed by a blank line so it never joins a doc comment.
func apply(src []byte, prefix string) []byte {
	h := header(prefix)
	if hasHeader(src, prefix) {
		return src
	}
	if bytes.HasPrefix(src, []byte("#!")) {
		i := bytes.IndexByte(src, '\n')
		if i < 0 {
			return append(append(append([]byte{}, src...), '\n'), h...)
		}
		out := append([]byte{}, src[:i+1]...)
		out = append(out, h...)
		out = append(out, '\n')
		return append(out, src[i+1:]...)
	}
	out := append([]byte(h), '\n')
	return append(out, src...)
}

func main() {
	check := flag.Bool("check", false, "report files without the header and exit 1 instead of writing")
	flag.Parse()
	root := "."
	if flag.NArg() > 0 {
		root = flag.Arg(0)
	}
	var missing []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "third_party", "node_modules", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		prefix := commentPrefix(path)
		if prefix == "" {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out := apply(src, prefix)
		if bytes.Equal(out, src) {
			return nil
		}
		if *check {
			missing = append(missing, path)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(path, out, info.Mode().Perm())
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "headers:", err)
		os.Exit(2)
	}
	if len(missing) > 0 {
		for _, p := range missing {
			fmt.Println(p)
		}
		fmt.Fprintf(os.Stderr, "headers: %d files lack the header; run go run ./internal/tools/headers\n", len(missing))
		os.Exit(1)
	}
}
