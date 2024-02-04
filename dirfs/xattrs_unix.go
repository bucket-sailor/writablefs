//go:build unix

/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dirfs

import (
	"errors"
	"os"
	"strings"

	"github.com/bucket-sailor/writablefs"
	"github.com/pkg/xattr"
)

type fileAttrs struct {
	*os.File
}

func (a *fileAttrs) Get(name string) ([]byte, error) {
	data, err := xattr.FGet(a.File, "user."+strings.ToLower(name))
	if err != nil {
		if errors.Is(err, xattr.ENOATTR) {
			return nil, writablefs.ErrNoSuchAttr
		}

		return nil, err
	}

	return data, nil
}

func (a *fileAttrs) Set(name string, data []byte) error {
	return xattr.FSet(a.File, "user."+strings.ToLower(name), data)
}

func (a *fileAttrs) Remove(name string) error {
	if err := xattr.FRemove(a.File, "user."+strings.ToLower(name)); err != nil {
		if errors.Is(err, xattr.ENOATTR) {
			return nil
		}

		return err
	}

	return nil
}

func (a *fileAttrs) List() ([]string, error) {
	names, err := xattr.FList(a.File)
	if err != nil {
		return nil, err
	}

	var userAttrNames []string
	for _, name := range names {
		if strings.HasPrefix(name, "user.") {
			userAttrNames = append(userAttrNames, strings.ToLower(strings.TrimPrefix(name, "user.")))
		}
	}

	return userAttrNames, nil
}
