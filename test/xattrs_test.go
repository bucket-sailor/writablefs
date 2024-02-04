/* SPDX-License-Identifier: MPL-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package test

import (
	"strings"
	"testing"

	"github.com/bucket-sailor/writablefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testXAttrs(t *testing.T, fsys writablefs.FS) {
	setupXAttrs := func() (writablefs.ExtendedAttributes, error) {
		testPath := randomString(10)

		f, err := fsys.OpenFile(testPath, writablefs.FlagCreate|writablefs.FlagReadWrite)
		if err != nil {
			return nil, err
		}

		if _, err := f.Write([]byte("just a test")); err != nil {
			return nil, err
		}

		if err := f.Sync(); err != nil {
			return nil, err
		}

		xattrs, err := f.XAttrs()
		if err != nil {
			return nil, err
		}

		return xattrs, nil
	}

	t.Run("Extended Attributes", func(t *testing.T) {
		t.Run("Get and Set", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Add a basic attribute.
			attrName := "test-attr"
			attrValue := []byte("test-value")
			require.NoError(t, xattrs.Set(attrName, attrValue))

			// Commit the attribute change.
			require.NoError(t, xattrs.Sync())

			retrievedValue, err := xattrs.Get(attrName)
			require.NoError(t, err)
			assert.Equal(t, attrValue, retrievedValue)
		})

		t.Run("Get and Set - Multiple", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Add a basic attribute.
			attrName := "test-attr"
			attrValue := []byte("test-value")
			require.NoError(t, xattrs.Set(attrName, attrValue))

			// Add another attribute.
			attrName2 := "test-attr2"
			attrValue2 := []byte("test-value2")
			require.NoError(t, xattrs.Set(attrName2, attrValue2))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			retrievedValue, err := xattrs.Get(attrName)
			require.NoError(t, err)
			assert.Equal(t, attrValue, retrievedValue)

			retrievedValue, err = xattrs.Get(attrName2)
			require.NoError(t, err)
			assert.Equal(t, attrValue2, retrievedValue)
		})

		t.Run("Get - Non-Existent", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Attempt to retrieve a non-existent attribute.
			attrName := "test-attr"
			_, err = xattrs.Get(attrName)
			assert.ErrorIs(t, err, writablefs.ErrNoSuchAttr)
		})

		t.Run("List", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Add a basic attribute.
			attrName := "test-attr"
			attrValue := []byte("test-value")

			// Add another attribute.
			attrName2 := "test-attr2"
			attrValue2 := []byte("test-value2")

			require.NoError(t, xattrs.Set(attrName, attrValue))
			require.NoError(t, xattrs.Set(attrName2, attrValue2))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			names, err := xattrs.List()
			require.NoError(t, err)
			assert.Contains(t, names, attrName)
			assert.Contains(t, names, attrName2)
		})

		t.Run("Remove", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Add a basic attribute.
			attrName := "test-attr"
			attrValue := []byte("test-value")

			// Add another attribute.
			attrName2 := "test-attr2"
			attrValue2 := []byte("test-value2")

			require.NoError(t, xattrs.Set(attrName, attrValue))
			require.NoError(t, xattrs.Set(attrName2, attrValue2))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			// Remove the first attribute.
			require.NoError(t, xattrs.Remove(attrName))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			_, err = xattrs.Get(attrName)
			assert.ErrorIs(t, err, writablefs.ErrNoSuchAttr)

			// Ensure the second attribute is still present.
			retrievedValue, err := xattrs.Get(attrName2)
			require.NoError(t, err)
			assert.Equal(t, attrValue2, retrievedValue)

			// Remove the second attribute.
			require.NoError(t, xattrs.Remove(attrName2))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			_, err = xattrs.Get(attrName2)
			assert.ErrorIs(t, err, writablefs.ErrNoSuchAttr)
		})

		t.Run("Remove - Non-Existent", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Remove a non-existent attribute.
			attrName := "test-attr"
			require.NoError(t, xattrs.Remove(attrName))

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())
		})

		t.Run("Persist after Close", func(t *testing.T) {
			testPath := randomString(10)

			f, err := fsys.OpenFile(testPath, writablefs.FlagCreate|writablefs.FlagReadWrite)
			require.NoError(t, err)

			_, err = f.Write([]byte("just a test"))
			require.NoError(t, err)

			require.NoError(t, f.Sync())

			xattrs, err := f.XAttrs()
			require.NoError(t, err)

			// Add a basic attribute.
			attrName := "test-attr"
			attrValue := []byte("test-value")
			require.NoError(t, xattrs.Set(attrName, attrValue))

			// Commit the attribute change.
			require.NoError(t, xattrs.Sync())

			// Close the file.
			require.NoError(t, f.Close())

			// Re-open the file.
			f, err = fsys.OpenFile(testPath, writablefs.FlagReadOnly)
			require.NoError(t, err)

			// Retrieve the extended attributes.
			xattrs, err = f.XAttrs()
			require.NoError(t, err)

			retrievedValue, err := xattrs.Get(attrName)
			require.NoError(t, err)

			assert.Equal(t, attrValue, retrievedValue)
		})

		t.Run("Nonalphanumeric Attribute Names", func(t *testing.T) {
			xattrs, err := setupXAttrs()
			require.NoError(t, err)

			// Define a set of test attributes with non-alphanumeric names.
			testAttrs := map[string][]byte{
				"test.attr":      []byte("value1"),
				"attr#2":         []byte("value2"),
				"another_attr$":  []byte("value3"),
				"$special%&":     []byte("value4"),
				"attr-with-dash": []byte("value5"),
				".startingDot":   []byte("value6"),
				"endingDot.":     []byte("value7"),
				"_underscore":    []byte("value8"),
			}

			// Set each attribute.
			for name, value := range testAttrs {
				require.NoError(t, xattrs.Set(name, value), "Failed to set attribute with non-alphanumeric name")
			}

			// Commit the attribute changes.
			require.NoError(t, xattrs.Sync())

			// Retrieve and check each attribute.
			for name, expectedValue := range testAttrs {
				retrievedValue, err := xattrs.Get(name)
				require.NoError(t, err, "Failed to get attribute with name %s", name)

				assert.Equal(t, expectedValue, retrievedValue)
			}

			// Validate the complete list of attributes.
			names, err := xattrs.List()
			require.NoError(t, err)

			for name := range testAttrs {
				assert.Contains(t, names, strings.ToLower(name))
			}
		})
	})
}
