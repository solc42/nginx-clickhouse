package log_field_mapper

import (
	"github.com/mintance/nginx-clickhouse/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStrMapper(t *testing.T) {
	um := NewFieldMapper(
		map[string]config.MappingOptions{
			"configuredKey": {
				Alias: "aliasName",
				Style: "add",
				Rules: map[string]string{
					"/base/[0-9]+/foo":          "/base/*/foo",
					"/base/[a-zA-Z0-9]+/bar":    "/base/*/bar",
					"/base/[a-zA-Z0-9]+/buz/.*": "/base/*/buz/*",
				},
			},
		},
	)

	opts := um.GetOpts("someUnconfiguredKey")
	assert.EqualValues(t, map[string]string(nil), opts.Rules)
	assert.EqualValues(t, "", opts.Alias)
	assert.EqualValues(t, "", opts.Style)

	opts = um.GetOpts("configuredKey")
	assert.EqualValues(t,
		map[string]string{
			"/base/[0-9]+/foo":          "/base/*/foo",
			"/base/[a-zA-Z0-9]+/bar":    "/base/*/bar",
			"/base/[a-zA-Z0-9]+/buz/.*": "/base/*/buz/*",
		},
		opts.Rules,
	)
	assert.EqualValues(t, "aliasName", opts.Alias)
	assert.EqualValues(t, config.MapStyleAdd, opts.Style)

	actualMapping, err := um.Map("someUnconfiguredKey", "/base/123/foo")
	assert.EqualValues(t, "key not registered for mapping", err.Error())
	assert.EqualValues(t, "", actualMapping)

	actualMapping, err = um.Map("configuredKey", "/base/123/foo")
	assert.EqualValues(t, nil, err)
	assert.EqualValues(t, "/base/*/foo", actualMapping)

	actualMapping, err = um.Map("configuredKey", "/base/123foo")
	assert.EqualValues(t, nil, err)
	assert.EqualValues(t, "/base/123foo", actualMapping)

	actualMapping, err = um.Map("configuredKey", "/base/abcde1234ABCD/buz/omgzomg")
	assert.EqualValues(t, nil, err)
	assert.EqualValues(t, actualMapping, "/base/*/buz/*")
}

func TestStrMapper_MapperReIsContainsNotStrictMatch(t *testing.T) {
	um := NewFieldMapper(
		map[string]config.MappingOptions{

			"key": {
				Alias: "keyAlias",
				Style: "replace",
				Rules: map[string]string{
					"/b/[a-zA-Z0-9]+":   "/b/*",
					"$/b/[a-zA-Z0-9]+^": "$/b/*^",
				},
			},
		},
	)

	//matches first one that contains!
	actualMapping, _ := um.Map("key", "somethingBefore/b/aaaa/somethingAfter")
	assert.EqualValues(t, "/b/*", actualMapping)

	actualMapping, _ = um.Map("key", "/b/aaaa/somethingAfter")
	assert.EqualValues(t, "/b/*", actualMapping)

	actualMapping, _ = um.Map("key", "somethingBefore/b/aaaa")
	assert.EqualValues(t, "/b/*", actualMapping)

	//explicit match with anchors at both ends
	actualMapping, _ = um.Map("key", "/b/aaaa")
	assert.EqualValues(t, "/b/*", actualMapping)

}
