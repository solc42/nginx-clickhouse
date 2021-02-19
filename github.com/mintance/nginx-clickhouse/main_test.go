package main

import (
	"github.com/mintance/nginx-clickhouse/config"
	"github.com/mintance/nginx-clickhouse/log_field_mapper"
	"github.com/satyrius/gonx"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRemapEntryValuesInplace_SmokeTest(t *testing.T) {
	um := log_field_mapper.NewFieldMapper(
		map[string]config.MappingOptions{
			"uri": {
				Alias: "uri_group",
				Style: "add",
				Rules: map[string]string{
					".*":          "inserted1",
				},
			},
			"configuredKey2": {
				Alias: "aliasName2",
				Style: "replace",
				Rules: map[string]string{
					".*":          "mapped2",
				},
			},
		},
	)

	entries := []gonx.Entry{
		*gonx.NewEntry(
			gonx.Fields{
				"uri": "keptAsIs",
				"configuredKey2": "mapped2",
				"unmappedKey": "keptAsIs",
			},
		),
	}
	RemapEntryValuesInplace(um, entries)

	f,_ := entries[0].Field("uri")
	assert.EqualValues(t, "keptAsIs", f)

	f,_ = entries[0].Field("configuredKey2")
	assert.EqualValues(t, "mapped2", f)

	f,_ = entries[0].Field("uri_group")
	assert.EqualValues(t, "inserted1", f)

	f,_ = entries[0].Field("unmappedKey")
	assert.EqualValues(t, "keptAsIs", f)

}

func TestRemapEntryValuesInplace_NoRulesMatched(t *testing.T) {
	um := log_field_mapper.NewFieldMapper(
		map[string]config.MappingOptions{
			"uri": {
				Alias: "uri_group",
				Style: "add",
				Rules: map[string]string{
					"[0-9]+":          "inserted1",
				},
			},
		},
	)

	entries := []gonx.Entry{
		*gonx.NewEntry(
			gonx.Fields{
				"uri": "keptAsIs",
				"unmappedKey": "keptAsIs",
			},
		),
	}
	RemapEntryValuesInplace(um, entries)

	f,_ := entries[0].Field("uri")
	assert.EqualValues(t, "keptAsIs", f)

	f,_ = entries[0].Field("uri_group")
	assert.EqualValues(t, "keptAsIs", f)

	f,_ = entries[0].Field("unmappedKey")
	assert.EqualValues(t, "keptAsIs", f)

}
