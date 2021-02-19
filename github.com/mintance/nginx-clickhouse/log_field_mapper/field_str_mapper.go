package log_field_mapper

import (
	"errors"
	"fmt"
	"github.com/mintance/nginx-clickhouse/config"
	"github.com/sirupsen/logrus"
	"regexp"
)

const KeyNotRegistered = "key not registered for mapping"

type FieldMapper struct {
	reRulesByNginxFieldName map[string]map[*regexp.Regexp]string
	optsByNginxFieldName map[string]config.MappingOptions
}

func NewFieldMapper(opts map[string]config.MappingOptions) (r *FieldMapper) {
	reRulesByNFN := map[string]map[*regexp.Regexp]string{}
	optsByNFN := map[string]config.MappingOptions{}

	for nginxFieldName, opt := range opts {
		reRulesByNFN[nginxFieldName] = buildReRules(opt.Rules)
		optsByNFN[nginxFieldName] = opt
	}

	mapper := FieldMapper{
		reRulesByNginxFieldName: reRulesByNFN,
		optsByNginxFieldName: optsByNFN,
	}

	logrus.Info("Mapper: ", fmt.Sprintf("%+v", mapper))

	return &mapper
}

func (receiver *FieldMapper)GetOpts(nginxKey string)(opts config.MappingOptions){
	return receiver.optsByNginxFieldName[nginxKey]
}

func buildReRules(regexRules map[string]string) (r map[*regexp.Regexp]string) {
	aliasByRe := make(map[*regexp.Regexp]string)

	for regex, alias := range regexRules {
		re, err := regexp.Compile(regex)
		if err != nil {
			logrus.Fatal("Failed to compile regex", err)
		}
		aliasByRe[re] = alias
	}

	return aliasByRe
}

func (receiver *FieldMapper) Map(nginxField string, value string) (r string, err error) {
	keyRules := receiver.reRulesByNginxFieldName[nginxField]
	if keyRules == nil {
		return "", errors.New(KeyNotRegistered)
	}

	for re, alias := range keyRules {
		if re.MatchString(value) {
			return alias, nil
		}
	}

	return value, nil
}
