package main

import (
	"github.com/mintance/nginx-clickhouse/clickhouse"
	"github.com/mintance/nginx-clickhouse/log_field_mapper"
	"github.com/satyrius/gonx"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	configParser "github.com/mintance/nginx-clickhouse/config"
	"github.com/mintance/nginx-clickhouse/nginx"
	"github.com/papertrail/go-tail/follower"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var (
	locker sync.Mutex
	logs   []string
)

var (
	linesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nginx_clickhouse_lines_processed_total",
		Help: "The total number of processed log lines",
	})
	linesNotProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nginx_clickhouse_lines_not_processed_total",
		Help: "The total number of log lines which was not processed",
	})
)

func RemapEntryValuesInplace(mapper *log_field_mapper.FieldMapper, entries []gonx.Entry){
	for _, entry := range entries {

		valueByField := entry.Fields()
		for field, value := range valueByField {
			mappedValue, err := mapper.Map(field, value)
			if err != nil {
				continue
			}

			opt := mapper.GetOpts(field)
			if opt.Style == configParser.MapStyleReplace {
				valueByField[field] = mappedValue
			} else if opt.Style == configParser.MapStyleAdd {
				valueByField[opt.Alias] = mappedValue
			} else {
				logrus.Error("Unexpected rule style for ", opt.Style)
			}
		}
	}
}

func main() {

	// Read config & incoming flags
	config := configParser.Read()

	// Update config with environment variables if exist
	config.SetEnvVariables()

	nginxParser, err := nginx.GetParser(config)
	fieldMapper := log_field_mapper.NewFieldMapper(config.ClickHouse.Mappings)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	if err != nil {
		logrus.Fatal("Can`t parse nginx log format: ", err)
	}

	logs = []string{}

	logrus.Info("Trying to open logfile: " + config.Settings.LogPath)

	whenceSeek := io.SeekStart
	if config.Settings.SeekFromEnd {
		whenceSeek = io.SeekEnd
	}

	t, err := follower.New(config.Settings.LogPath, follower.Config{
		Whence: whenceSeek,
		Offset: 0,
		Reopen: true,
	})

	if err != nil {
		logrus.Fatal("Can`t tail logfile: ", err)
	}

	go func() {
		for {
			time.Sleep(time.Second * time.Duration(config.Settings.Interval))

			if len(logs) > 0 {

				logrus.Info("Preparing to save ", len(logs), " new log entries.")
				locker.Lock()

				logEntries := nginx.ParseLogs(nginxParser, logs)
				RemapEntryValuesInplace(fieldMapper, logEntries)

				err := clickhouse.Save(config, logEntries)

				if err != nil {
					logrus.Error("Can`t save logs: ", err)
					linesNotProcessed.Add(float64(len(logs)))
				} else {
					logrus.Info("Saved ", len(logs), " new logs.")
					linesProcessed.Add(float64(len(logs)))
				}

				logs = []string{}
				locker.Unlock()
			}
		}
	}()

	// Push new log entries to array
	for line := range t.Lines() {
		locker.Lock()
		logs = append(logs, strings.TrimSpace(line.String()))
		locker.Unlock()
	}
}
