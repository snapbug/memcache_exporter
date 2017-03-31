package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/Snapbug/gomemcache/memcache"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "memcache"
)

var (
	cacheOperations  = []string{"get", "delete", "incr", "decr", "cas", "touch", "set"}
	cacheStatuses    = []string{"hits", "misses"}
	cmdCounts        = []string{"get", "set", "flush", "touch"}
	usageTimes       = []string{"curr", "total"}
	usageResources   = []string{"items", "connections"}
	bytesDirections  = []string{"read", "written"}
	removalsStatuses = []string{"expired", "evicted"}
)

// Exporter collects metrics from a set of memcache servers.
type Exporter struct {
	mutex        sync.RWMutex
	mc           *memcache.Client
	up           prometheus.Gauge
	uptime       prometheus.Counter
	cache        *prometheus.CounterVec
	commands     *prometheus.CounterVec
	usage        *prometheus.GaugeVec
	bytes        *prometheus.CounterVec
	removals     *prometheus.CounterVec
	active_slabs prometheus.Gauge
	malloced     prometheus.Gauge

	items_number            *prometheus.GaugeVec
	items_age               *prometheus.GaugeVec
	items_evicted           *prometheus.CounterVec
	items_evicted_unfetched *prometheus.CounterVec
	items_outofmemory       *prometheus.CounterVec
	items_tailrepairs       *prometheus.CounterVec
	items_reclaimed         *prometheus.CounterVec
	items_expired_unfetched *prometheus.CounterVec

	slabs_chunk_size      *prometheus.GaugeVec
	slabs_chunks_per_page *prometheus.GaugeVec
	slabs_total_pages     *prometheus.GaugeVec
	slabs_total_chunks    *prometheus.GaugeVec
	slabs_used_chunks     *prometheus.GaugeVec
	slabs_free_chunks     *prometheus.GaugeVec
	slabs_free_chunks_end *prometheus.GaugeVec
	slabs_mem_requested   *prometheus.GaugeVec
	slabs_get_hits        *prometheus.CounterVec
	slabs_cmd_set         *prometheus.CounterVec
	slabs_delete_hits     *prometheus.CounterVec
	slabs_incr_hits       *prometheus.CounterVec
	slabs_decr_hits       *prometheus.CounterVec
	slabs_cas_hits        *prometheus.CounterVec
	slabs_cas_badval_hits *prometheus.CounterVec
	slabs_touch_hits      *prometheus.CounterVec
}

// NewExporter returns an initialized exporter
func NewExporter(server string) *Exporter {
	return &Exporter{
		mc: memcache.New(server),
		up: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        "up",
				Namespace:   namespace,
				Help:        "Are the servers up.",
				ConstLabels: prometheus.Labels{"server": server},
			},
		),
		uptime: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name:        "uptime",
				Namespace:   namespace,
				Help:        "The uptime of the server.",
				ConstLabels: prometheus.Labels{"server": server},
			},
		),
		cache: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "cache",
				Namespace:   namespace,
				Help:        "The cache hits/misses broken down by command (get, touch, etc.).",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"command", "status"},
		),
		commands: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "commands_total",
				Namespace:   namespace,
				Help:        "Count of memcache operations by command (set, flush, etc).",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"command"},
		),
		usage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "usage",
				Namespace:   namespace,
				Help:        "Details the resource usage (items/connections) of the server, by time (current/total).",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"time", "resource"},
		),
		bytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "bytes",
				Namespace:   namespace,
				Help:        "The bytes sent/received by the server.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"direction"},
		),
		removals: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "removal",
				Namespace:   namespace,
				Help:        "Number of items that have been evicted/expired (status), and if the were fetched ever or not.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"status", "fetched"},
		),
		active_slabs: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        "active_slabs",
				Namespace:   namespace,
				Help:        "Total number of slab classes allocated.",
				ConstLabels: prometheus.Labels{"server": server},
			},
		),
		malloced: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name:        "malloced_bytes",
				Namespace:   namespace,
				Help:        "Total amount of memory allocated to slab pages.",
				ConstLabels: prometheus.Labels{"server": server},
			},
		),
		items_number: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "items_number",
				Namespace:   namespace,
				Help:        "The number of items currently stored in this slab class.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_age: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "items_age_seconds",
				Namespace:   namespace,
				Help:        "The age of the oldest item within the slab class, in seconds.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_evicted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_evicted",
				Namespace:   namespace,
				Help:        "The number of items evicted to make way for new entries.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_evicted_unfetched: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_evicted_unfetched",
				Namespace:   namespace,
				Help:        "The number of items evicted and never fetched.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_outofmemory: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_outofmemory",
				Namespace:   namespace,
				Help:        "The number of items for this slab class that have triggered an out of memory error.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_tailrepairs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_tail_repairs",
				Namespace:   namespace,
				Help:        "Number of times the entries for a particular ID need repairing",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_reclaimed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_reclaimed",
				Namespace:   namespace,
				Help:        "Items reclaimed",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		items_expired_unfetched: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "items_expired_unfetched",
				Namespace:   namespace,
				Help:        "The number of items expired and never fetched.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_chunk_size: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_chunk_size_bytes",
				Namespace:   namespace,
				Help:        "Space allocated to each chunk within this slab class.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_chunks_per_page: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_chunks_per_page",
				Namespace:   namespace,
				Help:        "Number of chunks within a single page for this slab class.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_total_pages: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_total_pages",
				Namespace:   namespace,
				Help:        "Number of pages allocated to this slab class.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_total_chunks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_total_chunks",
				Namespace:   namespace,
				Help:        "Number of chunks allocated to the slab class.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_used_chunks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_used_chunks",
				Namespace:   namespace,
				Help:        "Number of chunks allocated to an item.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_free_chunks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_free_chunks",
				Namespace:   namespace,
				Help:        "Number of chunks not yet allocated to items.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_free_chunks_end: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_free_chunks_end",
				Namespace:   namespace,
				Help:        "Number of free chunks at the end of the last allocated page.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_mem_requested: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        "slabs_mem_requested_bytes",
				Namespace:   namespace,
				Help:        "The age of the oldest item within the slab class, in seconds.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_get_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_get_hits",
				Namespace:   namespace,
				Help:        "Number of get hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_cmd_set: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_set_cmds",
				Namespace:   namespace,
				Help:        "Number of set commands on this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_delete_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_delete_hits",
				Namespace:   namespace,
				Help:        "Number of delete hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_incr_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_incr_hits",
				Namespace:   namespace,
				Help:        "Number of increment hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_decr_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_decr_hits",
				Namespace:   namespace,
				Help:        "Number of decrement hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_cas_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_cas_hits",
				Namespace:   namespace,
				Help:        "Number of CAS hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_cas_badval_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_cas_badval_hits",
				Namespace:   namespace,
				Help:        "Number of CAS hits on this chunk where the existing value did not match.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
		slabs_touch_hits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "slabs_touch_hits",
				Namespace:   namespace,
				Help:        "The number of touch hits to this chunk.",
				ConstLabels: prometheus.Labels{"server": server},
			},
			[]string{"slab"},
		),
	}
}

// Describe describes all the metrics exported by the memcache exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.up.Desc()
	ch <- e.uptime.Desc()
	ch <- e.active_slabs.Desc()
	ch <- e.malloced.Desc()

	e.cache.Describe(ch)
	e.commands.Describe(ch)
	e.usage.Describe(ch)
	e.bytes.Describe(ch)
	e.removals.Describe(ch)

	e.items_number.Describe(ch)
	e.items_age.Describe(ch)
	e.items_evicted.Describe(ch)
	e.items_evicted_unfetched.Describe(ch)
	e.items_outofmemory.Describe(ch)
	e.items_tailrepairs.Describe(ch)
	e.items_reclaimed.Describe(ch)
	e.items_expired_unfetched.Describe(ch)

	e.slabs_chunk_size.Describe(ch)
	e.slabs_chunks_per_page.Describe(ch)
	e.slabs_total_pages.Describe(ch)
	e.slabs_total_chunks.Describe(ch)
	e.slabs_used_chunks.Describe(ch)
	e.slabs_free_chunks.Describe(ch)
	e.slabs_free_chunks_end.Describe(ch)
	e.slabs_mem_requested.Describe(ch)
	e.slabs_get_hits.Describe(ch)
	e.slabs_cmd_set.Describe(ch)
	e.slabs_delete_hits.Describe(ch)
	e.slabs_incr_hits.Describe(ch)
	e.slabs_decr_hits.Describe(ch)
	e.slabs_cas_hits.Describe(ch)
	e.slabs_cas_badval_hits.Describe(ch)
	e.slabs_touch_hits.Describe(ch)
}

// Collect fetches the statistics from the configured memcache servers, and
// delivers them as prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// prevent concurrent metric collections
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.cache.Reset()
	e.commands.Reset()
	e.usage.Reset()
	e.bytes.Reset()
	e.removals.Reset()

	e.items_number.Reset()
	e.items_age.Reset()
	e.items_evicted.Reset()
	e.items_evicted_unfetched.Reset()
	e.items_outofmemory.Reset()
	e.items_tailrepairs.Reset()
	e.items_reclaimed.Reset()
	e.items_expired_unfetched.Reset()

	e.slabs_chunk_size.Reset()
	e.slabs_chunks_per_page.Reset()
	e.slabs_total_pages.Reset()
	e.slabs_total_chunks.Reset()
	e.slabs_used_chunks.Reset()
	e.slabs_free_chunks.Reset()
	e.slabs_free_chunks_end.Reset()
	e.slabs_mem_requested.Reset()
	e.slabs_get_hits.Reset()
	e.slabs_cmd_set.Reset()
	e.slabs_delete_hits.Reset()
	e.slabs_incr_hits.Reset()
	e.slabs_decr_hits.Reset()
	e.slabs_cas_hits.Reset()
	e.slabs_cas_badval_hits.Reset()
	e.slabs_touch_hits.Reset()

	itemsvecs := map[string]interface{}{
		"number":            e.items_number,
		"age":               e.items_age,
		"evicted":           e.items_evicted,
		"evicted_unfetched": e.items_evicted_unfetched,
		"outofmemory":       e.items_outofmemory,
		"tailrepairs":       e.items_tailrepairs,
		"reclaimed":         e.items_reclaimed,
		"expired_unfetched": e.items_expired_unfetched,
	}

	slabsvecs := map[string]interface{}{
		"chunk_size":      e.slabs_chunk_size,
		"chunks_per_page": e.slabs_chunks_per_page,
		"total_pages":     e.slabs_total_pages,
		"total_chunks":    e.slabs_total_chunks,
		"used_chunks":     e.slabs_used_chunks,
		"free_chunks":     e.slabs_free_chunks,
		"free_chunks_end": e.slabs_free_chunks_end,
		"mem_requested":   e.slabs_mem_requested,
		"get_hits":        e.slabs_get_hits,
		"cmd_set":         e.slabs_cmd_set,
		"delete_hits":     e.slabs_delete_hits,
		"incr_hits":       e.slabs_incr_hits,
		"decr_hits":       e.slabs_decr_hits,
		"cas_hits":        e.slabs_cas_hits,
		"cas_badval":      e.slabs_cas_badval_hits,
		"touch_hits":      e.slabs_touch_hits,
	}

	stats, err := e.mc.Stats()

	if err != nil {
		e.up.Set(0)
		ch <- e.up
		glog.Infof("Failed to collect stats from memcache: %s", err)
		return
	}

	e.up.Set(1)
	for server, _ := range stats {
		m, err := strconv.ParseUint(stats[server].Stats["uptime"], 10, 64)
		if err != nil {
			e.uptime.Set(0)
		} else {
			e.uptime.Set(float64(m))
		}
		m, err = strconv.ParseUint(stats[server].Stats["total_malloced"], 10, 64)
		if err != nil {
			e.malloced.Set(0)
		} else {
			e.malloced.Set(float64(m))
		}
		m, err = strconv.ParseUint(stats[server].Stats["active_slabs"], 10, 64)
		if err != nil {
			e.active_slabs.Set(0)
		} else {
			e.active_slabs.Set(float64(m))
		}

		for key, vector := range itemsvecs {
			for slab := range stats[server].Items {
				var value float64
				m, err := strconv.ParseUint(stats[server].Items[slab][key], 10, 64)
				if err == nil {
					value = float64(m)
				}
				switch vector := vector.(type) {
				case *prometheus.GaugeVec:
					vector.WithLabelValues(fmt.Sprintf("%d", slab)).Set(value)
				case *prometheus.CounterVec:
					vector.WithLabelValues(fmt.Sprintf("%d", slab)).Set(value)
				}
			}
		}

		for key, vector := range slabsvecs {
			for slab := range stats[server].Slabs {
				var value float64
				m, err := strconv.ParseUint(stats[server].Items[slab][key], 10, 64)
				if err == nil {
					value = float64(m)
				}
				switch vector := vector.(type) {
				case *prometheus.GaugeVec:
					vector.WithLabelValues(fmt.Sprintf("%d", slab)).Set(value)
				case *prometheus.CounterVec:
					vector.WithLabelValues(fmt.Sprintf("%d", slab)).Set(value)
				}
			}
		}

		for _, op := range cacheOperations {
			for _, st := range cacheStatuses {
				m, err := strconv.ParseUint(stats[server].Stats[fmt.Sprintf("%s_%s", op, st)], 10, 64)
				if err != nil {
					e.cache.WithLabelValues(op, st).Set(0)
				} else {
					e.cache.WithLabelValues(op, st).Set(float64(m))
				}
			}
		}

		for _, op := range cmdCounts {
			m, err := strconv.ParseUint(stats[server].Stats[fmt.Sprintf("cmd_%s", op)], 10, 64)
			if err != nil {
				e.commands.WithLabelValues(op).Set(0)
			} else {
				e.commands.WithLabelValues(op).Set(float64(m))
			}
		}

		for _, t := range usageTimes {
			for _, r := range usageResources {
				m, err := strconv.ParseUint(stats[server].Stats[fmt.Sprintf("%s_%s", t, r)], 10, 64)
				if err != nil {
					e.usage.WithLabelValues(t, r).Set(0)
				} else {
					e.usage.WithLabelValues(t, r).Set(float64(m))
				}
			}
		}

		for _, dir := range bytesDirections {
			m, err := strconv.ParseUint(stats[server].Stats[fmt.Sprintf("bytes_%s", dir)], 10, 64)
			if err != nil {
				e.bytes.WithLabelValues(dir).Set(0)
			} else {
				e.bytes.WithLabelValues(dir).Set(float64(m))
			}
		}

		for _, st := range removalsStatuses {
			m, err := strconv.ParseUint(stats[server].Stats[fmt.Sprintf("%s_unfetched", st)], 10, 64)
			if err != nil {
				e.removals.WithLabelValues(st, "unfetched").Set(0)
			} else {
				e.removals.WithLabelValues(st, "unfetched").Set(float64(m))
			}
		}
		m, err = strconv.ParseUint(stats[server].Stats["evicted"], 10, 64)
		if err != nil {
			e.removals.WithLabelValues("evicted", "fetched").Set(0)
		} else {
			e.removals.WithLabelValues("evicted", "fetched").Set(float64(m))
		}
	}

	ch <- e.up
	ch <- e.uptime
	ch <- e.malloced
	ch <- e.active_slabs
	e.cache.Collect(ch)
	e.commands.Collect(ch)
	e.usage.Collect(ch)
	e.bytes.Collect(ch)
	e.removals.Collect(ch)

	for _, vector := range itemsvecs {
		switch vector := vector.(type) {
		case *prometheus.CounterVec:
			vector.Collect(ch)
		case *prometheus.GaugeVec:
			vector.Collect(ch)
		}
	}
	for _, vector := range slabsvecs {
		switch vector := vector.(type) {
		case *prometheus.CounterVec:
			vector.Collect(ch)
		case *prometheus.GaugeVec:
			vector.Collect(ch)
		}
	}
}

func main() {
	var (
		listenAddress = flag.String("web.listen-address", ":9106", "Address to listen on for web interface and telemetry.")
		metricsPath   = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	)

	flag.Parse()

	for _, server := range flag.Args() {
		exporter := NewExporter(server)
		prometheus.MustRegister(exporter)
	}

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Memcached Exporter</title></head>
             <body>
             <h1>Memcached Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	glog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
