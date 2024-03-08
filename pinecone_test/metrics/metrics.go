package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	Namespace = "pinecone_test"
	Subsystem = "test_agent"
)

var (
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: Subsystem,
			Name:      "request_duration_seconds",
		},
		[]string{"name", "method", "status"},
	)
	VectorTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: Subsystem,
			Name:      "upsert_vector_total",
		},
		[]string{"name", "method", "status"},
	)
	ReadUnitsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: Subsystem,
			Name:      "read_units_total",
		},
		[]string{"name"},
	)
)

func init() {
	prometheus.DefaultRegisterer.MustRegister(RequestDuration)
	prometheus.DefaultRegisterer.MustRegister(VectorTotal)
	prometheus.DefaultRegisterer.MustRegister(ReadUnitsTotal)
}
