package metrics

import (
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

type Metrics struct {
	preCollectFns []func()
}

type MetricsHandler struct {
	handler         http.Handler
	lastCollectTime time.Time
}

var metrics *Metrics = &Metrics{
	preCollectFns: []func(){},
}

func AddPreCollectFn(fn func()) {
	metrics.preCollectFns = append(metrics.preCollectFns, fn)
}

func StartMetricsServer(logger logrus.FieldLogger, host string, port string) error {
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "9090"
	}

	srv := &http.Server{
		Addr:    host + ":" + port,
		Handler: promhttp.Handler(),
	}

	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}

	go func() {
		logger.Infof("metrics server listening on %v", srv.Addr)
		if err := srv.Serve(listener); err != nil {
			logger.WithError(err).Fatal("Error serving metrics")
		}
	}()

	return nil
}

func GetMetricsHandler() http.Handler {
	return &MetricsHandler{
		handler:         promhttp.Handler(),
		lastCollectTime: time.Now(),
	}
}

func (mh *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if time.Since(mh.lastCollectTime) > 1*time.Second {
		for _, fn := range metrics.preCollectFns {
			fn()
		}
		mh.lastCollectTime = time.Now()
	}

	mh.handler.ServeHTTP(w, r)
}
