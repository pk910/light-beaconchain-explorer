package execution

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/ethpandaops/dora/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

type Pool struct {
	ctx           context.Context
	logger        logrus.FieldLogger
	clientCounter uint16
	clients       []*Client
	chainState    *ChainState
}

func NewPool(ctx context.Context, logger logrus.FieldLogger) *Pool {
	pool := &Pool{
		ctx:        ctx,
		logger:     logger,
		clients:    make([]*Client, 0),
		chainState: newChainState(),
	}

	pool.registerMetrics()

	return pool
}

func (pool *Pool) GetChainState() *ChainState {
	return pool.chainState
}

func (pool *Pool) AddEndpoint(endpoint *ClientConfig) (*Client, error) {
	clientIdx := pool.clientCounter
	pool.clientCounter++
	client, err := pool.newPoolClient(clientIdx, endpoint)

	if err != nil {
		return nil, err
	}

	pool.clients = append(pool.clients, client)

	return client, nil
}

func (pool *Pool) GetAllEndpoints() []*Client {
	return pool.clients
}

func (pool *Pool) GetReadyEndpoints(clientType ClientType) []*Client {
	readyClients := []*Client{}

	for _, client := range pool.clients {
		if !client.isOnline {
			continue
		}

		if clientType > 0 && clientType != client.clientType {
			continue
		}

		readyClients = append(readyClients, client)
	}

	rand.Shuffle(len(readyClients), func(i, j int) {
		readyClients[i], readyClients[j] = readyClients[j], readyClients[i]
	})

	return readyClients
}

func (pool *Pool) GetReadyEndpoint(clientType ClientType) *Client {
	readyClients := pool.GetReadyEndpoints(clientType)
	if len(readyClients) == 0 {
		return nil
	}

	return readyClients[0]
}

func (pool *Pool) AwaitReadyEndpoint(ctx context.Context, clientType ClientType) *Client {
	for {
		client := pool.GetReadyEndpoint(clientType)
		if client != nil {
			return client
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(1 * time.Second):
		}
	}
}

func (pool *Pool) registerMetrics() {
	clientCountGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dora_el_pool_clients",
		Help: "Number of execution clients",
	})
	onlineCountGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dora_el_pool_clients_online",
		Help: "Number of execution clients online",
	})
	syncingCountGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dora_el_pool_clients_syncing",
		Help: "Number of execution clients syncing",
	})

	metrics.AddPreCollectFn(func() {
		online := 0
		syncing := 0
		for _, client := range pool.clients {
			if client.isOnline {
				online++
			}
			if client.isSyncing {
				syncing++
			}
		}

		clientCountGauge.Set(float64(len(pool.clients)))
		onlineCountGauge.Set(float64(online))
		syncingCountGauge.Set(float64(syncing))
	})
}
