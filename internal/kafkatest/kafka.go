//go:build integration

package kafkatest

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Start(t testing.TB, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hostPort := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(hostPort)
	containerRequest := testcontainers.ContainerRequest{
		Image:        "apache/kafka:4.4.0",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT_HOST://127.0.0.1:" + port + ",PLAINTEXT://localhost:19092",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:29093",
			"KAFKA_LISTENERS":                                "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"CLUSTER_ID":                                     "4L6g3nShT-eMCtK--X86sw",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
			"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR":    "1",
			"KAFKA_LOG_DIRS":                                "/tmp/kraft-combined-logs",
		},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = network.PortMap{
				network.MustParsePort("9092/tcp"): []network.PortBinding{{
					HostIP:   netip.MustParseAddr("127.0.0.1"),
					HostPort: port,
				}},
			}
		},
		WaitingFor: wait.ForListeningPort("9092/tcp").WithStartupTimeout(90 * time.Second),
	}
	broker, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	})
	testcontainers.CleanupContainer(t, broker)
	if err != nil {
		t.Fatal(err)
	}
	return broker, fmt.Sprintf("127.0.0.1:%s", port)
}
