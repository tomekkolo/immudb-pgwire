package main

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
)

func runPostgres(t *testing.T) string {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer cli.Close()

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "postgres",
		Tty:   false,
		Env:   []string{"POSTGRES_PASSWORD=immudb", "POSTGRES_USER=immudb", "POSTGRES_DB=defaultdb"},
		ExposedPorts: nat.PortSet{
			nat.Port("5432/tcp"): {},
		},
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			nat.Port("5432/tcp"): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "5432"}},
		},
	}, nil, nil, "")
	if err != nil {
		t.Fatalf(err.Error())
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(2 * time.Second)

	return resp.ID
}

func runImmudb(t *testing.T) string {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer cli.Close()

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "codenotary/immudb:1.4.1",
		Tty:   false,
		Env:   []string{"IMMUDB_PGSQL_SERVER=true"},
		ExposedPorts: nat.PortSet{
			nat.Port("5432/tcp"): {},
		},
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			nat.Port("5432/tcp"): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "5432"}},
		},
	}, nil, nil, "")
	if err != nil {
		t.Fatalf(err.Error())
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(2 * time.Second)

	return resp.ID
}

func stopContainer(t *testing.T, containerID string) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer cli.Close()
	ctx := context.Background()

	assert.NoError(t, cli.ContainerStop(ctx, containerID, container.StopOptions{}))
	assert.NoError(t, cli.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	}))
}
