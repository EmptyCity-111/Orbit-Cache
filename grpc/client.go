package grpc

import (
	pb "Orbit-Cache/pb"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type Client struct {
	addr    string
	svcName string
	etcdCli *clientv3.Client
	conn    *grpc.ClientConn
	grpcCli pb.OrbitCacheClient
}

func NewClient(addr string, svcName string, etcdCli *clientv3.Client) (*Client, error) {
	var err error
	if etcdCli == nil {
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"127.0.0.1:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %v", err)
		}
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	grpcClient := pb.NewOrbitCacheClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

func (c *Client) Get(group string, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from OrbitCache: %v", err)
	}
	return res.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group string, key string, value []byte) error {
	res, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value from OrbitCache: %v", err)
	}
	logrus.Infof("grpc set request resp: %+v", res)
	return nil
}

func (c *Client) Delete(group string, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from OrbitCache: %v", err)
	}
	return res.GetValue(), nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
