package api

import (
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Client struct {
	protos.ServiceClient
	conn *grpc.ClientConn
}

func New() *Client {
	return &Client{}
}

func (c *Client) Dial(address string) error {
	// if !strings.HasPrefix(address, "localhost:") {
	// 	// security not implemented atm.
	// 	panic("CLI tool supports only local connections")
	// }

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return errors.Wrap(err, "Pregel API connection failed")
	}

	c.conn = conn
	c.ServiceClient = protos.NewServiceClient(conn)
	return nil
}

func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return err
	}

	c.ServiceClient = nil
	c.conn = nil
	return nil
}
