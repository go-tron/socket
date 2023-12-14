package socket

import (
	"slices"
	"sync"
)

var clientMu sync.Mutex

type ClientList []*Client

func (cl *ClientList) RemoveByConnectId(connectId string) {
	clientMu.Lock()
	defer clientMu.Unlock()
	*cl = slices.DeleteFunc(*cl, func(i *Client) bool {
		return connectId == i.Conn.ID()
	})
}

func (cl *ClientList) Add(c *Client) {
	clientMu.Lock()
	defer clientMu.Unlock()
	*cl = append(*cl, c)
}

func (cl *ClientList) GetByClientId(clientId string) (client *Client) {
	for _, c := range *cl {
		if c.ClientId == clientId {
			return c
		}
	}
	return nil
}
