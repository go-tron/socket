package socket

import "slices"

type ClientList []*Client

func (cl *ClientList) Remove(clientId string) {
	*cl = slices.DeleteFunc(*cl, func(i *Client) bool {
		return clientId == i.ClientId
	})
}
func (cl *ClientList) Add(c *Client) {
	*cl = append(*cl, c)
}

func (cl *ClientList) Get(clientId string) *Client {
	for _, c := range *cl {
		if c.ClientId == clientId {
			return c
		}
	}
	return nil
}
