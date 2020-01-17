package proxy

import (
	"errors"
	"log"
	"net"
	"sync"
)

type ShadowChannelInfo struct {
	targetHost string //此shadowConn 对应的连接的host
	shadowConn net.Conn
	key        string //用于唯一标示此次请求的连接
	appId      string
	sendBytes  int64  //发给shadowConn的字节数
	readBytes  int64  //从shadowConn读取的字节数
}

type ShadowDataChanMgr struct {
	sync.Mutex
	channels map[string]*ShadowChannelInfo //targethost 和shadow 通道关联映射表
}

func (c *ShadowChannelInfo) GetTargetHost() string {
	return c.targetHost
}

func (c *ShadowChannelInfo) GetStoryKey() string {
	return c.key
}

func (c *ShadowChannelInfo) GetConn() net.Conn {
	return c.shadowConn
}

func NewShadowChannel(targetHost string, key string, conn net.Conn, appId string) *ShadowChannelInfo {
	return &ShadowChannelInfo{
		targetHost: targetHost,
		shadowConn: conn,
		key:        key,
		appId : appId,
	}
}

func NewShadowChanMgr() *ShadowDataChanMgr {
	return &ShadowDataChanMgr{}
}

func (m *ShadowDataChanMgr) AddShadowChannel(ch *ShadowChannelInfo) error {
	defer func() {
		m.Unlock()
	}()
	m.Lock()
	if m.channels == nil {
		m.channels = make(map[string]*ShadowChannelInfo)
	}
	if _, ok := m.channels[ch.GetStoryKey()]; ok {
		log.Printf("add shadow channel has existed  %s  %s\n", ch.GetStoryKey(), ch.GetTargetHost())
		return errors.New("add shadow channel has existed \n")
	}
	log.Printf("add target url %s for shadow channel %s \n", ch.GetTargetHost(), ch.GetStoryKey())
	m.channels[ch.GetStoryKey()] = ch
	return nil
}

func (m *ShadowDataChanMgr) RemoveShadowChannel(conn net.Conn) {
	defer func() {
		m.Unlock()
	}()
	m.Lock()
	for _, v := range m.channels {
		if v.shadowConn.RemoteAddr().String() == conn.RemoteAddr().String() {
			delete(m.channels, v.targetHost)
			log.Printf("remove shadow channel for url : %s %s \n", conn.RemoteAddr().String(), v.key)
			break
		}
	}
}

func (m *ShadowDataChanMgr) FindShadowChannel(storyKey string) *ShadowChannelInfo {
	defer func() {
		m.Unlock()
	}()
	m.Lock()
	if _, ok := m.channels[storyKey]; ok {
		log.Printf("find channel for target %s \n", storyKey)
	}
	return m.channels[storyKey]

}
