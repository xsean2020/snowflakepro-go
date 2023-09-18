package snowflakepro

import (
	"sync"
	"time"
)

const (
	SNMask = 0xFFFFFF // 24bit  16777215
)

type SnowflakePro struct {
	sn    uint32
	node  uint16
	nonce uint64
	tms   uint64
	sync.Mutex
}

func NewSnowflakePro(nodeid uint16, nonce uint64) (*SnowflakePro, error) {
	if nonce > MaxNonce {
		return nil, ErrBigNonce
	}

	return &SnowflakePro{
		nonce: nonce,
		node:  nodeid,
	}, nil
}

func (s *SnowflakePro) until(tms int64) {
	dur := tms - time.Now().UnixMilli()
	for dur > 0 {
		time.Sleep(time.Duration(dur) * time.Millisecond)
		dur = tms - time.Now().UnixMilli()
	}
}

func (s *SnowflakePro) Next() SFID {
	var sfid SFID
	sfid.SetNode(s.node)
	sfid.SetNonce(s.nonce)
	s.Lock()
	now := uint64(time.Now().UnixMilli())
	if now <= s.tms { // 同一时刻产生的序列
		s.sn = (s.sn + 1) & SNMask
		if s.sn == 0 {
			s.tms++
			s.until(int64(s.tms))
		}
	} else {
		s.sn = 0
		s.tms = now
	}
	tms := s.tms
	sn := s.sn
	s.Unlock()
	sfid.SetTime(tms)
	sfid.SetSN(sn)
	return sfid
}
