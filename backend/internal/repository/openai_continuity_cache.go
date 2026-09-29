package repository

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func (c *gatewayCache) AcquireContinuityLease(ctx context.Context, group int64, hash, owner string, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, buildSessionKey(group, hash)+":lease", owner, ttl).Result()
}

var renewContinuityScript = redis.NewScript(`
 if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
 redis.call('PEXPIRE',KEYS[1],ARGV[2]); return 1
`)

func (c *gatewayCache) RenewContinuityLease(ctx context.Context, group int64, hash, owner string, ttl time.Duration) (bool, error) {
	n, err := renewContinuityScript.Run(ctx, c.rdb, []string{buildSessionKey(group, hash) + ":lease"}, owner, ttl.Milliseconds()).Int()
	return n == 1, err
}

var commitContinuityScript = redis.NewScript(`
 local lease = redis.call('GET',KEYS[2])
 if lease ~= ARGV[1] then
   if lease or redis.call('GET',KEYS[3]) ~= ARGV[1] then return 0 end
 end
 redis.call('SET',KEYS[1],ARGV[2],'PX',ARGV[3])
 redis.call('SET',KEYS[3],ARGV[1],'PX',ARGV[3])
 if lease == ARGV[1] then redis.call('DEL',KEYS[2]) end
 return 1
`)

func (c *gatewayCache) CommitContinuityBinding(ctx context.Context, group int64, hash, owner string, accountID int64, ttl time.Duration) (bool, error) {
	key := buildSessionKey(group, hash)
	n, err := commitContinuityScript.Run(ctx, c.rdb, []string{key, key + ":lease", key + ":version"}, owner, accountID, ttl.Milliseconds()).Int()
	return n == 1, err
}

var releaseContinuityScript = redis.NewScript(`
 if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end
 return redis.call('DEL',KEYS[1])
`)

func (c *gatewayCache) ReleaseContinuityLease(ctx context.Context, group int64, hash, owner string) error {
	return releaseContinuityScript.Run(ctx, c.rdb, []string{buildSessionKey(group, hash) + ":lease"}, owner).Err()
}

var checkContinuityScript = redis.NewScript(`
 local lease = redis.call('GET',KEYS[2])
 if lease == ARGV[1] then return 1 end
 if lease then return 0 end
 if redis.call('GET',KEYS[1]) == ARGV[2] then return 1 end
 return 0
`)

func (c *gatewayCache) CheckContinuityBinding(ctx context.Context, group int64, hash, owner string, accountID int64) (bool, error) {
	key := buildSessionKey(group, hash)
	n, err := checkContinuityScript.Run(ctx, c.rdb, []string{key, key + ":lease"}, owner, accountID).Int()
	return n == 1, err
}
