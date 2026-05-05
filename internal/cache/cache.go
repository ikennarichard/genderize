package cache

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ProfileListTTL = 5 * time.Minute
	ProfileByIDTTL = 10 * time.Minute
	ListKeyPrefix  = "profiles:list:"
	ByIDKeyPrefix  = "profiles:id:"
)

type Cache struct {
	client *redis.Client
}

func New(redisURL string) (*Cache, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis unreachable: %w", err)
	}

	return &Cache{client: client}, nil
}


func ListKey(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString("&")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(params[k])
	}

	h := sha256.Sum256([]byte(b.String()))
	return ListKeyPrefix + fmt.Sprintf("%x", h)
}

func IDKey(id string) string {
	return ByIDKeyPrefix + id
}

func (c *Cache) Get(ctx context.Context, key string, dest any) (bool, error) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil // cache miss
	}
	if err != nil {
		return false, nil
	}
	if err := json.Unmarshal(val, dest); err != nil {
		return false, nil
	}
	return true, nil
}

func (c *Cache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

func (c *Cache) InvalidateListCache(ctx context.Context) error {
	return c.scanAndDelete(ctx, ListKeyPrefix+"*")
}

func (c *Cache) InvalidateProfileID(ctx context.Context, id string) error {
	return c.client.Del(ctx, IDKey(id)).Err()
}

func (c *Cache) scanAndDelete(ctx context.Context, pattern string) error {
	var cursor uint64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := c.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}