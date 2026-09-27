package repo

// type CacheClient struct {
// 	rds      redis.Client
// 	consumer mq.Consumer[*datas.Cache]
// }

// var _ mq.Handler[*datas.Cache] = (*CacheClient)(nil)

// func (c *CacheClient) Handle(data *datas.Cache) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	err := c.save(ctx, data.ReceiveId, data.ToByte())
// 	if err != nil {
// 		return fmt.Errorf("unable to save to redis: %w", err)
// 	}
// 	return nil
// }

// const (
// 	PREFIX_PENDING_MESSAGE = "pending_message:"
// 	PREFIX_MIN_OFFSET      = "min_offset:"
// 	PREFIX_MAX_OFFSET      = "max_offset:"
// 	FETCH_SCRIPT_FILENAME  = "static/fetch.lua"
// 	ACK_SCRIPT_FILENAME    = "static/ack.lua"
// 	PUSH_SCRIPT_FILENAME   = "static/push.lua"
// )

// var fetchScript *redis.Script
// var ackScript *redis.Script
// var pushScript *redis.Script

// func LoadScript(filename string) *redis.Script {
// 	data, err := os.ReadFile(filename)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return redis.NewScript(string(data))
// }

// func init() {
// 	fetchScript = LoadScript(FETCH_SCRIPT_FILENAME)
// 	ackScript = LoadScript(ACK_SCRIPT_FILENAME)
// 	pushScript = LoadScript(PUSH_SCRIPT_FILENAME)
// }

// func (c *CacheClient) save(ctx context.Context, id string, data []byte) error {
// 	_, err := pushScript.Run(ctx, c.rds, []string{id}, data).Int()
// 	if err != nil && err != redis.Nil {
// 		return fmt.Errorf("unable to enqueue redis: %w", err)
// 	}
// 	return nil
// }

// func (c *CacheClient) GetRange(ctx context.Context, id string) (int64, int64, error) {
// 	data, err := c.rds.MGet(ctx, PREFIX_MIN_OFFSET+id, PREFIX_MAX_OFFSET+id).Result()
// 	if err != nil {
// 		return 0, 0, err
// 	}
// 	if data[0] == nil {
// 		data[0] = "0"
// 	}
// 	if data[1] == nil {
// 		data[1] = "0"
// 	}
// 	minOffset, _ := strconv.ParseInt(data[0].(string), 10, 64)
// 	maxOffset, _ := strconv.ParseInt(data[1].(string), 10, 64)
// 	return minOffset, maxOffset, nil
// }
