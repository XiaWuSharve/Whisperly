package config

import (
	"errors"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

type Config struct {
	server     *ServerConfig     `mapstructure:"server"`
	mq         *MqConfig         `mapstructure:"mq"`
	component  *ComponentConfig  `mapstructure:"component"`
	tablestore *TablestoreConfig `mapstructure:"tablestore"`
}

type ServerConfig struct {
	// kcp, websocket, all
	Protocol        string `mapstructure:"protocol"`
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	ReadBufferSize  int    `mapstructure:"read-buffer-size"`
	WriteBufferSize int    `mapstructure:"write-buffer-size"`
	TimeTolerance   int64  `mapstructure:"time-tolerance"`
	NodeId          int64  `mapstructure:"node-id"`
	ConnBufSize     int    `mapstructure:"conn-buf-size"`
}

type MqConfig struct {
	NsqdAddress       []string `mapstructure:"nsqd-address"`
	NsqlookupdAddress string   `mapstructure:"nsqlookupd-address"`
}

type ComponentConfig struct {
	ReceiverNum  int `mapstructure:"receiver-num"`
	SenderNum    int `mapstructure:"sender-num"`
	ProcessorNum int `mapstructure:"processor-num"`
}

type TablestoreConfig struct {
	Endpoint      string `mapstructure:"endpoint"`
	Instance      string `mapstructure:"instance"`
	AkId          string `mapstructure:"ak-id"`
	AkSecret      string `mapstructure:"ak-secret"`
	BatchChanSize int    `mapstructure:"batch-chan-size"`
}

var Server = &ServerConfig{
	Protocol:        "kcp",
	Host:            "0.0.0.0",
	Port:            3001,
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	TimeTolerance:   300,
	NodeId:          0,
}

var Mq = &MqConfig{
	NsqdAddress:       []string{"localhost:4150"},
	NsqlookupdAddress: "localhost:4161",
}

var Component = &ComponentConfig{
	ReceiverNum:  10,
	SenderNum:    10,
	ProcessorNum: 3,
}

var Tablestore = &TablestoreConfig{
	Endpoint: "http://101.37.76.38:8084",
	Instance: "x02caat39505",
	AkId:     "abcdefg",
	AkSecret: "abcdefg",
}

var Cfg = Config{
	server:     Server,
	mq:         Mq,
	component:  Component,
	tablestore: Tablestore,
}

func Init(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")
	// cmd
	// TODO 支持嵌套绑定命令行
	cmd.PersistentFlags().String("protocol", Server.Protocol, "protocol for the server (support: kcp, websocket)")
	cmd.PersistentFlags().String("host", Server.Host, "host to bind")
	cmd.PersistentFlags().IntP("port", "p", Server.Port, "port to listen on")
	cmd.PersistentFlags().Int("read-buffer-size", Server.ReadBufferSize, "read buffer size")
	cmd.PersistentFlags().Int("write-buffer-size", Server.WriteBufferSize, "write buffer size")
	cmd.PersistentFlags().Int64("time-tolerance", Server.TimeTolerance, "time tolerance in seconds")
	cmd.PersistentFlags().Int64("node-id", Server.NodeId, "node id")
	cmd.PersistentFlags().Int("conn-buf-size", Server.ConnBufSize, "queue connecting buffer size")
	cmd.PersistentFlags().StringSlice("nsqd-address", Mq.NsqdAddress, "nsqd address list")
	cmd.PersistentFlags().String("nsqlookupd-address", Mq.NsqlookupdAddress, "nsqlookupd address")
	cmd.PersistentFlags().Int("receiver-num", Component.ReceiverNum, "receiver num")
	cmd.PersistentFlags().Int("sender-num", Component.SenderNum, "sender num")
	cmd.PersistentFlags().Int("processor-num", Component.ProcessorNum, "processor num")
	cmd.PersistentFlags().String("endpoint", Tablestore.Endpoint, "endpoint")
	cmd.PersistentFlags().String("instance", Tablestore.Instance, "instance")
	cmd.PersistentFlags().String("ak-id", Tablestore.AkId, "ak id")
	cmd.PersistentFlags().String("ak-secret", Tablestore.AkSecret, "ak secret")

	viper.BindPFlag("server.protocol", cmd.Flags().Lookup("protocol"))
	viper.BindPFlag("server.host", cmd.Flags().Lookup("host"))
	viper.BindPFlag("server.port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("server.read-buffer-size", cmd.Flags().Lookup("read-buffer-size"))
	viper.BindPFlag("server.write-buffer-size", cmd.Flags().Lookup("write-buffer-size"))
	viper.BindPFlag("server.time-tolerance", cmd.Flags().Lookup("time-tolerance"))
	viper.BindPFlag("server.node-id", cmd.Flags().Lookup("node-id"))
	viper.BindPFlag("server.conn-buf-size", cmd.Flags().Lookup("conn-buf-size"))
	viper.BindPFlag("mq.nsqd-address", cmd.Flags().Lookup("nsqd-address"))
	viper.BindPFlag("mq.nsqlookupd-address", cmd.Flags().Lookup("nsqlookupd-address"))
	viper.BindPFlag("component.receiver-num", cmd.Flags().Lookup("receiver-num"))
	viper.BindPFlag("component.sender-num", cmd.Flags().Lookup("sender-num"))
	viper.BindPFlag("component.processor-num", cmd.Flags().Lookup("processor-num"))
	viper.BindPFlag("tablestore.endpoint", cmd.Flags().Lookup("endpoint"))
	viper.BindPFlag("tablestore.instance", cmd.Flags().Lookup("instance"))
	viper.BindPFlag("tablestore.ak-id", cmd.Flags().Lookup("ak-id"))
	viper.BindPFlag("tablestore.ak-secret", cmd.Flags().Lookup("ak-secret"))
}

func Bind(cmd *cobra.Command) {
	// env
	viper.SetEnvPrefix("whisperly")
	viper.AutomaticEnv()
	// config file
	if cfgFile != "" {
		// from cmd
		viper.SetConfigFile(cfgFile)
	} else {
		// from path
		env := viper.GetString("env")
		if env == "" {
			env = "prod"
		}
		viper.SetConfigName("config." + env) // 配置文件名称（没有文件扩展名）
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")                // 把当前目录加入到配置文件的搜索路径中
		viper.AddConfigPath("$HOME/.whisperly") // 配置文件搜索路径，可以设置多个配置文件搜索路径
	}
	if err := viper.ReadInConfig(); err != nil {
		// It's okay if the config file doesn't exist.
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			panic(err)
		}
		slog.Warn("config file not found, using higher level arguments")
	}
	// 绑定 pflag，但仅当 flag 被显式设置时才写入 viper
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&Cfg); err != nil {
		panic(err)
	}
}
