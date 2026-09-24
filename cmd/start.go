/*
Copyright © 2025 XiaWuSharve <sharve@foxmail.com>
*/
package cmd

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/XiaWuSharve/whisperly/network/conn"
	"github.com/XiaWuSharve/whisperly/network/listener"
	"github.com/XiaWuSharve/whisperly/persistence/repo"
	"github.com/XiaWuSharve/whisperly/utils"
	"github.com/bwmarrin/snowflake"
	"github.com/gorilla/websocket"
	"github.com/nsqio/go-nsq"
	"github.com/spf13/cobra"
	"github.com/xtaci/kcp-go/v5"
)

// startCmd represents the start command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a broadcast server. ",
	Long:  ``,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		config.Bind(cmd)
		node, err := snowflake.NewNode(config.Server.NodeId)
		if err != nil {
			panic(err)
		}
		datas.Ids = node
	},
	Run: func(cmd *cobra.Command, args []string) {
		mqConfig := nsq.NewConfig()
		receiveMq := mq.ReceiveMq{
			Config:  mqConfig,
			Topic:   "receive",
			Decoder: &datas.Receive{},
		}
		sendMq := mq.ReceiveMq{
			Config:  mqConfig,
			Topic:   "send",
			Decoder: &datas.Receive{},
		}
		storeMq := mq.ReceiveMq{
			Config:  mqConfig,
			Topic:   "store",
			Decoder: &datas.Receive{},
		}
		store, err := repo.NewSyncStore()
		if err != nil {
			// TODO store不可用时降级
			panic(err)
		}
		var pool *conn.Pool = utils.NewShardMap[string, *conn.SendHandler](128, utils.HashFunc)
		cfg := config.Server
		protocol := cfg.Protocol
		host := cfg.Host
		port := cfg.Port
		var l listener.Listener
		slog.Info("starting...", "protocol", protocol)
		switch protocol {
		case "kcp":
			lis, err := kcp.Listen(host + ":" + strconv.Itoa(port))
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			l = &listener.KcpListener{Listener: lis}
		case "websocket":
			upgrader := &websocket.Upgrader{
				ReadBufferSize:  cfg.ReadBufferSize,
				WriteBufferSize: cfg.WriteBufferSize,
			}
			lis, err := net.Listen("tcp", host+":"+strconv.Itoa(port))
			if err != nil {
				slog.Error("cannot create listener", "err", err)
				// 处理错误...
			}
			l = &listener.WsListener{
				Listener: lis,
				Upgrader: upgrader,
			}
		default:
			slog.Error("protocol param cannot be", "value", protocol)
			return
		}
		slog.Info("server starting", "addr", l.Addr().String())
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		signal.Notify(sig, syscall.SIGTERM)
		go func() {
			<-sig
			slog.Info("exiting...")
			l.Close()
		}()
		connChan := make(chan conn.Conn, cfg.ConnBufSize)
		defer close(connChan)
		if err := l.Listen(connChan); !errors.Is(err, net.ErrClosed) {
			slog.Error("cannot serve", "err", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	// Here you will define your flags and configuration settings.
	config.Init(startCmd)
	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// startCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// startCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
