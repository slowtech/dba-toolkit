package cmd

import (
	"fmt"
	"github.com/slowtech/redis_migrate/common"
	"github.com/spf13/cobra"
)

var (
	scpFileCmd = &cobra.Command{
		Use:   "scp",
		Short: "scp the specified file to dest ip",
		Long:  `scp the specified file to dest ip`,
		Example: `
  $ redis_migrate scp --sfile /opt/redis/data/dump_6379.rdb --dest 192.168.244.20 --dfile /opt/redis/data/dump_6379.rdb
  `,
		Run:   scpFile,
	}
	sourceFile string
	destFile string
	sourceHost string
	destHost string
)

func init() {
	rootCmd.AddCommand(scpFileCmd)
	scpFileCmd.Flags().StringVarP(&destHost, "dest", "d", "", "The dest Host IP")
	scpFileCmd.Flags().StringVarP(&sourceFile, "sfile", "", "", "The file to scp in source Host")
	scpFileCmd.Flags().StringVarP(&destFile, "dfile", "", "", "The location for the file to save")
	scpFileCmd.MarkFlagRequired("dest")
	scpFileCmd.MarkFlagRequired("sfile")
	scpFileCmd.MarkFlagRequired("dfile")
}

func scpFile(cmd *cobra.Command, args []string) {
	var host common.Host
	host.Init(destHost,"22","root","123456")
	host.Scp(sourceFile,destFile)
	cmdString := fmt.Sprintf("sshpass -p %s scp -r %s %s@%s:%s", "123456", "/tmp/123.txt", "dba", "192.168.244.10", "/tmp/456")
	host.Run(cmdString)
}


