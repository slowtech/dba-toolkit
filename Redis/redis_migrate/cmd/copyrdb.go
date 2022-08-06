package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	copyRDBCmd = &cobra.Command{
		Use:   "copy",
		Short: "copy RDB from source Redis Cluster to dest Redis Cluster",
		Long:  `copy RDB from source Redis Cluster to dest Redis Cluster`,
		Example: `
  $ redis_migrate copy --source 192.168.244.10:6379 --dest 192.168.244.20:6379
  `,
		Run: copyRDB,
	}
	runSource string
	runDest   string
)

func init() {
	rootCmd.AddCommand(copyRDBCmd)
	copyRDBCmd.Flags().StringVarP(&runSource, "source", "", "", "The dest Host IP")
	copyRDBCmd.Flags().StringVarP(&runDest, "dest", "", "", "The file to scp in source Host")
	copyRDBCmd.MarkFlagRequired("source")
	copyRDBCmd.MarkFlagRequired("dest")
}

func copyRDB(cmd *cobra.Command, args []string) {
	fmt.Println("helloworld")
}
