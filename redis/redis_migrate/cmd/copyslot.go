package cmd

import (
	"github.com/slowtech/redis_migrate/redisUtil"
	"github.com/spf13/cobra"
)

var (
	copySlotCmd = &cobra.Command{
		Use:   "prepare",
		Short: "Reset the dest Redis Cluster",
		Long:  `Reset the dest Redis Cluster,rearrange the slot followed the source redist Cluster`,
		Example: `
  $ redis_migrate prepare --source 192.168.244.10:6379 --dest 192.168.244.20:6379
  `,
		Run:   copySlot,
	}
	source        string
	dest     string
)

func init() {
	rootCmd.AddCommand(copySlotCmd)
	copySlotCmd.Flags().StringVarP(&source, "source", "s", "", "The source Redis Cluster Address")
	copySlotCmd.Flags().StringVarP(&dest, "dest", "d", "", "The dest Redis Cluster Address")
	copySlotCmd.MarkFlagRequired("source")
	copySlotCmd.MarkFlagRequired("dest")

}

func copySlot(cmd *cobra.Command, args []string) {
	redisUtil.CopySlotInfo(source,dest)
}
