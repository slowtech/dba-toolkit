package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "redis_migrate",
	Long:    `Migrate Redis Cluster by copying the RDB file`,
	Version: "0.1",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		//os.Exit(1)
	}

}
