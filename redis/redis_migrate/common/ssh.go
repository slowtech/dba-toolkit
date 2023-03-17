package common

import (
	"bytes"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"path"
	"strings"
)


type Host struct {
	Connection *ssh.Client
}

func (host *Host)Init(hostname string,port string,username string,password string) {
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{ssh.Password(password)},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}
	hostaddress := strings.Join([]string{hostname, port}, ":")
	var err error
	host.Connection, err = ssh.Dial("tcp", hostaddress, config)
	if err != nil {
		panic(err.Error())
	}

}

func (host *Host) Run(cmd string) {
	session, err := host.Connection.NewSession()
	if err != nil {
		panic(err.Error())
	}
	defer session.Close()
	fmt.Println(cmd)
	var buff bytes.Buffer
	session.Stdout = &buff
	if err := session.Run(cmd); err != nil {
		panic(err)
	}
	fmt.Println(buff.String())
}

func (host *Host) Scp(sourceFilePath string,destFilePath string)  {
	session, err := host.Connection.NewSession()
	if err != nil {
		panic(err.Error())
	}
	defer session.Close()

	destFile:= path.Base(destFilePath)
	destDir := path.Dir(destFilePath)

	go func() {
		Buf := make([]byte, 1024)
		w, _ := session.StdinPipe()
		defer w.Close()
		f, _ := os.Open(sourceFilePath)
		defer f.Close()
		fileInfo, _ := f.Stat()
		fmt.Fprintln(w, "C0644", fileInfo.Size(), destFile)
		for {
			n, err := f.Read(Buf)
			fmt.Fprint(w, string(Buf[:n]))
			//time.Sleep(time.Second*1)
			if err != nil {
				if err == io.EOF {
					return
				} else {
					panic(err)
				}
			}
		}
	}()
	if err := session.Run("/usr/bin/scp -qrt "+ destDir); err != nil {
		fmt.Println(err)
	}
}
