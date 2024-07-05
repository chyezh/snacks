package main

import (
	"log"
	"os"
	"syscall"
)

func getWd() (path string) {
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	log.Println(path)
	return
}

func main() {
	RealRoot, err := os.Open("/")
	defer RealRoot.Close()
	if err != nil {
		log.Fatalf("[ Error ] - /: %v\n", err)
	}
	path := getWd()

	err = syscall.Chroot(path)
	if err != nil {
		log.Fatalf("[ Error ] - chroot: %v\n", err)
	}
	getWd()

	err = RealRoot.Chdir()
	if err != nil {
		log.Fatalf("[ Error ] - chdir(): %v", err)
	}
	getWd()

	err = syscall.Chroot(".")
	if err != nil {
		log.Fatalf("[ Error ] - chroot back: %v", err)
	}
	getWd()
}
