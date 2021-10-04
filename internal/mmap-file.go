package internal

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

const maxMMapSize = 0xFFFFFFFFFFFF

type mMapFile struct {
	size    int
	file    *os.File
	dataRef []byte
	data    *[maxMMapSize]byte
}

func OpenMMapFile(path string, size int) (*mMapFile, error) {
	err := os.MkdirAll(filepath.Dir(path), 0660)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	mmf := new(mMapFile)
	mmf.size = size
	mmf.file = file
	err = mmf.init()
	if err != nil {
		mmf.Close()
	}
	return mmf, err
}

func (mmf *mMapFile) Data() []byte {
	return (*mmf.data)[0:mmf.size]
}

func (mmf *mMapFile) init() error {
	err := mmf.initFile()
	if err != nil {
		return err
	}
	mmf.dataRef, err = syscall.Mmap(int(mmf.file.Fd()), 0, mmf.size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	mmf.data = (*[maxMMapSize]byte)(unsafe.Pointer(&mmf.dataRef[0]))
	return nil
}

func (mmf *mMapFile) initFile() error {
	stat, err := mmf.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= int64(mmf.size) {
		return nil
	}
	_, err = mmf.file.WriteAt(make([]byte, int64(mmf.size)-stat.Size()), stat.Size())
	return err
}

func (mmf *mMapFile) Close() {
	if mmf.dataRef != nil {
		syscall.Munmap(mmf.dataRef)
	}
	if mmf.file != nil {
		mmf.file.Sync()
		mmf.file.Close()
	}
}
