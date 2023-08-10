package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	blockPath  = "data/block.parquet"
	walletPath = fmt.Sprintf(
		"data/%s.parquet", "0x0de9e70e3b2c9f3d2976ba0faa055ee5e14ad7f7",
	)
)

func main() {
	readBlock()
	readWallet()
}

func readBlock() {
	if _, err := os.Stat(blockPath); err == nil {
		fmt.Printf("File block exists\n")
	} else {
		fmt.Printf("File block does not exist\n")
		return
	}

	fr, err := local.NewLocalFileReader(blockPath)
	if err != nil {
		log.Print(err)
	}
	pr, err := reader.NewParquetReader(fr, new(Block), 1)
	if err != nil {
		log.Print(err)
	}

	numRows := pr.GetNumRows()
	u := make([]*Block, numRows)
	if err = pr.Read(&u); err != nil {
		log.Print(err)
	}

	pr.ReadStop()
	fr.Close()

	m, _ := json.Marshal(u)
	fmt.Printf("blocks: %s\n", m)
}

func readWallet() {
	if _, err := os.Stat(walletPath); err == nil {
		fmt.Printf("File wallet exists\n")
	} else {
		fmt.Printf("File wallet does not exist\n")
		return
	}

	fr, err := local.NewLocalFileReader(walletPath)
	if err != nil {
		log.Print(err)
	}
	pr, err := reader.NewParquetReader(fr, new(Balance), 1)
	if err != nil {
		log.Print(err)
	}

	numRows := pr.GetNumRows()
	u := make([]*Balance, numRows)
	if err = pr.Read(&u); err != nil {
		log.Print(err)
	}

	pr.ReadStop()
	fr.Close()

	m, _ := json.Marshal(u)
	fmt.Printf("balances: %s\n", m)
}

type Block struct {
	BlockNumber    int64  `parquet:"name=blocknumber, type=INT64"`
	BlockHash      string `parquet:"name=blockhash, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
	BlockTimestamp int64  `parquet:"name=blocktimestamp, type=INT64"`
}

type Balance struct {
	TokenAddress string `parquet:"name=tokenaddress, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
	Value        string `parquet:"name=value, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
	ValueUSD     string `parquet:"name=valueusd, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
}
