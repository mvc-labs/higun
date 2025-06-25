package blockindexer

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcutil"
)

func GetMinerAndReward(coinbaseTx *btcutil.Tx) (miner string, reward int64) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		miner = ""
	// 		reward = 0
	// 	}
	// }()
	if len(coinbaseTx.MsgTx().TxIn) == 0 {
		return "", 0
	}
	if len(coinbaseTx.MsgTx().TxIn[0].SignatureScript) > 0 {
		miner = getMiner(string(coinbaseTx.MsgTx().TxIn[0].SignatureScript))
	}
	// 4. 统计奖励
	for _, out := range coinbaseTx.MsgTx().TxOut {
		reward += out.Value
	}
	return miner, reward
}
func getMiner(scriptStr string) (miner string) {
	// 2. 用正则提取所有子串
	re := regexp.MustCompile(`([0-9a-zA-Z\.\-]+)`)
	subs := re.FindAllString(scriptStr, -1)
	if len(subs) > 0 {
		// 3. 找到最长的子串作为miner
		maxLen := 0
		for _, sub := range subs {
			if len(sub) > maxLen {
				maxLen = len(sub)
				miner = sub
			}
		}
	}
	return
}
func GetBlockHeightByHashFromNode(hash string) (int64, error) {
	blockInfo, err := client.GetBlockHeaderWithTimeout(hash, 15*time.Second)
	if err != nil {
		return 0, err
	}
	height, ok := blockInfo["height"].(float64)
	if !ok {
		return 0, fmt.Errorf("区块头信息中未找到高度")
	}
	return int64(height), nil
}
func ParseBlockHeightOrHash(s string) (int64, error) {
	// 尝试直接解析为数字
	if height, err := strconv.ParseInt(s, 10, 64); err == nil {
		return height, nil
	}
	// 如果不是数字，判断是否为 hash（长度一般为64且为hex）
	if len(s) == 64 && isHex(s) {
		return GetBlockHeightByHashFromNode(s)
	}
	return 0, fmt.Errorf("参数既不是区块高度也不是合法区块哈希: %s", s)
}

func isHex(s string) bool {
	for _, c := range s {
		if !strings.Contains("0123456789abcdefABCDEF", string(c)) {
			return false
		}
	}
	return true
}
