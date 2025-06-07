package blockindexer

import (
	"regexp"

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
