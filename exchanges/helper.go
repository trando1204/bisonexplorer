package exchanges

import "strings"

func isDCRBTCExchange(token string) bool {
	return strings.HasPrefix(token, "btc_")
}
