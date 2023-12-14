package dbtypes

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/decred/dcrd/chaincfg/v3"
	"github.com/decred/dcrd/wire"
)

func Test_processTransactions(t *testing.T) {
	blkHex := "09000000f3823cf4fb4c8c44a33737d5ebd7b62d0c366484d1df5bb12a2c2a325b000000553a4380e76df1f63ee52cabde33bde686c7f51961ba083327736181f532ff90036e0943187232a052ec03743a6505571ff276a8bc63dfb1e639b348c170c3ac010015064db0196604000001af1300000e3d5b1ddc0174ce01000000f08e0800eb0800005f3bb85fe7675f00fd8fbb3eacb00bce000000000000000000000000000000000000000000000000090000000103000000010000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff02000000000000000000000e6a0cf08e08003b77cb9b80ac61b5ce8fba040000000000001976a9142da3aaa402b110247f08c3ea2300a0567de77a5b88ac000000000000000001f680ba040000000000000000ffffffff0800002f646372642f0703000000010000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff023330fc0000000000000001c1000000000000000000000e6a0cf08e080092e2adc2996bda3b0000000000000000013330fc000000000000000000ffffffff0001000000020000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffffb41f00f9de5ff9453f5c296597a7cc431ac5a23fffe718879c9050a215f5f3fb0000000001ffffffff0300000000000000000000266a24f3823cf4fb4c8c44a33737d5ebd7b62d0c366484d1df5bb12a2c2a325b000000ef8e080000000000000000000000086a06010009000000e4cd3ae50100000000001abb76a9148aa8ff0b0bb1cc18d8ccbde0f3238934cf511bd788ac0000000000000000021e5097000000000000000000ffffffff020000c67da3e40100000051840800140000006a47304402204428c58798235c129f2cd6b8844143e7c7ecdb195a2c301e5878be0628b9074102207d7757546a65640eacc238aec1daeeed527d78d09639f7c6f9d23d3f97ab16cd012103103f1ba1bdf22504fd670accd5c90bb77c445367e95094225d6bd0e31d05533701000000020000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff747d3121caf765326fde80c8c653845b07ba5bb65e8574c9a8ad480a1efbf2940000000001ffffffff0300000000000000000000266a24f3823cf4fb4c8c44a33737d5ebd7b62d0c366484d1df5bb12a2c2a325b000000ef8e080000000000000000000000086a06010009000000b005fec50100000000001abb76a914926b696efedf5f41223cbb14eef52738dbbf1d0c88ac0000000000000000021e5097000000000000000000ffffffff02000092b566c501000000d78e08000a0000006b4830450221008c4438c9518fb798b4b88e29f342d461921acba78ee2d6028f5fa687260a218a022079cfa3fd3701b8ddc741b9a68194592c6c9be9a12795b23d09ac4d50d25b42060121036bf4f4bd0f45dfc39b9a68a75f7c233957c9a5361e8b88b7974b5cda8a27a34a01000000020000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffffef847259e5ca29f066ad06984ba7785e5c7b6054d0bfdbdae6223168dbc973e30000000001ffffffff0300000000000000000000266a24f3823cf4fb4c8c44a33737d5ebd7b62d0c366484d1df5bb12a2c2a325b000000ef8e080000000000000000000000086a0601000900000038475c380200000000001abb76a9144f8618172208c558e8c3349e517b08f30cf5b5d788ac0000000000000000021e5097000000000000000000ffffffff0200001af7c43702000000da840800080000006a47304402207aa0150390c92b66a1b311265c0b9b77301b5f0f35c262408921edd3b52b57b5022014e22c7a5f9cc568efd6f206fca2bd7b7e965f86be9168c6bf7516e41a065d930121028f9a6268cb20ad9131835d5b3978f935e0ac61d42889c1acd6666523c3d14ad401000000020000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff32b3a64f7aa27779f2ebc0d055bbcc0384fad8580fb4aabaa870fb6b0c6c17db0000000001ffffffff0300000000000000000000266a24f3823cf4fb4c8c44a33737d5ebd7b62d0c366484d1df5bb12a2c2a325b000000ef8e080000000000000000000000086a060100090000005cd60df60100000000001abb76a91452f035ea3fb8676ae1ab36578da8e1840fe4c2f888ac0000000000000000021e5097000000000000000000ffffffff0200003e8676f501000000448d0800070000006a473044022004fbd18cee24dcc6e50484fa843b0396861acb0a1a726a311a29b5b758f25ddb022034631352b96a039ce904bd8352acb37f48cdad89eb45529ea0a6c2fc12adb2ae01210365083a49414d26482b505f1a4f6944a3058f6ae804f076e05ffec26cb55b80ac0100000001a0c2b0a6aa8c06fcbc540f06406109b164ca83e8eb375fbd174b232a12c7bcfb0000000001ffffffff01a67d76f50100000000001abc76a9145bac30978b45a3a4a4ae6c5d594bf61ef36dc69788ac0000000000000000013e8676f501000000498d0800100000006b483045022100925c9529a5dcbd023b6e731b7f6980288094c62d535082fe2faa1e1efd3ddd62022075291d0939671b86c5d8dca625e3519d96903705de497a46ce444a8d322ec272012103b5951978e2a1d0a83359a022a61ea7deb4dd50731efcfbb1083d8f8a894bf4cc03000000010000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff0200000000000000000000226a20f6eaf505000000000c9ef1c885c62d089ff9988eb4ca4c2c620614eb75c620bc00e1f5050000000000001ac376a914bd15503ed7d24fc5b36ceba70ee8741a36fe3dca88ac00000000f28e080001f6eaf5050000000000000000ffffffff644054a36eb67457facef5a075f8ebba5d7a9342dd13b45c6d3dd43b22bead35428603fd805eea93674aa7cd120e46e3fa7a2c563926686909c8eb1b06b40237f3a52103beca9bbd227ca6bb5a58e03a36ba2b52fff09093bd7a50aee1193bccd257fb8ac2"
	var msgBlock wire.MsgBlock
	err := msgBlock.Deserialize(hex.NewDecoder(strings.NewReader(blkHex)))
	if err != nil {
		t.Fatalf("Failed to deserialize block: %v", err)
	}

	txs, vouts, vins := processTransactions(&msgBlock, wire.TxTreeStake, chaincfg.TestNet3Params(), true, true)
	tspendidx := 6
	spew.Dump("tx:", txs[tspendidx])
	spew.Dump("vout[0]:", vouts[tspendidx][0])
	spew.Dump("vout[1]:", vouts[tspendidx][1])
	spew.Dump("vins:", vins[tspendidx])
}
