package rootmulti

import (
	scproto "cosmossdk.io/store/seidb/sc/proto"
	sstypes "cosmossdk.io/store/seidb/ss/types"
	ssutils "cosmossdk.io/store/seidb/ss/utils"
)

func toProtoChangeSets(changeSets []*NamedChangeSet) []*scproto.NamedChangeSet {
	return ssutils.ToProtoChangeSets([]*sstypes.NamedChangeSet(changeSets))
}
