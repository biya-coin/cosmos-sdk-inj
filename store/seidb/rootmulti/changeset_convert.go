package rootmulti

import (
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	scproto "cosmossdk.io/store/seidb/sc/proto"
)

func toProtoChangeSets(changeSets []*NamedChangeSet) []*scproto.NamedChangeSet {
	if len(changeSets) == 0 {
		return nil
	}
	out := make([]*scproto.NamedChangeSet, 0, len(changeSets))
	for _, named := range changeSets {
		if named == nil || named.ChangeSet == nil || len(named.ChangeSet.Pairs) == 0 {
			continue
		}
		pairs := make([]*iavl.KVPair, 0, len(named.ChangeSet.Pairs))
		for _, pair := range named.ChangeSet.Pairs {
			if pair == nil {
				continue
			}
			pairs = append(pairs, &iavl.KVPair{
				Key:    pair.Key,
				Value:  pair.Value,
				Delete: pair.Delete,
			})
		}
		out = append(out, &scproto.NamedChangeSet{
			Name:      named.Name,
			Changeset: iavl.ChangeSet{Pairs: pairs},
		})
	}
	return out
}
