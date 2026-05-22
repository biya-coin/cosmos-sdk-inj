package utils

import (
	iavl "cosmossdk.io/store/seidb/sc/sei-iavl"
	scproto "cosmossdk.io/store/seidb/sc/proto"
	sstypes "cosmossdk.io/store/seidb/ss/types"
)

func CloneBytesNonNil(bz []byte) []byte {
	if bz == nil || len(bz) == 0 {
		return []byte{}
	}
	return append([]byte(nil), bz...)
}

func CloneNamedChangeSets(in []*sstypes.NamedChangeSet) []*sstypes.NamedChangeSet {
	out := make([]*sstypes.NamedChangeSet, 0, len(in))
	for _, named := range in {
		if named == nil || named.ChangeSet == nil {
			continue
		}
		cs := &iavl.ChangeSet{
			Pairs: make([]*iavl.KVPair, 0, len(named.ChangeSet.Pairs)),
		}
		for _, p := range named.ChangeSet.Pairs {
			if p == nil {
				continue
			}
			cs.Pairs = append(cs.Pairs, &iavl.KVPair{
				Delete: p.Delete,
				Key:    CloneBytesNonNil(p.Key),
				Value:  CloneBytesNonNil(p.Value),
			})
		}
		out = append(out, &sstypes.NamedChangeSet{
			Name:      named.Name,
			ChangeSet: cs,
		})
	}
	return out
}

func ToProtoChangeSets(changeSets []*sstypes.NamedChangeSet) []*scproto.NamedChangeSet {
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

func FromProtoChangeSets(protoSets []*scproto.NamedChangeSet) []*sstypes.NamedChangeSet {
	if len(protoSets) == 0 {
		return nil
	}
	out := make([]*sstypes.NamedChangeSet, 0, len(protoSets))
	for _, p := range protoSets {
		if p == nil {
			continue
		}
		pairs := make([]*iavl.KVPair, 0, len(p.Changeset.Pairs))
		for _, pair := range p.Changeset.Pairs {
			if pair == nil {
				continue
			}
			pairs = append(pairs, &iavl.KVPair{
				Key:    pair.Key,
				Value:  pair.Value,
				Delete: pair.Delete,
			})
		}
		out = append(out, &sstypes.NamedChangeSet{
			Name:      p.Name,
			ChangeSet: &iavl.ChangeSet{Pairs: pairs},
		})
	}
	return out
}
