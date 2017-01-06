// Code generated by protoc-gen-gogo.
// source: store.proto
// DO NOT EDIT!

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	store.proto

It has these top-level messages:
	CassandraStoreParams
	CassandraTokenRange
*/
package protos

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type CassandraStoreParams struct {
	Hosts         []string                           `protobuf:"bytes,1,rep,name=hosts" json:"hosts,omitempty"`
	Keyspace      string                             `protobuf:"bytes,2,opt,name=keyspace,proto3" json:"keyspace,omitempty"`
	VerticesTable string                             `protobuf:"bytes,3,opt,name=verticesTable,proto3" json:"verticesTable,omitempty"`
	EdgesTable    string                             `protobuf:"bytes,4,opt,name=edgesTable,proto3" json:"edgesTable,omitempty"`
	BatchOptions  *CassandraStoreParams_BatchOptions `protobuf:"bytes,5,opt,name=batchOptions" json:"batchOptions,omitempty"`
	Timeout       int64                              `protobuf:"varint,6,opt,name=timeout,proto3" json:"timeout,omitempty"`
}

func (m *CassandraStoreParams) Reset()                    { *m = CassandraStoreParams{} }
func (m *CassandraStoreParams) String() string            { return proto.CompactTextString(m) }
func (*CassandraStoreParams) ProtoMessage()               {}
func (*CassandraStoreParams) Descriptor() ([]byte, []int) { return fileDescriptorStore, []int{0} }

func (m *CassandraStoreParams) GetHosts() []string {
	if m != nil {
		return m.Hosts
	}
	return nil
}

func (m *CassandraStoreParams) GetKeyspace() string {
	if m != nil {
		return m.Keyspace
	}
	return ""
}

func (m *CassandraStoreParams) GetVerticesTable() string {
	if m != nil {
		return m.VerticesTable
	}
	return ""
}

func (m *CassandraStoreParams) GetEdgesTable() string {
	if m != nil {
		return m.EdgesTable
	}
	return ""
}

func (m *CassandraStoreParams) GetBatchOptions() *CassandraStoreParams_BatchOptions {
	if m != nil {
		return m.BatchOptions
	}
	return nil
}

func (m *CassandraStoreParams) GetTimeout() int64 {
	if m != nil {
		return m.Timeout
	}
	return 0
}

type CassandraStoreParams_BatchOptions struct {
	MaxSize  int32 `protobuf:"varint,1,opt,name=MaxSize,proto3" json:"MaxSize,omitempty"`
	MaxBytes int32 `protobuf:"varint,2,opt,name=MaxBytes,proto3" json:"MaxBytes,omitempty"`
}

func (m *CassandraStoreParams_BatchOptions) Reset()         { *m = CassandraStoreParams_BatchOptions{} }
func (m *CassandraStoreParams_BatchOptions) String() string { return proto.CompactTextString(m) }
func (*CassandraStoreParams_BatchOptions) ProtoMessage()    {}
func (*CassandraStoreParams_BatchOptions) Descriptor() ([]byte, []int) {
	return fileDescriptorStore, []int{0, 0}
}

func (m *CassandraStoreParams_BatchOptions) GetMaxSize() int32 {
	if m != nil {
		return m.MaxSize
	}
	return 0
}

func (m *CassandraStoreParams_BatchOptions) GetMaxBytes() int32 {
	if m != nil {
		return m.MaxBytes
	}
	return 0
}

type CassandraTokenRange struct {
	Partitioner string `protobuf:"bytes,1,opt,name=partitioner,proto3" json:"partitioner,omitempty"`
	StartToken  string `protobuf:"bytes,2,opt,name=startToken,proto3" json:"startToken,omitempty"`
	EndToken    string `protobuf:"bytes,3,opt,name=endToken,proto3" json:"endToken,omitempty"`
}

func (m *CassandraTokenRange) Reset()                    { *m = CassandraTokenRange{} }
func (m *CassandraTokenRange) String() string            { return proto.CompactTextString(m) }
func (*CassandraTokenRange) ProtoMessage()               {}
func (*CassandraTokenRange) Descriptor() ([]byte, []int) { return fileDescriptorStore, []int{1} }

func (m *CassandraTokenRange) GetPartitioner() string {
	if m != nil {
		return m.Partitioner
	}
	return ""
}

func (m *CassandraTokenRange) GetStartToken() string {
	if m != nil {
		return m.StartToken
	}
	return ""
}

func (m *CassandraTokenRange) GetEndToken() string {
	if m != nil {
		return m.EndToken
	}
	return ""
}

func init() {
	proto.RegisterType((*CassandraStoreParams)(nil), "protos.CassandraStoreParams")
	proto.RegisterType((*CassandraStoreParams_BatchOptions)(nil), "protos.CassandraStoreParams.BatchOptions")
	proto.RegisterType((*CassandraTokenRange)(nil), "protos.CassandraTokenRange")
}

func init() { proto.RegisterFile("store.proto", fileDescriptorStore) }

var fileDescriptorStore = []byte{
	// 284 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x6c, 0x91, 0xc1, 0x4e, 0x3a, 0x31,
	0x10, 0xc6, 0x53, 0xf8, 0x2f, 0x7f, 0x19, 0xf0, 0x52, 0x39, 0x34, 0x1c, 0x4c, 0x43, 0x3c, 0xac,
	0x17, 0x0e, 0xfa, 0x06, 0xe8, 0x95, 0x68, 0x0a, 0x2f, 0x30, 0xb0, 0x13, 0xd8, 0x20, 0xdb, 0x4d,
	0x67, 0x34, 0xe0, 0xbb, 0xf9, 0x6e, 0x66, 0xbb, 0xee, 0x66, 0x31, 0x9e, 0x9a, 0xdf, 0xd7, 0x6f,
	0xa6, 0x33, 0x5f, 0x61, 0xc4, 0xe2, 0x03, 0xcd, 0xcb, 0xe0, 0xc5, 0xeb, 0x41, 0x3c, 0x78, 0xf6,
	0xd5, 0x83, 0xc9, 0x13, 0x32, 0x63, 0x91, 0x05, 0x5c, 0x55, 0x86, 0x57, 0x0c, 0x78, 0x64, 0x3d,
	0x81, 0x64, 0xef, 0x59, 0xd8, 0x28, 0xdb, 0x4f, 0x87, 0xae, 0x06, 0x3d, 0x85, 0xab, 0x03, 0x9d,
	0xb9, 0xc4, 0x2d, 0x99, 0x9e, 0x55, 0xe9, 0xd0, 0xb5, 0xac, 0xef, 0xe0, 0xfa, 0x83, 0x82, 0xe4,
	0x5b, 0xe2, 0x35, 0x6e, 0xde, 0xc8, 0xf4, 0xa3, 0xe1, 0x52, 0xd4, 0xb7, 0x00, 0x94, 0xed, 0x1a,
	0xcb, 0xbf, 0x68, 0xe9, 0x28, 0x7a, 0x09, 0xe3, 0x0d, 0xca, 0x76, 0xff, 0x52, 0x4a, 0xee, 0x0b,
	0x36, 0x89, 0x55, 0xe9, 0xe8, 0xe1, 0xbe, 0x1e, 0x9b, 0xe7, 0x7f, 0xcd, 0x3a, 0x5f, 0x74, 0x0a,
	0xdc, 0x45, 0xb9, 0x36, 0xf0, 0x5f, 0xf2, 0x23, 0xf9, 0x77, 0x31, 0x03, 0xab, 0xd2, 0xbe, 0x6b,
	0x70, 0xfa, 0x0c, 0xe3, 0xc5, 0x2f, 0xe7, 0x12, 0x4f, 0xab, 0xfc, 0x93, 0x8c, 0xb2, 0x2a, 0x4d,
	0x5c, 0x83, 0xd5, 0xd2, 0x4b, 0x3c, 0x2d, 0xce, 0x42, 0x1c, 0x97, 0x4e, 0x5c, 0xcb, 0x33, 0x86,
	0x9b, 0x76, 0xa4, 0xb5, 0x3f, 0x50, 0xe1, 0xb0, 0xd8, 0x91, 0xb6, 0x30, 0x2a, 0x31, 0x48, 0x5e,
	0xb5, 0xa6, 0x10, 0x1b, 0x0e, 0x5d, 0x57, 0xaa, 0x72, 0x60, 0xc1, 0x20, 0xb1, 0xe8, 0x27, 0xcb,
	0x8e, 0x52, 0x3d, 0x4a, 0x45, 0x56, 0xdf, 0xd6, 0x41, 0xb6, 0xbc, 0xa9, 0x3f, 0xef, 0xf1, 0x3b,
	0x00, 0x00, 0xff, 0xff, 0xb2, 0xcc, 0xb0, 0x63, 0xd2, 0x01, 0x00, 0x00,
}
