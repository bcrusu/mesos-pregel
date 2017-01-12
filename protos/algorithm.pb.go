// Code generated by protoc-gen-gogo.
// source: algorithm.proto
// DO NOT EDIT!

/*
Package protos is a generated protocol buffer package.

It is generated from these files:
	algorithm.proto

It has these top-level messages:
	ShortestPathAlgorithmParams
	ShortestPathAlgorithmResult
	ShortestPathAlgorithmMessage
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

type ShortestPathAlgorithmParams struct {
	From string `protobuf:"bytes,1,opt,name=from,proto3" json:"from,omitempty"`
	To   string `protobuf:"bytes,2,opt,name=to,proto3" json:"to,omitempty"`
}

func (m *ShortestPathAlgorithmParams) Reset()         { *m = ShortestPathAlgorithmParams{} }
func (m *ShortestPathAlgorithmParams) String() string { return proto.CompactTextString(m) }
func (*ShortestPathAlgorithmParams) ProtoMessage()    {}
func (*ShortestPathAlgorithmParams) Descriptor() ([]byte, []int) {
	return fileDescriptorAlgorithm, []int{0}
}

func (m *ShortestPathAlgorithmParams) GetFrom() string {
	if m != nil {
		return m.From
	}
	return ""
}

func (m *ShortestPathAlgorithmParams) GetTo() string {
	if m != nil {
		return m.To
	}
	return ""
}

type ShortestPathAlgorithmResult struct {
	PathLength int64 `protobuf:"varint,1,opt,name=pathLength,proto3" json:"pathLength,omitempty"`
}

func (m *ShortestPathAlgorithmResult) Reset()         { *m = ShortestPathAlgorithmResult{} }
func (m *ShortestPathAlgorithmResult) String() string { return proto.CompactTextString(m) }
func (*ShortestPathAlgorithmResult) ProtoMessage()    {}
func (*ShortestPathAlgorithmResult) Descriptor() ([]byte, []int) {
	return fileDescriptorAlgorithm, []int{1}
}

func (m *ShortestPathAlgorithmResult) GetPathLength() int64 {
	if m != nil {
		return m.PathLength
	}
	return 0
}

type ShortestPathAlgorithmMessage struct {
	PathLength int64 `protobuf:"varint,1,opt,name=pathLength,proto3" json:"pathLength,omitempty"`
}

func (m *ShortestPathAlgorithmMessage) Reset()         { *m = ShortestPathAlgorithmMessage{} }
func (m *ShortestPathAlgorithmMessage) String() string { return proto.CompactTextString(m) }
func (*ShortestPathAlgorithmMessage) ProtoMessage()    {}
func (*ShortestPathAlgorithmMessage) Descriptor() ([]byte, []int) {
	return fileDescriptorAlgorithm, []int{2}
}

func (m *ShortestPathAlgorithmMessage) GetPathLength() int64 {
	if m != nil {
		return m.PathLength
	}
	return 0
}

func init() {
	proto.RegisterType((*ShortestPathAlgorithmParams)(nil), "protos.ShortestPathAlgorithmParams")
	proto.RegisterType((*ShortestPathAlgorithmResult)(nil), "protos.ShortestPathAlgorithmResult")
	proto.RegisterType((*ShortestPathAlgorithmMessage)(nil), "protos.ShortestPathAlgorithmMessage")
}

func init() { proto.RegisterFile("algorithm.proto", fileDescriptorAlgorithm) }

var fileDescriptorAlgorithm = []byte{
	// 151 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0x4f, 0xcc, 0x49, 0xcf,
	0x2f, 0xca, 0x2c, 0xc9, 0xc8, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x03, 0x53, 0xc5,
	0x4a, 0x8e, 0x5c, 0xd2, 0xc1, 0x19, 0xf9, 0x45, 0x25, 0xa9, 0xc5, 0x25, 0x01, 0x89, 0x25, 0x19,
	0x8e, 0x30, 0x65, 0x01, 0x89, 0x45, 0x89, 0xb9, 0xc5, 0x42, 0x42, 0x5c, 0x2c, 0x69, 0x45, 0xf9,
	0xb9, 0x12, 0x8c, 0x0a, 0x8c, 0x1a, 0x9c, 0x41, 0x60, 0xb6, 0x10, 0x1f, 0x17, 0x53, 0x49, 0xbe,
	0x04, 0x13, 0x58, 0x84, 0xa9, 0x24, 0x5f, 0xc9, 0x16, 0x87, 0x11, 0x41, 0xa9, 0xc5, 0xa5, 0x39,
	0x25, 0x42, 0x72, 0x5c, 0x5c, 0x05, 0x89, 0x25, 0x19, 0x3e, 0xa9, 0x79, 0xe9, 0x25, 0x19, 0x60,
	0x83, 0x98, 0x83, 0x90, 0x44, 0x94, 0xec, 0xb8, 0x64, 0xb0, 0x6a, 0xf7, 0x4d, 0x2d, 0x2e, 0x4e,
	0x4c, 0x4f, 0x25, 0xa4, 0x3f, 0x09, 0xe2, 0x13, 0x63, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0xec,
	0x19, 0xdb, 0xc0, 0xe3, 0x00, 0x00, 0x00,
}
